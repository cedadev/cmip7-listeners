import json
import logging
import os
import time
from typing import Any
import base64
from datetime import datetime, timezone

import httpx
import requests
from esgf_core_utils.models.kafka.consumer import KafkaConsumer, KafkaException
from esgf_core_utils.models.kafka.message_processor import MessageProcessor
from httpx_auth import OAuth2ClientCredentials

from citation_listener.external import poll_wdc_api
from citation_listener.utils import SUPPORTED_PROJECTS, logstream, cite_as_needed

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class CitationKafkaConsumer(KafkaConsumer):

    def static_start(self, startwith) -> None:

        for pseudo_message in startwith:
            message = {
                'data':{
                    'payload':pseudo_message
                }
            }
            try:
                _ = self.message_processor.ingest(json.dumps(message))
            except Exception as e:
                print(e)
                raise

    def start(self) -> None:
        """Start consuming messages"""
        self.consumer.subscribe(self.settings.topics)

        try:
            logger.info(
                "Kafka consumer started. Subscribed to topics: %s",
                self.settings.topics,
            )

            logged = 0
            while True:
                message = None
                try:
                    # Reduce logs by not acknowledging messages until we confirm
                    # they are one of the supported projects
                    message = self.consumer.poll(timeout=self.settings.timeout)

                    if logged > 100:
                        logged = 0
                        logger.info('Silently skipped 100 messages - no action needed')

                    if message is None:
                        logged += 1
                        continue

                    commit, processed = self.message_processor.ingest(
                        message.value().decode('utf-8'))

                    if processed:
                        logged = 0
                    else:
                        logged += 1

                    if commit:
                        self.consumer.commit(message=message, asynchronous=False)

                except KafkaException as e:
                    logger.error("Kafka exception: %s", e)
                    if bool(os.environ.get("RAISE_ALL_INTERNAL_ERRORS",'')):
                        raise

                except ConnectionError:
                    # Always raise Citation Connection Issues
                    raise
        
                except Exception as e:
                    logger.error("Other exception: %s", e)
                    if bool(os.environ.get("RAISE_ALL_INTERNAL_ERRORS",'')):
                        raise

        except KeyboardInterrupt:
            logger.info("Closing Kafka consumer")

            self.consumer.close()

        except Exception:
            raise


class STACItemUpdater:

    def __init__(self, allow_update_stac: bool = True):

        self.timeout     = int(os.environ.get('REQUEST_TIMEOUT',"30"))

        self.allow_update_stac = allow_update_stac

        self.stac_api_endpoint = os.environ['STAC_TRANSACTION_API']
        
        self.stac_headers = {"User-Agent": "citation_listener/0.1.0", "Content-Type": "application/json-patch+json"}

        self.stac_auth = OAuth2ClientCredentials(
            'https://aai.egi.eu/auth/realms/egi/protocol/openid-connect/token',
            client_id=os.environ['STAC_CLIENT_ID'],
            client_secret=os.environ['STAC_API_SECRET'],
            scope="entitlements:urn:mace:egi.eu:group:esgf.vo.egi.eu:project:*:role=CITATION#aai.egi.eu"
        )


    def update_stac_links(self, stac_id: str, stac_collection: str, citation_url: str):
            
        payload = [{
            "op":"add",
            "path": "/links/-",
            "value": {
                "href": citation_url,
                "type": "application/json",
                "rel":"cite-as"
            }
        }]

        stac_url = os.path.join(
            self.stac_api_endpoint,
            f'collections/{stac_collection}/items/{stac_id}'
        )

        logger.info(f"Updating STAC: {stac_url}")

        with httpx.Client(timeout=self.timeout,verify=False) as client:
            response = client.patch(
                url=stac_url,
                auth=self.stac_auth,
                json=payload,
                headers=self.stac_headers)
            
        logger.info(f'{stac_url}: {response.status_code}')
        logger.info(response.content)
    

class CitationMessageProcessor(STACItemUpdater, MessageProcessor):

    def __init__(self, allow_update_stac: bool = True):

        self.skip_exceptions = os.environ.get("RAISE_ALL_INTERNAL_ERRORS")

        self.citation_base_url = os.environ['CITATION_BASE_URL']
        self.citation_api_token = os.environ.get('CITATION_API_TOKEN')

        self.citation_username = os.environ['CITATION_USERNAME']
        self.citation_password = os.environ['CITATION_PASSWORD']
        self.pause_delay = int(os.environ.get('PAUSE_DELAY',"30"))

        self.citation_api_new = os.path.join(self.citation_base_url, 'citation/')

        super().__init__(allow_update_stac)

        if not self.citation_api_token:
            self.refresh_token()

    def refresh_token(self):
        """
        Obtain new or existing token from the citation service.

        This will occur every time the pause cycle triggers, where
        the listener is instructed to pause from the citation service.
        """

        logger.info('Obtaining API token from citation service')
        
        userpass = f'{self.citation_username}:{self.citation_password}'
        header = f'Basic {base64.b64encode(userpass.encode()).decode('utf-8')}'

        with httpx.Client(timeout=self.timeout, verify=False) as client:
            response = client.post(
                url = os.path.join(self.citation_base_url, 'superuser_token',''), 
                headers={'Authorization':header}
            )

            if str(response.status_code) != '200':
                raise ConnectionError(f'Received {response.status_code} from Citation Service')

            self.citation_api_token = response.json().get('token')

    def cycle_pause(self, check_url):
        """
        Remain paused in this loop until the citation service shows unpaused."""

        logger.info('Received instruction to pause. Entering pause cycle...')

        stay_paused = True

        while stay_paused:
            logger.info('Polling citation service...')
            with httpx.Client(timeout=self.timeout, verify=False) as client:
                response = client.get(check_url)
    
                if str(response.status_code) in ['503','504']:
                    logger.error(response.content)
                    raise ConnectionError(f'Received {response.status_code} from Citation Service')
    
                if not response.json().get('pause', False):
                    stay_paused = False
                else:
                    # Re-check every 10 minutes
                    time.sleep(self.pause_delay)
                    

        logger.info('Pause cycle complete.')

        self.refresh_token()


    def post_citation(self, citation_url: str, citation_data: dict[str, Any]) -> None:
        """
        Assemble the citation record for publication.

        Auto-generating a citation record should include:
        - Record Title
        - Primary Default Author or Author from alternative source.
        - Additional Institutions/Funding Streams/Contacts if available
        """

        with httpx.Client(timeout=self.timeout, verify=False) as client:
            response = client.post(
                url=os.path.join(self.citation_base_url, "api/citations/"),
                json=citation_data,
                headers={"Authorization": f"Token {self.citation_api_token}"},
            )

        logger.info(f'{citation_url}: {response.status_code}')
        logger.info(response.content)

        try:
            response.raise_for_status()
            return 200
        except Exception as e:
            logger.error(f'Exception encountered: {e}')
            if not self.skip_exceptions:
                raise
            return 500

    def citation_exists(self, citation_url: str) -> bool:
        """Check if a citation exists."""

        check_url = citation_url.replace(
            '/citation','/listener_check').replace(
                '?httpAccept=application/json','/'
            )

        with httpx.Client(timeout=self.timeout, verify=False) as client:
            response = client.get(check_url)

            if str(response.status_code) in ['503','504']:
                logger.error(response.content)
                raise ConnectionError(f'Received {response.status_code} from Citation Service')

            if response.json().get('pause', False):
                self.cycle_pause(check_url)

            return bool(str(response.status_code) == '200')
    
    def citation_url(self, facet_labels: list, stac_info: dict[str,Any]):

        facet_list = []
        facet_values = {}
        for facet in facet_labels:
            value = stac_info["properties"].get(facet)

            if value is None:
                raise ValueError(f'Required property "{facet}" missing from Item.')

            # Account for list or string properties - pick the first item only for use as citation DRS + facet
            if isinstance(value, list):
                value = value[0]
            facet_values[facet.split(":")[-1]] = value
            facet_list.append(value)

        facet_values['project_id'] = facet_values.pop('mip_era', facet_values.get('project_id')) 
        facet_values['experiment_id'] = facet_values.pop('driving_experiment_id', facet_values.get('experiment_id'))

        citation_url = self.citation_api_new + ".".join(facet_list) + '?httpAccept=application/json'
        return citation_url, facet_values

    def cordex_citation(self, stac_info: dict):

        cordex_facets = [
            "cordex-cmip6:project_id",
            "cordex-cmip6:activity_id",
            "cordex-cmip6:domain_id",
            "cordex-cmip6:institution_id",
            "cordex-cmip6:driving_experiment_id",
            "cordex-cmip6:source_id",
        ]

        citation_url, facet_values = self.citation_url(cordex_facets, stac_info)
        return citation_url, facet_values

    def cmip6plus_citation(self, stac_info: dict):

        cmip6plus_facets = [
            "cmip6plus:mip_era",
            "cmip6plus:activity_id",
            "cmip6plus:institution_id",
            "cmip6plus:source_id",
            "cmip6plus:experiment_id",
        ]

        citation_url, facet_values = self.citation_url(cmip6plus_facets, stac_info)
        return citation_url, facet_values
    
    def cmip7_citation(self, stac_info: dict):

        cmip7_facets = [
            "cmip7:mip_era",
            "cmip7:activity_id",
            "cmip7:institution_id",
            "cmip7:source_id",
            "cmip7:experiment_id",
        ]

        citation_url, facet_values = self.citation_url(cmip7_facets, stac_info)
        return citation_url, facet_values
    
    def get_author_info(self, facets: dict, collection: str, item_id: str) -> dict:
        """
        Get EMD-based author information collected somewhere.

        Also needs to cope with getting CORDEX author information.
        """

        # Special handling for CORDEX CMIP6 using WDC portal
        if collection == 'CORDEX-CMIP6':
            authorset = poll_wdc_api(item_id)
            if authorset:
                return authorset

        return {
            'primary':{
                'first_name':'Citation',
                'last_name': 'Support',
            }
        }
    
    def has_citation_url(self, stac_info: dict):
        return False

    def ingest(self, data: str) -> tuple: # Commit, logged something
        """
        Handle a message received from the kafka topic.

        :param data: (str) JSON serialized kafka message (or static replacement for testing.)
        """
    
        # Pull the STAC item from the message into stac_item
        if 'error' in data:
            raise ValueError(data)
        
        payload = json.loads(data).get('data',{}).get('payload',None)
        if not payload:
            raise ValueError('Message contains no payload')
        
        collection = payload['collection_id']

        # From message poll to here should be minimised
        if collection not in SUPPORTED_PROJECTS:
            return True, False

        item_id = payload['item_id']

        logger.info(f'Assessing info for {item_id}')

        if collection == 'CMIP6Plus':
            pass

        stac_item = requests.get(f'{self.stac_api_endpoint}/collections/{collection}/items/{item_id}').json()

        if 'type' not in stac_item:
            logger.info(f'Error: Failed to fetch item: {item_id} from {collection}')
            return False, True
        
        if self.has_citation_url(stac_item):
            # No further action required
            return True, True
        
        match collection:
            case 'CORDEX-CMIP6':
                citation_url, facet_data = self.cordex_citation(stac_item)
            case 'CMIP6Plus':
                citation_url, facet_data = self.cmip6plus_citation(stac_item)
            case _:
                citation_url, facet_data = self.cmip7_citation(stac_item)

        citation_data = facet_data | self.get_author_info(facet_data, collection, item_id)

        status = 200
        if not self.citation_exists(citation_url):
            status = self.post_citation(citation_url, citation_data)

        add = cite_as_needed(stac_item, citation_url)

        if status != 200:
            return False, True

        if add and self.allow_update_stac:
            # If citation does exist, update the stac record if the citation_url is not present yet.
            self.update_stac_links(item_id, collection, citation_url)
        else:
            logger.info(f'Skipped pre-existing citation for STAC item {item_id}')

        return True, True

def get_all_items(
        stac_query: str, 
        first_only: bool = False,
        count_missing_only: bool = False,
        obtain_citeas: bool = False,
        instant_process: CitationMessageProcessor | None = None) -> list:
    """
    Identify all STAC items corresponding to a query.
    
    May return just one item in a list or all items.
    May also return no items if items are processed instantly.
    """

    resp = requests.get(stac_query).json()
    count = 0
    citeas = []

    item_payloads = []
    has_next = True
    while has_next:

        logger.debug(f'Querying: {stac_query}')
        time.sleep(0.1)

        items = [
            {
                'item_id':item['id'],
                'collection_id': item['collection']}
            for item in resp['features']]

        if count:
            count += len([item for item in resp['features'] if cite_as_needed(item, None)])

        # Instantly process the STAC item 
        if instant_process is not None:
            for item in items:

                # Reprocess all items - only ones needing a cite-as
                # link will actually be reprocessed.
                instant_process.ingest(json.dumps(item))
        else:
            item_payloads += items

        has_next = ('next' in [link['rel'] for link in resp['links']])
        if has_next:
            stac_query = next(link['href'] for link in resp['links'] if link['rel'] == 'next')
            resp = requests.get(stac_query)
            resp.raise_for_status()
            
            resp = resp.json()

        if first_only:
            has_next = False
            item_payloads = item_payloads[:1]

    if count_missing_only:
        return count
    return item_payloads