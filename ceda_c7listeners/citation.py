import logging
from typing import Any
import json
import os
import requests
import time

import httpx
from confluent_kafka import Message as KafkaMessage
from esgf_core_utils.models.kafka.message_processor import MessageProcessor
from esgf_core_utils.models.kafka.consumer import KafkaConsumer, KafkaException

from httpx_auth import OAuth2ClientCredentials

from .utils import logstream, SUPPORTED_PROJECTS
from .external import poll_wdc_api

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class CitationKafkaConsumer(KafkaConsumer):

    def start(self) -> None:
        """Start consuming messages"""
        self.consumer.subscribe(self.settings.topics)

        try:
            logging.info(
                "Kafka consumer started. Subscribed to topics: %s",
                self.settings.topics,
            )

            while True:
                message = None
                message = self.consumer.poll(timeout=self.settings.timeout)
                logging.info(f"Kafka consuming message: {message}")

                if message is None:
                    time.sleep(0.1)
                    continue

                self.message_processor.ingest(message)

                self.consumer.commit(message=message, asynchronous=False)


        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting")

        except KafkaException as e:
            logging.error("Kafka exception: %s", e)

        except Exception as e:
            logging.error("Other exception: %s", e)

        finally:
            logging.info("Closing Kafka consumer")

            self.consumer.close()
    

class CitationMessageProcessor(MessageProcessor):

    def __init__(self, skip_exceptions: bool = False):

        self.skip_exceptions = skip_exceptions

        self.citation_base_url = os.environ['CITATION_BASE_URL']
        self.citation_api_token = os.environ['CITATION_API_TOKEN']

        self.citation_api_new = self.citation_base_url + '/citation/'

        self.stac_api_endpoint = os.environ['STAC_TRANSACTION_API']

        self.stac_headers = {"User-Agent": "citation_listener/0.1.0", "Content-Type": "application/json-patch+json"}
        self.timeout = 30

        self.stac_auth = OAuth2ClientCredentials(
            'https://aai.egi.eu/auth/realms/egi/protocol/openid-connect/token',
            client_id='ae695f1b-3120-400b-a870-6e951c3356fd',
            client_secret=os.environ['STAC_API_SECRET'],
            scope="entitlements:urn:mace:egi.eu:group:esgf.vo.egi.eu:project:*:role=CITATION#aai.egi.eu"
        )

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
                url=f"{self.citation_base_url}/api/citations/",
                json=citation_data,
                headers={"Authorization": f"Token {self.citation_api_token}"},
            )

        logger.info(f'{citation_url}: {response.status_code}')
        try:
            response.raise_for_status()
            return 200
        except Exception as e:
            logger.error(f'Exception encountered: {e}')
            if not self.skip_exceptions:
                raise e
            return 500

    def citation_exists(self, citation_url: str) -> bool:
        """Check if a citation exists."""
        with httpx.Client(timeout=self.timeout) as client:
            return bool(client.get(citation_url).status_code == 200)
    
    def citation_url(self, facet_labels: list, stac_info: dict[str,Any]):

        facet_list = []
        facet_values = {}
        for facet in facet_labels:
            value = stac_info["properties"].get(facet)

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

        return self.citation_url(cordex_facets, stac_info)
    
    def cmip7_citation(self, stac_info: dict):

        cmip7_facets = [
            "cmip7:mip_era",
            "cmip7:activity_id",
            "cmip7:institution_id",
            "cmip7:source_id",
            "cmip7:experiment_id",
        ]

        return self.citation_url(cmip7_facets, stac_info)
    
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

    def ingest(self, message: KafkaMessage | dict) -> None:
        """
        Handle a message received from the kafka topic
        """
    
        # Pull the STAC item from the message into stac_item

        try:
            data    = message.value().decode("utf-8")
            payload = json.loads(data).get('data',{}).get('payload',None)
        except:
            payload = message['data']['payload']

        if not payload:
            raise ValueError('Message contains no payload')
        
        item_id = payload['item_id']
        collection = payload['collection_id']

        logger.info(f'Assessing info for {item_id}')
        
        if collection not in SUPPORTED_PROJECTS:
            logger.info(f'Skipped item: {item_id} - collection {collection} ignored')
            return

        stac_item = requests.get(f'{self.stac_api_endpoint}/collections/{collection}/items/{item_id}').json()

        if 'type' not in stac_item:
            logger.info(f'Error: Failed to fetch item: {item_id} from {collection}')
            return
        
        if self.has_citation_url(stac_item):
            # No further action required
            return

        match collection:
            case 'CORDEX-CMIP6':
                citation_url, facet_data = self.cordex_citation(stac_item)
                
            case _:
                citation_url, facet_data = self.cmip7_citation(stac_item)

        citation_data = facet_data | self.get_author_info(facet_data, collection, item_id) | facet_data

        status = 200
        if not self.citation_exists(citation_url):
            status = self.post_citation(citation_url, citation_data)

        add = True
        for link in stac_item['links']:
            if link['rel'] == 'citeas':
                if link['href'] != citation_url:
                    logger.error(f"STAC Item already has citation at: {link['href']} - new citation would be {citation_url}")
                add = False

        if status != 200:
            return 

        if add:
            # If citation does exist, update the stac record if the citation_url is not present yet.
            self.update_stac(item_id, collection, citation_url)
        else:
            logger.info(f'Skipped pre-existing citation for STAC item {item_id}')
        

    def update_stac(self, stac_id: str, stac_collection: str, citation_url: str):
        
        payload = [{
            "op":"add",
            "path": "/links/-",
            "value": {
    # {
    #   "rel": "self",
    #   "type": "application/geo+json",
    #   "href": "https://transaction-int.east.esgf.io/collections/CORDEX-CMIP6/items/CORDEX-CMIP6.DD.NAM-25.CCCma.CanESM5-1.historical.r1i1p1f2.CanRCM5-SN.v1-r2.mon.tas.v20250101"
    # },
    # {
    #   "rel": "parent",
    #   "type": "application/json",
    #   "href": "https://transaction-int.east.esgf.io/collections/CORDEX-CMIP6"
    # },
    # {
    #   "rel": "collection",
    #   "type": "application/json",
    #   "href": "https://transaction-int.east.esgf.io/collections/CORDEX-CMIP6"
    # },
    # {
    #   "rel": "root",
    #   "type": "application/json",
    #   "href": "https://transaction-int.east.esgf.io/"
    # },
    # {
    #   "rel": "citeas",
    #   "href": "http://127.0.0.1:8000/citation/CORDEX-CMIP6.DD.NAM-25.CCCma.historical.CanRCM5-SN?httpAccept=application/json",
    #   "type": "application/json"
    # }]}]
                "href": citation_url,
                "type": "application/json",
                "rel":"citeas"
            }
        }]

        stac_url = f"{self.stac_api_endpoint}collections/{stac_collection}/items/{stac_id}"

        with httpx.Client(verify=False) as client:
            response = client.patch(
                url=stac_url,
                auth=self.stac_auth,
                json=payload,
                headers=self.stac_headers)
            
        logger.info(f'{stac_url}: {response.status_code}')
        logger.info(response.content)