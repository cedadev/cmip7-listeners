import logging
from typing import Any

import httpx
from confluent_kafka import Message as KafkaMessage
from esgf_core_utils.models.kafka.message_processor import MessageProcessor
from esgcet.egi_oauth2_device_flow import OAuthDeviceFlowPKCE

from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class CitationMessageProcessor(MessageProcessor):

    def __init__(self, citation_base_url: str, citation_api_token: str):
        self.citation_base_url = citation_base_url
        self.citation_api_token = citation_api_token

        self.citation_api_new = citation_base_url + '/api/citations/'

        self.stac_api_endpoint = "https://api.esgf.stac.ceda.ac.uk"
        self.stac_collection = 'CMIP7'
        self.stac_headers = {"User-Agent": "citation_listener/0.1.0", "Content-Type": "application/json-patch+json"},
        
        self.stac_auth = OAuthDeviceFlowPKCE(
            device_endpoint='',
            token_endpoint='',
            client_id='',
            scope='',
            resource=self.stac_api_endpoint
        )

    def post_citation(self, citation_url: str, citation_data: dict[str, Any]) -> None:
        """
        Assemble the citation record for publication.

        Auto-generating a citation record should include:
        - Record Title
        - Primary Default Author or Author from alternative source.
        - Additional Institutions/Funding Streams/Contacts if available
        """

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                url=f"{self.citation_base_url}/api/citations/",
                json=citation_data,
                headers={"Authorization": f"Token {self.citation_api_token}"},
            )

        logger.info(f'{citation_url}: {response.status_code}')

    def citation_exists(self, citation_url: str) -> bool:
        """Check if a citation exists."""
        with httpx.Client(timeout=self.timeout) as client:
            return bool(client.get(citation_url).status_code == 200)
    
    def citation_url(self, facet_labels: list, stac_info: dict[str,Any]):

        facet_list = []
        facet_values = {}
        for facet in facet_labels:
            facet_list.append(stac_info["properties"].get(facet))
            facet_values[facet.split(":")[-1]] = stac_info["properties"].get(facet)

        citation_url = self.citation_api_new + ".".join(facet_list)
        return citation_url, facet_values

    def cordex_citation(self, stac_info: dict):

        cordex_facets = [
            "cmip7:mip_era",
            "cmip7:activity_id",
            "cmip7:institution_id",
            "cmip7:domain_id",
            "cmip7:source_id",
            "cmip7:experiment_id",
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
    
    def get_author_info(self, facets: dict):
        """
        Get EMD-based author information collected somewhere.

        Also needs to cope with getting CORDEX author information.
        """
        return {}
    
    def has_citation_url(self, stac_info: dict):
        return False

    def ingest(self, message: KafkaMessage) -> None:
        """
        Handle a message received from the kafka topic
        """

        # get stac content from message
        # stac_publication = message.value().get("data", {}).get("payload", {}).get("item", None)
        stac_publication: dict[str, Any] = {}

        if not stac_publication:
            # Ignore message with no payload content
            return
        
        if self.has_citation_url(stac_publication):
            # No further action required
            return

        if stac_publication.get('properties',{}).get('cmip7:domain_id'):
            citation_url, facet_data = self.cordex_citation(stac_publication)
        else:
            citation_url, facet_data = self.cmip7_citation(stac_publication)

        citation_data = {
            **self.get_author_info(facet_data)
            **facet_data
        }

        if not self.citation_exists(citation_url):
            self.post_citation(citation_url, citation_data)

        self.update_stac(stac_publication['id'], citation_url)
        # If citation does exist, update the stac record if the citation_url is not present yet.

    def update_stac(self, stac_id: str, citation_url: str):
        
        payload = {
            "id": stac_id,
            "patch": [{
                "op": "add",
                "path": "/links/-",
                "value": {
                    "href": citation_url,
                    "rel": "citeas",
                }
            }]
        }

        stac_url = f"{self.stac_api_endpoint}/collections/{self.stac_collection}/items/{stac_id}"

        with httpx.Client(verify=False) as client:
            response = client.patch(
                url=stac_url,
                auth=self.stac_auth,
                json=payload,
                headers=self.stac_headers)
            
        logger.info(f'{stac_url}: {response.status_code}')

        