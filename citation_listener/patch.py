import json
import logging
import os

import httpx
import requests

from citation_listener.facet_mappings import CMIP_TITLE_ORDER, CORDEX_TITLE_ORDER
from citation_listener.stac import build_query_url
from citation_listener.citation import CitationMessageProcessor

from citation_listener.utils import logstream, SUPPORTED_PROJECTS

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def obtain_failed(token: str, timeout: int, citation_base_url):

    with httpx.Client(timeout=timeout, verify=False) as client:
        resp = client.get(os.path.join(citation_base_url, 'failed/?httpAccept=application/json'),
                          headers={'Authorization': f'Token {token}'}).json()

    return [i['id'] for i in resp['items']]

def reverse_id(id: str, facets: list):

    data = {}
    for x, facet in enumerate(facets):
        data[facet] = id.split('.')[x]
    return data

def get_all_items(
        stac_query: str, 
        first_only: bool = False, 
        instant_process: CitationMessageProcessor | None = None) -> list:

    resp = requests.get(stac_query).json()

    item_payloads = []
    has_next = True
    while has_next:

        items = [
            {
                'item_id':item['id'],
                'collection_id': item['collection']}
            for item in resp['features']]

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
            resp = requests.get(
                next(link['href'] for link in resp['links'] if link['rel'] == 'next')).json()

        if first_only:
            has_next = False
            item_payloads = item_payloads[:1]

    return item_payloads

def retry_all_items(items: list, mp:  CitationMessageProcessor):

    for item in items:
        mp.ingest(json.dumps({
            'data':{
                'payload':item
            }
        }))

def patch_stac():

    stac_api = os.environ['STAC_TRANSACTION_API']

    mp = CitationMessageProcessor()

    for collection in SUPPORTED_PROJECTS:
        get_all_items(
            os.path.join(stac_api, f'collections/{collection}/items'),
            instant_process=mp
        )


def patch_citations(additions: list, allow_update_stac: bool = False):

    mp = CitationMessageProcessor(allow_update_stac=allow_update_stac)

    ids = additions + obtain_failed(mp.citation_api_token, mp.timeout, mp.citation_base_url)

    stac_api = os.environ['STAC_TRANSACTION_API']

    all_items = []
    for id in ids:
        logger.info(f'Fetching for record {id}')
        if 'CORDEX-CMIP6' in id:
            data = reverse_id(id, CORDEX_TITLE_ORDER)
        else:
            data = reverse_id(id, CMIP_TITLE_ORDER)

        stac_query = build_query_url(stac_api, data)

        new_items = get_all_items(stac_query, first_only=not allow_update_stac)
        logger.info(f' > Fetched {len(new_items)}')
        all_items += new_items

    retry_all_items(all_items, mp)


    # Map IDs to STAC queries
    # Identify all STAC item/collections
    # Use message processor ingest as normal.
