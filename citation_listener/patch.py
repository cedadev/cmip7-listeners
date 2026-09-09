import json
import logging
import os

import httpx
import requests
import click

from citation_listener.facet_mappings import CMIP_TITLE_ORDER, CORDEX_TITLE_ORDER
from citation_listener.citation import CitationMessageProcessor, get_all_items

from citation_listener.utils import logstream, build_query_url

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def obtain_failed(token: str, timeout: int, citation_base_url):
    """
    Obtain responses from the 'failed' endpoint from the Citation Service
    """

    with httpx.Client(timeout=timeout, verify=False) as client:
        resp = client.get(os.path.join(citation_base_url, 'failed/?httpAccept=application/json'),
                          headers={'Authorization': f'Token {token}'}).json()

    return [i['id'] for i in resp['items']]

def reverse_id(id: str, facets: list):
    """
    Reverse a citation record ID back into search facets
    
    Required where the citation record itself does not exist.
    """

    data = {}
    for x, facet in enumerate(facets):
        data[facet] = id.split('.')[x]
    return data

def retry_all_items(items: list, mp:  CitationMessageProcessor):
    """
    Ingest all items identified to message processor"""

    for item in items:
        mp.ingest(json.dumps({
            'data':{
                'payload':item
            }
        }))

def reprocess_citations(
        ids: list, 
        mp: CitationMessageProcessor,
        allow_update_stac: bool = False
    ):
    """
    Reprocess citations and update STAC items as needed"""

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

def patch_citations_main(
    citations_file_or_additions: list | str | None, 
    allow_update_stac: bool = False, 
    update_failed: bool = False):


    additions = []
    if isinstance(citations_file_or_additions, str):
        with open(citations_file_or_additions) as f:
            additions = [r.strip() for r in f]
    elif isinstance(citations_file_or_additions, list):
        additions = citations_file_or_additions
    else:
        pass

    mp = CitationMessageProcessor(allow_update_stac=allow_update_stac)

    if update_failed:
        additions += obtain_failed(mp.citation_api_token, mp.timeout, mp.citation_base_url)

    if len(additions) == 0:
        logger.info('No citations identified to patch')

    reprocess_citations(additions, mp, allow_update_stac=allow_update_stac)


@click.command()
@click.argument("citations_file")
@click.option("--stac", "allow_update_stac", help="allow updates to STAC index", is_flag=True, default=False)
@click.option("--failed", "update_failed", help="Patch failed items", is_flag=True, default=False)
def patch_citations(
    citations_file_or_additions: list | str | None, 
    allow_update_stac: bool = False, 
    update_failed: bool = False):

    patch_citations_main(
        citations_file_or_additions, 
        allow_update_stac=allow_update_stac,
        update_failed=update_failed)
