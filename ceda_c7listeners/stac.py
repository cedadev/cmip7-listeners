import json
import logging
import os

import click
import requests

from .citation import CitationMessageProcessor
from .facet_mappings import ESGVOC_FACET_LABELS, STAC_COLLECTIONS, STAC_LABELS
from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

@click.command()
@click.argument("citations_file")
def update_all(citations_file: str):
    """
    Workflow to take a set of existing citation records and update the
    corresponding STAC items with a 'citeas' link.
    """

    with open(citations_file) as f:
        citations = [r.strip() for r in f]

    for citation in citations:
        update_stac_items(citation)


def build_query_url(stac_api: str, data: dict):
    """
    Obtain the valid STAC query that should yield datasets for this record
    """

    project_id = data["project_id"].lower()
    
    query = {}
    for label, facet in ESGVOC_FACET_LABELS[project_id].items():
        if data.get(facet, None) is None:
            return False
        
        if facet == 'project_id':
            continue

        query[
            f'{project_id}:{STAC_LABELS.get(label,facet)}'
        ] = {'eq':data[facet]}

    query_url = f'{os.path.join(stac_api,'search')}?collections={STAC_COLLECTIONS[project_id]}'

    query_url += f'&query={json.dumps(query)}'

    # Remove whitespaces
    query_url = query_url.replace(' ','')
    return query_url


def update_stac_items(citation: str):

    citation_url = f'https://cmip7-citations.ceda.ac.uk/citation/{citation}?httpAccept=application/json'

    data = requests.get(citation_url).json()

    query_url = build_query_url(data)
    check_update_stac(query_url, citation_url)


def check_update_stac(query_url: str, citeas_url: dict):
    
    logger.info(f'Querying STAC using: {query_url}')

    processor = CitationMessageProcessor()

    next = True

    while next:

        r = requests.get(query_url).json()

        for item in r['features']:
            check_update_item(item, citeas_url, processor)

        next_url = ''
        next_url = [link['href'] for link in r['links'] if link['rel'] == 'next']

        next = bool(next_url)
        query_url = next_url


def check_update_item(item: dict, citeas_url: str, processor: object):

    update = True
    for link in item['links']:
        if link['rel'] != 'citeas':
            continue
        # Only citeas
        if link['href'] == citeas_url:
            update = False

    if update:
        logger.info(f'PATCH Item: {item["id"]}')
        processor.update_stac(item['id'], item['collection'], citeas_url)

