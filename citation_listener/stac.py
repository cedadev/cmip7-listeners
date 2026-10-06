import json
import logging
import os
import click

from citation_listener.citation import CitationMessageProcessor, get_all_items

from citation_listener.utils import logstream, SUPPORTED_PROJECTS, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

@click.command
@click.argument('collections', type=str)
@click.option('--count-only', is_flag=True)
@click.option('--process', is_flag=True)
@click.option('-v','--verbose', count=True)
def patch_stac(collections: str, count_only: bool, process: bool, verbose: int):
    """
    Update STAC items across ALL projects where 
    cite-as links are missing."""

    set_verbose(verbose)

    stac_api = os.environ['STAC_TRANSACTION_API']

    mp = None
    if process:
        mp = CitationMessageProcessor()

    all_collections = SUPPORTED_PROJECTS
    if collections != 'all':
        all_collections = [collections]

    counts = []
    for collection in all_collections:
        counts.append((collection,get_all_items(
            os.path.join(stac_api, f'collections/{collection}/items','?limit=200'),
            instant_process=mp,
            count_missing_only=count_only
        )))

    if count_only:
        for c in counts:

            query = os.path.join(stac_api, f'collections/{collection}/items','?limit=200').json()
            maxq = query['numberMatched']
            
            print(f'{c[0]}: missing cite-as: {c[1]}/{maxq}')

            # CORDEX-CMIP6: 0/27931
            # CMIP7: 0/12329
            # CMIP6Plus: 0/3113