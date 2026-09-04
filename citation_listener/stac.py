import json
import logging
import os

from .facet_mappings import ESGVOC_FACET_LABELS, STAC_COLLECTIONS, STAC_LABELS
from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


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

