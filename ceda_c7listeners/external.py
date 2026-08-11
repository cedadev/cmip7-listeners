import os
import logging
import json
import requests

from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


def poll_wdc_api(item_id: str):

    # Not required for non-CORDEX mappings.
    if not os.environ.get('WDC_API_MAPPING_FILE'):
        logger.error('No WDC API Mapping File provided.')
        return None

    mapping_file = os.environ.get('WDC_API_MAPPING_FILE')
    if not os.path.isfile(mapping_file):
        logger.error('WDC API Mapping File missing from filesystem.')
        return None

    with open(mapping_file) as f:
        mappings = json.load(f)

    match_map = '.'.join(item_id.split('.')[:9])
    acronym = mappings.get(match_map)
    if not acronym:
        logger.error(f'No match for {match_map} in WDC API Mapping')
        return None

    r = requests.get(f'https://www.wdc-climate.de/ui/cerarest/entry?acronym={acronym}')
    if r.status_code >= 300:
        logger.error('WDC API not available.')
        return None

    contacts = r.json()['contact']
    authors = []
    primary = None

    for c in contacts:

        if c['CONTACT_TYPE'] not in ['Contact','Author']:
            continue

        name  = c['PERSON_NAME'].split('.')[-1].lstrip()
        email = c['EMAIL'] if isinstance(c['EMAIL'], str) else None
        orcid = c.get('PERSON_EXTERNAL_IDS',[''])[0].split('orcid.org/')[-1] if 'orcid' in c.get('PERSON_EXTERNAL_IDS',[''])[0] else None

        author = {
            'first_name': name.split(' ')[0],
            'last_name': name.split(' ')[-1]
        }
        if len(name.split(' ')) > 2:
            author['middle_names'] = ' '.join(name.split(' ')[1:-1])

        if email:
            author['email'] = email
        if orcid:
            author['orcid'] = orcid

        if c['CONTACT_TYPE'] == 'Contact':
            primary = author
        elif primary is not None and author != primary:
            authors.append(author)

    return {
        'primary':primary,
        'contacts':authors
    }