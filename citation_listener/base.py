import logging
import os
import time

import click

from citation_listener.citation import CitationKafkaConsumer, CitationMessageProcessor
from citation_listener.utils import probe_fail, probe_success, raise_missing_env_errors

listeners = {"create_citations": CitationMessageProcessor}

from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def ping_citation_service():
    import requests
    url = os.path.join(os.environ['CITATION_BASE_URL'],"citations","")
    logger.info(f"Waiting to connect to {url}")
    try:
        r = requests.head(url, verify=False)
    except Exception:
        logger.exception(f'Failed to access {url}')
        return False
    return r.status_code == 200

def listen(
        listener: str, healthcheck: str | None = None, 
        skip_exceptions: bool = False, 
        startwith_items: list | None = None,
        allow_update_stac: bool = True):

    # Immediately mark as ready on setup - will change to fail if there are errors.
    if healthcheck:
        probe_success(healthcheck)

    raise_missing_env_errors(healthcheck)

    # Start KafkaListener
    if listener not in listeners:
        if healthcheck:
            probe_fail(healthcheck)
        raise ValueError(
            f'Listener "{listener}" not recognised - available listeners: {list(listeners.keys())}'
        )

    mptype = listeners.get(listener)
    if mptype is None:
        if healthcheck:
            probe_fail(healthcheck)
        raise ValueError("No listener defined")

    connected = ping_citation_service()
    tries = 0
    while not connected and tries < 100:
        time.sleep(1)
        connected = ping_citation_service()
        tries += 1

    if not connected:
        if healthcheck:
            probe_fail(healthcheck)
        raise ValueError('Could not establish connection to Citation Service in 100s')

    message_processor = mptype(skip_exceptions=skip_exceptions, allow_update_stac=allow_update_stac)
    consumer = CitationKafkaConsumer(message_processor=message_processor)
    try:
        if startwith_items:
            consumer.static_start(startwith_items)
            
        consumer.start()
    except Exception:
        if healthcheck:
            probe_fail(healthcheck)
        raise


@click.command()
@click.argument("listener")
@click.option("--healthcheck", help="path to healthcheck probe")
@click.option("--stac", "allow_update_stac", help="allow updates to STAC index", is_flag=True, default=False)
@click.option("--raise", "raise_exceptions", help="Raise all internal errors", is_flag=True, default=False)
def main(listener: str, healthcheck: str | None, allow_update_stac: bool = False, raise_exceptions: bool = False) -> None:
    """
    Set up a listener given a listener type and set of configurations."""

    listen(listener, healthcheck, skip_exceptions= not raise_exceptions, allow_update_stac=allow_update_stac)

if __name__ == "__main__":
    main()
