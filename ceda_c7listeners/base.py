import os
import click
import logging
import time

from ceda_c7listeners.citation import CitationMessageProcessor, CitationKafkaConsumer
from ceda_c7listeners.utils import probe_success, probe_fail, raise_missing_env_errors

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
    except Exception as e:
        print(e)
        return False
    print(r)
    return r.status_code == 200

def listen(listener: str, healthcheck: str | None = None):

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

    message_processor = mptype()
    consumer = CitationKafkaConsumer(message_processor=message_processor)
    try:
        consumer.start()
    except Exception as e:
        if healthcheck:
            probe_fail(healthcheck)


@click.command()
@click.argument("listener")
@click.option("--healthcheck", help="path to healthcheck probe")
def main(listener: str, healthcheck: str | None) -> None:
    """
    Set up a listener given a listener type and set of configurations."""

    listen(listener, healthcheck)

if __name__ == "__main__":
    main()
