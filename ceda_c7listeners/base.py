import os
import click
import logging
import time
from ceda_c7listeners.citation import CitationMessageProcessor
from esgf_core_utils.models.kafka.consumer import KafkaConsumer

listeners = {"create_citations": CitationMessageProcessor}

from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def probe_success(healthcheck: str) -> None:

    hdir = "/".join(healthcheck.split("/")[:-1])
    if not os.access(hdir, os.W_OK):
        raise PermissionError("Permission denied accessing healthcheck area")
    open(healthcheck, "a").close()


def probe_fail(healthcheck: str) -> None:
    hdir = "/".join(healthcheck.split("/")[:-1])
    if not os.access(hdir, os.W_OK):
        raise PermissionError("Permission denied accessing healthcheck area")
    os.remove(healthcheck)

def ping_citation_service():
    import requests
    logger.info(f"Waiting to connect to {os.environ['CITATION_BASE_URL']}")
    r = requests.head(f"{os.environ['CITATION_BASE_URL']}/citations")
    return r.status_code == 200

def listen(listener: str, healthcheck: str | None = None):

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

    if not os.environ['CITATION_BASE_URL']:
        if healthcheck:
            probe_fail(healthcheck)
        raise ValueError('Citation Base URL missing')

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
    consumer = KafkaConsumer(message_processor=message_processor)
    try:
        if healthcheck:
            probe_success(healthcheck)
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
