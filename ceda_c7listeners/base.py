import json
import os

import click
import yaml
from ceda_c7listeners.citation import CitationMessageProcessor
from esgf_core_utils.models.kafka.consumer import KafkaConsumer

listeners = {"create_citations": CitationMessageProcessor}


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

def listen(listener: str, healthcheck: str | None = None):

    # Start KafkaListener
    if listener not in listeners:
        raise ValueError(
            f'Listener "{listener}" not recognised - available listeners: {list(listeners.keys())}'
        )

    mptype = listeners.get(listener)
    if mptype is None:
        raise ValueError("No listener defined")

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
