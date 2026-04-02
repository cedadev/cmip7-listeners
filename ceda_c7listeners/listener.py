__author__ = "Daniel Westwood"
__contact__ = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"

## Consumer Unit for Kafka queues, able to utilise the ORM directly

import glob
import logging
import os
from typing import Union

import requests
from confluent_kafka import Consumer, KafkaException, Producer

from .utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


class BaseKafkaListener:
    def __init__(
        self,
        config: Union[dict, None] = None,
        topics: Union[list, None] = None,
        timeout: Union[int, None] = None,
    ):

        if config is not None:
            # Front: Writes made to write_request -> send_message
            # Back: Writes received via start(msg_received) -> process_message -> handle_message
            self.consumer = Consumer(config)
            self.producer = Producer(config)
        else:
            # Writes made directly from write_request -> handle_update
            self.consumer = None
            self.producer = None

        self.timeout = timeout or 5000  # ms
        self.topics = topics

    def start(self):
        """
        Consumer Listener Loop Start Function

        Runs ONLY on Listener deployment, in which case write_request CANNOT be utilised
        as the management command is blocking this from being used.
        """

        if self.consumer is None:
            raise KafkaException("No configuration provided")

        self.consumer.subscribe(self.topics)
        try:
            logger.info(
                "Kafka consumer started. Subscribed to topics: %s",
                self.topics,
            )

            while True:
                message = self.consumer.poll(timeout_ms=self.timeout)
                logger.info(
                    "Kafka consuming message: %s",
                    message,
                )

                if message is None:
                    continue

                self.receive_message(message)

                self.consumer.commit(message=message, asynchronous=False)

        except KeyboardInterrupt:
            logger.info("Kafka consumer interrupted. Exiting...")

        except KafkaException as e:
            logger.error("Kafka exception: %s", e)

        finally:
            logger.info("Closing Kafka consumer...")

            self.consumer.close()

    def write_request(self, table: str, method: str, content: dict):
        """
        Request to update the database

        This can ONLY be executed by the Frontend service as the backend
        is stuck in the `start()` method loop.

        If the consumer is defined the message system will be utilised for write
        requesting. Otherwise the write can be made directly to the database.
        """
        logger.info('Update Request: None')

        self.send_message(table=table, method=method, content=content)

    def send_message(self, table: str, method: str, content: dict):
        """
        Send message for write request to the events queue.

        If any formal message checks are required beyond the validation of data
        through the Views, here is where they should go.
        """

        message = {"table": table, "method": method, "content": content}

        def delivery_report(err, msg):
            if err is not None:
                raise ValueError(err)
            else:
                logger.info(
                    f"Message {msg.key()} successfully delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
                )

        self.producer.produce(
            topic=self.topics[0],
            key="CitationSvc",
            value=message,
            callback=delivery_report,
        )
        self.producer.flush()

    def receive_message(self, message):
        """
        Interpret message.value in terms of ORM

        Note: Use a separate module to write using ORM so it can
        be used outside the Kafka consumer.
        """
        self.handle_update(**dict(message.value))

    def handle_update(self, **kwargs):
        raise NotImplementedError

class PublisherListener(BaseKafkaListener):

    def __init__(
        self,
        citation_base_url: str,
        citation_api_token: str,
        input_dir: str,
        remote: str = 'https://dap.ceda.ac.uk',
        local: str = '',
        **kwargs
    ):
        self.citation_base_url = citation_base_url
        self.citation_api_token = citation_api_token
        self.remote = remote
        self.local = local
        super().__init__(**kwargs)
        
    def handle_update(self, **stac_publication):
        """
        Handle a message received from the kafka topic
        """

        # Assume message content is a STAC record?

        # Replace as needed
        citation_url = stac_publication.get('properties').get('citation_url')

        if not self.citation_exists(citation_url):
            self.post_citation(citation_url, stac_publication)

        assets = [a['href'] for a in stac_publication['assets'] if a['type'] == 'data' and 'ceda.ac.uk' in a['href']]
        if assets:
            self.create_aggregation(stac_publication['id'], assets)

    def citation_exists(self, citation_url: str) -> bool:
        return requests.get(citation_url.replace('citation','api/citation')).status_code == 200
    
    def post_citation(self, citation_url: str, stac_item: dict):
        """
        Assemble the citation record for publication.
        
        Auto-generating a citation record should include:
        - Record Title
        - Primary Default Author or Author from alternative source.
        - Additional Institutions/Funding Streams/Contacts if available
        """

        title = citation_url.split('/')[-1]

        citation_data = {
            'title': title,
            'primary': {'first_name': 'Daniel', 'last_name': 'Westwood'}
        }

        requests.post(
            url = f'{self.citation_base_url}/api/citations',
            data=citation_data,
            headers={'Authorization':f'Token {self.citation_api_token}'},
            verify=False
        )

    def create_aggregation(self, id: str, assets: list):
        """
        Create an aggregation input file in the 'input' directory.
        """

        with open(f'{self.input_dir}/{id}.txt','w') as f:
            f.write([self.make_local(a) for a in assets])

    def make_local(self, asset_href: str) -> str:
        """
        Return a local path based on the expectation of the remote provider service.
        """
        return asset_href.replace(self.remote, self.local)
    
class AggregationListener(BaseKafkaListener):

    def __init__(
            self,
            output_dir: str,
            quarantine_dir: str,
            **kwargs
        ):
        self.output_dir = output_dir
        super().__init__(**kwargs)

    def start(self):

        if self.producer is None:
            raise KafkaException("No Configuration Provided")
        try:
            logger.info(
                "Kafka producer started. Producing to topics: %s",
                self.topics,
            )
            while True:
                files = glob.glob(self.output_dir)

                # Wait some time to ensure files have fully been written
                for f in files:
                    status, archived_f = self.ingest_file(f)
                    if status:
                        update = self.process_aggregation_as_update(archived_f)
                        self.producer.commit(message=update, asynchronous=False)
                    else:
                        # Quarantine file 
                        os.system(f'mv {f} {f.replace(self.output_dir, self.quarantine_dir)}')
                
        except KeyboardInterrupt:
            logger.info("Kafka consumer interrupted. Exiting...")

        except KafkaException as e:
            logger.error("Kafka exception: %s", e)

        finally:
            logger.info("Closing Kafka consumer...")

            self.consumer.close()

    def ingest_file(self, file):
        """
        Run ingestion via command line or otherwise to archive the aggregation file
        """
        # Ingest to correct archival location
        return True, file

    def process_aggregation_as_update(self, file: str) -> dict:
        """
        Process new aggregation and determine update to STAC item
        """

        #item_map = file.split('/')[-1].split('.kr')[0]
        # Ensure regex map to krX.X
        item_map = ''
        stac_update = {}

        # Arrange aggregation item + STAC 

        return stac_update