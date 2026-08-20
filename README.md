# citation-listener
Package for CEDA Citation Kafka Listener.

## Citation Listener
- Listens to Publisher Success queue.
- Interacts with the Citation Service to create new citations.
- Interacts with author information to fill record info before creation.
- Posts updates to the STAC API endpoint with the citation url.

### Environment Variables

The citation listener uses the `esgf_core_utils` Kafka Consumer which uses a pydantic environment variable configuration to enable the consumer main functions. These environment variables are:
- `KAFKA_CONSUMER_CONFIG__GROUP_ID`: must be of the form 'esgf2.east.*'
- `KAFKA_CONSUMER_CONFIG__BOOTSTRAP_SERVERS`
- `KAFKA_CONSUMER_TOPICS`
- `KAFKA_CONSUMER_CONFIG__SECURITY_PROTOCOL`
- `KAFKA_CONSUMER_CONFIG__SASL_MECHANISM`
- `KAFKA_CONSUMER_CONFIG__SASL_USERNAME`
- `KAFKA_CONSUMER_CONFIG__SASL_PASSWORD`

The listener connects to a citation service instance for GET/POST operations which requires:
- `CITATION_BASE_URL`
- `CITATION_API_TOKEN`

The listener also creates PATCH requests to the ESGF STAC to add the `citeas` link to items where necessary, which requires:
- `STAC_TRANSACTION_API`
- `STAC_API_SECRET`

There is also an optional variable to specify the location of a `WDCC_API_MAPPING_FILE` which is used for `CORDEX-CMIP6` mappings where a WDCC entry exists that lists author information.