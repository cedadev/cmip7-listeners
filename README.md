# cmip7-listeners
Package for CEDA CMIP7 Kafka Listeners.

## Citation Listener
- Listens to Publisher Success queue.
- Interacts with the Citation Service to create new citations.
- Interacts with author information to fill record info before creation.
- Posts updates to the STAC API endpoint with the citation url.
