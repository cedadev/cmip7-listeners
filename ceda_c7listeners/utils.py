import logging
import os

DEBUG = bool(os.environ.get("DEBUG"))

if DEBUG:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)
logstream = logging.StreamHandler()

formatter = logging.Formatter("%(levelname)s [%(name)s]: %(message)s")
logstream.setFormatter(formatter)

SUPPORTED_PROJECTS = ['CMIP6', 'CMIP7', 'CORDEX-CMIP6']

SUCCESS_MESSAGE = {
        "data": {
            "type": "STAC",
            "payload": {
                "collection_id": "CMIP6",
                "method": "POST",
                "item_id": "CORDEX-CMIP6.DD.NAM-25.CCCma.CanESM5-1.historical.r1i1p1f2.CanRCM5-SN.v1-r2.mon.tas.v20250101",
            },
        },
        "metadata": {
            "auth": {
                "auth_policy_id": None,
                "requester_data": {
                    "client_id": "3da9c21e-2bb9-4576-9054-af420514cb7b",
                    "iss": "https://aai.egi.eu/auth/realms/egi",
                    "sub": "9a70cacc236c77ed52dba355f29114c3e44ba0dc57de37260fc5cf43411d80e3@egi.eu",
                },
            },
            "event_id": "84835111af984493bbf6af37fe33fa0c",
            "publisher": {"package": "west-consumer", "version": "0.1.0"},
            "request_id": "6e378a2b46ef4c448513ef4ca34c3454",
            "time": "2026-07-17T09:18:58.880582",
            "schema_version": "1.0.0",
        },
        "original_event": {
            "event_id": "0508bf6346d443968679a3473eaaacdf",
            "offset": 6323,
            "partition": 4,
        },
}

STAC_ITEM_TEMPLATE = {
    "type": "Feature",
    "stac_version": "1.1.0",
    "stac_extensions": [
        "https://esgf.github.io/stac-transaction-api/cmip7/v1.2.8/schema.json",
        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    ],
    "id": "MIP-DRS7.CMIP7.CMIP.MOHC.DUMMY-MODEL.1pctCO2.r1i1p1f3.glb.mon.tas.tavg-h2m-hxy-u.g999.v20270703",
    "collection": "CMIP7",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-179.0625, -89.375],
                [179.0625, -89.375],
                [179.0625, 89.375],
                [-179.0625, 89.375],
                [-179.0625, -89.375],
            ]
        ],
    },
    "bbox": [-179.0625, -89.375, 179.0625, 89.375],
    "properties": {
        "title": "MIP-DRS7.CMIP7.CMIP.MOHC.DUMMY-MODEL.1pctCO2.r1i1p1f3.glb.mon.tas.tavg-h2m-hxy-u.g999",
        "datetime": None,
        "created": "2026-07-03T10:29:45.495360Z",
        "updated": "2026-07-03T10:29:45.762124Z",
        "start_datetime": "1850-01-16T00:00:00Z",
        "end_datetime": "1999-12-16T00:00:00Z",
        "size": 94856765,
        "retracted": True,
        "access": ["HTTPServer"],
        "latest": False,
        "version": "20270703",
        "project": "CMIP7",
        "cmip7:activity_id": "CMIP",
        "cmip7:area_label": "u",
        "cmip7:region": "glb",
        "cmip7:variable_cf_standard_name": "air_temperature",
        "cmip7:data_specs_version": "MIP-DS7.1.0.0",
        "cmip7:drs_specs": "MIP-DRS7",
        "cmip7:experiment_id": "1pctCO2",
        "cmip7:frequency": "mon",
        "cmip7:grid_label": "g999",
        "cmip7:institution_id": "MOHC",
        "cmip7:nominal_resolution": "100 km",
        "cmip7:product": "model-output",
        "cmip7:realm": ["atmos"],
        "cmip7:source_id": "cnrm_esm2_1e",
        "cmip7:variable_id": "tas",
        "cmip7:variable_long_name": "Near-Surface Air Temperature",
        "cmip7:variable_units": "K",
        "cmip7:variant_label": "r1i1p1f3",
        "cmip7:variable_branding_suffix": "tavg-h2m-hxy-u",
        "cmip7:Conventions": ["CF-1.12"],
        "cmip7:license_id": "CC-BY-4.0",
        "cmip7:mip_era": "CMIP7",
        "cmip7:variable_branded_name": "tas_tavg-h2m-hxy-u",
        "cmip7:temporal_label": "tavg",
        "cmip7:vertical_label": "h2m",
        "cmip7:forcing_index": "f3",
        "cmip7:initialization_index": "i1",
        "cmip7:realization_index": "r1",
        "cmip7:physics_index": "p1",
        "cmip7:pid": "hdl:21.14107/c06c2cde-8659-3947-a6fb-f9a37eed2271",
        "cmip7:parent_activity_id": "CMIP",
        "cmip7:parent_experiment_id": "piControl",
        "cmip7:parent_mip_era": "CMIP7",
        "cmip7:parent_source_id": "DUMMY-MODEL",
        "cmip7:parent_time_units": "days since 1850-01-01",
        "cmip7:parent_variant_label": "r1i1p1f3",
        "cmip7:horizontal_label": "hxy",
    },
    "links": [
        {
            "rel": "self",
            "type": "application/geo+json",
            "href": "https://search-int.east.esgf.io/collections/CMIP7/items/MIP-DRS7.CMIP7.CMIP.MOHC.DUMMY-MODEL.1pctCO2.r1i1p1f3.glb.mon.tas.tavg-h2m-hxy-u.g999.v20270703",
        },
        {
            "rel": "parent",
            "type": "application/json",
            "href": "https://search-int.east.esgf.io/collections/CMIP7",
        },
        {
            "rel": "collection",
            "type": "application/json",
            "href": "https://search-int.east.esgf.io/collections/CMIP7",
        },
        {
            "rel": "root",
            "type": "application/json",
            "href": "https://search-int.east.esgf.io/",
        },
    ],
    "assets": {},
}

ENVIRONMENT_REQUIREMENTS = [
    "CITATION_BASE_URL",
    "CITATION_API_TOKEN",
    "STAC_TRANSACTION_API"
]

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

def raise_missing_env_errors(healthcheck):

    missing = []
    for env in ENVIRONMENT_REQUIREMENTS:
        if not os.environ.get(env):
            missing.append(env)

    if missing:
        if healthcheck:
            probe_fail(healthcheck)
        raise ValueError(f'Missing variables: {", ".join(missing)}')

