from django.conf import settings

CORDEX_FACETS = {
    "project_id": "project_id",
    "driving_experiment_id": "experiment_id", # Yes this one apparently needs the ID added (17/06/2026)
    "domain_id": "domain_id",
    "activity_id": "activity_id",
    "source_id": "source_id",
    "institution_id": "institution_id",
}

CMIP_FACETS = {
    "project_id": "project_id",
    "experiment": "experiment_id",
    "activity": "activity_id",
    "source": "source_id",
    "institution": "institution_id",
}

CMIP_FACETS_OLD = {
    "project_id": "project_id",
    "experiment_id": "experiment_id",
    "activity_id": "activity_id",
    "source_id": "source_id",
    "institution_id": "institution_id",
}

CORDEX_TITLE_ORDER = [
    "project_id",
    "activity_id",
    "domain_id",
    "institution_id",
    "experiment_id",
    "source_id",
]

CMIP_TITLE_ORDER = [
    "project_id",
    "activity_id",
    "institution_id",
    "source_id",
    "experiment_id",
]

STAC_LABELS = {"driving_experiment_id": "driving_experiment_id"}

STAC_COLLECTIONS = {
    'cmip7':'CMIP7',
    'cmip6':'CMIP6',
    'cordex-cmip6':'CORDEX-CMIP6'
}

# Mapping internal database facets to the User-facing view pages.
UI_FACET_LABELS = {
    "cmip7": CMIP_FACETS,
    "cordex-cmip6": CORDEX_FACETS,
    "cmip6plus": CMIP_FACETS,
}

# Mapping internal database facets to those used in the esgvoc package
ESGVOC_FACET_LABELS = {
    "cmip7": CMIP_FACETS,
    "cordex-cmip6": CORDEX_FACETS,
    "cmip6plus": CMIP_FACETS_OLD,
}

ESGVOC_TITLE_LABELS = {
    "cmip7": CMIP_TITLE_ORDER,
    "cordex-cmip6": CORDEX_TITLE_ORDER,
    "cmip6plus": CMIP_TITLE_ORDER,
}

BACKUP_REPOS = {
    "cmip7": getattr(settings, "CV_REPO", None),
    "cordex-cmip6": getattr(settings, "CORDEX_CV_REPO", None),
}

# All labels to use based on labels applied to the facets in different project IDs
FACET_ABSTRACT_DESCRIPTIONS = {
    "project_id": "Project: ",
    "activity": "",
    "activity_id": "",
    "domain": "CORDEX Domain: ",
    "domain_id": "CORDEX Domain: ",
    "institution": [
        "Produced by: ",
        " (Using Model/Source: ",
        " with Experiment ",
        ")",
    ],
    "institution_id": [
        "Produced by: ",
        " (Using Model/Source: ",
        " with Experiment ",
        ")",
    ],
    "driving_experiment": "Driving Experiment: ",
    "source": "",
    "source_id": "",
    "experiment": "Experiment: ",
    "experiment_id": "Experiment: ",
}
