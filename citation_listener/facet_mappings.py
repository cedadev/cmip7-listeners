try:
    from django.conf import settings
except Exception as _:
    settings = None

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
    'cordex-cmip6':'CORDEX-CMIP6',
    'cmip6plus': 'CMIP6Plus'
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

BACKUP_REPOS = {}
if settings is not None:
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


GENERAL_INFO = {
    'CMIP7'.lower():"CMIP7 is a project of the [World Climate Research Programme " \
        "(WCRP)](https://www.wcrp-climate.org/), providing climate projections " \
        "to understand past, present, and future climate changes. It is part of the " \
        "[WCRP Earth System Modelling and Observations (ESMO) Core Project](https://" \
        "wcrp-esmo.org/), which was formed to coordinate all modelling, data, and " \
        "observation activities across WCRP and its key partners. Further details can " \
        "be found [here](https://www.wcrp-cmip.org/cmip-overview/) and in the [CMIP7 " \
        "Guidance pages](https://wcrp-cmip.github.io/cmip7-guidance/). Parties using CMIP " \
        "data should ensure they have read the [WCRP CMIP (2026) CMIP Data Disclaimer](" \
        "https://doi.org/10.5281/zenodo.18155119).",
    'CMIP6Plus'.lower():"CMIP Phase 6 Plus (CMIP6Plus) is a continuity activity that enables " \
        "modelling groups to contribute to projects initiated post-CMIP6 but before processes " \
        "for registering CMIP7 community MIPs and experiments are available. CMIP6Plus is supported by the " \
        "WCRP-ESMO Infrastructure Panel alongside other activities and will remain open until " \
        "CMIP7 procedures fully support the establishment of community MIPs and their data " \
        "requirements. Further information can be found [here](https://www.wcrp-cmip.org/cmip-phases/cmip6plus/). " \
        "Parties using CMIP data should ensure they have read the [WCRP CMIP (2026) CMIP Data Disclaimer](" \
        "https://doi.org/10.5281/zenodo.18155119).",
    'CORDEX-CMIP6'.lower(): "[CORDEX](https://cordex.org) is a [World Climate Research Programme (WCRP)]" \
        "(https://www.wcrp-climate.org/) initiative that advances the science and " \
        "application of regional climate downscaling by coordinating experiments " \
        "across fourteen continental domains and fostering related strategic " \
        "activities and partnerships. It provides regional climate projections " \
        "(driven by CMIP projections) to support the understanding of recent and "\
        "future regional climate change. CORDEX contributes to the [Regional " \
        "Information for Society (RIfS)](https://www.wcrp-rifs.org/) Core Project. " \
        "Users of CORDEX data should be familiar with the [CORDEX Terms of Use](" \
        "https://cordex.org/data-access/cordex-cmip6-data/cordex-cmip6-terms-of-use/)." \
        " A list of institutions contributing to CORDEX is available [here](" \
        "https://wcrp-cordex.github.io/cordex-cmip6-cv/CORDEX-CMIP6_institution_id.html)."
}
