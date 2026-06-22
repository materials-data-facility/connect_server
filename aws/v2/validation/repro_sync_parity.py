#!/usr/bin/env python3
"""Throwaway repro for SYNC-02 / INFRA-06 / migrate_v1_payload loss findings.

Read-only against the app. Run with:
  cd /Users/blaiszik/Desktop/git/mdf_client/cs/aws
  /tmp/mdfv312/bin/python v2/validation/repro_sync_parity.py
"""
import json
import os

# Mock search so build_gmeta_entry's parse_metadata path doesn't need creds.
os.environ.setdefault("USE_MOCK_SEARCH", "true")

from v2.metadata import migrate_v1_payload, parse_metadata  # noqa: E402
from v2.search_client import GlobusSearchClient  # noqa: E402


def hr(t):
    print("\n" + "=" * 70)
    print(t)
    print("=" * 70)


# ---------------------------------------------------------------------------
# INFRA-06: source_name = source_id.rsplit('-',1)[0] for v2 ids 'mdf-<uuid>'
# ---------------------------------------------------------------------------
hr("INFRA-06: build_gmeta_entry source_name derivation")
client = GlobusSearchClient.__new__(GlobusSearchClient)  # no creds needed for build

examples = [
    "mdf-9f8a7b6c-1234-4def-9012-abcdef012345",  # v2 native uuid id
    "oqmd_v13-1",                                  # v1-style collision suffix
    "ab_initio_solidification_v1",                # v1 with no dash suffix
    "mdf-collision-2",                            # hypothetical collision suffix
]
for sid in examples:
    derived = sid.rsplit("-", 1)[0] if "-" in sid else sid
    print(f"  source_id={sid!r:55} -> mdf.source_name={derived!r}")

# Show full mdf block of a v2-native submission
sub_v2 = {
    "source_id": "mdf-9f8a7b6c-1234-4def-9012-abcdef012345",
    "version": "1.0",
    "organization": "MDF Open",
    "created_at": "2026-01-02T00:00:00+00:00",
    "dataset_mdata": json.dumps({
        "title": "A v2-native dataset",
        "authors": [{"name": "Ada Lovelace", "orcid": "0000-0002-1825-0097"}],
        "keywords": ["alloys", "dft"],
        "publication_year": 2026,
        "domains": ["materials science"],
    }),
}
entry_v2 = client.build_gmeta_entry(sub_v2)
print("\n  v2-native gmeta mdf block:")
print("   ", json.dumps(entry_v2["content"]["mdf"], indent=2).replace("\n", "\n    "))

# ---------------------------------------------------------------------------
# SYNC-02: does build_gmeta_entry emit mdf.resource_type?
# ---------------------------------------------------------------------------
hr("SYNC-02: mdf.resource_type presence in v2 gmeta content")
mdf_keys = list(entry_v2["content"]["mdf"].keys())
print("  mdf.* keys in v2 gmeta entry:", mdf_keys)
print("  'resource_type' present in mdf block? ", "resource_type" in entry_v2["content"]["mdf"])
print("  v1 extraction query (extract_mdf_production_datasets.py:73): mdf.resource_type:\"dataset\"")
print("  => v1 re-enumeration of the v2 index returns this many v2-native datasets: 0")

# ---------------------------------------------------------------------------
# migrate_v1_payload field-mapping + loss
# ---------------------------------------------------------------------------
hr("migrate_v1_payload: realistic v1 doc -> v2 mapping + loss")

v1_doc = {
    "dc": {
        "titles": [{"title": "High-throughput DFT of Mg alloys"}],
        "creators": [
            {
                "creatorName": "Curie, Marie",
                "givenName": "Marie",
                "familyName": "Curie",
                "affiliations": ["Sorbonne"],
                "nameIdentifiers": [
                    {"nameIdentifier": "https://orcid.org/0000-0002-1825-0097",
                     "nameIdentifierScheme": "ORCID"}
                ],
            },
            {"creatorName": "Bohr, Niels", "affiliations": ["Copenhagen"]},
        ],
        "subjects": [{"subject": "alloys"}, {"subject": "DFT"}],
        "descriptions": [{"description": "An abstract.", "descriptionType": "Abstract"},
                         {"description": "A second TechInfo desc.", "descriptionType": "TechnicalInfo"}],
        "publisher": "Materials Data Facility",
        "publicationYear": "2021",
        "resourceType": {"resourceTypeGeneral": "Dataset", "resourceType": "Dataset"},
        "rightsList": [{"rights": "CC-BY-4.0", "rightsURI": "https://creativecommons.org/licenses/by/4.0/",
                        "rightsIdentifier": "cc-by-4.0"}],
        "relatedIdentifiers": [
            {"relatedIdentifier": "10.1/xyz", "relatedIdentifierType": "DOI", "relationType": "IsCitedBy"},
        ],
        "fundingReferences": [
            {"funderName": "NSF", "awardNumber": "DMR-123456", "awardTitle": "Alloy ML"},
        ],
        "geoLocations": [{"geoLocationPlace": "Argonne, IL"}],
        "dates": [{"date": "2020-06-01", "dateType": "Collected"}],
        "formats": ["text/csv", "application/hdf5"],
        "language": "en",
        "identifier": {"identifier": "10.18126/abc123", "identifierType": "DOI"},
    },
    "mdf": {
        "source_id": "mg_alloys_dft_v3",
        "source_name": "mg_alloys_dft",
        "version": 3,
        "resource_type": "dataset",
        "organizations": ["MDF Open", "Argonne"],
        "instruments": ["VASP"],
        "facility": "ALCF",
        "acl": ["public"],
        "ingest_date": "2021-03-04T00:00:00Z",
        "scroll_id": 12345,
        "mdf_id": "abcdef",
    },
    "custom": {"sample_prep": "arc-melted", "raw_temperature_K": 1200},
    "projects": {
        "foundry": {
            "data_type": "tabular", "short_name": "mg_alloys", "n_items": 5000,
            "keys": [{"key": ["formation_energy"], "type": "output", "units": "eV"}],
            "splits": [{"type": "train", "path": "train.csv", "n_items": 4000}],
        },
        "other_project": {"foo": "bar"},
    },
}

v2 = migrate_v1_payload(v1_doc)
print("  migrate_v1_payload result keys:", sorted(v2.keys()))
print("\n  Full v2 dict:")
print("   ", json.dumps(v2, indent=2, default=str).replace("\n", "\n    "))

# What v1 keys never appear anywhere in the v2 result (dropped/collapsed)?
hr("Lossy fields: present in v1 doc, NOT carried into migrate_v1_payload output")
v2_json = json.dumps(v2, default=str)
checks = {
    "dc.descriptions[1] (TechnicalInfo, 2nd desc)": "second TechInfo" in v2_json,
    "dc.fundingReferences (NSF/DMR-123456)": "NSF" in v2_json or "DMR-123456" in v2_json,
    "dc.geoLocations (Argonne, IL)": "Argonne, IL" in v2_json,
    "dc.dates (Collected 2020-06-01)": "2020-06-01" in v2_json,
    "dc.formats (text/csv, hdf5)": "text/csv" in v2_json,
    "dc.language (en) [non-default carry]": '"language"' in v2_json,
    "dc.identifier / DOI (10.18126/abc123)": "10.18126/abc123" in v2_json,
    "mdf.organizations[1] (Argonne, 2nd org)": '"Argonne"' in v2_json and v2.get("organization") == "Argonne",
    "mdf.version (=3)": '"version"' in v2_json,
    "mdf.resource_type (=dataset)": "dataset" in v2_json.lower() and v2.get("resource_type", "").lower() == "dataset",
    "mdf.ingest_date": "2021-03-04" in v2_json,
    "mdf.scroll_id / mdf_id (internal)": "12345" in v2_json or "abcdef" in v2_json,
}
for label, present in checks.items():
    status = "carried" if present else "DROPPED"
    print(f"  [{status:8}] {label}")

# ---------------------------------------------------------------------------
# build_gmeta_entry on the migrated v2 record: what does the SEARCH doc lose?
# ---------------------------------------------------------------------------
hr("build_gmeta_entry(migrated): author detail collapse + fields absent in search doc")
sub = {
    "source_id": "mg_alloys_dft",
    "version": "3.0",
    "organization": "MDF Open",
    "created_at": "2021-03-04T00:00:00Z",
    "dataset_mdata": json.dumps(v2),
}
gmeta = client.build_gmeta_entry(sub)
print("  gmeta content.dc.creators (note: only {name}):")
print("   ", json.dumps(gmeta["content"]["dc"]["creators"], indent=2).replace("\n", "\n    "))
print("\n  Parsed meta authors[0] BEFORE collapse (full Author):")
meta = parse_metadata(sub)
print("   ", meta.authors[0].model_dump())
print("\n  Top-level content keys in search doc:", list(gmeta["content"].keys()))
print("  content.dc keys in search doc:", list(gmeta["content"]["dc"].keys()))
print("  -> NOT in search doc: funding, related_works, methods, facility, geo_locations,")
print("     formats, language, ml, fields_of_science, affiliations, orcid (only creators[].name kept)")
print("\n  resource_type in search-doc mdf block? ",
      "resource_type" in gmeta["content"]["mdf"])
