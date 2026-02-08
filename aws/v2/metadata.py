"""Canonical metadata schema for MDF v2.

Defines a flat, validated Pydantic model with researcher-friendly field names.
Translates to DataCite format only when minting DOIs.

Key components:
- DatasetMetadata: the top-level schema (replaces dc/mdf/custom triple nesting)
- MLMetadata: first-class ML-readiness metadata (replaces projects.foundry)
- to_datacite(): DataCite kernel-4 payload builder
- migrate_v1_payload(): convert old dc/mdf/custom format to flat format
- parse_metadata(): parse a DB record into DatasetMetadata (handles both schemas)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class Author(BaseModel):
    name: str
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    orcid: Optional[str] = None
    affiliations: List[str] = Field(default_factory=list)


class FundingReference(BaseModel):
    funder_name: str
    award_number: Optional[str] = None
    award_title: Optional[str] = None
    funder_id: Optional[str] = None
    funder_id_type: Optional[str] = None


class RelatedWork(BaseModel):
    identifier: str
    identifier_type: str = "DOI"
    relation_type: str = "References"
    description: Optional[str] = None


class GeoLocation(BaseModel):
    place: Optional[str] = None
    point: Optional[Dict[str, Any]] = None
    box: Optional[Dict[str, Any]] = None


class License(BaseModel):
    name: str
    url: Optional[str] = None
    identifier: Optional[str] = None


# ---------------------------------------------------------------------------
# ML-ready metadata (replaces projects.foundry)
# ---------------------------------------------------------------------------

class DataKey(BaseModel):
    name: str
    role: str = "input"
    description: Optional[str] = None
    units: Optional[str] = None
    dtype: Optional[str] = None
    shape: Optional[List[int]] = None
    classes: Optional[List[str]] = None


class DataSplit(BaseModel):
    type: str
    path: str
    label: Optional[str] = None
    n_items: Optional[int] = None


class MLMetadata(BaseModel):
    data_format: str
    task_type: List[str] = Field(default_factory=list)
    domain: List[str] = Field(default_factory=list)
    n_items: Optional[int] = None
    splits: List[DataSplit] = Field(default_factory=list)
    keys: List[DataKey] = Field(default_factory=list)
    short_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Top-level dataset metadata
# ---------------------------------------------------------------------------

class DatasetMetadata(BaseModel):
    # Required (DataCite mandatory)
    title: str
    authors: List[Author]

    # Recommended
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    publisher: str = "Materials Data Facility"
    publication_year: Optional[int] = None
    resource_type: str = "Dataset"

    # Attribution & Provenance
    license: Optional[License] = None
    funding: List[FundingReference] = Field(default_factory=list)
    related_works: List[RelatedWork] = Field(default_factory=list)

    # Scientific Context
    methods: List[str] = Field(default_factory=list)
    facility: Optional[str] = None
    fields_of_science: List[str] = Field(default_factory=list)
    domains: List[str] = Field(default_factory=list)

    # ML-Ready Data Structure
    ml: Optional[MLMetadata] = None

    # Geospatial
    geo_locations: List[GeoLocation] = Field(default_factory=list)

    # Data Description
    data_sources: List[str] = Field(default_factory=list)
    data_type: Optional[str] = None
    formats: List[str] = Field(default_factory=list)
    language: str = "en"

    # External Import Provenance
    external_doi: Optional[str] = None
    external_url: Optional[str] = None
    external_source: Optional[str] = None

    # MDF Platform
    organization: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    acl: List[str] = Field(default_factory=list)
    extensions: Dict[str, Any] = Field(default_factory=dict)

    # Submission flags (not stored in metadata proper)
    test: bool = False
    update: bool = False


# ---------------------------------------------------------------------------
# DataCite translation
# ---------------------------------------------------------------------------

def _parse_author_name(author: Author) -> dict:
    """Build a DataCite creator dict from an Author."""
    creator: Dict[str, Any] = {}

    given = author.given_name or ""
    family = author.family_name or ""

    # Auto-parse if given/family not provided
    if not given and not family and author.name:
        if "," in author.name:
            parts = author.name.split(",", 1)
            family = parts[0].strip()
            given = parts[1].strip()
        else:
            parts = author.name.rsplit(" ", 1)
            if len(parts) == 2:
                given = parts[0].strip()
                family = parts[1].strip()
            else:
                family = author.name

    if family and given:
        creator["name"] = f"{family}, {given}"
    else:
        creator["name"] = author.name

    if given:
        creator["givenName"] = given
    if family:
        creator["familyName"] = family

    if author.affiliations:
        creator["affiliation"] = [{"name": a} for a in author.affiliations]

    if author.orcid:
        creator["nameIdentifiers"] = [{
            "nameIdentifier": f"https://orcid.org/{author.orcid}",
            "nameIdentifierScheme": "ORCID",
            "schemeUri": "https://orcid.org",
        }]

    return creator


def to_datacite(
    meta: DatasetMetadata,
    doi: str,
    url: str,
    source_id: Optional[str] = None,
    created_at: Optional[str] = None,
    published_at: Optional[str] = None,
) -> dict:
    """Translate DatasetMetadata to a DataCite API payload (kernel 4.5).

    Returns the full payload ready for POST to DataCite /dois endpoint.
    """
    pub_year = meta.publication_year or datetime.now().year

    creators = [_parse_author_name(a) for a in meta.authors]
    if not creators:
        creators = [{"name": "Materials Data Facility"}]

    attributes: Dict[str, Any] = {
        "doi": doi,
        "url": url,
        "titles": [{"title": meta.title}],
        "creators": creators,
        "publisher": meta.publisher,
        "publicationYear": int(pub_year),
        "types": {"resourceTypeGeneral": meta.resource_type or "Dataset"},
        "schemaVersion": "http://datacite.org/schema/kernel-4",
    }

    # Description
    if meta.description:
        attributes["descriptions"] = [{
            "description": meta.description,
            "descriptionType": "Abstract",
        }]

    # Subjects (keywords + fields_of_science)
    subjects = []
    for kw in meta.keywords:
        subjects.append({"subject": kw})
    for fos in meta.fields_of_science:
        subjects.append({"subject": fos, "subjectScheme": "Fields of Science"})
    if subjects:
        attributes["subjects"] = subjects

    # Language
    if meta.language:
        attributes["language"] = meta.language

    # Formats
    if meta.formats:
        attributes["formats"] = meta.formats

    # Rights / License
    if meta.license:
        rights_entry: Dict[str, str] = {"rights": meta.license.name}
        if meta.license.url:
            rights_entry["rightsUri"] = meta.license.url
        if meta.license.identifier:
            rights_entry["rightsIdentifier"] = meta.license.identifier
        attributes["rightsList"] = [rights_entry]

    # Funding references
    if meta.funding:
        funding_refs = []
        for f in meta.funding:
            ref: Dict[str, Any] = {"funderName": f.funder_name}
            if f.award_number:
                ref["awardNumber"] = f.award_number
            if f.award_title:
                ref["awardTitle"] = f.award_title
            if f.funder_id:
                ref["funderIdentifier"] = f.funder_id
                ref["funderIdentifierType"] = f.funder_id_type or "Crossref Funder ID"
            funding_refs.append(ref)
        attributes["fundingReferences"] = funding_refs

    # Related identifiers
    if meta.related_works:
        related = []
        for rw in meta.related_works:
            related.append({
                "relatedIdentifier": rw.identifier,
                "relatedIdentifierType": rw.identifier_type,
                "relationType": rw.relation_type,
            })
        attributes["relatedIdentifiers"] = related

    # GeoLocations
    if meta.geo_locations:
        geo_locs = []
        for gl in meta.geo_locations:
            loc: Dict[str, Any] = {}
            if gl.place:
                loc["geoLocationPlace"] = gl.place
            if gl.point:
                loc["geoLocationPoint"] = {
                    "pointLatitude": gl.point.get("latitude"),
                    "pointLongitude": gl.point.get("longitude"),
                }
            if gl.box:
                loc["geoLocationBox"] = gl.box
            geo_locs.append(loc)
        attributes["geoLocations"] = geo_locs

    # Alternate identifiers (source_id)
    if source_id:
        attributes["alternateIdentifiers"] = [{
            "alternateIdentifier": source_id,
            "alternateIdentifierType": "MDF Source ID",
        }]

    # Dates
    dates = []
    if created_at:
        dates.append({"date": created_at[:10], "dateType": "Created"})
    if published_at:
        dates.append({"date": published_at[:10], "dateType": "Available"})
    if dates:
        attributes["dates"] = dates

    return {
        "data": {
            "type": "dois",
            "attributes": attributes,
        }
    }


# ---------------------------------------------------------------------------
# v1 migration (dc/mdf/custom/projects.foundry -> flat format)
# ---------------------------------------------------------------------------

def migrate_v1_payload(old: dict) -> dict:
    """Convert a v1 dc/mdf/custom payload to the new flat format.

    Handles:
    - dc.titles -> title
    - dc.creators -> authors (with name parsing)
    - dc.descriptions -> description
    - dc.subjects -> keywords
    - dc.publisher -> publisher
    - dc.publicationYear -> publication_year
    - dc.resourceType -> resource_type
    - dc.relatedIdentifiers -> related_works
    - mdf.organization -> organization
    - mdf.instruments -> methods
    - mdf.facility -> facility
    - mdf.acl -> acl
    - mdf.doi -> (stored separately on record)
    - projects.foundry -> ml
    - custom -> extensions
    - tags (subjects) -> keywords or tags
    """
    dc = old.get("dc") or {}
    mdf = old.get("mdf") or {}
    custom = old.get("custom") or {}
    projects = old.get("projects") or {}

    result: Dict[str, Any] = {}

    # Title
    titles = dc.get("titles") or []
    if titles:
        first = titles[0]
        result["title"] = first.get("title") if isinstance(first, dict) else str(first)
    else:
        result["title"] = "Untitled"

    # Authors
    authors = []
    for c in dc.get("creators") or []:
        if isinstance(c, dict):
            author: Dict[str, Any] = {}
            author["name"] = c.get("creatorName") or f"{c.get('givenName', '')} {c.get('familyName', '')}".strip()
            if c.get("givenName"):
                author["given_name"] = c["givenName"]
            if c.get("familyName"):
                author["family_name"] = c["familyName"]
            if c.get("affiliation"):
                aff = c["affiliation"]
                author["affiliations"] = [aff] if isinstance(aff, str) else aff
            if c.get("affiliations"):
                author["affiliations"] = c["affiliations"]
            # ORCID from nameIdentifiers
            for ni in c.get("nameIdentifiers") or []:
                if isinstance(ni, dict) and ni.get("nameIdentifierScheme") == "ORCID":
                    orcid = ni.get("nameIdentifier", "")
                    # Strip URL prefix if present
                    orcid = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "")
                    author["orcid"] = orcid
            authors.append(author)
        elif isinstance(c, str):
            authors.append({"name": c})
    result["authors"] = authors if authors else [{"name": "Unknown"}]

    # Description
    descriptions = dc.get("descriptions") or []
    if descriptions:
        first_desc = descriptions[0]
        result["description"] = first_desc.get("description") if isinstance(first_desc, dict) else str(first_desc)

    # Keywords (from dc.subjects)
    keywords = []
    for subj in dc.get("subjects") or []:
        if isinstance(subj, dict):
            keywords.append(subj.get("subject", ""))
        elif isinstance(subj, str):
            keywords.append(subj)
    keywords = [k for k in keywords if k]
    if keywords:
        result["keywords"] = keywords

    # Publisher
    result["publisher"] = dc.get("publisher") or "Materials Data Facility"

    # Publication year
    pub_year = dc.get("publicationYear")
    if pub_year:
        try:
            result["publication_year"] = int(pub_year)
        except (ValueError, TypeError):
            pass

    # Resource type
    rt = dc.get("resourceType")
    if isinstance(rt, dict):
        result["resource_type"] = rt.get("resourceType") or rt.get("resourceTypeGeneral") or "Dataset"
    elif isinstance(rt, str):
        result["resource_type"] = rt

    # Related identifiers -> related_works
    related = dc.get("relatedIdentifiers") or []
    if related:
        works = []
        for ri in related:
            if isinstance(ri, dict):
                works.append({
                    "identifier": ri.get("relatedIdentifier", ""),
                    "identifier_type": ri.get("relatedIdentifierType", "DOI"),
                    "relation_type": ri.get("relationType", "References"),
                })
        if works:
            result["related_works"] = works

    # Rights -> license
    rights = dc.get("rights") or dc.get("rightsList") or []
    if rights and isinstance(rights, list) and rights:
        r = rights[0]
        if isinstance(r, dict):
            result["license"] = {
                "name": r.get("rights", ""),
                "url": r.get("rightsURI") or r.get("rightsUri"),
            }

    # MDF block
    if mdf.get("organization"):
        result["organization"] = mdf["organization"]
    if mdf.get("instruments"):
        instr = mdf["instruments"]
        result["methods"] = instr if isinstance(instr, list) else [str(instr)]
    if mdf.get("facility"):
        result["facility"] = mdf["facility"]
    if mdf.get("acl"):
        result["acl"] = mdf["acl"]
    if mdf.get("source_id"):
        result.setdefault("extensions", {})["mdf_source_id"] = mdf["source_id"]
    if mdf.get("source_name"):
        result.setdefault("extensions", {})["mdf_source_name"] = mdf["source_name"]

    # Data sources
    if old.get("data_sources"):
        result["data_sources"] = old["data_sources"]

    # Tags
    if old.get("tags"):
        result["tags"] = old["tags"]

    # Test / Update flags
    if old.get("test"):
        result["test"] = old["test"]
    if old.get("update"):
        result["update"] = old["update"]

    # Custom -> extensions
    if custom:
        result.setdefault("extensions", {}).update(custom)

    # Projects -> extensions (except foundry which becomes ml)
    foundry = projects.get("foundry")
    if foundry:
        result["ml"] = _migrate_foundry(foundry)

    other_projects = {k: v for k, v in projects.items() if k != "foundry"}
    if other_projects:
        result.setdefault("extensions", {}).update(other_projects)

    return result


def _migrate_foundry(foundry: dict) -> dict:
    """Convert projects.foundry schema to MLMetadata dict."""
    ml: Dict[str, Any] = {}

    ml["data_format"] = foundry.get("data_type") or foundry.get("data_format") or "unknown"

    if foundry.get("short_name"):
        ml["short_name"] = foundry["short_name"]

    if foundry.get("n_items"):
        ml["n_items"] = foundry["n_items"]

    # Splits
    splits = []
    for s in foundry.get("splits") or []:
        if isinstance(s, dict):
            splits.append({
                "type": s.get("type", ""),
                "path": s.get("path", ""),
                "label": s.get("label"),
                "n_items": s.get("n_items"),
            })
    if splits:
        ml["splits"] = splits

    # Keys
    keys = []
    for k in foundry.get("keys") or foundry.get("key") or []:
        if isinstance(k, dict):
            key_entry: Dict[str, Any] = {}
            # Foundry uses key[].key (list of strings) or just a string
            key_names = k.get("key") or k.get("name")
            if isinstance(key_names, list):
                # Foundry style: key: ["col1", "col2"] with a shared type
                for kn in key_names:
                    keys.append({
                        "name": kn,
                        "role": k.get("type", "input"),
                        "units": k.get("units"),
                        "description": k.get("description"),
                    })
                continue
            else:
                key_entry["name"] = str(key_names) if key_names else ""

            key_entry["role"] = k.get("type") or k.get("role") or "input"
            if k.get("units"):
                key_entry["units"] = k["units"]
            if k.get("description"):
                key_entry["description"] = k["description"]
            if k.get("classes"):
                key_entry["classes"] = k["classes"]
            keys.append(key_entry)

    if keys:
        ml["keys"] = keys

    return ml


# ---------------------------------------------------------------------------
# Metadata parsing from DB records
# ---------------------------------------------------------------------------

def _is_v1_format(mdata: dict) -> bool:
    """Check if metadata is in the old dc/mdf/custom v1 format."""
    dc = mdata.get("dc")
    if isinstance(dc, dict) and ("titles" in dc or "creators" in dc):
        return True
    return False


def parse_metadata(record: dict) -> DatasetMetadata:
    """Parse a submission record into DatasetMetadata.

    Handles both v1 (dc/mdf/custom stored in dataset_mdata) and v2 (flat)
    stored formats. Accepts either a full DB record (with dataset_mdata key)
    or a raw metadata dict.
    """
    import json as _json

    # Extract the metadata dict from the record
    mdata = record.get("dataset_mdata") or record
    if isinstance(mdata, str):
        try:
            mdata = _json.loads(mdata)
        except Exception:
            mdata = {}
    if not isinstance(mdata, dict):
        mdata = {}

    # If it's v1 format, migrate first
    if _is_v1_format(mdata):
        mdata = migrate_v1_payload(mdata)

    # If it's already a flat format (has "title" at top level), use directly
    if "title" not in mdata:
        # Fallback: might be a record without dataset_mdata
        mdata.setdefault("title", record.get("title", "Untitled"))
        if "authors" not in mdata:
            mdata["authors"] = [{"name": "Unknown"}]

    # Ensure authors is well-formed
    authors = mdata.get("authors", [])
    if not authors:
        mdata["authors"] = [{"name": "Unknown"}]

    return DatasetMetadata.model_validate(mdata)
