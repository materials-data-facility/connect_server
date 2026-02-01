"""Citation export for MDF v2 datasets.

Generates citations in multiple formats:
- BibTeX (for LaTeX papers)
- RIS (for EndNote, Zotero, Mendeley)
- APA (plain text)
- DataCite XML (for DOI registration)
"""

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

from v2.request import parse_authorizer
from v2.responses import bad_request, ok
from v2.store import get_store


def _clean_bibtex_value(value: str) -> str:
    """Escape special characters for BibTeX."""
    if not value:
        return ""
    # Escape special LaTeX characters
    replacements = [
        ("&", r"\&"),
        ("%", r"\%"),
        ("$", r"\$"),
        ("#", r"\#"),
        ("_", r"\_"),
        ("{", r"\{"),
        ("}", r"\}"),
        ("~", r"\textasciitilde{}"),
        ("^", r"\textasciicircum{}"),
    ]
    for old, new in replacements:
        value = value.replace(old, new)
    return value


def _make_bibtex_key(source_id: str, year: str) -> str:
    """Generate a BibTeX citation key."""
    # Clean the source_id for use as a key
    key = re.sub(r"[^a-zA-Z0-9]", "_", source_id)
    return f"{key}_{year}" if year else key


def _format_authors_bibtex(creators: List[Dict]) -> str:
    """Format authors for BibTeX (Last, First and Last, First)."""
    authors = []
    for c in (creators or []):
        if isinstance(c, dict):
            family = c.get("familyName", "")
            given = c.get("givenName", "")
            if family and given:
                authors.append(f"{family}, {given}")
            elif c.get("creatorName"):
                authors.append(c["creatorName"])
        elif isinstance(c, str):
            authors.append(c)
    return " and ".join(authors) if authors else "Unknown"


def _format_authors_ris(creators: List[Dict]) -> List[str]:
    """Format authors for RIS (one AU tag per author)."""
    authors = []
    for c in (creators or []):
        if isinstance(c, dict):
            family = c.get("familyName", "")
            given = c.get("givenName", "")
            if family and given:
                authors.append(f"{family}, {given}")
            elif c.get("creatorName"):
                authors.append(c["creatorName"])
        elif isinstance(c, str):
            authors.append(c)
    return authors if authors else ["Unknown"]


def _format_authors_apa(creators: List[Dict]) -> str:
    """Format authors for APA style."""
    authors = []
    for c in (creators or []):
        if isinstance(c, dict):
            family = c.get("familyName", "")
            given = c.get("givenName", "")
            if family and given:
                # APA: Last, F. M.
                initials = ". ".join([n[0] for n in given.split() if n]) + "."
                authors.append(f"{family}, {initials}")
            elif c.get("creatorName"):
                authors.append(c["creatorName"])
        elif isinstance(c, str):
            authors.append(c)

    if not authors:
        return "Unknown"
    elif len(authors) == 1:
        return authors[0]
    elif len(authors) == 2:
        return f"{authors[0]} & {authors[1]}"
    else:
        return ", ".join(authors[:-1]) + f", & {authors[-1]}"


def generate_bibtex(record: Dict[str, Any]) -> str:
    """Generate BibTeX citation."""
    mdata_str = record.get("dataset_mdata") or "{}"
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else (mdata_str or {})
    except Exception:
        mdata = {}

    dc = mdata.get("dc") or {}
    mdf = mdata.get("mdf") or {}

    # Extract fields
    titles = dc.get("titles") or []
    title = titles[0].get("title") if titles and isinstance(titles[0], dict) else str(titles[0]) if titles else "Untitled"

    year = dc.get("publicationYear") or datetime.now().strftime("%Y")
    publisher = dc.get("publisher") or "Materials Data Facility"
    doi = mdf.get("doi") or ""
    source_id = record.get("source_id", "unknown")
    version = record.get("version", "1.0")

    key = _make_bibtex_key(source_id, year)
    authors = _format_authors_bibtex(dc.get("creators"))

    lines = [
        f"@dataset{{{key},",
        f"  author = {{{_clean_bibtex_value(authors)}}},",
        f"  title = {{{{{_clean_bibtex_value(title)}}}}},",
        f"  year = {{{year}}},",
        f"  publisher = {{{_clean_bibtex_value(publisher)}}},",
        f"  version = {{{version}}},",
    ]

    if doi:
        lines.append(f"  doi = {{{doi}}},")
        lines.append(f"  url = {{https://doi.org/{doi}}},")

    # Add note with MDF source_id
    lines.append(f"  note = {{MDF Source ID: {source_id}}},")
    lines.append("}")

    return "\n".join(lines)


def generate_ris(record: Dict[str, Any]) -> str:
    """Generate RIS citation (for EndNote, Zotero, Mendeley)."""
    mdata_str = record.get("dataset_mdata") or "{}"
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else (mdata_str or {})
    except Exception:
        mdata = {}

    dc = mdata.get("dc") or {}
    mdf = mdata.get("mdf") or {}

    # Extract fields
    titles = dc.get("titles") or []
    title = titles[0].get("title") if titles and isinstance(titles[0], dict) else str(titles[0]) if titles else "Untitled"

    year = dc.get("publicationYear") or datetime.now().strftime("%Y")
    publisher = dc.get("publisher") or "Materials Data Facility"
    doi = mdf.get("doi") or ""
    source_id = record.get("source_id", "unknown")

    # Extract description
    descriptions = dc.get("descriptions") or []
    abstract = ""
    if descriptions:
        if isinstance(descriptions[0], dict):
            abstract = descriptions[0].get("description", "")
        else:
            abstract = str(descriptions[0])

    lines = [
        "TY  - DATA",  # Type: Dataset
        f"TI  - {title}",
    ]

    # Add authors
    for author in _format_authors_ris(dc.get("creators")):
        lines.append(f"AU  - {author}")

    lines.extend([
        f"PY  - {year}",
        f"PB  - {publisher}",
    ])

    if doi:
        lines.append(f"DO  - {doi}")
        lines.append(f"UR  - https://doi.org/{doi}")

    if abstract:
        lines.append(f"AB  - {abstract}")

    # Add keywords
    for subj in (dc.get("subjects") or []):
        if isinstance(subj, dict):
            lines.append(f"KW  - {subj.get('subject', '')}")
        elif isinstance(subj, str):
            lines.append(f"KW  - {subj}")

    lines.append(f"N1  - MDF Source ID: {source_id}")
    lines.append("ER  - ")  # End of record

    return "\n".join(lines)


def generate_apa(record: Dict[str, Any]) -> str:
    """Generate APA style citation (plain text)."""
    mdata_str = record.get("dataset_mdata") or "{}"
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else (mdata_str or {})
    except Exception:
        mdata = {}

    dc = mdata.get("dc") or {}
    mdf = mdata.get("mdf") or {}

    # Extract fields
    titles = dc.get("titles") or []
    title = titles[0].get("title") if titles and isinstance(titles[0], dict) else str(titles[0]) if titles else "Untitled"

    year = dc.get("publicationYear") or datetime.now().strftime("%Y")
    publisher = dc.get("publisher") or "Materials Data Facility"
    doi = mdf.get("doi")
    version = record.get("version", "1.0")

    authors = _format_authors_apa(dc.get("creators"))

    # APA format for datasets:
    # Author, A. A., & Author, B. B. (Year). Title of dataset (Version X.X) [Data set]. Publisher. https://doi.org/xxxxx
    citation = f"{authors} ({year}). {title} (Version {version}) [Data set]. {publisher}."

    if doi:
        citation += f" https://doi.org/{doi}"

    return citation


def generate_datacite_xml(record: Dict[str, Any]) -> str:
    """Generate DataCite XML for DOI registration."""
    mdata_str = record.get("dataset_mdata") or "{}"
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else (mdata_str or {})
    except Exception:
        mdata = {}

    dc = mdata.get("dc") or {}
    mdf = mdata.get("mdf") or {}

    # Create root element
    root = ET.Element("resource")
    root.set("xmlns", "http://datacite.org/schema/kernel-4")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("xsi:schemaLocation", "http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4/metadata.xsd")

    # Identifier (DOI or placeholder)
    identifier = ET.SubElement(root, "identifier")
    identifier.set("identifierType", "DOI")
    identifier.text = mdf.get("doi") or "10.xxxxx/pending"

    # Creators
    creators_elem = ET.SubElement(root, "creators")
    for c in (dc.get("creators") or [{"creatorName": "Unknown"}]):
        creator = ET.SubElement(creators_elem, "creator")
        if isinstance(c, dict):
            name = ET.SubElement(creator, "creatorName")
            name.text = c.get("creatorName") or f"{c.get('familyName', '')}, {c.get('givenName', '')}".strip(", ")
            if c.get("givenName"):
                given = ET.SubElement(creator, "givenName")
                given.text = c["givenName"]
            if c.get("familyName"):
                family = ET.SubElement(creator, "familyName")
                family.text = c["familyName"]
            if c.get("affiliation"):
                affil = ET.SubElement(creator, "affiliation")
                affil.text = c["affiliation"]
        else:
            name = ET.SubElement(creator, "creatorName")
            name.text = str(c)

    # Titles
    titles_elem = ET.SubElement(root, "titles")
    for t in (dc.get("titles") or [{"title": "Untitled"}]):
        title = ET.SubElement(titles_elem, "title")
        title.text = t.get("title") if isinstance(t, dict) else str(t)

    # Publisher
    publisher = ET.SubElement(root, "publisher")
    publisher.text = dc.get("publisher") or "Materials Data Facility"

    # Publication Year
    pub_year = ET.SubElement(root, "publicationYear")
    pub_year.text = str(dc.get("publicationYear") or datetime.now().year)

    # Resource Type
    resource_type = ET.SubElement(root, "resourceType")
    resource_type.set("resourceTypeGeneral", "Dataset")
    resource_type.text = "Dataset"

    # Descriptions
    if dc.get("descriptions"):
        descriptions_elem = ET.SubElement(root, "descriptions")
        for d in dc["descriptions"]:
            desc = ET.SubElement(descriptions_elem, "description")
            desc.set("descriptionType", d.get("descriptionType", "Abstract") if isinstance(d, dict) else "Abstract")
            desc.text = d.get("description") if isinstance(d, dict) else str(d)

    # Subjects
    if dc.get("subjects"):
        subjects_elem = ET.SubElement(root, "subjects")
        for s in dc["subjects"]:
            subj = ET.SubElement(subjects_elem, "subject")
            subj.text = s.get("subject") if isinstance(s, dict) else str(s)

    # Version
    version = ET.SubElement(root, "version")
    version.text = str(record.get("version", "1.0"))

    # Format XML with indentation
    ET.indent(root)
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def lambda_handler(event, context):
    """Get citation for a dataset.

    GET /citation/{source_id}?format=bibtex&version=1.0

    Supported formats: bibtex, ris, apa, datacite, all
    """
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    source_id = path_params.get("source_id")
    version = query_params.get("version")
    fmt = query_params.get("format", "all").lower()

    if not source_id:
        return bad_request("source_id is required")

    store = get_store()
    record = store.get(source_id, version=version)

    if not record:
        return bad_request(f"Dataset not found: {source_id}")

    # Generate requested format(s)
    result = {
        "success": True,
        "source_id": source_id,
        "version": record.get("version"),
    }

    if fmt == "bibtex":
        result["bibtex"] = generate_bibtex(record)
        result["content_type"] = "application/x-bibtex"
    elif fmt == "ris":
        result["ris"] = generate_ris(record)
        result["content_type"] = "application/x-research-info-systems"
    elif fmt == "apa":
        result["apa"] = generate_apa(record)
        result["content_type"] = "text/plain"
    elif fmt == "datacite":
        result["datacite"] = generate_datacite_xml(record)
        result["content_type"] = "application/xml"
    else:  # all
        result["bibtex"] = generate_bibtex(record)
        result["ris"] = generate_ris(record)
        result["apa"] = generate_apa(record)
        result["datacite"] = generate_datacite_xml(record)

    return ok(result)
