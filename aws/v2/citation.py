"""Citation export for MDF v2 datasets.

Generates citations in multiple formats:
- BibTeX (for LaTeX papers)
- RIS (for EndNote, Zotero, Mendeley)
- APA (plain text)
- DataCite XML (for DOI registration)
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

from v2.metadata import DatasetMetadata, Author, parse_metadata
from v2.store import get_store


def _clean_bibtex_value(value: str) -> str:
    """Escape special characters for BibTeX."""
    if not value:
        return ""
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
    key = re.sub(r"[^a-zA-Z0-9]", "_", source_id)
    return f"{key}_{year}" if year else key


def _author_family_given(author: Author) -> tuple:
    """Extract (family, given) from an Author, auto-parsing if needed."""
    family = author.family_name or ""
    given = author.given_name or ""
    if not family and not given and author.name:
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
    return family, given


def _format_authors_bibtex(authors: List[Author]) -> str:
    """Format authors for BibTeX (Last, First and Last, First)."""
    names = []
    for a in authors:
        family, given = _author_family_given(a)
        if family and given:
            names.append(f"{family}, {given}")
        else:
            names.append(a.name)
    return " and ".join(names) if names else "Unknown"


def _format_authors_ris(authors: List[Author]) -> List[str]:
    """Format authors for RIS (one AU tag per author)."""
    names = []
    for a in authors:
        family, given = _author_family_given(a)
        if family and given:
            names.append(f"{family}, {given}")
        else:
            names.append(a.name)
    return names if names else ["Unknown"]


def _format_authors_apa(authors: List[Author]) -> str:
    """Format authors for APA style."""
    formatted = []
    for a in authors:
        family, given = _author_family_given(a)
        if family and given:
            initials = ". ".join([n[0] for n in given.split() if n]) + "."
            formatted.append(f"{family}, {initials}")
        else:
            formatted.append(a.name)

    if not formatted:
        return "Unknown"
    elif len(formatted) == 1:
        return formatted[0]
    elif len(formatted) == 2:
        return f"{formatted[0]} & {formatted[1]}"
    else:
        return ", ".join(formatted[:-1]) + f", & {formatted[-1]}"


def generate_bibtex(record: Dict[str, Any]) -> str:
    """Generate BibTeX citation."""
    meta = parse_metadata(record)

    year = str(meta.publication_year or datetime.now().year)
    source_id = record.get("source_id", "unknown")
    version = record.get("version", "1.0")
    doi = record.get("doi") or ""

    key = _make_bibtex_key(source_id, year)
    authors = _format_authors_bibtex(meta.authors)

    lines = [
        f"@dataset{{{key},",
        f"  author = {{{_clean_bibtex_value(authors)}}},",
        f"  title = {{{{{_clean_bibtex_value(meta.title)}}}}},",
        f"  year = {{{year}}},",
        f"  publisher = {{{_clean_bibtex_value(meta.publisher)}}},",
        f"  version = {{{version}}},",
    ]

    if doi:
        lines.append(f"  doi = {{{doi}}},")
        lines.append(f"  url = {{https://doi.org/{doi}}},")

    lines.append(f"  note = {{MDF Source ID: {source_id}}},")
    lines.append("}")

    return "\n".join(lines)


def generate_ris(record: Dict[str, Any]) -> str:
    """Generate RIS citation (for EndNote, Zotero, Mendeley)."""
    meta = parse_metadata(record)

    year = str(meta.publication_year or datetime.now().year)
    source_id = record.get("source_id", "unknown")
    doi = record.get("doi") or ""

    lines = [
        "TY  - DATA",
        f"TI  - {meta.title}",
    ]

    for author in _format_authors_ris(meta.authors):
        lines.append(f"AU  - {author}")

    lines.extend([
        f"PY  - {year}",
        f"PB  - {meta.publisher}",
    ])

    if doi:
        lines.append(f"DO  - {doi}")
        lines.append(f"UR  - https://doi.org/{doi}")

    if meta.description:
        lines.append(f"AB  - {meta.description}")

    for kw in meta.keywords:
        lines.append(f"KW  - {kw}")

    lines.append(f"N1  - MDF Source ID: {source_id}")
    lines.append("ER  - ")

    return "\n".join(lines)


def generate_apa(record: Dict[str, Any]) -> str:
    """Generate APA style citation (plain text)."""
    meta = parse_metadata(record)

    year = str(meta.publication_year or datetime.now().year)
    version = record.get("version", "1.0")
    doi = record.get("doi")

    authors = _format_authors_apa(meta.authors)

    citation = f"{authors} ({year}). {meta.title} (Version {version}) [Data set]. {meta.publisher}."

    if doi:
        citation += f" https://doi.org/{doi}"

    return citation


def generate_datacite_xml(record: Dict[str, Any]) -> str:
    """Generate DataCite XML for DOI registration."""
    meta = parse_metadata(record)
    doi = record.get("doi") or "10.xxxxx/pending"

    root = ET.Element("resource")
    root.set("xmlns", "http://datacite.org/schema/kernel-4")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("xsi:schemaLocation", "http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4/metadata.xsd")

    # Identifier
    identifier = ET.SubElement(root, "identifier")
    identifier.set("identifierType", "DOI")
    identifier.text = doi

    # Creators
    creators_elem = ET.SubElement(root, "creators")
    for a in meta.authors:
        creator = ET.SubElement(creators_elem, "creator")
        family, given = _author_family_given(a)
        name_elem = ET.SubElement(creator, "creatorName")
        if family and given:
            name_elem.text = f"{family}, {given}"
        else:
            name_elem.text = a.name
        if given:
            given_elem = ET.SubElement(creator, "givenName")
            given_elem.text = given
        if family:
            family_elem = ET.SubElement(creator, "familyName")
            family_elem.text = family
        for aff in a.affiliations:
            affil = ET.SubElement(creator, "affiliation")
            affil.text = aff

    # Titles
    titles_elem = ET.SubElement(root, "titles")
    title = ET.SubElement(titles_elem, "title")
    title.text = meta.title

    # Publisher
    publisher = ET.SubElement(root, "publisher")
    publisher.text = meta.publisher

    # Publication Year
    pub_year = ET.SubElement(root, "publicationYear")
    pub_year.text = str(meta.publication_year or datetime.now().year)

    # Resource Type
    resource_type = ET.SubElement(root, "resourceType")
    resource_type.set("resourceTypeGeneral", "Dataset")
    resource_type.text = meta.resource_type or "Dataset"

    # Descriptions
    if meta.description:
        descriptions_elem = ET.SubElement(root, "descriptions")
        desc = ET.SubElement(descriptions_elem, "description")
        desc.set("descriptionType", "Abstract")
        desc.text = meta.description

    # Subjects
    if meta.keywords:
        subjects_elem = ET.SubElement(root, "subjects")
        for kw in meta.keywords:
            subj = ET.SubElement(subjects_elem, "subject")
            subj.text = kw

    # Version
    version_elem = ET.SubElement(root, "version")
    version_elem.text = str(record.get("version", "1.0"))

    ET.indent(root)
    return ET.tostring(root, encoding="unicode", xml_declaration=True)
