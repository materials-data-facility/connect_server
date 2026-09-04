"""Dataset preview cards for MDF v2.

Provides compact, human-readable summaries of datasets for quick preview
without downloading the full dataset or metadata.
"""

import json as _json
import os
from typing import Any, Dict, List, Optional

from v2.link_health import public_link_health
from v2.metadata import parse_metadata
from v2.store import get_store
from v2.submission_utils import record_previous_version, record_root_version

# Agent cards trade completeness for context budget: an LLM reading 40 cards
# does not need a 12k-character abstract, and the full text is one request away
# on the normal card.
AGENT_DESCRIPTION_MAX_CHARS = 600


def _parse_size(total_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if total_bytes is None:
        return "Unknown"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total_bytes < 1024:
            return f"{total_bytes:.1f} {unit}" if unit != "B" else f"{total_bytes} {unit}"
        total_bytes /= 1024
    return f"{total_bytes:.1f} PB"


def _extract_file_types(data_sources: List[str]) -> List[str]:
    """Extract file type hints from data sources."""
    extensions = set()
    for source in (data_sources or []):
        if "." in source:
            ext = source.rsplit(".", 1)[-1].lower()
            if len(ext) <= 5 and ext.isalnum():
                extensions.add(ext)
    return sorted(extensions) if extensions else ["unknown"]


def build_dataset_card(record: Dict[str, Any]) -> Dict[str, Any]:
    """Build a preview card from a submission record.

    Returns a compact summary suitable for display in search results,
    dashboards, or quick previews.
    """
    meta = parse_metadata(record)

    description = meta.description or ""

    card = {
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": meta.title,
        # Full author objects, not just names: the metadata edit form loads
        # authors from the card and the backend replaces the array wholesale
        # on save — serving names only made every edit strip affiliations,
        # ORCIDs and given/family names from the record.
        "authors": [
            {
                key: value
                for key, value in {
                    "name": a.name,
                    "given_name": a.given_name,
                    "family_name": a.family_name,
                    "orcid": a.orcid,
                    "affiliations": a.affiliations,
                }.items()
                if value
            }
            for a in meta.authors
        ],
        "description": description,
        "keywords": meta.keywords,
        "publisher": meta.publisher,
        "publication_year": meta.publication_year,
        "organization": record.get("organization") or meta.organization,
        "methods": meta.methods,
        "facility": meta.facility,
        "status": record.get("status"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        # Version chain, as bare version strings (N3). Read from the top-level
        # record attributes, normalizing the legacy "{source_id}-{version}"
        # composite on rows the backfill has not reached. Never acl — see
        # _public_submission_view.
        "root_version": record_root_version(record),
        "previous_version": record_previous_version(record),
        # Quick stats
        "stats": {
            "file_types": _extract_file_types(meta.data_sources),
            "data_sources_count": len(meta.data_sources),
            "file_count": record.get("file_count", 0),
            "total_bytes": record.get("total_bytes", 0),
            "size_human": _parse_size(record.get("total_bytes", 0)),
        },
        # Links
        "links": {
            "self": f"/status/{record.get('source_id')}",
            "citation": f"/citation/{record.get('source_id')}",
        }
    }

    # Download info (for clone/download)
    if meta.download_url:
        card["download_url"] = meta.download_url
    if meta.archive_size:
        card["archive_size"] = meta.archive_size
    card["data_sources"] = list(meta.data_sources)

    # ML summary when present
    if meta.ml:
        ml_summary = {
            "data_format": meta.ml.data_format,
            "task_type": meta.ml.task_type,
            "n_items": meta.ml.n_items,
        }
        if meta.ml.splits:
            ml_summary["splits"] = [
                {"type": s.type, "n_items": s.n_items} for s in meta.ml.splits
            ]
        if meta.ml.keys:
            inputs = [k.name for k in meta.ml.keys if k.role == "input"]
            targets = [k.name for k in meta.ml.keys if k.role == "target"]
            if inputs:
                ml_summary["input_keys"] = inputs
            if targets:
                ml_summary["target_keys"] = targets
        if meta.ml.short_name:
            ml_summary["short_name"] = meta.ml.short_name
        card["ml"] = ml_summary

    # License
    if meta.license:
        card["license"] = meta.license.name

    # DOI — versions created by a metadata edit carry only the inherited
    # dataset (concept) DOI, never a per-version one.
    doi = record.get("doi") or record.get("dataset_doi")
    if doi:
        card["doi"] = doi
        card["links"]["doi"] = f"https://doi.org/{doi}"
    if record.get("dataset_doi"):
        card["dataset_doi"] = record["dataset_doi"]

    # External provenance (cross-published datasets)
    if meta.external:
        provenance = {"source": meta.external.source}
        if meta.external.doi:
            provenance["doi"] = meta.external.doi
            provenance["doi_url"] = f"https://doi.org/{meta.external.doi}"
        if meta.external.url:
            provenance["url"] = meta.external.url
        provenance["notice"] = f"Originally published at {meta.external.source}"
        card["external_provenance"] = provenance

    # Data availability (extensions proposal P1). Status + checked_at only —
    # the per-URL checks (internal paths, upstream error strings) stay in the
    # curator-only admin report.
    link_health = public_link_health(record)
    if link_health:
        card["link_health"] = link_health

    # Profile summary when available
    profile = record.get("dataset_profile")
    if profile:
        if isinstance(profile, str):
            try:
                profile = _json.loads(profile)
            except Exception:
                profile = None
        if isinstance(profile, dict):
            ps = {
                "total_files": profile.get("total_files"),
                "total_bytes": profile.get("total_bytes"),
                "formats": profile.get("formats", {}),
            }
            # Tabular summary from first file with columns
            for fp in profile.get("files", []):
                cols = fp.get("columns", [])
                if cols:
                    ps["tabular_summary"] = {
                        "filename": fp.get("filename"),
                        "n_rows": fp.get("n_rows"),
                        "columns": [{"name": c.get("name"), "dtype": c.get("dtype")} for c in cols],
                    }
                    ps["sample_rows"] = fp.get("sample_rows", [])[:3]
                    break
            card["profile_summary"] = ps

    return card


# ---------------------------------------------------------------------------
# Agent card
#
# `GET /card/{source_id}?format=agent`. Same record, same visibility rules, a
# different consumer: an LLM or a coding agent that has a few thousand tokens
# to spend and wants (a) enough to cite the dataset correctly and (b) code that
# actually loads it. So: no nested `stats`/`links` indirection, no per-file
# profile dump, a truncated description, and a ready-to-run snippet.
# ---------------------------------------------------------------------------

def _portal_base() -> str:
    """Public portal base URL — the same env var the notification emails use."""
    return os.environ.get("PORTAL_URL", "https://www.materialsdatafacility.org").rstrip("/")


def _landing_url(source_id: Optional[str], version: Optional[str] = None) -> Optional[str]:
    """Versioned portal landing page for a record.

    Prefers ``doi_utils.landing_url`` so the card and the URL registered with
    DataCite can never disagree; falls back to building it here for as long as
    that helper does not exist.
    """
    if not source_id:
        return None
    try:
        from v2.doi_utils import landing_url  # type: ignore[attr-defined]
    except ImportError:
        base = f"{_portal_base()}/detail/{source_id}"
        return f"{base}?version={version}" if version else base
    return landing_url(source_id, version)


def _coerce_profile(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    profile = record.get("dataset_profile")
    if isinstance(profile, str):
        try:
            profile = _json.loads(profile)
        except Exception:
            return None
    return profile if isinstance(profile, dict) else None


def _agent_columns(profile: Optional[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Tabular columns from the first profiled file that has any."""
    if not profile:
        return None
    for fp in profile.get("files", []) or []:
        cols = fp.get("columns") or []
        if cols:
            return [
                {"name": c.get("name"), "dtype": c.get("dtype")}
                for c in cols
                if c.get("name")
            ]
    return None


def _truncate(text: Optional[str], limit: int) -> Optional[str]:
    """Truncate on a word boundary with an ellipsis, or return as-is."""
    if not text:
        return None
    clean = " ".join(str(text).split())
    if len(clean) <= limit:
        return clean
    cut = clean[: limit - 1]
    space = cut.rfind(" ")
    if space > limit // 2:
        cut = cut[:space]
    return cut.rstrip(" ,;:.") + "…"


def _loading_recipe(record: Dict[str, Any], meta: Any) -> Dict[str, str]:
    """Copy-pasteable code that actually loads this dataset.

    ML-ready records route through ``foundry`` (which understands splits and
    keys); everything else through ``mdf clone``, which is the only path that
    works for an arbitrary directory of files.
    """
    source_id = record.get("source_id") or ""
    version = record.get("version")
    version_kw = f', version="{version}"' if version else ""

    if getattr(meta, "ml", None):
        doi = record.get("doi") or record.get("dataset_doi") or source_id
        python = (
            "# pip install foundry-ml\n"
            "from foundry import Foundry\n\n"
            "f = Foundry()\n"
            f'ds = f.get_dataset("{doi}"{version_kw})\n'
            'X, y = ds.get_as_dict()["train"]'
        )
        shell = f"pip install foundry-ml"
    else:
        python = (
            "# pip install mdf-cli\n"
            "from mdf import MDFAgent\n\n"
            "agent = MDFAgent()\n"
            f'agent.clone("{source_id}"{version_kw})'
        )
        shell = f"pip install mdf-cli && mdf clone {source_id}"
    return {"python": python, "shell": shell}


def build_agent_card(record: Dict[str, Any]) -> Dict[str, Any]:
    """Compact, machine-first summary of a dataset record.

    Keys are always present (``None``/``[]`` when unknown) so a consumer never
    has to branch on absence — except the optional ``ml`` and ``columns``
    blocks, which are omitted entirely when the dataset has neither.
    """
    meta = parse_metadata(record)
    source_id = record.get("source_id")
    version = record.get("version")
    profile = _coerce_profile(record)

    file_count = record.get("file_count") or (profile or {}).get("total_files") or 0
    size_bytes = (
        record.get("total_bytes")
        or (profile or {}).get("total_bytes")
        or meta.archive_size
        or 0
    )

    from v2.citation import generate_apa

    card: Dict[str, Any] = {
        "source_id": source_id,
        "version": version,
        "title": meta.title,
        "doi": record.get("doi") or record.get("dataset_doi"),
        "license": (
            {
                key: value
                for key, value in {
                    "name": meta.license.name,
                    "identifier": meta.license.identifier,
                    "url": meta.license.url,
                }.items()
                if value
            }
            if meta.license
            else None
        ),
        "organization": record.get("organization") or meta.organization,
        "authors": [
            {
                key: value
                for key, value in {
                    "name": a.name,
                    "orcid": a.orcid,
                    "affiliations": a.affiliations,
                }.items()
                if value
            }
            for a in meta.authors
        ],
        "keywords": list(meta.keywords or []),
        "description": _truncate(meta.description, AGENT_DESCRIPTION_MAX_CHARS),
        "size_bytes": int(size_bytes or 0),
        "file_count": int(file_count or 0),
        "data_sources": list(meta.data_sources or []),
        "download_url": meta.download_url,
        "loading_recipe": _loading_recipe(record, meta),
        "citation_apa": generate_apa(record),
        "link_health": public_link_health(record),
        "urls": {
            # Absolute: the portal page a human should be sent to. The other
            # two are API paths, matching the `links` convention on the normal
            # card, because the caller already knows which API host it asked.
            "landing": _landing_url(source_id, version),
            "citation": f"/citation/{source_id}" if source_id else None,
            "files": f"/preview/{source_id}/files" if source_id else None,
        },
    }

    if meta.ml:
        ml_block: Dict[str, Any] = {
            "data_format": meta.ml.data_format,
            "task_type": list(meta.ml.task_type or []),
            "domain": list(meta.ml.domain or []),
            "n_items": meta.ml.n_items,
            "short_name": meta.ml.short_name,
        }
        if meta.ml.splits:
            ml_block["splits"] = [
                {"type": s.type, "path": s.path, "n_items": s.n_items}
                for s in meta.ml.splits
            ]
        if meta.ml.keys:
            ml_block["keys"] = [
                {"name": k.name, "role": k.role} for k in meta.ml.keys
            ]
        card["ml"] = ml_block

    columns = _agent_columns(profile)
    if columns:
        card["columns"] = columns

    return card


def build_stream_card(stream: Dict[str, Any]) -> Dict[str, Any]:
    """Build a preview card for a stream."""
    import json

    metadata = stream.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}
    if metadata is None:
        metadata = {}

    return {
        "stream_id": stream.get("stream_id"),
        "title": stream.get("title"),
        "lab_id": stream.get("lab_id"),
        "organization": stream.get("organization"),
        "status": stream.get("status"),
        "created_at": stream.get("created_at"),
        "updated_at": stream.get("updated_at"),
        "stats": {
            "file_count": stream.get("file_count", 0),
            "total_size": _parse_size(stream.get("total_bytes", 0)),
            "total_bytes": stream.get("total_bytes", 0),
        },
        "metadata": {
            "instrument": metadata.get("instrument"),
            "facility": metadata.get("facility"),
            "operator": metadata.get("operator"),
            "run_id": metadata.get("run_id"),
        },
        "links": {
            "self": f"/stream/{stream.get('stream_id')}",
            "files": f"/stream/{stream.get('stream_id')}/files",
        }
    }
