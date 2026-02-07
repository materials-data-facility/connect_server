"""Dataset preview cards for MDF v2.

Provides compact, human-readable summaries of datasets for quick preview
without downloading the full dataset or metadata.
"""

from typing import Any, Dict, List, Optional

from v2.metadata import parse_metadata
from v2.store import get_store


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
    if len(description) > 300:
        description = description[:297] + "..."

    card = {
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": meta.title,
        "authors": [a.name for a in meta.authors],
        "description": description,
        "keywords": meta.keywords[:10],
        "publisher": meta.publisher,
        "publication_year": meta.publication_year,
        "organization": record.get("organization") or meta.organization,
        "methods": meta.methods,
        "facility": meta.facility,
        "status": record.get("status"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
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

    # DOI
    doi = record.get("doi")
    if doi:
        card["doi"] = doi
        card["links"]["doi"] = f"https://doi.org/{doi}"

    # Profile summary when available
    profile = record.get("dataset_profile")
    if profile:
        import json as _json
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
