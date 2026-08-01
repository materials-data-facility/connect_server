import logging
import re
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from v2.app.auth import can_view_dataset, get_optional_auth, is_curator
from v2.app.deps import get_submission_store
from v2.app.models import AuthContext
from v2.citation import generate_apa, generate_bibtex, generate_datacite_xml, generate_ris
from v2.dataset_card import build_dataset_card
from v2.store import SubmissionStore
from v2.submission_utils import latest_version

logger = logging.getLogger(__name__)

router = APIRouter()

_VERSION_SUFFIX_RE = re.compile(r"^(.+)-(\d+\.\d+)$")
_EDITABLE_STATUSES = {"pending_curation", "rejected", "published"}


def _resolve_published(
    store: SubmissionStore,
    source_id: str,
    version: Optional[str],
    auth: Optional[AuthContext] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Resolve a record the caller may view, falling back to a pre-migration id.

    Returns (record, canonical_source_id). If ``source_id`` is an old v1 id that
    was promoted during migration, the matching record is returned and the
    canonical (current) source_id is reported so callers can surface a redirect.

    Visibility is ``can_view_dataset``, not just "published": a restricted
    dataset is only indexed for the identities in its acl, so its card,
    citation and detail page must 404 for everyone else rather than serve the
    metadata that Globus Search deliberately withholds.
    """
    record = store.get(source_id, version=version)
    if can_view_dataset(auth, record):
        return record, record.get("source_id", source_id)

    # "Latest" means the newest version the CALLER may see. The absolute
    # newest row is not viewable whenever a version is mid-publish (status
    # "approved" for the seconds-to-minutes the queue takes) or an update is
    # pending curation — an in-flight version must never 404 the whole
    # dataset for everyone else. Fall back to the newest viewable version:
    # owners get their in-flight version, anonymous callers get the newest
    # published one.
    if version is None and record is not None:
        fallback = _newest_visible(store, source_id, auth)
        if fallback is not None:
            return fallback, fallback.get("source_id", source_id)

    legacy = store.get_by_legacy_source_id(source_id)
    if can_view_dataset(auth, legacy):
        return legacy, legacy.get("source_id")

    return None, None


def _newest_visible(
    store: SubmissionStore,
    source_id: str,
    auth: Optional[AuthContext],
) -> Optional[Dict[str, Any]]:
    """The newest version of a dataset that the caller is allowed to view."""
    viewable = [
        r for r in store.list_versions(source_id) if can_view_dataset(auth, r)
    ]
    if not viewable:
        return None
    target = latest_version(viewable)
    for r in viewable:
        if r.get("version") == target:
            return r
    return None


def _build_permissions(auth: Optional[AuthContext], record: Dict[str, Any]) -> Dict[str, bool]:
    """Compute user permissions for a dataset record."""
    if not auth:
        return {"can_edit": False, "can_delete": False, "can_curate": False}

    owner = record.get("user_id") == auth.user_id
    curator = is_curator(auth)
    status = record.get("status", "")

    return {
        "can_edit": (owner or curator) and status in _EDITABLE_STATUSES,
        "can_delete": curator,
        "can_curate": curator and status == "pending_curation",
    }


@router.get("/card/{source_id}")
async def get_card(
    source_id: str,
    version: Optional[str] = Query(None),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    if version and version.lower() == "latest":
        version = None

    record, canonical = _resolve_published(store, source_id, version, auth)
    if not record:
        raise HTTPException(404, "Dataset not found")

    card = build_dataset_card(record)

    # Fire-and-forget view count increment (use the canonical id in case the
    # request came in on a legacy id).
    try:
        store.increment_counter(record["source_id"], record["version"], "view_count")
    except Exception:
        logger.debug("Failed to increment view_count for %s", record.get("source_id"), exc_info=True)

    resp = {"success": True, "card": card, "permissions": _build_permissions(auth, record)}
    if canonical and canonical != source_id:
        resp["canonical_source_id"] = canonical
        resp["redirected_from"] = source_id
    return resp


@router.get("/citation/{source_id}")
async def get_citation(
    source_id: str,
    version: Optional[str] = Query(None),
    format: Optional[str] = Query("all"),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    record, canonical = _resolve_published(store, source_id, version, auth)
    if not record:
        raise HTTPException(404, "Dataset not found")

    fmt = (format or "all").lower()

    result = {
        "success": True,
        "source_id": canonical or source_id,
        "version": record.get("version"),
    }
    if canonical and canonical != source_id:
        result["redirected_from"] = source_id

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

    return result


def _parse_detail_slug(slug: str) -> Tuple[str, Optional[str]]:
    """Parse a frontend detail slug into (source_id, version).

    Handles both formats:
      - "81d55710-5bec-4e71-91b0-6f269e8da85a-1.0" → (UUID, "1.0")
      - "levine_abo2179_database_v2.1-1.0"          → (name, "1.0")
      - "levine_abo2179_database_v2.1"               → (name, None)
    """
    m = _VERSION_SUFFIX_RE.match(slug)
    if m:
        return m.group(1), m.group(2)
    return slug, None


@router.get("/detail/{slug}")
async def get_card_by_slug(
    slug: str,
    version: Optional[str] = Query(None),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    """Resolve a frontend URL slug to a dataset card.

    Accepts two URL styles:
      - /detail/{source_id}-{version}    (slug format)
      - /detail/{source_id}?version=X    (query param format)

    The ?version query param takes precedence over a version embedded in the slug.
    """
    source_id, slug_version = _parse_detail_slug(slug)
    version = version or slug_version
    if version and version.lower() == "latest":
        version = None

    record, canonical = _resolve_published(store, source_id, version, auth)
    if not record:
        raise HTTPException(404, "Dataset not found")

    card = build_dataset_card(record)

    try:
        store.increment_counter(record["source_id"], record["version"], "view_count")
    except Exception:
        logger.debug("Failed to increment view_count for %s", record.get("source_id"), exc_info=True)

    resp = {
        "success": True,
        "source_id": canonical or source_id,
        "version": record.get("version"),
        "card": card,
        "permissions": _build_permissions(auth, record),
    }
    if canonical and canonical != source_id:
        resp["redirected_from"] = source_id
    return resp
