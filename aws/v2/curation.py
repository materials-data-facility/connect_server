"""Curation handlers for MDF v2.

Provides API endpoints for curators to review, approve, or reject submissions.
This replaces the Globus Automate weboption-based curation workflow.

Curation states:
- pending_curation: Awaiting curator review
- approved: Curator approved, ready for DOI/indexing
- rejected: Curator rejected with reason
- published: DOI minted and indexed

Endpoints:
- GET /curation/pending - List submissions awaiting curation
- GET /curation/{source_id} - Get submission details for curation
- POST /curation/{source_id}/approve - Approve a submission
- POST /curation/{source_id}/reject - Reject a submission
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from v2.metadata import parse_metadata, to_datacite
from v2.store import get_store as get_submission_store


# Curators can be defined by user ID or group
CURATOR_USER_IDS = set(
    os.environ.get("CURATOR_USER_IDS", "").split(",")
) - {""}

CURATOR_GROUP_IDS = set(
    os.environ.get("CURATOR_GROUP_IDS", "").split(",")
) - {""}


def _is_curator(auth: Dict[str, Any]) -> bool:
    """Check if the authenticated user is a curator."""
    user_id = auth.get("user_id", "")

    if user_id in CURATOR_USER_IDS:
        return True

    group_info = auth.get("group_info", "{}")
    if isinstance(group_info, str):
        try:
            group_info = json.loads(group_info)
        except Exception:
            group_info = {}

    user_groups = set(group_info.keys()) if isinstance(group_info, dict) else set()
    if user_groups & CURATOR_GROUP_IDS:
        return True

    if os.environ.get("ALLOW_ALL_CURATORS", "").lower() in ("true", "1", "yes"):
        return True

    return False


def _extract_title(submission: Dict[str, Any]) -> str:
    """Extract title from submission metadata."""
    meta = parse_metadata(submission)
    return meta.title


def _extract_doi_metadata(submission: Dict[str, Any]) -> Dict[str, Any]:
    """Extract DataCite-compatible metadata dict from a submission."""
    meta = parse_metadata(submission)
    source_id = submission.get("source_id", "unknown")

    doi_payload = to_datacite(
        meta,
        doi="",
        url=f"https://materialsdatafacility.org/detail/{source_id}",
        source_id=source_id,
        created_at=submission.get("created_at"),
        published_at=submission.get("published_at"),
    )

    attrs = doi_payload["data"]["attributes"]
    doi_metadata = {
        "titles": attrs.get("titles", []),
        "creators": attrs.get("creators", []),
        "publisher": attrs.get("publisher", "Materials Data Facility"),
        "publication_year": attrs.get("publicationYear", datetime.now().year),
    }
    if attrs.get("descriptions"):
        doi_metadata["descriptions"] = attrs["descriptions"]
    if attrs.get("subjects"):
        doi_metadata["subjects"] = attrs["subjects"]
    if attrs.get("rightsList"):
        doi_metadata["rightsList"] = attrs["rightsList"]
    if attrs.get("fundingReferences"):
        doi_metadata["fundingReferences"] = attrs["fundingReferences"]

    return doi_metadata


def _find_dataset_doi(all_versions: List[Dict[str, Any]]) -> Optional[str]:
    """Find the dataset (concept) DOI from prior published versions."""
    for v in all_versions:
        ddoi = v.get("dataset_doi")
        if ddoi:
            return ddoi
    # Fallback: look for doi on any published version
    for v in all_versions:
        if v.get("doi") and v.get("status") == "published":
            return v["doi"]
    return None


def _mint_doi_for_submission(
    submission: Dict[str, Any],
    all_versions: Optional[List[Dict[str, Any]]] = None,
    mint_doi: bool = True,
) -> Dict[str, Any]:
    """Mint or update a DOI for an approved submission.

    Version-aware logic:
    - First version (no prior DOI): mint dataset DOI, store as both doi and dataset_doi
    - Subsequent + mint_doi=True: mint version-specific DOI with -v suffix,
      add IsVersionOf relation, update dataset DOI metadata + HasVersion
    - Subsequent + mint_doi=False: no new DOI, but update dataset DOI metadata
      on DataCite to reflect this version
    """
    from v2.datacite import get_datacite_client

    try:
        client = get_datacite_client()
        source_id = submission.get("source_id", "unknown")
        version = submission.get("version", "1.0")
        doi_metadata = _extract_doi_metadata(submission)

        dataset_doi = _find_dataset_doi(all_versions or [])

        if not dataset_doi:
            if not mint_doi:
                # First version with mint_doi=False: no DOI at all
                client.close()
                return {"success": True, "doi": None, "dataset_doi": None}
            # First version: mint the dataset DOI
            result = client.mint_doi(
                source_id=source_id,
                metadata=doi_metadata,
                publish=True,
            )
            if result.get("success"):
                result["dataset_doi"] = result["doi"]
            client.close()
            return result

        # Subsequent version
        if mint_doi:
            # Mint a version-specific DOI with -v{version} suffix
            version_suffix = client._generate_suffix(source_id) + f"-v{version}"
            related = [
                {
                    "relatedIdentifier": dataset_doi,
                    "relatedIdentifierType": "DOI",
                    "relationType": "IsVersionOf",
                }
            ]
            result = client.mint_doi(
                source_id=source_id,
                metadata=doi_metadata,
                publish=True,
                doi_suffix=version_suffix,
                related_identifiers=related,
            )

            # Also update the dataset DOI metadata to reflect latest version
            # and add HasVersion pointing to the new version DOI
            if result.get("success"):
                version_doi = result["doi"]
                has_version = [
                    {
                        "relatedIdentifier": version_doi,
                        "relatedIdentifierType": "DOI",
                        "relationType": "HasVersion",
                    }
                ]
                client.update_metadata(
                    doi=dataset_doi,
                    metadata=doi_metadata,
                    related_identifiers=has_version,
                )
                result["dataset_doi"] = dataset_doi

            client.close()
            return result
        else:
            # No new DOI — just update the dataset DOI metadata on DataCite
            update_result = client.update_metadata(
                doi=dataset_doi,
                metadata=doi_metadata,
            )
            client.close()
            return {
                "success": update_result.get("success", False),
                "dataset_doi": dataset_doi,
                "doi": None,
                "metadata_updated": True,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}
