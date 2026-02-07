from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from v2.app.deps import get_submission_store
from v2.citation import generate_apa, generate_bibtex, generate_datacite_xml, generate_ris
from v2.dataset_card import build_dataset_card
from v2.store import SubmissionStore

router = APIRouter()


@router.get("/card/{source_id}")
async def get_card(
    source_id: str,
    version: Optional[str] = Query(None),
    store: SubmissionStore = Depends(get_submission_store),
):
    record = store.get(source_id, version=version)
    if not record:
        raise HTTPException(400, f"Dataset not found: {source_id}")

    card = build_dataset_card(record)
    return {"success": True, "card": card}


@router.get("/citation/{source_id}")
async def get_citation(
    source_id: str,
    version: Optional[str] = Query(None),
    format: Optional[str] = Query("all"),
    store: SubmissionStore = Depends(get_submission_store),
):
    record = store.get(source_id, version=version)
    if not record:
        raise HTTPException(400, f"Dataset not found: {source_id}")

    fmt = (format or "all").lower()

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

    return result
