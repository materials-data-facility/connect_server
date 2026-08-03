"""Text embeddings for MDF datasets.

Embeds dataset title + description into a vector suitable for semantic search.
Uses OpenAI's text-embedding-3-small at 1536 dims.

Title is duplicated in the embedded text so it dominates cosine similarity —
the title is the single most important signal for matching a user query.
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

import httpx

from v2.metadata import DatasetMetadata

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIMS = int(os.environ.get("EMBEDDING_DIMS", "1536"))

OPENAI_EMBEDDINGS_URL = "https://api.openai.com/v1/embeddings"
OPENAI_TIMEOUT_SECONDS = float(os.environ.get("OPENAI_TIMEOUT_SECONDS", "30"))

# Keep payload under model limits and avoid paying for boilerplate
MAX_DESCRIPTION_CHARS = 2000


class EmbeddingError(RuntimeError):
    pass


def build_embedding_text(meta: DatasetMetadata) -> str:
    """Build the text that will be embedded for a dataset.

    Title appears twice so it dominates similarity scoring.
    Keywords help surface topical matches when descriptions are sparse.
    """
    parts: List[str] = []
    title = (meta.title or "").strip()
    if title:
        parts.append(title)
        parts.append(title)

    if meta.keywords:
        parts.append("Keywords: " + ", ".join(meta.keywords))

    description = (meta.description or "").strip()
    if description:
        if len(description) > MAX_DESCRIPTION_CHARS:
            description = description[:MAX_DESCRIPTION_CHARS].rsplit(" ", 1)[0] + "..."
        parts.append(description)

    return "\n\n".join(parts)


def _api_key() -> str:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key or key == "not-configured":
        raise EmbeddingError(
            "OPENAI_API_KEY is not configured. Required for embedding generation and semantic search."
        )
    return key


def embed_text(text: str, model: Optional[str] = None) -> List[float]:
    """Embed a single string. Returns a list of floats of length EMBEDDING_DIMS."""
    if not text or not text.strip():
        raise EmbeddingError("Cannot embed empty text")

    model_name = model or EMBEDDING_MODEL
    payload = {"model": model_name, "input": text}
    if EMBEDDING_DIMS and model_name.startswith("text-embedding-3"):
        payload["dimensions"] = EMBEDDING_DIMS

    headers = {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
    }

    try:
        resp = httpx.post(
            OPENAI_EMBEDDINGS_URL,
            headers=headers,
            json=payload,
            timeout=OPENAI_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise EmbeddingError(f"OpenAI embedding request failed: {exc}") from exc

    data = resp.json()
    embeddings = data.get("data") or []
    if not embeddings or "embedding" not in embeddings[0]:
        raise EmbeddingError(f"OpenAI returned no embedding: {data}")
    return embeddings[0]["embedding"]


def embed_batch(texts: List[str], model: Optional[str] = None) -> List[List[float]]:
    """Embed multiple strings in a single API call.

    OpenAI accepts arrays of inputs per request — much faster than one-by-one.
    Caller is responsible for batching large lists (keep each batch <~100 items
    and <~8k tokens per input).
    """
    if not texts:
        return []

    model_name = model or EMBEDDING_MODEL
    payload = {"model": model_name, "input": texts}
    if EMBEDDING_DIMS and model_name.startswith("text-embedding-3"):
        payload["dimensions"] = EMBEDDING_DIMS

    headers = {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
    }

    try:
        resp = httpx.post(
            OPENAI_EMBEDDINGS_URL,
            headers=headers,
            json=payload,
            timeout=OPENAI_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise EmbeddingError(f"OpenAI batch embedding request failed: {exc}") from exc

    data = resp.json()
    embeddings = data.get("data") or []
    if len(embeddings) != len(texts):
        raise EmbeddingError(
            f"OpenAI returned {len(embeddings)} embeddings for {len(texts)} inputs"
        )
    # Results are indexed; sort by the index field to be safe
    embeddings.sort(key=lambda e: e.get("index", 0))
    return [e["embedding"] for e in embeddings]
