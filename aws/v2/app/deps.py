from fastapi import Request
from v2.store import SubmissionStore, get_store
from v2.stream_store import StreamStore, get_stream_store
from v2.storage import StorageBackend, get_storage_backend


def get_submission_store() -> SubmissionStore:
    return get_store()


def get_stream_store_dep() -> StreamStore:
    return get_stream_store()


def get_storage() -> StorageBackend:
    return get_storage_backend()


def guard_source_id_path(request: Request) -> None:
    """Router-level guard: a malformed ``{source_id}`` path segment is a 404.

    Reads the path param off the request so it can be attached to a whole router
    without touching handler signatures (routes without a ``source_id`` are a
    no-op). Uses the LENIENT grammar so legacy ids with uppercase, non-ASCII or
    long names keep resolving; only path-hostile input (``/``, ``..``,
    whitespace, control chars, >160 chars) is rejected, and it is rejected the
    same way a nonexistent record is, so nothing about the grammar leaks.
    """
    source_id = request.path_params.get("source_id")
    if source_id is None:
        return
    from fastapi import HTTPException

    from v2.submission_utils import validate_source_id_lenient

    try:
        validate_source_id_lenient(source_id)
    except ValueError:
        raise HTTPException(404, "Dataset not found")
