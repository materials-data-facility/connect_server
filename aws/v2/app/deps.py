from v2.store import SubmissionStore, get_store
from v2.stream_store import StreamStore, get_stream_store
from v2.storage import StorageBackend, get_storage_backend


def get_submission_store() -> SubmissionStore:
    return get_store()


def get_stream_store_dep() -> StreamStore:
    return get_stream_store()


def get_storage() -> StorageBackend:
    return get_storage_backend()
