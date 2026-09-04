import json
import re
import uuid
from typing import Any, Dict, List, Optional


SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")

SOURCE_ID_GRAMMAR = "^[a-z0-9][a-z0-9._-]{2,63}$"

#: Upper bound for a source_id we merely *address* (see
#: ``validate_source_id_lenient``). The longest id in the live corpus is 139
#: characters; this leaves headroom without letting an unbounded path segment
#: through.
MAX_LENIENT_SOURCE_ID_LENGTH = 160

_UNSAFE_SOURCE_ID_CHARS = ("/", "\\", "\x00")


def validate_source_id(value: str) -> str:
    """Return a valid NEW MDF source ID or raise ``ValueError``.

    The strict grammar, applied to ids MDF mints or newly accepts: 3--64
    lowercase characters, beginning with an alphanumeric, containing only
    lowercase alphanumerics plus ``.``, ``_``, and ``-``, and never ``..``.

    Use :func:`validate_source_id_lenient` for an id that merely *addresses* an
    existing record. The live corpus predates this grammar: 32 of 1,547 staging
    ids carry uppercase (``Dataset_Li_conductivity``), non-ASCII
    (``kononov_identifying_native_αalumina``) or run past 64 characters, and
    one is the two-character ``cs``. Holding path parameters to the strict
    grammar would 404 every one of them.
    """
    if not isinstance(value, str) or not SOURCE_ID_RE.fullmatch(value) or ".." in value:
        raise ValueError(
            "source_id must match {} and must not contain '..'".format(
                SOURCE_ID_GRAMMAR
            )
        )
    return value


def validate_source_id_lenient(value: str) -> str:
    """Return a source ID safe to *address*, unchanged, or raise ``ValueError``.

    Rejects only what is actually dangerous or unusable as a key/path segment —
    path separators, control characters, whitespace, ``..`` traversal, a leading
    ``-``/``.``, emptiness, and absurd length — and passes everything else
    through byte-for-byte. This is what route path parameters and ``update=True``
    submits use, so the pre-grammar ids already in the store stay reachable.
    """
    if not isinstance(value, str) or not value:
        raise ValueError("source_id is required")
    if len(value) > MAX_LENIENT_SOURCE_ID_LENGTH:
        raise ValueError(
            "source_id may not exceed {} characters".format(
                MAX_LENIENT_SOURCE_ID_LENGTH
            )
        )
    if ".." in value:
        raise ValueError("source_id may not contain '..'")
    if value[0] in "-.":
        raise ValueError("source_id may not begin with '-' or '.'")
    for char in value:
        if char.isspace() or ord(char) < 0x20 or ord(char) == 0x7F:
            raise ValueError("source_id may not contain whitespace or control characters")
        if char in ("/", "\\"):
            raise ValueError("source_id may not contain path separators")
    return value


def latest_version(items: List[Dict[str, Any]]) -> Optional[str]:
    if not items:
        return None
    versions = [item.get("version") for item in items if item.get("version")]
    if not versions:
        return None

    def sort_key(value: str):
        parts = []
        for part in value.split("."):
            if part.isdigit():
                parts.append(int(part))
            else:
                parts.append(part)
        return parts

    versions_sorted = sorted(versions, key=sort_key)
    return versions_sorted[-1]


def increment_version(current: Optional[str], major: bool = False) -> str:
    if not current:
        return "1.0"
    try:
        maj, _min = current.split(".")
        if major:
            return "{}.0".format(int(maj) + 1)
        return "{}.{}".format(maj, int(_min) + 1)
    except Exception:
        return "1.0"


def deep_merge(base: dict, updates: dict) -> None:
    """Deep merge updates into base dict (mutates base)."""
    for key, value in updates.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            deep_merge(base[key], value)
        else:
            base[key] = value


def generate_source_id(prefix: str = "mdf") -> str:
    return "{}-{}".format(prefix, uuid.uuid4().hex)


# ---------------------------------------------------------------------------
# Record shape (v2.1)
#
# Four system attributes used to live inside the user-editable ``dataset_mdata``
# JSON blob: the access-control list, the search facet grouping key, the
# original v1 id, and the version-chain pointers. That made authorization a
# JSON-parse away from a user-supplied string (N5), let a submitter rewrite
# their own search grouping or aim a submit at someone else's partition key
# (N4), and encoded pointers as ``"{source_id}-{version}"`` composites that are
# ambiguous for legacy ids ending in a version-like suffix (N3/N12).
#
# In the v2.1 shape they are TOP-LEVEL record attributes, and dataset identity
# is always the ``(source_id, version)`` pair:
#
#   acl                : List[str]        default ["public"]
#   source_name        : str              facet grouping key, default source_id
#   legacy_source_id   : Optional[str]    original v1 id (omitted when == source_id)
#   root_version       : Optional[str]    BARE version string, e.g. "1.0"
#   previous_version   : Optional[str]    BARE version string, e.g. "1.0"
#
# ``normalize_record_shape`` is the single writer of that shape; every store
# write goes through it, and ``scripts/backfill_record_shape.py`` replays it
# over existing rows. The ``record_*`` readers below prefer the top-level
# attribute and fall back to the blob so un-backfilled rows keep working.
# ---------------------------------------------------------------------------

RECORD_SHAPE_VERSION = "2.1"

DEFAULT_ACL: List[str] = ["public"]

#: Reserved ``extensions`` keys. ``extensions`` is user metadata; anything
#: namespaced ``mdf_*`` is system identity and is rejected on write.
RESERVED_EXTENSION_PREFIX = "mdf_"

#: Deprecated ``extensions`` keys still sent by released clients. Accepted on
#: write, copied to their top-level home, and stripped from what gets stored.
DEPRECATED_EXTENSION_ALIASES: Dict[str, str] = {
    "mdf_source_id": "source_id",
    "mdf_source_name": "source_name",
}

#: System attributes that must not survive inside ``dataset_mdata``.
_PROMOTED_METADATA_FIELDS = ("acl", "root_version", "previous_version")

_BARE_VERSION_RE = re.compile(r"^\d+(?:\.\d+)+$")

_IDENTITY_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

#: The public marker. Not a principal: Globus Search spells "anyone" as the
#: literal string "public" in ``visible_to``.
PUBLIC_PRINCIPAL = "public"


def normalize_acl_principal(value: Any) -> Optional[str]:
    """Canonical Globus principal for one ACL entry, or ``None`` if unusable.

    Three input forms occur in the corpus and must all end up spelled the way
    Globus Search expects in ``visible_to``:

      * ``"public"``                             -> passed through
      * a bare identity UUID                     -> ``urn:globus:auth:identity:<uuid>``
      * an already-qualified ``urn:globus:...``  -> passed through VERBATIM

    The third case is why this function exists: blindly prefixing produced
    ``urn:globus:auth:identity:urn:globus:auth:identity:<uuid>``, which Globus
    Search treats as an unknown principal — so a restricted dataset became
    invisible to the very collaborators it was shared with, and group ACLs
    (``urn:globus:groups:id:<uuid>``) never worked at all.

    Anything else yields ``None``; callers must drop it and deny, never widen
    to public.
    """
    text = str(value or "").strip()
    if not text:
        return None
    if text == PUBLIC_PRINCIPAL:
        return PUBLIC_PRINCIPAL
    if text.startswith("urn:globus:"):
        return text
    if _IDENTITY_UUID_RE.fullmatch(text.lower()):
        return "urn:globus:auth:identity:{}".format(text.lower())
    return None


def normalize_acl(acl: Any) -> List[str]:
    """Normalize every entry of an ACL, keeping unrecognized ones verbatim.

    Recognized entries are canonicalized by :func:`normalize_acl_principal`.
    An unrecognized entry is KEPT as-is rather than dropped: dropping it could
    empty a restricted ACL, and an empty ACL reads as ``["public"]`` (the legacy
    default for migrated records), so pruning on write would fail OPEN. The
    index writer drops unusable entries at ingest time instead, where it has a
    deny-all principal to fall back on.
    """
    normalized: List[str] = []
    for entry in acl or []:
        canonical = normalize_acl_principal(entry)
        candidate = canonical if canonical is not None else str(entry).strip()
        if candidate and candidate not in normalized:
            normalized.append(candidate)
    return normalized


def _as_dict(value: Any) -> Optional[Dict[str, Any]]:
    """Parse a value that may be a dict or a JSON string. None when unreadable."""
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def record_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """The record's ``dataset_mdata`` as a dict ({} when absent/unreadable).

    Accepts a full record or a bare metadata dict, matching
    ``v2.metadata.parse_metadata``'s tolerance.
    """
    if not isinstance(record, dict):
        return {}
    if "dataset_mdata" not in record:
        return record
    return _as_dict(record.get("dataset_mdata")) or {}


def normalize_version_pointer(
    value: Any, source_id: Optional[str] = None
) -> Optional[str]:
    """Normalize a version-chain pointer to a bare version string.

    Accepts the v2.1 form (``"1.0"``) unchanged and translates the legacy
    composite form (``"{source_id}-1.0"``) written by earlier code. A pointer
    that carries no version at all — the migrator wrote a bare ``source_id`` or
    ``source_name`` for single-entry groups — yields ``None``: an id is not a
    version, and inventing one would corrupt the chain.
    """
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if _BARE_VERSION_RE.fullmatch(text):
        return text
    if source_id and text == source_id:
        return None
    head, sep, tail = text.rpartition("-")
    if sep and _BARE_VERSION_RE.fullmatch(tail) and (not source_id or head == source_id):
        return tail
    return None


def resolve_record_acl(record: Dict[str, Any]) -> Optional[List[str]]:
    """Effective ACL for a submission record, or ``None`` when unreadable.

    Reads the top-level ``acl`` attribute first, falls back to the legacy
    ``dataset_mdata.acl`` (including the v1 ``mdf.acl`` nesting) for rows the
    backfill has not reached, and defaults to ``["public"]`` when neither is
    present — legacy migrated records carry no ACL and are public.

    ``None`` means "this record cannot be inspected". Callers must fail closed
    on it: an unparseable blob previously produced an empty ACL, which read as
    public and published restricted datasets to the world.
    """
    if not isinstance(record, dict):
        return None

    def _coerce(value: Any) -> Optional[List[str]]:
        if isinstance(value, str):
            parsed = value.strip()
            if parsed.startswith("["):
                try:
                    value = json.loads(parsed)
                except (TypeError, ValueError):
                    return None
            else:
                value = [parsed] if parsed else []
        if isinstance(value, (list, tuple)):
            entries = [str(entry) for entry in value if entry]
            return entries or list(DEFAULT_ACL)
        return None

    if record.get("acl") is not None:
        return _coerce(record.get("acl"))

    if "dataset_mdata" in record:
        blob = _as_dict(record.get("dataset_mdata"))
        if blob is None:
            return None
    else:
        blob = record

    candidate = blob.get("acl")
    if candidate is None:
        nested = blob.get("mdf")
        if isinstance(nested, dict):
            candidate = nested.get("acl")
    if candidate is None:
        return list(DEFAULT_ACL)
    return _coerce(candidate)


def record_source_name(record: Dict[str, Any]) -> Optional[str]:
    """Search facet grouping key: top-level, then the deprecated blob alias."""
    if not isinstance(record, dict):
        return None
    top = record.get("source_name")
    if top:
        return str(top)
    extensions = record_metadata(record).get("extensions")
    if isinstance(extensions, dict):
        alias = extensions.get("mdf_source_name")
        if alias:
            return str(alias)
    source_id = record.get("source_id")
    return str(source_id) if source_id else None


def record_legacy_source_id(record: Dict[str, Any]) -> Optional[str]:
    """Original v1 source id: top-level, then the blob copy."""
    if not isinstance(record, dict):
        return None
    top = record.get("legacy_source_id")
    if top:
        return str(top)
    extensions = record_metadata(record).get("extensions")
    if isinstance(extensions, dict):
        # Deliberately NOT extensions.mdf_source_id: on a v1-format submit that
        # is the *canonical* id the caller asked for, not a superseded one, and
        # registering it in the legacy-id index would point another dataset's
        # old id at this record.
        legacy = extensions.get("legacy_source_id")
        if legacy:
            return str(legacy)
    return None


def record_root_version(record: Dict[str, Any]) -> Optional[str]:
    """Bare root version: top-level, then the legacy composite in the blob."""
    if not isinstance(record, dict):
        return None
    source_id = record.get("source_id")
    for value in (record.get("root_version"), record_metadata(record).get("root_version")):
        normalized = normalize_version_pointer(value, source_id)
        if normalized:
            return normalized
    return None


def record_previous_version(record: Dict[str, Any]) -> Optional[str]:
    """Bare previous version: top-level, then the legacy composite in the blob."""
    if not isinstance(record, dict):
        return None
    source_id = record.get("source_id")
    for value in (
        record.get("previous_version"),
        record_metadata(record).get("previous_version"),
    ):
        normalized = normalize_version_pointer(value, source_id)
        if normalized:
            return normalized
    return None


def reserved_extension_keys(extensions: Any) -> List[str]:
    """Reserved (``mdf_*``) keys present in an ``extensions`` blob."""
    if not isinstance(extensions, dict):
        return []
    return sorted(
        key for key in extensions if str(key).startswith(RESERVED_EXTENSION_PREFIX)
    )


def normalize_record_shape(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``record`` in the canonical v2.1 shape.

    Promotes ``acl`` / ``source_name`` / ``legacy_source_id`` / ``root_version``
    / ``previous_version`` to top-level attributes, normalizes the two pointers
    to bare version strings, and strips the promoted fields plus the deprecated
    ``extensions.mdf_*`` aliases out of the stored ``dataset_mdata``.

    Idempotent and total: every store write and the backfill script share this
    one definition, and re-running it over an already-normalized record changes
    nothing. A record whose ``dataset_mdata`` cannot be parsed is returned with
    the blob untouched (nothing to promote out of it) but still gets the
    top-level defaults, so authorization never has to guess.
    """
    if not isinstance(record, dict):
        return record

    item = dict(record)
    source_id = item.get("source_id")

    raw = item.get("dataset_mdata")
    blob = _as_dict(raw)
    blob_readable = blob is not None
    blob = dict(blob) if blob else {}

    # --- acl (N5). An unreadable ACL is left exactly as it is: there is no
    # top-level value that means "readable by nobody" (an empty list reads as
    # public, matching the legacy default), so writing anything here would fail
    # OPEN. Readers call resolve_record_acl, get None, and deny.
    acl = resolve_record_acl(record)
    if acl is not None:
        item["acl"] = normalize_acl(acl) or list(DEFAULT_ACL)
    else:
        return item

    # --- version pointers (N3): bare version strings, top level.
    for field, reader in (
        ("root_version", record_root_version),
        ("previous_version", record_previous_version),
    ):
        pointer = reader(record)
        if pointer:
            item[field] = pointer
        else:
            item.pop(field, None)

    # --- identity (N4)
    source_name = record_source_name(record)
    if source_name:
        item["source_name"] = source_name

    legacy_source_id = record_legacy_source_id(record)
    if legacy_source_id and legacy_source_id != source_id:
        item["legacy_source_id"] = legacy_source_id
    else:
        # Dropped when absent or self-referential: this is the partition key of
        # the legacy-source-id GSI, DynamoDB rejects empty key attributes, and
        # indexing a record under its own canonical id buys nothing.
        item.pop("legacy_source_id", None)

    if not blob_readable:
        return item

    for field in _PROMOTED_METADATA_FIELDS:
        blob.pop(field, None)

    extensions = blob.get("extensions")
    if isinstance(extensions, dict):
        stripped = {
            key: value
            for key, value in extensions.items()
            if key not in DEPRECATED_EXTENSION_ALIASES
        }
        if stripped != extensions:
            blob = dict(blob)
            if stripped:
                blob["extensions"] = stripped
            else:
                blob.pop("extensions", None)

    item["dataset_mdata"] = json.dumps(blob) if isinstance(raw, str) else blob
    return item
