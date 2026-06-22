import uuid
from typing import Any, Dict, List, Optional


def version_sort_key(value: str):
    """Numeric-aware sort key for dotted version strings.

    Ensures "2.0" < "10.0" (numeric) instead of lexicographic "10.0" < "2.0".
    Non-numeric segments fall back to their string value.
    """
    parts = []
    for part in str(value or "0").split("."):
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part))
    return parts


def latest_version(items: List[Dict[str, Any]]) -> Optional[str]:
    if not items:
        return None
    versions = [item.get("version") for item in items if item.get("version")]
    if not versions:
        return None

    versions_sorted = sorted(versions, key=version_sort_key)
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
