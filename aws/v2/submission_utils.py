import uuid
from typing import Any, Dict, List, Optional


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
