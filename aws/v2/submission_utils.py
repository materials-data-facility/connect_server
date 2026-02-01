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


def increment_version(current: Optional[str]) -> str:
    if not current:
        return "1.0"
    try:
        major, minor = current.split(".")
        return "{}.{}".format(major, int(minor) + 1)
    except Exception:
        return "1.0"


def generate_source_id(prefix: str = "mdf") -> str:
    return "{}-{}".format(prefix, uuid.uuid4().hex)
