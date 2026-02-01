#!/usr/bin/env python3
import os
import sys
import time
from datetime import datetime
from typing import Dict, List

from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent))

from v2.store import get_store

STATUS_SEQUENCE = ["submitted", "processing", "indexing", "complete"]


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _should_advance(updated_at: str, delay_seconds: int) -> bool:
    try:
        last = _parse_time(updated_at)
    except Exception:
        return True
    delta = datetime.utcnow().timestamp() - last.timestamp()
    return delta >= delay_seconds


def _next_status(current: str) -> str | None:
    if current not in STATUS_SEQUENCE:
        return None
    idx = STATUS_SEQUENCE.index(current)
    if idx + 1 >= len(STATUS_SEQUENCE):
        return None
    return STATUS_SEQUENCE[idx + 1]


def run_once(store, delays: Dict[str, int]) -> int:
    candidates = store.list_by_status(["submitted", "processing", "indexing"])
    updated = 0
    for record in candidates:
        status = record.get("status")
        updated_at = record.get("updated_at") or record.get("created_at")
        delay = delays.get(status, 5)
        if updated_at and _should_advance(updated_at, delay):
            next_status = _next_status(status)
            if next_status:
                store.update_status(record["source_id"], record["version"], next_status)
                updated += 1
                print(f"Advanced {record['source_id']}:{record['version']} -> {next_status}")
    return updated


def main():
    os.environ.setdefault("STORE_BACKEND", "sqlite")
    os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_connect_v2.db")

    interval = int(os.environ.get("FLOW_SIM_INTERVAL", "5"))
    delays = {
        "submitted": int(os.environ.get("FLOW_SIM_SUBMITTED_DELAY", "5")),
        "processing": int(os.environ.get("FLOW_SIM_PROCESSING_DELAY", "5")),
        "indexing": int(os.environ.get("FLOW_SIM_INDEXING_DELAY", "5")),
    }

    store = get_store()
    print("Local flow simulator running")
    print(f"Interval: {interval}s | Delays: {delays}")

    try:
        while True:
            run_once(store, delays)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Flow simulator stopped")


if __name__ == "__main__":
    main()
