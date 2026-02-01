#!/usr/bin/env python3
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

API_URL = os.environ.get("MDF_API_URL", "http://127.0.0.1:8080").rstrip("/")

LAB_NAME = "Argonne National Laboratory"
LAB_ID = "anl-xrd-tga"
SAMPLE_ID = "ANL-SAMPLE-042"
RUN_ID = "RUN-2026-01-31-01"
OPERATOR = "A. Researcher"

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich import box
except Exception:  # pragma: no cover
    Console = None
    Panel = None
    Table = None
    box = None
    Panel = None
    Table = None
    box = None


class SimpleConsole:
    def print(self, message=""):
        print(message)


console = Console() if Console else SimpleConsole()


def request(method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = f"{API_URL}{path}"
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    try:
        import httpx

        with httpx.Client(timeout=30.0) as client:
            resp = client.request(method, url, json=payload, headers=headers)
            return resp.json()
    except Exception:
        from urllib import request as urlrequest

        req = urlrequest.Request(url, data=body, headers=headers, method=method)
        with urlrequest.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))


def format_bytes(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.1f} MB"


def _metadata_field(stream: Dict[str, Any], key: str, default: str = "") -> str:
    metadata = stream.get("metadata") or {}
    value = metadata.get(key, default)
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value) if value is not None else default


def make_stream_table(stream: Dict[str, Any]):
    if Table is None:
        lines = [
            "Stream Summary:",
            f"  Stream ID: {stream.get('stream_id', '')}",
            f"  Status: {stream.get('status', '')}",
            f"  Title: {stream.get('title', '')}",
            f"  Lab ID: {stream.get('lab_id', '')}",
            f"  Organization: {stream.get('organization', '')}",
            f"  Sample ID: {_metadata_field(stream, 'sample_id')}",
            f"  Run ID: {_metadata_field(stream, 'run_id')}",
            f"  Instruments: {_metadata_field(stream, 'instruments')}",
            f"  File Count: {stream.get('file_count', 0)}",
            f"  Total Bytes: {format_bytes(int(stream.get('total_bytes', 0) or 0))}",
            f"  Last Append: {stream.get('last_append_at') or '-'}",
            f"  Updated: {stream.get('updated_at') or '-'}",
        ]
        return "\n".join(lines)

    table = Table(title="Stream Summary", box=box.SIMPLE, show_header=True)
    table.add_column("Field", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Stream ID", stream.get("stream_id", ""))
    table.add_row("Status", stream.get("status", ""))
    table.add_row("Title", stream.get("title", ""))
    table.add_row("Lab ID", stream.get("lab_id", ""))
    table.add_row("Organization", stream.get("organization", ""))
    table.add_row("Sample ID", _metadata_field(stream, "sample_id"))
    table.add_row("Run ID", _metadata_field(stream, "run_id"))
    table.add_row("Instruments", _metadata_field(stream, "instruments"))
    table.add_row("File Count", str(stream.get("file_count", 0)))
    table.add_row("Total Bytes", format_bytes(int(stream.get("total_bytes", 0) or 0)))
    table.add_row("Last Append", stream.get("last_append_at") or "-")
    table.add_row("Updated", stream.get("updated_at") or "-")

    return table


def make_dataset_table(snapshot: Dict[str, Any]):
    if Table is None:
        lines = [
            "Snapshot Dataset:",
            f"  Source ID: {snapshot.get('source_id', '')}",
            f"  Version: {snapshot.get('version', '')}",
            f"  Versioned Source ID: {snapshot.get('versioned_source_id', '')}",
            f"  Stream ID: {snapshot.get('stream_id', '')}",
        ]
        return "\n".join(lines)

    table = Table(title="Snapshot Dataset", box=box.SIMPLE, show_header=True)
    table.add_column("Field", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Source ID", snapshot.get("source_id", ""))
    table.add_row("Version", snapshot.get("version", ""))
    table.add_row("Versioned Source ID", snapshot.get("versioned_source_id", ""))
    table.add_row("Stream ID", snapshot.get("stream_id", ""))
    return table


def make_files_table(title: str, files: List[Dict[str, Any]]):
    if Table is None:
        lines = [f"{title}:"]
        for entry in files:
            lines.append(
                "  - {path} ({size} B, {instrument}, {timestamp})".format(
                    path=entry.get("path", ""),
                    size=entry.get("size", 0),
                    instrument=entry.get("instrument", ""),
                    timestamp=entry.get("timestamp", ""),
                )
            )
        return "\n".join(lines)

    table = Table(title=title, box=box.SIMPLE, show_header=True)
    table.add_column("Path", style="cyan")
    table.add_column("Size", justify="right")
    table.add_column("Instrument")
    table.add_column("Timestamp")
    for entry in files:
        table.add_row(
            entry.get("path", ""),
            format_bytes(int(entry.get("size", 0))),
            entry.get("instrument", ""),
            entry.get("timestamp", ""),
        )
    return table


def print_intro():
    text = (
        "This demo simulates a lab instrument workflow at Argonne National Laboratory.\n\n"
        "What will happen:\n"
        "- Create a streaming session for a sample that includes XRD and TGA data.\n"
        "- Append XRD pattern files, then append TGA run files.\n"
        "- Fetch stream status, close the stream, and snapshot to a dataset.\n\n"
        "What you can do with MDF Streaming:\n"
        "- Continuously append new files as instruments generate data.\n"
        "- Track stream status, counts, and total bytes in near real time.\n"
        "- Close a stream when a run completes and snapshot it into a dataset submission.\n"
    )
    if Console and Panel and box:
        console.print(Panel(text, title="MDF Streaming Demo", box=box.SIMPLE))
    else:
        console.print(text)


def build_files(base_time: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    xrd_files = [
        {
            "path": f"xrd/{SAMPLE_ID}/pattern_001.xy",
            "size": 24576,
            "instrument": "XRD",
            "sample_id": SAMPLE_ID,
            "run_id": RUN_ID,
            "timestamp": (base_time + timedelta(seconds=0)).isoformat() + "Z",
        },
        {
            "path": f"xrd/{SAMPLE_ID}/pattern_002.xy",
            "size": 25120,
            "instrument": "XRD",
            "sample_id": SAMPLE_ID,
            "run_id": RUN_ID,
            "timestamp": (base_time + timedelta(seconds=30)).isoformat() + "Z",
        },
    ]
    tga_files = [
        {
            "path": f"tga/{SAMPLE_ID}/tga_run_001.csv",
            "size": 10240,
            "instrument": "TGA",
            "sample_id": SAMPLE_ID,
            "run_id": RUN_ID,
            "timestamp": (base_time + timedelta(minutes=2)).isoformat() + "Z",
        },
        {
            "path": f"tga/{SAMPLE_ID}/tga_run_002.csv",
            "size": 11264,
            "instrument": "TGA",
            "sample_id": SAMPLE_ID,
            "run_id": RUN_ID,
            "timestamp": (base_time + timedelta(minutes=3)).isoformat() + "Z",
        },
    ]
    return xrd_files, tga_files


def main():
    print_intro()

    console.print("Step 1/6: Creating stream...")
    create_payload = {
        "title": f"{LAB_NAME} XRD+TGA Stream - {SAMPLE_ID}",
        "lab_id": LAB_ID,
        "organization": "ANL",
        "metadata": {
            "facility": LAB_NAME,
            "instruments": ["XRD", "TGA"],
            "operator": OPERATOR,
            "sample_id": SAMPLE_ID,
            "run_id": RUN_ID,
            "beamline": "11-ID-B",
            "notes": "Local demo stream with XRD + TGA instrumentation",
        },
    }
    create_res = request("POST", "/stream/create", create_payload)
    stream = create_res.get("stream", {})
    stream_id = stream.get("stream_id")

    if not stream_id:
        console.print(f"Failed to create stream: {create_res}")
        raise SystemExit(1)

    console.print(make_stream_table(stream))

    base_time = datetime.utcnow()
    xrd_files, tga_files = build_files(base_time)

    console.print("\nStep 2/6: Appending XRD patterns...")
    console.print(make_files_table("XRD Files", xrd_files))
    xrd_res = request("POST", f"/stream/{stream_id}/append", {"files": xrd_files})
    stream = xrd_res.get("stream", stream)
    console.print(make_stream_table(stream))

    console.print("\nStep 3/6: Appending TGA runs...")
    console.print(make_files_table("TGA Files", tga_files))
    tga_res = request("POST", f"/stream/{stream_id}/append", {"files": tga_files})
    stream = tga_res.get("stream", stream)
    console.print(make_stream_table(stream))

    console.print("\nStep 4/6: Fetching stream status...")
    status_res = request("GET", f"/stream/{stream_id}")
    stream = status_res.get("stream", stream)
    console.print(make_stream_table(stream))

    console.print("\nStep 5/6: Closing stream...")
    close_res = request("POST", f"/stream/{stream_id}/close", {"stream_id": stream_id})
    stream = close_res.get("stream", stream)
    console.print(make_stream_table(stream))

    console.print("\nStep 6/6: Snapshotting stream into a dataset...")
    snapshot_res = request("POST", f"/stream/{stream_id}/snapshot", {"stream_id": stream_id})
    if snapshot_res.get("success"):
        console.print(make_dataset_table(snapshot_res))
    else:
        console.print(f"Snapshot failed: {snapshot_res}")

    console.print(
        "\nDemo complete. Next steps:\n"
        "- Append new files as instruments run.\n"
        "- Snapshot streams into datasets for indexing.\n"
        "- Start parallel streams for additional samples.\n"
    )


if __name__ == "__main__":
    main()
