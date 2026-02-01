#!/usr/bin/env python3
"""
Test dataset preview and cloning from Globus.

This script:
1. Creates a stream with test files on Globus
2. Tests preview functionality
3. Clones the files back from Globus to local

Run with: python test_preview_clone.py
"""

import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

os.environ.setdefault("STORE_BACKEND", "sqlite")
os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_preview_test.db")
os.environ.setdefault("STORAGE_BACKEND", "globus")

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax

console = Console()


def main():
    console.print(Panel.fit(
        "[bold blue]MDF v2 Preview & Clone Test[/bold blue]",
        border_style="blue",
    ))
    console.print()

    # Check for Globus token
    from v2.storage.globus_https import load_cached_token
    token = load_cached_token()
    if not token:
        console.print("[red]No Globus token found. Run test_globus_upload.py first.[/red]")
        return

    # =========================================================================
    # Step 1: Create a stream and upload test files
    # =========================================================================
    console.print("[bold cyan]1. Creating stream with test files on Globus...[/bold cyan]")

    from v2.stream_store import get_stream_store
    from v2.storage import get_storage_backend, reset_storage_backend

    # Force Globus backend
    reset_storage_backend()
    storage = get_storage_backend()

    store = get_stream_store()
    now = datetime.now(timezone.utc).isoformat()
    stream_id = f"preview-test-{uuid.uuid4().hex[:8]}"

    store.create_stream({
        "stream_id": stream_id,
        "title": "Preview & Clone Test Stream",
        "status": "open",
        "file_count": 0,
        "total_bytes": 0,
        "created_at": now,
        "updated_at": now,
        "user_id": "test-user",
    })

    # Create test files with various types
    test_files = [
        {
            "filename": "experiment_data.csv",
            "content": b"""sample_id,temperature_k,pressure_mpa,yield_percent,notes
1,300,0.1,85.2,baseline
2,350,0.5,91.7,optimal
3,400,1.0,78.3,degradation observed
4,450,2.0,95.1,high pressure test
5,500,2.5,62.4,thermal decomposition
""",
            "content_type": "text/csv",
        },
        {
            "filename": "synthesis_params.json",
            "content": json.dumps({
                "experiment_id": "synth-2026-001",
                "catalyst": {"type": "Pt/Al2O3", "loading_wt_percent": 5.0},
                "conditions": {
                    "temperature_range": [300, 500],
                    "pressure_range": [0.1, 2.5],
                    "duration_hours": 4
                },
                "results": [
                    {"sample": 1, "phase": "cubic", "crystallinity": 0.92},
                    {"sample": 2, "phase": "tetragonal", "crystallinity": 0.88},
                ]
            }, indent=2).encode(),
            "content_type": "application/json",
        },
        {
            "filename": "notes.txt",
            "content": b"""Experiment Log - 2026-01-31

Sample preparation began at 09:00.
Reactor stabilized by 09:30.

Key observations:
- Sample 2 showed unexpected color change at 350K
- Pressure fluctuation detected at 14:22
- All samples collected successfully

Next steps:
- XRD analysis pending
- Send samples for TEM imaging
""",
            "content_type": "text/plain",
        },
    ]

    uploaded_files = []
    for f in test_files:
        meta = storage.store_file(
            stream_id=stream_id,
            filename=f["filename"],
            content=f["content"],
            content_type=f["content_type"],
        )
        uploaded_files.append(meta)
        console.print(f"  Uploaded: {meta.filename} ({meta.size_bytes} bytes)")
        console.print(f"    URL: {meta.download_url}")

    # Update stream stats
    store.append_stream(
        stream_id=stream_id,
        file_count=len(test_files),
        total_bytes=sum(len(f["content"]) for f in test_files),
    )

    console.print(f"\n  [green]Stream created: {stream_id}[/green]")
    console.print()

    # =========================================================================
    # Step 2: Test preview functionality
    # =========================================================================
    console.print("[bold cyan]2. Testing file previews...[/bold cyan]")

    from v2.preview import generate_preview

    for f in test_files:
        console.print(f"\n  [bold]{f['filename']}[/bold]")
        preview = generate_preview(f["content"], f["filename"])

        if preview["type"] == "csv":
            console.print(f"    Type: CSV ({preview['total_rows']} rows)")
            console.print(f"    Columns: {', '.join(preview['headers'])}")

            # Show column stats
            for col in preview["columns"][:3]:
                if col["type"] == "numeric":
                    console.print(f"      {col['name']}: numeric, range [{col.get('min', 'N/A')}, {col.get('max', 'N/A')}]")
                else:
                    console.print(f"      {col['name']}: string, {col.get('unique_count', 'N/A')} unique values")

            # Show preview rows
            console.print("    Preview:")
            for row in preview["rows"][:3]:
                console.print(f"      {row}")

        elif preview["type"] == "json":
            console.print(f"    Type: JSON")
            console.print(f"    Top-level keys: {preview.get('top_level_keys', [])}")
            console.print(f"    Structure preview:")
            console.print(Syntax(json.dumps(preview["structure"], indent=2)[:500], "json", theme="monokai"))

        elif preview["type"] == "text":
            console.print(f"    Type: Text ({preview['total_lines']} lines)")
            console.print("    Preview:")
            for line in preview["lines"][:5]:
                console.print(f"      {line}")

    console.print()

    # =========================================================================
    # Step 3: Test cloning from Globus
    # =========================================================================
    console.print("[bold cyan]3. Cloning stream from Globus to local...[/bold cyan]")

    from v2.clone import clone_stream

    # Create a temp directory for cloning
    with tempfile.TemporaryDirectory() as tmpdir:
        console.print(f"  Cloning to: {tmpdir}")

        result = clone_stream(
            stream_id=stream_id,
            dest_dir=tmpdir,
            verbose=False,
        )

        console.print(f"\n  [green]Clone complete![/green]")
        console.print(f"    Downloaded: {result['downloaded']} files")
        console.print(f"    Total bytes: {result['total_bytes']:,}")

        # Verify files
        console.print("\n  Verifying cloned files:")
        clone_dir = Path(tmpdir) / stream_id
        for f in clone_dir.iterdir():
            size = f.stat().st_size
            console.print(f"    {f.name}: {size} bytes")

        # Read one back to verify content
        csv_path = clone_dir / "experiment_data.csv"
        if csv_path.exists():
            content = csv_path.read_text()
            lines = content.strip().split("\n")
            console.print(f"\n  Content verification (first 3 lines of CSV):")
            for line in lines[:3]:
                console.print(f"    {line}")

    console.print()

    # =========================================================================
    # Step 4: Test cloning a single URL
    # =========================================================================
    console.print("[bold cyan]4. Testing single file clone from URL...[/bold cyan]")

    from v2.clone import clone_url

    # Get one of the file URLs
    single_url = uploaded_files[0].download_url
    console.print(f"  URL: {single_url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        result = clone_url(
            url=single_url,
            dest_dir=tmpdir,
            verbose=False,
        )

        console.print(f"  [green]Downloaded: {result['filename']}[/green]")
        console.print(f"  Size: {result['size_bytes']} bytes")
        console.print(f"  Path: {result['path']}")

    console.print()

    # =========================================================================
    # Summary
    # =========================================================================
    console.print(Panel.fit(
        f"[bold green]Test Complete![/bold green]\n\n"
        f"Stream: {stream_id}\n"
        f"Files on Globus: {len(uploaded_files)}\n\n"
        f"Features tested:\n"
        f"  [green]✓[/green] File upload to Globus\n"
        f"  [green]✓[/green] CSV preview with statistics\n"
        f"  [green]✓[/green] JSON structure preview\n"
        f"  [green]✓[/green] Text file preview\n"
        f"  [green]✓[/green] Stream cloning from Globus\n"
        f"  [green]✓[/green] Single file clone from URL",
        border_style="green",
    ))


if __name__ == "__main__":
    main()
