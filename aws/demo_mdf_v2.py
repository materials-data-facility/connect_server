#!/usr/bin/env python3
"""
MDF v2 Backend Demo
===================

Comprehensive demonstration of the MDF v2 backend capabilities:
1. Stream creation and management (SQLite metadata store)
2. File uploads to local storage
3. File uploads to Globus HTTPS endpoint (1PB free storage)
4. Dataset cards and citations
5. Search functionality

Run with: python demo_mdf_v2.py
"""

import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add the aws directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Check for required packages
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.syntax import Syntax
    from rich.markdown import Markdown
except ImportError:
    print("Please install rich: pip install rich")
    sys.exit(1)

console = Console()

# ============================================================================
# Demo Configuration
# ============================================================================

DEMO_FILES = [
    {
        "filename": "experiment_001.csv",
        "content": b"sample_id,temperature_k,pressure_mpa,yield_percent\n1,300,0.1,85.2\n2,350,0.5,91.7\n3,400,1.0,78.3\n4,450,2.0,95.1\n",
        "content_type": "text/csv",
        "metadata": {"experiment_type": "synthesis", "instrument": "reactor-a"},
    },
    {
        "filename": "parameters.json",
        "content": b'{"catalyst": "Pt/Al2O3", "flow_rate_sccm": 50, "duration_hours": 4}',
        "content_type": "application/json",
        "metadata": {"schema_version": "1.0"},
    },
    {
        "filename": "notes.txt",
        "content": b"Experiment conducted on 2026-01-31.\nObserved unexpected phase transition at 380K.\nRequires further investigation.",
        "content_type": "text/plain",
        "metadata": {"author": "Dr. Jane Smith"},
    },
]


def setup_environment():
    """Configure environment for demo."""
    os.environ.setdefault("STORE_BACKEND", "sqlite")
    os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_demo.db")
    os.environ.setdefault("USE_MOCK_FLOW", "true")

    # Clean up old demo database
    db_path = os.environ.get("SQLITE_PATH")
    if db_path and os.path.exists(db_path):
        os.remove(db_path)


def create_demo_submission():
    """Create a demo submission record for dataset card/citation demo."""
    from v2.store import get_store
    import json

    store = get_store()
    now = datetime.now(timezone.utc).isoformat()

    # DataCite-style metadata structure
    dataset_mdata = {
        "dc": {
            "titles": [{"title": "High-Throughput Perovskite Synthesis Dataset"}],
            "creators": [
                {"givenName": "Jane", "familyName": "Smith", "affiliation": "Argonne National Laboratory"},
                {"givenName": "John", "familyName": "Doe", "affiliation": "University of Chicago"},
            ],
            "descriptions": [{
                "description": "A comprehensive dataset of perovskite synthesis experiments conducted using autonomous laboratory workflows. Contains XRD patterns, synthesis parameters, and yield measurements for 1,247 samples.",
                "descriptionType": "Abstract"
            }],
            "subjects": [
                {"subject": "perovskite"},
                {"subject": "synthesis"},
                {"subject": "autonomous"},
                {"subject": "materials science"},
            ],
            "publisher": "Materials Data Facility",
            "publicationYear": "2026",
            "resourceType": {"resourceTypeGeneral": "Dataset"},
            "rightsList": [{"rights": "CC-BY-4.0"}],
        },
        "mdf": {
            "organization": "argonne",
            "doi": "10.18126/demo-12345",
        }
    }

    record = {
        "source_id": "demo_perovskite_synthesis_v1",
        "version": "1.0",
        "dataset_mdata": json.dumps(dataset_mdata),
        "status": "published",
        "created_at": now,
        "updated_at": now,
        "user_id": "demo-user",
        "organization": "argonne",
        "file_count": 1247,
        "total_bytes": 2_500_000_000,
        "data_sources": ["xrd_patterns.zip", "synthesis_params.csv", "yields.json"],
    }

    store.put_submission(record)
    return record


# ============================================================================
# Demo Sections
# ============================================================================

def demo_header():
    """Display demo header."""
    console.print()
    console.print(Panel.fit(
        "[bold blue]MDF v2 Backend Demo[/bold blue]\n"
        "[dim]Materials Data Facility - Next Generation Data Infrastructure[/dim]",
        border_style="blue",
    ))
    console.print()


def demo_stream_lifecycle():
    """Demonstrate stream creation and management."""
    from v2.stream_store import get_stream_store

    console.print(Panel("[bold cyan]1. Stream Lifecycle Management[/bold cyan]", expand=False))
    console.print()

    store = get_stream_store()
    now = datetime.now(timezone.utc).isoformat()

    # Create stream
    import uuid
    stream_id = f"stream-{uuid.uuid4().hex[:12]}"

    record = {
        "stream_id": stream_id,
        "title": "Autonomous Synthesis Campaign - Lab 42",
        "lab_id": "argonne-lab-42",
        "status": "open",
        "file_count": 0,
        "total_bytes": 0,
        "created_at": now,
        "updated_at": now,
        "user_id": "demo-user",
        "organization": "argonne",
        "metadata": {"instrument": "robotic-synthesizer", "campaign": "perovskite-q1-2026"},
    }

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Creating stream...", total=None)
        store.create_stream(record)
        time.sleep(0.5)

    console.print(f"  [green]✓[/green] Created stream: [bold]{stream_id}[/bold]")
    console.print(f"    Title: {record['title']}")
    console.print(f"    Lab ID: {record['lab_id']}")
    console.print(f"    Status: [green]{record['status']}[/green]")
    console.print()

    return stream_id


def demo_local_storage(stream_id: str):
    """Demonstrate local storage backend."""
    from v2.storage import get_storage_backend, reset_storage_backend

    console.print(Panel("[bold cyan]2. Local Storage Backend[/bold cyan]", expand=False))
    console.print()

    # Force local storage
    reset_storage_backend()
    os.environ["STORAGE_BACKEND"] = "local"
    storage = get_storage_backend()

    console.print(f"  Backend: [bold]{storage.backend_name}[/bold]")
    console.print(f"  Base path: {storage.base_path}")
    console.print()

    table = Table(title="Uploaded Files (Local Storage)")
    table.add_column("Filename", style="cyan")
    table.add_column("Size", justify="right")
    table.add_column("Checksum (MD5)", style="dim")
    table.add_column("Path")

    for file_data in DEMO_FILES:
        meta = storage.store_file(
            stream_id=stream_id,
            filename=file_data["filename"],
            content=file_data["content"],
            content_type=file_data["content_type"],
            metadata=file_data["metadata"],
        )
        table.add_row(
            meta.filename,
            f"{meta.size_bytes} bytes",
            meta.checksum_md5[:12] + "...",
            meta.path,
        )

    console.print(table)
    console.print()

    # List files
    files = storage.list_files(stream_id)
    console.print(f"  [green]✓[/green] {len(files)} files stored locally")
    console.print()


def demo_globus_storage(stream_id: str):
    """Demonstrate Globus HTTPS storage backend."""
    from v2.storage import get_storage_backend, reset_storage_backend
    from v2.storage.globus_https import load_cached_token

    console.print(Panel("[bold cyan]3. Globus HTTPS Storage Backend[/bold cyan]", expand=False))
    console.print()

    # Check for Globus token
    token = load_cached_token()
    if not token:
        console.print("  [yellow]⚠[/yellow] No Globus token found. Run test_globus_upload.py first to authenticate.")
        console.print("    Skipping Globus upload demo.")
        console.print()
        return None

    # Force Globus storage
    reset_storage_backend()
    os.environ["STORAGE_BACKEND"] = "globus"

    try:
        storage = get_storage_backend()
    except Exception as e:
        console.print(f"  [red]✗[/red] Failed to initialize Globus storage: {e}")
        console.print()
        return None

    console.print(f"  Backend: [bold]{storage.backend_name}[/bold]")
    console.print(f"  Endpoint: [dim]{storage.endpoint_id}[/dim]")
    console.print(f"  Base URL: {storage.base_url}")
    console.print()

    table = Table(title="Uploaded Files (Globus HTTPS)")
    table.add_column("Filename", style="cyan")
    table.add_column("Size", justify="right")
    table.add_column("Globus URL")

    uploaded_files = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        for file_data in DEMO_FILES:
            task = progress.add_task(f"Uploading {file_data['filename']}...", total=None)
            try:
                meta = storage.store_file(
                    stream_id=stream_id,
                    filename=file_data["filename"],
                    content=file_data["content"],
                    content_type=file_data["content_type"],
                    metadata=file_data["metadata"],
                )
                uploaded_files.append(meta)
                table.add_row(
                    meta.filename,
                    f"{meta.size_bytes} bytes",
                    meta.download_url,
                )
            except Exception as e:
                console.print(f"  [red]✗[/red] Failed to upload {file_data['filename']}: {e}")
            progress.remove_task(task)

    if uploaded_files:
        console.print(table)
        console.print()
        console.print(f"  [green]✓[/green] {len(uploaded_files)} files uploaded to Globus endpoint")
        console.print()
        console.print("  [dim]Files are now accessible via HTTPS (with auth) at the URLs above.[/dim]")

    console.print()
    return uploaded_files


def demo_stream_status(stream_id: str, file_count: int, total_bytes: int):
    """Demonstrate stream status tracking."""
    from v2.stream_store import get_stream_store

    console.print(Panel("[bold cyan]4. Stream Status Tracking[/bold cyan]", expand=False))
    console.print()

    store = get_stream_store()

    # Update stream with file stats
    store.append_stream(
        stream_id=stream_id,
        file_count=file_count,
        total_bytes=total_bytes,
    )

    # Get updated status
    stream = store.get_stream(stream_id)

    table = Table(title="Stream Status", show_header=False)
    table.add_column("Property", style="bold")
    table.add_column("Value")

    table.add_row("Stream ID", stream["stream_id"])
    table.add_row("Title", stream["title"])
    table.add_row("Status", f"[green]{stream['status']}[/green]")
    table.add_row("File Count", str(stream["file_count"]))
    table.add_row("Total Bytes", f"{stream['total_bytes']:,}")
    table.add_row("Created", stream["created_at"])
    table.add_row("Last Append", stream.get("last_append_at") or "N/A")

    console.print(table)
    console.print()


def demo_dataset_card():
    """Demonstrate dataset preview cards."""
    from v2.dataset_card import build_dataset_card

    console.print(Panel("[bold cyan]5. Dataset Preview Card[/bold cyan]", expand=False))
    console.print()

    # Create a demo submission first
    record = create_demo_submission()
    card = build_dataset_card(record)

    console.print(f"  [bold]{card['title']}[/bold]")
    console.print(f"  [dim]Source ID: {card['source_id']} | Version: {card['version']}[/dim]")
    console.print()

    # Authors
    authors = ", ".join(card["authors"]) if card["authors"] else "Unknown"
    console.print(f"  [bold]Authors:[/bold] {authors}")
    console.print()

    # Description
    desc = card.get("description", "")
    if desc:
        console.print(f"  [bold]Description:[/bold]")
        console.print(f"  {desc[:200]}{'...' if len(desc) > 200 else ''}")
        console.print()

    # Keywords
    keywords = card.get("keywords", [])
    if keywords:
        console.print(f"  [bold]Keywords:[/bold] {', '.join(keywords)}")
        console.print()

    # Stats
    stats = card.get("stats", {})
    console.print(f"  [bold]Statistics:[/bold]")
    console.print(f"    Files: {stats.get('file_count', 0):,}")
    console.print(f"    Size: {stats.get('size_human', 'Unknown')}")
    console.print(f"    File types: {', '.join(stats.get('file_types', []))}")
    console.print()

    # Links
    console.print(f"  [bold]Links:[/bold]")
    for name, url in card.get("links", {}).items():
        if url:
            console.print(f"    {name}: {url}")
    console.print()


def demo_citations():
    """Demonstrate citation export."""
    from v2.citation import generate_bibtex, generate_apa, generate_ris
    from v2.store import get_store

    console.print(Panel("[bold cyan]6. Citation Export[/bold cyan]", expand=False))
    console.print()

    store = get_store()
    record = store.get_submission("demo_perovskite_synthesis_v1", "1.0")

    # BibTeX
    console.print("  [bold]BibTeX:[/bold]")
    bibtex = generate_bibtex(record)
    console.print(Syntax(bibtex, "bibtex", theme="monokai", line_numbers=False, word_wrap=True))
    console.print()

    # APA
    console.print("  [bold]APA:[/bold]")
    apa = generate_apa(record)
    console.print(f"  {apa}")
    console.print()


def demo_search():
    """Demonstrate search functionality."""
    from v2.search import search_datasets, search_streams
    from v2.stream_store import get_stream_store

    console.print(Panel("[bold cyan]7. Search Functionality[/bold cyan]", expand=False))
    console.print()

    # Create a few more streams for search demo
    store = get_stream_store()
    now = datetime.now(timezone.utc).isoformat()

    demo_streams = [
        {"title": "Iron Oxide Nanoparticle Synthesis", "organization": "mit"},
        {"title": "Perovskite Solar Cell Characterization", "organization": "stanford"},
        {"title": "Battery Electrolyte Screening", "organization": "argonne"},
    ]

    import uuid
    for s in demo_streams:
        store.create_stream({
            "stream_id": f"stream-{uuid.uuid4().hex[:8]}",
            "title": s["title"],
            "status": "open",
            "file_count": 0,
            "total_bytes": 0,
            "created_at": now,
            "updated_at": now,
            "user_id": "demo-user",
            "organization": s["organization"],
        })

    # Search streams
    console.print("  [bold]Search: 'perovskite'[/bold]")
    results = search_streams("perovskite")

    if results:
        table = Table()
        table.add_column("Stream ID", style="cyan")
        table.add_column("Title")
        table.add_column("Organization")

        for r in results[:5]:
            table.add_row(
                r["stream_id"][:20] + "...",
                r["title"][:40],
                r.get("organization", "N/A"),
            )

        console.print(table)
    else:
        console.print("  [dim]No results found[/dim]")

    console.print()

    # Search datasets
    console.print("  [bold]Search datasets: 'synthesis'[/bold]")
    results = search_datasets("synthesis")

    if results:
        for r in results[:3]:
            console.print(f"    - {r['title'][:50]}...")
    else:
        console.print("  [dim]No results found[/dim]")

    console.print()


def demo_preview_clone(stream_id: str):
    """Demonstrate preview and clone functionality."""
    from v2.preview import generate_preview
    from v2.storage import get_storage_backend

    console.print(Panel("[bold cyan]8. Data Preview[/bold cyan]", expand=False))
    console.print()

    storage = get_storage_backend()
    files = storage.list_files(stream_id)

    if not files:
        console.print("  [dim]No files to preview (stream may be empty)[/dim]")
        console.print()
        return

    console.print(f"  Previewing files from stream: {stream_id}")
    console.print()

    for f in files[:2]:  # Preview first 2 files
        content = storage.get_file(f.path)
        if not content:
            continue

        preview = generate_preview(content, f.filename)
        console.print(f"  [bold]{f.filename}[/bold] ({f.size_bytes} bytes)")

        if preview["type"] == "csv":
            console.print(f"    Type: CSV, {preview.get('total_rows', 0)} rows")
            headers = preview.get("headers", [])[:5]
            console.print(f"    Columns: {', '.join(headers)}")
            for row in preview.get("rows", [])[:2]:
                console.print(f"      {row[:5]}...")

        elif preview["type"] == "json":
            console.print(f"    Type: JSON")
            keys = preview.get("top_level_keys", [])[:5]
            console.print(f"    Keys: {keys}")

        elif preview["type"] == "text":
            console.print(f"    Type: Text, {preview.get('total_lines', 0)} lines")
            for line in preview.get("lines", [])[:2]:
                console.print(f"      {line[:60]}...")

        console.print()

    console.print("  [dim]Use client.stream_preview(stream_id) for full previews[/dim]")
    console.print("  [dim]Use client.stream_clone(stream_id, dest_dir) to download files[/dim]")
    console.print()


def demo_api_example():
    """Show example API usage."""
    console.print(Panel("[bold cyan]9. API Usage Examples[/bold cyan]", expand=False))
    console.print()

    example_code = '''
# Python client example
from mdf_agent import BackendClient

client = BackendClient.from_env()

# Create a stream
stream = client.stream_create(
    title="My Experiment Data",
    organization="my-lab"
)
print(f"Created: {stream['stream_id']}")

# Upload files
with open("data.csv", "rb") as f:
    result = client.stream_upload(
        stream_id=stream["stream_id"],
        filename="data.csv",
        content=f.read(),
    )

# Get stream status
status = client.stream_status(stream["stream_id"])
print(f"Files: {status['file_count']}")

# Close and publish
client.stream_close(stream["stream_id"])
'''

    console.print(Syntax(example_code, "python", theme="monokai", line_numbers=True))
    console.print()


def demo_summary():
    """Display demo summary."""
    console.print(Panel.fit(
        "[bold green]Demo Complete![/bold green]\n\n"
        "The MDF v2 backend provides:\n"
        "  • Stream-based data ingestion for automated labs\n"
        "  • Multiple storage backends (local, Globus, S3)\n"
        "  • 1PB free storage on Globus HTTPS endpoints\n"
        "  • Dataset cards and citation export\n"
        "  • Full-text search across datasets and streams\n"
        "  • Data preview (CSV stats, JSON structure, text)\n"
        "  • Clone/download from Globus to local\n\n"
        "[dim]Start the local server: python v2/local_server.py[/dim]",
        border_style="green",
    ))
    console.print()


# ============================================================================
# Main
# ============================================================================

def main():
    setup_environment()

    demo_header()

    # Stream lifecycle
    stream_id = demo_stream_lifecycle()

    # Local storage
    demo_local_storage(stream_id)

    # Globus storage (uses same stream_id for comparison)
    globus_files = demo_globus_storage(stream_id)

    # Calculate totals
    file_count = len(DEMO_FILES)
    total_bytes = sum(len(f["content"]) for f in DEMO_FILES)

    # If Globus worked, double the count (uploaded to both)
    if globus_files:
        file_count *= 2
        total_bytes *= 2

    # Stream status
    demo_stream_status(stream_id, file_count, total_bytes)

    # Dataset card
    demo_dataset_card()

    # Citations
    demo_citations()

    # Search
    demo_search()

    # Preview and clone (use Globus stream if available)
    if globus_files:
        demo_preview_clone(stream_id)

    # API examples
    demo_api_example()

    # Summary
    demo_summary()


if __name__ == "__main__":
    main()
