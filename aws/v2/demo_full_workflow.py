#!/usr/bin/env python3
"""MDF v2 Backend Demo - Full Workflow Showcase.

This script demonstrates all capabilities of the MDF v2 local backend:
1. Dataset publishing (git-style workflow)
2. Stream creation with file uploads
3. Unified search across datasets and streams

Requirements:
    pip install rich httpx

Usage:
    # Start the local server first (in another terminal):
    cd cs/aws && python -m v2.local_server

    # Then run this demo:
    python v2/demo_full_workflow.py
"""

import base64
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.syntax import Syntax
from rich.tree import Tree
from rich import box

console = Console()

API_URL = os.environ.get("MDF_API_URL", "http://127.0.0.1:8080")


def banner():
    """Display the demo banner."""
    console.print()
    console.print(Panel.fit(
        "[bold blue]MDF v2 Backend Demo[/bold blue]\n"
        "[dim]Materials Data Facility - Local Development Server[/dim]",
        border_style="blue",
    ))
    console.print()


def section(title: str, description: str = ""):
    """Display a section header."""
    console.print()
    console.rule(f"[bold cyan]{title}[/bold cyan]")
    if description:
        console.print(f"[dim]{description}[/dim]")
    console.print()


def api_call(method: str, path: str, data: dict = None, params: dict = None) -> dict:
    """Make an API call to the local backend."""
    url = f"{API_URL}{path}"
    with httpx.Client(timeout=30.0) as client:
        if method == "GET":
            response = client.get(url, params=params)
        else:
            response = client.post(url, json=data)
        return response.json()


def demo_dataset_publishing():
    """Demonstrate the git-style dataset publishing workflow."""
    section(
        "1. Dataset Publishing",
        "Git-style workflow: define metadata → submit → track status"
    )

    # Show the payload we'll submit
    payload = {
        "dc": {
            "titles": [{"title": "High-Throughput DFT Study of Perovskite Stability"}],
            "creators": [
                {"creatorName": "Chen, Alice", "givenName": "Alice", "familyName": "Chen",
                 "affiliation": "Argonne National Laboratory"},
                {"creatorName": "Kumar, Raj", "givenName": "Raj", "familyName": "Kumar",
                 "affiliation": "University of Chicago"},
            ],
            "publisher": "Materials Data Facility",
            "publicationYear": "2026",
            "descriptions": [{
                "description": "Density functional theory calculations examining the thermodynamic "
                              "stability of 500+ perovskite compositions for solar cell applications.",
                "descriptionType": "Abstract"
            }],
            "subjects": [
                {"subject": "perovskite"},
                {"subject": "DFT"},
                {"subject": "solar cells"},
                {"subject": "stability"},
            ],
            "resourceType": {"resourceTypeGeneral": "Dataset", "resourceType": "Dataset"},
        },
        "data_sources": ["globus://my-endpoint/perovskite_dft_2026/"],
        "mdf": {
            "source_name": "perovskite_stability_highthroughput",
            "organization": "argonne_msd",
            "lab_id": "chen-lab",
            "facility": "ALCF",
            "instruments": ["Theta", "Polaris"],
        }
    }

    console.print("[bold]Submission Payload:[/bold]")
    syntax = Syntax(json.dumps(payload, indent=2), "json", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="POST /submit", border_style="green"))

    # Submit
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Submitting dataset...", total=None)
        result = api_call("POST", "/submit", data=payload)
        progress.update(task, completed=True)

    if result.get("success"):
        console.print(f"[green]✓[/green] Dataset submitted successfully!")

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Field", style="dim")
        table.add_column("Value", style="cyan")
        table.add_row("Source ID", result.get("source_id", ""))
        table.add_row("Version", result.get("version", ""))
        table.add_row("Status", result.get("status", "submitted"))
        console.print(table)
    else:
        console.print(f"[red]✗[/red] Error: {result.get('error')}")

    return result.get("source_id")


def demo_streaming_workflow():
    """Demonstrate the streaming data workflow with file uploads."""
    section(
        "2. Streaming Data Workflow",
        "For automated labs: create stream → upload files → track → close"
    )

    # Create a stream
    console.print("[bold]Creating a new data stream...[/bold]")

    stream_payload = {
        "title": "Autonomous XRD Synthesis Campaign",
        "lab_id": "selfdriving-lab-01",
        "organization": "argonne_asl",
        "metadata": {
            "instrument": "Bruker D8 Advance",
            "facility": "Argonne Self-Driving Lab",
            "operator": "AutoBot v2.1",
            "run_id": "campaign-2026-01-31",
        }
    }

    result = api_call("POST", "/stream/create", data=stream_payload)

    if not result.get("success"):
        console.print(f"[red]✗[/red] Failed to create stream: {result.get('error')}")
        return None

    stream_id = result.get("stream_id")
    console.print(f"[green]✓[/green] Stream created: [cyan]{stream_id}[/cyan]")
    console.print()

    # Simulate uploading experimental data files
    console.print("[bold]Uploading experimental data files...[/bold]")

    # Generate some fake XRD data
    sample_files = [
        {
            "filename": "sample_001_BaTiO3.xy",
            "content": "# BaTiO3 XRD Pattern\n# 2theta intensity\n20.0 150\n22.5 890\n31.5 2100\n38.9 450\n45.0 1800\n",
            "metadata": {"composition": "BaTiO3", "temperature_K": 300, "sample_id": "001"}
        },
        {
            "filename": "sample_002_SrTiO3.xy",
            "content": "# SrTiO3 XRD Pattern\n# 2theta intensity\n22.8 920\n32.4 2300\n39.9 520\n46.5 1950\n57.8 780\n",
            "metadata": {"composition": "SrTiO3", "temperature_K": 300, "sample_id": "002"}
        },
        {
            "filename": "sample_003_PbTiO3.xy",
            "content": "# PbTiO3 XRD Pattern\n# 2theta intensity\n21.5 780\n31.2 1900\n38.1 380\n44.5 1650\n55.2 620\n",
            "metadata": {"composition": "PbTiO3", "temperature_K": 300, "sample_id": "003"}
        },
        {
            "filename": "synthesis_log.json",
            "content": json.dumps({
                "campaign": "perovskite-screen-01",
                "start_time": "2026-01-31T10:00:00Z",
                "samples_synthesized": 3,
                "success_rate": 1.0,
                "notes": "All samples crystallized successfully"
            }, indent=2),
            "metadata": {"file_type": "log", "format": "json"}
        }
    ]

    upload_table = Table(title="Uploaded Files", box=box.ROUNDED)
    upload_table.add_column("Filename", style="cyan")
    upload_table.add_column("Size", justify="right")
    upload_table.add_column("Checksum", style="dim")
    upload_table.add_column("Metadata", style="yellow")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        for sample in sample_files:
            task = progress.add_task(f"Uploading {sample['filename']}...", total=None)

            upload_payload = {
                "filename": sample["filename"],
                "content_base64": base64.b64encode(sample["content"].encode()).decode("ascii"),
                "metadata": sample["metadata"],
            }

            result = api_call("POST", f"/stream/{stream_id}/upload", data=upload_payload)
            progress.update(task, completed=True)

            if result.get("success"):
                for f in result.get("files", []):
                    meta_str = ", ".join(f"{k}={v}" for k, v in sample["metadata"].items())
                    upload_table.add_row(
                        f["filename"],
                        f"{f['size_bytes']} bytes",
                        f["checksum_md5"][:12] + "...",
                        meta_str[:30] + ("..." if len(meta_str) > 30 else "")
                    )

            time.sleep(0.2)  # Small delay for visual effect

    console.print()
    console.print(upload_table)

    # Show stream status
    console.print()
    console.print("[bold]Stream Status:[/bold]")
    status = api_call("GET", f"/stream/{stream_id}")

    if status.get("success"):
        stream = status.get("stream", {})
        status_table = Table(show_header=False, box=box.SIMPLE)
        status_table.add_column("Field", style="dim")
        status_table.add_column("Value", style="cyan")
        status_table.add_row("Stream ID", stream.get("stream_id", ""))
        status_table.add_row("Title", stream.get("title", ""))
        status_table.add_row("Status", f"[green]{stream.get('status', '')}[/green]")
        status_table.add_row("File Count", str(stream.get("file_count", 0)))
        status_table.add_row("Total Bytes", str(stream.get("total_bytes", 0)))
        status_table.add_row("Lab ID", stream.get("lab_id", ""))
        console.print(status_table)

    # List files in stream
    console.print()
    console.print("[bold]Files in Stream:[/bold]")
    files_result = api_call("GET", f"/stream/{stream_id}/files")

    if files_result.get("success"):
        files = files_result.get("files", [])
        tree = Tree(f"[cyan]{stream_id}[/cyan]")
        for f in files:
            meta = f.get("metadata", {})
            meta_str = f" [dim]({meta.get('composition', meta.get('file_type', ''))})[/dim]" if meta else ""
            tree.add(f"[green]{f['filename']}[/green]{meta_str}")
        console.print(tree)

    return stream_id


def demo_search(source_id: str = None, stream_id: str = None):
    """Demonstrate unified search across datasets and streams."""
    section(
        "3. Unified Search",
        "Search across both published datasets and active streams"
    )

    searches = [
        ("perovskite", "all", "Searching for 'perovskite' across all content..."),
        ("DFT", "datasets", "Searching datasets for 'DFT'..."),
        ("XRD", "streams", "Searching streams for 'XRD'..."),
        ("synthesis", "all", "Searching for 'synthesis'..."),
    ]

    for query, search_type, description in searches:
        console.print(f"[bold]{description}[/bold]")

        result = api_call("GET", "/search", params={"q": query, "type": search_type, "limit": "5"})

        if result.get("total", 0) > 0:
            results_table = Table(box=box.SIMPLE)
            results_table.add_column("Type", width=8)
            results_table.add_column("Title")
            results_table.add_column("ID", style="cyan")
            results_table.add_column("Score", justify="right", style="yellow")

            for r in result.get("results", []):
                type_style = "[blue]dataset[/blue]" if r.get("type") == "dataset" else "[green]stream[/green]"
                results_table.add_row(
                    type_style,
                    (r.get("title", "")[:35] + "...") if len(r.get("title", "")) > 35 else r.get("title", ""),
                    r.get("source_id", r.get("stream_id", "")),
                    f"{r.get('score', 0):.1f}",
                )

            console.print(results_table)
        else:
            console.print(f"[dim]No results found[/dim]")

        console.print()


def demo_cards_and_citations(source_id: str):
    """Demonstrate dataset cards and citation export."""
    section(
        "4. Dataset Cards & Citations",
        "Quick previews and citation export for researchers"
    )

    if not source_id:
        console.print("[dim]No dataset to show card for[/dim]")
        return

    # Get dataset card
    console.print("[bold]Dataset Preview Card:[/bold]")
    result = api_call("GET", f"/card/{source_id}")

    if result.get("success"):
        card = result.get("card", {})
        console.print(Panel(
            f"[bold]{card.get('title', 'Untitled')}[/bold]\n\n"
            f"[dim]{card.get('description', 'No description')}[/dim]",
            title=f"[cyan]{card.get('source_id')}[/cyan] v{card.get('version', '1.0')}",
            border_style="blue",
        ))

        # Metadata
        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Field", style="dim", width=12)
        table.add_column("Value")
        if card.get("authors"):
            table.add_row("Authors", ", ".join(card["authors"]))
        if card.get("keywords"):
            table.add_row("Keywords", ", ".join(card["keywords"][:5]))
        if card.get("methods"):
            table.add_row("Methods", ", ".join(card["methods"]))
        table.add_row("Status", f"[green]{card.get('status')}[/green]")
        console.print(table)
    else:
        console.print(f"[red]Error:[/red] {result.get('error')}")

    console.print()

    # Get citations
    console.print("[bold]Citation Export:[/bold]")

    # APA
    result = api_call("GET", f"/citation/{source_id}", params={"format": "apa"})
    if result.get("success"):
        console.print(Panel(result.get("apa", ""), title="APA Format", border_style="green"))

    # BibTeX
    result = api_call("GET", f"/citation/{source_id}", params={"format": "bibtex"})
    if result.get("success"):
        from rich.syntax import Syntax
        bibtex = result.get("bibtex", "")
        syntax = Syntax(bibtex, "bibtex", theme="monokai")
        console.print(Panel(syntax, title="BibTeX", border_style="green"))


def demo_api_overview():
    """Show an overview of all available API endpoints."""
    section(
        "5. API Reference",
        "All endpoints available in the MDF v2 local backend"
    )

    endpoints = [
        ("Dataset Publishing", [
            ("POST", "/submit", "Submit a new dataset"),
            ("GET", "/status/{source_id}", "Get dataset status"),
            ("GET", "/submissions", "List all submissions"),
            ("POST", "/status/update", "Update submission status"),
        ]),
        ("Streaming Data", [
            ("POST", "/stream/create", "Create a new stream"),
            ("POST", "/stream/{id}/upload", "Upload files to stream"),
            ("GET", "/stream/{id}/files", "List files in stream"),
            ("POST", "/stream/{id}/append", "Append metadata to stream"),
            ("POST", "/stream/{id}/snapshot", "Create searchable snapshot"),
            ("POST", "/stream/{id}/close", "Close/finalize stream"),
            ("GET", "/stream/{id}", "Get stream status"),
        ]),
        ("Discovery", [
            ("GET", "/search?q={query}", "Search datasets and streams"),
            ("GET", "/card/{source_id}", "Get dataset preview card"),
            ("GET", "/citation/{source_id}", "Export citation (bibtex, ris, apa)"),
        ]),
    ]

    for category, routes in endpoints:
        table = Table(title=f"[bold]{category}[/bold]", box=box.ROUNDED, title_justify="left")
        table.add_column("Method", style="magenta", width=6)
        table.add_column("Endpoint", style="cyan")
        table.add_column("Description", style="dim")

        for method, path, desc in routes:
            table.add_row(method, path, desc)

        console.print(table)
        console.print()


def demo_cli_commands():
    """Show the CLI commands available."""
    section(
        "6. CLI Quick Reference",
        "Use these commands with the mdf CLI tool"
    )

    cli_examples = """
# Dataset publishing workflow
mdf init ./my_dataset --title "My Dataset" --author "Jane Doe"
mdf add *.csv data/*.json
mdf commit -m "Initial dataset"
mdf validate
mdf publish --local --submit

# Streaming workflow
mdf stream create --title "Lab Experiment" --lab-id "lab-01"
mdf stream upload --stream-id <id> data.csv results.json
mdf stream files --stream-id <id>
mdf stream status --stream-id <id>
mdf stream close --stream-id <id>

# Search
mdf search "perovskite"
mdf search "XRD" --type streams
mdf backend search "DFT calculations" --limit 10
"""

    syntax = Syntax(cli_examples.strip(), "bash", theme="monokai", line_numbers=False)
    console.print(Panel(syntax, title="CLI Examples", border_style="green"))


def main():
    """Run the full demo."""
    banner()

    # Check if server is running
    console.print("[dim]Checking connection to local server...[/dim]")
    try:
        result = api_call("GET", "/submissions")
        console.print(f"[green]✓[/green] Connected to {API_URL}")
    except Exception as e:
        console.print(f"[red]✗[/red] Cannot connect to {API_URL}")
        console.print(f"[dim]Start the server with: cd cs/aws && python -m v2.local_server[/dim]")
        sys.exit(1)

    # Run demos
    source_id = demo_dataset_publishing()
    stream_id = demo_streaming_workflow()
    demo_search(source_id, stream_id)
    demo_cards_and_citations(source_id)
    demo_api_overview()
    demo_cli_commands()

    # Final summary
    section("Demo Complete!")

    summary = Table(show_header=False, box=box.SIMPLE)
    summary.add_column("", style="green")
    summary.add_column("")
    summary.add_row("✓", "Dataset published and indexed")
    summary.add_row("✓", "Stream created with file uploads")
    summary.add_row("✓", "Unified search working")
    summary.add_row("✓", "Full API available at " + API_URL)
    console.print(summary)

    console.print()
    console.print("[dim]Explore more at: https://github.com/materials-data-facility/mdf-connect[/dim]")
    console.print()


if __name__ == "__main__":
    main()
