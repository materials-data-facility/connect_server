#!/usr/bin/env python3
"""Demo: Full MDF workflow with search.

This demonstrates the complete local MDF experience:
1. Create multiple datasets and streams
2. Search across all of them
3. Find what you're looking for!

Run with: python cs/aws/v2/demo_search.py
"""

import os
import sys
import time
from pathlib import Path

# Setup paths
THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent))
sys.path.insert(0, str(THIS_DIR.parent.parent.parent / "src"))

os.environ.setdefault("MDF_API_URL", "http://127.0.0.1:8080")
os.environ.setdefault("STORE_BACKEND", "sqlite")
os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_connect_v2.db")

from mdf_agent.core.backend_client import BackendClient

# Rich console for pretty output
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich import print as rprint
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    console = None
    def rprint(*args, **kwargs):
        print(*args)


def print_header(text):
    if HAS_RICH:
        console.print(f"\n[bold cyan]{'='*60}[/bold cyan]")
        console.print(f"[bold cyan]{text}[/bold cyan]")
        console.print(f"[bold cyan]{'='*60}[/bold cyan]\n")
    else:
        print(f"\n{'='*60}")
        print(text)
        print(f"{'='*60}\n")


def print_step(num, text):
    if HAS_RICH:
        console.print(f"[bold yellow]Step {num}:[/bold yellow] {text}")
    else:
        print(f"Step {num}: {text}")


def main():
    client = BackendClient.from_env()

    # Intro
    if HAS_RICH:
        console.print(Panel.fit(
            "[bold]MDF Local Demo: Datasets, Streams & Search[/bold]\n\n"
            "This demo creates sample datasets and streams,\n"
            "then shows how to search across all of them.",
            title="Materials Data Facility",
            border_style="blue",
        ))
    else:
        print("=" * 60)
        print("MDF Local Demo: Datasets, Streams & Search")
        print("=" * 60)

    time.sleep(1)

    # Step 1: Create some datasets
    print_header("Creating Sample Datasets")

    datasets = [
        {
            "dc": {
                "titles": [{"title": "Fe-Al Intermetallic Formation Energies"}],
                "creators": [{"creatorName": "Doe, Jane"}, {"creatorName": "Smith, John"}],
                "publisher": "Materials Data Facility",
                "publicationYear": "2024",
                "descriptions": [{"description": "DFT calculations of iron-aluminum intermetallic compounds"}],
                "subjects": [{"subject": "DFT"}, {"subject": "intermetallics"}, {"subject": "iron"}, {"subject": "aluminum"}],
            },
            "data_sources": ["globus://endpoint/fe-al-data/"],
            "test": True,
        },
        {
            "dc": {
                "titles": [{"title": "Perovskite Solar Cell Efficiency Database"}],
                "creators": [{"creatorName": "Chen, Wei"}],
                "publisher": "Materials Data Facility",
                "publicationYear": "2024",
                "descriptions": [{"description": "Experimental efficiency measurements for perovskite solar cells with XRD characterization"}],
                "subjects": [{"subject": "perovskite"}, {"subject": "solar cells"}, {"subject": "XRD"}, {"subject": "efficiency"}],
            },
            "data_sources": ["globus://endpoint/perovskite-solar/"],
            "test": True,
        },
        {
            "dc": {
                "titles": [{"title": "High-Entropy Alloy Mechanical Properties"}],
                "creators": [{"creatorName": "Kumar, Raj"}, {"creatorName": "Williams, Emma"}],
                "publisher": "Materials Data Facility",
                "publicationYear": "2024",
                "descriptions": [{"description": "Tensile testing and microstructure analysis of HEA compositions"}],
                "subjects": [{"subject": "high-entropy alloys"}, {"subject": "mechanical properties"}, {"subject": "tensile testing"}],
            },
            "data_sources": ["globus://endpoint/hea-data/"],
            "test": True,
        },
    ]

    for i, dataset in enumerate(datasets, 1):
        print_step(i, f"Submitting: {dataset['dc']['titles'][0]['title']}")
        result = client.submit(dataset)
        if HAS_RICH:
            console.print(f"   [green]Created:[/green] {result.get('source_id')} v{result.get('version')}")
        else:
            print(f"   Created: {result.get('source_id')} v{result.get('version')}")
        time.sleep(0.3)

    # Step 2: Create some streams
    print_header("Creating Lab Streams")

    streams = [
        {"title": "Argonne XRD Beamline - Jan 2024", "lab_id": "argonne-11-id-c"},
        {"title": "NIST Neutron Diffraction Run", "lab_id": "nist-ncnr"},
        {"title": "Stanford SLAC LCLS XRD Campaign", "lab_id": "slac-lcls"},
    ]

    stream_ids = []
    for i, stream in enumerate(streams, 1):
        print_step(i, f"Creating stream: {stream['title']}")
        result = client.stream_create(stream["title"], lab_id=stream["lab_id"])
        stream_id = result.get("stream_id")
        stream_ids.append(stream_id)
        if HAS_RICH:
            console.print(f"   [green]Created:[/green] {stream_id}")
        else:
            print(f"   Created: {stream_id}")

        # Add some files
        client.stream_append(stream_id, file_count=25, total_bytes=1024*1024*100)
        time.sleep(0.2)

    # Step 3: Demo search!
    print_header("Searching the Repository")

    queries = [
        ("perovskite", "all"),
        ("XRD", "all"),
        ("iron", "datasets"),
        ("argonne", "streams"),
        ("DFT intermetallics", "all"),
    ]

    for query, search_type in queries:
        if HAS_RICH:
            console.print(f"\n[bold]Searching:[/bold] [cyan]'{query}'[/cyan] (type: {search_type})")
        else:
            print(f"\nSearching: '{query}' (type: {search_type})")

        result = client.search(query, search_type=search_type)

        if result.get("results"):
            if HAS_RICH:
                table = Table(show_header=True, header_style="bold")
                table.add_column("Type", width=8)
                table.add_column("Title", width=40)
                table.add_column("ID")

                for item in result["results"]:
                    if item.get("type") == "dataset":
                        table.add_row(
                            "[blue]dataset[/blue]",
                            item.get("title", "")[:40],
                            item.get("source_id", ""),
                        )
                    else:
                        table.add_row(
                            "[green]stream[/green]",
                            item.get("title", "")[:40],
                            item.get("stream_id", ""),
                        )
                console.print(table)
            else:
                for item in result["results"]:
                    print(f"  - [{item.get('type')}] {item.get('title', '')[:40]}")
        else:
            print("  (no results)")

        time.sleep(0.5)

    # Summary
    print_header("Demo Complete!")

    if HAS_RICH:
        console.print(Panel.fit(
            "[bold green]What we demonstrated:[/bold green]\n\n"
            "• Created 3 datasets with rich metadata\n"
            "• Created 3 streaming lab data feeds\n"
            "• Searched across all content by keyword\n"
            "• Filtered by type (datasets vs streams)\n\n"
            "[dim]Try it yourself:[/dim]\n"
            "  mdf search 'perovskite'\n"
            "  mdf search 'XRD' --type streams\n"
            "  mdf backend search 'iron oxide'",
            title="Summary",
            border_style="green",
        ))
    else:
        print("What we demonstrated:")
        print("• Created 3 datasets with rich metadata")
        print("• Created 3 streaming lab data feeds")
        print("• Searched across all content by keyword")
        print("\nTry: mdf search 'perovskite'")

    client.close()


if __name__ == "__main__":
    main()
