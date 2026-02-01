#!/usr/bin/env python3
"""Test DOI minting with the MDF v2 backend.

This script tests:
1. Creating a stream
2. Appending files
3. Closing with DOI minting
"""

import json
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

BASE_URL = "http://localhost:8080"


def test_doi_minting():
    """Test the full stream lifecycle with DOI minting."""

    console.print(Panel.fit("[bold cyan]MDF v2 DOI Minting Test[/bold cyan]"))

    # 1. Create a stream
    console.print("\n[bold]1. Creating stream...[/bold]")
    response = requests.post(f"{BASE_URL}/stream/create", json={
        "title": "Test Dataset for DOI Minting",
        "lab_id": "test-lab",
        "metadata": {
            "description": "A test dataset to verify DOI minting works correctly",
            "authors": [
                {"given_name": "Jane", "family_name": "Doe", "affiliation": "Test University"},
                {"given_name": "John", "family_name": "Smith"}
            ],
            "keywords": ["test", "materials", "simulation"]
        }
    })

    if response.status_code != 200:
        console.print(f"[red]Failed to create stream: {response.text}[/red]")
        return

    data = response.json()
    stream_id = data["stream_id"]
    console.print(f"  Stream created: [green]{stream_id}[/green]")

    # 2. Upload some test files
    console.print("\n[bold]2. Uploading test files...[/bold]")

    import base64

    test_files = [
        ("data.csv", "element,energy,bandgap\nFe,100.5,2.1\nAl,50.2,1.8\nCu,75.3,0.0", "text/csv"),
        ("parameters.json", json.dumps({"method": "DFT", "basis": "PBE", "cutoff": 500}), "application/json"),
        ("notes.txt", "Experimental notes for the test dataset.\nAll calculations converged successfully.", "text/plain"),
    ]

    for filename, content, content_type in test_files:
        response = requests.post(
            f"{BASE_URL}/stream/{stream_id}/upload",
            json={
                "filename": filename,
                "content_base64": base64.b64encode(content.encode()).decode(),
                "content_type": content_type,
            },
        )
        if response.status_code == 200:
            console.print(f"  Uploaded: [green]{filename}[/green]")
        else:
            console.print(f"  [red]Failed to upload {filename}: {response.text}[/red]")

    # 3. Check stream status
    console.print("\n[bold]3. Stream status before close...[/bold]")
    response = requests.get(f"{BASE_URL}/stream/{stream_id}")
    if response.status_code == 200:
        data = response.json()
        stream = data.get("stream", data)  # Handle both wrapped and unwrapped responses
        table = Table(show_header=False, box=None)
        table.add_column("Field", style="cyan")
        table.add_column("Value")
        table.add_row("Status", stream.get("status", "unknown"))
        table.add_row("File Count", str(stream.get("file_count", 0)))
        table.add_row("Total Bytes", str(stream.get("total_bytes", 0)))
        console.print(table)

    # 4. Close with DOI minting
    console.print("\n[bold]4. Closing stream with DOI minting...[/bold]")
    response = requests.post(f"{BASE_URL}/stream/{stream_id}/close", json={
        "mint_doi": True,
        "title": "Test Materials Dataset v1.0",
        "description": "DFT calculations for Fe, Al, and Cu with band gap analysis",
        "authors": [
            {"given_name": "Jane", "family_name": "Doe", "affiliation": "Test University"},
            {"given_name": "John", "family_name": "Smith", "affiliation": "Research Lab"}
        ],
        "keywords": ["DFT", "band gap", "materials science"],
        "license": "CC-BY-4.0"
    })

    if response.status_code != 200:
        console.print(f"[red]Failed to close stream: {response.text}[/red]")
        return

    result = response.json()

    console.print("\n[bold green]Stream closed successfully![/bold green]")

    # Display DOI result
    if "doi" in result:
        doi_info = result["doi"]
        console.print(Panel.fit(
            f"[bold]DOI Minting Result[/bold]\n\n"
            f"Success: [green]{doi_info.get('success', False)}[/green]\n"
            f"DOI: [cyan]{doi_info.get('doi', 'N/A')}[/cyan]\n"
            f"URL: {doi_info.get('url', 'N/A')}\n"
            f"State: {doi_info.get('state', 'N/A')}\n"
            f"Mock: {doi_info.get('mock', False)}",
            title="DOI Info"
        ))

    # 5. Verify stream metadata was updated
    console.print("\n[bold]5. Verifying stream metadata update...[/bold]")
    response = requests.get(f"{BASE_URL}/stream/{stream_id}")
    if response.status_code == 200:
        data = response.json()
        stream = data.get("stream", data)  # Handle both wrapped and unwrapped responses
        metadata = stream.get("metadata", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}

        table = Table(show_header=False, box=None)
        table.add_column("Field", style="cyan")
        table.add_column("Value")
        table.add_row("Status", stream.get("status", "unknown"))
        table.add_row("DOI", str(metadata.get("doi", "N/A")))
        table.add_row("Published At", str(metadata.get("published_at", "N/A")))
        console.print(table)

    console.print("\n[bold green]DOI minting test complete![/bold green]")
    return result


if __name__ == "__main__":
    # Check if server is running
    try:
        requests.get(f"{BASE_URL}/health", timeout=2)
    except requests.exceptions.ConnectionError:
        console.print("[yellow]Local server not running. Starting it...[/yellow]")
        console.print("Run: [cyan]./deploy.sh local[/cyan]")
        console.print("Then run this test again.")
        sys.exit(1)

    test_doi_minting()
