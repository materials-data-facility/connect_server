#!/usr/bin/env python3
"""Test the curation workflow for MDF v2.

This script tests:
1. Creating a submission pending curation
2. Listing pending submissions
3. Viewing submission details
4. Approving with DOI minting
5. Rejecting with reason
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

BASE_URL = "http://localhost:8080"


def create_test_submission(source_id: str, title: str, status: str = "pending_curation"):
    """Create a test submission directly in the database."""
    from v2.store import get_store

    store = get_store()
    now = datetime.now(timezone.utc).isoformat()

    record = {
        "source_id": source_id,
        "version": "1.0",
        "versioned_source_id": f"{source_id}_v1.0",
        "user_id": "test-user-123",
        "user_email": "researcher@example.edu",
        "organization": "Test University",
        "status": status,
        "dataset_mdata": json.dumps({
            "dc": {
                "titles": [{"title": title}],
                "creators": [
                    {"name": "Jane Researcher", "affiliation": "Test University"},
                    {"name": "John Scientist", "affiliation": "Research Lab"}
                ],
                "publisher": "Materials Data Facility",
                "publicationYear": 2026,
                "descriptions": [{"description": "A test dataset for curation workflow", "descriptionType": "Abstract"}],
                "subjects": [{"subject": "materials science"}, {"subject": "DFT"}]
            },
            "mdf": {
                "source_id": source_id,
                "versioned_source_id": f"{source_id}_v1.0",
                "organization": "Test University"
            }
        }),
        "test": 1,
        "created_at": now,
        "updated_at": now,
    }

    store.put_submission(record)
    return record


def test_curation_workflow():
    """Test the full curation workflow."""

    console.print(Panel.fit("[bold cyan]MDF v2 Curation Workflow Test[/bold cyan]"))

    # 1. Create test submissions
    console.print("\n[bold]1. Creating test submissions...[/bold]")

    # Clear any existing test data
    os.environ.setdefault("STORE_BACKEND", "sqlite")
    os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_connect_v2.db")

    submissions = [
        create_test_submission("test_fe_al_dft", "Iron-Aluminum DFT Calculations"),
        create_test_submission("test_perovskite_xrd", "Perovskite XRD Measurements"),
        create_test_submission("test_polymer_md", "Polymer MD Simulations"),
    ]

    for sub in submissions:
        console.print(f"  Created: [green]{sub['source_id']}[/green] - {sub['status']}")

    # 2. List pending submissions
    console.print("\n[bold]2. Listing pending submissions...[/bold]")
    response = requests.get(f"{BASE_URL}/curation/pending")

    if response.status_code != 200:
        console.print(f"[red]Failed: {response.text}[/red]")
        return

    data = response.json()
    console.print(f"  Found [cyan]{data.get('pending_count', 0)}[/cyan] pending submissions")

    if data.get("submissions"):
        table = Table(title="Pending Curation")
        table.add_column("Source ID", style="cyan")
        table.add_column("Title")
        table.add_column("Organization")
        table.add_column("Submitted")

        for sub in data["submissions"]:
            table.add_row(
                sub.get("source_id", ""),
                sub.get("title", "")[:40],
                sub.get("organization", ""),
                sub.get("submitted_at", "")[:19]
            )
        console.print(table)

    # 3. Get details for one submission
    console.print("\n[bold]3. Getting submission details...[/bold]")
    response = requests.get(f"{BASE_URL}/curation/test_fe_al_dft")

    if response.status_code == 200:
        data = response.json()
        sub = data.get("submission", {})
        console.print(f"  Source ID: [cyan]{sub.get('source_id')}[/cyan]")
        console.print(f"  Status: [yellow]{sub.get('status')}[/yellow]")
        console.print(f"  Can Approve: {data.get('can_approve')}")
        console.print(f"  Can Reject: {data.get('can_reject')}")
    else:
        console.print(f"[red]Failed: {response.text}[/red]")

    # 4. Approve one submission
    console.print("\n[bold]4. Approving submission with DOI...[/bold]")
    response = requests.post(f"{BASE_URL}/curation/test_fe_al_dft/approve", json={
        "notes": "Looks good! Metadata is complete.",
        "mint_doi": True,
    })

    if response.status_code == 200:
        data = response.json()
        console.print(f"  Status: [green]{data.get('status')}[/green]")
        console.print(f"  Approved by: {data.get('approved_by')}")

        if data.get("doi"):
            doi_info = data["doi"]
            console.print(f"  DOI Success: {doi_info.get('success')}")
            console.print(f"  DOI: [cyan]{doi_info.get('doi')}[/cyan]")
    else:
        console.print(f"[red]Failed: {response.text}[/red]")

    # 5. Reject another submission
    console.print("\n[bold]5. Rejecting submission...[/bold]")
    response = requests.post(f"{BASE_URL}/curation/test_polymer_md/reject", json={
        "reason": "Missing required metadata: authors need ORCID identifiers",
        "suggestions": "Please add ORCID IDs for all authors and resubmit",
    })

    if response.status_code == 200:
        data = response.json()
        console.print(f"  Status: [red]{data.get('status')}[/red]")
        console.print(f"  Rejected by: {data.get('rejected_by')}")
        console.print(f"  Reason: {data.get('reason')}")
    else:
        console.print(f"[red]Failed: {response.text}[/red]")

    # 6. List pending again (should be fewer)
    console.print("\n[bold]6. Checking remaining pending submissions...[/bold]")
    response = requests.get(f"{BASE_URL}/curation/pending")

    if response.status_code == 200:
        data = response.json()
        console.print(f"  Remaining pending: [cyan]{data.get('pending_count', 0)}[/cyan]")
    else:
        console.print(f"[red]Failed: {response.text}[/red]")

    # 7. Verify approved submission has DOI
    console.print("\n[bold]7. Verifying approved submission...[/bold]")
    response = requests.get(f"{BASE_URL}/curation/test_fe_al_dft")

    if response.status_code == 200:
        data = response.json()
        sub = data.get("submission", {})
        console.print(f"  Status: [green]{sub.get('status')}[/green]")
        console.print(f"  DOI: [cyan]{sub.get('doi', 'N/A')}[/cyan]")
        console.print(f"  Published At: {sub.get('published_at', 'N/A')}")

        history = data.get("curation_history", [])
        if history:
            console.print(f"  Curation History: {len(history)} action(s)")
            for h in history:
                console.print(f"    - {h.get('action')} by {h.get('curator_id')} at {h.get('timestamp', '')[:19]}")
    else:
        console.print(f"[red]Failed: {response.text}[/red]")

    console.print("\n[bold green]Curation workflow test complete![/bold green]")


if __name__ == "__main__":
    # Check if server is running
    try:
        requests.get(f"{BASE_URL}/health", timeout=2)
    except requests.exceptions.ConnectionError:
        console.print("[yellow]Local server not running.[/yellow]")
        console.print("Run: [cyan]./deploy.sh local[/cyan]")
        sys.exit(1)

    # Allow all users to curate in local dev mode
    os.environ["ALLOW_ALL_CURATORS"] = "true"

    test_curation_workflow()
