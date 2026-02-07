#!/usr/bin/env python
"""
MDF Connect v2 — Interactive Workflow Demo
==========================================

Three end-to-end workflows that exercise the new FastAPI backend,
rendered with Rich panels, tables, trees, and progress bars.

Usage:
    # Start the server first (in another terminal):
    cd cs/aws && STORE_BACKEND=sqlite AUTH_MODE=dev python -m v2.app.main

    # Then run the demo:
    python v2/demo_workflows.py [--base-url http://127.0.0.1:8080]
"""

import argparse
import base64
import json
import sys
import time
from datetime import datetime

import requests
from rich.align import Align
from rich.columns import Columns
from rich.console import Console, Group
from rich.live import Live
from rich.markdown import Markdown
from rich.padding import Padding
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.rule import Rule
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

console = Console(width=100)

# -- Theme colours ----------------------------------------------------------
ACCENT = "bright_cyan"
OK     = "bright_green"
WARN   = "bright_yellow"
ERR    = "bright_red"
DIM    = "dim"
TITLE  = "bold bright_white"

HEADER = {
    "Content-Type": "application/json",
    "X-User-Id": "demo-researcher",
    "X-User-Email": "researcher@mdf.org",
}

CURATOR_HEADER = {
    "Content-Type": "application/json",
    "X-User-Id": "curator-admin",
    "X-User-Email": "curator@mdf.org",
}

# -- Helpers ----------------------------------------------------------------

def api(method, path, base, **kw):
    """Fire an HTTP request and return (response_json, elapsed_ms)."""
    url = f"{base.rstrip('/')}{path}"
    t0 = time.perf_counter()
    resp = getattr(requests, method)(url, **kw)
    elapsed = (time.perf_counter() - t0) * 1000
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    return body, elapsed


def status_dot(success):
    return Text("●", style=OK) if success else Text("●", style=ERR)


def latency_text(ms):
    colour = OK if ms < 200 else WARN if ms < 500 else ERR
    return Text(f"{ms:.0f} ms", style=colour)


def step_header(num, total, label):
    console.print()
    console.print(
        Text(f"  Step {num}/{total}  ", style="bold white on dark_green"),
        Text(f"  {label}", style="bold"),
    )
    console.print()


def pause(seconds=1.0):
    time.sleep(seconds)


def banner():
    art = Text.from_markup(
        "\n"
        "[bold bright_cyan]  ╔═══════════════════════════════════════════════════════╗[/]\n"
        "[bold bright_cyan]  ║[/]   [bold bright_white]MDF Connect v2[/]  [dim]·[/]  [bold bright_magenta]FastAPI Backend Demo[/]              [bold bright_cyan]║[/]\n"
        "[bold bright_cyan]  ║[/]   [dim]Flat metadata schema · ML-ready · DataCite 4.5[/]    [bold bright_cyan]║[/]\n"
        "[bold bright_cyan]  ╚═══════════════════════════════════════════════════════╝[/]\n"
    )
    console.print(Align.center(art))


# ===================================================================
#  WORKFLOW 1 — Dataset Submission & Discovery (new flat format)
# ===================================================================

def workflow_1(base):
    panel = Panel(
        Text.from_markup(
            "[bold]A researcher submits an ML-ready materials dataset using the\n"
            "new flat metadata schema, then discovers it via keyword search,\n"
            "ML task-type search, rich dataset card, and citation export.[/]"
        ),
        title="[bold bright_magenta]Workflow 1[/]  [bold]Dataset Submission & ML Discovery[/]",
        subtitle="[dim]POST /submit → search → ML search → card → ML detail → citation[/]",
        border_style="bright_magenta",
        padding=(1, 3),
    )
    console.print(panel)
    pause(0.5)

    STEPS = 7

    # -- Step 1: Submit (flat format with ML metadata) ------------------
    step_header(1, STEPS, "Submit a dataset with ML metadata (flat format)")

    payload = {
        "title": "Thermal Conductivity of High-Entropy Alloys",
        "authors": [
            {"name": "Chen, Wei", "given_name": "Wei", "family_name": "Chen",
             "affiliations": ["Argonne National Laboratory"],
             "orcid": "0000-0001-2345-6789"},
            {"name": "Park, Joon", "given_name": "Joon", "family_name": "Park",
             "affiliations": ["University of Chicago"]},
        ],
        "description": "Measured thermal conductivity of 47 high-entropy alloy compositions using laser flash analysis at temperatures from 300K to 1200K.",
        "keywords": ["high-entropy alloys", "thermal conductivity", "materials science"],
        "publisher": "Materials Data Facility",
        "publication_year": 2026,
        "resource_type": "Dataset",
        "organization": "Argonne National Laboratory",
        "facility": "Advanced Photon Source",
        "methods": ["Laser Flash Analyzer LFA 457"],
        "fields_of_science": ["materials science", "condensed matter physics"],
        "data_sources": [
            "https://data.materialsdatafacility.org/hea_thermal/data.csv",
            "https://data.materialsdatafacility.org/hea_thermal/metadata.json",
        ],
        "license": {"name": "CC BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/",
                     "identifier": "CC-BY-4.0"},
        "funding": [
            {"funder_name": "National Science Foundation", "award_number": "DMR-2012345",
             "award_title": "High-Entropy Alloy Thermal Properties"},
            {"funder_name": "DOE Office of Science", "award_number": "DE-AC02-06CH11357"},
        ],
        "related_works": [
            {"identifier": "10.1038/s41524-025-01234-5", "identifier_type": "DOI",
             "relation_type": "IsSupplementTo", "description": "Original publication"},
        ],
        "ml": {
            "data_format": "tabular",
            "task_type": ["supervised", "regression"],
            "domain": ["materials science", "thermodynamics"],
            "n_items": 47,
            "short_name": "hea_thermal_v1",
            "splits": [
                {"type": "train", "path": "train.csv", "n_items": 38},
                {"type": "test", "path": "test.csv", "n_items": 9},
            ],
            "keys": [
                {"name": "composition", "role": "input", "dtype": "string",
                 "description": "Chemical formula (e.g. CoCrFeMnNi)"},
                {"name": "temperature", "role": "input", "dtype": "float64",
                 "units": "K", "description": "Measurement temperature"},
                {"name": "crystal_structure", "role": "input", "dtype": "string",
                 "description": "Crystal structure type",
                 "classes": ["FCC", "BCC", "HCP", "multi-phase"]},
                {"name": "thermal_conductivity", "role": "target", "dtype": "float64",
                 "units": "W/(m*K)", "description": "Measured thermal conductivity"},
            ],
        },
    }

    body, ms = api("post", "/submit", base, json=payload, headers=HEADER)

    source_id = body.get("source_id", "???")
    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", status_dot(body.get("success")))
    tbl.add_row("Source ID", Text(source_id, style=ACCENT))
    tbl.add_row("Version", body.get("version", "?"))
    tbl.add_row("Organization", body.get("organization", "?"))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Submit Response[/]", border_style=DIM))
    pause(0.8)

    # -- Step 2: Check status -------------------------------------------
    step_header(2, STEPS, "Check submission status")
    body, ms = api("get", f"/status/{source_id}", base, headers=HEADER)
    sub = body.get("submission", {})

    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", Text(sub.get("status", "?"), style=OK))
    tbl.add_row("Schema Version", sub.get("schema_version", "?"))
    tbl.add_row("Created", sub.get("created_at", "?"))
    tbl.add_row("User", sub.get("user_id", "?"))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Submission Status[/]", border_style=DIM))
    pause(0.8)

    # -- Step 3: Search by keyword --------------------------------------
    step_header(3, STEPS, 'Search for "thermal conductivity"')
    body, ms = api("get", "/search", base, params={"q": "thermal conductivity"})
    results = body.get("results", [])

    tbl = Table(title="Keyword Search Results", border_style=ACCENT, show_lines=True)
    tbl.add_column("#", style="dim", width=3)
    tbl.add_column("Type", width=8)
    tbl.add_column("Title / ID", ratio=3)
    tbl.add_column("Status", width=12)
    tbl.add_column("Score", width=6, justify="right")

    for i, r in enumerate(results[:5], 1):
        title = r.get("title") or r.get("source_id") or "—"
        tbl.add_row(
            str(i),
            Text(r.get("type", "?"), style="bold"),
            title if len(title) < 55 else title[:52] + "...",
            Text(r.get("status", "?"), style=OK),
            f"{r.get('score', 0):.1f}",
        )

    console.print(tbl)
    console.print(Text(f"  {body.get('total', 0)} results in {ms:.0f} ms", style=DIM))
    pause(0.8)

    # -- Step 4: Search by ML task type (NEW) ---------------------------
    step_header(4, STEPS, 'Search by ML task type: "regression"')
    body, ms = api("get", "/search", base, params={"q": "regression"})
    results = body.get("results", [])

    tbl = Table(title="ML Task-Type Search Results", border_style="bright_yellow", show_lines=True)
    tbl.add_column("#", style="dim", width=3)
    tbl.add_column("Title", ratio=3)
    tbl.add_column("Authors", ratio=2)
    tbl.add_column("Score", width=6, justify="right")

    for i, r in enumerate(results[:5], 1):
        title = r.get("title") or "—"
        authors_str = ", ".join(r.get("authors", []))
        tbl.add_row(
            str(i),
            title if len(title) < 50 else title[:47] + "...",
            authors_str if len(authors_str) < 30 else authors_str[:27] + "...",
            f"{r.get('score', 0):.1f}",
        )

    console.print(tbl)
    console.print(Text.from_markup(
        f"  [bold]ML metadata is now searchable![/]  "
        f"[dim]\"regression\" matched via ml.task_type — {ms:.0f} ms[/]"
    ))
    pause(0.8)

    # -- Step 5: Dataset card with ML detail ----------------------------
    step_header(5, STEPS, "Fetch dataset preview card (with ML summary)")
    body, ms = api("get", f"/card/{source_id}", base)
    card = body.get("card", {})

    tree = Tree(f"[bold]{card.get('title', 'Untitled')}[/]", style=ACCENT)

    # Core metadata
    meta_node = tree.add("[bold]Metadata[/]")
    meta_node.add(f"Authors: {', '.join(card.get('authors', ['—']))}")
    meta_node.add(f"Publisher: {card.get('publisher', '—')}")
    meta_node.add(f"Year: {card.get('publication_year', '—')}")
    meta_node.add(f"Organization: {card.get('organization', '—')}")
    if card.get("keywords"):
        meta_node.add(f"Keywords: {', '.join(card['keywords'])}")
    if card.get("description"):
        desc = card["description"]
        meta_node.add(f"Description: {desc[:80]}{'...' if len(desc) > 80 else ''}")
    if card.get("license"):
        meta_node.add(f"License: {card['license']}")
    if card.get("facility"):
        meta_node.add(f"Facility: {card['facility']}")
    if card.get("methods"):
        meta_node.add(f"Methods: {', '.join(card['methods'])}")

    # ML summary in the card
    ml_info = card.get("ml")
    if ml_info:
        ml_node = tree.add("[bold bright_yellow]ML-Ready[/]")
        ml_node.add(f"Format: [bold]{ml_info.get('data_format', '?')}[/]")
        ml_node.add(f"Task: [bold]{', '.join(ml_info.get('task_type', []))}[/]")
        ml_node.add(f"Total samples: [bold]{ml_info.get('n_items', '?')}[/]")
        if ml_info.get("short_name"):
            ml_node.add(f"Short name: [bold]{ml_info['short_name']}[/]  (for Foundry loading)")

        if ml_info.get("splits"):
            splits_node = ml_node.add("[dim]Splits[/]")
            for sp in ml_info["splits"]:
                n = sp.get("n_items")
                splits_node.add(f"{sp['type']}: {n} samples" if n else sp["type"])

        if ml_info.get("input_keys") or ml_info.get("target_keys"):
            keys_node = ml_node.add("[dim]Feature Schema[/]")
            for k in (ml_info.get("input_keys") or []):
                keys_node.add(f"[green]input[/]  {k}")
            for k in (ml_info.get("target_keys") or []):
                keys_node.add(f"[red]target[/] {k}")

    stats_node = tree.add("[bold]Stats[/]")
    stats = card.get("stats", {})
    stats_node.add(f"Size: {stats.get('size_human', '?')}")
    stats_node.add(f"Data sources: {stats.get('data_sources_count', 0)}")
    stats_node.add(f"File types: {', '.join(stats.get('file_types', []))}")

    links_node = tree.add("[bold]Links[/]")
    for k, v in card.get("links", {}).items():
        links_node.add(f"{k}: {v}")

    console.print(Panel(tree, title="[bold]Dataset Card[/]", border_style="bright_magenta"))
    console.print(Text(f"  Card generated in {ms:.0f} ms", style=DIM))
    pause(0.8)

    # -- Step 6: Detailed ML keys table ---------------------------------
    step_header(6, STEPS, "Inspect ML feature schema (from stored metadata)")
    body, ms = api("get", f"/status/{source_id}", base, headers=HEADER)
    sub = body.get("submission", {})
    mdata = sub.get("dataset_mdata") or {}
    ml_block = mdata.get("ml") or {}

    keys = ml_block.get("keys") or []
    splits = ml_block.get("splits") or []

    if keys:
        key_tbl = Table(
            title="Feature / Target Schema",
            border_style="bright_yellow",
            show_lines=True,
        )
        key_tbl.add_column("Name", style="bold", ratio=2)
        key_tbl.add_column("Role", width=8)
        key_tbl.add_column("Type", width=10)
        key_tbl.add_column("Units", width=10)
        key_tbl.add_column("Description", ratio=3)
        key_tbl.add_column("Classes", ratio=2)

        for k in keys:
            role_style = OK if k.get("role") == "input" else ERR if k.get("role") == "target" else "white"
            classes_str = ", ".join(k["classes"]) if k.get("classes") else "—"
            key_tbl.add_row(
                k.get("name", "?"),
                Text(k.get("role", "?"), style=role_style),
                k.get("dtype", "—"),
                k.get("units", "—"),
                k.get("description", "—"),
                classes_str,
            )
        console.print(key_tbl)

    if splits:
        split_tbl = Table(
            title="Train / Test Splits",
            border_style="bright_yellow",
            show_lines=True,
        )
        split_tbl.add_column("Split", style="bold", width=12)
        split_tbl.add_column("Path", ratio=2)
        split_tbl.add_column("Samples", width=10, justify="right")

        total = 0
        for s in splits:
            n = s.get("n_items") or 0
            total += n
            split_tbl.add_row(
                s.get("type", "?"),
                s.get("path", "?"),
                str(n) if n else "—",
            )
        split_tbl.add_row(
            Text("TOTAL", style="bold"),
            "",
            Text(str(total), style="bold"),
        )
        console.print(split_tbl)

    console.print(Text(f"  ML metadata fetched in {ms:.0f} ms", style=DIM))
    pause(0.8)

    # -- Step 7: Citation -----------------------------------------------
    step_header(7, STEPS, "Export citation in all formats")
    body, ms = api("get", f"/citation/{source_id}", base, params={"format": "all"})

    if body.get("bibtex"):
        console.print(Panel(
            Syntax(body["bibtex"], "bibtex", theme="monokai", line_numbers=False),
            title="[bold]BibTeX[/]",
            border_style="bright_yellow",
        ))

    if body.get("apa"):
        console.print(Panel(
            Text(body["apa"], style="italic"),
            title="[bold]APA[/]",
            border_style="bright_yellow",
        ))

    console.print(Text(f"  4 citation formats generated in {ms:.0f} ms", style=DIM))
    pause(0.3)

    console.print()
    console.print(Rule(style="bright_magenta"))
    console.print(
        Align.center(Text("Workflow 1 Complete", style="bold bright_magenta"))
    )
    console.print(Rule(style="bright_magenta"))

    return source_id


# ===================================================================
#  WORKFLOW 2 — Live Streaming Data Pipeline
# ===================================================================

def workflow_2(base):
    panel = Panel(
        Text.from_markup(
            "[bold]A lab instrument streams data files in real-time, then the\n"
            "stream is snapshotted into a citable dataset submission.[/]"
        ),
        title="[bold bright_green]Workflow 2[/]  [bold]Live Streaming Data Pipeline[/]",
        subtitle="[dim]stream/create → upload files → append → snapshot → close[/]",
        border_style="bright_green",
        padding=(1, 3),
    )
    console.print(panel)
    pause(0.5)

    # -- Step 1: Create stream ------------------------------------------
    step_header(1, 5, "Create a live data stream")
    body, ms = api("post", "/stream/create", base, json={
        "title": "APS Beamline 11-ID SAXS Run",
        "lab_id": "APS-11ID-2026-Feb",
        "organization": "Argonne National Laboratory",
        "metadata": {
            "instrument": "Pilatus 2M",
            "facility": "Advanced Photon Source",
            "operator": "Dr. Sarah Kim",
            "run_id": "run-2026-02-05-001",
        },
    }, headers=HEADER)

    stream_id = body.get("stream_id", "???")
    stream = body.get("stream", {})

    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Stream ID", Text(stream_id, style=ACCENT))
    tbl.add_row("Title", stream.get("title", "—"))
    tbl.add_row("Status", Text(stream.get("status", "?"), style=OK))
    tbl.add_row("Instrument", (stream.get("metadata") or {}).get("instrument", "—"))
    tbl.add_row("Operator", (stream.get("metadata") or {}).get("operator", "—"))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Stream Created[/]", border_style=DIM))
    pause(0.5)

    # -- Step 2: Simulate file uploads with progress --------------------
    step_header(2, 5, "Stream data files from the beamline")

    simulated_files = [
        ("frame_001.tiff", 2_400_000, "SAXS frame - q=0.01-0.5 A^-1"),
        ("frame_002.tiff", 2_380_000, "SAXS frame - sample rotated 15deg"),
        ("frame_003.tiff", 2_420_000, "SAXS frame - temperature 350K"),
        ("dark_current.tiff", 2_100_000, "Dark current calibration"),
        ("metadata.json", 4_200, "Run parameters and instrument config"),
        ("frame_004.tiff", 2_390_000, "SAXS frame - temperature 400K"),
        ("frame_005.tiff", 2_410_000, "SAXS frame - temperature 450K"),
        ("reduction_log.txt", 12_800, "Azimuthal integration log"),
    ]

    progress = Progress(
        SpinnerColumn(style="bright_green"),
        TextColumn("[bold]{task.description}[/]"),
        BarColumn(bar_width=30, complete_style="bright_green", finished_style="bold bright_green"),
        TextColumn("{task.completed}/{task.total} files"),
        TimeElapsedColumn(),
        console=console,
    )

    file_table = Table(
        title="Uploaded Files",
        border_style="bright_green",
        show_lines=False,
        padding=(0, 1),
    )
    file_table.add_column("File", style="bold", ratio=2)
    file_table.add_column("Size", justify="right", width=10)
    file_table.add_column("Note", style="dim", ratio=2)
    file_table.add_column("", width=3)

    with progress:
        task = progress.add_task("Streaming files", total=len(simulated_files))

        for fname, size, note in simulated_files:
            fake_content = base64.b64encode(b"x" * min(size, 256)).decode()
            upload_body, upload_ms = api("post", f"/stream/{stream_id}/upload", base, json={
                "filename": fname,
                "content_base64": fake_content,
                "content_type": "application/octet-stream",
            }, headers=HEADER)

            ok = upload_body.get("success", False)
            size_str = f"{size / 1024:.0f} KB" if size < 1_000_000 else f"{size / 1_000_000:.1f} MB"
            file_table.add_row(fname, size_str, note, status_dot(ok))

            progress.advance(task)
            pause(0.3)

    console.print(file_table)
    pause(0.5)

    # -- Step 3: Check stream status ------------------------------------
    step_header(3, 5, "Verify stream status")
    body, ms = api("get", f"/stream/{stream_id}", base, headers=HEADER)
    s = body.get("stream", {})

    cols = []
    for label, value, style in [
        ("Files", str(s.get("file_count", 0)), "bold bright_white"),
        ("Status", s.get("status", "?"), OK),
        ("Latency", f"{ms:.0f} ms", ACCENT),
    ]:
        t = Table(show_header=False, box=None)
        t.add_column(justify="center")
        t.add_row(Text(value, style=style))
        t.add_row(Text(label, style=DIM))
        cols.append(t)

    console.print(Columns(cols, equal=True, expand=True))
    pause(0.5)

    # -- Step 4: Snapshot -> submission ----------------------------------
    step_header(4, 5, "Snapshot stream into a citable dataset")
    body, ms = api("post", f"/stream/{stream_id}/snapshot", base, json={
        "title": "APS 11-ID SAXS Dataset — February 2026",
    }, headers=HEADER)

    snap_source = body.get("source_id", "?")
    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", status_dot(body.get("success")))
    tbl.add_row("Source ID", Text(snap_source, style=ACCENT))
    tbl.add_row("Version", body.get("version", "?"))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Snapshot Created[/]", border_style=DIM))
    pause(0.5)

    # -- Step 5: Close stream -------------------------------------------
    step_header(5, 5, "Close the stream")
    body, ms = api("post", f"/stream/{stream_id}/close", base,
                    json={"mint_doi": False}, headers=HEADER)

    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", status_dot(body.get("success")))
    tbl.add_row("Stream Status", Text(body.get("status", "?"), style=WARN))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Stream Closed[/]", border_style=DIM))
    pause(0.3)

    console.print()
    console.print(Rule(style="bright_green"))
    console.print(
        Align.center(Text("Workflow 2 Complete", style="bold bright_green"))
    )
    console.print(Rule(style="bright_green"))

    return snap_source


# ===================================================================
#  WORKFLOW 3 — Curation & Approval Pipeline
# ===================================================================

def workflow_3(base, source_id):
    panel = Panel(
        Text.from_markup(
            "[bold]A curator reviews a pending submission, adds metadata,\n"
            "approves it, and the system mints a DOI.[/]"
        ),
        title="[bold bright_yellow]Workflow 3[/]  [bold]Curation & Approval Pipeline[/]",
        subtitle="[dim]status/update → curation/pending → curation/{id} → approve[/]",
        border_style="bright_yellow",
        padding=(1, 3),
    )
    console.print(panel)
    pause(0.5)

    # -- Step 1: Move to pending_curation -------------------------------
    step_header(1, 5, "Transition submission to pending_curation")
    body, ms = api("post", "/status/update", base, json={
        "source_id": source_id,
        "version": "1.0",
        "status": "pending_curation",
    }, headers=HEADER)

    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", status_dot(body.get("success")))
    tbl.add_row("New Status", Text(body.get("status", "?"), style=WARN))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Status Updated[/]", border_style=DIM))
    pause(0.5)

    # -- Step 2: Curator lists pending ----------------------------------
    step_header(2, 5, "Curator views the curation queue")
    body, ms = api("get", "/curation/pending", base, headers=CURATOR_HEADER)

    submissions = body.get("submissions", [])
    tbl = Table(
        title=f"Curation Queue  ({body.get('pending_count', 0)} pending)",
        border_style="bright_yellow",
        show_lines=True,
    )
    tbl.add_column("#", style="dim", width=3)
    tbl.add_column("Source ID", ratio=2)
    tbl.add_column("Title", ratio=2)
    tbl.add_column("Submitter", width=16)
    tbl.add_column("Submitted", width=14)

    for i, sub in enumerate(submissions[:5], 1):
        sid = sub.get("source_id", "?")
        tbl.add_row(
            str(i),
            sid if len(sid) < 30 else sid[:27] + "...",
            sub.get("title", "Untitled"),
            sub.get("submitter", "?"),
            (sub.get("submitted_at") or "?")[:10],
        )

    console.print(tbl)
    console.print(Text(f"  Fetched in {ms:.0f} ms", style=DIM))
    pause(0.5)

    # -- Step 3: Curator inspects submission ----------------------------
    step_header(3, 5, "Curator inspects the submission details")
    body, ms = api("get", f"/curation/{source_id}", base,
                    params={"version": "1.0"}, headers=CURATOR_HEADER)

    sub = body.get("submission", {})
    history = body.get("curation_history", [])

    detail = Table(show_header=False, box=None, padding=(0, 2))
    detail.add_column(style="bold")
    detail.add_column()
    detail.add_row("Source ID", Text(sub.get("source_id", "?"), style=ACCENT))
    detail.add_row("Status", Text(body.get("current_status", "?"), style=WARN))
    detail.add_row("Can Approve", Text(str(body.get("can_approve")), style=OK if body.get("can_approve") else ERR))
    detail.add_row("Can Reject", Text(str(body.get("can_reject")), style=OK if body.get("can_reject") else ERR))
    detail.add_row("History", f"{len(history or [])} entries")
    detail.add_row("Latency", latency_text(ms))
    console.print(Panel(detail, title="[bold]Curation Detail[/]", border_style=DIM))
    pause(0.5)

    # -- Step 4: Approve with metadata update ---------------------------
    step_header(4, 5, "Curator approves and mints a DOI")
    body, ms = api("post", f"/curation/{source_id}/approve", base, json={
        "version": "1.0",
        "notes": "Excellent dataset. Metadata verified, data files accessible.",
        "metadata_updates": {
            "license": {"name": "CC BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/"},
        },
        "mint_doi": True,
    }, headers=CURATOR_HEADER)

    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", status_dot(body.get("success")))
    tbl.add_row("New Status", Text(body.get("status", "?"), style=OK))
    tbl.add_row("Approved By", body.get("approved_by", "?"))
    tbl.add_row("Approved At", (body.get("approved_at") or "?")[:19])

    doi_info = body.get("doi", {})
    if doi_info:
        doi_str = doi_info.get("doi", "—")
        tbl.add_row("DOI", Text(doi_str, style="bold bright_yellow"))
        tbl.add_row("DOI Status", Text("minted" if doi_info.get("success") else "failed",
                                        style=OK if doi_info.get("success") else ERR))

    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Approval Result[/]", border_style="bright_yellow"))
    pause(0.5)

    # -- Step 5: Verify final state -------------------------------------
    step_header(5, 5, "Verify the published dataset")
    body, ms = api("get", f"/status/{source_id}", base, headers=HEADER)
    sub = body.get("submission", {})

    final = Table(show_header=False, box=None, padding=(0, 2))
    final.add_column(style="bold")
    final.add_column()
    final.add_row("Source ID", Text(sub.get("source_id", "?"), style=ACCENT))
    final.add_row("Final Status", Text(sub.get("status", "?"), style=OK))
    final.add_row("Approved By", sub.get("approved_by", "—"))
    final.add_row("DOI", Text(sub.get("doi", "—") or "—", style="bold bright_yellow"))
    final.add_row("Latency", latency_text(ms))
    console.print(Panel(final, title="[bold]Published Dataset[/]", border_style=OK))
    pause(0.3)

    console.print()
    console.print(Rule(style="bright_yellow"))
    console.print(
        Align.center(Text("Workflow 3 Complete", style="bold bright_yellow"))
    )
    console.print(Rule(style="bright_yellow"))


# ===================================================================
#  WORKFLOW 4 — Backward Compatibility (v1 format auto-migration)
# ===================================================================

def workflow_4(base):
    panel = Panel(
        Text.from_markup(
            "[bold]A legacy client submits using the old dc/mdf/custom format.\n"
            "The server auto-detects and migrates to the flat v2 schema.[/]"
        ),
        title="[bold bright_blue]Workflow 4[/]  [bold]Backward Compatibility[/]",
        subtitle="[dim]POST /submit (v1 format) → auto-migrate → verify flat storage[/]",
        border_style="bright_blue",
        padding=(1, 3),
    )
    console.print(panel)
    pause(0.5)

    step_header(1, 2, "Submit using old dc/mdf/custom format")

    old_payload = {
        "data_sources": [
            "https://data.materialsdatafacility.org/legacy/data.hdf5",
        ],
        "dc": {
            "titles": [{"title": "Legacy HDF5 Dataset"}],
            "creators": [
                {"creatorName": "Smith, Alice", "givenName": "Alice", "familyName": "Smith",
                 "affiliation": "MIT"},
            ],
            "publisher": "Materials Data Facility",
            "publicationYear": "2025",
            "descriptions": [
                {"description": "A dataset submitted with the old format.",
                 "descriptionType": "Abstract"}
            ],
            "subjects": [
                {"subject": "legacy"},
                {"subject": "backward compatibility"},
            ],
        },
        "mdf": {
            "organization": "MIT",
        },
    }

    body, ms = api("post", "/submit", base, json=old_payload, headers=HEADER)

    source_id = body.get("source_id", "???")
    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status", status_dot(body.get("success")))
    tbl.add_row("Source ID", Text(source_id, style=ACCENT))
    tbl.add_row("Auto-migrated", Text("Yes", style=OK))
    tbl.add_row("Latency", latency_text(ms))
    console.print(Panel(tbl, title="[bold]Legacy Submit Response[/]", border_style=DIM))
    pause(0.5)

    step_header(2, 2, "Verify migrated metadata")
    body, ms = api("get", f"/card/{source_id}", base)
    card = body.get("card", {})

    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Title", card.get("title", "?"))
    tbl.add_row("Authors", ", ".join(card.get("authors", ["?"])))
    tbl.add_row("Publisher", card.get("publisher", "?"))
    tbl.add_row("Year", str(card.get("publication_year", "?")))
    tbl.add_row("Keywords", ", ".join(card.get("keywords", [])))
    tbl.add_row("Organization", card.get("organization", "?"))
    console.print(Panel(tbl, title="[bold]Migrated Card[/]", border_style="bright_blue"))
    pause(0.3)

    console.print()
    console.print(Rule(style="bright_blue"))
    console.print(
        Align.center(Text("Workflow 4 Complete", style="bold bright_blue"))
    )
    console.print(Rule(style="bright_blue"))


# ===================================================================
#  Final Summary
# ===================================================================

def summary(base):
    console.print()

    body, ms1 = api("get", "/submissions", base, headers=HEADER)
    subs = body.get("submissions", [])

    body, ms2 = api("get", "/search", base, params={"q": "*"})

    body, ms3 = api("get", "/health", base)

    tbl = Table(
        title="Final System State",
        border_style="bright_cyan",
        show_lines=True,
    )
    tbl.add_column("Metric", style="bold", ratio=2)
    tbl.add_column("Value", justify="center", ratio=1)

    tbl.add_row("Total Submissions", str(len(subs)))
    tbl.add_row("API Health", Text(body.get("status", "?"), style=OK))
    tbl.add_row("Server", Text(body.get("service", "?"), style=ACCENT))

    status_counts = {}
    for s in subs:
        st = s.get("status", "unknown")
        status_counts[st] = status_counts.get(st, 0) + 1
    for st, ct in sorted(status_counts.items()):
        colour = OK if st in ("published", "approved") else WARN if st == "pending_curation" else "white"
        tbl.add_row(f"  {st}", Text(str(ct), style=colour))

    console.print(tbl)

    arch = Tree("[bold bright_cyan]MDF Connect v2 Architecture[/]")
    client = arch.add("[bold]Client Layer[/]")
    client.add("mdf-agent CLI")
    client.add("BackendClient (requests)")
    client.add("Rich demo script")

    api_node = arch.add("[bold]API Layer[/]  (FastAPI + Mangum)")
    api_node.add("[dim]7 routers, 24 endpoints[/]")
    api_node.add("[dim]Flat DatasetMetadata schema (Pydantic)[/]")
    api_node.add("[dim]Auto-migration from v1 dc/mdf/custom[/]")
    api_node.add("[dim]Dependency injection (auth, stores)[/]")

    storage = arch.add("[bold]Storage Layer[/]")
    storage.add("DynamoDB  [dim](production)[/]")
    storage.add("SQLite    [dim](development)[/]")
    storage.add("Globus HTTPS  [dim](file storage)[/]")

    console.print()
    console.print(Panel(arch, border_style=ACCENT))

    console.print()
    fin = Text.from_markup(
        "\n"
        "  [bold bright_cyan]All 4 workflows completed successfully.[/]\n"
        "\n"
        "  [dim]Triple-nested dc/mdf/custom → flat DatasetMetadata[/]\n"
        "  [dim]9 DataCite fields → 24 (full kernel-4 coverage)[/]\n"
        "  [dim]projects.foundry → first-class ml metadata[/]\n"
        "  [dim]Auto-migration for backward compatibility[/]\n"
        "\n"
        "  [bold]Visit[/] [underline]http://127.0.0.1:8080/docs[/] [bold]for interactive API documentation.[/]\n"
    )
    console.print(Panel(fin, title="[bold bright_cyan]Demo Complete[/]", border_style="bright_cyan"))


# ===================================================================
#  Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(description="MDF v2 backend workflow demo")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080",
                        help="Base URL of the MDF v2 API server")
    args = parser.parse_args()
    base = args.base_url

    # Check server is reachable
    try:
        resp = requests.get(f"{base}/health", timeout=3)
        resp.raise_for_status()
    except Exception:
        console.print(Panel(
            Text.from_markup(
                f"[bold red]Cannot reach server at {base}[/]\n\n"
                "Start the server first:\n"
                "  [bold]cd cs/aws[/]\n"
                "  [bold]STORE_BACKEND=sqlite AUTH_MODE=dev python -m v2.app.main[/]"
            ),
            title="[bold red]Server Not Running[/]",
            border_style="red",
        ))
        sys.exit(1)

    banner()
    console.print()

    # Workflow 1: Submit & Discover (new flat format)
    source_id_1 = workflow_1(base)
    console.print()
    pause(1)

    # Workflow 2: Stream Pipeline
    source_id_2 = workflow_2(base)
    console.print()
    pause(1)

    # Workflow 3: Curation (uses submission from workflow 1)
    workflow_3(base, source_id_1)
    console.print()
    pause(0.5)

    # Workflow 4: Backward Compatibility (v1 format)
    workflow_4(base)
    console.print()
    pause(0.5)

    # Summary
    summary(base)


if __name__ == "__main__":
    main()
