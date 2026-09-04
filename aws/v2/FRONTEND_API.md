# MDF v2 Frontend API Reference

Complete API contract for building a frontend against the MDF Connect v2 backend. Every endpoint, request shape, and response shape is documented here.

**Base URL (staging):** `https://3xicgt0g7l.execute-api.us-east-1.amazonaws.com/staging`

> **Contract changes (2026-07-30 hardening):** `POST /embed` and `GET /search/semantic` now require auth (401 anonymously; keyword `GET /search` stays public). Restricted-but-published datasets return 404 on card/citation/detail/preview for non-owner/non-curator callers. `GET /status` responses are field-reduced for non-owners (submitter PII, curation history, and internal transfer state are stripped). `GET /versions` ordering is numeric (`2.0 < 10.0`). Delete responses may include a `search_index` block describing index reconciliation. `POST /submissions/{id}/metadata` on a published dataset returns the new version as `approved` with a `publish_job` block (the publish job flips it to `published`), and returns 502 if indexing fails.

---

## Authentication

The backend supports two auth modes. The frontend should always send the user's Globus token as a Bearer token. Unauthenticated requests are fine for public endpoints.

**Authenticated request:**
```
Authorization: Bearer <globus_access_token>
```

**Dev mode only** (local development, no Globus):
```
X-User-Id: <user_id>
X-User-Email: <email>
X-User-Name: <display_name>
```

### Auth context

When authenticated, the backend resolves:
- `user_id` — Globus identity UUID
- `user_email` — email address
- `name` — display name
- Group memberships (for curator/submitter authorization)

### Roles

| Role | How determined | Can do |
|------|---------------|--------|
| **Anonymous** | No auth header | Search, view published cards/citations/stats/versions |
| **Authenticated user** | Valid Globus token | Submit datasets, view own submissions |
| **Submitter** | Member of MDF submitter group | Submit new datasets |
| **Owner** | `user_id` matches submission's `user_id` | Edit own metadata, withdraw own submissions |
| **Curator** | Member of curator Globus group | Approve/reject, delete, view all submissions, admin stats |

---

## URL Compatibility

The frontend currently uses URLs like:
```
/detail/81d55710-5bec-4e71-91b0-6f269e8da85a-1.0
/detail/levine_abo2179_database_v2.1-1.0
```

The slug format is `{source_id}-{version}`. The backend provides `GET /detail/{slug}` which parses this automatically. Source IDs come in two styles:
- **UUID-style:** `81d55710-5bec-4e71-91b0-6f269e8da85a`
- **Name-style:** `levine_abo2179_database_v2.1`

Slugs without a `-X.Y` version suffix (e.g. `/detail/levine_abo2179_database_v2.1`) resolve to the latest published version.

---

## Response Conventions

All responses include `"success": true|false`. Error responses use standard HTTP status codes with:
```json
{"detail": "Error message"}
```

Unhandled errors return:
```json
{"detail": "Internal server error", "request_id": "a1b2c3d4e5f6"}
```

---

## Endpoints

### Health

```
GET /health
```
```json
{"status": "ok", "service": "mdf-v2"}
```

---

### Search

```
GET /search?q={query}&limit={20}&offset={0}&type={all|datasets}&sort={relevance}
```

No auth required. Returns published datasets only. Supports faceted filtering.

`q` is **optional**. Omitting it is a *browse* request: the newest published
datasets, in exactly the same response shape as a keyword search (with
`"query": ""`), so a client renders both identically.

**Query params:**

| Param | Type | Description |
|-------|------|-------------|
| `q` (or `query`) | string | Search query. Omit to browse the newest datasets. |
| `limit` | int | Page size (default 20, max 50) |
| `offset` | int | Pagination offset (default 0) |
| `type` | string | `all`, `datasets`, or `streams` (default `all`) |
| `sort` | string | `relevance` (default), `newest`, or `most_viewed`. Without a `q` this is always `newest` — relevance has nothing to rank. `most_viewed` currently falls back to `newest`. |
| `year` | string | Filter by publication year (e.g. `2024`) |
| `organization` | string | Filter by organization (e.g. `MDF Open`) |
| `author` | string | Filter by author name (e.g. `Blaiszik, Ben`) |
| `keyword` | string | Filter by keyword/subject (e.g. `perovskite`) |
| `domain` | string | Filter by scientific domain (e.g. `batteries`) |

**Multi-select filters — repeat the param:**

```
GET /search?author=Blaiszik%2C%20Ben&author=Ward%2C%20Logan
```

Filter values must be a **whole facet value** as returned in `facets`; these
fields are indexed as exact keywords, so a fragment matches nothing.

`year`, `organization`, `keyword` and `domain` additionally accept a legacy
comma-separated single occurrence (`?keyword=perovskite,DFT`) — no value of
those fields contains a comma. **`author` never splits on commas**: authors are
indexed `"Family, Given"` (`Blaiszik, Ben`, `Hersam, Mark C.`), so the comma is
part of the name. Use repeated `author` params to select several.

**Response:**
```json
{
  "query": "perovskite",
  "total": 23,
  "offset": 0,
  "results": [
    {
      "type": "dataset",
      "source_id": "abx3_perovs_alloys_v1.1",
      "version": "1.0",
      "title": "ABX3 Perovskite Alloys Dataset",
      "authors": ["Chibueze Amanchukwu", "Chris Wolverton"],
      "keywords": ["perovskite", "DFT", "alloys"],
      "description": "A dataset of ABX3 perovskite alloy calculations...",
      "publication_year": 2023,
      "organization": "MDF Open",
      "domains": ["materials science"],
      "doi": "10.18126/abc123",
      "license": "CC-BY-4.0",
      "size_bytes": 10485760,
      "file_count": 42,
      "status": "published",
      "score": 4.5,
      "latest": true,
      "root_version": "abx3_perovs_alloys_v1.1-1.0",
      "download_url": "https://data.materialsdatafacility.org/..."
    }
  ],
  "facets": {
    "Year":         [{"value": "2024", "count": 12}, {"value": "2023", "count": 8}],
    "Organization": [{"value": "MDF Open", "count": 15}, {"value": "Foundry", "count": 5}],
    "Authors":      [{"value": "Wolverton, Chris", "count": 7}],
    "Keywords":     [{"value": "DFT", "count": 10}, {"value": "perovskite", "count": 8}],
    "Domains":      [{"value": "batteries", "count": 4}]
  }
}
```

**Notes:**
- `score` is relevance ranking (higher = better match)
- `latest` — whether this is the most recent version
- `download_url` — direct download link (may be absent)
- `root_version` — the first version's versioned ID (for version chain navigation)
- `facets` — counts for each facet bucket, reflecting current query and filters. Render in a sidebar; on click, re-query with the corresponding filter param.
- When filters are applied, both `results` and `facets` reflect the filtered view

**Usage pattern:**
```
1. User searches "perovskite"
   → GET /search?q=perovskite
   ← results + facets {Year: [...], Organization: [...], ...}

2. Frontend renders facet sidebar with counts

3. User clicks "2024" under Year
   → GET /search?q=perovskite&year=2024
   ← filtered results + updated facets

4. User also clicks "MDF Open" under Organization
   → GET /search?q=perovskite&year=2024&organization=MDF+Open
   ← further filtered results + updated facets
```

---

### Dataset Card

Two endpoints return identical data — use whichever matches your routing:

```
GET /card/{source_id}?version={optional}
GET /detail/{slug}
```

Both accept optional auth. When authenticated, the response includes permissions.

**Response:**
```json
{
  "success": true,
  "source_id": "levine_abo2179_database_v2.1",
  "version": "1.0",
  "card": {
    "source_id": "levine_abo2179_database_v2.1",
    "version": "1.0",
    "title": "ABO2179 Electroadhesives Database",
    "authors": ["Daniel Levine", "Arjun Bhorkar", "..."],
    "description": "Database of electroadhesives...",
    "keywords": ["electroadhesion", "soft robotics"],
    "publisher": "Materials Data Facility",
    "publication_year": 2023,
    "organization": "MDF Open",
    "methods": [],
    "facility": null,
    "status": "published",
    "created_at": "2023-05-15T12:00:00Z",
    "updated_at": "2023-05-15T12:00:00Z",
    "stats": {
      "file_types": ["csv", "json"],
      "data_sources_count": 1,
      "file_count": 0,
      "total_bytes": 0,
      "size_human": "0 B"
    },
    "links": {
      "self": "/status/levine_abo2179_database_v2.1",
      "citation": "/citation/levine_abo2179_database_v2.1",
      "doi": "https://doi.org/10.18126/jx14-t0v8"
    },
    "download_url": "https://data.materialsdatafacility.org/...",
    "archive_size": 1048576,
    "data_sources": ["https://data.materialsdatafacility.org/..."],
    "doi": "10.18126/jx14-t0v8",
    "license": "CC-BY-4.0",
    "ml": {
      "data_format": "csv",
      "task_type": "regression",
      "n_items": 5000,
      "splits": [{"type": "train", "n_items": 4000}, {"type": "test", "n_items": 1000}],
      "input_keys": ["composition", "temperature"],
      "target_keys": ["bandgap"],
      "short_name": "perovskite_bg"
    },
    "profile_summary": {
      "total_files": 3,
      "total_bytes": 2048000,
      "formats": {"csv": 2, "json": 1},
      "tabular_summary": {
        "filename": "data.csv",
        "n_rows": 5000,
        "columns": [{"name": "composition", "dtype": "object"}, {"name": "bandgap", "dtype": "float64"}]
      },
      "sample_rows": [{"composition": "CsPbI3", "bandgap": 1.73}]
    }
  },
  "permissions": {
    "can_edit": true,
    "can_delete": false,
    "can_curate": false
  }
}
```

**`card` fields — always present:**
- `source_id`, `version`, `title`, `authors`, `description`, `keywords`
- `publisher`, `organization`, `status`, `created_at`, `updated_at`
- `stats` — file type hints, counts
- `links` — relative API links
- `data_sources` — list of data source URIs

**`card` fields — present when available:**
- `doi` — DOI string (without `https://doi.org/` prefix)
- `download_url` — direct download URL for the dataset archive
- `archive_size` — size of the zip archive in bytes
- `license` — license name string
- `ml` — ML metadata summary (only for ML-ready datasets)
- `profile_summary` — file-level profiling data (only for profiled datasets)
- `methods`, `facility` — experimental context
- `link_health` — data availability, `{"status": "ok"|"degraded"|"broken"|"unverifiable", "checked_at": "<iso>"}`.
  Absent on datasets that have never been checked (do not render "unverified" for a missing key — render
  nothing). `unverifiable` means MDF could not check anonymously (e.g. a Globus collection that requires
  consent); it is **not** a quality signal and must not be styled as a problem. The per-URL detail behind the
  status is curator-only (see [Link Health](#link-health-curator)).

**`permissions` object:**

Always present. All false when unauthenticated.

| Field | Type | Meaning |
|-------|------|---------|
| `can_edit` | bool | User can edit metadata (owner or curator, status is `pending_curation`/`rejected`/`published`) |
| `can_delete` | bool | User can soft-delete (curator only) |
| `can_curate` | bool | User can approve/reject (curator and status is `pending_curation`) |

#### Agent card (`?format=agent`)

```
GET /card/{source_id}?version={optional}&format=agent
```

Same record, same visibility rules, same `permissions` block — a different projection, aimed at LLM/agent
consumers (the `mdf` MCP server, `mdf import`, notebook assistants) that pay per token and want code they can
run. `format` accepts `full` (the default, byte-identical to the response above) or `agent`; anything else is
a `400`. An agent response additionally carries `"format": "agent"` at the top level.

Differences from the full card: no `stats`/`links` indirection, no `profile_summary`, `description` truncated
to 600 characters on a word boundary (with a trailing `…`), and two extra blocks.

**Response:**
```json
{
  "success": true,
  "format": "agent",
  "card": {
    "source_id": "levine_abo2179_database_v2.1",
    "version": "1.0",
    "title": "ABO2179 Electroadhesives Database",
    "doi": "10.18126/jx14-t0v8",
    "license": {"name": "CC-BY-4.0", "identifier": "CC-BY-4.0", "url": "https://..."},
    "organization": "MDF Open",
    "authors": [{"name": "Daniel Levine", "orcid": "0000-...", "affiliations": ["..."]}],
    "keywords": ["electroadhesion"],
    "description": "Database of electroadhesives…",
    "size_bytes": 1048576,
    "file_count": 3,
    "data_sources": ["https://data.materialsdatafacility.org/..."],
    "download_url": "https://data.materialsdatafacility.org/...",
    "loading_recipe": {
      "python": "# pip install mdf-cli\nfrom mdf import MDFAgent\n\nagent = MDFAgent()\nagent.clone(\"levine_abo2179_database_v2.1\", version=\"1.0\")",
      "shell": "pip install mdf-cli && mdf clone levine_abo2179_database_v2.1"
    },
    "citation_apa": "Levine, D., & Bhorkar, A. (2023). ABO2179 ... Materials Data Facility.",
    "link_health": {"status": "ok", "checked_at": "2026-09-04T02:11:00Z"},
    "urls": {
      "landing": "https://www.materialsdatafacility.org/detail/levine_abo2179_database_v2.1?version=1.0",
      "citation": "/citation/levine_abo2179_database_v2.1",
      "files": "/preview/levine_abo2179_database_v2.1/files"
    },
    "ml": {"data_format": "csv", "task_type": ["regression"], "domain": [], "n_items": 5000,
           "short_name": "perovskite_bg",
           "splits": [{"type": "train", "path": "train.csv", "n_items": 4000}],
           "keys": [{"name": "bandgap", "role": "target"}]},
    "columns": [{"name": "composition", "dtype": "object"}, {"name": "bandgap", "dtype": "float64"}]
  },
  "permissions": {"can_edit": false, "can_delete": false, "can_curate": false}
}
```

Notes:
- Every key above except `ml` and `columns` is always present; unknown values are `null`/`[]`/`0` rather than
  omitted, so consumers never branch on absence.
- `loading_recipe.python` routes ML-ready datasets through `foundry` (`Foundry().get_dataset(...)`) and
  everything else through `mdf clone`, because that is the only path that works for an arbitrary file tree.
- `urls.landing` is an absolute portal URL (`PORTAL_URL`, version-pinned when the record is versioned).
  `urls.citation` / `urls.files` are API-relative, matching the `links` convention on the full card.
- `columns` comes from the stored `dataset_profile` (first profiled file with columns), so it is absent on
  migrated records that have never been profiled.

---

### Citation

```
GET /citation/{source_id}?version={optional}&format={all|bibtex|ris|apa|datacite}
```

No auth required.

**Response (format=all):**
```json
{
  "success": true,
  "source_id": "levine_abo2179_database_v2.1",
  "version": "1.0",
  "bibtex": "@misc{levine_abo2179_database_v2.1,\n  title = {...},\n  ...\n}",
  "ris": "TY  - DATA\nTI  - ...\nER  - ",
  "apa": "Levine, D., ... (2023). ABO2179 Electroadhesives Database. Materials Data Facility. https://doi.org/...",
  "datacite": "<?xml version=\"1.0\" ...?><resource ...>...</resource>"
}
```

When a specific format is requested, only that key is included, plus `content_type` (e.g. `"application/x-bibtex"`).

---

### Dataset Access Stats

```
GET /stats/{source_id}
```

No auth required. Returns aggregate metrics across all published versions.

**Response:**
```json
{
  "success": true,
  "source_id": "levine_abo2179_database_v2.1",
  "view_count": 142,
  "download_count": 37,
  "version_count": 3,
  "first_published": "2023-05-15T12:00:00Z",
  "last_updated": "2026-02-28T09:30:00Z"
}
```

**Counter sources:**
- `view_count` — incremented on every `GET /card`, `GET /detail`, `GET /citation`, `GET /preview` hit
- `download_count` — incremented on `POST /stream/{id}/download-url` *(disabled for the initial v2 release — the streams/files routers are unmounted, so this counter does not currently increment)*

---

### Versions

```
GET /versions/{source_id}?limit={50}&offset={0}
```

Optional auth. Without auth (or for non-owner/non-curator), only published versions are shown.

**Response:**
```json
{
  "success": true,
  "source_id": "Dataset_hea_hardness",
  "versions": [
    {
      "version": "1.0",
      "title": "HEA Hardness Dataset",
      "status": "published",
      "doi": "10.18126/abc123",
      "created_at": "2023-01-01T00:00:00Z",
      "updated_at": "2023-01-01T00:00:00Z",
      "root_version": "1.0",
      "previous_version": null
    },
    {
      "version": "1.1",
      "title": "HEA Hardness Dataset (updated)",
      "status": "published",
      "doi": "10.18126/abc123",
      "created_at": "2023-06-01T00:00:00Z",
      "updated_at": "2023-06-01T00:00:00Z",
      "root_version": "1.0",
      "previous_version": "1.0"
    }
  ],
  "total_count": 2,
  "dataset_doi": "10.18126/abc123"
}
```

`root_version` and `previous_version` are **bare version strings** — the version
of this dataset they point at, never a composite id. Dataset identity is always
the `(source_id, version)` pair. `null` means there is no such link (the root
version has no previous, and a lineage whose earlier versions never made it into
v2 has no nameable root). Rows migrated before this change may still carry the
old `"{source_id}-1.0"` composite in storage; the API normalizes it on read.

---

### Version Diff

```
GET /versions/{source_id}/diff?from={version}&to={version}
```

No auth required.

**Response:**
```json
{
  "success": true,
  "source_id": "Dataset_hea_hardness",
  "from_version": {"version": "1.0", "status": "published", "created_at": "..."},
  "to_version": {"version": "1.1", "status": "published", "created_at": "..."},
  "diff": {
    "added": {"new_keyword": ["materials"]},
    "removed": {},
    "changed": {"title": {"from": "Old Title", "to": "New Title"}},
    "unchanged": ["authors", "description", "keywords"]
  }
}
```

---

### Dataset Preview / Profile

```
GET /preview/{source_id}           → Full DatasetProfile
GET /preview/{source_id}/files     → File list with metadata
GET /preview/{source_id}/files/{path} → Single file detail
GET /preview/{source_id}/sample    → Sample rows from first tabular file
```

No auth required. Published datasets only.

**`GET /preview/{source_id}` response:**
```json
{
  "success": true,
  "profile": {
    "source_id": "...",
    "profiled_at": "...",
    "total_files": 3,
    "total_bytes": 2048000,
    "formats": {"csv": 2, "json": 1},
    "files": [
      {
        "path": "data.csv",
        "filename": "data.csv",
        "size_bytes": 1024000,
        "content_type": "text/csv",
        "format": "csv",
        "n_rows": 5000,
        "columns": [
          {"name": "composition", "dtype": "object", "count": 5000, "nulls": 0, "unique": 4800}
        ],
        "sample_rows": [{"composition": "CsPbI3", "bandgap": 1.73}]
      }
    ]
  }
}
```

**`GET /preview/{source_id}/sample` response:**
```json
{
  "success": true,
  "source_id": "...",
  "filename": "data.csv",
  "format": "csv",
  "columns": [{"name": "composition", "dtype": "object"}],
  "n_rows": 5000,
  "sample_rows": [{"composition": "CsPbI3", "bandgap": 1.73}]
}
```

---

### Submission Status

```
GET /status/{source_id}?version={optional}
GET /status?source_id={source_id}&version={optional}
```

Optional auth. Unauthenticated callers only see published submissions. Owner/curator sees all statuses.

**Response:**
```json
{
  "success": true,
  "submission": {
    "source_id": "mdf-abc123",
    "version": "1.0",
    "versioned_source_id": "mdf-abc123-1.0",
    "user_id": "globus-uuid",
    "user_email": "user@example.com",
    "organization": "MDF Open",
    "status": "published",
    "dataset_mdata": { ... },
    "schema_version": "2",
    "test": false,
    "created_at": "2026-01-15T12:00:00Z",
    "updated_at": "2026-01-15T12:00:00Z",
    "published_at": "2026-01-15T12:00:00Z",
    "doi": "10.18126/...",
    "dataset_doi": "10.18126/...",
    "view_count": 42,
    "download_count": 7
  }
}
```

`dataset_mdata` is the full metadata object (parsed from JSON, not a string).

---

### Submit a Dataset

```
POST /submit
Authorization: Bearer <token>
Content-Type: application/json
```

Requires submitter group membership in production.

**Request body — flat v2 metadata:**
```json
{
  "title": "My Dataset",
  "authors": [
    {"name": "Jane Smith", "orcid": "0000-0002-1234-5678", "affiliations": ["MIT"]}
  ],
  "description": "A dataset about...",
  "keywords": ["materials", "DFT"],
  "data_sources": ["globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/path/to/data"],
  "organization": "MDF Open",
  "license": {"name": "CC-BY-4.0", "url": "https://creativecommons.org/licenses/by/4.0/"},
  "funding": [{"funder_name": "NSF", "award_number": "DMR-1234567"}],
  "related_works": [{"identifier": "10.1234/paper", "identifier_type": "DOI", "relation_type": "IsDescribedBy"}],
  "methods": ["DFT", "molecular dynamics"],
  "facility": "ALCF",
  "fields_of_science": ["Materials Science"],
  "domains": ["batteries", "energy storage"],
  "ml": {
    "data_format": "csv",
    "task_type": ["regression"],
    "n_items": 5000,
    "keys": [
      {"name": "composition", "role": "input"},
      {"name": "bandgap", "role": "target", "units": "eV"}
    ],
    "splits": [
      {"type": "train", "path": "train.csv", "n_items": 4000},
      {"type": "test", "path": "test.csv", "n_items": 1000}
    ]
  },
  "tags": ["featured"],
  "extensions": {"custom_field": "value"},
  "source_id": "my-dataset-id",
  "source_name": "my_dataset_family",
  "acl": ["public"],
  "test": false
}
```

**Required fields:** `title`, `authors` (at least one with `name`), `data_sources` (unless `update: true` metadata-only)

**System fields (top level, not metadata):**

| Field | Type | Notes |
|---|---|---|
| `source_id` | string | Optional. The dataset id you want. Must match `^[a-z0-9][a-z0-9._-]{2,63}$` (no `..`) or the request is rejected with 400 and the grammar in `detail`. Omit it and the server mints `mdf-<hex>`. If the id is already taken by a *different* dataset (and `update` is false) a short suffix is appended. |
| `source_name` | string | Optional. Dataset-family grouping key behind the search facet. Defaults to `source_id`. |
| `acl` | string[] | Optional. `["public"]` (the default) or a list of Globus identity/group principals. Bare identity UUIDs are accepted and canonicalized to `urn:globus:auth:identity:<uuid>`; `urn:globus:...` values are stored verbatim. On `update: true`, omitting `acl` **inherits** the prior version's visibility. |

`source_id`, `source_name`, `acl`, `legacy_source_id`, `root_version` and
`previous_version` are top-level *record* attributes, not dataset metadata — see
"Record shape (v2.1)" in `v2.md`. Do not put them in `extensions`.

**`extensions` is user metadata only.** Any key namespaced `mdf_*` is reserved
and rejected with 400 on both submit and edit. Two exceptions are grandfathered
**on submit only**, for released CLI versions:

- `extensions.mdf_source_id` → copied to top-level `source_id` (DEPRECATED)
- `extensions.mdf_source_name` → copied to top-level `source_name` (DEPRECATED)

Both are stripped from what gets stored and logged as a deprecation warning.
Send the top-level fields instead. On the **edit** route every `mdf_*` key
(including these two) is rejected: an edit may not repoint dataset identity.

**`data_sources` formats:**
- `globus://{collection_uuid}/path/to/data` — Globus transfer
- `https://...` — HTTP download
- `stream://{stream_id}` — internal stream reference *(disabled for the initial v2 release — the streams/files routers that create and populate streams are unmounted, so there is no way to obtain a `stream_id`; publish datasets by reference to a `globus://` or `https://` source instead)*

**For updates** (new version of existing dataset), include:
```json
{
  "update": true,
  "source_id": "existing_source_id",
  ...
}
```

On `update: true` the `source_id` is validated **leniently** — it addresses an
existing record, and the live corpus contains ids that predate the strict
grammar (uppercase, non-ASCII, longer than 64 characters). Only genuinely
unsafe shapes are rejected: path separators, whitespace/control characters,
`..`, a leading `-`/`.`, and lengths above 160. The same lenient rule applies to
every `{source_id}` **path parameter**; an unsafe one is answered with 404 and
no grammar is disclosed.

**Response:**
```json
{
  "success": true,
  "source_id": "mdf-abc123",
  "version": "1.0",
  "versioned_source_id": "mdf-abc123-1.0",
  "organization": "MDF Open"
}
```

New submissions land with `status: "pending_curation"`.

---

### Edit Metadata

```
POST /submissions/{source_id}/metadata
Authorization: Bearer <token>
```

Owner or curator. Only fields explicitly provided (non-null) are applied — omitted fields are left unchanged.

**Editable fields:**

| Field | Type | Notes |
|-------|------|-------|
| `title` | string | |
| `authors` | `[{name, orcid?, affiliations?}]` | Replaces author list |
| `description` | string | |
| `keywords` | `[string]` | Replaces keyword list |
| `license` | `{name, url?, identifier?}` | |
| `funding` | `[{funder_name, award_number?}]` | |
| `related_works` | `[{identifier, identifier_type, relation_type}]` | |
| `methods` | `[string]` | |
| `facility` | string | |
| `fields_of_science` | `[string]` | |
| `domains` | `[string]` | |
| `ml` | object | |
| `geo_locations` | `[{place}]` | |
| `tags` | `[string]` | |
| `extensions` | object | **Deep-merged** into existing extensions — existing keys not in the update are preserved. Any `mdf_*` key is rejected with 400. |
| `acl` | `[string]` | Dataset visibility. `["public"]` or a list of Globus identity/group principals. Applied to the top-level record attribute, never merged into metadata. May not be empty, and may not mix `"public"` with specific identities. |
| `version` | string | Targets a specific version (defaults to latest) |

Not editable: `data_sources`, `organization`, `publisher`, `test`, `update`.

Editing a `published` version creates a new minor version that **inherits the
current `acl`** — visibility follows the dataset and is never silently reset to
public.

**Request body (any subset of the above):**
```json
{
  "title": "Updated Title",
  "keywords": ["new", "keywords"],
  "domains": ["batteries"],
  "version": "1.0"
}
```

**Behavior by current status:**
| Status | What happens |
|--------|-------------|
| `pending_curation` | Updates metadata in-place |
| `rejected` | Updates metadata in-place (fix before resubmit) |
| `published` | Creates a new minor version (1.0 → 1.1), immediately `published`, no re-curation. DataCite metadata updated, **no new DOI minted** — the existing `dataset_doi` is inherited. |
| `withdrawn`, `approved` | Returns 400 |

**Response (pending_curation / rejected):**
```json
{
  "success": true,
  "source_id": "mdf-abc123",
  "version": "1.0",
  "updated_fields": ["title", "keywords"]
}
```

**Response (published → minor version bump):**
```json
{
  "success": true,
  "source_id": "mdf-abc123",
  "version": "1.0",
  "new_version": "1.1",
  "updated_fields": ["title", "keywords"]
}
```

`version` is the version that was edited. `new_version` is the newly created version — only present when editing a published dataset. After a minor bump, the new version is immediately live and `GET /card/{source_id}` resolves to it.

---

### Withdraw

```
POST /submissions/{source_id}/withdraw
Authorization: Bearer <token>
```

Owner or curator. Only allowed when `status == "pending_curation"`.

**Request body:**
```json
{"reason": "Duplicate submission", "version": "1.0"}
```

**Response:**
```json
{"success": true, "source_id": "...", "version": "1.0", "status": "withdrawn"}
```

---

### Resubmit

```
POST /submissions/{source_id}/resubmit
Authorization: Bearer <token>
```

Owner or curator. Only allowed when `status == "rejected"`.

**Request body:**
```json
{"notes": "Fixed the title and added authors", "version": "1.0"}
```

**Response:**
```json
{"success": true, "source_id": "...", "version": "1.0", "status": "pending_curation"}
```

---

### Soft-Delete

```
POST /submissions/{source_id}/delete
Authorization: Bearer <token>
```

Curator only. Works on any status except already deleted.

**Request body:**
```json
{"reason": "Spam submission", "version": "1.0"}
```

`reason` is required. `version` is optional (defaults to latest).

**Response:**
```json
{"success": true, "source_id": "...", "version": "1.0", "status": "deleted"}
```

---

### List My Submissions

```
GET /submissions?status={filter}&include_counts={true}&limit={50}&start_key={...}
Authorization: Bearer <token>
```

Returns the caller's own submissions. Add `?organization=X` to see all org submissions (curator only).

**Query params:**
| Param | Type | Description |
|-------|------|-------------|
| `status` | string | Comma-separated filter, e.g. `published,pending_curation` |
| `include_counts` | bool | Include per-status counts over all submissions |
| `limit` | int | Page size (default 50) |
| `start_key` | string | Pagination cursor from previous response's `next_key` |
| `organization` | string | Org-wide view (curator only) |

**Response:**
```json
{
  "success": true,
  "submissions": [
    {
      "source_id": "mdf-abc123",
      "version": "1.0",
      "status": "published",
      "user_id": "...",
      "organization": "MDF Open",
      "dataset_mdata": { ... },
      "created_at": "...",
      "updated_at": "..."
    }
  ],
  "counts": {"pending_curation": 3, "published": 12, "rejected": 1},
  "total": 16,
  "next_key": "eyJvZmZzZXQiOjJ9"
}
```

`counts` and `total` are only present when `include_counts=true`.

**Paging.** `next_key` is an **opaque cursor**: base64url text with no internal
structure a client may rely on (it encodes a DynamoDB key on the deployed
backend and an offset locally). Feed it back verbatim as `?start_key=` to get
the next page, and stop when it comes back `null`.

```
GET /submissions?limit=2                      -> next_key: "eyJvZmZzZXQiOjJ9"
GET /submissions?limit=2&start_key=eyJvZmZ... -> next_key: "eyJvZmZzZXQiOjR9"
GET /submissions?limit=2&start_key=eyJvZmZ... -> next_key: null
```

`next_key` is now returned in **counts mode too**. Previously
`include_counts=true` always answered `next_key: null` even when `limit` had
truncated the result (`limit=2` over 19 rows returned 2 rows and no cursor), so
page 2 was unreachable. An unrecognized or stale cursor is treated as "no
cursor" and restarts the listing rather than returning an error.

---

### Curation Queue (Curator)

```
GET /curation/pending?limit={50}&offset={0}&organization={optional}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "success": true,
  "pending_count": 5,
  "submissions": [
    {
      "source_id": "mdf-abc123",
      "version": "1.0",
      "title": "Some Dataset",
      "organization": "MDF Open",
      "submitter": "globus-user-uuid",
      "submitted_at": "2026-02-28T12:00:00Z",
      "file_count": 3,
      "total_bytes": 2048000
    }
  ],
  "limit": 50,
  "offset": 0
}
```

### Curation Review

```
GET /curation/{source_id}?version={optional}
Authorization: Bearer <token>
```

**Response:**
```json
{
  "success": true,
  "submission": { ... },
  "curation_history": [
    {"action": "rejected", "curator_id": "...", "timestamp": "...", "reason": "Missing authors"}
  ],
  "current_status": "pending_curation",
  "can_approve": true,
  "can_reject": true
}
```

### Approve

```
POST /curation/{source_id}/approve
Authorization: Bearer <token>
```

**Request body:**
```json
{
  "notes": "Looks good",
  "mint_doi": true,
  "metadata_updates": {"keywords": ["added-by-curator"]},
  "version": "1.0"
}
```

All fields optional. `mint_doi` defaults to `true`. `metadata_updates` lets the curator fix metadata inline during approval.

**Response:**
```json
{
  "success": true,
  "source_id": "...",
  "version": "1.0",
  "status": "published",
  "approved_by": "curator-uuid",
  "approved_at": "...",
  "doi": {"success": true, "doi": "10.18126/..."}
}
```

### Reject

```
POST /curation/{source_id}/reject
Authorization: Bearer <token>
```

**Request body:**
```json
{
  "reason": "Missing data source URLs",
  "suggestions": "Please add the Globus endpoint path",
  "version": "1.0"
}
```

`reason` is required.

**Response:**
```json
{
  "success": true,
  "source_id": "...",
  "version": "1.0",
  "status": "rejected",
  "rejected_by": "curator-uuid",
  "rejected_at": "...",
  "reason": "Missing data source URLs"
}
```

---

### Admin Stats (Curator)

```
GET /admin/stats
Authorization: Bearer <token>
```

**Response:**
```json
{
  "success": true,
  "total": 922,
  "by_status": {
    "published": 904,
    "pending_curation": 10,
    "rejected": 5,
    "deleted": 3
  },
  "access_totals": {
    "view_count": 15230,
    "download_count": 4521
  }
}
```

---

### Link Health (Curator)

Data availability reporting. Curator-only, because the per-URL detail exposes internal paths and upstream
error strings; the public half is the `link_health` key on the dataset card.

```
POST /admin/link-health/run        {"force": false, "limit": null}
GET  /admin/link-health/summary
Authorization: Bearer <token>
```

`POST /admin/link-health/run` enqueues one sweep meta-job and returns immediately — the scan and the
per-record fan-out happen in the async worker, exactly like `/admin/embeddings/rebuild`, because a full-corpus
scan does not fit in API Gateway's 30s window. The sweep skips records checked within the last
`LINK_HEALTH_MAX_AGE_HOURS` (default 24) unless `force` is true; `limit` caps the fan-out. A dispatch failure
returns `200` with `{"success": false, "error": "..."}`, matching the embedding endpoints.

**`POST /admin/link-health/run` response:**
```json
{
  "success": true,
  "force": false,
  "limit": null,
  "sweep_job": {"mode": "sqs", "queued": true, "job_type": "link_health_sweep"},
  "message": "Link health sweep dispatched to async worker. Poll /admin/link-health/summary to watch results land."
}
```

**`GET /admin/link-health/summary` response:**
```json
{
  "success": true,
  "published_total": 904,
  "checked": 870,
  "unchecked": 34,
  "by_status": {"ok": 800, "degraded": 24, "broken": 18, "unverifiable": 28},
  "oldest_checked_at": "2026-09-01T04:00:00Z",
  "newest_checked_at": "2026-09-04T04:00:00Z",
  "broken_sample": [
    {
      "source_id": "old_dataset_2018",
      "version": "1.0",
      "status": "broken",
      "checked_at": "2026-09-04T04:00:00Z",
      "failed_checks": [{"url": "https://...", "http_status": 404, "error": null}]
    }
  ],
  "broken_total": 42
}
```

Counts cover published, latest records only. `broken_sample` holds the 50 most recently checked datasets whose
status is `broken` **or** `degraded` (one dead source out of four is still work to do), most recent first;
`broken_total` is the un-truncated count.

Status semantics — the aggregate never claims more than was observed:

| Status | Meaning |
|--------|---------|
| `ok` | Every URL MDF could reach answered. |
| `degraded` | Some URLs answered, at least one refused (4xx/5xx). |
| `broken` | Nothing answered and at least one URL refused. |
| `unverifiable` | Nothing could be checked anonymously — no data sources, a Globus collection outside the NCSA MDF endpoint, or transport failures (timeout/DNS/TLS). **Not** a quality signal. |

Timeouts and connection errors are deliberately `unverifiable`, never `broken`: a false "broken" is worse than
silence. At most 5 URLs are probed per record (`download_url` first, then `data_sources`), HEAD with a ranged
`GET` fallback, 10s timeout, no auth.

---

## Submission Lifecycle

```
                                ┌─────────────┐
                 POST /submit   │   pending    │
                ───────────────>│  _curation   │
                                └──────┬───────┘
                                       │
                    ┌──────────────────┬┴───────────────────┐
                    │                  │                     │
                    ▼                  ▼                     ▼
             ┌────────────┐    ┌─────────────┐      ┌──────────────┐
             │  approved   │    │  rejected    │      │  withdrawn   │
             │             │    │              │      │  (by owner)  │
             └──────┬──────┘    └──────┬───────┘      └──────────────┘
                    │                  │
                    │           edit + resubmit ──> pending_curation
                    ▼
             ┌─────────────┐
             │  published   │──── edit metadata ──> new minor version (stays published)
             └─────────────┘

  Any status ──── curator delete ──> deleted (soft-delete, recorded in history)
```

**Status values:** `pending_curation`, `approved`, `published`, `rejected`, `withdrawn`, `deleted`

---

## CORS

**Staging/dev:** `Access-Control-Allow-Origin: *` (no credentials)

**Production:** Only `https://materialsdatafacility.org` and `https://app.materialsdatafacility.org` with credentials enabled.

**Allowed headers:** `Content-Type`, `Authorization`, `X-User-Id`, `X-User-Email`, `X-User-Name`, `X-Globus-Token`

---

## Error Codes

| Status | Meaning |
|--------|---------|
| 400 | Bad request (validation error, invalid status transition) |
| 401 | Missing or invalid authentication token |
| 403 | Not authorized (not owner, not curator, not submitter group member) |
| 404 | Resource not found (or not published for unauthenticated requests) |
| 413 | Payload too large (metadata > 256KB, too many data sources/authors) |
| 500 | Internal error (check `request_id` in response) |

---

## Data Model Quick Reference

### Source ID & Versioning

- `source_id` — unique dataset identifier (UUID or human-readable name)
- `version` — semver-style string: `"1.0"`, `"1.1"`, `"2.0"`
- `versioned_source_id` — `"{source_id}-{version}"`
- `root_version`, `previous_version` — **bare version strings** naming another
  version of the *same* `source_id`, or `null`
- **Major bump** (1.0 → 2.0): new data sources provided
- **Minor bump** (1.0 → 1.1): metadata-only edit on a published dataset

Dataset identity is the `(source_id, version)` pair. Do not parse a version out
of a `versioned_source_id` or a chain pointer: legacy ids legitimately end in
version-like suffixes (`levine_abo2179_database_v2.1`), so the split is
ambiguous. Read `source_id` and `version` as separate fields.

### Record attributes vs. dataset metadata

Six fields are properties of the *record*, not of the dataset description, and
live at the top level of a submission (never inside `dataset_mdata` or
`extensions`). See "Record shape (v2.1)" in `v2.md`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `acl` | `[string]` | Visibility. `["public"]` or Globus principals. **Never served to a caller who is not the owner or a curator.** |
| `source_name` | string | Dataset-family grouping key behind the search facet |
| `legacy_source_id` | string | Original v1 id, for old-id redirects |
| `root_version` | string | Bare version of the lineage's first version |
| `previous_version` | string | Bare version of the immediately prior version |
| `curation_queue` | string | Internal: mirrors `status` while in the curation queue |

### Key Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `title` | string | Dataset title (required) |
| `authors` | `[{name, orcid?, affiliations?}]` | At least one required |
| `description` | string | Free-text description |
| `keywords` | `[string]` | Subject keywords |
| `data_sources` | `[string]` | URIs to the data (`globus://`, `https://`; `stream://` disabled for the initial v2 release) |
| `organization` | string | Publishing org (default: "Materials Data Facility") |
| `doi` | string | DOI (assigned on approval) |
| `download_url` | string | Direct download link |
| `license` | `{name, url?, identifier?}` | License info |
| `ml` | object | ML-readiness metadata (see card response) |
| `domains` | `[string]` | Scientific domains |
| `methods` | `[string]` | Experimental/computational methods |
| `facility` | string | Research facility |
| `fields_of_science` | `[string]` | Broad scientific fields |
| `funding` | `[{funder_name, award_number?}]` | Funding sources |
| `related_works` | `[{identifier, identifier_type, relation_type}]` | Related publications/datasets |
| `tags` | `[string]` | Platform tags |
| `extensions` | object | Arbitrary extra metadata |
