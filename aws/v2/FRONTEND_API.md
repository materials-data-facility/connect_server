# MDF v2 Frontend API Reference

Complete API contract for building a frontend against the MDF Connect v2 backend. Every endpoint, request shape, and response shape is documented here.

**Base URL (staging):** `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging`

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
GET /search?q={query}&limit={20}&offset={0}&type={all|datasets}
```

No auth required. Returns published datasets only. Supports faceted filtering.

**Query params:**

| Param | Type | Description |
|-------|------|-------------|
| `q` (or `query`) | string | Search query (required) |
| `limit` | int | Page size (default 20, max 50) |
| `offset` | int | Pagination offset (default 0) |
| `type` | string | `all`, `datasets`, or `streams` (default `all`) |
| `year` | string | Filter by publication year, comma-separated (e.g. `2024,2025`) |
| `organization` | string | Filter by organization, comma-separated (e.g. `MDF Open`) |
| `author` | string | Filter by author name, comma-separated (e.g. `Wolverton`) |
| `keyword` | string | Filter by keyword/subject, comma-separated (e.g. `perovskite,DFT`) |
| `domain` | string | Filter by scientific domain, comma-separated (e.g. `batteries`) |

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

**`permissions` object:**

Always present. All false when unauthenticated.

| Field | Type | Meaning |
|-------|------|---------|
| `can_edit` | bool | User can edit metadata (owner or curator, status is `pending_curation`/`rejected`/`published`) |
| `can_delete` | bool | User can soft-delete (curator only) |
| `can_curate` | bool | User can approve/reject (curator and status is `pending_curation`) |

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
      "updated_at": "2023-01-01T00:00:00Z"
    },
    {
      "version": "1.1",
      "title": "HEA Hardness Dataset (updated)",
      "status": "published",
      "doi": "10.18126/abc123",
      "created_at": "2023-06-01T00:00:00Z",
      "updated_at": "2023-06-01T00:00:00Z"
    }
  ],
  "total_count": 2,
  "dataset_doi": "10.18126/abc123"
}
```

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
  "test": false
}
```

**Required fields:** `title`, `authors` (at least one with `name`), `data_sources` (unless `update: true` metadata-only)

**`data_sources` formats:**
- `globus://{collection_uuid}/path/to/data` — Globus transfer
- `https://...` — HTTP download
- `stream://{stream_id}` — internal stream reference *(disabled for the initial v2 release — the streams/files routers that create and populate streams are unmounted, so there is no way to obtain a `stream_id`; publish datasets by reference to a `globus://` or `https://` source instead)*

**For updates** (new version of existing dataset), include:
```json
{
  "update": true,
  "extensions": {"mdf_source_id": "existing_source_id"},
  ...
}
```

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
| `extensions` | object | **Deep-merged** into existing extensions — existing keys not in the update are preserved |
| `version` | string | Targets a specific version (defaults to latest) |

Not editable: `data_sources`, `organization`, `publisher`, `test`, `update`.

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
  "next_key": null
}
```

`counts` and `total` only present when `include_counts=true`. `next_key` is `null` when no more pages.

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
- **Major bump** (1.0 → 2.0): new data sources provided
- **Minor bump** (1.0 → 1.1): metadata-only edit on a published dataset

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
