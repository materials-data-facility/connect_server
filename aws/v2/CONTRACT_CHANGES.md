# API Contract Changes — Backend Hardening Session

These are the changes from this session that **affect the CLI / Web UI contract** (request shape, response shape, status codes, auth requirements, enum values, or search-document schema). Purely internal changes (IaC, IAM, encryption, deploy scripts, storage serialization) are intentionally **excluded** — they do not change the wire contract.

Propagate the items below to the CLI and Web UI.

---

## 1. New authentication requirements (BREAKING for anonymous callers)

| Endpoint | Before | After | Client action |
|---|---|---|---|
| `POST /embed` | No auth — returned `200` + embedding | **Requires auth** — `401` without a valid Globus Bearer token | Send `Authorization: Bearer <token>`. Do **not** call anonymously. |
| `GET /search/semantic` | No auth — returned `200` | **Requires auth** — `401` without a token | Same as above. |

`GET /search` (keyword search) is **unchanged** and remains anonymous. Only the two OpenAI-backed endpoints now require login (they spend the server's OpenAI key).

---

## 2. ACL-gated reads now return 404 for restricted datasets (BREAKING for anonymous views of restricted data)

These endpoints previously returned data for any **published** dataset, ignoring its `acl`. They now return data only when the dataset is **public** OR the caller is the **owner/curator**; otherwise **`404 {"detail":"Dataset not found"}`**:

- `GET /card/{source_id}`
- `GET /citation/{source_id}`  *(now also accepts optional auth)*
- `GET /detail/{slug}`
- `GET /preview/{source_id}`, `GET /preview/{source_id}/files`, `GET /preview/{source_id}/files/{path}`, `GET /preview/{source_id}/sample`

**Client action:** Public datasets behave exactly as before. For restricted-but-published datasets, the UI must handle `404` for anonymous users and send the user's token to view them as owner/curator.

---

## 3. `GET /status/{source_id}` and `GET /status` — sanitized response for non-owners

For a **published** dataset viewed by an **anonymous / non-owner / non-curator** caller, the `submission` object is now a **sanitized public view**. The following fields are **removed** from that response (they are still returned in full to the owner or a curator):

```
user_id, user_email, curation_history, approved_by, approved_at,
rejected_by, rejected_at, rejection_reason, deleted_by, deleted_at,
transfer_status, transfer_destination, transfer_task_ids,
transfer_acl_rule_ids, transfer_bytes_transferred,
transfer_files_transferred, metadata_updated_at
```
Additionally, `dataset_mdata.acl` is stripped from the sanitized view. The `transfer` block (top level) is also omitted for non-owners.

**Client action:** Do not rely on the fields above being present when displaying *another* user's submission. The CLI checking the owner's *own* submission is unaffected (owner sees the full record).

---

## 4. New error paths (authorization)

| Endpoint | New behavior |
|---|---|
| `POST /submit` with `update: true` | Returns **`403 {"detail":"You do not have permission for this submission"}`** if the caller does not own the prior version (and is not a curator). Previously any submitter could update any dataset by `source_id`. |
| `POST /status/update` with `status: "published"` | Returns **`400`** — direct publish is no longer allowed; publishing must go through `POST /curation/{source_id}/approve`. Other status values (`pending_curation`, `approved`, `rejected`) still work. |

---

## 5. New submission status value: `publish_failed`

A submission whose publish pipeline runs but whose **search ingest fails** is now left in status **`publish_failed`** (instead of being marked `published` while invisible in search).

**Client action:** Add `publish_failed` to any status enum / display mapping. Treat it as "approved but not yet live; will retry / needs attention."

Current status values a client may see: `pending_curation`, `approved`, `published`, `publish_failed`, `rejected`, `withdrawn`, `deleted`.

---

## 6. Behavioral changes (non-breaking, but visible)

- **`GET /versions/{source_id}` ordering**: versions are now sorted **numerically** (`1.0, 2.0, … 10.0`) instead of lexicographically (`1.0, 10.0, 2.0`). UI version pickers will show the corrected order.
- **`POST /submissions/{source_id}/delete`**: now also **removes the dataset from the search index** (or re-points it to the remaining latest published version). Deleted datasets no longer appear in `GET /search` results / detail pages.
- **Published metadata edit ownership**: when a curator edits another user's published dataset, the new minor version now **retains the original owner** (`user_id`/`user_email`), so it still appears under the original submitter's `GET /submissions`, not the curator's.

---

## 7. Search-document schema (only relevant if a client reads Globus Search directly)

If the Web UI does client-side reads against the Globus Search index (rather than going through `GET /search`), note the published document now includes/corrects:

- **`mdf.resource_type` = `"dataset"`** — newly written (was absent). Needed for parity with the v1 index and any `mdf.resource_type:"dataset"` filters.
- **`mdf.source_name`** — now the preserved original family name (from `extensions.mdf_source_name`) or the full `source_id`, instead of a truncated `source_id.rsplit("-",1)[0]` value.

Documents returned by `GET /search` (the formatted result objects) are **unchanged** in shape.

---

## 8. Authorization-outcome change (configuration, but visible via `/auth/check`)

The "every authenticated user is a curator" default was removed. Curator rights now require membership in the configured Globus curator group. As a result, `GET /auth/check` may now return **`is_curator: false`** for users who previously saw `true`, and curation endpoints (`/curation/*`, `/status/update`, `/submissions/{id}/delete`) will return `403` for non-curators.

**Client action:** Continue to drive curation UI affordances off the `is_curator` flag in `/auth/check` (no shape change — just be aware the value is now correctly restricted).

---

*Generated during the backend validation/hardening session. Response shapes for the success cases of all other endpoints are unchanged.*
