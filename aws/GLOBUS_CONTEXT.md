# Globus HTTPS Endpoints: Technical Context for MDF v2

This document provides context for AI agents and developers working with Globus HTTPS endpoints in the MDF v2 backend.

## Overview

MDF uses Globus HTTPS endpoints for file storage. These endpoints provide:
- Direct HTTPS GET/PUT/DELETE operations (no Globus Transfer needed for small files)
- Bearer token authentication via Globus Auth
- 1PB of free storage on the NCSA MDF endpoint

## Key Identifiers

```
NCSA MDF Endpoint UUID: 82f1b5c6-6e9b-11e5-ba47-22000b92c6ec
HTTPS Server: data.materialsdatafacility.org
MDF Native App Client ID: 984464e2-90ab-433d-8145-ac0215d26c8e
```

## Authentication

### Scopes

To access an endpoint via HTTPS, you need a token with the HTTPS scope:

```
https://auth.globus.org/scopes/{endpoint_uuid}/https
```

For the NCSA MDF endpoint:
```
https://auth.globus.org/scopes/82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/https
```

### Token Acquisition

**Interactive (Native App Flow):**
```python
from globus_sdk import NativeAppAuthClient

client = NativeAppAuthClient("984464e2-90ab-433d-8145-ac0215d26c8e")
client.oauth2_start_flow(
    requested_scopes=["https://auth.globus.org/scopes/82f1b5c6.../https"],
    refresh_tokens=True,
)
url = client.oauth2_get_authorize_url()
# User visits URL, gets auth code
tokens = client.oauth2_exchange_code_for_tokens(auth_code)
access_token = tokens.by_resource_server["82f1b5c6..."]["access_token"]
```

**Service Account (Client Credentials):**
```python
from globus_sdk import ConfidentialAppAuthClient

client = ConfidentialAppAuthClient(client_id, client_secret)
tokens = client.oauth2_client_credentials_tokens(
    requested_scopes="https://auth.globus.org/scopes/{endpoint}/https"
)
```

### Token Storage

Cached tokens are stored at: `~/.mdf/v2_https_tokens.json`

Format:
```json
{
  "access_token": "AgdNNrB9Y...",
  "refresh_token": "AgP9r...",
  "expires_at_seconds": 1738456789,
  "resource_server": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
}
```

## HTTPS Operations

### Upload (PUT)

```bash
curl -X PUT "https://data.materialsdatafacility.org/path/to/file.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain" \
  -d "file contents"
```

**Critical:** Parent directories must exist. PUT does not auto-create directories.

### Download (GET)

```bash
curl "https://data.materialsdatafacility.org/path/to/file.txt" \
  -H "Authorization: Bearer $TOKEN"
```

Public files (if endpoint allows) can be accessed without auth.

### Delete (DELETE)

```bash
curl -X DELETE "https://data.materialsdatafacility.org/path/to/file.txt" \
  -H "Authorization: Bearer $TOKEN"
```

### Create Directory (MKCOL)

```bash
curl -X MKCOL "https://data.materialsdatafacility.org/path/to/dir/" \
  -H "Authorization: Bearer $TOKEN"
```

**Note:** MKCOL may require additional permissions. Some endpoints return 403.

## Path Strategy

Because directories cannot be auto-created, MDF v2 uses a **flat path structure**:

```
{base_path}/{stream_id}_{date}_{filename}
```

Example:
```
/tmp/testing/stream-abc123_20260201_data.csv
```

This avoids needing to create `streams/abc123/2026-02-01/` directories.

For production with directory support:
```
/mdf/streams/{stream_id}/{date}/{filename}
```

## User Token Pass-Through

For proper authorization and audit trails, user operations should use the user's token:

```python
# In API handler
user_token = request.headers.get("X-Globus-Token")

# Pass to storage backend
storage.store_file(stream_id, filename, content, user_token=user_token)
```

This ensures:
1. **Access control:** Users can only access paths they're authorized for
2. **Audit trail:** Globus logs show the actual user who performed the action
3. **Security:** Server doesn't need blanket write access

## Common Issues

### 307 Redirect
HTTPS endpoints may return 307 redirects. Use `follow_redirects=True` in HTTP clients.

### 404 on PUT
Parent directory doesn't exist. Either:
1. Create directories first with MKCOL
2. Use flat path structure (recommended)

### 403 on MKCOL
Endpoint doesn't allow directory creation via HTTPS. Use Globus Transfer API or flat paths.

### Token Expired
Access tokens expire (typically 48 hours). Use refresh tokens to get new access tokens.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GLOBUS_ENDPOINT_ID` | Endpoint UUID | NCSA MDF endpoint |
| `GLOBUS_BASE_PATH` | Base path on endpoint | `/tmp/testing` |
| `GLOBUS_HTTPS_SERVER` | HTTPS server hostname | `data.materialsdatafacility.org` |
| `GLOBUS_ACCESS_TOKEN` | Static access token | (none) |
| `GLOBUS_CLIENT_ID` | Client ID for creds flow | (none) |
| `GLOBUS_CLIENT_SECRET` | Client secret | (none) |
| `STORAGE_BACKEND` | Storage type | `local` |

## Code References

- `v2/storage/globus_https.py` - Globus storage backend implementation
- `v2/storage/base.py` - Abstract storage interface
- `test_globus_upload.py` - Interactive auth and upload test
- `~/.mdf/v2_https_tokens.json` - Token cache location

## Integration with mdf_toolbox

The existing `mdf_toolbox.login()` function can be used to get tokens with all required scopes:

```python
import mdf_toolbox

auths = mdf_toolbox.login(
    services=[
        "https://auth.globus.org/scopes/82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/https",
        # ... other scopes
    ],
    app_name="MDF",
    make_clients=True,
)
```

See Foundry's `PubAuths` class for a pattern of managing multiple endpoint tokens.
