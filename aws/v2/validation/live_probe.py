#!/usr/bin/env python3
"""Black-box probes against the LIVE deployed staging backend.
Authorized: validating the user's own backend. Read-mostly; two tiny billable
probes (/embed, /search/semantic) to confirm auth posture on cost-amplifying endpoints.
"""
import json
import httpx

API = "https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging"
SID = "pub_59_miao_loadpartitioning_v1.2"
c = httpx.Client(timeout=30)

def banner(t): print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)

def allkeys(o, acc):
    if isinstance(o, dict):
        for k, v in o.items():
            acc.add(k); allkeys(v, acc)
    elif isinstance(o, list):
        for x in o: allkeys(x, acc)

results = {}

# 1. /status unauth full key dump
banner("SUB-09/10  GET /status/{sid} unauth — full sensitive-key enumeration")
r = c.get(f"{API}/status/{SID}")
acc = set();
try:
    j = r.json(); allkeys(j, acc)
    sens = sorted(k for k in acc if any(s in k.lower() for s in
        ['email','user_id','curation','approved','rejected','reviewer','task_id','transfer','history','reason','acl','source_path']))
    print(f"[HTTP {r.status_code}] sensitive keys exposed: {sens}")
    results['SUB-09/10 /status unauth'] = {"status": r.status_code, "sensitive": sens}
except Exception as e:
    print("err", e, r.text[:200])

# 2. /embed unauth (tiny) — confirm unauthenticated billable proxy
banner("SRCH-03  POST /embed unauth (1-char) — billable OpenAI proxy?")
try:
    r = c.post(f"{API}/embed", json={"text": "x"})
    body = r.text[:240]
    emb = None
    try: emb = len(r.json().get("embedding", []))
    except Exception: pass
    print(f"[HTTP {r.status_code}] embedding_dims={emb} body={body[:160]}")
    results['SRCH-03 /embed unauth'] = {"status": r.status_code, "embedding_dims": emb}
except Exception as e:
    print("err", e)

# 3. /search/semantic unauth (tiny)
banner("SRCH-02  GET /search/semantic unauth — cost amplification?")
try:
    r = c.get(f"{API}/search/semantic", params={"q": "steel", "limit": 1})
    print(f"[HTTP {r.status_code}] body={r.text[:200]}")
    results['SRCH-02 /search/semantic unauth'] = {"status": r.status_code}
except Exception as e:
    print("err", e)

# 4. /card unauth
banner("CARD-01  GET /card/{sid} unauth")
r = c.get(f"{API}/card/{SID}")
acc = set()
try:
    j = r.json(); allkeys(j, acc)
    sens = sorted(k for k in acc if any(s in k.lower() for s in ['email','user_id','curation','approved','task','transfer','source_path','download_url']))
    print(f"[HTTP {r.status_code}] sensitive/url keys: {sens}")
    results['CARD-01 /card unauth'] = {"status": r.status_code, "sensitive": sens}
except Exception as e:
    print("err", e, r.text[:200])

# 5. /preview unauth — internal paths / download urls?
banner("PREV-03  GET /preview/{sid} unauth")
r = c.get(f"{API}/preview/{SID}")
print(f"[HTTP {r.status_code}] body={r.text[:300]}")
results['PREV-03 /preview unauth'] = {"status": r.status_code, "snippet": r.text[:120]}

# 6. search-doc schema (sync-parity): resource_type, source_name, content layout
banner("SYNC-02 / INFRA-06  GET /search schema — resource_type & source_name parity")
r = c.get(f"{API}/search", params={"q": "steel", "limit": 1})
try:
    j = r.json()
    res = (j.get("results") or [{}])[0]
    print("result top keys:", sorted(res.keys()))
    print("source_id:", res.get("source_id"), "| has resource_type key:", 'resource_type' in json.dumps(res))
    results['search schema'] = {"keys": sorted(res.keys()), "source_id": res.get("source_id")}
except Exception as e:
    print("err", e, r.text[:200])

# 7. CORS / AllowCredentials
banner("CORS  OPTIONS /search with cross-origin")
r = c.options(f"{API}/search", headers={"Origin": "https://evil.example.com",
    "Access-Control-Request-Method": "GET"})
aco = {k: v for k, v in r.headers.items() if k.lower().startswith("access-control")}
print(f"[HTTP {r.status_code}] {aco}")
results['CORS'] = {"status": r.status_code, "headers": aco}

# 8. error envelope / 404 contract drift + security headers
banner("CONTRACT  GET /nonexistent — error envelope + security headers")
r = c.get(f"{API}/this/does/not/exist")
sec_hdrs = {k: v for k, v in r.headers.items() if k.lower() in
    ('strict-transport-security','x-content-type-options','x-frame-options','content-security-policy','x-request-id')}
print(f"[HTTP {r.status_code}] body={r.text[:160]}")
print("security headers present:", sec_hdrs or "NONE")
results['contract/404'] = {"status": r.status_code, "body": r.text[:120], "sec_headers": sec_hdrs}

banner("SUMMARY (machine-readable)")
print(json.dumps(results, indent=1)[:2000])
