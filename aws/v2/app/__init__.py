import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from v2.app.middleware import configure_app_middleware

_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, _log_level, logging.INFO))

app = FastAPI(title="MDF Connect v2")

_cors_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "*")
_cors_origins = [o.strip() for o in _cors_raw.split(",") if o.strip()] if _cors_raw != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-User-Id", "X-User-Email", "X-User-Name", "X-Globus-Token"],
    allow_credentials=True,
)
configure_app_middleware(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "mdf-v2"}


from v2.app.routers import submissions, streams, files, search, cards, curation, preview  # noqa: E402

app.include_router(submissions.router)
app.include_router(streams.router)
app.include_router(files.router)
app.include_router(search.router)
app.include_router(cards.router)
app.include_router(curation.router)
app.include_router(preview.router)
