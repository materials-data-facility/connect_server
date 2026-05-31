import os

from mangum import Mangum

from v2.app import app

# Strip the API Gateway stage prefix (e.g. "/dev") so FastAPI sees clean paths
_stage = os.environ.get("ENVIRONMENT", "")
handler = Mangum(app, lifespan="off", api_gateway_base_path=f"/{_stage}" if _stage else "/")

if __name__ == "__main__":
    import os
    import uvicorn

    # Load .env file if python-dotenv is installed
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    os.environ.setdefault("STORE_BACKEND", "sqlite")
    os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
    os.environ.setdefault("ASYNC_DISPATCH_MODE", "inline")
    os.environ.setdefault("AUTH_MODE", "dev")
    os.environ.setdefault("LOCAL_DEV_AUTH", "true")
    os.environ.setdefault("ALLOW_ALL_CURATORS", "true")
    os.environ.setdefault("CURATOR_GROUP_IDS", "")
    os.environ.setdefault("REQUIRED_GROUP_MEMBERSHIP", "")
    os.environ.setdefault("USE_MOCK_DATACITE", "true")

    uvicorn.run("v2.app:app", host="127.0.0.1", port=8080, reload=True)
