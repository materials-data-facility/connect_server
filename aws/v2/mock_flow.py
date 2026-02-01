import uuid
from datetime import datetime
from typing import Any, Dict


def run_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "action_id": f"mock-{uuid.uuid4().hex}",
        "status": "SUCCEEDED",
        "submitted_at": datetime.utcnow().isoformat("T") + "Z",
    }
