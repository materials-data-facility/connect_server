import argparse
import json
import logging
import time
from typing import Any, Dict

from v2.async_jobs import handle_sqs_event, run_sqlite_worker_once

logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # EventBridge scheduled events have "source": "aws.events"
    if event.get("source") == "aws.events":
        from v2.async_jobs import JOB_CLEANUP_TRANSFERS, process_job

        logger.info("Handling EventBridge scheduled event: %s", event.get("detail-type"))
        return process_job(JOB_CLEANUP_TRANSFERS, {})

    return handle_sqs_event(event)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MDF async job worker")
    parser.add_argument("--once", action="store_true", help="Run one pass and exit")
    parser.add_argument("--limit", type=int, default=20, help="Max pending sqlite jobs to process per pass")
    parser.add_argument("--interval", type=float, default=2.0, help="Sleep interval between passes")
    args = parser.parse_args()

    if args.once:
        print(json.dumps(run_sqlite_worker_once(limit=args.limit), indent=2))
        return

    while True:
        result = run_sqlite_worker_once(limit=args.limit)
        print(json.dumps(result))
        if result["total_claimed"] == 0:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
