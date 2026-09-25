import argparse
import json
import logging
import time
from typing import Any, Dict

from v2.async_jobs import handle_sqs_event, run_sqlite_worker_once
from v2.config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # EventBridge scheduled events have "source": "aws.events". Each schedule in
    # template.yaml passes an Input naming its job; a schedule without one is
    # the original transfer-cleanup rule.
    if event.get("source") == "aws.events" or "job" in event:
        from v2.async_jobs import (
            JOB_CLEANUP_TRANSFERS, JOB_LINK_HEALTH_SWEEP, fanout_time_budget, process_job,
        )

        job = event.get("job") or JOB_CLEANUP_TRANSFERS
        allowed = {JOB_CLEANUP_TRANSFERS, JOB_LINK_HEALTH_SWEEP}
        if job not in allowed:
            logger.warning("Ignoring scheduled event for unknown job %r", job)
            return {"ignored": job}
        logger.info("Handling scheduled job %s (%s)", job, event.get("detail-type"))
        with fanout_time_budget(context):
            return process_job(job, dict(event.get("payload") or {}))

    return handle_sqs_event(event, context)


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
