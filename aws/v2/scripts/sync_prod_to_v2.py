#!/usr/bin/env python3
"""Run the unattended legacy-v1 to v2 delta synchronization pipeline."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LOCK_MAX_AGE = timedelta(hours=2)
REPORT_COUNT_KEYS = (
    "created",
    "updated",
    "unchanged",
    "conflicts",
    "search_pending",
    "store_errors",
    "search_errors",
)


class LockHeldError(RuntimeError):
    """Raised when a live synchronization lock already exists."""


def _now():
    return datetime.now(timezone.utc)


def _iso(value):
    return value.astimezone(timezone.utc).isoformat()


def _parse_iso(value):
    candidate = str(value or "").strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _error_code(exc):
    response = getattr(exc, "response", {}) or {}
    return (response.get("Error") or {}).get("Code")


def _parameter_name(env, suffix):
    return "/mdf/{}/{}".format(env, suffix)


def acquire_ssm_lock(ssm, env, run_source, now=None):
    """Acquire the SSM lock, evicting a holder older than two hours."""
    now = now or _now()
    name = _parameter_name(env, "sync-lock")
    value = json.dumps(
        {"acquired_at": _iso(now), "run_source": run_source},
        separators=(",", ":"),
    )
    try:
        ssm.put_parameter(
            Name=name, Value=value, Type="String", Overwrite=False
        )
        return
    except Exception as exc:
        if _error_code(exc) != "ParameterAlreadyExists":
            raise

    try:
        current = json.loads(
            ssm.get_parameter(Name=name)["Parameter"]["Value"]
        )
        acquired_at = _parse_iso(current.get("acquired_at"))
    except Exception:
        raise LockHeldError("sync lock is held and its age cannot be verified")

    if now - acquired_at <= LOCK_MAX_AGE:
        raise LockHeldError(
            "sync lock is held since {} by {}".format(
                current.get("acquired_at"), current.get("run_source", "unknown")
            )
        )

    ssm.delete_parameter(Name=name)
    try:
        ssm.put_parameter(
            Name=name, Value=value, Type="String", Overwrite=False
        )
    except Exception as exc:
        if _error_code(exc) == "ParameterAlreadyExists":
            raise LockHeldError("sync lock was acquired by another runner")
        raise


def release_ssm_lock(ssm, env):
    """Release this environment's synchronization lock."""
    ssm.delete_parameter(Name=_parameter_name(env, "sync-lock"))


def _local_paths(state_dir):
    state = Path(state_dir)
    return {
        "lock": state / "sync-lock",
        "watermark": state / "sync-watermark",
        "report": state / "sync-last-report",
    }


def acquire_local_lock(state_dir, run_source, now=None):
    """Acquire the developer lock with atomic O_EXCL semantics."""
    now = now or _now()
    paths = _local_paths(state_dir)
    paths["lock"].parent.mkdir(parents=True, exist_ok=True)
    value = json.dumps(
        {"acquired_at": _iso(now), "run_source": run_source},
        separators=(",", ":"),
    )

    def create():
        fd = os.open(
            str(paths["lock"]), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(fd, "w") as handle:
            handle.write(value)

    try:
        create()
        return
    except FileExistsError:
        pass

    try:
        current = json.loads(paths["lock"].read_text())
        acquired_at = _parse_iso(current.get("acquired_at"))
    except Exception:
        raise LockHeldError("local sync lock is held and its age cannot be verified")

    if now - acquired_at <= LOCK_MAX_AGE:
        raise LockHeldError(
            "local sync lock is held since {} by {}".format(
                current.get("acquired_at"), current.get("run_source", "unknown")
            )
        )

    try:
        paths["lock"].unlink()
        create()
    except FileNotFoundError:
        try:
            create()
        except FileExistsError:
            raise LockHeldError("local sync lock was acquired by another runner")
    except FileExistsError:
        raise LockHeldError("local sync lock was acquired by another runner")


def release_local_lock(state_dir):
    """Release a local synchronization lock if it still exists."""
    try:
        _local_paths(state_dir)["lock"].unlink()
    except FileNotFoundError:
        pass


def _get_ssm_value(ssm, name):
    try:
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception as exc:
        if _error_code(exc) == "ParameterNotFound":
            return None
        raise


def read_watermark(ssm, env):
    return _get_ssm_value(ssm, _parameter_name(env, "sync-watermark"))


def write_ssm_watermark(ssm, env, watermark):
    ssm.put_parameter(
        Name=_parameter_name(env, "sync-watermark"),
        Value=watermark,
        Type="String",
        Overwrite=True,
    )


def write_ssm_report(ssm, env, report):
    """Write the pinned last-report payload to Parameter Store."""
    ssm.put_parameter(
        Name=_parameter_name(env, "sync-last-report"),
        Value=json.dumps(report, separators=(",", ":"), default=str),
        Type="String",
        Overwrite=True,
    )


def _read_local_watermark(state_dir):
    path = _local_paths(state_dir)["watermark"]
    return path.read_text().strip() if path.exists() else None


def _atomic_write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=".{}-".format(path.name), dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(value)
        os.replace(temporary, str(path))
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _run_script(name, arguments):
    command = [sys.executable, str(SCRIPT_DIR / name)] + list(arguments)
    return subprocess.run(command, text=True, capture_output=True, check=False)


def _count(value):
    return len(value) if isinstance(value, list) else int(value or 0)


def _report_counts(ingest_report):
    counts = {key: 0 for key in REPORT_COUNT_KEYS}
    for key in REPORT_COUNT_KEYS:
        counts[key] = _count(ingest_report.get(key))
    return counts


def _print_summary(report, exit_code, error_detail=None):
    counts = report["counts"]
    print("\nMDF v1 -> v2 synchronization")
    print("  Status:         {}".format(report["status"]))
    print("  Source:         {}".format(report["run_source"]))
    print("  Started:        {}".format(report["started_at"]))
    print("  Finished:       {}".format(report["finished_at"]))
    print("  Created:        {}".format(counts["created"]))
    print("  Updated:        {}".format(counts["updated"]))
    print("  Unchanged:      {}".format(counts["unchanged"]))
    print("  Conflicts:      {}".format(counts["conflicts"]))
    print("  Search pending: {}".format(counts["search_pending"]))
    print("  Store errors:   {}".format(counts["store_errors"]))
    print("  Search errors:  {}".format(counts["search_errors"]))
    print("  Watermark:      {}".format(report["watermark"] or "(none)"))
    print("  Exit code:      {}".format(exit_code))
    if error_detail:
        first_line = next(
            (
                line.strip()
                for line in error_detail.splitlines()
                if line.strip()
            ),
            "unknown pipeline error",
        )
        print("  Error:          {}".format(first_line[:180]))


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env", required=True, choices=["dev", "staging", "prod"]
    )
    parser.add_argument("--run-source", default="manual")
    parser.add_argument("--no-ssm", action="store_true")
    parser.add_argument("--state-dir", default=".sync-state")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--report-file")
    parser.add_argument(
        "--from-extract-file",
        help="Use an existing extract JSON instead of querying legacy Search.",
    )
    parser.add_argument(
        "--local-config",
        action="store_true",
        help="Use current environment configuration instead of CloudFormation.",
    )
    return parser


def run(args):
    started = _now()
    ssm = None
    acquired = False
    report = None
    exit_code = 1
    error_detail = None

    try:
        if args.no_ssm:
            acquire_local_lock(args.state_dir, args.run_source, now=started)
        else:
            import boto3

            ssm = boto3.client("ssm", region_name="us-east-1")
            acquire_ssm_lock(ssm, args.env, args.run_source, now=started)
        acquired = True
    except LockHeldError as exc:
        finished = _now()
        report = {
            "status": "errors",
            "started_at": _iso(started),
            "finished_at": _iso(finished),
            "counts": {key: 0 for key in REPORT_COUNT_KEYS},
            "watermark": None,
            "run_source": args.run_source,
        }
        if args.json:
            print(json.dumps(report, separators=(",", ":")))
        else:
            _print_summary(report, 4, str(exc))
        return 4

    try:
        watermark = (
            _read_local_watermark(args.state_dir)
            if args.no_ssm
            else read_watermark(ssm, args.env)
        )
        ingest_report = {}
        with tempfile.TemporaryDirectory(prefix="mdf-v1-v2-sync-") as workdir:
            work = Path(workdir)
            extract_file = work / "extract.json"
            converted_file = work / "converted.json"
            ingest_report_file = work / "ingest-report.json"
            candidate_file = work / "candidate-watermark"

            if args.from_extract_file:
                shutil.copyfile(args.from_extract_file, str(extract_file))
            else:
                extract_args = [
                    "--output",
                    str(extract_file),
                    "--non-interactive",
                ]
                if watermark:
                    extract_args.extend(["--since", watermark])
                result = _run_script(
                    "extract_mdf_production_datasets.py", extract_args
                )
                if result.returncode:
                    raise RuntimeError(
                        "extract failed ({}): {}".format(
                            result.returncode, result.stderr or result.stdout
                        )
                    )

            result = _run_script(
                "convert_production_datasets.py",
                ["--input", str(extract_file), "--output", str(converted_file)],
            )
            if result.returncode:
                raise RuntimeError(
                    "convert failed ({}): {}".format(
                        result.returncode, result.stderr or result.stdout
                    )
                )

            ingest_args = [
                "--input",
                str(converted_file),
                "--extract-file",
                str(extract_file),
                "--watermark-file",
                str(candidate_file),
                "--report-file",
                str(ingest_report_file),
            ]
            if not args.local_config:
                ingest_args.extend(["--env", args.env])
            if args.dry_run:
                ingest_args.append("--dry-run")
            ingest_result = _run_script(
                "ingest_converted_datasets.py", ingest_args
            )
            if ingest_report_file.exists():
                ingest_report = json.loads(ingest_report_file.read_text())
            if ingest_result.returncode not in (0, 3):
                raise RuntimeError(
                    "ingest failed ({}): {}".format(
                        ingest_result.returncode,
                        ingest_result.stderr or ingest_result.stdout,
                    )
                )

            reconcile_args = ["--input", str(converted_file), "--json"]
            if not args.local_config:
                reconcile_args.extend(["--env", args.env])
            reconcile_result = _run_script(
                "reconcile_migration.py", reconcile_args
            )

            # A preview necessarily differs from the current store; retain its
            # would-change counts without treating that expected drift as a
            # failed dry run.
            # Reconcile reports the deliberately preserved, user-owned
            # conflict records as stale content. That exit 1 is the same
            # conflicts-only outcome, not an additional pipeline error.
            reconcile_failed = (
                reconcile_result.returncode != 0
                and not args.dry_run
                and not (
                    ingest_result.returncode == 3
                    and reconcile_result.returncode == 1
                )
            )
            if reconcile_failed:
                raise RuntimeError(
                    "reconcile detected drift ({}): {}".format(
                        reconcile_result.returncode,
                        reconcile_result.stderr or reconcile_result.stdout,
                    )
                )

            new_watermark = (
                candidate_file.read_text().strip()
                if candidate_file.exists()
                else None
            )
            effective_watermark = new_watermark or watermark
            exit_code = ingest_result.returncode
            status = "conflicts" if exit_code == 3 else "success"

            if new_watermark and not args.dry_run:
                if args.no_ssm:
                    _atomic_write(
                        _local_paths(args.state_dir)["watermark"], new_watermark
                    )
                else:
                    write_ssm_watermark(ssm, args.env, new_watermark)

            report = {
                "status": status,
                "started_at": _iso(started),
                "finished_at": _iso(_now()),
                "counts": _report_counts(ingest_report),
                "watermark": effective_watermark,
                "run_source": args.run_source,
            }
    except Exception as exc:
        error_detail = str(exc)
        report = {
            "status": "errors",
            "started_at": _iso(started),
            "finished_at": _iso(_now()),
            "counts": _report_counts(locals().get("ingest_report", {})),
            "watermark": locals().get("watermark"),
            "run_source": args.run_source,
        }
        exit_code = 1
    finally:
        if report is not None:
            serialized = json.dumps(report, indent=2, default=str)
            if args.report_file:
                _atomic_write(args.report_file, serialized + "\n")
            if args.no_ssm:
                _atomic_write(
                    _local_paths(args.state_dir)["report"], serialized + "\n"
                )
            elif ssm is not None and acquired and not args.dry_run:
                try:
                    write_ssm_report(ssm, args.env, report)
                except Exception as exc:
                    report["status"] = "errors"
                    report["finished_at"] = _iso(_now())
                    error_detail = "last-report write failed: {}".format(exc)
                    exit_code = 1
        if acquired:
            try:
                if args.no_ssm:
                    release_local_lock(args.state_dir)
                else:
                    release_ssm_lock(ssm, args.env)
            except Exception as exc:
                report["status"] = "errors"
                report["finished_at"] = _iso(_now())
                error_detail = "lock release failed: {}".format(exc)
                exit_code = 1

    if args.json:
        print(json.dumps(report, separators=(",", ":"), default=str))
    else:
        _print_summary(report, exit_code, error_detail)
    return exit_code


def main():
    sys.exit(run(_parser().parse_args()))


if __name__ == "__main__":
    main()
