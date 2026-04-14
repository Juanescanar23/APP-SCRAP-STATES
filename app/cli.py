from __future__ import annotations

import argparse
from collections.abc import Iterable

from app.services.canary_report import run_canary_report
from app.services.cohort_report import run_cohort_report
from app.services.entity_cohorts import COHORT_SELECTION_VALUES
from app.workers.tasks_domains import run_domain_resolution
from app.workers.tasks_evidence import run_public_contact_collection


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    resolve_parser = subparsers.add_parser("resolve-domains", help="Resolve official domains.")
    resolve_parser.add_argument("--state", required=True)
    resolve_parser.add_argument("--limit", type=int, default=50)
    resolve_parser.add_argument("--cohort", choices=COHORT_SELECTION_VALUES, default="priority")
    resolve_parser.add_argument("--dry-run", action="store_true")

    evidence_parser = subparsers.add_parser(
        "collect-evidence",
        help="Collect public contact evidence.",
    )
    evidence_parser.add_argument("--state", required=True)
    evidence_parser.add_argument("--limit", type=int, default=50)
    evidence_parser.add_argument("--cohort", choices=COHORT_SELECTION_VALUES, default="priority")
    evidence_parser.add_argument("--verified-only", action="store_true")
    evidence_parser.add_argument("--dry-run", action="store_true")

    canary_parser = subparsers.add_parser(
        "report-canary",
        help="Summarize a Florida canary run and emit Go/No-Go guardrails.",
    )
    canary_parser.add_argument("--state", required=True)
    canary_parser.add_argument("--hours", type=int, default=24)

    cohorts_parser = subparsers.add_parser(
        "report-cohorts",
        help="Summarize Florida operational metrics by cohort.",
    )
    cohorts_parser.add_argument("--state", required=True)

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "resolve-domains":
        metrics = run_domain_resolution(
            args.state,
            limit=args.limit,
            cohort=args.cohort,
            dry_run=args.dry_run,
        )
        print_metrics("resolve-domains", metrics.as_dict())
        return 0

    if args.command == "collect-evidence":
        metrics = run_public_contact_collection(
            args.state,
            limit=args.limit,
            cohort=args.cohort,
            verified_only=args.verified_only,
            dry_run=args.dry_run,
        )
        print_metrics("collect-evidence", metrics.as_dict())
        return 0

    if args.command == "report-canary":
        report = run_canary_report(args.state, hours=args.hours)
        print_metrics("report-canary", report.as_dict())
        return 0 if report.go_ready else 1

    if args.command == "report-cohorts":
        report = run_cohort_report(args.state)
        print_metrics("report-cohorts", report.as_flat_dict())
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


def print_metrics(command: str, metrics: dict[str, object]) -> None:
    print(f"{command} summary")
    for key, value in metrics.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    raise SystemExit(main())
