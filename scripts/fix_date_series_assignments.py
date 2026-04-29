"""One-off in-place DB fixer for synthetic series assignments.

This script updates programmes assigned to synthetic series IDs like
``brand_pid::something`` without making any API calls.

Supported modes:

* ``clear-date`` (default): clear ``series_pid``/``series_title`` only for
    date-labelled synthetic series like ``brand_pid::26/04/2026``.
* ``collapse-brand-synthetic``: for selected ``--brand-pid`` values,
    collapse all synthetic ``brand_pid::...`` assignments into one canonical
    series per brand (``series_pid = brand_pid``, ``series_title = brand_title``).
* ``collapse-self-titled-synthetic``: collapse rows matching
    ``series_pid = brand_pid || '::' || series_title`` and
    ``title = series_title`` into one canonical series per brand.

No BBC/API calls are made. Only local SQLite rows are updated.

Usage examples:

    # Preview only (default)
    uv run python scripts/fix_date_series_assignments.py --db radio_cache.db

    # Apply changes in place
    uv run python scripts/fix_date_series_assignments.py --db radio_cache.db --apply

    # Apply date-only cleanup and create a backup file first
    uv run python scripts/fix_date_series_assignments.py --db radio_cache.db --apply --backup

    # Collapse all Archers synthetic series into canonical brand series
    uv run python scripts/fix_date_series_assignments.py --db radio_cache.db --mode collapse-brand-synthetic --brand-pid b006qpgr --apply --backup

    # Collapse self-titled synthetic series rows across all brands
    uv run python scripts/fix_date_series_assignments.py --db radio_cache.db --mode collapse-self-titled-synthetic --apply --backup
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_DATE_LABEL_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")


@dataclass(frozen=True, slots=True)
class CandidateRow:
    pid: str
    title: str
    brand_pid: str
    brand_title: str
    series_pid: str
    series_title: str
    target_series_pid: str
    target_series_title: str


def _load_candidates(
    conn: sqlite3.Connection,
    mode: str,
    brand_pids: set[str],
) -> list[CandidateRow]:
    rows = conn.execute(
        """
        SELECT pid, title, brand_pid, brand_title, series_pid, series_title
        FROM programmes
        WHERE brand_pid != ''
        """
    ).fetchall()

    candidates: list[CandidateRow] = []
    for row in rows:
        pid = str(row[0] or "")
        title = str(row[1] or "")
        brand_pid = str(row[2] or "")
        brand_title = str(row[3] or "")
        series_pid = str(row[4] or "")
        series_title = str(row[5] or "")

        prefix = f"{brand_pid}::"

        if mode == "clear-date":
            if not series_pid or not series_pid.startswith(prefix):
                continue
            suffix = series_pid[len(prefix) :]
            if suffix != series_title:
                continue
            if not _DATE_LABEL_RE.match(series_title):
                continue
            target_series_pid = ""
            target_series_title = ""
        elif mode == "collapse-brand-synthetic":
            if not brand_pids or brand_pid not in brand_pids:
                continue
            if series_pid:
                if not series_pid.startswith(prefix):
                    continue
                suffix = series_pid[len(prefix) :]
                if suffix != series_title:
                    continue
            target_series_pid = brand_pid
            target_series_title = brand_title
        else:
            # collapse-self-titled-synthetic
            if not series_pid or not series_title:
                continue
            if not series_pid.startswith(prefix):
                continue
            suffix = series_pid[len(prefix) :]
            if suffix != series_title:
                continue
            if title != series_title:
                continue
            target_series_pid = brand_pid
            target_series_title = brand_title

        if series_pid == target_series_pid and series_title == target_series_title:
            continue

        candidates.append(
            CandidateRow(
                pid=pid,
                title=title,
                brand_pid=brand_pid,
                brand_title=brand_title,
                series_pid=series_pid,
                series_title=series_title,
                target_series_pid=target_series_pid,
                target_series_title=target_series_title,
            )
        )

    return candidates


def _print_summary(candidates: list[CandidateRow]) -> None:
    print(f"Candidate episodes to rewrite: {len(candidates)}")
    if not candidates:
        return

    by_brand: dict[tuple[str, str], int] = {}
    for c in candidates:
        key = (c.brand_pid, c.brand_title)
        by_brand[key] = by_brand.get(key, 0) + 1

    print("Affected brands:")
    for (brand_pid, brand_title), count in sorted(
        by_brand.items(), key=lambda kv: (-kv[1], kv[0][1].lower())
    ):
        label = brand_title or brand_pid
        print(f"  {label} ({brand_pid}): {count}")

    print("Sample rows:")
    for c in candidates[:10]:
        print(f"  {c.pid}: {c.series_pid}  [{c.title}]")


def _apply(conn: sqlite3.Connection, candidates: list[CandidateRow]) -> int:
    if not candidates:
        return 0

    now = datetime.now(UTC).isoformat()
    with conn:
        conn.executemany(
            """
            UPDATE programmes
            SET series_pid = ?,
                series_title = ?,
                updated_at = ?
            WHERE pid = ?
            """,
            [
                (
                    c.target_series_pid,
                    c.target_series_title,
                    now,
                    c.pid,
                )
                for c in candidates
            ],
        )
    return len(candidates)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rewrite date-labelled synthetic series (brand::DD/MM/YYYY) to "
            "brand-level episodes in a local radio_cache SQLite DB."
        )
    )
    parser.add_argument(
        "--db",
        default="radio_cache.db",
        help="Path to SQLite database file (default: radio_cache.db)",
    )
    parser.add_argument(
        "--mode",
        choices=[
            "clear-date",
            "collapse-brand-synthetic",
            "collapse-self-titled-synthetic",
        ],
        default="clear-date",
        help=(
            "Fix strategy: 'clear-date' clears only date-labelled synthetic "
            "series; 'collapse-brand-synthetic' collapses all synthetic "
            "brand::... series into a canonical series per --brand-pid; "
            "'collapse-self-titled-synthetic' collapses rows where "
            "title == series_title and series_pid matches brand::series_title."
        ),
    )
    parser.add_argument(
        "--brand-pid",
        action="append",
        default=[],
        help=(
            "Target brand PID (repeatable). Required for mode "
            "'collapse-brand-synthetic'."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the update in place (default is dry-run)",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create '<db>.bak' before applying updates",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)

    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 1

    if args.mode == "collapse-brand-synthetic" and not args.brand_pid:
        print("--brand-pid is required when --mode collapse-brand-synthetic")
        return 1

    if args.apply and args.backup:
        backup_path = db_path.with_suffix(db_path.suffix + ".bak")
        shutil.copy2(db_path, backup_path)
        print(f"Backup created: {backup_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        target_brands = {pid.strip() for pid in args.brand_pid if pid.strip()}
        candidates = _load_candidates(conn, mode=args.mode, brand_pids=target_brands)
        print(f"Mode: {args.mode}")
        if target_brands:
            print("Target brands:")
            for pid in sorted(target_brands):
                print(f"  {pid}")
        _print_summary(candidates)

        if not args.apply:
            print("Dry-run only. Re-run with --apply to persist changes.")
            return 0

        updated = _apply(conn, candidates)
        print(f"Updated rows: {updated}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
