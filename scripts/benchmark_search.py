"""Benchmark core search query paths against the local SQLite cache.

This script helps detect regressions in search performance over time by timing
both the classic programme search path and the grouped search path used by the
search results page.

Examples:
    c:/Users/Mich/radiocache/.venv/Scripts/python.exe scripts/benchmark_search.py
    c:/Users/Mich/radiocache/.venv/Scripts/python.exe scripts/benchmark_search.py --query drama --query the --iterations 5
    c:/Users/Mich/radiocache/.venv/Scripts/python.exe scripts/benchmark_search.py --category audiobooks
"""

from __future__ import annotations

import argparse
import statistics
import time
from dataclasses import dataclass

from radio_cache.cache_db import CacheDB
from radio_cache.search import (
    search_by_groups,
    search_groups_count,
    search_programmes,
    search_programmes_count,
)


@dataclass(slots=True)
class BenchResult:
    """Timing summary for one benchmarked operation."""

    operation: str
    query: str
    category: str
    rows_or_count: int
    avg_ms: float
    p50_ms: float
    p95_ms: float
    min_ms: float
    max_ms: float


def _percentile(values: list[float], percentile: float) -> float:
    """Return percentile using linear interpolation between sample points."""
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _time_call(iterations: int, fn: callable[[], int]) -> tuple[int, list[float]]:
    """Run and time a callable multiple times.

    Returns:
        A tuple of (last_result_value, list_of_durations_ms).
    """
    durations_ms: list[float] = []
    value = 0
    for _ in range(iterations):
        start = time.perf_counter()
        value = fn()
        end = time.perf_counter()
        durations_ms.append((end - start) * 1000.0)
    return value, durations_ms


def _summarise(
    operation: str,
    query: str,
    category: str,
    rows_or_count: int,
    durations_ms: list[float],
) -> BenchResult:
    """Build a benchmark summary row."""
    return BenchResult(
        operation=operation,
        query=query,
        category=category,
        rows_or_count=rows_or_count,
        avg_ms=statistics.fmean(durations_ms),
        p50_ms=_percentile(durations_ms, 0.50),
        p95_ms=_percentile(durations_ms, 0.95),
        min_ms=min(durations_ms),
        max_ms=max(durations_ms),
    )


def run_benchmark(
    db_path: str,
    queries: list[str],
    category: str,
    limit: int,
    offset: int,
    iterations: int,
    warmup: int,
    sort: str,
) -> list[BenchResult]:
    """Execute benchmark suite and return timing summaries."""
    results: list[BenchResult] = []

    with CacheDB(db_path) as db:
        print(f"DB: {db_path} | programmes={db.programme_count()}")
        print(
            "Ops: old_count=search_programmes_count, "
            "old_page=search_programmes, "
            "new_count=search_groups_count, "
            "new_page=search_by_groups"
        )

        for query in queries:
            query = query.strip()
            if not query:
                continue

            print(f"\nQuery: {query!r} | category={category!r}")

            for _ in range(warmup):
                search_programmes_count(db, query, category=category)
                search_programmes(db, query, limit=limit, offset=offset, category=category)
                search_groups_count(db, query, category=category)
                search_by_groups(
                    db,
                    query,
                    limit=limit,
                    offset=offset,
                    category=category,
                    sort=sort,
                )

            count_old, t_count_old = _time_call(
                iterations,
                lambda: search_programmes_count(db, query, category=category),
            )
            rows_old, t_rows_old = _time_call(
                iterations,
                lambda: len(
                    search_programmes(
                        db,
                        query,
                        limit=limit,
                        offset=offset,
                        category=category,
                    )
                ),
            )
            count_new, t_count_new = _time_call(
                iterations,
                lambda: search_groups_count(db, query, category=category),
            )
            rows_new, t_rows_new = _time_call(
                iterations,
                lambda: len(
                    search_by_groups(
                        db,
                        query,
                        limit=limit,
                        offset=offset,
                        category=category,
                        sort=sort,
                    )
                ),
            )

            results.extend(
                [
                    _summarise("old_count", query, category, count_old, t_count_old),
                    _summarise("old_page", query, category, rows_old, t_rows_old),
                    _summarise("new_count", query, category, count_new, t_count_new),
                    _summarise("new_page", query, category, rows_new, t_rows_new),
                ]
            )

    return results


def _print_table(results: list[BenchResult]) -> None:
    """Print benchmark results in a compact table."""
    headers = (
        "query",
        "operation",
        "rows/count",
        "avg_ms",
        "p50_ms",
        "p95_ms",
        "min_ms",
        "max_ms",
    )
    print("\n" + " | ".join(headers))
    print("-" * 106)
    for row in results:
        print(
            f"{row.query:<16} | "
            f"{row.operation:<9} | "
            f"{row.rows_or_count:>10} | "
            f"{row.avg_ms:>7.1f} | "
            f"{row.p50_ms:>7.1f} | "
            f"{row.p95_ms:>7.1f} | "
            f"{row.min_ms:>7.1f} | "
            f"{row.max_ms:>7.1f}"
        )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="radio_cache.db", help="Path to SQLite DB file")
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Query term to benchmark (repeatable). Defaults to a mixed set.",
    )
    parser.add_argument(
        "--category",
        default="",
        help="Optional category filter (same as /search?category=...)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Page size passed to search functions",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset passed to search functions",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Timed iterations per operation",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup iterations (not included in results)",
    )
    parser.add_argument(
        "--sort",
        default="relevance",
        choices=[
            "relevance",
            "title-asc",
            "title-desc",
            "date-desc",
            "date-asc",
            "duration-desc",
            "duration-asc",
        ],
        help="Sort order for grouped search path",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    queries = args.queries or ["mystery", "drama", "radio", "the"]
    results = run_benchmark(
        db_path=args.db,
        queries=queries,
        category=args.category,
        limit=args.limit,
        offset=args.offset,
        iterations=max(1, args.iterations),
        warmup=max(0, args.warmup),
        sort=args.sort,
    )
    _print_table(results)


if __name__ == "__main__":
    main()
