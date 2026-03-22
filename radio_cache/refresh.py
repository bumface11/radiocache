"""Cache refresh script for the BBC Radio Drama cache.

Can be run as a CLI command or called programmatically.  Designed to
be executed by a GitHub Actions cron job once per day.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from radio_cache.bbc_feed_parser import fetch_drama_programmes
from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme

logger = logging.getLogger(__name__)


def refresh_cache(
    db_path: str = "radio_cache.db",
    export_json: bool = True,
    json_path: str = "radio_cache_export.json",
    purge_expired: bool = True,
) -> int:
    """Refresh the programme cache from BBC feeds.

    Args:
        db_path: Path to the SQLite database file.
        export_json: Whether to also export a static JSON snapshot.
        json_path: Output path for the JSON export.
        purge_expired: Whether to remove expired programmes.

    Returns:
        Number of programmes in the updated cache.
    """
    logger.info("Starting cache refresh -> %s", db_path)

    programmes = fetch_drama_programmes()
    logger.info("Fetched %d programmes from BBC feeds", len(programmes))

    with CacheDB(db_path) as db:
        upserted = db.upsert_programmes(programmes)
        logger.info("Upserted %d programmes", upserted)

        if purge_expired:
            purged = db.purge_expired()
            if purged:
                logger.info("Purged %d expired programmes", purged)

        now = datetime.now(UTC).isoformat()
        db.set_meta("last_refreshed", now)

        stats = db.stats()
        logger.info(
            "Cache stats: %d programmes, %d series, %d brands",
            stats.total_programmes,
            stats.total_series,
            stats.total_brands,
        )

        if export_json:
            _export_json(db, json_path)

        return stats.total_programmes


def _export_json(db: CacheDB, json_path: str) -> None:
    """Export the entire cache as a static JSON file.

    The JSON file can be hosted on GitHub Pages or any static file
    host for cheap, serverless access.

    Args:
        db: Open cache database.
        json_path: Output file path.
    """
    programmes = db.all_programmes()
    data = {
        "meta": {
            "last_refreshed": db.get_meta("last_refreshed"),
            "total_programmes": len(programmes),
        },
        "programmes": [_programme_to_dict(p) for p in programmes],
    }

    path = Path(json_path)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Exported %d programmes to %s", len(programmes), json_path)


def _programme_to_dict(prog: Programme) -> dict:
    """Convert a programme to a JSON-serialisable dict.

    Args:
        prog: Programme instance.

    Returns:
        Dictionary representation.
    """
    return {
        "pid": prog.pid,
        "title": prog.title,
        "synopsis": prog.synopsis,
        "duration_secs": prog.duration_secs,
        "available_until": prog.available_until,
        "first_broadcast": prog.first_broadcast,
        "programme_type": prog.programme_type,
        "series_pid": prog.series_pid,
        "series_title": prog.series_title,
        "brand_pid": prog.brand_pid,
        "brand_title": prog.brand_title,
        "episode_number": prog.episode_number,
        "channel": prog.channel,
        "thumbnail_url": prog.thumbnail_url,
        "categories": prog.categories,
        "url": prog.url,
    }


def import_from_json(
    json_path: str,
    db_path: str = "radio_cache.db",
) -> int:
    """Import programmes from a JSON export file into the cache.

    Args:
        json_path: Path to the JSON file.
        db_path: Path to the SQLite database file.

    Returns:
        Number of programmes imported.
    """
    path = Path(json_path)
    data = json.loads(path.read_text(encoding="utf-8"))

    programmes = [
        Programme(**{k: v for k, v in item.items() if k != "updated_at"})
        for item in data.get("programmes", [])
    ]

    with CacheDB(db_path) as db:
        count = db.upsert_programmes(programmes)
        now = datetime.now(UTC).isoformat()
        db.set_meta("last_refreshed", now)
        logger.info("Imported %d programmes from %s", count, json_path)
        return count


def main() -> None:
    """CLI entry point for cache refresh."""
    parser = argparse.ArgumentParser(
        description="Refresh the BBC Radio Drama programme cache"
    )
    parser.add_argument(
        "--db",
        default="radio_cache.db",
        help="SQLite database path (default: radio_cache.db)",
    )
    parser.add_argument(
        "--json",
        default="radio_cache_export.json",
        help="JSON export path (default: radio_cache_export.json)",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip JSON export",
    )
    parser.add_argument(
        "--import-json",
        metavar="FILE",
        help="Import from JSON file instead of fetching from BBC",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.import_json:
        count = import_from_json(args.import_json, args.db)
        logger.info("Imported %d programmes", count)
    else:
        count = refresh_cache(
            db_path=args.db,
            export_json=not args.no_json,
            json_path=args.json,
        )
        logger.info("Cache contains %d programmes", count)


if __name__ == "__main__":
    main()
