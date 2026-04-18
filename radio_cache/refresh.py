"""Cache refresh script for the BBC Radio Drama cache.

Can be run as a CLI command or called programmatically.  Designed to
be executed by a GitHub Actions cron job once per day.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import shutil
import sqlite3
import tempfile
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal

from radio_cache.bbc_feed_parser import (
    fetch_all_category_slugs,
    fetch_category_counts,
    fetch_drama_programmes,
)
from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme

logger = logging.getLogger(__name__)

RefreshDepth = Literal["recent", "full"]

_RECENT_MAX_PAGES: Final[int] = 3
_FULL_MAX_PAGES: Final[int] = 50

_DEFAULT_GITHUB_URL: str = (
    "https://raw.githubusercontent.com/bumface11/radiocache/main/"
    "radio_cache_export.json"
)
_DEFAULT_DB_SNAPSHOT_URL: str = (
    "https://github.com/bumface11/radiocache/releases/latest/download/"
    "radio_cache.db.zip"
)


def refresh_cache(
    db_path: str = "radio_cache.db",
    export_json: bool = True,
    json_path: str = "radio_cache_export.json",
    db_snapshot_path: str = "",
    purge_expired: bool = True,
    export_get_iplayer: bool = True,
    get_iplayer_path: str = "radio.cache",
    category_slugs: list[str] | None = None,
    all_categories: bool = False,
    depth: RefreshDepth = "full",
) -> int:
    """Refresh the programme cache from BBC feeds.

    Args:
        db_path: Path to the SQLite database file.
        export_json: Whether to also export a static JSON snapshot.
        json_path: Output path for the JSON export.
        db_snapshot_path: Optional SQLite snapshot output path.  When the
            path ends in ``.zip``, a compressed archive containing the
            database file is written.
        purge_expired: Whether to remove expired programmes.
        export_get_iplayer: Whether to export a get_iplayer-compatible cache file.
        get_iplayer_path: Output path for the get_iplayer cache file.
        category_slugs: Specific category slugs to fetch.  When ``None``
            and *all_categories* is ``False``, the built-in drama slugs
            are used (default behaviour).
        all_categories: When ``True``, fetch every category slug
            discovered by the BBC categories API, overriding
            *category_slugs*.
        depth: Controls how many pages to fetch per category.
            ``"recent"`` fetches only the first few pages (newest
            broadcasts), suitable for daily refreshes.  ``"full"``
            pages through the entire back-catalogue, capturing
            long-running shows like *In Our Time*.  The ``"full"``
            depth also enables container backfill, which fetches
            every episode of each discovered brand.

    Returns:
        Number of programmes in the updated cache.
    """
    max_pages = _RECENT_MAX_PAGES if depth == "recent" else _FULL_MAX_PAGES
    backfill = depth == "full"
    logger.info(
        "Starting %s cache refresh -> %s (max %d pages/category, backfill=%s)",
        depth, db_path, max_pages, backfill,
    )

    slugs: list[str] | None = category_slugs
    if all_categories:
        slugs = fetch_all_category_slugs()
        logger.info("Using all %d category slugs from BBC API", len(slugs))

    programmes = fetch_drama_programmes(
        category_slugs=slugs,
        max_pages=max_pages,
        backfill_containers=backfill,
        existing_pids=(
            _existing_valid_pids(db_path)
            if depth == "recent"
            else None
        ),
    )
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
        db.set_meta(f"last_refreshed_{depth}", now)

        stats = db.stats()
        logger.info(
            "Cache stats: %d programmes, %d series, %d brands",
            stats.total_programmes,
            stats.total_series,
            stats.total_brands,
        )

        if export_json:
            _export_json(db, json_path)

        if export_get_iplayer:
            _export_get_iplayer_cache(db, get_iplayer_path)

        total_programmes = stats.total_programmes

    if db_snapshot_path:
        export_db_snapshot(db_path, db_snapshot_path)

    return total_programmes


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


def _existing_valid_pids(db_path: str) -> set[str]:
    """Return currently cached valid PIDs when a DB already exists."""
    path = Path(db_path)
    if not path.exists():
        return set()

    with CacheDB(db_path) as db:
        return db.valid_pids()


def _export_get_iplayer_cache(db: CacheDB, cache_path: str) -> None:
    """Export the cache as a get_iplayer-compatible pipe-delimited file.

    Writes the native get_iplayer v3.36 format::

        #index|type|name|episode|seriesnum|episodenum|pid|channel|available|expires|duration|desc|web|thumbnail|timeadded

    Args:
        db: Open cache database.
        cache_path: Output file path.
    """
    count = db.export_get_iplayer_cache(cache_path)
    logger.info("Exported %d programmes to get_iplayer cache %s", count, cache_path)


def _copy_sqlite_database(source_path: Path, dest_path: Path) -> None:
    """Create a consistent SQLite copy using the backup API."""
    if source_path.resolve() == dest_path.resolve():
        return

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists():
        dest_path.unlink()

    source_conn = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    dest_conn = sqlite3.connect(dest_path)
    try:
        source_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        source_conn.close()


def _cleanup_sqlite_sidecars(db_path: Path) -> None:
    """Remove stale SQLite sidecar files before replacing the database."""
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()


def export_db_snapshot(db_path: str, snapshot_path: str) -> None:
    """Export a consistent SQLite snapshot.

    Args:
        db_path: Source SQLite database path.
        snapshot_path: Destination path for the snapshot.  A ``.zip``
            suffix writes a compressed archive containing the database.
    """
    source = Path(db_path)
    target = Path(snapshot_path)

    if not source.exists():
        raise FileNotFoundError(source)

    if source.resolve() == target.resolve():
        logger.info(
            "Skipping DB snapshot export because source and target match: %s",
            source,
        )
        return

    target.parent.mkdir(parents=True, exist_ok=True)

    if target.suffix.lower() == ".zip":
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_db = Path(tmpdir) / source.name
            _copy_sqlite_database(source, temp_db)
            with zipfile.ZipFile(
                target,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            ) as archive:
                archive.write(temp_db, arcname=source.name)
    else:
        _copy_sqlite_database(source, target)

    logger.info("Exported SQLite snapshot to %s", target)


def _restore_db_snapshot_file(snapshot_file: Path, db_path: str, source: str) -> int:
    """Replace the target database with a snapshot file and return its row count."""
    target = Path(db_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    _cleanup_sqlite_sidecars(target)
    shutil.copyfile(snapshot_file, target)

    with CacheDB(str(target)) as db:
        count = db.programme_count()

    logger.info("Imported %d programmes from DB snapshot %s", count, source)
    return count


def _extract_snapshot_archive(raw: bytes, temp_dir: Path) -> Path:
    """Extract a zipped DB snapshot and return the extracted DB path."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = [
            info
            for info in archive.infolist()
            if not info.is_dir()
        ]
        if not members:
            raise ValueError("DB snapshot archive is empty")

        preferred = next(
            (info for info in members if info.filename.lower().endswith(".db")),
            members[0],
        )
        extracted = temp_dir / Path(preferred.filename).name
        with archive.open(preferred) as src, extracted.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        return extracted


def import_from_db_snapshot(
    snapshot_path: str,
    db_path: str = "radio_cache.db",
) -> int:
    """Import a SQLite DB snapshot into the cache.

    Args:
        snapshot_path: Path to a raw ``.db`` file or a zipped snapshot.
        db_path: Destination SQLite database path.

    Returns:
        Number of programmes available in the imported database.
    """
    source = Path(snapshot_path)
    if not source.exists():
        raise FileNotFoundError(source)

    if source.suffix.lower() == ".zip":
        with tempfile.TemporaryDirectory() as tmpdir:
            extracted = _extract_snapshot_archive(source.read_bytes(), Path(tmpdir))
            return _restore_db_snapshot_file(extracted, db_path, snapshot_path)

    return _restore_db_snapshot_file(source, db_path, snapshot_path)


def import_db_snapshot_from_github(
    url: str = _DEFAULT_DB_SNAPSHOT_URL,
    db_path: str = "radio_cache.db",
    timeout: int = 60,
) -> int:
    """Download a SQLite DB snapshot and import it.

    Args:
        url: Snapshot URL.
        db_path: Destination SQLite database path.
        timeout: HTTP request timeout in seconds.

    Returns:
        Number of programmes available in the imported database.
    """
    logger.info("Fetching DB snapshot from %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "RadioCache/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        raw = resp.read()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        if zipfile.is_zipfile(io.BytesIO(raw)):
            extracted = _extract_snapshot_archive(raw, tmp_path)
            return _restore_db_snapshot_file(extracted, db_path, url)

        snapshot_file = tmp_path / Path(url).name
        snapshot_file.write_bytes(raw)
        return _restore_db_snapshot_file(snapshot_file, db_path, url)


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


def _import_json_data(data: dict, db_path: str, source: str) -> int:
    """Import programme data from a parsed JSON dict into the cache.

    Args:
        data: Parsed JSON dict with ``meta`` and ``programmes`` keys.
        db_path: Path to the SQLite database file.
        source: Label describing where the data came from (for logging).

    Returns:
        Number of programmes imported.
    """
    programmes = [
        Programme(**{k: v for k, v in item.items() if k != "updated_at"})
        for item in data.get("programmes", [])
    ]

    last_refreshed = (data.get("meta") or {}).get(
        "last_refreshed"
    ) or datetime.now(UTC).isoformat()

    with CacheDB(db_path) as db:
        count = db.upsert_programmes(programmes)
        db.set_meta("last_refreshed", last_refreshed)
        logger.info("Imported %d programmes from %s", count, source)
        return count


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
    return _import_json_data(data, db_path, json_path)


def import_from_github(
    url: str = _DEFAULT_GITHUB_URL,
    db_path: str = "radio_cache.db",
    timeout: int = 60,
) -> int:
    """Fetch the JSON export from a GitHub URL and import it.

    Args:
        url: URL to the raw JSON file on GitHub.
        db_path: Path to the SQLite database file.
        timeout: HTTP request timeout in seconds.

    Returns:
        Number of programmes imported.
    """
    logger.info("Fetching cache from %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "RadioCache/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        raw = resp.read().decode("utf-8")

    data = json.loads(raw)
    return _import_json_data(data, db_path, url)


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
        "--db-snapshot",
        default="",
        help=(
            "Optional SQLite snapshot output path. Use a .zip path for a "
            "compressed snapshot."
        ),
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip JSON export",
    )
    parser.add_argument(
        "--get-iplayer-cache",
        default="radio.cache",
        help="get_iplayer cache export path (default: radio.cache)",
    )
    parser.add_argument(
        "--no-get-iplayer-cache",
        action="store_true",
        help="Skip get_iplayer cache export",
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        metavar="SLUG",
        help="Category slugs to fetch (e.g. drama thriller comedy)",
    )
    parser.add_argument(
        "--all-categories",
        action="store_true",
        help="Fetch all available category slugs from the BBC API",
    )
    parser.add_argument(
        "--depth",
        choices=("recent", "full"),
        default="full",
        help=(
            "Refresh depth: 'recent' fetches only the newest pages "
            "(suitable for daily runs), 'full' pages through the "
            "entire back-catalogue (default: full)"
        ),
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List available BBC category slugs with programme counts and exit",
    )
    parser.add_argument(
        "--import-db-snapshot",
        metavar="FILE",
        help="Import from a SQLite snapshot file instead of fetching from BBC",
    )
    parser.add_argument(
        "--import-db-github",
        nargs="?",
        const=_DEFAULT_DB_SNAPSHOT_URL,
        metavar="URL",
        help=(
            "Import from a downloadable SQLite snapshot instead of fetching "
            "from BBC. Optionally provide a custom URL."
        ),
    )
    parser.add_argument(
        "--import-json",
        metavar="FILE",
        help="Import from JSON file instead of fetching from BBC",
    )
    parser.add_argument(
        "--import-github",
        nargs="?",
        const=_DEFAULT_GITHUB_URL,
        metavar="URL",
        help=(
            "Import from GitHub instead of fetching from BBC. "
            "Optionally provide a custom raw JSON URL."
        ),
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

    if args.list_categories:
        counts = fetch_category_counts(
            category_slugs=args.categories if args.categories else None,
        )
        print(f"{'Slug':<25} {'Display Name':<25} {'Programmes':>10}")
        print("-" * 62)
        for entry in counts:
            print(
                f"{entry['slug']:<25} {entry['display_name']:<25} "
                f"{entry['programme_count']:>10}"
            )
        return

    if args.import_db_github:
        count = import_db_snapshot_from_github(args.import_db_github, args.db)
        logger.info("Imported %d programmes from DB snapshot URL", count)
    elif args.import_db_snapshot:
        count = import_from_db_snapshot(args.import_db_snapshot, args.db)
        logger.info("Imported %d programmes from DB snapshot", count)
    elif args.import_github:
        count = import_from_github(args.import_github, args.db)
        logger.info("Imported %d programmes from GitHub", count)
    elif args.import_json:
        count = import_from_json(args.import_json, args.db)
        logger.info("Imported %d programmes", count)
    else:
        count = refresh_cache(
            db_path=args.db,
            export_json=not args.no_json,
            json_path=args.json,
            db_snapshot_path=args.db_snapshot,
            export_get_iplayer=not args.no_get_iplayer_cache,
            get_iplayer_path=args.get_iplayer_cache,
            category_slugs=args.categories,
            all_categories=args.all_categories,
            depth=args.depth,
        )
        logger.info("Cache contains %d programmes", count)


if __name__ == "__main__":
    main()
