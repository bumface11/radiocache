"""SQLite-backed programme cache for BBC Radio Dramas.

Provides persistent storage with fast full-text search.  The database
uses two tables:

- ``programmes`` -- one row per programme/episode.
- ``cache_meta`` -- single-row metadata (last refresh timestamp).

An FTS5 virtual table (``programmes_fts``) enables efficient free-text
search over titles and synopses.
"""

from __future__ import annotations

import io
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from radio_cache.models import CacheStats, Programme

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH: Final[str] = "radio_cache.db"

_CREATE_TABLES_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS programmes (
    pid            TEXT PRIMARY KEY,
    title          TEXT NOT NULL,
    synopsis       TEXT NOT NULL DEFAULT '',
    duration_secs  INTEGER NOT NULL DEFAULT 0,
    available_until TEXT NOT NULL DEFAULT '',
    first_broadcast TEXT NOT NULL DEFAULT '',
    programme_type TEXT NOT NULL DEFAULT 'episode',
    series_pid     TEXT NOT NULL DEFAULT '',
    series_title   TEXT NOT NULL DEFAULT '',
    brand_pid      TEXT NOT NULL DEFAULT '',
    brand_title    TEXT NOT NULL DEFAULT '',
    episode_number INTEGER NOT NULL DEFAULT 0,
    channel        TEXT NOT NULL DEFAULT '',
    thumbnail_url  TEXT NOT NULL DEFAULT '',
    categories     TEXT NOT NULL DEFAULT '',
    url            TEXT NOT NULL DEFAULT '',
    updated_at     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS cache_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS categories (
    name             TEXT PRIMARY KEY,
    programme_count  INTEGER NOT NULL DEFAULT 0
);

CREATE VIRTUAL TABLE IF NOT EXISTS programmes_fts USING fts5(
    pid,
    title,
    synopsis,
    series_title,
    brand_title,
    categories,
    content='programmes',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS programmes_ai
AFTER INSERT ON programmes BEGIN
    INSERT INTO programmes_fts(
        rowid, pid, title, synopsis,
        series_title, brand_title, categories
    ) VALUES (
        new.rowid, new.pid, new.title, new.synopsis,
        new.series_title, new.brand_title, new.categories
    );
END;

CREATE TRIGGER IF NOT EXISTS programmes_ad
AFTER DELETE ON programmes BEGIN
    INSERT INTO programmes_fts(
        programmes_fts, rowid, pid, title,
        synopsis, series_title, brand_title, categories
    ) VALUES (
        'delete', old.rowid, old.pid, old.title,
        old.synopsis, old.series_title,
        old.brand_title, old.categories
    );
END;

CREATE TRIGGER IF NOT EXISTS programmes_au
AFTER UPDATE ON programmes BEGIN
    INSERT INTO programmes_fts(
        programmes_fts, rowid, pid, title,
        synopsis, series_title, brand_title, categories
    ) VALUES (
        'delete', old.rowid, old.pid, old.title,
        old.synopsis, old.series_title,
        old.brand_title, old.categories
    );
    INSERT INTO programmes_fts(
        rowid, pid, title, synopsis,
        series_title, brand_title, categories
    ) VALUES (
        new.rowid, new.pid, new.title, new.synopsis,
        new.series_title, new.brand_title, new.categories
    );
END;
"""

_UPSERT_SQL: Final[str] = """
INSERT INTO programmes (
    pid, title, synopsis, duration_secs, available_until, first_broadcast,
    programme_type, series_pid, series_title, brand_pid, brand_title,
    episode_number, channel, thumbnail_url, categories, url, updated_at
) VALUES (
    :pid, :title, :synopsis, :duration_secs, :available_until, :first_broadcast,
    :programme_type, :series_pid, :series_title, :brand_pid, :brand_title,
    :episode_number, :channel, :thumbnail_url, :categories, :url, :updated_at
)
ON CONFLICT(pid) DO UPDATE SET
    title=excluded.title,
    synopsis=excluded.synopsis,
    duration_secs=excluded.duration_secs,
    available_until=excluded.available_until,
    first_broadcast=excluded.first_broadcast,
    programme_type=excluded.programme_type,
    series_pid=excluded.series_pid,
    series_title=excluded.series_title,
    brand_pid=excluded.brand_pid,
    brand_title=excluded.brand_title,
    episode_number=excluded.episode_number,
    channel=excluded.channel,
    thumbnail_url=excluded.thumbnail_url,
    categories=excluded.categories,
    url=excluded.url,
    updated_at=excluded.updated_at
"""


class CacheDB:
    """SQLite-backed programme cache.

    Args:
        db_path: File path for the SQLite database.  Use ``":memory:"``
            for an in-memory database (useful for tests).
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_CREATE_TABLES_SQL)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> CacheDB:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def query(
        self,
        sql: str,
        params: tuple | dict = (),
    ) -> list[sqlite3.Row]:
        """Execute a read-only SQL query and return all rows.

        Args:
            sql: SQL query string.
            params: Positional or named bind parameters.

        Returns:
            List of Row objects.
        """
        return self._conn.execute(sql, params).fetchall()

    def upsert_programme(self, prog: Programme) -> None:
        """Insert or update a single programme.

        Args:
            prog: Programme to upsert.
        """
        now = datetime.now(UTC).isoformat()
        params = {
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
            "updated_at": now,
        }
        self._conn.execute(_UPSERT_SQL, params)
        self._conn.commit()

    def upsert_programmes(self, programmes: list[Programme]) -> int:
        """Bulk insert or update programmes.

        Args:
            programmes: Programmes to upsert.

        Returns:
            Number of programmes upserted.
        """
        now = datetime.now(UTC).isoformat()
        rows = [
            {
                "pid": p.pid,
                "title": p.title,
                "synopsis": p.synopsis,
                "duration_secs": p.duration_secs,
                "available_until": p.available_until,
                "first_broadcast": p.first_broadcast,
                "programme_type": p.programme_type,
                "series_pid": p.series_pid,
                "series_title": p.series_title,
                "brand_pid": p.brand_pid,
                "brand_title": p.brand_title,
                "episode_number": p.episode_number,
                "channel": p.channel,
                "thumbnail_url": p.thumbnail_url,
                "categories": p.categories,
                "url": p.url,
                "updated_at": now,
            }
            for p in programmes
        ]
        self._conn.executemany(_UPSERT_SQL, rows)
        self._conn.commit()
        self._rebuild_categories()
        return len(rows)

    def get_programme(self, pid: str) -> Programme | None:
        """Fetch a single programme by PID.

        Args:
            pid: BBC programme identifier.

        Returns:
            The programme, or ``None`` if not found.
        """
        row = self._conn.execute(
            "SELECT * FROM programmes WHERE pid = ?", (pid,)
        ).fetchone()
        return _row_to_programme(row) if row else None

    def search(
        self,
        query: str,
        limit: int = 50,
        offset: int = 0,
        category: str = "",
    ) -> list[Programme]:
        """Full-text search across titles, synopses, and categories.

        Args:
            query: Search terms (FTS5 query syntax).
            limit: Maximum results to return.
            offset: Result offset for pagination.
            category: Optional category tag to filter results at the
                database level.  The match is case-insensitive.

        Returns:
            Matching programmes ordered by relevance.
        """
        safe_query = _sanitise_fts_query(query)
        if not safe_query:
            return []

        if category:
            sql = """
                SELECT p.* FROM programmes p
                JOIN programmes_fts fts ON p.rowid = fts.rowid
                WHERE programmes_fts MATCH :query
                AND ',' || LOWER(p.categories) || ',' LIKE '%,' || LOWER(:category) || ',%'
                ORDER BY rank
                LIMIT :limit OFFSET :offset
            """
            rows = self._conn.execute(
                sql,
                {
                    "query": safe_query,
                    "category": category,
                    "limit": limit,
                    "offset": offset,
                },
            ).fetchall()
        else:
            sql = """
                SELECT p.* FROM programmes p
                JOIN programmes_fts fts ON p.rowid = fts.rowid
                WHERE programmes_fts MATCH :query
                ORDER BY rank
                LIMIT :limit OFFSET :offset
            """
            rows = self._conn.execute(
                sql, {"query": safe_query, "limit": limit, "offset": offset}
            ).fetchall()
        return [_row_to_programme(r) for r in rows]

    def search_count(
        self,
        query: str,
        category: str = "",
    ) -> int:
        """Count full-text search results (without fetching rows).

        Args:
            query: Search terms (FTS5 query syntax).
            category: Optional category tag filter.

        Returns:
            Number of matching programmes.
        """
        safe_query = _sanitise_fts_query(query)
        if not safe_query:
            return 0

        if category:
            sql = """
                SELECT COUNT(*) FROM programmes p
                JOIN programmes_fts fts ON p.rowid = fts.rowid
                WHERE programmes_fts MATCH :query
                AND ',' || LOWER(p.categories) || ',' LIKE '%,' || LOWER(:category) || ',%'
            """
            row = self._conn.execute(
                sql, {"query": safe_query, "category": category}
            ).fetchone()
        else:
            sql = """
                SELECT COUNT(*) FROM programmes p
                JOIN programmes_fts fts ON p.rowid = fts.rowid
                WHERE programmes_fts MATCH :query
            """
            row = self._conn.execute(sql, {"query": safe_query}).fetchone()
        return int(row[0]) if row else 0

    def programmes_by_category_count(self, category: str) -> int:
        """Count programmes in a category.

        Reads from the cached ``categories`` table when possible,
        falling back to a ``COUNT(*)`` query.

        Args:
            category: Category tag to count.

        Returns:
            Number of matching programmes.
        """
        row = self._conn.execute(
            "SELECT programme_count FROM categories WHERE name = ?",
            (category,),
        ).fetchone()
        if row:
            return int(row[0])

        row = self._conn.execute(
            "SELECT COUNT(*) FROM programmes "
            "WHERE ',' || LOWER(categories) || ',' LIKE '%,' || LOWER(?) || ',%'",
            (category,),
        ).fetchone()
        return int(row[0]) if row else 0

    def list_series(self) -> list[dict[str, str | int]]:
        """List all distinct series with episode counts.

        Returns:
            List of dicts with ``series_pid``, ``series_title``,
            ``brand_title``, and ``episode_count``.
        """
        sql = """
            SELECT series_pid, series_title, brand_title,
                   COUNT(*) as episode_count
            FROM programmes
            WHERE series_pid != ''
            GROUP BY series_pid
            ORDER BY series_title
        """
        rows = self._conn.execute(sql).fetchall()
        return [
            {
                "series_pid": r["series_pid"],
                "series_title": r["series_title"],
                "brand_title": r["brand_title"],
                "episode_count": r["episode_count"],
            }
            for r in rows
        ]

    def get_series_episodes(self, series_pid: str) -> list[Programme]:
        """Fetch all episodes in a series, ordered by episode number.

        Args:
            series_pid: BBC series PID.

        Returns:
            Programmes in the series, sorted by episode number.
        """
        sql = """
            SELECT * FROM programmes
            WHERE series_pid = ?
            ORDER BY
              CASE WHEN episode_number > 0 THEN 0 ELSE 1 END,
              CASE WHEN episode_number > 0 THEN episode_number ELSE 2147483647 END,
              CASE WHEN first_broadcast = '' THEN 1 ELSE 0 END,
              first_broadcast,
              title,
              pid
        """
        rows = self._conn.execute(sql, (series_pid,)).fetchall()
        return [_row_to_programme(r) for r in rows]

    def list_brands(self) -> list[dict[str, str | int]]:
        """List all distinct brands with series and episode counts.

        Returns:
            List of dicts with ``brand_pid``, ``brand_title``,
            ``series_count``, and ``total_episodes``.
        """
        sql = """
            SELECT brand_pid, brand_title,
                   COUNT(DISTINCT series_pid) as series_count,
                   COUNT(*) as total_episodes
            FROM programmes
            WHERE brand_pid != ''
            GROUP BY brand_pid
            ORDER BY brand_title
        """
        rows = self._conn.execute(sql).fetchall()
        return [
            {
                "brand_pid": r["brand_pid"],
                "brand_title": r["brand_title"],
                "series_count": r["series_count"],
                "total_episodes": r["total_episodes"],
            }
            for r in rows
        ]

    def get_brand_series(self, brand_pid: str) -> list[dict[str, str | int]]:
        """Fetch all series within a brand.

        Args:
            brand_pid: BBC brand PID.

        Returns:
            List of series dicts.
        """
        sql = """
            SELECT series_pid, series_title, COUNT(*) as episode_count
            FROM programmes
            WHERE brand_pid = ?
            GROUP BY series_pid
            ORDER BY series_title
        """
        rows = self._conn.execute(sql, (brand_pid,)).fetchall()
        return [
            {
                "series_pid": r["series_pid"],
                "series_title": r["series_title"],
                "episode_count": r["episode_count"],
            }
            for r in rows
        ]

    def list_categories(self) -> list[dict[str, str | int]]:
        """List all distinct category tags with programme counts.

        Reads from the ``categories`` summary table which is rebuilt
        whenever programmes are bulk-upserted.  Falls back to scanning
        the ``programmes`` table directly if the summary table is empty
        (e.g. after individual single-programme inserts).

        Returns:
            List of dicts with ``category`` (display name) and
            ``programme_count`` keys, ordered alphabetically by category.
        """
        cached = self._conn.execute(
            "SELECT name, programme_count FROM categories ORDER BY name"
        ).fetchall()
        if cached:
            return [
                {"category": r["name"], "programme_count": r["programme_count"]}
                for r in cached
            ]

        # Fallback: scan programmes table directly.
        rows = self._conn.execute(
            "SELECT categories FROM programmes WHERE categories != ''"
        ).fetchall()

        counts: dict[str, int] = {}
        for row in rows:
            for tag in row["categories"].split(","):
                tag = tag.strip()
                if tag:
                    counts[tag] = counts.get(tag, 0) + 1

        return [
            {"category": tag, "programme_count": count}
            for tag, count in sorted(counts.items())
        ]

    def programmes_by_category(
        self,
        category: str,
        limit: int = 200,
        offset: int = 0,
    ) -> list[Programme]:
        """Fetch programmes whose ``categories`` field contains *category*.

        The match is case-insensitive and checks for whole comma-separated
        tokens to avoid partial matches (e.g. ``"Crime"`` should not match
        ``"Crime Drama"`` unless ``"Crime Drama"`` is explicitly requested).

        Args:
            category: Category tag to filter by.
            limit: Maximum results.
            offset: Pagination offset.

        Returns:
            Matching programmes ordered by title.
        """
        sql = """
            SELECT * FROM programmes
            WHERE ',' || LOWER(categories) || ',' LIKE '%,' || LOWER(?) || ',%'
            ORDER BY title
            LIMIT ? OFFSET ?
        """
        rows = self._conn.execute(sql, (category, limit, offset)).fetchall()
        return [_row_to_programme(r) for r in rows]

    def recent_programmes(self, limit: int = 50) -> list[Programme]:
        """Fetch the most recently broadcast programmes.

        Args:
            limit: Maximum results.

        Returns:
            Programmes ordered by broadcast date descending.
        """
        sql = """
            SELECT * FROM programmes
            ORDER BY first_broadcast DESC
            LIMIT ?
        """
        rows = self._conn.execute(sql, (limit,)).fetchall()
        return [_row_to_programme(r) for r in rows]

    def all_programmes(self) -> list[Programme]:
        """Fetch every programme in the cache.

        Returns:
            All programmes ordered by title.
        """
        rows = self._conn.execute(
            "SELECT * FROM programmes ORDER BY title"
        ).fetchall()
        return [_row_to_programme(r) for r in rows]

    def programme_count(self) -> int:
        """Return the total number of cached programmes.

        Returns:
            Programme count.
        """
        row = self._conn.execute("SELECT COUNT(*) FROM programmes").fetchone()
        return int(row[0]) if row else 0

    def set_meta(self, key: str, value: str) -> None:
        """Set a cache metadata value.

        Args:
            key: Metadata key.
            value: Metadata value.
        """
        self._conn.execute(
            "INSERT INTO cache_meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self._conn.commit()

    def get_meta(self, key: str) -> str:
        """Get a cache metadata value.

        Args:
            key: Metadata key.

        Returns:
            Value string, or empty string if not found.
        """
        row = self._conn.execute(
            "SELECT value FROM cache_meta WHERE key = ?", (key,)
        ).fetchone()
        return str(row[0]) if row else ""

    def stats(self) -> CacheStats:
        """Compute summary statistics for the cache.

        Returns:
            A :class:`CacheStats` instance.
        """
        count = self.programme_count()
        series_row = self._conn.execute(
            "SELECT COUNT(DISTINCT series_pid) FROM programmes "
            "WHERE series_pid != ''"
        ).fetchone()
        brand_row = self._conn.execute(
            "SELECT COUNT(DISTINCT brand_pid) FROM programmes "
            "WHERE brand_pid != ''"
        ).fetchone()
        return CacheStats(
            total_programmes=count,
            total_series=int(series_row[0]) if series_row else 0,
            total_brands=int(brand_row[0]) if brand_row else 0,
            last_refreshed=self.get_meta("last_refreshed"),
        )

    def purge_expired(self) -> int:
        """Remove programmes whose availability has passed.

        Returns:
            Number of programmes removed.
        """
        now = datetime.now(UTC).isoformat()
        cur = self._conn.execute(
            "DELETE FROM programmes WHERE available_until != '' "
            "AND available_until < ?",
            (now,),
        )
        self._conn.commit()
        if cur.rowcount:
            self._rebuild_categories()
        return cur.rowcount

    def _rebuild_categories(self) -> None:
        """Rebuild the ``categories`` summary table from programme data.

        Scans every programme's comma-separated ``categories`` field,
        aggregates counts, and replaces the contents of the
        ``categories`` table.
        """
        rows = self._conn.execute(
            "SELECT categories FROM programmes WHERE categories != ''"
        ).fetchall()

        counts: dict[str, int] = {}
        for row in rows:
            for tag in row["categories"].split(","):
                tag = tag.strip()
                if tag:
                    counts[tag] = counts.get(tag, 0) + 1

        self._conn.execute("DELETE FROM categories")
        if counts:
            self._conn.executemany(
                "INSERT INTO categories (name, programme_count) VALUES (?, ?)",
                list(counts.items()),
            )
        self._conn.commit()

    def rebuild_fts(self) -> None:
        """Rebuild the FTS index from scratch."""
        self._conn.execute(
            "INSERT INTO programmes_fts(programmes_fts) VALUES('rebuild')"
        )
        self._conn.commit()

    def export_get_iplayer_cache(
        self,
        dest: str | io.TextIOBase,
    ) -> int:
        """Export all programmes as a get_iplayer-compatible flat cache file.

        The output uses the native get_iplayer v3.36 pipe-delimited format::

            #index|type|name|episode|seriesnum|episodenum|pid|channel|available|expires|duration|desc|web|thumbnail|timeadded

        ISO-8601 datetime strings are converted to Unix epoch seconds.
        Missing or zero values are written as empty strings.

        Args:
            dest: File path string, or a writable text file-like object.

        Returns:
            Number of programme rows written.
        """
        rows = self._conn.execute(
            "SELECT * FROM programmes ORDER BY title"
        ).fetchall()

        header = (
            "#index|type|name|episode|seriesnum|episodenum"
            "|pid|channel|available|expires|duration|desc|web|thumbnail|timeadded\n"
        )

        def _field(value: str | None) -> str:
            return (value or "").replace("|", "-")

        def _ts(iso: str | None) -> str:
            if not iso:
                return ""
            try:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                return str(int(dt.timestamp()))
            except (ValueError, OSError):
                return ""

        lines: list[str] = [header]
        for index, row in enumerate(rows, start=1):
            series_title = row["series_title"] or ""
            brand_title = row["brand_title"] or ""
            title = row["title"] or ""
            name = _field(series_title or brand_title or title)
            episode = _field(title)
            episodenum = str(row["episode_number"]) if row["episode_number"] else ""
            duration = str(row["duration_secs"]) if row["duration_secs"] else ""
            available = _ts(row["first_broadcast"])
            expires = _ts(row["available_until"])
            timeadded = _ts(row["updated_at"])
            fields = (
                str(index),
                "radio",
                name,
                episode,
                "",  # seriesnum — no series number field in schema
                episodenum,
                _field(row["pid"]),
                _field(row["channel"]),
                available,
                expires,
                duration,
                _field(row["synopsis"]),
                _field(row["url"]),
                _field(row["thumbnail_url"]),
                timeadded,
            )
            lines.append("|".join(fields) + "\n")

        content = "".join(lines)

        if isinstance(dest, str):
            Path(dest).write_text(content, encoding="utf-8")
        else:
            dest.write(content)

        logger.info(
            "Exported %d programmes to get_iplayer cache", len(rows)
        )
        return len(rows)


def _row_to_programme(row: sqlite3.Row) -> Programme:
    """Convert a database row to a :class:`Programme`.

    Args:
        row: SQLite Row object.

    Returns:
        A ``Programme`` instance.
    """
    return Programme(
        pid=row["pid"],
        title=row["title"],
        synopsis=row["synopsis"],
        duration_secs=row["duration_secs"],
        available_until=row["available_until"],
        first_broadcast=row["first_broadcast"],
        programme_type=row["programme_type"],
        series_pid=row["series_pid"],
        series_title=row["series_title"],
        brand_pid=row["brand_pid"],
        brand_title=row["brand_title"],
        episode_number=row["episode_number"],
        channel=row["channel"],
        thumbnail_url=row["thumbnail_url"],
        categories=row["categories"],
        url=row["url"],
    )


def _sanitise_fts_query(query: str) -> str:
    """Sanitise a user query for FTS5 safety.

    Wraps each word in double quotes to prevent FTS5 syntax errors
    from special characters.

    Args:
        query: Raw user search string.

    Returns:
        Sanitised FTS5 query string.
    """
    words = query.strip().split()
    if not words:
        return ""
    escaped = ['"' + w.replace('"', '""') + '"' for w in words]
    return " ".join(escaped)
