"""SQLite database layer for community features.

Manages users, sessions, magic tokens, ratings, and discussions
in a separate database from the programme cache to keep concerns
cleanly separated.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import Final

from radio_cache.community.models import (
    Discussion,
    MagicToken,
    Rating,
    RatingSummary,
    Session,
    User,
)

_DEFAULT_COMMUNITY_DB_PATH: Final[str] = "community.db"

_MAGIC_TOKEN_LIFETIME_MINUTES: Final[int] = 15
_SESSION_LIFETIME_DAYS: Final[int] = 30

_CREATE_TABLES_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS users (
    user_id       TEXT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE COLLATE NOCASE,
    display_name  TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT '',
    last_login_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS magic_tokens (
    token      TEXT PRIMARY KEY,
    email      TEXT NOT NULL COLLATE NOCASE,
    created_at TEXT NOT NULL DEFAULT '',
    expires_at TEXT NOT NULL DEFAULT '',
    used       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL DEFAULT '',
    expires_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id        TEXT NOT NULL REFERENCES users(user_id),
    programme_pid  TEXT NOT NULL,
    score          INTEGER NOT NULL CHECK(score >= 1 AND score <= 5),
    created_at     TEXT NOT NULL DEFAULT '',
    updated_at     TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, programme_pid)
);

CREATE TABLE IF NOT EXISTS discussions (
    comment_id    TEXT PRIMARY KEY,
    programme_pid TEXT NOT NULL,
    user_id       TEXT NOT NULL REFERENCES users(user_id),
    parent_id     TEXT NOT NULL DEFAULT '',
    body          TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT '',
    updated_at    TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_discussions_programme
    ON discussions(programme_pid, created_at);
CREATE INDEX IF NOT EXISTS idx_discussions_parent
    ON discussions(parent_id);
CREATE INDEX IF NOT EXISTS idx_ratings_programme
    ON ratings(programme_pid);
CREATE INDEX IF NOT EXISTS idx_sessions_user
    ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_magic_tokens_email
    ON magic_tokens(email);
"""


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat(timespec="seconds")


class CommunityDB:
    """SQLite-backed community database.

    Args:
        db_path: File path for the SQLite database.  Use ``":memory:"``
            for an in-memory database (useful for tests).
    """

    def __init__(self, db_path: str = _DEFAULT_COMMUNITY_DB_PATH) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_CREATE_TABLES_SQL)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> CommunityDB:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ── Authentication ────────────────────────────────────────────────

    def create_magic_token(self, email: str) -> MagicToken:
        """Generate a new magic link token for the given email.

        Args:
            email: User's email address.

        Returns:
            The created MagicToken with token string and expiry.
        """
        token = uuid.uuid4().hex
        now = _now_iso()
        expires = (
            datetime.now(UTC) + timedelta(minutes=_MAGIC_TOKEN_LIFETIME_MINUTES)
        ).isoformat(timespec="seconds")
        self._conn.execute(
            "INSERT INTO magic_tokens (token, email, created_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (token, email, now, expires),
        )
        self._conn.commit()
        return MagicToken(
            token=token, email=email, created_at=now, expires_at=expires
        )

    def verify_magic_token(self, token: str) -> str | None:
        """Consume a magic token and return the associated email.

        Returns None if the token is invalid, expired, or already used.

        Args:
            token: The token string from the magic link.

        Returns:
            The email address if valid, or None.
        """
        row = self._conn.execute(
            "SELECT email, expires_at, used FROM magic_tokens WHERE token = ?",
            (token,),
        ).fetchone()
        if not row:
            return None
        if row["used"]:
            return None
        if row["expires_at"] and row["expires_at"] < _now_iso():
            return None
        # Mark as used
        self._conn.execute(
            "UPDATE magic_tokens SET used = 1 WHERE token = ?", (token,)
        )
        self._conn.commit()
        return str(row["email"])

    def get_or_create_user(self, email: str) -> User:
        """Get an existing user or create a new one for the given email.

        Args:
            email: User's email address.

        Returns:
            The User record.
        """
        row = self._conn.execute(
            "SELECT * FROM users WHERE email = ?", (email,)
        ).fetchone()
        if row:
            now = _now_iso()
            self._conn.execute(
                "UPDATE users SET last_login_at = ? WHERE user_id = ?",
                (now, row["user_id"]),
            )
            self._conn.commit()
            return User(
                user_id=row["user_id"],
                email=row["email"],
                display_name=row["display_name"],
                created_at=row["created_at"],
                last_login_at=now,
            )
        user_id = uuid.uuid4().hex
        now = _now_iso()
        # Default display name from email local part
        display_name = email.split("@")[0]
        self._conn.execute(
            "INSERT INTO users (user_id, email, display_name, "
            "created_at, last_login_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, email, display_name, now, now),
        )
        self._conn.commit()
        return User(
            user_id=user_id,
            email=email,
            display_name=display_name,
            created_at=now,
            last_login_at=now,
        )

    def create_session(self, user_id: str) -> Session:
        """Create a new authenticated session for a user.

        Args:
            user_id: The user's ID.

        Returns:
            The created Session.
        """
        session_id = uuid.uuid4().hex
        now = _now_iso()
        expires = (
            datetime.now(UTC) + timedelta(days=_SESSION_LIFETIME_DAYS)
        ).isoformat(timespec="seconds")
        self._conn.execute(
            "INSERT INTO sessions (session_id, user_id, created_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (session_id, user_id, now, expires),
        )
        self._conn.commit()
        return Session(
            session_id=session_id,
            user_id=user_id,
            created_at=now,
            expires_at=expires,
        )

    def get_session_user(self, session_id: str) -> User | None:
        """Look up the user for a valid session.

        Returns None if the session is invalid or expired.

        Args:
            session_id: Session token from cookie.

        Returns:
            The authenticated User, or None.
        """
        row = self._conn.execute(
            "SELECT s.user_id, s.expires_at, u.email, u.display_name, "
            "u.created_at, u.last_login_at "
            "FROM sessions s JOIN users u ON s.user_id = u.user_id "
            "WHERE s.session_id = ?",
            (session_id,),
        ).fetchone()
        if not row:
            return None
        if row["expires_at"] and row["expires_at"] < _now_iso():
            return None
        return User(
            user_id=row["user_id"],
            email=row["email"],
            display_name=row["display_name"],
            created_at=row["created_at"],
            last_login_at=row["last_login_at"],
        )

    def delete_session(self, session_id: str) -> None:
        """Delete a session (logout).

        Args:
            session_id: Session token to invalidate.
        """
        self._conn.execute(
            "DELETE FROM sessions WHERE session_id = ?", (session_id,)
        )
        self._conn.commit()

    def get_user(self, user_id: str) -> User | None:
        """Look up a user by ID.

        Args:
            user_id: The user's unique identifier.

        Returns:
            The User, or None if not found.
        """
        row = self._conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not row:
            return None
        return User(
            user_id=row["user_id"],
            email=row["email"],
            display_name=row["display_name"],
            created_at=row["created_at"],
            last_login_at=row["last_login_at"],
        )

    def update_display_name(self, user_id: str, display_name: str) -> None:
        """Update a user's display name.

        Args:
            user_id: The user's unique identifier.
            display_name: New display name.
        """
        self._conn.execute(
            "UPDATE users SET display_name = ? WHERE user_id = ?",
            (display_name, user_id),
        )
        self._conn.commit()

    # ── Ratings ───────────────────────────────────────────────────────

    def upsert_rating(
        self, user_id: str, programme_pid: str, score: int
    ) -> Rating:
        """Create or update a user's rating for a programme.

        Args:
            user_id: The rating user's ID.
            programme_pid: BBC programme PID.
            score: Rating 1-5.

        Returns:
            The stored Rating.
        """
        now = _now_iso()
        self._conn.execute(
            "INSERT INTO ratings "
            "(user_id, programme_pid, score, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id, programme_pid) DO UPDATE SET "
            "score = excluded.score, updated_at = excluded.updated_at",
            (user_id, programme_pid, score, now, now),
        )
        self._conn.commit()
        return Rating(
            user_id=user_id,
            programme_pid=programme_pid,
            score=score,
            created_at=now,
            updated_at=now,
        )

    def get_user_rating(
        self, user_id: str, programme_pid: str
    ) -> Rating | None:
        """Get a user's rating for a specific programme.

        Args:
            user_id: The user's ID.
            programme_pid: BBC programme PID.

        Returns:
            The Rating, or None if the user hasn't rated this programme.
        """
        row = self._conn.execute(
            "SELECT * FROM ratings WHERE user_id = ? AND programme_pid = ?",
            (user_id, programme_pid),
        ).fetchone()
        if not row:
            return None
        return Rating(
            user_id=row["user_id"],
            programme_pid=row["programme_pid"],
            score=row["score"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_rating_summary(self, programme_pid: str) -> RatingSummary:
        """Get aggregate rating stats for a programme.

        Args:
            programme_pid: BBC programme PID.

        Returns:
            RatingSummary with average and count.
        """
        row = self._conn.execute(
            "SELECT AVG(score) as avg_score, COUNT(*) as total "
            "FROM ratings WHERE programme_pid = ?",
            (programme_pid,),
        ).fetchone()
        if not row or row["total"] == 0:
            return RatingSummary(programme_pid=programme_pid)
        return RatingSummary(
            programme_pid=programme_pid,
            average_score=round(float(row["avg_score"]), 1),
            total_ratings=int(row["total"]),
        )

    def delete_rating(self, user_id: str, programme_pid: str) -> None:
        """Remove a user's rating for a programme.

        Args:
            user_id: The user's ID.
            programme_pid: BBC programme PID.
        """
        self._conn.execute(
            "DELETE FROM ratings WHERE user_id = ? AND programme_pid = ?",
            (user_id, programme_pid),
        )
        self._conn.commit()

    # ── Discussions ───────────────────────────────────────────────────

    def create_comment(
        self,
        programme_pid: str,
        user_id: str,
        body: str,
        parent_id: str = "",
    ) -> Discussion:
        """Post a new comment in a programme's discussion.

        Args:
            programme_pid: BBC programme PID.
            user_id: Author's user ID.
            body: Comment text.
            parent_id: Parent comment ID for replies (empty for top-level).

        Returns:
            The created Discussion comment.
        """
        comment_id = uuid.uuid4().hex
        now = _now_iso()
        self._conn.execute(
            "INSERT INTO discussions "
            "(comment_id, programme_pid, user_id, parent_id, "
            "body, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (comment_id, programme_pid, user_id, parent_id, body, now, now),
        )
        self._conn.commit()
        user = self.get_user(user_id)
        display_name = user.display_name if user else ""
        return Discussion(
            comment_id=comment_id,
            programme_pid=programme_pid,
            user_id=user_id,
            parent_id=parent_id,
            body=body,
            created_at=now,
            updated_at=now,
            display_name=display_name,
        )

    def get_programme_discussions(
        self, programme_pid: str
    ) -> list[Discussion]:
        """Get all comments for a programme, threaded.

        Returns top-level comments with nested replies.

        Args:
            programme_pid: BBC programme PID.

        Returns:
            List of top-level Discussion objects with replies populated.
        """
        rows = self._conn.execute(
            "SELECT d.*, u.display_name FROM discussions d "
            "JOIN users u ON d.user_id = u.user_id "
            "WHERE d.programme_pid = ? ORDER BY d.created_at ASC",
            (programme_pid,),
        ).fetchall()

        comments_by_id: dict[str, Discussion] = {}
        top_level: list[Discussion] = []

        for row in rows:
            comment = Discussion(
                comment_id=row["comment_id"],
                programme_pid=row["programme_pid"],
                user_id=row["user_id"],
                parent_id=row["parent_id"],
                body=row["body"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                display_name=row["display_name"],
            )
            comments_by_id[comment.comment_id] = comment

        # Build tree structure
        for comment in comments_by_id.values():
            if comment.parent_id and comment.parent_id in comments_by_id:
                parent = comments_by_id[comment.parent_id]
                parent.replies.append(comment)
            else:
                top_level.append(comment)

        return top_level

    def get_discussion_count(self, programme_pid: str) -> int:
        """Get the total number of comments for a programme.

        Args:
            programme_pid: BBC programme PID.

        Returns:
            Total comment count.
        """
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM discussions WHERE programme_pid = ?",
            (programme_pid,),
        ).fetchone()
        return int(row["cnt"]) if row else 0
