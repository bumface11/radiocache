"""Data models for community features."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class User:
    """A community user authenticated via magic link.

    Attributes:
        user_id: Unique user identifier (UUID).
        email: User's email address.
        display_name: Public display name.
        created_at: ISO-8601 creation timestamp.
        last_login_at: ISO-8601 timestamp of last login.
    """

    user_id: str
    email: str
    display_name: str = ""
    created_at: str = ""
    last_login_at: str = ""


@dataclass(frozen=True, slots=True)
class MagicToken:
    """A single-use magic link token for passwordless authentication.

    Attributes:
        token: The unique token string.
        email: Email address this token authenticates.
        created_at: ISO-8601 creation timestamp.
        expires_at: ISO-8601 expiry timestamp.
        used: Whether the token has been consumed.
    """

    token: str
    email: str
    created_at: str = ""
    expires_at: str = ""
    used: bool = False


@dataclass(frozen=True, slots=True)
class Session:
    """An authenticated user session.

    Attributes:
        session_id: Unique session token.
        user_id: ID of the authenticated user.
        created_at: ISO-8601 session creation timestamp.
        expires_at: ISO-8601 session expiry timestamp.
    """

    session_id: str
    user_id: str
    created_at: str = ""
    expires_at: str = ""


@dataclass(frozen=True, slots=True)
class Rating:
    """A user's rating for a programme.

    Attributes:
        user_id: The rating user's ID.
        programme_pid: BBC programme PID being rated.
        score: Rating score (1-5 stars).
        created_at: ISO-8601 timestamp when rating was submitted.
        updated_at: ISO-8601 timestamp when rating was last updated.
    """

    user_id: str
    programme_pid: str
    score: int
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True, slots=True)
class RatingSummary:
    """Aggregate rating statistics for a programme.

    Attributes:
        programme_pid: BBC programme PID.
        average_score: Mean rating (0.0 if unrated).
        total_ratings: Number of ratings submitted.
    """

    programme_pid: str
    average_score: float = 0.0
    total_ratings: int = 0


@dataclass(frozen=True, slots=True)
class Discussion:
    """A comment in a threaded discussion about a programme.

    Attributes:
        comment_id: Unique comment identifier (UUID).
        programme_pid: BBC programme PID this comment is about.
        user_id: Author's user ID.
        parent_id: Parent comment ID for threading (empty for top-level).
        body: Comment text content.
        created_at: ISO-8601 creation timestamp.
        updated_at: ISO-8601 last-edit timestamp.
        display_name: Author's display name (denormalised for rendering).
        replies: Nested child comments.
    """

    comment_id: str
    programme_pid: str
    user_id: str
    parent_id: str = ""
    body: str = ""
    created_at: str = ""
    updated_at: str = ""
    display_name: str = ""
    replies: list[Discussion] = field(default_factory=list)
