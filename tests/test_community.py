"""Tests for the community module: auth, ratings, discussions."""

from __future__ import annotations

import sqlite3

import pytest

from radio_cache.community.db import CommunityDB


@pytest.fixture
def community_db() -> CommunityDB:
    """Create an in-memory community database for testing."""
    return CommunityDB(":memory:")


class TestMagicLinkAuth:
    """Tests for magic link authentication flow."""

    def test_create_magic_token(self, community_db: CommunityDB) -> None:
        token = community_db.create_magic_token("test@example.com")
        assert token.token
        assert token.email == "test@example.com"
        assert token.created_at
        assert token.expires_at

    def test_verify_valid_token(self, community_db: CommunityDB) -> None:
        token = community_db.create_magic_token("user@example.com")
        email = community_db.verify_magic_token(token.token)
        assert email == "user@example.com"

    def test_verify_token_marks_as_used(self, community_db: CommunityDB) -> None:
        token = community_db.create_magic_token("user@example.com")
        community_db.verify_magic_token(token.token)
        # Second use should fail
        email = community_db.verify_magic_token(token.token)
        assert email is None

    def test_verify_invalid_token(self, community_db: CommunityDB) -> None:
        email = community_db.verify_magic_token("nonexistent_token")
        assert email is None

    def test_get_or_create_user_new(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("new@example.com")
        assert user.email == "new@example.com"
        assert user.display_name == "new"
        assert user.user_id
        assert user.created_at

    def test_get_or_create_user_existing(self, community_db: CommunityDB) -> None:
        user1 = community_db.get_or_create_user("existing@example.com")
        user2 = community_db.get_or_create_user("existing@example.com")
        assert user1.user_id == user2.user_id

    def test_create_and_validate_session(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("sess@example.com")
        session = community_db.create_session(user.user_id)
        assert session.session_id
        resolved = community_db.get_session_user(session.session_id)
        assert resolved is not None
        assert resolved.user_id == user.user_id

    def test_invalid_session(self, community_db: CommunityDB) -> None:
        resolved = community_db.get_session_user("bad_session_id")
        assert resolved is None

    def test_delete_session(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("del@example.com")
        session = community_db.create_session(user.user_id)
        community_db.delete_session(session.session_id)
        assert community_db.get_session_user(session.session_id) is None

    def test_update_display_name(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("name@example.com")
        community_db.update_display_name(user.user_id, "New Name")
        updated = community_db.get_user(user.user_id)
        assert updated is not None
        assert updated.display_name == "New Name"


class TestRatings:
    """Tests for the rating system."""

    def test_upsert_rating(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("rater@example.com")
        rating = community_db.upsert_rating(user.user_id, "b09xyz12", 4)
        assert rating.score == 4
        assert rating.programme_pid == "b09xyz12"

    def test_update_existing_rating(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("rater@example.com")
        community_db.upsert_rating(user.user_id, "b09xyz12", 3)
        rating = community_db.upsert_rating(user.user_id, "b09xyz12", 5)
        assert rating.score == 5

    def test_get_user_rating(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("r@example.com")
        community_db.upsert_rating(user.user_id, "pid1", 4)
        rating = community_db.get_user_rating(user.user_id, "pid1")
        assert rating is not None
        assert rating.score == 4

    def test_get_user_rating_not_found(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("r2@example.com")
        assert community_db.get_user_rating(user.user_id, "nope") is None

    def test_rating_summary(self, community_db: CommunityDB) -> None:
        u1 = community_db.get_or_create_user("a@x.com")
        u2 = community_db.get_or_create_user("b@x.com")
        community_db.upsert_rating(u1.user_id, "pid1", 4)
        community_db.upsert_rating(u2.user_id, "pid1", 2)
        summary = community_db.get_rating_summary("pid1")
        assert summary.average_score == 3.0
        assert summary.total_ratings == 2

    def test_rating_summary_no_ratings(self, community_db: CommunityDB) -> None:
        summary = community_db.get_rating_summary("unrated")
        assert summary.average_score == 0.0
        assert summary.total_ratings == 0

    def test_delete_rating(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("del@x.com")
        community_db.upsert_rating(user.user_id, "pid1", 5)
        community_db.delete_rating(user.user_id, "pid1")
        assert community_db.get_user_rating(user.user_id, "pid1") is None

    def test_rating_score_constraint(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("c@x.com")
        with pytest.raises(sqlite3.IntegrityError):
            community_db.upsert_rating(user.user_id, "pid1", 0)
        with pytest.raises(sqlite3.IntegrityError):
            community_db.upsert_rating(user.user_id, "pid1", 6)


class TestDiscussions:
    """Tests for threaded discussions."""

    def test_create_comment(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("comm@x.com")
        comment = community_db.create_comment("pid1", user.user_id, "Great show!")
        assert comment.comment_id
        assert comment.body == "Great show!"
        assert comment.programme_pid == "pid1"
        assert comment.parent_id == ""

    def test_create_reply(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("reply@x.com")
        parent = community_db.create_comment("pid1", user.user_id, "Parent")
        reply = community_db.create_comment(
            "pid1", user.user_id, "Reply", parent_id=parent.comment_id
        )
        assert reply.parent_id == parent.comment_id

    def test_get_threaded_discussions(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("thread@x.com")
        parent = community_db.create_comment("pid1", user.user_id, "Top level")
        community_db.create_comment(
            "pid1", user.user_id, "Reply 1", parent_id=parent.comment_id
        )
        community_db.create_comment(
            "pid1", user.user_id, "Reply 2", parent_id=parent.comment_id
        )
        comments = community_db.get_programme_discussions("pid1")
        assert len(comments) == 1
        assert len(comments[0].replies) == 2

    def test_discussion_count(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("cnt@x.com")
        community_db.create_comment("pid2", user.user_id, "One")
        community_db.create_comment("pid2", user.user_id, "Two")
        assert community_db.get_discussion_count("pid2") == 2

    def test_empty_discussions(self, community_db: CommunityDB) -> None:
        comments = community_db.get_programme_discussions("empty_pid")
        assert comments == []
        assert community_db.get_discussion_count("empty_pid") == 0

    def test_comment_display_name(self, community_db: CommunityDB) -> None:
        user = community_db.get_or_create_user("display@x.com")
        community_db.update_display_name(user.user_id, "ShowFan")
        comment = community_db.create_comment("pid1", user.user_id, "Hello")
        assert comment.display_name == "ShowFan"
