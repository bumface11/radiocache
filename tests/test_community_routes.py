"""Tests for community API routes."""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

# Use in-memory DB for tests
os.environ["COMMUNITY_DB_PATH"] = ":memory:"

from radio_cache_api import app  # noqa: E402


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


class TestCommunityRoutes:
    """Integration tests for community HTTP endpoints."""

    def test_login_page(self, client: TestClient) -> None:
        resp = client.get("/community/login")
        assert resp.status_code == 200
        assert "Magic Link" in resp.text

    def test_request_magic_link_invalid_email(self, client: TestClient) -> None:
        resp = client.post(
            "/community/login", data={"email": "not-an-email"}
        )
        assert resp.status_code == 200
        assert "valid email" in resp.text

    def test_request_magic_link_valid(self, client: TestClient) -> None:
        resp = client.post(
            "/community/login", data={"email": "test@example.com"}
        )
        assert resp.status_code == 200
        assert "verify?token=" in resp.text

    def test_verify_invalid_token(self, client: TestClient) -> None:
        resp = client.get(
            "/community/verify", params={"token": "bad_token"},
            follow_redirects=False,
        )
        assert resp.status_code == 200
        assert "invalid or has expired" in resp.text

    def test_profile_requires_auth(self, client: TestClient) -> None:
        resp = client.get("/community/profile", follow_redirects=False)
        assert resp.status_code == 303
        assert "/community/login" in resp.headers["location"]

    def test_rate_requires_auth(self, client: TestClient) -> None:
        resp = client.post(
            "/api/community/ratings/b09xyz12",
            json={"score": 4},
        )
        assert resp.status_code == 401

    def test_get_rating_no_ratings(self, client: TestClient) -> None:
        resp = client.get("/api/community/ratings/b09xyz12")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_ratings"] == 0
        assert data["average_score"] == 0.0

    def test_post_comment_requires_auth(self, client: TestClient) -> None:
        resp = client.post(
            "/api/community/discussions/b09xyz12",
            json={"body": "Hello"},
        )
        assert resp.status_code == 401

    def test_get_discussions_empty(self, client: TestClient) -> None:
        resp = client.get("/api/community/discussions/b09xyz12")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_discussion_page(self, client: TestClient) -> None:
        resp = client.get("/community/discussions/b09xyz12")
        assert resp.status_code == 200
        assert "Discussion" in resp.text
