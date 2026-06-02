"""FastAPI routes for community features: auth, ratings, discussions.

Mounts as a sub-application or included router in the main app.
"""

from __future__ import annotations

import html
import logging
import os
import re
from typing import Final

from fastapi import APIRouter, Cookie, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from radio_cache.community.db import CommunityDB
from radio_cache.community.models import Discussion, User

logger = logging.getLogger(__name__)

router = APIRouter(tags=["community"])

_COMMUNITY_DB_PATH: Final[str] = os.environ.get(
    "COMMUNITY_DB_PATH", "community.db"
)

_EMAIL_RE: Final[re.Pattern[str]] = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

templates = Jinja2Templates(directory="templates/radio_cache")


def _get_community_db() -> CommunityDB:
    """Get a community database connection."""
    return CommunityDB(_COMMUNITY_DB_PATH)


def _get_current_user(session_id: str | None) -> User | None:
    """Resolve the current user from a session cookie."""
    if not session_id:
        return None
    with _get_community_db() as db:
        return db.get_session_user(session_id)


def _sanitise_body(text: str) -> str:
    """Sanitise user-provided text to prevent XSS."""
    return html.escape(text.strip())


# ── Authentication endpoints ──────────────────────────────────────────


@router.get("/community/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    """Render the magic link login page."""
    return templates.TemplateResponse(
        request, "community/login.html", {"request": request}
    )


@router.post("/community/login")
async def request_magic_link(request: Request) -> HTMLResponse:
    """Generate a magic link and show confirmation.

    In production, this would send an email. For this implementation,
    the magic link is displayed directly (suitable for development and
    self-hosted instances where the user is the admin).
    """
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()

    if not email or not _EMAIL_RE.match(email):
        return templates.TemplateResponse(
            request,
            "community/login.html",
            {"request": request, "error": "Please enter a valid email address."},
        )

    with _get_community_db() as db:
        token = db.create_magic_token(email)

    # Build the verification URL
    base_url = str(request.base_url).rstrip("/")
    magic_url = f"{base_url}/community/verify?token={token.token}"

    return templates.TemplateResponse(
        request,
        "community/magic_link_sent.html",
        {
            "request": request,
            "email": email,
            "magic_url": magic_url,
        },
    )


@router.get("/community/verify")
async def verify_magic_link(
    request: Request,
    token: str = Query(..., description="Magic link token"),
) -> Response:
    """Verify a magic link token and create a session."""
    with _get_community_db() as db:
        email = db.verify_magic_token(token)
        if not email:
            return templates.TemplateResponse(
                request,
                "community/login.html",
                {
                    "request": request,
                    "error": (
                        "This link is invalid or has expired."
                        " Please request a new one."
                    ),
                },
            )
        user = db.get_or_create_user(email)
        session = db.create_session(user.user_id)

    response = RedirectResponse(url="/community/profile", status_code=303)
    response.set_cookie(
        key="rc_session",
        value=session.session_id,
        max_age=30 * 24 * 3600,
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/community/profile", response_class=HTMLResponse)
async def profile_page(
    request: Request,
    rc_session: str | None = Cookie(default=None),
) -> Response:
    """Show the user's profile page."""
    user = _get_current_user(rc_session)
    if not user:
        return RedirectResponse(url="/community/login", status_code=303)
    return templates.TemplateResponse(
        request,
        "community/profile.html",
        {"request": request, "user": user},
    )


@router.post("/community/profile")
async def update_profile(
    request: Request,
    rc_session: str | None = Cookie(default=None),
) -> Response:
    """Update the user's display name."""
    user = _get_current_user(rc_session)
    if not user:
        return RedirectResponse(url="/community/login", status_code=303)

    form = await request.form()
    display_name = _sanitise_body(str(form.get("display_name", "")))[:50]
    if display_name:
        with _get_community_db() as db:
            db.update_display_name(user.user_id, display_name)

    return RedirectResponse(url="/community/profile", status_code=303)


@router.post("/community/logout")
async def logout(
    request: Request,
    rc_session: str | None = Cookie(default=None),
) -> RedirectResponse:
    """Log out and clear the session."""
    if rc_session:
        with _get_community_db() as db:
            db.delete_session(rc_session)
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("rc_session")
    return response


# ── Ratings endpoints ─────────────────────────────────────────────────


@router.post("/api/community/ratings/{programme_pid}")
async def rate_programme(
    request: Request,
    programme_pid: str,
    rc_session: str | None = Cookie(default=None),
) -> JSONResponse:
    """Submit or update a rating for a programme."""
    user = _get_current_user(rc_session)
    if not user:
        return JSONResponse(
            {"error": "Authentication required"}, status_code=401
        )

    body = await request.json()
    score = body.get("score")
    if not isinstance(score, int) or score < 1 or score > 5:
        return JSONResponse(
            {"error": "Score must be an integer between 1 and 5"},
            status_code=400,
        )

    with _get_community_db() as db:
        rating = db.upsert_rating(user.user_id, programme_pid, score)
        summary = db.get_rating_summary(programme_pid)

    return JSONResponse({
        "score": rating.score,
        "average_score": summary.average_score,
        "total_ratings": summary.total_ratings,
    })


@router.get("/api/community/ratings/{programme_pid}")
async def get_programme_rating(
    request: Request,
    programme_pid: str,
    rc_session: str | None = Cookie(default=None),
) -> JSONResponse:
    """Get rating summary and the current user's rating for a programme."""
    with _get_community_db() as db:
        summary = db.get_rating_summary(programme_pid)
        user_score = None
        user = _get_current_user(rc_session)
        if user:
            user_rating = db.get_user_rating(user.user_id, programme_pid)
            if user_rating:
                user_score = user_rating.score

    return JSONResponse({
        "average_score": summary.average_score,
        "total_ratings": summary.total_ratings,
        "user_score": user_score,
    })


@router.delete("/api/community/ratings/{programme_pid}")
async def delete_programme_rating(
    request: Request,
    programme_pid: str,
    rc_session: str | None = Cookie(default=None),
) -> JSONResponse:
    """Remove the current user's rating for a programme."""
    user = _get_current_user(rc_session)
    if not user:
        return JSONResponse(
            {"error": "Authentication required"}, status_code=401
        )
    with _get_community_db() as db:
        db.delete_rating(user.user_id, programme_pid)
        summary = db.get_rating_summary(programme_pid)

    return JSONResponse({
        "average_score": summary.average_score,
        "total_ratings": summary.total_ratings,
    })


# ── Discussion endpoints ──────────────────────────────────────────────


@router.get("/community/discussions/{programme_pid}", response_class=HTMLResponse)
async def discussion_page(
    request: Request,
    programme_pid: str,
    rc_session: str | None = Cookie(default=None),
) -> HTMLResponse:
    """Render the discussion thread for a programme."""
    user = _get_current_user(rc_session)
    with _get_community_db() as db:
        comments = db.get_programme_discussions(programme_pid)

    return templates.TemplateResponse(
        request,
        "community/discussions.html",
        {
            "request": request,
            "programme_pid": programme_pid,
            "comments": comments,
            "user": user,
        },
    )


@router.post("/api/community/discussions/{programme_pid}")
async def post_comment(
    request: Request,
    programme_pid: str,
    rc_session: str | None = Cookie(default=None),
) -> JSONResponse:
    """Post a new comment or reply in a programme's discussion."""
    user = _get_current_user(rc_session)
    if not user:
        return JSONResponse(
            {"error": "Authentication required"}, status_code=401
        )

    body_data = await request.json()
    comment_body = _sanitise_body(str(body_data.get("body", "")))
    parent_id = str(body_data.get("parent_id", ""))

    if not comment_body:
        return JSONResponse(
            {"error": "Comment body cannot be empty"}, status_code=400
        )
    if len(comment_body) > 2000:
        return JSONResponse(
            {"error": "Comment must be 2000 characters or fewer"},
            status_code=400,
        )

    with _get_community_db() as db:
        comment = db.create_comment(
            programme_pid=programme_pid,
            user_id=user.user_id,
            body=comment_body,
            parent_id=parent_id,
        )

    return JSONResponse(
        {
            "comment_id": comment.comment_id,
            "body": comment.body,
            "display_name": comment.display_name,
            "created_at": comment.created_at,
            "parent_id": comment.parent_id,
        },
        status_code=201,
    )


@router.get("/api/community/discussions/{programme_pid}")
async def get_discussions(
    request: Request,
    programme_pid: str,
) -> JSONResponse:
    """Get all discussion comments for a programme as JSON."""
    with _get_community_db() as db:
        comments = db.get_programme_discussions(programme_pid)

    def _serialise(comment: Discussion) -> dict:
        return {
            "comment_id": comment.comment_id,
            "user_id": comment.user_id,
            "display_name": comment.display_name,
            "parent_id": comment.parent_id,
            "body": comment.body,
            "created_at": comment.created_at,
            "replies": [_serialise(r) for r in comment.replies],
        }

    return JSONResponse([_serialise(c) for c in comments])
