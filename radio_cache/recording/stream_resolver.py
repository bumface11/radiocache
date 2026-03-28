"""Resolve BBC programme PIDs and station IDs to recordable HLS URLs.

Only public, non-DRM endpoints are used:

* **Live streams** — Well-known BBC Radio HLS URLs served via Akamai
  CDN.  No authentication required.
* **Catch-up programmes** — BBC iBL API returns version PIDs (vpids)
  for an episode; each vpid is fed to the BBC Media Selector which
  returns connection records.  The first HLS connection that is not
  DRM-gated is returned.

DRM circumvention and reverse-engineering are explicitly out of scope.
If no suitable non-DRM HLS URL can be found the function raises
:class:`StreamNotSupportedError`.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Final

from radio_cache.recording.config import HTTP_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

# ── BBC live HLS CDN pattern ────────────────────────────────────────────

_BBC_LIVE_HLS_TMPL: Final[str] = (
    "https://as-hls-ww-live.akamaized.net/pool_904/live/ww"
    "/{station}/{station}.isml/{station}.m3u8"
)

# Stations for which the public Akamai HLS endpoint is known to work.
# Expand as new stations are verified.
KNOWN_STATIONS: Final[frozenset[str]] = frozenset(
    {
        "bbc_radio_one",
        "bbc_radio_oneextra",
        "bbc_radio_two",
        "bbc_radio_three",
        "bbc_radio_fourfm",
        "bbc_radio_four_extra",
        "bbc_radio_five_live",
        "bbc_radio_five_live_sports_extra",
        "bbc_6music",
        "bbc_radio_asian_network_england",
        "bbc_world_service",
        "bbc_radio_scotland_fm",
        "bbc_radio_wales_fm",
        "bbc_radio_cymru",
        "bbc_radio_ulster",
    }
)

# ── BBC catch-up API endpoints ──────────────────────────────────────────

# Returns episode metadata including nested versions (more reliable than the
# deprecated /versions sub-path).
_IBL_EPISODE_URL: Final[str] = (
    "https://ibl.api.bbc.co.uk/ibl/v1/episodes/{pid}"
)

# Classic BBC Programmes JSON API – stable fallback for version PID lookup.
_BBC_PROGRAMMES_JSON_URL: Final[str] = (
    "https://www.bbc.co.uk/programmes/{pid}.json"
)

# Returns media connection records for one version.
_MEDIA_SELECTOR_URL: Final[str] = (
    "https://open.live.bbc.co.uk/mediaselector/6/select/version/2.0"
    "/mediaset/iptv-all/vpid/{vpid}/format/json"
)

_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (compatible; RadioCacheBot/1.0; "
    "+https://github.com/bumface11/radiocache)"
)


# ── Public result type ───────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolvedStream:
    """A successfully resolved, directly recordable stream.

    Attributes:
        manifest_url: HLS (``.m3u8``) URL.
        transfer_format: Always ``"hls"`` for currently supported streams.
        is_live: ``True`` for a live station, ``False`` for catch-up.
    """

    manifest_url: str
    transfer_format: str
    is_live: bool


# ── Exceptions ───────────────────────────────────────────────────────────


class StreamNotSupportedError(Exception):
    """Raised when the stream type is not directly recordable.

    This covers DRM-only content, unsupported station IDs, or manifests
    that are not plain HLS.  It does **not** indicate a transient error;
    retrying will not help.
    """


class StreamUnavailableError(Exception):
    """Raised when a stream exists but is temporarily inaccessible.

    Examples: HTTP 401/403/404, geographic restrictions, expired tokens.
    The caller may surface this as an ``unavailable`` error code.
    """


# ── Public resolution functions ─────────────────────────────────────────


def resolve_live_stream(station_id: str) -> ResolvedStream:
    """Resolve a BBC Radio live station to a public HLS URL.

    Args:
        station_id: BBC station identifier such as ``"bbc_radio_fourfm"``.
            Must be in :data:`KNOWN_STATIONS`.

    Returns:
        :class:`ResolvedStream` with the Akamai HLS manifest URL.

    Raises:
        StreamNotSupportedError: Station is not in the known list.
    """
    sid = station_id.lower().strip()
    if sid not in KNOWN_STATIONS:
        raise StreamNotSupportedError(
            f"Station '{station_id}' is not in the supported BBC Radio station "
            "list.  Only stations with public Akamai HLS endpoints are "
            f"recordable.  Known stations: {sorted(KNOWN_STATIONS)}"
        )
    url = _BBC_LIVE_HLS_TMPL.format(station=sid)
    logger.info("Resolved live stream: %s -> %s", station_id, url)
    return ResolvedStream(manifest_url=url, transfer_format="hls", is_live=True)


def resolve_programme_stream(pid: str) -> ResolvedStream:
    """Resolve a BBC catch-up episode PID to a non-DRM HLS URL.

    Steps:
    1. Query BBC iBL API for version PIDs associated with the episode.
    2. For each version PID, query BBC Media Selector for connections.
    3. Return the first HLS connection that is not DRM-gated.

    Args:
        pid: BBC programme/episode PID (e.g. ``"m002snjn"``).

    Returns:
        :class:`ResolvedStream` with the HLS manifest URL.

    Raises:
        StreamUnavailableError: No versions found or all returned HTTP
            error responses.
        StreamNotSupportedError: Versions found but none offered a
            plain HLS connection (likely DRM-only or proprietary).
    """
    logger.info("Resolving programme stream for pid=%s", pid)
    vpids = _fetch_version_pids(pid)
    if not vpids:
        raise StreamUnavailableError(
            f"No playable versions found for programme '{pid}'.  "
            "The programme may be expired or unavailable in your region."
        )

    last_exc: Exception | None = None
    for vpid in vpids:
        try:
            hls_url = _select_hls_from_media_selector(vpid)
            logger.info(
                "Resolved programme pid=%s via vpid=%s -> %s", pid, vpid, hls_url
            )
            return ResolvedStream(
                manifest_url=hls_url, transfer_format="hls", is_live=False
            )
        except (StreamNotSupportedError, StreamUnavailableError) as exc:
            last_exc = exc
            logger.debug("vpid %s not usable: %s", vpid, exc)

    if isinstance(last_exc, StreamUnavailableError):
        raise last_exc
    raise StreamNotSupportedError(
        f"No non-DRM HLS stream found for programme '{pid}'.  "
        "This title may only be available with DRM protection, which is "
        "not supported by this tool."
    )


# ── Internal helpers ─────────────────────────────────────────────────────


def _fetch_version_pids(pid: str) -> list[str]:
    """Return version PIDs for an episode, trying two BBC endpoints.

    Strategy:
    1. ``ibl.api.bbc.co.uk/ibl/v1/episodes/{pid}`` — iBL episode object;
       versions are nested under ``episode.versions[].id``.
    2. ``www.bbc.co.uk/programmes/{pid}.json`` — classic Programmes API;
       versions sit under ``programme.versions[].pid``.

    Any HTTP error from endpoint 1 is caught and logged so that
    endpoint 2 is always attempted before giving up.

    Args:
        pid: Episode PID.

    Returns:
        Ordered list of version PIDs, empty when neither endpoint
        returns usable data.

    Raises:
        StreamUnavailableError: Both endpoints returned HTTP 401/403
            (geo-blocked or rights issue).
    """
    # ── Attempt 1: iBL episode endpoint ─────────────────────────────
    ibl_url = _IBL_EPISODE_URL.format(pid=pid)
    try:
        data = _fetch_json(ibl_url, reraise_http=True)
        if data:
            versions: list = (
                (data.get("episode") or {}).get("versions")
                or data.get("versions")
                or []
            )
            pids = [v["id"] for v in versions if isinstance(v, dict) and v.get("id")]
            if pids:
                logger.debug("iBL endpoint returned %d version(s) for pid=%s", len(pids), pid)
                return pids
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise StreamUnavailableError(
                f"BBC iBL API returned HTTP {exc.code} for programme '{pid}'.  "
                "The programme may be geo-blocked or rights-protected."
            ) from exc
        # 404 or other transient errors: fall through to next attempt.
        logger.debug("iBL API HTTP %s for pid=%s, trying BBC Programmes API", exc.code, pid)

    # ── Attempt 2: BBC Programmes JSON API ───────────────────────────
    prog_url = _BBC_PROGRAMMES_JSON_URL.format(pid=pid)
    try:
        data2 = _fetch_json(prog_url, reraise_http=True)
        if data2:
            versions2: list = (data2.get("programme") or {}).get("versions") or []
            pids2 = [v["pid"] for v in versions2 if isinstance(v, dict) and v.get("pid")]
            if pids2:
                logger.debug(
                    "BBC Programmes API returned %d version(s) for pid=%s", len(pids2), pid
                )
                return pids2
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise StreamUnavailableError(
                f"BBC Programmes API returned HTTP {exc.code} for programme '{pid}'.  "
                "The programme may be geo-blocked or rights-protected."
            ) from exc
        logger.debug("BBC Programmes API HTTP %s for pid=%s", exc.code, pid)

    return []


def _select_hls_from_media_selector(vpid: str) -> str:
    """Query BBC Media Selector and return the best HLS URL.

    Args:
        vpid: Version PID.

    Returns:
        HLS manifest URL string.

    Raises:
        StreamUnavailableError: HTTP 4xx or empty/error response.
        StreamNotSupportedError: Response contains no HLS connection.
    """
    url = _MEDIA_SELECTOR_URL.format(vpid=vpid)
    try:
        data = _fetch_json(url, reraise_http=True)
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403, 404):
            raise StreamUnavailableError(
                f"Media Selector returned HTTP {exc.code} for vpid '{vpid}'.  "
                "Programme may be expired, geo-blocked, or rights-protected."
            ) from exc
        raise

    if not data:
        raise StreamUnavailableError(
            f"Empty response from Media Selector for vpid '{vpid}'."
        )

    result = data.get("result")
    if result and result != "success":
        raise StreamUnavailableError(
            f"Media Selector result='{result}' for vpid '{vpid}'."
        )

    for media in data.get("media") or []:
        if not isinstance(media, dict):
            continue
        if media.get("kind") in ("captions", "sign_language"):
            continue
        for conn in media.get("connection") or []:
            if not isinstance(conn, dict):
                continue
            if conn.get("transferFormat") == "hls":
                href = conn.get("href", "")
                if href:
                    return href

    raise StreamNotSupportedError(
        f"No HLS connection in Media Selector response for vpid '{vpid}'.  "
        "The stream may only be available with DRM or in an unsupported format."
    )


def _fetch_json(url: str, *, reraise_http: bool = False) -> dict | None:
    """Fetch and parse JSON from *url*.

    Args:
        url: Target URL.
        reraise_http: When ``True``, :class:`urllib.error.HTTPError` is
            re-raised so callers can inspect the status code.  When
            ``False`` (default), HTTP errors are logged and ``None`` is
            returned, the same as other network failures.

    Returns:
        Parsed dict, or ``None`` on network/parse failure.

    Raises:
        urllib.error.HTTPError: Only when *reraise_http* is ``True``.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        if reraise_http:
            raise
        logger.error("Failed to fetch %s: HTTP %s", url, exc.code)
        return None
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
        logger.error("Failed to fetch %s: %s", url, exc)
        return None
