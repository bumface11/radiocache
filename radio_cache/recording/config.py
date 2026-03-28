"""Environment-based configuration for the recording service.

All values are read from environment variables at import time so that
they can be overridden per-process without code changes.  Tests that
need different values should patch the relevant constant.
"""

from __future__ import annotations

import os
from typing import Final

# Directory where recorded audio files are saved.
RECORDINGS_OUTPUT_DIR: Final[str] = os.environ.get(
    "RECORDINGS_OUTPUT_DIR", "recordings"
)

# Default container format for captured audio.  ``m4a`` streams AAC
# directly from BBC without re-encoding; ``mp3`` triggers a transcode.
DEFAULT_RECORDING_FORMAT: Final[str] = os.environ.get(
    "DEFAULT_RECORDING_FORMAT", "m4a"
)

# Hard cap on live recording duration to prevent runaway captures.
MAX_LIVE_RECORDING_SECONDS: Final[int] = int(
    os.environ.get("MAX_LIVE_RECORDING_SECONDS", "14400")  # 4 hours
)

# Seconds before giving up on an HTTP request.
HTTP_TIMEOUT_SECONDS: Final[int] = int(
    os.environ.get("HTTP_TIMEOUT_SECONDS", "30")
)

# Number of times to retry a failed ffmpeg invocation.
HTTP_RETRY_COUNT: Final[int] = int(
    os.environ.get("HTTP_RETRY_COUNT", "3")
)

# Path to the ffmpeg binary.  ``"ffmpeg"`` resolves via PATH.
FFMPEG_PATH: Final[str] = os.environ.get("FFMPEG_PATH", "ffmpeg")
