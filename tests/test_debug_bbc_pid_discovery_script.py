"""Tests for scripts/debug_bbc_pid_discovery.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "debug_bbc_pid_discovery.py"
)


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "debug_bbc_pid_discovery",
        _SCRIPT_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_found_in_category_reports_match(capsys: pytest.CaptureFixture[str]) -> None:
    module = _load_module()

    def fake_fetch(url: str) -> dict[str, Any]:
        if url.endswith("/b01c7s27.json"):
            return {
                "programme": {
                    "urn": "urn:bbc:radio:episode:b01c7s27",
                    "titles": {"primary": "Tim Key's Poetry Programme"},
                    "categories": [{"id": "comedy", "title": "Comedy"}],
                }
            }
        return {
            "data": [
                {
                    "urn": "urn:bbc:radio:episode:b01c7s27",
                    "titles": {"primary": "Tim Key's Poetry Programme"},
                }
            ],
            "total": 1,
        }

    with patch.object(module, "_fetch_json", side_effect=fake_fetch):
        code = module.main(["b01c7s27", "--category", "comedy", "--pages", "2"])

    out = capsys.readouterr().out
    assert code == 0
    assert "Resolved PID: b01c7s27" in out
    assert out.count("=== Category scan: comedy") == 1
    assert "FOUND in category 'comedy'" in out


def test_not_found_is_not_an_error(capsys: pytest.CaptureFixture[str]) -> None:
    module = _load_module()

    def fake_fetch(url: str) -> dict[str, Any]:
        if url.endswith("/b01c7s27.json"):
            return {
                "programme": {
                    "urn": "urn:bbc:radio:episode:b01c7s27",
                    "titles": {"primary": "Tim Key's Poetry Programme"},
                }
            }
        return {"data": [], "total": 0}

    with patch.object(module, "_fetch_json", side_effect=fake_fetch):
        code = module.main(["b01c7s27", "--category", "comedy"])

    out = capsys.readouterr().out
    assert code == 0
    assert "NOT FOUND in category 'comedy'" in out


def test_programme_detail_fetch_failure_returns_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()

    with patch.object(module, "_fetch_json", return_value=None):
        code = module.main(["b01c7s27", "--category", "comedy"])

    out = capsys.readouterr().out
    assert code == 1
    assert "ERROR: Failed to fetch programme detail" in out
