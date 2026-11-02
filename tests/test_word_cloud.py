"""Tests for the word-cloud frequency query."""

from __future__ import annotations

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme


def _db() -> CacheDB:
    db = CacheDB(":memory:")
    db.upsert_programmes(
        [
            Programme(pid="p1", title="Mystery at the Manor", synopsis="A detective investigates murder."),
            Programme(pid="p2", title="Murder on the Moor", synopsis="Detective work in the fog."),
            Programme(pid="p3", title="Gardening Hour", synopsis="Roses and soil.", categories="Factual"),
        ]
    )
    return db


def test_counts_words_once_per_programme_and_skips_stopwords() -> None:
    words = {w["word"]: w["count"] for w in _db().word_frequencies()}
    assert words["detective"] == 2
    assert words["murder"] == 2
    assert "the" not in words


def test_large_series_counts_once() -> None:
    db = CacheDB(":memory:")
    db.upsert_programmes(
        [
            Programme(
                pid=f"e{i}", title=f"Ep {i}", synopsis="", series_pid="s1", series_title="Free Thinking"
            )
            for i in range(50)
        ]
        + [Programme(pid="solo", title="Free Spirit", synopsis="")]
    )
    words = {w["word"]: w["count"] for w in db.word_frequencies()}
    assert words["free"] == 2
    assert words["thinking"] == 1


def test_query_narrows_programmes_and_excludes_query_words() -> None:
    words = {w["word"]: w["count"] for w in _db().word_frequencies("detective")}
    assert "detective" not in words
    assert "fog" in words
    assert "roses" not in words


def test_category_filter_and_limit() -> None:
    db = _db()
    words = {w["word"] for w in db.word_frequencies(category="Factual")}
    assert "roses" in words and "murder" not in words
    assert len(db.word_frequencies(limit=2)) == 2
