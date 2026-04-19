"""Tests for the radio_cache.bbc_feed_parser module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from radio_cache.bbc_feed_parser import (
    _PAGE_LIMIT,
    _parse_programme_item,
    fetch_all_category_slugs,
    fetch_drama_programmes,
    _fetch_container_episodes,
)


class TestParseProgrammeItem:
    """Tests for _parse_programme_item."""

    def test_minimal_item(self) -> None:
        """Parses an item using the episode PID from URN."""
        item = {"urn": "urn:bbc:radio:episode:b09test", "title": "Test"}
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "b09test"

    def test_missing_urn(self) -> None:
        """Returns None when no URN is present."""
        item = {"title": "No ID"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_full_item(self) -> None:
        """Parses a fully populated BBC API item."""
        item = {
            "urn": "urn:bbc:radio:episode:b09full",
            "titles": {
                "primary": "Drama Hour",
                "secondary": "Episode 1",
            },
            "synopses": {
                "short": "Short desc",
                "medium": "Medium desc",
            },
            "duration": {"value": 1800},
            "availability": {"to": "2026-06-01T00:00:00Z"},
            "release": {"date": "2026-03-01T14:00:00Z"},
            "container": {"id": "s_drama", "title": "Drama Hour Series"},
            "network": {"short_title": "Radio 4"},
            "image_url": "https://example.com/{recipe}/thumb.jpg",
            "episode_number": 1,
            "type": "episode",
            "categories": [
                {"id": "drama", "title": "Drama"},
                {"id": "thriller", "title": "Thriller"},
            ],
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "b09full"
        assert prog.title == "Drama Hour: Episode 1"
        assert prog.synopsis == "Short desc"
        assert prog.duration_secs == 1800
        assert prog.series_pid == "s_drama"
        assert prog.channel == "Radio 4"
        assert "624x624" in prog.thumbnail_url
        assert "Drama" in prog.categories
        assert "Thriller" in prog.categories

    def test_categories_broader_hierarchy(self) -> None:
        """Traverses broader.category hierarchy like get_iplayer."""
        item = {
            "urn": "urn:bbc:radio:episode:b09hier",
            "categories": [
                {
                    "id": "thriller",
                    "title": "Thriller",
                    "broader": {
                        "category": {
                            "id": "drama",
                            "title": "Drama",
                            "broader": {
                                "category": {
                                    "id": "audio",
                                    "title": "Audio",
                                }
                            },
                        }
                    },
                }
            ],
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        cats = prog.categories.split(",")
        assert "Thriller" in cats
        assert "Drama" in cats
        assert "Audio" in cats
        # Broadest should appear before narrowest (get_iplayer ordering)
        assert cats.index("Audio") < cats.index("Drama")
        assert cats.index("Drama") < cats.index("Thriller")

    def test_categories_empty_when_no_field(self) -> None:
        """Returns empty categories string when categories field absent."""
        item = {"urn": "urn:bbc:radio:episode:b09nocat", "title": "No cats"}
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.categories == ""

    def test_urn_extracts_episode_pid(self) -> None:
        """Extracts episode PID from the URN, ignoring pid and id fields."""
        item = {
            "id": "p0n6f5q8",
            "pid": "m002t10q",
            "urn": "urn:bbc:radio:episode:m002snjn",
            "title": "URN Episode",
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "m002snjn"

    def test_urn_clip(self) -> None:
        """Extracts clip PID from a clip URN."""
        item = {
            "id": "some_id",
            "urn": "urn:bbc:radio:clip:p0abc123",
            "title": "Clip",
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "p0abc123"

    def test_id_fallback_without_urn(self) -> None:
        """Rejects 'id' when 'urn' is not present."""
        item = {"id": "b09test", "title": "ID fallback"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_pid_fallback_without_urn_or_id(self) -> None:
        """Rejects version pid when no URN is present."""
        item = {"pid": "b09pid", "title": "PID only"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_synopsis_fallback(self) -> None:
        """Falls back through synopsis fields."""
        item = {
            "urn": "urn:bbc:radio:episode:b09syn",
            "synopses": {"long": "Long description only"},
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.synopsis == "Long description only"


class TestFetchDramaProgrammes:
    """Tests for fetch_drama_programmes."""

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_uses_playable_endpoint(self, mock_fetch: MagicMock) -> None:
        """Uses the /v2/programmes/playable endpoint."""
        mock_fetch.return_value = {"data": [], "total": 0}
        fetch_drama_programmes(category_slugs=["drama"], max_pages=1, delay=0)
        url = mock_fetch.call_args[0][0]
        assert "/v2/programmes/playable?" in url
        assert "category=drama" in url
        assert "inline/categories" not in url

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_pagination_uses_offset(self, mock_fetch: MagicMock) -> None:
        """Pagination uses offset and limit query parameters."""
        items = [
            {
                "urn": f"urn:bbc:radio:episode:p{i:04d}",
                "title": f"P{i}",
            }
            for i in range(_PAGE_LIMIT)
        ]
        mock_fetch.side_effect = [
            {"data": items, "total": _PAGE_LIMIT + 5},
            {
                "data": [
                    {
                        "urn": "urn:bbc:radio:episode:p9999",
                        "title": "Last",
                    }
                ],
                "total": _PAGE_LIMIT + 5,
            },
        ]
        fetch_drama_programmes(category_slugs=["drama"], max_pages=5, delay=0)
        urls = [c[0][0] for c in mock_fetch.call_args_list]
        assert "offset=0" in urls[0]
        assert f"offset={_PAGE_LIMIT}" in urls[1]

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_default_fetches_beyond_ten_pages(self, mock_fetch: MagicMock) -> None:
        """Default max_pages should allow discovery past page 10."""
        pages = []
        for page in range(11):
            items = [
                {
                    "urn": (
                        f"urn:bbc:radio:episode:p{page:02d}_{i:02d}"
                    ),
                    "title": f"Episode {page}-{i}",
                }
                for i in range(_PAGE_LIMIT)
            ]
            pages.append(
                {
                    "data": items,
                    "total": 11 * _PAGE_LIMIT,
                }
            )
        pages.append({"data": [], "total": 11 * _PAGE_LIMIT})
        mock_fetch.side_effect = pages

        result = fetch_drama_programmes(category_slugs=["drama"], delay=0)
        pids = {p.pid for p in result}

        assert len(result) == 11 * _PAGE_LIMIT
        assert "p10_29" in pids

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_includes_tleo_distinct(self, mock_fetch: MagicMock) -> None:
        """URL includes tleoDistinct=true."""
        mock_fetch.return_value = {"data": [], "total": 0}
        fetch_drama_programmes(category_slugs=["drama"], max_pages=1, delay=0)
        url = mock_fetch.call_args[0][0]
        assert "tleoDistinct=true" in url

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_deduplicates_across_categories(self, mock_fetch: MagicMock) -> None:
        """Programmes seen in multiple categories are returned once."""
        item = {"urn": "urn:bbc:radio:episode:b09dup", "title": "Shared Drama"}
        mock_fetch.return_value = {"data": [item], "total": 1}
        result = fetch_drama_programmes(
            category_slugs=["drama", "thriller"], max_pages=1, delay=0
        )
        assert len(result) == 1
        assert result[0].pid == "b09dup"

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_slug_used_as_category_fallback(self, mock_fetch: MagicMock) -> None:
        """Slug display name is recorded as category when API returns no categories."""
        item = {"urn": "urn:bbc:radio:episode:b09nocat", "title": "No Category Item"}
        mock_fetch.return_value = {"data": [item], "total": 1}
        result = fetch_drama_programmes(
            category_slugs=["thriller"], max_pages=1, delay=0
        )
        assert len(result) == 1
        assert "Thriller" in result[0].categories

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_categories_merged_across_slugs(self, mock_fetch: MagicMock) -> None:
        """When a programme appears in two slug searches its categories are merged."""
        item = {"urn": "urn:bbc:radio:episode:b09multi", "title": "Multi Genre"}
        mock_fetch.return_value = {"data": [item], "total": 1}
        result = fetch_drama_programmes(
            category_slugs=["drama", "thriller"], max_pages=1, delay=0
        )
        assert len(result) == 1
        cats = set(c.strip() for c in result[0].categories.split(","))
        assert "Drama" in cats
        assert "Thriller" in cats

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_api_categories_merged_with_slug(self, mock_fetch: MagicMock) -> None:
        """Categories from the API response are preserved alongside slug category."""
        item = {
            "urn": "urn:bbc:radio:episode:b09apicat",
            "title": "API Category Item",
            "categories": [{"id": "crime", "title": "Crime"}],
        }
        mock_fetch.return_value = {"data": [item], "total": 1}
        result = fetch_drama_programmes(
            category_slugs=["thriller"], max_pages=1, delay=0
        )
        assert len(result) == 1
        cats = set(c.strip() for c in result[0].categories.split(","))
        assert "Crime" in cats
        assert "Thriller" in cats

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_existing_pids_stop_pagination_when_page_is_fully_cached(
        self, mock_fetch: MagicMock
    ) -> None:
        """Recent refresh can stop paging once a page contains only cached items."""
        cached_page = [
            {
                "urn": f"urn:bbc:radio:episode:old{i}",
                "title": f"Old {i}",
            }
            for i in range(_PAGE_LIMIT)
        ]
        mock_fetch.side_effect = [
            {"data": cached_page, "total": _PAGE_LIMIT * 3},
        ]

        result = fetch_drama_programmes(
            category_slugs=["drama"],
            max_pages=5,
            delay=0,
            existing_pids={f"old{i}" for i in range(_PAGE_LIMIT)},
        )

        assert len(result) == _PAGE_LIMIT
        assert mock_fetch.call_count == 1


class TestFetchAllCategorySlugs:
    """Tests for fetch_all_category_slugs."""

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_returns_slugs_from_api(self, mock_fetch: MagicMock) -> None:
        """Extracts slug ids from the BBC categories API response."""
        mock_fetch.return_value = {
            "data": [
                {"id": "drama", "title": "Drama"},
                {"id": "thriller", "title": "Thriller"},
                {"id": "comedy", "title": "Comedy"},
            ]
        }
        slugs = fetch_all_category_slugs()
        assert "drama" in slugs
        assert "thriller" in slugs
        assert "comedy" in slugs

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_falls_back_to_builtin_on_api_failure(self, mock_fetch: MagicMock) -> None:
        """Falls back to built-in slug list when API returns None."""
        mock_fetch.return_value = None
        from radio_cache.bbc_feed_parser import _CATEGORY_SLUGS
        slugs = fetch_all_category_slugs()
        assert slugs == list(_CATEGORY_SLUGS)

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_falls_back_to_builtin_on_empty_response(self, mock_fetch: MagicMock) -> None:
        """Falls back to built-in slug list when API returns empty data."""
        mock_fetch.return_value = {"data": []}
        from radio_cache.bbc_feed_parser import _CATEGORY_SLUGS
        slugs = fetch_all_category_slugs()
        assert slugs == list(_CATEGORY_SLUGS)

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_queries_correct_endpoint(self, mock_fetch: MagicMock) -> None:
        """Queries the BBC categories API with medium=audio."""
        mock_fetch.return_value = None
        fetch_all_category_slugs()
        url = mock_fetch.call_args[0][0]
        assert "rms.api.bbc.co.uk/v2/categories" in url
        assert "medium=audio" in url


class TestFetchContainerEpisodes:
    """Tests for _fetch_container_episodes."""

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_fetches_episodes_for_container(self, mock_fetch: MagicMock) -> None:
        """Returns episodes from the container endpoint."""
        items = [
            {"urn": f"urn:bbc:radio:episode:ep{i}", "title": f"Ep {i}"}
            for i in range(3)
        ]
        mock_fetch.return_value = {"data": items, "total": 3}
        result = _fetch_container_episodes("b006qykl", max_pages=5, delay=0)
        assert len(result) == 3
        assert result[0].pid == "ep0"

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_uses_container_param(self, mock_fetch: MagicMock) -> None:
        """URL uses container= parameter, not category=."""
        mock_fetch.return_value = {"data": [], "total": 0}
        _fetch_container_episodes("b006qykl", max_pages=1, delay=0)
        url = mock_fetch.call_args[0][0]
        assert "container=b006qykl" in url
        assert "category=" not in url

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_paginates(self, mock_fetch: MagicMock) -> None:
        """Pages through container results."""
        page1 = [
            {"urn": f"urn:bbc:radio:episode:ep{i}", "title": f"Ep {i}"}
            for i in range(_PAGE_LIMIT)
        ]
        page2 = [
            {"urn": "urn:bbc:radio:episode:last", "title": "Last"}
        ]
        mock_fetch.side_effect = [
            {"data": page1, "total": _PAGE_LIMIT + 1},
            {"data": page2, "total": _PAGE_LIMIT + 1},
        ]
        result = _fetch_container_episodes("brand1", max_pages=5, delay=0)
        assert len(result) == _PAGE_LIMIT + 1
        urls = [c[0][0] for c in mock_fetch.call_args_list]
        assert "offset=0" in urls[0]
        assert f"offset={_PAGE_LIMIT}" in urls[1]

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_handles_api_failure(self, mock_fetch: MagicMock) -> None:
        """Returns empty list on API failure."""
        mock_fetch.return_value = None
        result = _fetch_container_episodes("brand_bad", max_pages=1, delay=0)
        assert result == []


class TestBackfillContainers:
    """Tests for the container backfill phase in fetch_drama_programmes."""

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_backfill_expands_brand_episodes(self, mock_fetch: MagicMock) -> None:
        """Backfill fetches additional episodes for discovered brands."""
        # Phase 1: category scan finds one episode with a brand PID
        category_item = {
            "urn": "urn:bbc:radio:episode:ep1",
            "title": "Latest Episode",
            "container": {"id": "series1", "title": "My Series"},
            "brand": {"id": "brand1", "title": "My Brand"},
        }
        category_response = {"data": [category_item], "total": 1}

        # Phase 2: container backfill returns more episodes
        backfill_items = [
            {"urn": "urn:bbc:radio:episode:ep1", "title": "Latest Episode"},
            {"urn": "urn:bbc:radio:episode:ep2", "title": "Older Episode"},
            {"urn": "urn:bbc:radio:episode:ep3", "title": "Oldest Episode"},
        ]
        backfill_response = {"data": backfill_items, "total": 3}

        mock_fetch.side_effect = [
            category_response,  # category scan
            backfill_response,  # container backfill for series1
        ]
        result = fetch_drama_programmes(
            category_slugs=["drama"],
            max_pages=1,
            delay=0,
            backfill_containers=True,
            container_max_pages=5,
        )
        pids = {p.pid for p in result}
        assert len(result) == 3
        assert "ep1" in pids
        assert "ep2" in pids
        assert "ep3" in pids

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_backfill_inherits_categories(self, mock_fetch: MagicMock) -> None:
        """Backfilled episodes inherit categories from the category scan."""
        category_item = {
            "urn": "urn:bbc:radio:episode:ep1",
            "title": "Episode",
            "container": {"id": "series1", "title": "Series"},
        }
        category_response = {"data": [category_item], "total": 1}

        backfill_items = [
            {"urn": "urn:bbc:radio:episode:ep2", "title": "Backfill Ep"},
        ]
        backfill_response = {"data": backfill_items, "total": 1}

        mock_fetch.side_effect = [category_response, backfill_response]
        result = fetch_drama_programmes(
            category_slugs=["thriller"],
            max_pages=1,
            delay=0,
            backfill_containers=True,
            container_max_pages=5,
        )
        ep2 = [p for p in result if p.pid == "ep2"][0]
        assert "Thriller" in ep2.categories

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_no_backfill_by_default(self, mock_fetch: MagicMock) -> None:
        """Container backfill is disabled by default."""
        item = {
            "urn": "urn:bbc:radio:episode:ep1",
            "title": "Ep",
            "container": {"id": "series1", "title": "Series"},
        }
        mock_fetch.return_value = {"data": [item], "total": 1}
        result = fetch_drama_programmes(
            category_slugs=["drama"], max_pages=1, delay=0
        )
        # Only the category scan call should have been made
        assert mock_fetch.call_count == 1
        assert len(result) == 1
