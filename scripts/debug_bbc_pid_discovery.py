"""Debug BBC Sounds PID discovery in RMS category listings.

Example:
    python scripts/debug_bbc_pid_discovery.py b01c7s27 --category comedy --pages 10
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from radio_cache.bbc_feed_parser import (
    _BBC_PLAYABLE_API,
    _BBC_PROGRAMMES_API,
    _PAGE_LIMIT,
    _fetch_json,
    _parse_programme_item,
)


def _extract_category_titles(item: dict) -> list[str]:
    categories_list = item.get("categories") or []
    if not isinstance(categories_list, list):
        return []

    seen: set[str] = set()
    titles: list[str] = []
    for cat in categories_list:
        if not isinstance(cat, dict):
            continue

        current = cat
        while isinstance(current, dict) and current:
            title_value = current.get("title")
            title = (
                title_value if title_value is not None else current.get("id")
            )
            if isinstance(title, str) and title and title not in seen:
                seen.add(title)
                titles.append(title)
            broader = current.get("broader")
            if not isinstance(broader, dict):
                break
            next_category = broader.get("category")
            if not isinstance(next_category, dict):
                break
            current = next_category
    return titles


def _resolve_display_pid(
    requested_pid: str,
    raw_programme: dict,
    parsed_pid: str | None,
) -> str:
    if parsed_pid:
        return parsed_pid

    urn = raw_programme.get("urn")
    if isinstance(urn, str) and ":" in urn:
        urn_pid = urn.rsplit(":", 1)[-1].strip()
        if urn_pid:
            return urn_pid

    for key in ("pid", "id"):
        value = raw_programme.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return requested_pid


def _print_programme_detail(pid: str) -> bool:
    url = f"{_BBC_PROGRAMMES_API}/{pid}.json"
    payload = _fetch_json(url)
    if not isinstance(payload, dict):
        print(f"ERROR: Failed to fetch programme detail for PID {pid}")
        return False

    raw_programme = payload.get("programme") or payload
    if not isinstance(raw_programme, dict):
        print(f"ERROR: Unexpected programme payload for PID {pid}")
        return False

    parsed = _parse_programme_item(raw_programme)
    resolved_pid = _resolve_display_pid(
        requested_pid=pid,
        raw_programme=raw_programme,
        parsed_pid=parsed.pid if parsed is not None else None,
    )
    print(f"=== Programme detail: {pid} ===")
    print(f"Request URL: {url}")
    print(f"Resolved PID: {resolved_pid}")

    titles = raw_programme.get("titles")
    if isinstance(titles, dict):
        print("Titles:")
        for key in ("primary", "secondary", "tertiary", "entity_title"):
            value = titles.get(key)
            if isinstance(value, str) and value:
                print(f"  {key}: {value}")
    if parsed is not None and parsed.title:
        print(f"Parsed display title: {parsed.title}")

    container = raw_programme.get("container")
    if isinstance(container, dict):
        print("Container:")
        for key in ("id", "type", "title"):
            value = container.get(key)
            if isinstance(value, str) and value:
                print(f"  {key}: {value}")

    for label in ("brand", "master_brand"):
        node = raw_programme.get(label)
        if isinstance(node, dict):
            print(f"{label}:")
            for key in ("id", "type", "title"):
                value = node.get(key)
                if isinstance(value, str) and value:
                    print(f"  {key}: {value}")

    ancestors = raw_programme.get("ancestors")
    if isinstance(ancestors, list) and ancestors:
        print("Ancestors:")
        for idx, ancestor in enumerate(ancestors, start=1):
            if not isinstance(ancestor, dict):
                continue
            aid = ancestor.get("id")
            atitle = ancestor.get("title")
            atype = ancestor.get("type")
            print(
                f"  {idx}. id={aid or '-'} title={atitle or '-'} type={atype or '-'}"
            )

    category_titles = _extract_category_titles(raw_programme)
    if category_titles:
        print(f"Programme categories: {', '.join(category_titles)}")
    elif parsed is not None and parsed.categories:
        print(f"Programme categories: {parsed.categories}")
    else:
        print("Programme categories: <none>")

    if parsed is not None:
        print(
            "Parsed identifiers: "
            f"brand_pid={parsed.brand_pid or '-'} "
            f"series_pid={parsed.series_pid or '-'}"
        )
    return True


def _inspect_category(pid: str, slug: str, max_pages: int) -> bool:
    checked_items = 0
    checked_pages = 0

    print(f"\n=== Category scan: {slug} (max_pages={max_pages}) ===")
    for page in range(max_pages):
        offset = page * _PAGE_LIMIT
        url = (
            f"{_BBC_PLAYABLE_API}?category={slug}"
            f"&sort=date&tleoDistinct=true&offset={offset}&limit={_PAGE_LIMIT}"
        )
        payload = _fetch_json(url)
        if not isinstance(payload, dict):
            print(f"  ERROR: Failed to fetch page {page + 1} for category '{slug}'")
            return False

        items = payload.get("data")
        if not isinstance(items, list) or not items:
            break

        checked_pages += 1
        checked_items += len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            parsed = _parse_programme_item(item)
            if parsed is None:
                continue
            if parsed.pid != pid:
                continue

            print(f"FOUND in category '{slug}' on page {page + 1} (offset={offset})")
            print(f"  pid: {parsed.pid}")
            print(f"  title: {parsed.title}")
            print(f"  brand_pid: {parsed.brand_pid or '-'}")
            print(f"  series_pid: {parsed.series_pid or '-'}")
            print(f"  categories: {parsed.categories or '<none>'}")
            return True

        total = payload.get("total")
        if isinstance(total, int) and (
            offset + len(items) >= total or len(items) < _PAGE_LIMIT
        ):
            break

    print(
        f"NOT FOUND in category '{slug}' "
        f"after checking {checked_pages} page(s), {checked_items} item(s)"
    )
    return True


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pid", help="BBC programme PID to inspect (e.g. b01c7s27)")
    parser.add_argument(
        "--category",
        "-c",
        action="append",
        default=[],
        dest="categories",
        help="Category slug to inspect (repeatable; default: comedy)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=5,
        help="Max pages to fetch per category (default: 5)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.pages < 1:
        print("ERROR: --pages must be >= 1")
        return 1

    pid = args.pid.strip()
    if not pid:
        print("ERROR: PID must not be empty")
        return 1

    categories = [c.strip() for c in args.categories if c.strip()] or ["comedy"]
    success = _print_programme_detail(pid)
    if not success:
        return 1

    for slug in categories:
        category_ok = _inspect_category(pid, slug, args.pages)
        success = category_ok and success

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
