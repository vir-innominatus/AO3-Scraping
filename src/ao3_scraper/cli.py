from __future__ import annotations

import argparse
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from pathlib import Path

from bs4 import UnicodeDammit

from ao3_scraper.http import AO3FetchError, fetch_html
from ao3_scraper.parser import parse_bookmarks_page, parse_tag_page
from ao3_scraper.rate_limit import DelayPolicy, RequestThrottler
from ao3_scraper.storage import init_db, upsert_bookmarks, upsert_works, write_works_csv

WORK_ID_FROM_URL_RE = re.compile(r"/works/(\d+)")


def _set_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params[key] = value
    return urlunsplit(parts._replace(query=urlencode(params)))


def _bookmarks_url_for_work(work_url: str, work_id: int) -> str:
    split = urlsplit(work_url)
    return urlunsplit((split.scheme, split.netloc, f"/works/{work_id}/bookmarks", "", ""))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ao3-scraper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape = subparsers.add_parser("scrape-tag-page", help="Fetch and parse one AO3 tag works page.")
    scrape.add_argument("--tag-url", required=True, help="AO3 tag works URL.")
    scrape.add_argument("--db-path", default="data/ao3.db", help="SQLite file path.")
    scrape.add_argument("--csv-path", default="data/works.csv", help="CSV output file path.")
    scrape.add_argument("--base-delay", type=float, default=3.0, help="Base delay between requests (seconds).")
    scrape.add_argument("--jitter", type=float, default=0.8, help="Random extra delay upper bound (seconds).")
    scrape.add_argument(
        "--input-html",
        help="Optional local HTML file path. If provided, no network request is made.",
    )

    scrape_range = subparsers.add_parser(
        "scrape-tag-range",
        help="Fetch and parse a page range for an AO3 works listing URL.",
    )
    scrape_range.add_argument("--tag-url", required=True, help="AO3 works listing URL.")
    scrape_range.add_argument("--start-page", type=int, required=True, help="Start page number (inclusive).")
    scrape_range.add_argument("--end-page", type=int, required=True, help="End page number (inclusive).")
    scrape_range.add_argument("--db-path", default="data/ao3.db", help="SQLite file path.")
    scrape_range.add_argument(
        "--csv-path",
        help="Optional CSV output file path for combined parsed records in this run.",
    )
    scrape_range.add_argument("--base-delay", type=float, default=3.0, help="Base delay between requests (seconds).")
    scrape_range.add_argument("--jitter", type=float, default=0.8, help="Random extra delay upper bound (seconds).")
    scrape_range.add_argument(
        "--progress-every",
        type=int,
        default=1,
        help="Print progress every N successfully parsed pages.",
    )
    scrape_range.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop range crawl on first fetch error. Default behavior is continue.",
    )

    bookmarks = subparsers.add_parser(
        "scrape-bookmarks-from-db",
        help="Fetch bookmark users for works already stored in the SQLite DB.",
    )
    bookmarks.add_argument("--db-path", default="data/ao3.db", help="SQLite file path.")
    bookmarks.add_argument("--base-delay", type=float, default=3.0, help="Base delay between requests (seconds).")
    bookmarks.add_argument("--jitter", type=float, default=0.8, help="Random extra delay upper bound (seconds).")
    bookmarks.add_argument(
        "--max-works",
        type=int,
        default=10,
        help="Maximum number of works to crawl in this run.",
    )
    bookmarks.add_argument(
        "--max-pages-per-work",
        type=int,
        default=1,
        help="Maximum bookmark pages to crawl per work.",
    )
    bookmarks.add_argument(
        "--min-bookmarks",
        type=int,
        default=1,
        help="Only crawl works with at least this many bookmark counts in works table.",
    )
    bookmarks.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help="Print progress every N processed works.",
    )

    return parser


def cmd_scrape_tag_page(args: argparse.Namespace) -> int:
    if args.input_html:
        input_path = Path(args.input_html)
        raw_html = input_path.read_bytes()
        html = UnicodeDammit(raw_html).unicode_markup
        if html is None:
            raise ValueError(f"Could not decode HTML file: {input_path}")
        source_url = args.tag_url
    else:
        throttler = RequestThrottler(DelayPolicy(base_seconds=args.base_delay, jitter_seconds=args.jitter))
        try:
            html = fetch_html(args.tag_url, throttler=throttler)
        except AO3FetchError as exc:
            print(f"Fetch failed: {exc}")
            return 1
        source_url = args.tag_url

    records = parse_tag_page(html, source_url=source_url)
    conn = init_db(Path(args.db_path))
    upsert_works(conn, records, source_tag_url=source_url)
    conn.close()
    write_works_csv(Path(args.csv_path), records)

    print(f"Parsed works: {len(records)}")
    print(f"SQLite: {Path(args.db_path)}")
    print(f"CSV: {Path(args.csv_path)}")
    return 0


def cmd_scrape_tag_range(args: argparse.Namespace) -> int:
    if args.start_page <= 0 or args.end_page <= 0:
        print("--start-page and --end-page must be > 0")
        return 2
    if args.progress_every <= 0:
        print("--progress-every must be > 0")
        return 2

    step = 1 if args.end_page >= args.start_page else -1
    pages = list(range(args.start_page, args.end_page + step, step))
    throttler = RequestThrottler(DelayPolicy(base_seconds=args.base_delay, jitter_seconds=args.jitter))
    conn = init_db(Path(args.db_path))
    collected_records = [] if args.csv_path else None

    pages_ok = 0
    pages_failed = 0
    works_upserted = 0

    for page_num in pages:
        page_url = _set_query_param(args.tag_url, "page", str(page_num))
        try:
            html = fetch_html(page_url, throttler=throttler)
        except AO3FetchError as exc:
            pages_failed += 1
            print(f"Fetch failed for page {page_num}: {exc}")
            if args.stop_on_error:
                break
            continue

        records = parse_tag_page(html, source_url=page_url)
        upsert_works(conn, records, source_tag_url=page_url)
        if collected_records is not None:
            collected_records.extend(records)
        pages_ok += 1
        works_upserted += len(records)
        is_last_page = page_num == pages[-1]
        if (pages_ok % args.progress_every) == 0 or is_last_page:
            print(
                f"Progress: parsed_pages={pages_ok}/{len(pages)} "
                f"last_page={page_num} works_on_last_page={len(records)} "
                f"total_upserted_this_run={works_upserted}"
            )

    conn.close()

    if args.csv_path and collected_records is not None:
        write_works_csv(Path(args.csv_path), collected_records)
        print(f"CSV: {Path(args.csv_path)}")

    direction = "ascending" if step == 1 else "descending"
    print(f"Direction: {direction}")
    print(f"Pages attempted: {pages_ok + pages_failed}")
    print(f"Pages parsed: {pages_ok}")
    print(f"Pages failed: {pages_failed}")
    print(f"Works parsed this run: {works_upserted}")
    print(f"SQLite: {Path(args.db_path)}")
    return 0


def cmd_scrape_bookmarks_from_db(args: argparse.Namespace) -> int:
    if args.max_works <= 0:
        print("--max-works must be > 0")
        return 2
    if args.max_pages_per_work <= 0:
        print("--max-pages-per-work must be > 0")
        return 2
    if args.progress_every <= 0:
        print("--progress-every must be > 0")
        return 2

    conn = init_db(Path(args.db_path))
    works = conn.execute(
        """
        SELECT work_id, work_url
        FROM works
        WHERE COALESCE(bookmarks, 0) >= ?
        ORDER BY COALESCE(bookmarks, 0) DESC, work_id DESC
        LIMIT ?
        """,
        (args.min_bookmarks, args.max_works),
    ).fetchall()

    if not works:
        conn.close()
        print("No works found to crawl bookmarks. Run scrape-tag-page first.")
        return 0

    throttler = RequestThrottler(DelayPolicy(base_seconds=args.base_delay, jitter_seconds=args.jitter))
    works_processed = 0
    pages_fetched = 0
    bookmark_rows = 0

    for work_id, work_url in works:
        if not work_url:
            continue
        if WORK_ID_FROM_URL_RE.search(work_url) is None:
            continue

        base_bookmarks_url = _bookmarks_url_for_work(work_url, int(work_id))
        has_next = True
        page_num = 1

        while has_next and page_num <= args.max_pages_per_work:
            url = _set_query_param(base_bookmarks_url, "page", str(page_num))
            try:
                html = fetch_html(url, throttler=throttler)
            except AO3FetchError as exc:
                print(f"Fetch failed for work {work_id} page {page_num}: {exc}")
                break

            records, has_next = parse_bookmarks_page(html, work_id=int(work_id))
            upsert_bookmarks(conn, records, source_bookmarks_url=url)
            pages_fetched += 1
            bookmark_rows += len(records)
            page_num += 1

        works_processed += 1
        if (works_processed % args.progress_every) == 0 or works_processed == len(works):
            print(
                f"Progress: works_processed={works_processed}/{len(works)} "
                f"last_work={work_id} pages_for_last_work={min(page_num - 1, args.max_pages_per_work)} "
                f"bookmarks_collected_so_far={bookmark_rows}"
            )

    conn.close()
    print(f"Works processed: {works_processed}")
    print(f"Bookmark pages fetched: {pages_fetched}")
    print(f"Bookmarks upserted: {bookmark_rows}")
    print(f"SQLite: {Path(args.db_path)}")
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "scrape-tag-page":
        return cmd_scrape_tag_page(args)
    if args.command == "scrape-tag-range":
        return cmd_scrape_tag_range(args)
    if args.command == "scrape-bookmarks-from-db":
        return cmd_scrape_bookmarks_from_db(args)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
