from __future__ import annotations

import argparse
import random
import re
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from pathlib import Path

from bs4 import UnicodeDammit

from ao3_scraper.http import (
    AO3FetchError,
    AO3RateLimitError,
    capture_storage_state,
    fetch_html,
    fetch_html_with_expanded_kudos,
)
from ao3_scraper.parser import parse_bookmarks_page, parse_guest_kudos_count, parse_kudos_page, parse_tag_page
from ao3_scraper.rate_limit import DelayPolicy, RequestThrottler
from ao3_scraper.storage import (
    init_db,
    update_work_guest_kudos,
    upsert_bookmarks,
    upsert_kudos,
    upsert_works,
    write_works_csv,
)

WORK_ID_FROM_URL_RE = re.compile(r"/works/(\d+)")


def _set_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params[key] = value
    return urlunsplit(parts._replace(query=urlencode(params)))


def _bookmarks_url_for_work(work_url: str, work_id: int) -> str:
    split = urlsplit(work_url)
    return urlunsplit((split.scheme, split.netloc, f"/works/{work_id}/bookmarks", "", ""))


def _fetch_with_429_retry(
    *,
    url: str,
    throttler: RequestThrottler,
    storage_state_path: str | None,
    max_429_retries: int,
    retry_cooldown_seconds: float,
    max_retry_cooldown_seconds: float,
    expand_kudos: bool = False,
    max_kudos_more_clicks: int = 250,
) -> str:
    attempt = 0
    while True:
        try:
            if expand_kudos:
                return fetch_html_with_expanded_kudos(
                    url,
                    throttler=throttler,
                    storage_state_path=storage_state_path,
                    max_kudos_more_clicks=max_kudos_more_clicks,
                )
            return fetch_html(url, throttler=throttler, storage_state_path=storage_state_path)
        except AO3RateLimitError as exc:
            if attempt >= max_429_retries:
                raise
            if exc.retry_after_seconds is not None:
                base_wait = exc.retry_after_seconds
            else:
                base_wait = min(retry_cooldown_seconds * (2**attempt), max_retry_cooldown_seconds)
            jitter = random.uniform(0.0, min(60.0, max(1.0, base_wait * 0.1)))
            wait_s = base_wait + jitter
            print(
                f"Rate limited (429). Waiting {wait_s:.1f}s before retry "
                f"{attempt + 1}/{max_429_retries} for {url}"
            )
            time.sleep(wait_s)
            attempt += 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ao3-scraper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape = subparsers.add_parser("scrape-tag-page", help="Fetch and parse one AO3 tag works page.")
    scrape.add_argument("--tag-url", required=True, help="AO3 tag works URL.")
    scrape.add_argument("--db-path", default="data/ao3.db", help="SQLite file path.")
    scrape.add_argument("--csv-path", default="data/works.csv", help="CSV output file path.")
    scrape.add_argument("--base-delay", type=float, default=3.0, help="Base delay between requests (seconds).")
    scrape.add_argument("--jitter", type=float, default=0.8, help="Random extra delay upper bound (seconds).")
    scrape.add_argument("--max-429-retries", type=int, default=6, help="Max retries for HTTP 429 responses.")
    scrape.add_argument(
        "--retry-cooldown-seconds",
        type=float,
        default=300.0,
        help="Initial cooldown when HTTP 429 is hit and Retry-After is absent.",
    )
    scrape.add_argument(
        "--max-retry-cooldown-seconds",
        type=float,
        default=1800.0,
        help="Upper bound for exponential 429 cooldown.",
    )
    scrape.add_argument(
        "--storage-state",
        help="Optional Playwright storage state JSON path for logged-in AO3 sessions.",
    )
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
    scrape_range.add_argument("--max-429-retries", type=int, default=6, help="Max retries for HTTP 429 responses.")
    scrape_range.add_argument(
        "--retry-cooldown-seconds",
        type=float,
        default=300.0,
        help="Initial cooldown when HTTP 429 is hit and Retry-After is absent.",
    )
    scrape_range.add_argument(
        "--max-retry-cooldown-seconds",
        type=float,
        default=1800.0,
        help="Upper bound for exponential 429 cooldown.",
    )
    scrape_range.add_argument(
        "--storage-state",
        help="Optional Playwright storage state JSON path for logged-in AO3 sessions.",
    )
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
    bookmarks.add_argument("--max-429-retries", type=int, default=6, help="Max retries for HTTP 429 responses.")
    bookmarks.add_argument(
        "--retry-cooldown-seconds",
        type=float,
        default=300.0,
        help="Initial cooldown when HTTP 429 is hit and Retry-After is absent.",
    )
    bookmarks.add_argument(
        "--max-retry-cooldown-seconds",
        type=float,
        default=1800.0,
        help="Upper bound for exponential 429 cooldown.",
    )
    bookmarks.add_argument(
        "--storage-state",
        help="Optional Playwright storage state JSON path for logged-in AO3 sessions.",
    )
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

    kudos = subparsers.add_parser(
        "scrape-kudos-from-db",
        help="Fetch kudos users for works already stored in the SQLite DB.",
    )
    kudos.add_argument("--db-path", default="data/ao3.db", help="SQLite file path.")
    kudos.add_argument("--base-delay", type=float, default=3.0, help="Base delay between requests (seconds).")
    kudos.add_argument("--jitter", type=float, default=0.8, help="Random extra delay upper bound (seconds).")
    kudos.add_argument("--max-429-retries", type=int, default=6, help="Max retries for HTTP 429 responses.")
    kudos.add_argument(
        "--retry-cooldown-seconds",
        type=float,
        default=300.0,
        help="Initial cooldown when HTTP 429 is hit and Retry-After is absent.",
    )
    kudos.add_argument(
        "--max-retry-cooldown-seconds",
        type=float,
        default=1800.0,
        help="Upper bound for exponential 429 cooldown.",
    )
    kudos.add_argument(
        "--storage-state",
        help="Optional Playwright storage state JSON path for logged-in AO3 sessions.",
    )
    kudos.add_argument(
        "--max-works",
        type=int,
        default=10,
        help="Maximum number of works to crawl in this run.",
    )
    kudos.add_argument(
        "--min-kudos",
        type=int,
        default=1,
        help="Only crawl works with at least this many kudos counts in works table.",
    )
    kudos.add_argument(
        "--max-kudos-more-clicks",
        type=int,
        default=250,
        help="Safety cap for repeated clicks on the kudos 'more users' link.",
    )
    kudos.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help="Print progress every N processed works.",
    )
    kudos.add_argument(
        "--skip-already-scraped-kudos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip works that already have stored kudos data. Default: true.",
    )

    capture_login = subparsers.add_parser(
        "capture-login-state",
        help="Open a headed browser to log in and save Playwright storage state JSON.",
    )
    capture_login.add_argument(
        "--storage-state",
        default="data/ao3_storage_state.json",
        help="Path to write storage state JSON.",
    )
    capture_login.add_argument(
        "--login-url",
        default="https://archiveofourown.org/users/login",
        help="AO3 login URL to open in browser.",
    )

    return parser


def cmd_scrape_tag_page(args: argparse.Namespace) -> int:
    if args.max_429_retries < 0:
        print("--max-429-retries must be >= 0")
        return 2
    if args.retry_cooldown_seconds < 0 or args.max_retry_cooldown_seconds < 0:
        print("--retry-cooldown-seconds and --max-retry-cooldown-seconds must be >= 0")
        return 2

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
            html = _fetch_with_429_retry(
                url=args.tag_url,
                throttler=throttler,
                storage_state_path=args.storage_state,
                max_429_retries=args.max_429_retries,
                retry_cooldown_seconds=args.retry_cooldown_seconds,
                max_retry_cooldown_seconds=args.max_retry_cooldown_seconds,
            )
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
    if args.max_429_retries < 0:
        print("--max-429-retries must be >= 0")
        return 2
    if args.retry_cooldown_seconds < 0 or args.max_retry_cooldown_seconds < 0:
        print("--retry-cooldown-seconds and --max-retry-cooldown-seconds must be >= 0")
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
            html = _fetch_with_429_retry(
                url=page_url,
                throttler=throttler,
                storage_state_path=args.storage_state,
                max_429_retries=args.max_429_retries,
                retry_cooldown_seconds=args.retry_cooldown_seconds,
                max_retry_cooldown_seconds=args.max_retry_cooldown_seconds,
            )
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
    if args.max_429_retries < 0:
        print("--max-429-retries must be >= 0")
        return 2
    if args.retry_cooldown_seconds < 0 or args.max_retry_cooldown_seconds < 0:
        print("--retry-cooldown-seconds and --max-retry-cooldown-seconds must be >= 0")
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
                html = _fetch_with_429_retry(
                    url=url,
                    throttler=throttler,
                    storage_state_path=args.storage_state,
                    max_429_retries=args.max_429_retries,
                    retry_cooldown_seconds=args.retry_cooldown_seconds,
                    max_retry_cooldown_seconds=args.max_retry_cooldown_seconds,
                )
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


def cmd_scrape_kudos_from_db(args: argparse.Namespace) -> int:
    if args.max_works <= 0:
        print("--max-works must be > 0")
        return 2
    if args.min_kudos < 0:
        print("--min-kudos must be >= 0")
        return 2
    if args.max_kudos_more_clicks <= 0:
        print("--max-kudos-more-clicks must be > 0")
        return 2
    if args.progress_every <= 0:
        print("--progress-every must be > 0")
        return 2
    if args.max_429_retries < 0:
        print("--max-429-retries must be >= 0")
        return 2
    if args.retry_cooldown_seconds < 0 or args.max_retry_cooldown_seconds < 0:
        print("--retry-cooldown-seconds and --max-retry-cooldown-seconds must be >= 0")
        return 2

    conn = init_db(Path(args.db_path))
    if args.skip_already_scraped_kudos:
        works = conn.execute(
            """
            SELECT w.work_id, w.work_url
            FROM works w
            WHERE COALESCE(w.kudos, 0) >= ?
              AND w.guest_kudos IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM kudos k WHERE k.work_id = w.work_id
              )
            ORDER BY COALESCE(w.kudos, 0) DESC, w.work_id DESC
            LIMIT ?
            """,
            (args.min_kudos, args.max_works),
        ).fetchall()
    else:
        works = conn.execute(
            """
            SELECT w.work_id, w.work_url
            FROM works w
            WHERE COALESCE(w.kudos, 0) >= ?
            ORDER BY COALESCE(w.kudos, 0) DESC, w.work_id DESC
            LIMIT ?
            """,
            (args.min_kudos, args.max_works),
        ).fetchall()

    if not works:
        conn.close()
        print("No works found to crawl kudos. Run scrape-tag-page first.")
        return 0

    throttler = RequestThrottler(DelayPolicy(base_seconds=args.base_delay, jitter_seconds=args.jitter))
    works_processed = 0
    works_failed = 0
    kudos_rows = 0

    for work_id, work_url in works:
        if not work_url:
            continue
        if WORK_ID_FROM_URL_RE.search(work_url) is None:
            continue
        work_url_with_adult = _set_query_param(work_url, "view_adult", "true")

        try:
            html = _fetch_with_429_retry(
                url=work_url_with_adult,
                throttler=throttler,
                storage_state_path=args.storage_state,
                max_429_retries=args.max_429_retries,
                retry_cooldown_seconds=args.retry_cooldown_seconds,
                max_retry_cooldown_seconds=args.max_retry_cooldown_seconds,
                expand_kudos=True,
                max_kudos_more_clicks=args.max_kudos_more_clicks,
            )
        except AO3FetchError as exc:
            works_failed += 1
            works_processed += 1
            print(f"Fetch failed for work {work_id}: {exc}")
            continue

        records = parse_kudos_page(html, work_id=int(work_id))
        guest_kudos = parse_guest_kudos_count(html)
        upsert_kudos(conn, records, source_work_url=work_url_with_adult)
        update_work_guest_kudos(conn, work_id=int(work_id), guest_kudos=guest_kudos)
        works_processed += 1
        kudos_rows += len(records)

        if (works_processed % args.progress_every) == 0 or works_processed == len(works):
            print(
                f"Progress: works_processed={works_processed}/{len(works)} "
                f"last_work={work_id} kudos_collected_so_far={kudos_rows} "
                f"works_failed={works_failed}"
            )

    conn.close()
    print(f"Works processed: {works_processed}")
    print(f"Works failed: {works_failed}")
    print(f"Kudos users upserted: {kudos_rows}")
    print(f"SQLite: {Path(args.db_path)}")
    return 0


def cmd_capture_login_state(args: argparse.Namespace) -> int:
    try:
        capture_storage_state(storage_state_path=args.storage_state, login_url=args.login_url)
    except AO3FetchError as exc:
        print(f"Capture failed: {exc}")
        return 1
    print(f"Saved storage state: {Path(args.storage_state)}")
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
    if args.command == "scrape-kudos-from-db":
        return cmd_scrape_kudos_from_db(args)
    if args.command == "capture-login-state":
        return cmd_capture_login_state(args)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
