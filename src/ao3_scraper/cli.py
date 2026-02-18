from __future__ import annotations

import argparse
from pathlib import Path

from bs4 import UnicodeDammit

from ao3_scraper.http import AO3BlockedError, fetch_html
from ao3_scraper.parser import parse_tag_page
from ao3_scraper.rate_limit import DelayPolicy, RequestThrottler
from ao3_scraper.storage import init_db, upsert_works, write_works_csv


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
        except AO3BlockedError as exc:
            print(f"Fetch blocked: {exc}")
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


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "scrape-tag-page":
        return cmd_scrape_tag_page(args)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
