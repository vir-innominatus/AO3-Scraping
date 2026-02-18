from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from bs4 import UnicodeDammit
from ao3_scraper import cli
from ao3_scraper.http import AO3FetchError
from ao3_scraper.parser import parse_tag_page
from ao3_scraper.storage import init_db, upsert_works


def test_cmd_scrape_tag_page_with_input_html_writes_outputs(tmp_path: Path):
    fixture_path = Path(__file__).resolve().parents[1] / "ao3_tag_sample.html"
    db_path = tmp_path / "out.db"
    csv_path = tmp_path / "out.csv"
    args = argparse.Namespace(
        tag_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
        db_path=str(db_path),
        csv_path=str(csv_path),
        base_delay=3.0,
        jitter=0.8,
        input_html=str(fixture_path),
    )

    code = cli.cmd_scrape_tag_page(args)

    assert code == 0
    assert db_path.exists()
    assert csv_path.exists()

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM works").fetchone()[0]
    finally:
        conn.close()

    assert count == 20


def test_cmd_scrape_tag_page_returns_1_on_fetch_error(monkeypatch, tmp_path: Path):
    def _raise(*_args, **_kwargs):
        raise AO3FetchError("blocked")

    monkeypatch.setattr(cli, "fetch_html", _raise)
    args = argparse.Namespace(
        tag_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
        db_path=str(tmp_path / "out.db"),
        csv_path=str(tmp_path / "out.csv"),
        base_delay=3.0,
        jitter=0.8,
        input_html=None,
    )

    code = cli.cmd_scrape_tag_page(args)
    assert code == 1


def test_cmd_scrape_bookmarks_from_db_with_mock_fetch(monkeypatch, tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    tag_html = root.joinpath("ao3_tag_sample.html").read_text(encoding="utf-8", errors="replace")
    bookmark_html = root.joinpath("ao3_bookmarks_sample.html").read_text(encoding="utf-8", errors="replace")
    db_path = tmp_path / "out.db"

    conn = init_db(db_path)
    works = parse_tag_page(
        tag_html,
        source_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
    )
    upsert_works(conn, [works[0]], source_tag_url="https://example.com/tag")
    conn.close()

    monkeypatch.setattr(cli, "fetch_html", lambda *_args, **_kwargs: bookmark_html)
    args = argparse.Namespace(
        db_path=str(db_path),
        base_delay=3.0,
        jitter=0.8,
        max_works=1,
        max_pages_per_work=1,
        min_bookmarks=1,
        progress_every=1,
    )

    code = cli.cmd_scrape_bookmarks_from_db(args)
    assert code == 0

    conn = sqlite3.connect(db_path)
    try:
        user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        bookmark_count = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
    finally:
        conn.close()

    assert user_count == 20
    assert bookmark_count == 20


def test_set_query_param_preserves_search_fields():
    url = (
        "https://archiveofourown.org/works?"
        "commit=Sort+and+Filter&work_search[sort_column]=created_at&"
        "work_search[other_tag_names]=&work_search[excluded_tag_names]=&"
        "work_search[crossover]=&work_search[complete]=&work_search[words_from]=&"
        "work_search[words_to]=&work_search[date_from]=&work_search[date_to]=&"
        "work_search[query]=&work_search[language_id]=&"
        "tag_id=Hermione+Granger*s*Harry+Potter&page=581"
    )
    new_url = cli._set_query_param(url, "page", "500")
    parsed = parse_qs(urlsplit(new_url).query, keep_blank_values=True)
    assert parsed["page"] == ["500"]
    assert parsed["tag_id"] == ["Hermione Granger*s*Harry Potter"]
    assert parsed["work_search[sort_column]"] == ["created_at"]


def test_cmd_scrape_tag_range_descending_pages(monkeypatch, tmp_path: Path):
    fixture_path = Path(__file__).resolve().parents[1] / "ao3_tag_sample.html"
    html = UnicodeDammit(fixture_path.read_bytes()).unicode_markup
    assert html is not None

    visited_pages: list[int] = []

    def _fake_fetch(url, throttler):  # noqa: ANN001
        parsed = parse_qs(urlsplit(url).query, keep_blank_values=True)
        visited_pages.append(int(parsed["page"][0]))
        return html

    monkeypatch.setattr(cli, "fetch_html", _fake_fetch)
    db_path = tmp_path / "range.db"
    args = argparse.Namespace(
        tag_url="https://archiveofourown.org/works?work_search[sort_column]=created_at&tag_id=Hermione+Granger*s*Harry+Potter",
        start_page=3,
        end_page=1,
        db_path=str(db_path),
        csv_path=None,
        base_delay=3.0,
        jitter=0.8,
        progress_every=1,
        stop_on_error=False,
    )

    code = cli.cmd_scrape_tag_range(args)
    assert code == 0
    assert visited_pages == [3, 2, 1]

    conn = sqlite3.connect(db_path)
    try:
        works_count = conn.execute("SELECT COUNT(*) FROM works").fetchone()[0]
    finally:
        conn.close()
    assert works_count == 20
