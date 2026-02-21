from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from bs4 import UnicodeDammit
from ao3_scraper import cli
from ao3_scraper.http import AO3FetchError, AO3RateLimitError
from ao3_scraper.parser import parse_tag_page
from ao3_scraper.rate_limit import DelayPolicy, RequestThrottler
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
        max_429_retries=1,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
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
        max_429_retries=0,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
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
        max_429_retries=0,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
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


def test_cmd_scrape_kudos_from_db_with_mock_fetch(monkeypatch, tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    tag_html = root.joinpath("ao3_tag_sample.html").read_text(encoding="utf-8", errors="replace")
    kudos_html = root.joinpath("ao3_kudos_sample.html").read_text(encoding="utf-8", errors="replace")
    db_path = tmp_path / "out.db"

    conn = init_db(db_path)
    works = parse_tag_page(
        tag_html,
        source_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
    )
    upsert_works(conn, [works[0]], source_tag_url="https://example.com/tag")
    conn.close()

    called_urls: list[str] = []

    def _fake_fetch(url, *_args, **_kwargs):  # noqa: ANN001
        called_urls.append(url)
        return kudos_html

    monkeypatch.setattr(cli, "fetch_html_with_expanded_kudos", _fake_fetch)
    args = argparse.Namespace(
        db_path=str(db_path),
        base_delay=3.0,
        jitter=0.8,
        max_429_retries=0,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
        max_works=1,
        min_kudos=1,
        max_kudos_more_clicks=10,
        progress_every=1,
        skip_already_scraped_kudos=True,
    )

    code = cli.cmd_scrape_kudos_from_db(args)
    assert code == 0

    conn = sqlite3.connect(db_path)
    try:
        kudos_count = conn.execute("SELECT COUNT(*) FROM kudos").fetchone()[0]
        guest_kudos = conn.execute("SELECT guest_kudos FROM works LIMIT 1").fetchone()[0]
    finally:
        conn.close()

    assert kudos_count == 3
    assert guest_kudos == 17
    assert called_urls
    assert "view_adult=true" in called_urls[0]


def test_cmd_scrape_kudos_from_db_skips_already_scraped_by_default(monkeypatch, tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    tag_html = root.joinpath("ao3_tag_sample.html").read_text(encoding="utf-8", errors="replace")
    db_path = tmp_path / "out.db"

    conn = init_db(db_path)
    works = parse_tag_page(
        tag_html,
        source_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
    )
    work = works[0]
    upsert_works(conn, [work], source_tag_url="https://example.com/tag")
    conn.execute(
        """
        INSERT INTO kudos (work_id, username, pseud_url, source_work_url, scraped_at_utc)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            work.work_id,
            "existing_user",
            "https://archiveofourown.org/users/existing_user",
            work.work_url,
            "2026-02-18T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        cli,
        "fetch_html_with_expanded_kudos",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fetch should not be called")),
    )

    args = argparse.Namespace(
        db_path=str(db_path),
        base_delay=3.0,
        jitter=0.8,
        max_429_retries=0,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
        max_works=1,
        min_kudos=1,
        max_kudos_more_clicks=10,
        progress_every=1,
        skip_already_scraped_kudos=True,
    )

    code = cli.cmd_scrape_kudos_from_db(args)
    assert code == 0


def test_cmd_scrape_kudos_from_db_can_disable_skip(monkeypatch, tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    tag_html = root.joinpath("ao3_tag_sample.html").read_text(encoding="utf-8", errors="replace")
    kudos_html = root.joinpath("ao3_kudos_sample.html").read_text(encoding="utf-8", errors="replace")
    db_path = tmp_path / "out.db"

    conn = init_db(db_path)
    works = parse_tag_page(
        tag_html,
        source_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
    )
    work = works[0]
    upsert_works(conn, [work], source_tag_url="https://example.com/tag")
    conn.execute(
        """
        INSERT INTO kudos (work_id, username, pseud_url, source_work_url, scraped_at_utc)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            work.work_id,
            "existing_user",
            "https://archiveofourown.org/users/existing_user",
            work.work_url,
            "2026-02-18T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    calls = {"count": 0}

    def _fake_fetch(*_args, **_kwargs):  # noqa: ANN001
        calls["count"] += 1
        return kudos_html

    monkeypatch.setattr(cli, "fetch_html_with_expanded_kudos", _fake_fetch)

    args = argparse.Namespace(
        db_path=str(db_path),
        base_delay=3.0,
        jitter=0.8,
        max_429_retries=0,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
        max_works=1,
        min_kudos=1,
        max_kudos_more_clicks=10,
        progress_every=1,
        skip_already_scraped_kudos=False,
    )

    code = cli.cmd_scrape_kudos_from_db(args)
    assert code == 0
    assert calls["count"] == 1


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

    def _fake_fetch(url, throttler, **_kwargs):  # noqa: ANN001
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
        max_429_retries=0,
        retry_cooldown_seconds=1.0,
        max_retry_cooldown_seconds=10.0,
        storage_state=None,
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


def test_cmd_capture_login_state_calls_capture(monkeypatch, tmp_path: Path):
    target = tmp_path / "state.json"
    called = {}

    def _fake_capture(storage_state_path, login_url):  # noqa: ANN001
        called["storage_state_path"] = storage_state_path
        called["login_url"] = login_url

    monkeypatch.setattr(cli, "capture_storage_state", _fake_capture)
    args = argparse.Namespace(
        storage_state=str(target),
        login_url="https://archiveofourown.org/users/login",
    )

    code = cli.cmd_capture_login_state(args)
    assert code == 0
    assert called["storage_state_path"] == str(target)


def test_fetch_with_429_retry_retries_then_succeeds(monkeypatch):
    calls = {"count": 0}
    sleeps: list[float] = []

    def _fake_fetch(_url, throttler, storage_state_path):  # noqa: ANN001
        calls["count"] += 1
        if calls["count"] == 1:
            raise AO3RateLimitError(final_url="https://archiveofourown.org/works", retry_after_seconds=1.0)
        return "<html>ok</html>"

    monkeypatch.setattr(cli, "fetch_html", _fake_fetch)
    monkeypatch.setattr(cli.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(cli.random, "uniform", lambda _a, _b: 0.0)

    throttler = RequestThrottler(DelayPolicy(base_seconds=0.0, jitter_seconds=0.0))
    html = cli._fetch_with_429_retry(
        url="https://archiveofourown.org/works",
        throttler=throttler,
        storage_state_path=None,
        max_429_retries=2,
        retry_cooldown_seconds=5.0,
        max_retry_cooldown_seconds=60.0,
    )

    assert html == "<html>ok</html>"
    assert calls["count"] == 2
    assert sleeps == [1.0]
