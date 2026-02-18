from __future__ import annotations

import csv
import json
from copy import deepcopy
from pathlib import Path

from ao3_scraper.parser import parse_bookmarks_page
from ao3_scraper.storage import init_db, upsert_works, write_works_csv
from ao3_scraper.storage import upsert_bookmarks


def test_upsert_works_writes_rows_and_tags(parsed_records, tmp_path: Path):
    db_path = tmp_path / "ao3_test.db"
    conn = init_db(db_path)

    records = parsed_records[:2]
    upsert_works(conn, records, source_tag_url="https://example.com/tag")

    work_count = conn.execute("SELECT COUNT(*) FROM works").fetchone()[0]
    tag_count = conn.execute("SELECT COUNT(*) FROM work_tags").fetchone()[0]
    fandoms_json = conn.execute("SELECT fandoms_json FROM works WHERE work_id = ?", (records[0].work_id,)).fetchone()[0]
    conn.close()

    assert work_count == 2
    assert tag_count == (
        len(records[0].warnings)
        + len(records[0].relationships)
        + len(records[0].characters)
        + len(records[0].freeforms)
        + len(records[1].warnings)
        + len(records[1].relationships)
        + len(records[1].characters)
        + len(records[1].freeforms)
    )
    assert json.loads(fandoms_json) == records[0].fandoms


def test_upsert_works_replaces_existing_tag_set(parsed_records, tmp_path: Path):
    db_path = tmp_path / "ao3_test.db"
    conn = init_db(db_path)

    original = deepcopy(parsed_records[0])
    modified = deepcopy(parsed_records[0])
    modified.warnings = ["Custom Warning"]
    modified.relationships = []
    modified.characters = []
    modified.freeforms = []

    upsert_works(conn, [original], source_tag_url="https://example.com/tag")
    upsert_works(conn, [modified], source_tag_url="https://example.com/tag")

    rows = conn.execute(
        "SELECT tag_type, tag_text FROM work_tags WHERE work_id = ? ORDER BY tag_type, tag_text",
        (original.work_id,),
    ).fetchall()
    conn.close()

    assert rows == [("warning", "Custom Warning")]


def test_write_works_csv_creates_expected_rows(parsed_records, tmp_path: Path):
    csv_path = tmp_path / "works.csv"
    records = parsed_records[:3]
    write_works_csv(csv_path, records)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert len(rows) == 3
    assert rows[0]["work_id"] == str(records[0].work_id)
    assert rows[0]["title"] == records[0].title


def test_upsert_bookmarks_writes_users_and_edges(parsed_records, sample_bookmarks_html, tmp_path: Path):
    db_path = tmp_path / "ao3_test.db"
    conn = init_db(db_path)
    work = parsed_records[0]
    upsert_works(conn, [work], source_tag_url="https://example.com/tag")
    bookmark_records, _ = parse_bookmarks_page(sample_bookmarks_html, work_id=work.work_id)

    upsert_bookmarks(conn, bookmark_records, source_bookmarks_url="https://example.com/bookmarks?page=1")

    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    bookmark_count = conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
    sample = conn.execute(
        "SELECT user_id, bookmarked_date FROM bookmarks WHERE work_id = ? ORDER BY user_id LIMIT 1",
        (work.work_id,),
    ).fetchone()
    conn.close()

    assert user_count == 20
    assert bookmark_count == 20
    assert sample == (336061, "06 Feb 2026")
