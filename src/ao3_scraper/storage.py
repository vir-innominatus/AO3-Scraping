from __future__ import annotations

import csv
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from ao3_scraper.models import WorkRecord


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def init_db(db_path: Path) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS works (
            work_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            work_url TEXT NOT NULL,
            author_name TEXT,
            author_url TEXT,
            fandoms_json TEXT NOT NULL,
            summary TEXT NOT NULL,
            summary_len INTEGER NOT NULL,
            rating TEXT,
            warning_summary TEXT,
            category TEXT,
            completion_status TEXT,
            language TEXT,
            words INTEGER,
            chapters_current INTEGER,
            chapters_total INTEGER,
            comments INTEGER,
            kudos INTEGER,
            bookmarks INTEGER,
            hits INTEGER,
            updated_date TEXT,
            source_tag_url TEXT NOT NULL,
            scraped_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS work_tags (
            work_id INTEGER NOT NULL,
            tag_type TEXT NOT NULL,
            tag_text TEXT NOT NULL,
            PRIMARY KEY (work_id, tag_type, tag_text),
            FOREIGN KEY (work_id) REFERENCES works(work_id) ON DELETE CASCADE
        );
        """
    )
    return conn


def upsert_works(conn: sqlite3.Connection, records: list[WorkRecord], source_tag_url: str) -> None:
    scraped_at = datetime.now(UTC).isoformat()
    with conn:
        for record in records:
            conn.execute(
                """
                INSERT INTO works (
                    work_id, title, work_url, author_name, author_url, fandoms_json, summary, summary_len,
                    rating, warning_summary, category, completion_status, language, words, chapters_current,
                    chapters_total, comments, kudos, bookmarks, hits, updated_date, source_tag_url, scraped_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(work_id) DO UPDATE SET
                    title = excluded.title,
                    work_url = excluded.work_url,
                    author_name = excluded.author_name,
                    author_url = excluded.author_url,
                    fandoms_json = excluded.fandoms_json,
                    summary = excluded.summary,
                    summary_len = excluded.summary_len,
                    rating = excluded.rating,
                    warning_summary = excluded.warning_summary,
                    category = excluded.category,
                    completion_status = excluded.completion_status,
                    language = excluded.language,
                    words = excluded.words,
                    chapters_current = excluded.chapters_current,
                    chapters_total = excluded.chapters_total,
                    comments = excluded.comments,
                    kudos = excluded.kudos,
                    bookmarks = excluded.bookmarks,
                    hits = excluded.hits,
                    updated_date = excluded.updated_date,
                    source_tag_url = excluded.source_tag_url,
                    scraped_at_utc = excluded.scraped_at_utc
                """,
                (
                    record.work_id,
                    record.title,
                    record.work_url,
                    record.author_name,
                    record.author_url,
                    json.dumps(record.fandoms, ensure_ascii=True),
                    record.summary,
                    record.summary_len,
                    record.rating,
                    record.warning_summary,
                    record.category,
                    record.completion_status,
                    record.language,
                    record.words,
                    record.chapters_current,
                    record.chapters_total,
                    record.comments,
                    record.kudos,
                    record.bookmarks,
                    record.hits,
                    record.updated_date,
                    source_tag_url,
                    scraped_at,
                ),
            )
            conn.execute("DELETE FROM work_tags WHERE work_id = ?", (record.work_id,))
            _insert_tags(conn, record.work_id, "warning", record.warnings)
            _insert_tags(conn, record.work_id, "relationship", record.relationships)
            _insert_tags(conn, record.work_id, "character", record.characters)
            _insert_tags(conn, record.work_id, "freeform", record.freeforms)


def _insert_tags(conn: sqlite3.Connection, work_id: int, tag_type: str, tags: list[str]) -> None:
    if not tags:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO work_tags (work_id, tag_type, tag_text) VALUES (?, ?, ?)",
        [(work_id, tag_type, tag) for tag in tags],
    )


def write_works_csv(csv_path: Path, records: list[WorkRecord]) -> None:
    ensure_parent_dir(csv_path)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "work_id",
                "title",
                "work_url",
                "author_name",
                "author_url",
                "fandoms",
                "warnings",
                "relationships",
                "characters",
                "freeforms",
                "summary",
                "summary_len",
                "rating",
                "warning_summary",
                "category",
                "completion_status",
                "language",
                "words",
                "chapters_current",
                "chapters_total",
                "comments",
                "kudos",
                "bookmarks",
                "hits",
                "updated_date",
            ],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "work_id": record.work_id,
                    "title": record.title,
                    "work_url": record.work_url,
                    "author_name": record.author_name or "",
                    "author_url": record.author_url or "",
                    "fandoms": "; ".join(record.fandoms),
                    "warnings": "; ".join(record.warnings),
                    "relationships": "; ".join(record.relationships),
                    "characters": "; ".join(record.characters),
                    "freeforms": "; ".join(record.freeforms),
                    "summary": record.summary,
                    "summary_len": record.summary_len,
                    "rating": record.rating or "",
                    "warning_summary": record.warning_summary or "",
                    "category": record.category or "",
                    "completion_status": record.completion_status or "",
                    "language": record.language or "",
                    "words": record.words,
                    "chapters_current": record.chapters_current,
                    "chapters_total": record.chapters_total,
                    "comments": record.comments,
                    "kudos": record.kudos,
                    "bookmarks": record.bookmarks,
                    "hits": record.hits,
                    "updated_date": record.updated_date or "",
                }
            )
