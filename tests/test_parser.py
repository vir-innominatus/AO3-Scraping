from __future__ import annotations

from ao3_scraper.parser import parse_bookmarks_page, parse_guest_kudos_count, parse_kudos_page


def test_parse_tag_page_extracts_expected_count(parsed_records):
    assert len(parsed_records) == 20


def test_parse_tag_page_extracts_core_fields(parsed_records):
    target = next(record for record in parsed_records if record.work_id == 68104166)

    assert target.title == "Blood of the Veil"
    assert target.author_name == "AMagicWord"
    assert target.work_url == "https://archiveofourown.org/works/68104166"
    assert target.rating == "Explicit"
    assert target.completion_status == "Work in Progress"
    assert target.words == 40507
    assert target.comments == 77
    assert target.kudos == 828
    assert target.bookmarks == 489
    assert target.hits == 64206
    assert target.chapters_current == 8
    assert target.chapters_total is None
    assert target.summary_len == len(target.summary)
    assert "Harry Potter - J. K. Rowling" in target.fandoms
    assert "Hermione Granger/Harry Potter" in target.relationships


def test_parse_tag_page_handles_chapter_totals(parsed_records):
    target = next(record for record in parsed_records if record.work_id == 56114314)
    assert target.chapters_current == 197
    assert target.chapters_total == 199


def test_parse_bookmarks_page_extracts_users(sample_bookmarks_html):
    records, has_next = parse_bookmarks_page(sample_bookmarks_html, work_id=68104166)
    assert len(records) == 20
    assert has_next is True

    first = records[0]
    assert first.work_id == 68104166
    assert first.user_id == 13783858
    assert first.username == "Mikel2121"
    assert first.pseud_url == "https://archiveofourown.org/users/Mikel2121/pseuds/Mikel2121"


def test_parse_kudos_page_extracts_users(sample_kudos_html):
    records = parse_kudos_page(sample_kudos_html, work_id=68104166)
    assert len(records) == 3

    first = records[0]
    assert first.work_id == 68104166
    assert first.username == "Mikel2121"
    assert first.pseud_url == "https://archiveofourown.org/users/Mikel2121/pseuds/Mikel2121"


def test_parse_guest_kudos_count(sample_kudos_html):
    assert parse_guest_kudos_count(sample_kudos_html) == 17
