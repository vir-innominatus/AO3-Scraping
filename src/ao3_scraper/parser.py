from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from ao3_scraper.http import BASE_URL
from ao3_scraper.models import WorkRecord

WORK_ID_RE = re.compile(r"work_(\d+)")
CHAPTERS_RE = re.compile(r"^\s*(\d+)\s*/\s*(\?|\d+)\s*$")


def _text_or_none(node: Tag | None) -> str | None:
    if node is None:
        return None
    text = node.get_text(" ", strip=True)
    return text if text else None


def _to_int(text: str | None) -> int | None:
    if not text:
        return None
    cleaned = text.replace(",", "").strip()
    if not cleaned.isdigit():
        return None
    return int(cleaned)


def _parse_required_tags(work_node: Tag) -> tuple[str | None, str | None, str | None, str | None]:
    rating = None
    warning_summary = None
    category = None
    completion_status = None

    for span in work_node.select("ul.required-tags span"):
        classes = span.get("class", [])
        title = span.get("title") or span.get_text(" ", strip=True)
        if any(cls.startswith("rating-") for cls in classes):
            rating = title
        elif any(cls.startswith("warning-") for cls in classes):
            warning_summary = title
        elif any(cls.startswith("category-") for cls in classes):
            category = title
        elif any(cls.startswith("complete-") for cls in classes) or "iswip" in classes:
            completion_status = title
    return rating, warning_summary, category, completion_status


def _parse_tag_lists(work_node: Tag) -> tuple[list[str], list[str], list[str], list[str]]:
    warnings: list[str] = []
    relationships: list[str] = []
    characters: list[str] = []
    freeforms: list[str] = []

    for li in work_node.select("ul.tags.commas > li"):
        classes = li.get("class", [])
        text = li.get_text(" ", strip=True)
        if not text:
            continue
        if "warnings" in classes:
            warnings.append(text)
        elif "relationships" in classes:
            relationships.append(text)
        elif "characters" in classes:
            characters.append(text)
        elif "freeforms" in classes:
            freeforms.append(text)
    return warnings, relationships, characters, freeforms


def _parse_stats(work_node: Tag) -> dict[str, str]:
    stats: dict[str, str] = {}
    for dt in work_node.select("dl.stats dt"):
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        key = dt.get_text(" ", strip=True).rstrip(":").lower()
        value = dd.get_text(" ", strip=True)
        stats[key] = value
    return stats


def _parse_chapters(raw_chapters: str | None) -> tuple[int | None, int | None]:
    if not raw_chapters:
        return None, None
    match = CHAPTERS_RE.match(raw_chapters)
    if not match:
        return None, None
    current = int(match.group(1))
    total_raw = match.group(2)
    total = int(total_raw) if total_raw.isdigit() else None
    return current, total


def parse_tag_page(html: str, source_url: str) -> list[WorkRecord]:
    soup = BeautifulSoup(html, "html.parser")
    work_nodes: Iterable[Tag] = soup.select("li.work.blurb.group")
    records: list[WorkRecord] = []

    for work_node in work_nodes:
        raw_id = work_node.get("id", "")
        match = WORK_ID_RE.search(raw_id)
        if not match:
            continue
        work_id = int(match.group(1))

        title_anchor = work_node.select_one("h4.heading a[href^='/works/']")
        title = _text_or_none(title_anchor)
        href = title_anchor.get("href") if title_anchor else None
        if not title or not href:
            continue
        work_url = urljoin(BASE_URL, href)

        author_anchor = work_node.select_one("a[rel='author']")
        author_name = _text_or_none(author_anchor)
        author_url = urljoin(BASE_URL, author_anchor["href"]) if author_anchor and author_anchor.get("href") else None

        fandoms = [a.get_text(" ", strip=True) for a in work_node.select("h5.fandoms a.tag")]
        warnings, relationships, characters, freeforms = _parse_tag_lists(work_node)
        rating, warning_summary, category, completion_status = _parse_required_tags(work_node)

        summary_node = work_node.select_one("blockquote.userstuff.summary")
        summary = summary_node.get_text(" ", strip=True) if summary_node else ""
        summary_len = len(summary)

        updated_date = _text_or_none(work_node.select_one("p.datetime"))
        stats = _parse_stats(work_node)
        chapters_current, chapters_total = _parse_chapters(stats.get("chapters"))

        record = WorkRecord(
            work_id=work_id,
            title=title,
            work_url=work_url,
            author_name=author_name,
            author_url=author_url,
            fandoms=fandoms,
            warnings=warnings,
            relationships=relationships,
            characters=characters,
            freeforms=freeforms,
            summary=summary,
            summary_len=summary_len,
            rating=rating,
            warning_summary=warning_summary,
            category=category,
            completion_status=completion_status,
            language=stats.get("language"),
            words=_to_int(stats.get("words")),
            chapters_current=chapters_current,
            chapters_total=chapters_total,
            comments=_to_int(stats.get("comments")),
            kudos=_to_int(stats.get("kudos")),
            bookmarks=_to_int(stats.get("bookmarks")),
            hits=_to_int(stats.get("hits")),
            updated_date=updated_date,
        )
        records.append(record)

    return records
