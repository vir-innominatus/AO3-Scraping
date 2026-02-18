from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkRecord:
    work_id: int
    title: str
    work_url: str
    author_name: str | None
    author_url: str | None
    fandoms: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    relationships: list[str] = field(default_factory=list)
    characters: list[str] = field(default_factory=list)
    freeforms: list[str] = field(default_factory=list)
    summary: str = ""
    summary_len: int = 0
    rating: str | None = None
    warning_summary: str | None = None
    category: str | None = None
    completion_status: str | None = None
    language: str | None = None
    words: int | None = None
    chapters_current: int | None = None
    chapters_total: int | None = None
    comments: int | None = None
    kudos: int | None = None
    bookmarks: int | None = None
    hits: int | None = None
    updated_date: str | None = None
