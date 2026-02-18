from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import UnicodeDammit

from ao3_scraper.parser import parse_tag_page


@pytest.fixture
def sample_tag_html() -> str:
    input_path = Path(__file__).resolve().parents[1] / "ao3_tag_sample.html"
    raw_html = input_path.read_bytes()
    html = UnicodeDammit(raw_html).unicode_markup
    if html is None:
        raise ValueError(f"Could not decode HTML fixture: {input_path}")
    return html


@pytest.fixture
def parsed_records(sample_tag_html):
    return parse_tag_page(
        sample_tag_html,
        source_url="https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works",
    )


@pytest.fixture
def sample_bookmarks_html() -> str:
    input_path = Path(__file__).resolve().parents[1] / "ao3_bookmarks_sample.html"
    raw_html = input_path.read_bytes()
    html = UnicodeDammit(raw_html).unicode_markup
    if html is None:
        raise ValueError(f"Could not decode HTML fixture: {input_path}")
    return html
