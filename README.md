# AO3 Scraper (Step 1-2)

This scaffold fetches and parses one AO3 tag works page, then writes:

- A SQLite database (`works` + `work_tags` tables)
- A flat CSV for works

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
playwright install chromium
python -m ao3_scraper.cli scrape-tag-page ^
  --tag-url "https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works" ^
  --db-path data\ao3.db ^
  --csv-path data\works.csv ^
  --base-delay 3.0 ^
  --jitter 0.8
```

## Notes

- Request throttling is enforced between requests as `base_delay + random(0, jitter)`.
- The parser currently handles one works page at a time by design.
- Live requests use Playwright Chromium for fetching.
- You can still parse local files with `--input-html` (no network request).
