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

python -m ao3_scraper.cli scrape-tag-range ^
  --tag-url "https://archiveofourown.org/works?commit=Sort+and+Filter&work_search[sort_column]=created_at&work_search[other_tag_names]=&work_search[excluded_tag_names]=&work_search[crossover]=&work_search[complete]=&work_search[words_from]=&work_search[words_to]=&work_search[date_from]=&work_search[date_to]=&work_search[query]=&work_search[language_id]=&tag_id=Hermione+Granger*s*Harry+Potter&page=581" ^
  --start-page 582 ^
  --end-page 500 ^
  --db-path data\ao3.db ^
  --base-delay 3.0 ^
  --jitter 0.8 ^
  --progress-every 10

python -m ao3_scraper.cli scrape-bookmarks-from-db ^
  --db-path data\ao3.db ^
  --max-works 10 ^
  --max-pages-per-work 2 ^
  --base-delay 3.0 ^
  --jitter 0.8 ^
  --progress-every 10
```

## Notes

- Request throttling is enforced between requests as `base_delay + random(0, jitter)`.
- The parser currently handles one works page at a time by design.
- Live requests use Playwright Chromium for fetching.
- You can still parse local files with `--input-html` (no network request).
- `scrape-tag-range` updates only the `page` parameter and preserves the rest of your query string.
- `scrape-tag-range` crawls descending when `start-page > end-page`.
- `scrape-bookmarks-from-db` reads `works` rows and writes `users` + `bookmarks` edges.
