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
python -m ao3_scraper.cli capture-login-state ^
  --storage-state data\ao3_storage_state.json

python -m ao3_scraper.cli scrape-tag-page ^
  --tag-url "https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works" ^
  --db-path data\ao3.db ^
  --csv-path data\works.csv ^
  --base-delay 3.0 ^
  --jitter 0.8 ^
  --storage-state data\ao3_storage_state.json

python -m ao3_scraper.cli scrape-tag-range ^
  --tag-url "https://archiveofourown.org/works?commit=Sort+and+Filter&work_search[sort_column]=created_at&work_search[other_tag_names]=&work_search[excluded_tag_names]=&work_search[crossover]=&work_search[complete]=&work_search[words_from]=&work_search[words_to]=&work_search[date_from]=&work_search[date_to]=&work_search[query]=&work_search[language_id]=&tag_id=Hermione+Granger*s*Harry+Potter&page=581" ^
  --start-page 582 ^
  --end-page 500 ^
  --db-path data\ao3.db ^
  --base-delay 3.0 ^
  --jitter 0.8 ^
  --storage-state data\ao3_storage_state.json ^
  --max-429-retries 6 ^
  --retry-cooldown-seconds 300 ^
  --max-retry-cooldown-seconds 1800 ^
  --progress-every 10

python -m ao3_scraper.cli scrape-bookmarks-from-db ^
  --db-path data\ao3.db ^
  --max-works 10 ^
  --max-pages-per-work 2 ^
  --base-delay 3.0 ^
  --jitter 0.8 ^
  --storage-state data\ao3_storage_state.json ^
  --max-429-retries 6 ^
  --retry-cooldown-seconds 300 ^
  --max-retry-cooldown-seconds 1800 ^
  --progress-every 10

python -m ao3_scraper.cli scrape-kudos-from-db ^
  --db-path data\ao3.db ^
  --max-works 10 ^
  --min-kudos 1 ^
  --max-kudos-more-clicks 250 ^
  --base-delay 3.0 ^
  --jitter 0.8 ^
  --storage-state data\ao3_storage_state.json ^
  --max-429-retries 6 ^
  --retry-cooldown-seconds 300 ^
  --max-retry-cooldown-seconds 1800 ^
  --progress-every 10

python scripts\recommend_similar_works.py ^
  --db-path data\ao3.db ^
  --work-id 1085412 ^
  --top-k 20 ^
  --min-overlap 3 ^
  --min-candidate-kudos 25

python scripts\export_recommendations_for_web.py ^
  --cache-path data\kudos_recommender_cache.pkl ^
  --output-path docs\data\recommendations.json ^
  --top-k 20 ^
  --min-overlap 3 ^
  --min-candidate-kudos 25 ^
  --min-target-kudos 25
```

## GitHub Pages App

This repo now includes a static web app in `docs/` that reads precomputed recommendations from JSON.

- `scripts/export_recommendations_for_web.py` uses the recommender cache file (`data/kudos_recommender_cache.pkl`) so it does not need to process `ao3.db` for web export.
- The generated payload is written to `docs/data/recommendations.json`.
- The page entry point is `docs/index.html`.

Local preview:

```bash
python -m http.server --directory docs 8000
```

Then open `http://localhost:8000`.

GitHub Pages setup:

1. Push `docs/` to your repo.
2. In GitHub repo settings, set Pages source to `Deploy from a branch`.
3. Choose your branch and folder `/docs`.

## Notes

- Request throttling is enforced between requests as `base_delay + random(0, jitter)`.
- The parser currently handles one works page at a time by design.
- Live requests use Playwright Chromium for fetching.
- Live requests reuse a single Playwright browser/context session (same process/arguments).
- `capture-login-state` opens a browser so you can log in and save a reusable Playwright session file.
- Pass `--storage-state` when crawling to include login-restricted works/bookmarks.
- HTTP 429 is retried with cooldown and exponential backoff (`Retry-After` is used when available).
- You can still parse local files with `--input-html` (no network request).
- `scrape-tag-range` updates only the `page` parameter and preserves the rest of your query string.
- `scrape-tag-range` crawls descending when `start-page > end-page`.
- `scrape-bookmarks-from-db` reads `works` rows and writes `users` + `bookmarks` edges.
- `scrape-kudos-from-db` opens each work page with `view_adult=true`, repeatedly clicks `#kudos_more_link`, and writes `kudos` edges.
- `scrape-kudos-from-db` also stores parsed guest kudos into `works.guest_kudos` when available.
- `scrape-kudos-from-db` skips works that already have saved kudos by default. Use `--no-skip-already-scraped-kudos` to force re-crawling.
- `scripts/recommend_similar_works.py` builds a sparse work-user kudos model and returns similar works using weighted cosine + overlap shrinkage.
- Use `--title-query "partial title"` instead of `--work-id` to search and pick a target work interactively.
- The recommender writes `data/kudos_recommender_cache.pkl` for faster repeat queries; pass `--rebuild-cache` to refresh.
- `scripts/export_recommendations_for_web.py` builds static JSON for the GitHub Pages app from the recommender cache.
