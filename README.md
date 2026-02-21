# AO3 Scraper + Similar Works Recommender

Scrape AO3 works/bookmarks/kudos into SQLite, generate similar-work recommendations from kudos overlap, and export static JSON for a browser-based recommendation app.

Live web app: https://vir-innominatus.github.io/AO3-Scraping/

## Quickstart

### 1. Set up Python and dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
playwright install chromium
```

### 2. (Optional) Capture login state AO3
This is needed to scrape works that are only visible when logged in.

```bash
python -m ao3_scraper.cli capture-login-state ^
  --storage-state data\ao3_storage_state.json
```

### 3. Scrape a range of AO3 listing pages into SQLite

```bash
python -m ao3_scraper.cli scrape-tag-range ^
  --tag-url "https://archiveofourown.org/tags/Hermione%20Granger*s*Harry%20Potter/works" ^
  --start-page 1 ^
  --end-page 500 ^
  --db-path data\ao3.db ^
  --base-delay 5.0 ^
  --jitter 2.0 ^
  --storage-state data\ao3_storage_state.json ^
  --max-429-retries 6 ^
  --retry-cooldown-seconds 180 ^
  --max-retry-cooldown-seconds 1800 ^
  --progress-every 10
```

### 4. Scrape kudos users for works already in your DB

```bash
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
```

### 5. Generate recommendations for a target work
This builds/uses the recommender model and prints top similar works in the terminal.

```bash
python scripts\recommend_similar_works.py ^
  --db-path data\ao3.db ^
  --work-id 1085412 ^
  --top-k 20 ^
  --min-overlap 3 ^
  --min-candidate-kudos 25
```

### 6. Export recommendations JSON for the web app
This writes the static payload used by `docs/index.html`.

```bash
python scripts\export_recommendations_for_web.py ^
  --cache-path data\kudos_recommender_cache.pkl ^
  --output-path docs\data\recommendations.json ^
  --top-k 20 ^
  --min-overlap 3 ^
  --min-candidate-kudos 25 ^
  --min-target-kudos 25
```

## GitHub Pages App

This repo includes a static web app in `docs/` that reads precomputed recommendations from JSON.

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
- Live requests reuse a single Playwright browser/context session (same process/arguments).
- Pass `--storage-state` when crawling to include login-restricted works/bookmarks.
- HTTP 429 is retried with cooldown and exponential backoff (`Retry-After` is used when available).
- `scrape-tag-range` updates only the `page` parameter and preserves the rest of your query string.
- `scrape-tag-range` crawls descending when `start-page < end-page`.
- `scrape-kudos-from-db` opens each work page with `view_adult=true`, repeatedly clicks `#kudos_more_link`, and writes `kudos` edges.
- `scrape-kudos-from-db` also stores parsed guest kudos into `works.guest_kudos` when available.
- `scrape-kudos-from-db` skips works that already have saved kudos by default. Use `--no-skip-already-scraped-kudos` to force re-crawling.
- `scripts/recommend_similar_works.py` builds a sparse work-user kudos model and returns similar works using weighted cosine + overlap shrinkage.
- Use `--title-query "partial title"` instead of `--work-id` to search and pick a target work interactively.
- The recommender writes `data/kudos_recommender_cache.pkl` for faster repeat queries; pass `--rebuild-cache` to refresh.