from __future__ import annotations

from ao3_scraper.rate_limit import RequestThrottler

BASE_URL = "https://archiveofourown.org"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)


class AO3FetchError(RuntimeError):
    pass


def fetch_html(
    url: str,
    throttler: RequestThrottler,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_ms: int = 45_000,
) -> str:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise AO3FetchError(
            "Playwright is required for live fetching. Install with "
            "`pip install playwright` and `playwright install chromium`."
        ) from exc

    throttler.wait()
    html: str | None = None
    status_code: int | None = None
    final_url: str | None = None

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            html = page.content()
            final_url = page.url
            status_code = response.status if response is not None else None
            context.close()
            browser.close()
    except PlaywrightTimeoutError as exc:
        raise AO3FetchError(f"Playwright timed out loading {url}") from exc
    except PlaywrightError as exc:
        raise AO3FetchError(f"Playwright failed loading {url}: {exc}") from exc
    finally:
        throttler.mark_request()

    if not html:
        raise AO3FetchError(f"Empty HTML fetched for {url}")
    if status_code is not None and status_code >= 400:
        raise AO3FetchError(f"HTTP {status_code} at {final_url or url}")
    if "Shields are up!" in html:
        raise AO3FetchError(f"AO3 shield page returned for {final_url or url}")

    return html
