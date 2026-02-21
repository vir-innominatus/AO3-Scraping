from __future__ import annotations

import atexit
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from ao3_scraper.rate_limit import RequestThrottler

BASE_URL = "https://archiveofourown.org"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)


class AO3FetchError(RuntimeError):
    pass


class AO3HTTPStatusError(AO3FetchError):
    def __init__(self, status_code: int, final_url: str, message: str | None = None):
        self.status_code = status_code
        self.final_url = final_url
        super().__init__(message or f"HTTP {status_code} at {final_url}")


class AO3RateLimitError(AO3HTTPStatusError):
    def __init__(self, final_url: str, retry_after_seconds: float | None = None):
        self.retry_after_seconds = retry_after_seconds
        suffix = f" (retry_after={retry_after_seconds:.0f}s)" if retry_after_seconds is not None else ""
        super().__init__(status_code=429, final_url=final_url, message=f"HTTP 429 at {final_url}{suffix}")


@dataclass(slots=True)
class _FetchResult:
    html: str
    status_code: int | None
    final_url: str
    headers: dict[str, str]


class _PlaywrightFetcher:
    def __init__(self, user_agent: str, storage_state_path: str | None) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise AO3FetchError(
                "Playwright is required for live fetching. Install with "
                "`pip install playwright` and `playwright install chromium`."
            ) from exc

        context_kwargs = {"user_agent": user_agent}
        if storage_state_path:
            state_path = Path(storage_state_path)
            if not state_path.exists():
                raise AO3FetchError(f"Storage state file not found: {state_path}")
            context_kwargs["storage_state"] = str(state_path)

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._context = self._browser.new_context(**context_kwargs)
        self._page = self._context.new_page()
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._context.close()
        finally:
            try:
                self._browser.close()
            finally:
                self._playwright.stop()

    def _goto(self, url: str, timeout_ms: int):
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        except ImportError as exc:
            raise AO3FetchError(
                "Playwright import failed unexpectedly while fetching. Reinstall playwright."
            ) from exc

        try:
            response = self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            self._page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        except PlaywrightTimeoutError as exc:
            raise AO3FetchError(f"Playwright timed out loading {url}") from exc
        except PlaywrightError as exc:
            raise AO3FetchError(f"Playwright failed loading {url}: {exc}") from exc

        return response

    def _expand_kudos(self, timeout_ms: int, max_kudos_more_clicks: int) -> None:
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        except ImportError as exc:
            raise AO3FetchError(
                "Playwright import failed unexpectedly while fetching. Reinstall playwright."
            ) from exc

        clicks = 0
        stalled_attempts = 0
        max_stalled_attempts = 3
        while clicks < max_kudos_more_clicks:
            link = self._page.locator("a#kudos_more_link").first
            if link.count() == 0:
                break

            href_before = link.get_attribute("href")
            if not href_before:
                break

            try:
                link.click(timeout=timeout_ms)
                self._page.wait_for_function(
                    """
                    (expectedHref) => {
                        const node = document.querySelector("a#kudos_more_link");
                        return !node || node.getAttribute("href") !== expectedHref;
                    }
                    """,
                    arg=href_before,
                    timeout=timeout_ms,
                )
                clicks += 1
                stalled_attempts = 0
            except PlaywrightTimeoutError:
                link_after = self._page.locator("a#kudos_more_link").first
                href_after = link_after.get_attribute("href") if link_after.count() > 0 else None
                if href_after != href_before:
                    clicks += 1
                    stalled_attempts = 0
                    continue
                stalled_attempts += 1
                if stalled_attempts >= max_stalled_attempts:
                    break
                self._page.wait_for_timeout(500)
            except PlaywrightError as exc:
                raise AO3FetchError(f"Playwright failed while expanding kudos users: {exc}") from exc

    def fetch(self, url: str, timeout_ms: int) -> _FetchResult:
        response = self._goto(url=url, timeout_ms=timeout_ms)

        html = self._page.content()
        final_url = self._page.url
        status_code = response.status if response is not None else None
        headers = response.headers if response is not None else {}

        return _FetchResult(html=html, status_code=status_code, final_url=final_url, headers=headers)

    def fetch_with_expanded_kudos(self, url: str, timeout_ms: int, max_kudos_more_clicks: int) -> _FetchResult:
        response = self._goto(url=url, timeout_ms=timeout_ms)
        self._expand_kudos(timeout_ms=timeout_ms, max_kudos_more_clicks=max_kudos_more_clicks)

        html = self._page.content()
        final_url = self._page.url
        status_code = response.status if response is not None else None
        headers = response.headers if response is not None else {}

        return _FetchResult(html=html, status_code=status_code, final_url=final_url, headers=headers)


_FETCHER: _PlaywrightFetcher | None = None
_FETCHER_KEY: tuple[str, str | None] | None = None


def _normalize_storage_state_path(storage_state_path: str | None) -> str | None:
    if not storage_state_path:
        return None
    return str(Path(storage_state_path).resolve())


def _close_global_fetcher() -> None:
    global _FETCHER, _FETCHER_KEY
    if _FETCHER is not None:
        _FETCHER.close()
        _FETCHER = None
        _FETCHER_KEY = None


atexit.register(_close_global_fetcher)


def _get_or_create_fetcher(user_agent: str, storage_state_path: str | None) -> _PlaywrightFetcher:
    global _FETCHER, _FETCHER_KEY
    normalized_state = _normalize_storage_state_path(storage_state_path)
    key = (user_agent, normalized_state)

    if _FETCHER is not None and _FETCHER_KEY == key:
        return _FETCHER

    _close_global_fetcher()
    _FETCHER = _PlaywrightFetcher(user_agent=user_agent, storage_state_path=normalized_state)
    _FETCHER_KEY = key
    return _FETCHER


def _parse_retry_after_seconds(headers: dict[str, str]) -> float | None:
    retry_after = None
    for key, value in headers.items():
        if key.lower() == "retry-after":
            retry_after = value.strip()
            break
    if not retry_after:
        return None
    if retry_after.isdigit():
        return float(retry_after)
    try:
        when = parsedate_to_datetime(retry_after)
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        now = datetime.now(UTC)
        delta = (when - now).total_seconds()
        return max(0.0, delta)
    except Exception:  # noqa: BLE001
        return None


def fetch_html(
    url: str,
    throttler: RequestThrottler,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_ms: int = 45_000,
    storage_state_path: str | None = None,
) -> str:
    throttler.wait()
    try:
        fetcher = _get_or_create_fetcher(user_agent=user_agent, storage_state_path=storage_state_path)
        result = fetcher.fetch(url=url, timeout_ms=timeout_ms)
    finally:
        throttler.mark_request()

    if not result.html:
        raise AO3FetchError(f"Empty HTML fetched for {url}")
    if result.status_code == 429:
        retry_after = _parse_retry_after_seconds(result.headers)
        raise AO3RateLimitError(final_url=result.final_url, retry_after_seconds=retry_after)
    if result.status_code is not None and result.status_code >= 400:
        raise AO3HTTPStatusError(status_code=result.status_code, final_url=result.final_url)
    if "Shields are up!" in result.html:
        raise AO3FetchError(f"AO3 shield page returned for {result.final_url}")

    return result.html


def fetch_html_with_expanded_kudos(
    url: str,
    throttler: RequestThrottler,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_ms: int = 45_000,
    storage_state_path: str | None = None,
    max_kudos_more_clicks: int = 250,
) -> str:
    throttler.wait()
    try:
        fetcher = _get_or_create_fetcher(user_agent=user_agent, storage_state_path=storage_state_path)
        result = fetcher.fetch_with_expanded_kudos(
            url=url,
            timeout_ms=timeout_ms,
            max_kudos_more_clicks=max_kudos_more_clicks,
        )
    finally:
        throttler.mark_request()

    if not result.html:
        raise AO3FetchError(f"Empty HTML fetched for {url}")
    if result.status_code == 429:
        retry_after = _parse_retry_after_seconds(result.headers)
        raise AO3RateLimitError(final_url=result.final_url, retry_after_seconds=retry_after)
    if result.status_code is not None and result.status_code >= 400:
        raise AO3HTTPStatusError(status_code=result.status_code, final_url=result.final_url)
    if "Shields are up!" in result.html:
        raise AO3FetchError(f"AO3 shield page returned for {result.final_url}")

    return result.html


def capture_storage_state(
    storage_state_path: str,
    login_url: str = "https://archiveofourown.org/users/login",
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_ms: int = 120_000,
) -> None:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise AO3FetchError(
            "Playwright is required for login state capture. Install with "
            "`pip install playwright` and `playwright install chromium`."
        ) from exc

    out_path = Path(storage_state_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=False)
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()
            page.goto(login_url, wait_until="domcontentloaded", timeout=timeout_ms)
            print("Browser opened for AO3 login.")
            print(f"1) Log in at: {login_url}")
            print("2) When done, return to this terminal and press Enter.")
            input()
            context.storage_state(path=str(out_path))
            context.close()
            browser.close()
    except PlaywrightTimeoutError as exc:
        raise AO3FetchError(f"Playwright timed out opening login page: {login_url}") from exc
    except PlaywrightError as exc:
        raise AO3FetchError(f"Playwright failed during login state capture: {exc}") from exc
