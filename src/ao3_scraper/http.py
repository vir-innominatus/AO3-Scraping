from __future__ import annotations

import httpx

from ao3_scraper.rate_limit import RequestThrottler

BASE_URL = "https://archiveofourown.org"
DEFAULT_USER_AGENT = "AO3ResearchBot/0.1 (+local research project; contact: you@example.com)"


class AO3BlockedError(RuntimeError):
    pass


def fetch_html(url: str, throttler: RequestThrottler, user_agent: str = DEFAULT_USER_AGENT) -> str:
    throttler.wait()
    headers = {"User-Agent": user_agent}
    with httpx.Client(follow_redirects=True, timeout=30.0, headers=headers) as client:
        response = client.get(url)

    if response.status_code == 403 and "Shields are up!" in response.text:
        raise AO3BlockedError(
            "AO3 returned a shield challenge page (403). Use --input-html for local parsing "
            "or switch to a browser-based fetch path."
        )
    response.raise_for_status()
    throttler.mark_request()
    return response.text
