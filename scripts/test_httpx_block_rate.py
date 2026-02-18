#!/usr/bin/env python
from __future__ import annotations

import argparse
import math
import random
import re
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)

PAGE_RE = re.compile(r"[?&]page=(\d+)")


@dataclass(slots=True)
class SampleResult:
    page: int
    status: str
    http_status: int | None
    elapsed_s: float
    url: str


def set_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params[key] = value
    return urlunsplit(parts._replace(query=urlencode(params)))


def pick_pages(samples: int, max_page: int, seed: int) -> list[int]:
    rng = random.Random(seed)
    if samples >= max_page:
        pages = list(range(1, max_page + 1))
        rng.shuffle(pages)
        return pages
    return sorted(rng.sample(range(1, max_page + 1), samples))


def classify_response(status_code: int, text: str) -> str:
    lowered = text.lower()
    if status_code == 403 and "shields are up!" in lowered:
        return "shield_403"
    if status_code == 200 and 'class="work blurb group' in lowered:
        return "ok_works_page"
    if status_code == 200 and "shields are up!" in lowered:
        return "shield_200"
    if status_code in {403, 429, 503}:
        return f"blocked_{status_code}"
    return f"other_{status_code}"


def discover_max_page(client: httpx.Client, tag_url: str) -> int | None:
    response = client.get(tag_url)
    if response.status_code != 200:
        return None
    page_nums = [int(m.group(1)) for m in PAGE_RE.finditer(response.text)]
    if not page_nums:
        return 1
    return max(page_nums)


def wilson_interval(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 0.0)
    phat = k / n
    denom = 1.0 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    margin = (z / denom) * math.sqrt((phat * (1 - phat) / n) + (z * z / (4 * n * n)))
    return (max(0.0, center - margin), min(1.0, center + margin))


def should_use_fallback(status: str) -> bool:
    return status != "ok_works_page"


def run_probe(
    tag_url: str,
    pages: Iterable[int],
    base_delay: float,
    jitter: float,
    user_agent: str,
) -> list[SampleResult]:
    results: list[SampleResult] = []
    headers = {"User-Agent": user_agent}
    with httpx.Client(follow_redirects=True, timeout=30.0, headers=headers) as client:
        for i, page in enumerate(pages, start=1):
            url = set_query_param(tag_url, "page", str(page))
            started = time.monotonic()
            http_status: int | None = None
            try:
                response = client.get(url)
                http_status = response.status_code
                status = classify_response(response.status_code, response.text)
            except Exception as exc:  # noqa: BLE001
                status = f"exception_{exc.__class__.__name__}"
            elapsed = time.monotonic() - started
            results.append(
                SampleResult(page=page, status=status, http_status=http_status, elapsed_s=elapsed, url=url)
            )

            print(
                f"[{i:>3}] page={page:<5} status={status:<20} "
                f"http={str(http_status):<4} elapsed={elapsed:.2f}s"
            )
            delay = base_delay + random.uniform(0.0, jitter)
            print(f"      sleeping {delay:.2f}s")
            time.sleep(delay)

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate AO3 httpx block/fallback rate.")
    parser.add_argument("--tag-url", required=True, help="AO3 tag works URL.")
    parser.add_argument("--samples", type=int, default=30, help="Number of pages to sample.")
    parser.add_argument("--max-page", type=int, help="Max page to sample from. If omitted, tries discovery.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for page sampling.")
    parser.add_argument("--base-delay", type=float, default=3.0, help="Base delay between requests.")
    parser.add_argument("--jitter", type=float, default=0.8, help="Jitter upper bound in seconds.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent header.")
    args = parser.parse_args()

    if args.samples <= 0:
        raise SystemExit("--samples must be > 0")
    if args.base_delay < 0 or args.jitter < 0:
        raise SystemExit("--base-delay and --jitter must be >= 0")

    max_page = args.max_page
    if max_page is None:
        print("Discovering max page from first request...")
        with httpx.Client(
            follow_redirects=True,
            timeout=30.0,
            headers={"User-Agent": args.user_agent},
        ) as client:
            max_page = discover_max_page(client, args.tag_url)
        if max_page is None:
            print("Could not discover max page (first request was blocked/non-200).")
            print("Use --max-page explicitly, e.g. --max-page 500")
            return 2
        print(f"Discovered max_page={max_page}")

    pages = pick_pages(args.samples, max_page, args.seed)
    print(f"Sampling {len(pages)} pages from 1..{max_page}")
    print(f"Delay policy: {args.base_delay}s + jitter(0..{args.jitter}s)")

    results = run_probe(
        tag_url=args.tag_url,
        pages=pages,
        base_delay=args.base_delay,
        jitter=args.jitter,
        user_agent=args.user_agent,
    )

    n = len(results)
    fallback_count = sum(1 for r in results if should_use_fallback(r.status))
    fallback_rate = fallback_count / n if n else 0.0
    lo, hi = wilson_interval(fallback_count, n)

    print("\nSummary")
    print(f"- Samples: {n}")
    print(f"- Fallback-needed count: {fallback_count}")
    print(f"- Estimated fallback rate: {fallback_rate:.1%}")
    print(f"- 95% Wilson CI: [{lo:.1%}, {hi:.1%}]")

    by_status: dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1

    print("- Status breakdown:")
    for status, count in sorted(by_status.items(), key=lambda item: (-item[1], item[0])):
        print(f"  - {status}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
