#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Guaranteed-hit scraper for Wyndham careers.
- Grabs many candidate links from the page (broad net).
- Filters obvious junk (help/sitemap/privacy/etc).
- Escapes XML properly to avoid parser errors.
- Always writes at least one <item>, even if no jobs are found.

Usage in GitHub Actions (.github/workflows/build.yml):
  - name: Run scraper
    run: |
      python src/scraper_basic.py
"""

import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

CAREERS_URL = "https://recruitment.wyndham.vic.gov.au/careers/"
OUT_FEED = Path("docs/feed.xml")
CHANNEL_TITLE = "Bandsight – Guaranteed Wyndham Feed"
CHANNEL_LINK = "https://bandsight.github.io/feeds/feed.xml"
CHANNEL_DESC = "Guaranteed hits from Wyndham City careers page."

# Heuristics: anything that looks like a job (in href or text)
JOB_HINTS_HREF = re.compile(
    r"(job|vacancy|position|role|apply|listing|opportunit|engineer|officer|manager|planner|coordinator|advisor)",
    re.I,
)
JOB_HINTS_TEXT = re.compile(
    r"(job|vacancy|position|officer|engineer|manager|planner|coordinator|advisor|specialist|technician)",
    re.I,
)

# Obvious noise we never want
NOISE_TITLE = re.compile(r"\b(help|site\s*map|privacy|terms|accessibility|feedback)\b", re.I)
NOISE_URL = re.compile(r"/careers/(?:info|sitemap)(?:/|$)", re.I)


def fetch_html(url: str, timeout: int = 30) -> str:
    headers = {"User-Agent": "BandsightRSSBot/1.0 (+contact: feeds@bandsight.example)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def collect_candidates(url: str) -> List[Tuple[str, str]]:
    """Return a broad set of (title, absolute_url) tuples likely to be jobs."""
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    picks: List[Tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(strip=True) or "").strip()
        abs_link = urljoin(url, href)

        # Broad include rules (either href or text looks job-like)
        looks_job = JOB_HINTS_HREF.search(href) or JOB_HINTS_TEXT.search(text)
        if not looks_job:
            continue

        # Exclude obvious noise
        if NOISE_TITLE.search(text) or NOISE_URL.search(abs_link):
            continue

        # Grab something human-readable; if blank, fall back to href tail
        title = text or href.rsplit("/", 1)[-1] or href
        picks.append((title, abs_link))

    # Dedupe by URL, keep first title seen
    seen = set()
    unique: List[Tuple[str, str]] = []
    for t, u in picks:
        if u in seen:
            continue
        seen.add(u)
        unique.append((t, u))

    return unique


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def build_rss(items: List[Tuple[str, str]]) -> str:
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    parts = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "<rss version=\"2.0\">",
        "<channel>",
        f"<title>{escape(CHANNEL_TITLE)}</title>",
        f"<link>{escape(CHANNEL_LINK)}</link>",
        f"<description>{escape(CHANNEL_DESC)}</description>",
        "<language>en-au</language>",
        f"<lastBuildDate>{now}</lastBuildDate>",
    ]

    # Always emit at least one item so the feed is never empty
    if not items:
        items = [("No listings found (placeholder)", CAREERS_URL)]

    for title, link in items:
        title_safe = escape(title)
        link_safe = escape(link)
        guid = sha1(link_safe)

        parts.extend(
            [
                "<item>",
                f"<title>{title_safe}</title>",
                f"<link>{link_safe}</link>",
                f"<guid isPermaLink=\"false\">{guid}</guid>",
                f"<pubDate>{now}</pubDate>",
                "<category>Wyndham City — Jobs</category>",
                "</item>",
            ]
        )

    parts.extend(["</channel>", "</rss>"])
    return "\n".join(parts)


def main() -> None:
    try:
        items = collect_candidates(CAREERS_URL)
    except Exception as e:
        # Fail open: write a placeholder feed if scraping throws
        items = []
        print(f"[warn] scraping error: {e!r}")

    OUT_FEED.parent.mkdir(parents=True, exist_ok=True)
    xml = build_rss(items)
    OUT_FEED.write_text(xml, encoding="utf-8")
    print(f"Wrote {OUT_FEED} with {len(items) or 1} item(s).")


if __name__ == "__main__":
    main()
