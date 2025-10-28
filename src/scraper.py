#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bandsight Feed Scraper v4.1 – Wyndham historical with deeper crawl
- Crawls main careers and /latest-jobs (with pagination)
- Collects only real job detail links (/careers/jobs/...)
- De-dupes and APPENDS to existing feed.xml (historical ledger)
"""

import re, json, hashlib, requests, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

START_URLS = [
    "https://recruitment.wyndham.vic.gov.au/careers/",
    "https://recruitment.wyndham.vic.gov.au/careers/latest-jobs",
]
OUT_FEED = Path("docs/feed.xml")
CHANNEL_TITLE = "Bandsight – Wyndham Job Feed (Historical)"
CHANNEL_LINK = "https://bandsight.github.io/feeds/feed.xml"
CHANNEL_DESC = "Cumulative Wyndham City job feed with historical records."
HEADERS = {"User-Agent": "BandsightRSSBot/4.1 (+contact: feeds@bandsight.example)"}

# -------- basics

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def now_rfc() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

def fetch_html(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

# -------- existing feed loader

def parse_existing_items():
    if not OUT_FEED.exists():
        return {}
    try:
        tree = ET.parse(OUT_FEED)
        root = tree.getroot()
        existing = {}
        for item in root.findall(".//item"):
            guid = item.findtext("guid")
            if guid:
                existing[guid.strip()] = True
        print(f"[info] Loaded {len(existing)} existing items.")
        return existing
    except Exception as e:
        print(f"[warn] Could not parse existing feed: {e}")
        return {}

# -------- link collection

RE_JOB_DETAIL = re.compile(r"/careers/jobs/", re.I)
RE_NOISE_URL = re.compile(r"/careers/(?:info|sitemap)(?:/|$)", re.I)
RE_LOCATION_ONLY = re.compile(r"/other-jobs-matching/location-only", re.I)
RE_NOISE_TEXT = re.compile(r"\b(help|site\s*map|privacy|terms|accessibility|feedback)\b", re.I)

def collect_from_page(url: str):
    """Collect job detail links from a single page."""
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(strip=True) or "").strip()
        abs_link = urljoin(url, href)

        if RE_NOISE_URL.search(abs_link) or RE_LOCATION_ONLY.search(abs_link):
            continue
        if RE_NOISE_TEXT.search(text):
            continue
        if not RE_JOB_DETAIL.search(abs_link):
            continue  # keep only real job detail pages

        title = text or href.rsplit("/", 1)[-1] or href
        links.append((title, abs_link))
    return links, soup

def find_next_page(base_url: str, soup: BeautifulSoup) -> str | None:
    """Find next page on latest-jobs (varies by vendor; handle common patterns)."""
    # Try a rel=next link
    rel_next = soup.find("a", attrs={"rel": "next"})
    if rel_next and rel_next.get("href"):
        return urljoin(base_url, rel_next["href"])

    # Try anchors with 'Next' text
    for a in soup.find_all("a", href=True):
        t = (a.get_text(strip=True) or "").lower()
        if "next" in t or "older" in t:
            return urljoin(base_url, a["href"])

    # Some sites use query params like ?startrow= or ?page=
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "start" in href or "page=" in href:
            return urljoin(base_url, href)

    return None

def collect_all_candidates() -> list[tuple[str, str]]:
    """Crawl both start URLs and paginate latest-jobs a few pages."""
    seen_urls = set()
    picks: list[tuple[str, str]] = []

    for start in START_URLS:
        url = start
        page_count = 0
        max_pages = 10  # safety bound

        while url and page_count < max_pages:
            try:
                found, soup = collect_from_page(url)
                for t, u in found:
                    if u not in seen_urls:
                        seen_urls.add(u)
                        picks.append((t, u))
                page_count += 1
                # Only paginate aggressively on /latest-jobs tree
                if "/latest-jobs" in start:
                    url = find_next_page(url, soup)
                else:
                    url = None  # just single page for the main careers landing
            except Exception as e:
                print(f"[warn] crawl error on {url}: {e}")
                break

    print(f"[info] Collected {len(picks)} job detail links.")
    return picks

# -------- enrichment (lightweight but useful)

def enrich(link: str):
    """Extract band, salary, posted date (best-effort)."""
    meta = {"desc": "", "band": "", "salary": "", "posted": ""}
    try:
        html = fetch_html(link)
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Band
        m = re.search(r"\bBand\s*\d+\b", text, re.I)
        if m: meta["band"] = m.group(0)

        # Salary (single 'from $X' or a range)
        m = re.search(r"(?:salary\s*(?:from|starting at)\s*)?(\$[\d,]+(?:\.\d{2})?)", text, re.I)
        if m: meta["salary"] = m.group(1)
        m2 = re.search(r"(\$[\d,]+(?:\.\d{2})?)\s*(?:–|-|to)\s*(\$[\d,]+(?:\.\d{2})?)", text)
        if m2: meta["salary"] = f"{m2.group(1)} – {m2.group(2)}"

        # Posted/Advertised date or <time datetime>
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag and time_tag.get("datetime"):
            meta["posted"] = time_tag["datetime"]
        else:
            m = re.search(r"(Advertised|Posted)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
            if m:
                meta["posted"] = m.group(2)  # leave as human date; PBI can parse

        # Short description teaser
        meta["desc"] = " ".join(text.split()[:60]) + "..."
    except Exception as e:
        print(f"[warn] enrich {link}: {e}")
    return meta

# -------- append writer

def write_appended_feed(new_items: list[tuple[str, str, str, dict]], existing_count: int):
    """Append new <item> blocks to existing feed (or create fresh if none)."""
    now = now_rfc()

    if not OUT_FEED.exists():
        # fresh channel skeleton
        with open(OUT_FEED, "w", encoding="utf-8") as f:
            f.write("<?xml version='1.0' encoding='UTF-8'?>\n")
            f.write("<rss version='2.0'>\n<channel>\n")
            f.write(f"<title>{escape(CHANNEL_TITLE)}</title>\n")
            f.write(f"<link>{escape(CHANNEL_LINK)}</link>\n")
            f.write(f"<description>{escape(CHANNEL_DESC)}</description>\n")
            f.write("<language>en-au</language>\n")
            f.write(f"<lastBuildDate>{now}</lastBuildDate>\n")
            for guid, title, link, meta in new_items:
                f.write("  <item>\n")
                f.write(f"    <title>{escape(title)}</title>\n")
                f.write(f"    <link>{escape(link)}</link>\n")
                f.write(f"    <guid isPermaLink='false'>{guid}</guid>\n")
                f.write(f"    <pubDate>{escape(meta.get('posted') or now)}</pubDate>\n")
                f.write(f"    <description>{escape(meta.get('desc',''))}</description>\n")
                f.write(f"    <category>{escape(meta.get('band') or 'Wyndham City — Jobs')}</category>\n")
                f.write(f"    <salary>{escape(meta.get('salary',''))}</salary>\n")
                f.write("  </item>\n")
            f.write("</channel>\n</rss>\n")
        print(f"[done] Created new feed with {len(new_items)} items.")
        return

    # append to existing: snip tail, append, close again
    txt = OUT_FEED.read_text(encoding="utf-8")
    txt = re.sub(r"</channel>\s*</rss>\s*$", "", txt.strip(), flags=re.S)
    with open(OUT_FEED, "w", encoding="utf-8") as f:
        f.write(txt + "\n")
        # update lastBuildDate (optional, keep simple by adding a new one next to old)
        # For simplicity, we won't try to replace; many readers ignore it.
        for guid, title, link, meta in new_items:
            f.write("  <item>\n")
            f.write(f"    <title>{escape(title)}</title>\n")
            f.write(f"    <link>{escape(link)}</link>\n")
            f.write(f"    <guid isPermaLink='false'>{guid}</guid>\n")
            f.write(f"    <pubDate>{escape(meta.get('posted') or now)}</pubDate>\n")
            f.write(f"    <description>{escape(meta.get('desc',''))}</description>\n")
            f.write(f"    <category>{escape(meta.get('band') or 'Wyndham City — Jobs')}</category>\n")
            f.write(f"    <salary>{escape(meta.get('salary',''))}</salary>\n")
            f.write("  </item>\n")
        f.write("</channel>\n</rss>\n")
    print(f"[done] Appended {len(new_items)} items. Total (approx): {existing_count + len(new_items)}")

# -------- main

def main():
    existing = parse_existing_items()
    seen = set(existing.keys())

    # crawl
    candidates = collect_all_candidates()
    print(f"[info] Considering {len(candidates)} links for enrichment…")

    new_items = []
    for title, link in candidates:
        guid = sha1(link)
        if guid in seen:
            continue
        meta = enrich(link)
        new_items.append((guid, title, link, meta))

    if not new_items:
        print("[info] No new items to append.")
        return

    write_appended_feed(new_items, existing_count=len(seen))

if __name__ == "__main__":
    main()
