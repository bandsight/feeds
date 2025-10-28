#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bandsight Feed Scraper v3.0
---------------------------------
✓ Guaranteed to produce a non-empty feed.
✓ Extracts job metadata (posted / closing dates, salary, band, description).
✓ Escapes XML properly.
✓ Designed for GitHub Actions.

Run from Actions with:
  - name: Run scraper
    run: |
      python src/scraper_v3.py
"""

import re, json, hashlib, requests
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

CAREERS_URL = "https://recruitment.wyndham.vic.gov.au/careers/"
OUT_FEED = Path("docs/feed.xml")

CHANNEL_TITLE = "Bandsight – Wyndham Job Feed"
CHANNEL_LINK = "https://bandsight.github.io/feeds/feed.xml"
CHANNEL_DESC = "Enriched Wyndham City careers feed with job metadata."

HEADERS = {"User-Agent": "BandsightRSSBot/3.0 (+contact: feeds@bandsight.example)"}


# ---------- Helpers -----------------------------------------------------------

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def fetch_html(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# ---------- Stage 1: collect broad links -------------------------------------

def collect_candidates(url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    picks = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(strip=True) or "").strip()
        abs_link = urljoin(url, href)
        # Broad inclusion rules
        looks_job = re.search(r"(job|vacancy|position|officer|engineer|manager|planner|coordinator|advisor)", href, re.I) \
                    or re.search(r"(job|vacancy|officer|engineer|manager|planner|coordinator|advisor)", text, re.I)
        if not looks_job:
            continue
        # Exclude navigation noise
        if re.search(r"\b(help|site\s*map|privacy|terms|accessibility)\b", text, re.I):
            continue
        if re.search(r"/careers/(?:info|sitemap)(?:/|$)", abs_link, re.I):
            continue
        picks.append((text or href, abs_link))

    # Dedupe by URL
    seen, unique = set(), []
    for t, u in picks:
        if u in seen: 
            continue
        seen.add(u)
        unique.append((t, u))
    return unique


# ---------- Stage 2: enrich job detail pages ---------------------------------

def enrich_job_details(link: str) -> dict:
    """Fetch job detail page and extract metadata."""
    info = {"posted": None, "closing": None, "salary": None, "band": None, "desc": None}
    try:
        html = fetch_html(link)
        soup = BeautifulSoup(html, "html.parser")

        # JSON-LD
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string)
                if isinstance(data, dict) and data.get("@type") == "JobPosting":
                    info["posted"] = data.get("datePosted")
                    info["closing"] = data.get("validThrough")
                    sal = data.get("baseSalary")
                    if isinstance(sal, dict):
                        val = sal.get("value")
                        if isinstance(val, dict):
                            info["salary"] = val.get("value")
                    info["desc"] = BeautifulSoup(data.get("description",""),"html.parser").get_text(" ",strip=True)
                    break
            except Exception:
                continue

        text = soup.get_text(" ", strip=True)
        if not info["posted"]:
            m = re.search(r"(Advertised|Posted)[:\s]+(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
            if m: info["posted"] = m.group(2)
        if not info["closing"]:
            m = re.search(r"(Close[s]?|Closing Date)[:\s]+(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
            if m: info["closing"] = m.group(2)
        if not info["salary"]:
            m = re.search(r"\$[\d,]+\s*(?:–|-|to)\s*\$[\d,]+", text)
            if m: info["salary"] = m.group(0)
        if not info["band"]:
            m = re.search(r"\bBand\s*\d+\b", text, re.I)
            if m: info["band"] = m.group(0)
        if not info["desc"]:
            info["desc"] = " ".join(text.split()[:60]) + "..."
    except Exception as e:
        print(f"[warn] {link}: {e}")
    return info


# ---------- Stage 3: Build RSS ------------------------------------------------

def build_rss(items):
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

    if not items:
        items = [("No listings found (placeholder)", CAREERS_URL)]

    for title, link in items:
        meta = enrich_job_details(link)
        pubdate = meta["posted"] or now
        desc = escape(meta["desc"] or "No description provided.")
        salary = escape(meta["salary"] or "")
        band = escape(meta["band"] or "")
        closing = escape(meta["closing"] or "")
        parts.extend([
            "<item>",
            f"<title>{escape(title)}</title>",
            f"<link>{escape(link)}</link>",
            f"<guid isPermaLink='false'>{sha1(link)}</guid>",
            f"<pubDate>{pubdate}</pubDate>",
            f"<description>{desc}</description>",
            f"<category>{band or 'Wyndham City — Jobs'}</category>",
            f"<salary>{salary}</salary>",
            f"<closing>{closing}</closing>",
            "</item>",
        ])

    parts.extend(["</channel>", "</rss>"])
    return "\n".join(parts)


# ---------- Main --------------------------------------------------------------

def main():
    try:
        items = collect_candidates(CAREERS_URL)
        print(f"[info] Found {len(items)} candidate links.")
    except Exception as e:
        print(f"[error] Cannot fetch listing: {e}")
        items = []

    OUT_FEED.parent.mkdir(parents=True, exist_ok=True)
    xml = build_rss(items)
    OUT_FEED.write_text(xml, encoding="utf-8")
    print(f"[done] Wrote {OUT_FEED} with {len(items) or 1} item(s).")


if __name__ == "__main__":
    main()
