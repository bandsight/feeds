#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bandsight Feed Scraper v4.0 – Historical Append Mode
----------------------------------------------------
Instead of overwriting feed.xml, this version reads any existing file
and appends new items (uniqued by GUID/link). Keeps a full running history.
"""

import re, json, hashlib, requests, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

CAREERS_URL = "https://recruitment.wyndham.vic.gov.au/careers/"
OUT_FEED = Path("docs/feed.xml")
CHANNEL_TITLE = "Bandsight – Wyndham Job Feed (Historical)"
CHANNEL_LINK = "https://bandsight.github.io/feeds/feed.xml"
CHANNEL_DESC = "Cumulative Wyndham City job feed with historical records."
HEADERS = {"User-Agent": "BandsightRSSBot/4.0 (+contact: feeds@bandsight.example)"}


# ---------- helpers ----------
def sha1(s): return hashlib.sha1(s.encode("utf-8")).hexdigest()
def now_rfc(): return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

def fetch_html(url):
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text

def parse_existing_items():
    """Return dict {guid: xml_element} from existing feed.xml, if any."""
    if not OUT_FEED.exists():
        return {}
    try:
        tree = ET.parse(OUT_FEED)
        root = tree.getroot()
        existing = {}
        for item in root.findall(".//item"):
            guid = item.findtext("guid")
            if guid:
                existing[guid.strip()] = item
        print(f"[info] Loaded {len(existing)} existing items.")
        return existing
    except Exception as e:
        print(f"[warn] Could not parse existing feed: {e}")
        return {}


# ---------- scraper ----------
def collect_candidates(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    picks = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(strip=True) or "").strip()
        abs_link = urljoin(url, href)
        if re.search(r"/other-jobs-matching/location-only", abs_link): continue
        if not re.search(r"(job|officer|manager|engineer|planner|coordinator)", href, re.I): continue
        if re.search(r"(help|site\s*map|privacy|terms)", text, re.I): continue
        title = text or href
        picks.append((title, abs_link))
    return picks


# ---------- enrichment ----------
def enrich(link):
    meta = {"desc": "", "band": "", "salary": "", "posted": ""}
    try:
        html = fetch_html(link)
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        m = re.search(r"\bBand\s*\d+\b", text, re.I)
        if m: meta["band"] = m.group(0)
        m = re.search(r"\$[\d,]+(?:\s*(?:–|-|to)\s*\$[\d,]+)?", text)
        if m: meta["salary"] = m.group(0)
        m = re.search(r"(Advertised|Posted)[:\s]+(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
        if m: meta["posted"] = m.group(2)
        meta["desc"] = " ".join(text.split()[:60]) + "..."
    except Exception as e:
        print(f"[warn] {link}: {e}")
    return meta


# ---------- build/append ----------
def main():
    existing = parse_existing_items()
    seen = set(existing.keys())

    new_items = []
    for title, link in collect_candidates(CAREERS_URL):
        guid = sha1(link)
        if guid in seen:
            continue
        meta = enrich(link)
        new_items.append((guid, title, link, meta))

    if not new_items:
        print("[info] No new items found.")
        return

    print(f"[info] Appending {len(new_items)} new items.")
    now = now_rfc()

    # if no feed, start from scratch
    if not OUT_FEED.exists():
        items_xml = ""
    else:
        # strip trailing tags so we can append
        xml_text = OUT_FEED.read_text(encoding="utf-8").strip()
        xml_text = re.sub(r"</channel>\s*</rss>\s*$", "", xml_text)
        items_xml = xml_text

    with open(OUT_FEED, "w", encoding="utf-8") as f:
        if not items_xml:
            f.write(f"<?xml version='1.0' encoding='UTF-8'?>\n<rss version='2.0'>\n<channel>\n")
            f.write(f"<title>{escape(CHANNEL_TITLE)}</title>\n<link>{escape(CHANNEL_LINK)}</link>\n")
            f.write(f"<description>{escape(CHANNEL_DESC)}</description>\n<language>en-au</language>\n")
        else:
            f.write(items_xml + "\n")

        for guid, title, link, meta in new_items:
            f.write("  <item>\n")
            f.write(f"    <title>{escape(title)}</title>\n")
            f.write(f"    <link>{escape(link)}</link>\n")
            f.write(f"    <guid isPermaLink='false'>{guid}</guid>\n")
            f.write(f"    <pubDate>{meta['posted'] or now}</pubDate>\n")
            f.write(f"    <description>{escape(meta['desc'])}</description>\n")
            f.write(f"    <category>{escape(meta['band'] or 'Wyndham City — Jobs')}</category>\n")
            f.write(f"    <salary>{escape(meta['salary'])}</salary>\n")
            f.write("  </item>\n")

        f.write("</channel>\n</rss>\n")

    print(f"[done] Feed updated with {len(new_items)} new items. Total historical items: {len(seen) + len(new_items)}")


if __name__ == "__main__":
    main()
