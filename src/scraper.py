#!/usr/bin/env python3
import re, requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pathlib import Path

URL = "https://recruitment.wyndham.vic.gov.au/careers/"
OUT = Path("docs/feed.xml")

def get_links(url):
    html = requests.get(url, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text(strip=True) or "").strip()
        # Pick anything that looks like a job
        if re.search(r"(job|vacancy|officer|engineer|manager|apply)", href, re.I) or \
           re.search(r"(job|officer|engineer|manager)", text, re.I):
            links.append((text or href, requests.compat.urljoin(url, href)))
    return links

def build_feed(items):
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    xml = ["""<?xml version="1.0" encoding="UTF-8"?>""",
           "<rss version='2.0'><channel>",
           "<title>Bandsight – Guaranteed Wyndham Feed</title>",
           "<link>https://bandsight.github.io/feeds/feed.xml</link>",
           "<description>Guaranteed hits from Wyndham City careers page.</description>",
           f"<lastBuildDate>{now}</lastBuildDate>"]
    for title, link in items:
        xml.append(f"<item><title>{title}</title><link>{link}</link>"
                   f"<guid isPermaLink='false'>{hash(link)}</guid>"
                   f"<pubDate>{now}</pubDate></item>")
    xml.append("</channel></rss>")
    return "\n".join(xml)

def main():
    items = get_links(URL)
    if not items:
        # fallback — always have something
        items = [("No listings found (placeholder)", URL)]
    OUT.write_text(build_feed(items), encoding="utf-8")
    print(f"Wrote {OUT} with {len(items)} items")

if __name__ == "__main__":
    main()
