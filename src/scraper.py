#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bandsight Multi-Council Scraper v5.0 (Historical)
- Crawls multiple councils within ~100 km of Ballarat
- Vendor-aware collectors (Pulse, PageUp, ApplyNow, generic pages)
- De-dupes by GUID (sha1(link)), APPENDS to docs/feed.xml (historical ledger)
- Tags each <item> with <council> and tries to extract Band/Salary/Posted
"""

import re, json, hashlib, requests, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

OUT_FEED = Path("docs/feed.xml")
CHANNEL_TITLE = "Bandsight – Central Vic Councils Job Feed (Historical)"
CHANNEL_LINK  = "https://bandsight.github.io/feeds/feed.xml"
CHANNEL_DESC  = "Cumulative jobs feed for councils within ~100 km of Ballarat."
HEADERS = {"User-Agent": "BandsightRSSBot/5.0 (+contact: feeds@bandsight.example)"}
TIMEOUT = 25

# ---- Councils & start URLs (seeded from official careers endpoints)
COUNCILS = [
    {"name": "City of Ballarat", "starts": [
        "https://ballarat.pulsesoftware.com/Pulse/jobs",    # Pulse listing
        "https://www.ballarat.vic.gov.au/careers"          # site hub (fallback)
    ], "vendor": "pulse"},
    {"name": "Golden Plains Shire", "starts": [
        "https://www.goldenplains.vic.gov.au/council/careers/vacancies"
    ], "vendor": "generic"},
    {"name": "Hepburn Shire", "starts": [
        "https://www.hepburn.vic.gov.au/Council/Work-for-Council/Job-vacancies"
    ], "vendor": "generic"},
    {"name": "Moorabool Shire", "starts": [
        "https://www.moorabool.vic.gov.au/About-Council/Careers/Vacancies"
    ], "vendor": "generic"},
    {"name": "Pyrenees Shire", "starts": [
        "https://www.pyrenees.vic.gov.au/About-Pyrenees-Shire-Council/Work-For-Pyrenees-Shire-Council/Employment-Opportunities-with-Pyrenees-Shire-Council"
    ], "vendor": "generic"},
    {"name": "Central Goldfields Shire", "starts": [
        "https://centralgoldfieldscareers.com.au/Vacancies/"
    ], "vendor": "generic"},
    {"name": "City of Greater Geelong", "starts": [
        "https://careers.pageuppeople.com/887/cw/en/listing/"
    ], "vendor": "pageup"},
    {"name": "City of Melton", "starts": [
        "https://meltoncity-vacancies.applynow.net.au/"
    ], "vendor": "applynow"},
    {"name": "Wyndham City", "starts": [
        "https://recruitment.wyndham.vic.gov.au/careers/latest-jobs",
        "https://recruitment.wyndham.vic.gov.au/careers/"
    ], "vendor": "generic"},
    # You can append Macedon Ranges / Mount Alexander easily: add {"name": "...", "starts": [...], "vendor": "generic"}
]

# ---- helpers
def sha1(s: str) -> str: return hashlib.sha1(s.encode("utf-8")).hexdigest()
def now_rfc() -> str:     return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

def get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def try_text(el) -> str:
    return (el.get_text(" ", strip=True) if el else "").strip()

def parse_existing_guids() -> set[str]:
    if not OUT_FEED.exists(): return set()
    try:
        root = ET.parse(OUT_FEED).getroot()
        return { (i.findtext("guid") or "").strip() for i in root.findall(".//item") if i.findtext("guid") }
    except Exception:
        return set()

# ---- vendor collectors → return list[(title, link)]
RE_JOB_WORD = re.compile(r"(job|position|officer|engineer|planner|coordinator|manager)", re.I)

def collect_generic(start_url: str) -> list[tuple[str,str]]:
    html = get(start_url); soup = BeautifulSoup(html, "html.parser")
    picks = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip(); text = try_text(a)
        link = urljoin(start_url, href)
        if any(s in link for s in ["sitemap", "/info/", "/privacy", "accessibility"]): continue
        if not RE_JOB_WORD.search(href) and not RE_JOB_WORD.search(text): continue
        if "/other-jobs-matching/location-only" in link: continue
        # Prefer deep job detail pages
        if any(k in link for k in ["/jobs/", "/Vacancies/", "/vacancies/", "/employment/", "/careers/"]):
            title = text or href.rsplit("/",1)[-1]
            picks.append((title, link))
    # de-dupe by link
    seen=set(); out=[]
    for t,u in picks:
        if u in seen: continue
        seen.add(u); out.append((t,u))
    return out

def collect_pulse(listing_url: str) -> list[tuple[str,str]]:
    # Pulse lists as table rows with links to /Pulse/job
    html = get(listing_url); soup = BeautifulSoup(html, "html.parser")
    picks=[]
    for a in soup.find_all("a", href=True):
        href=a["href"]; text=try_text(a)
        link=urljoin(listing_url, href)
        if "/Pulse/job" in link or "/Pulse/Job" in link:
            title = text or href.rsplit("/",1)[-1]
            picks.append((title, link))
    return picks

def collect_pageup(listing_url: str) -> list[tuple[str,str]]:
    html=get(listing_url); soup=BeautifulSoup(html,"html.parser")
    picks=[]
    for a in soup.find_all("a", href=True):
        href=a["href"]; text=try_text(a)
        link=urljoin(listing_url, href)
        # PageUp job detail often has /en/job/ or /en/listing/ → detail pages include /en/job/
        if "/en/job/" in link:
            title=text or href.rsplit("/",1)[-1]
            picks.append((title, link))
    return picks

def collect_applynow(listing_url: str) -> list[tuple[str,str]]:
    html=get(listing_url); soup=BeautifulSoup(html,"html.parser")
    picks=[]
    for a in soup.select("a[href]"):
        href=a["href"]; text=try_text(a)
        link=urljoin(listing_url, href)
        # ApplyNow uses /applynow/ or /apply/ or /register/ with jobId param; detail often contains '/applynow/'
        if "applynow.net.au" in link and ("/applynow/" in link or "job" in link.lower()):
            title=text or href.rsplit("/",1)[-1]
            picks.append((title, link))
    return picks

VENDOR_COLLECTORS = {
    "generic": collect_generic,
    "pulse":   collect_pulse,
    "pageup":  collect_pageup,
    "applynow":collect_applynow,
}

# ---- enrichment (light)
def enrich(link: str) -> dict:
    meta = {"desc":"", "band":"", "salary":"", "posted":"", "closing":""}
    try:
        html = get(link); soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Band
        m = re.search(r"\bBand\s*\d+\b", text, re.I)
        if m: meta["band"] = m.group(0)

        # Salary (from $X or range)
        m = re.search(r"(?:salary\s*(?:from|starting at)\s*)?(\$[\d,]+(?:\.\d{2})?)", text, re.I)
        if m: meta["salary"] = m.group(1)
        m2 = re.search(r"(\$[\d,]+(?:\.\d{2})?)\s*(?:–|-|to)\s*(\$[\d,]+(?:\.\d{2})?)", text)
        if m2: meta["salary"] = f"{m2.group(1)} – {m2.group(2)}"

        # Posted / Advertised / <time datetime>
        ttag = soup.find("time", attrs={"datetime": True})
        if ttag and ttag.get("datetime"):
            meta["posted"] = ttag["datetime"]
        else:
            m = re.search(r"(Advertised|Posted)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
            if m: meta["posted"] = m.group(2)

        # Closing date (nice-to-have)
        m = re.search(r"(Close[s]?|Closing Date)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
        if m: meta["closing"] = m.group(2)

        # Short desc
        meta["desc"] = " ".join(text.split()[:60]) + "…"
    except Exception as e:
        print(f"[warn] enrich fail {link}: {e}")
    return meta

# ---- feed writer (append)
def write_appended(master_new: list[dict]):
    now = now_rfc()
    if not OUT_FEED.exists():
        with open(OUT_FEED, "w", encoding="utf-8") as f:
            f.write("<?xml version='1.0' encoding='UTF-8'?>\n<rss version='2.0'>\n<channel>\n")
            f.write(f"<title>{escape(CHANNEL_TITLE)}</title>\n<link>{escape(CHANNEL_LINK)}</link>\n")
            f.write(f"<description>{escape(CHANNEL_DESC)}</description>\n<language>en-au</language>\n")
            f.write(f"<lastBuildDate>{now}</lastBuildDate>\n")
            for it in master_new:
                f.write(render_item(it, now))
            f.write("</channel>\n</rss>\n")
        print(f"[done] created feed.xml with {len(master_new)} items")
        return

    txt = OUT_FEED.read_text(encoding="utf-8").strip()
    txt = re.sub(r"</channel>\s*</rss>\s*$", "", txt, flags=re.S)
    with open(OUT_FEED, "w", encoding="utf-8") as f:
        f.write(txt + "\n")
        for it in master_new:
            f.write(render_item(it, now))
        f.write("</channel>\n</rss>\n")
    print(f"[done] appended {len(master_new)} items")

def render_item(it: dict, now: str) -> str:
    return (
        "  <item>\n"
        f"    <title>{escape(it['title'])}</title>\n"
        f"    <link>{escape(it['link'])}</link>\n"
        f"    <guid isPermaLink='false'>{it['guid']}</guid>\n"
        f"    <pubDate>{escape(it.get('posted') or now)}</pubDate>\n"
        f"    <description>{escape(it.get('desc',''))}</description>\n"
        f"    <category>{escape(it.get('band') or it['council'] + ' — Jobs')}</category>\n"
        f"    <salary>{escape(it.get('salary',''))}</salary>\n"
        f"    <closing>{escape(it.get('closing',''))}</closing>\n"
        f"    <council>{escape(it['council'])}</council>\n"
        "  </item>\n"
    )

# ---- main
def main():
    seen = parse_existing_guids()
    to_append = []

    for c in COUNCILS:
        vendor = c["vendor"]
        collector = VENDOR_COLLECTORS.get(vendor, collect_generic)
        collected=[]
        for start in c["starts"]:
            try:
                collected.extend(collector(start))
            except Exception as e:
                print(f"[warn] {c['name']} start {start}: {e}")
        # de-dupe within council
        seen_links=set(); unique=[]
        for t,u in collected:
            if u in seen_links: continue
            seen_links.add(u); unique.append((t,u))

        for title, link in unique:
            guid = sha1(link)
            if guid in seen: 
                continue
            meta = enrich(link)
            to_append.append({
                "guid": guid,
                "title": title or link.rsplit("/",1)[-1],
                "link": link,
                "posted": meta.get("posted",""),
                "desc": meta.get("desc",""),
                "band": meta.get("band",""),
                "salary": meta.get("salary",""),
                "closing": meta.get("closing",""),
                "council": c["name"],
            })

    if not to_append:
        print("[info] nothing new to append"); return
    write_appended(to_append)

if __name__ == "__main__":
    main()
