#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bandsight Feed Scraper v3.1 (Wyndham tuned)
- Guaranteed non-empty feed
- Real posted/closing dates when available (handles 'X days ago')
- Salary + Band extraction from body copy
- Filters out location-only & nav noise
"""

import re, json, hashlib, requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

CAREERS_URL = "https://recruitment.wyndham.vic.gov.au/careers/"
OUT_FEED = Path("docs/feed.xml")
CHANNEL_TITLE = "Bandsight – Wyndham Job Feed"
CHANNEL_LINK = "https://bandsight.github.io/feeds/feed.xml"
CHANNEL_DESC = "Enriched Wyndham City careers feed with job metadata."
HEADERS = {"User-Agent": "BandsightRSSBot/3.1 (+contact: feeds@bandsight.example)"}


# ------------------------ utils ------------------------

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def fetch_html(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def fmt_rfc2822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")

def parse_relative_date(s: str) -> datetime | None:
    s = (s or "").strip().lower()
    if not s:
        return None
    now = datetime.now(timezone.utc)
    m = re.search(r"(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago", s)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if "minute" in unit:  return now - timedelta(minutes=n)
    if "hour" in unit:    return now - timedelta(hours=n)
    if "day" in unit:     return now - timedelta(days=n)
    return None

def parse_dmy(s: str) -> datetime | None:
    s = (s or "").strip()
    # Accept 22 Oct 2025; 22 October 2025
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None

def money_normalise(s: str) -> str:
    # Keep the original string; just trim. (PBI can parse later.)
    return re.sub(r"\s+", " ", s).strip()


# ------------------ stage 1: candidates -----------------

NOISE_TITLE = re.compile(r"\b(help|site\s*map|privacy|terms|accessibility|feedback)\b", re.I)
NOISE_URL   = re.compile(r"/careers/(?:info|sitemap)(?:/|$)", re.I)
LOCATION_ONLY = re.compile(r"/other-jobs-matching/location-only", re.I)

HINT_HREF = re.compile(r"(job|vacancy|position|officer|engineer|manager|planner|coordinator|advisor)", re.I)
HINT_TEXT = re.compile(r"(job|vacancy|position|officer|engineer|manager|planner|coordinator|advisor)", re.I)

def collect_candidates(url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    picks = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(strip=True) or "").strip()
        abs_link = urljoin(url, href)

        if LOCATION_ONLY.search(abs_link):
            continue
        if NOISE_TITLE.search(text) or NOISE_URL.search(abs_link):
            continue

        looks_job = HINT_HREF.search(href) or HINT_TEXT.search(text)
        if not looks_job:
            continue

        # Avoid generic landing items like "View all New Opportunities"
        if re.search(r"\b(view\s*all|new\s*opportunit)\b", text, re.I):
            continue

        title = text or href.rsplit("/", 1)[-1] or href
        picks.append((title, abs_link))

    # Dedupe by URL
    seen, unique = set(), []
    for t, u in picks:
        if u in seen: 
            continue
        seen.add(u)
        unique.append((t, u))
    return unique


# ------------- stage 2: detail enrichment ----------------

def enrich_job_details(link: str) -> dict:
    """
    Extract posted/closing dates, salary, band, short desc.
    Tries JSON-LD first, then textual fallbacks.
    """
    info = {"posted": None, "closing": None, "salary": None, "band": None, "desc": None}
    try:
        html = fetch_html(link)
        soup = BeautifulSoup(html, "html.parser")

        # JSON-LD
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string)
            except Exception:
                continue
            blocks = data if isinstance(data, list) else [data]
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                if b.get("@type") == "JobPosting":
                    if b.get("datePosted"):
                        # ISO → RFC2822
                        try:
                            dt = datetime.fromisoformat(b["datePosted"].replace("Z", "+00:00")).astimezone(timezone.utc)
                            info["posted"] = fmt_rfc2822(dt)
                        except Exception:
                            info["posted"] = b["datePosted"]
                    if b.get("validThrough"):
                        try:
                            dt = datetime.fromisoformat(b["validThrough"].replace("Z", "+00:00")).astimezone(timezone.utc)
                            info["closing"] = fmt_rfc2822(dt)
                        except Exception:
                            info["closing"] = b["validThrough"]
                    sal = b.get("baseSalary")
                    if isinstance(sal, dict):
                        val = sal.get("value")
                        if isinstance(val, dict):
                            amount = val.get("value")
                            unit = val.get("unitText") or ""
                            if amount:
                                info["salary"] = money_normalise(f"${amount} {unit}".strip())
                    desc_html = b.get("description","")
                    if desc_html:
                        info["desc"] = BeautifulSoup(desc_html, "html.parser").get_text(" ", strip=True)
                    break  # prefer the first JobPosting
        # Text fallbacks
        text = soup.get_text(" ", strip=True)

        if not info["posted"]:
            # "1 day ago", "2 hours ago"
            dt = parse_relative_date(text)
            if dt:
                info["posted"] = fmt_rfc2822(dt)
            else:
                m = re.search(r"(Advertised|Posted)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
                if m:
                    d = parse_dmy(m.group(2))
                    if d: info["posted"] = fmt_rfc2822(d)

        if not info["closing"]:
            m = re.search(r"(Close[s]?|Closing Date)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})", text, re.I)
            if m:
                d = parse_dmy(m.group(2))
                if d: info["closing"] = fmt_rfc2822(d)

        if not info["salary"]:
            # Handle "Band 8 salary from $132,607.76 per annum plus superannuation"
            m = re.search(r"(?:salary\s*(?:from|starting at)\s*)?(\$[\d,]+(?:\.\d{2})?)\s*(?:per\s+annum|pa|p\.a\.)?", text, re.I)
            if m:
                info["salary"] = money_normalise(m.group(1))
            else:
                # Classic range: $100,000 – $110,000
                m = re.search(r"(\$[\d,]+(?:\.\d{2})?)\s*(?:–|-|to)\s*(\$[\d,]+(?:\.\d{2})?)", text)
                if m:
                    info["salary"] = money_normalise(f"{m.group(1)} – {m.group(2)}")

        if not info["band"]:
            m = re.search(r"\bBand\s*(\d+)\b", text, re.I)
            if m:
                info["band"] = f"Band {m.group(1)}"

        if not info["desc"]:
            info["desc"] = " ".join(text.split()[:60]) + "…"

    except Exception as e:
        print(f"[warn] {link}: {e}")
    return info


# ---------------- stage 3: build RSS --------------------

def build_rss(items):
    now = fmt_rfc2822(datetime.now(timezone.utc))
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


# --------------------------- main -----------------------

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

