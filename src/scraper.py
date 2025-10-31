#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py
Aggregates Victorian local-government job ads into a normalised schema.

Outputs JSON Lines (one object per line).

Usage examples:
  python scraper.py --out data/jobs_history.jsonl
  python scraper.py --councils data/councils.yaml --out data/jobs_history.jsonl
  python scraper.py --councils data/councils.json --out data/jobs_history.jsonl --delay 0.5 --log DEBUG

Schema:
{
  "council": "City of Greater Geelong",
  "title": "Traffic Engineer",
  "link": "https://…",
  "posted_date": "2025-10-28",
  "closing_date": "2025-11-12",
  "salary": "$95,760",
  "band": "Band 6",
  "employment_type": "Full time",
  "work_arrangement": "On-site / Hybrid",
  "location": "Civic Centre, WERRIBEE",
  "description_html": "<p>…</p>",
  "scrape_date": "2025-10-30T14:05:00+11:00",
  "source_engine": "pageup|pulse_rcm|scout|applynow|generic"
}
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from dateutil import tz
from dateutil.parser import parse as dateparse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

try:
    import yaml  # Optional, only needed for YAML council registry
except Exception:
    yaml = None  # We'll error nicely if someone passes a YAML path without PyYAML installed

AUS_TZ = tz.gettz("Australia/Melbourne")
HEADERS = {
    "User-Agent": "BandsightScraper/1.0 (+https://github.com/bandsight) requests/2.x",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
}

# -------- Normalised job record --------

@dataclass
class JobRecord:
    council: str
    title: str
    link: str
    posted_date: Optional[str]
    closing_date: Optional[str]
    salary: Optional[str]
    band: Optional[str]
    employment_type: Optional[str]
    work_arrangement: Optional[str]
    location: Optional[str]
    description_html: Optional[str]
    scrape_date: str
    source_engine: str

# -------- Utilities --------

def now_iso() -> str:
    return dt.datetime.now(tz=AUS_TZ).isoformat(timespec="seconds")

def clean_text(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    t = re.sub(r"\s+", " ", x).strip()
    return t or None

def to_date_iso(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        d = dateparse(s, dayfirst=True, fuzzy=True)
        return d.date().isoformat()
    except Exception:
        return None

def find_first(patterns: Iterable[str], text: str) -> Optional[str]:
    for p in patterns:
        m = re.search(p, text, flags=re.I | re.S)
        if m:
            return clean_text(m.group(1) if m.groups() else m.group(0))
    return None

def html_of(node: Optional[BeautifulSoup | Tag]) -> Optional[str]:
    if not node:
        return None
    return str(node)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    retry=retry_if_exception_type((requests.RequestException,))
)
def get(url: str, session: Optional[requests.Session] = None, **kw) -> requests.Response:
    kw.setdefault("timeout", (10, 20))
    sess = session or requests
    resp = sess.get(url, **kw)
    resp.raise_for_status()
    return resp

# -------- Base adapter --------

class BaseAdapter:
    engine_name = "generic"

    def __init__(self, council_name: str, start_url: str, session: Optional[requests.Session] = None, delay: float = 0.0):
        self.council_name = council_name
        self.start_url = start_url
        self.session = session or make_session()
        self.delay = max(0.0, delay)

    def _sleep(self, seconds: Optional[float] = None) -> None:
        time.sleep(self.delay if seconds is None else max(0.0, seconds))

    def fetch(self) -> List[JobRecord]:
        """Return list of JobRecord."""
        raise NotImplementedError

# -------- Pulse Software (RCM) adapter (e.g., Ballarat) --------

class PulseRCMAdapter(BaseAdapter):
    engine_name = "pulse_rcm"

    def fetch(self) -> List[JobRecord]:
        base = self.start_url.rstrip("/")
        if "/WebServices" in base:
            ws = base
        else:
            root = base.split("/Pulse")[0] if "/Pulse" in base else base
            ws = urljoin(root + "/", "WebServices/")
        jobs_url = urljoin(ws, "RCM/Jobs/Jobs?internalOnly=public")

        logging.debug("Pulse Jobs endpoint: %s", jobs_url)
        r = get(jobs_url, session=self.session)
        data = r.json()
        jobs = data.get("Jobs") or []
        out: List[JobRecord] = []

        for j in jobs:
            info = j.get("JobInfo") or {}
            title = clean_text(info.get("Title")) or "(untitled)"
            link_id = j.get("LinkId") or info.get("LinkId")
            slug = re.sub(r"[^\w\-]+", "-", title.lower()).strip("-")
            details_link = urljoin(base.split("/Pulse")[0] + "/", f"Pulse/job/{link_id}/{slug}?source=public")

            desc_html = None
            try:
                dr = get(details_link, session=self.session)
                dsoup = BeautifulSoup(dr.text, "html.parser")
                main = dsoup.select_one(".pulse-container") or dsoup.select_one("#main-content") or dsoup.select_one("main") or dsoup
                desc_html = html_of(main)
                self._sleep(0.2)
            except Exception:
                logging.debug("Pulse details fetch failed for %s", details_link, exc_info=True)

            posted = clean_text(info.get("PostedDate") or j.get("PostedDate"))
            close = clean_text(info.get("ClosingDate"))
            salary = clean_text(info.get("Compensation"))
            blob = " ".join([title or "", salary or "", desc_html or ""])
            band = find_first([r"(Band\s*\d+\w?)"], blob)
            employment_type = clean_text(info.get("EmploymentType"))
            work_arrangement = clean_text(info.get("WorkArrangement"))
            location = clean_text(info.get("Location"))

            out.append(JobRecord(
                council=self.council_name,
                title=title,
                link=details_link,
                posted_date=to_date_iso(posted),
                closing_date=to_date_iso(close),
                salary=salary,
                band=band,
                employment_type=employment_type,
                work_arrangement=work_arrangement,
                location=location,
                description_html=desc_html,
                scrape_date=now_iso(),
                source_engine=self.engine_name
            ))
        return out

# -------- PageUp People adapter --------

class PageUpAdapter(BaseAdapter):
    engine_name = "pageup"

    def fetch(self) -> List[JobRecord]:
        listing_url = self._coerce_listing(self.start_url)
        r = get(listing_url, session=self.session)
        soup = BeautifulSoup(r.text, "html.parser")

        candidates = soup.select("article, .job, .job-search-result, .job-list-item, .job-link, .search-result")
        if not candidates:
            candidates = soup.select("a[href*='/job/']")
        jobs: List[JobRecord] = []
        links_seen = set()

        for node in candidates:
            a = node if node.name == "a" else node.select_one("a[href*='/job/']")
            if not a:
                continue
            href = urljoin(listing_url, a.get("href") or "")
            if not href or href in links_seen:
                continue
            links_seen.add(href)
            try:
                jr = self._parse_job_page(href)
                if jr:
                    jobs.append(jr)
                self._sleep()
            except Exception:
                logging.debug("PageUp item failed: %s", href, exc_info=True)
        return jobs

    def _coerce_listing(self, url: str) -> str:
        # If it's already an /en/listing/ page, keep as-is; otherwise try to form it.
        parts = urlparse(url)
        if "/listing" in parts.path:
            return url
        path = parts.path or "/"
        prefix = re.sub(r"/en/.*", "/en/listing/", path)
        if prefix == path:
            # fallback to /en/listing/ root
            prefix = "/en/listing/"
        return f"{parts.scheme}://{parts.netloc}{prefix}"

    def _parse_job_page(self, url: str) -> Optional[JobRecord]:
        r = get(url, session=self.session)
        soup = BeautifulSoup(r.text, "html.parser")

        title_node = soup.select_one("h1, h2, .job-title, .job-title-text")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else "(untitled)"
        text = soup.get_text(" ", strip=True)

        salary = find_first([
            r"(?i)(?:Salary|Classification|Remuneration)\s*[:\-]\s*([^|•\n\r]+)",
            r"(?i)\bBand\s*\d+\w?\b[^|•\n\r]*",
        ], text)
        band = find_first([r"(?i)\bBand\s*\d+\w?\b"], text)
        posted = find_first([r"(?i)(?:Posted on|Advertised|Publication date)\s*[:\-]\s*([^\n\r]+)"], text)
        closing = find_first([r"(?i)(?:Closes|Closing|Applications close)\s*[:\-]\s*([^\n\r]+)"], text)
        employment_type = find_first([r"(?i)(?:Work type|Employment Type)\s*[:\-]\s*([^\n\r|•]+)"], text)
        location = find_first([r"(?i)(?:Location)\s*[:\-]\s*([^\n\r|•]+)"], text)

        main = (soup.select_one("main") or soup.select_one("#content") or
                soup.select_one(".job-description") or soup.select_one(".content") or soup)
        desc_html = html_of(main)

        return JobRecord(
            council=self.council_name,
            title=title or "(untitled)",
            link=url,
            posted_date=to_date_iso(posted),
            closing_date=to_date_iso(closing),
            salary=clean_text(salary),
            band=clean_text(band),
            employment_type=clean_text(employment_type),
            work_arrangement=None,
            location=clean_text(location),
            description_html=desc_html,
            scrape_date=now_iso(),
            source_engine=self.engine_name
        )

# -------- Scout/BigRedSky-ish adapter (incl. centralgoldfieldscareers.com.au) --------

class ScoutAdapter(BaseAdapter):
    engine_name = "scout"

    def fetch(self) -> List[JobRecord]:
        r = get(self.start_url, session=self.session)
        soup = BeautifulSoup(r.text, "html.parser")
        # Common listing anchors
        items = soup.select("a[href*='/Vacancies/'], a[href*='/vacancies/'], a[href*='/title/'], a[href*='/Position/']")
        links: List[str] = []
        for a in items:
            href = urljoin(self.start_url, a.get("href") or "")
            if not href:
                continue
            if href not in links:
                links.append(href)

        jobs: List[JobRecord] = []
        for href in links:
            try:
                jobs.append(self._parse(href))
                self._sleep()
            except Exception:
                logging.debug("Scout parse failed: %s", href, exc_info=True)
        return jobs

    def _parse(self, url: str) -> JobRecord:
        r = get(url, session=self.session)
        soup = BeautifulSoup(r.text, "html.parser")

        title = clean_text((soup.select_one("h1, h2, .job-title") or soup.title).get_text(" ", strip=True)
                           if soup.select_one("h1, h2, .job-title, title") else None) or "(untitled)"
        text = soup.get_text(" ", strip=True)

        closing = find_first([r"(?i)Closing\s*(?:Date)?\s*[:\-]\s*([^\n\r]+)"], text)
        posted = find_first([r"(?i)(?:Posted|Advertised)\s*[:\-]\s*([^\n\r]+)"], text)
        salary = find_first([r"(?i)(?:Salary|Remuneration|Package)\s*[:\-]\s*([^\n\r]+)"], text)
        band = find_first([r"(?i)\bBand\s*\d+\w?\b"], text)
        employment_type = find_first([r"(?i)(?:Work\s*Type|Employment\s*Type)\s*[:\-]\s*([^\n\r]+)"], text)
        location = find_first([r"(?i)(?:Location|Based at)\s*[:\-]\s*([^\n\r]+)"], text)

        content = soup.select_one(".job, .job-details, #content, main") or soup
        desc_html = html_of(content)

        return JobRecord(
            council=self.council_name,
            title=title,
            link=url,
            posted_date=to_date_iso(posted),
            closing_date=to_date_iso(closing),
            salary=clean_text(salary),
            band=clean_text(band),
            employment_type=clean_text(employment_type),
            work_arrangement=None,
            location=clean_text(location),
            description_html=desc_html,
            scrape_date=now_iso(),
            source_engine=self.engine_name
        )

# -------- ApplyNow adapter (e.g., Macedon Ranges) --------

DATE_PATTERNS_APPLYNOW = [
    r"(applications\s+close|closing|closes)\s*[:\-]\s*(?P<date>.+)$",
    r"(posted|advertised)\s*[:\-]\s*(?P<date>.+)$",
]

EMPLOYMENT_PATTERNS = [r"\b(full[-\s]?time)\b", r"\b(part[-\s]?time)\b", r"\b(casual)\b",
                       r"\b(temporary|fixed[-\s]?term)\b", r"\b(contract)\b"]

LOCATION_PATTERNS = [r"\b(location|based\s+at|work\s*location)\s*[:\-]\s*(?P<loc>.+)"]

SALARY_PATTERNS = [r"\b(salary|remuneration|package|band)\s*[:\-]\s*(?P<sal>.+)",
                   r"\b(\$[0-9][\d,]*(?:\s*-\s*\$?[0-9][\d,]*)?)\b"]

BAND_PATTERNS = [r"\bband\s*([0-9]{1,2}\w?)\b", r"\b(grade|classification)\s*([0-9A-Z]{1,3})\b"]

class ApplyNowAdapter(BaseAdapter):
    engine_name = "applynow"

    def _clean_text_node(self, node: Optional[Tag]) -> str:
        if not node:
            return ""
        return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

    def _parse_date_soft(self, text: str) -> Optional[str]:
        t = (text or "").strip()
        if not t:
            return None
        # direct parse
        try:
            dtv = dateparse(t, dayfirst=True, fuzzy=True)
            if dtv:
                return dtv.strftime("%Y-%m-%d")
        except Exception:
            pass
        # label-led parse
        for patt in DATE_PATTERNS_APPLYNOW:
            m = re.search(patt, t, flags=re.I)
            if m and m.groupdict().get("date"):
                try:
                    dtv = dateparse(m.group("date"), dayfirst=True, fuzzy=True)
                    if dtv:
                        return dtv.strftime("%Y-%m-%d")
                except Exception:
                    continue
        return None

    def _extract_with_patterns(self, text: str, patterns: list[str]) -> Optional[str]:
        for patt in patterns:
            m = re.search(patt, text or "", flags=re.I)
            if m:
                gd = m.groupdict() if hasattr(m, "groupdict") else {}
                for key in ("loc", "sal"):
                    if key in gd and gd[key]:
                        return gd[key].strip()
                return m.group(0).strip()
        return None

    def _find_listing_cards(self, soup: BeautifulSoup) -> list[Tag]:
        cards: list[Tag] = []
        selectors = [
            "ul li a[href*='applynow'], ul li a[href*='/jobs/'], ul li a[href*='/job/']",
            "article a[href*='applynow'], .job-list a[href*='applynow'], .job a[href*='applynow']",
            "a[href*='applynow.net.au/']",
            "a[href*='/applynow/']",
        ]
        for sel in selectors:
            for a in soup.select(sel):
                if isinstance(a, Tag) and a.get("href"):
                    cards.append(a)
            if cards:
                break
        # dedupe by href
        seen, uniq = set(), []
        for a in cards:
            href = a.get("href")
            if href and href not in seen:
                seen.add(href)
                uniq.append(a)
        return uniq

    def _detail_blob(self, soup: BeautifulSoup) -> Dict[str, str]:
        candidates = ["main", "article", ".content", ".container", "#content", ".job, .job-details, .job-detail"]
        html_node = None
        for sel in candidates:
            node = soup.select_one(sel)
            if node and self._clean_text_node(node):
                html_node = node
                break
        if not html_node:
            html_node = soup.body or soup
        blob = self._clean_text_node(html_node)
        return {"blob": blob, "html": str(html_node)}

    def _parse_detail(self, url: str) -> Dict[str, Optional[str]]:
        r = get(url, session=self.session)
        soup = BeautifulSoup(r.text, "html.parser")
        meta = self._detail_blob(soup)

        blob = meta["blob"]
        posted = None
        closing = None
        haystack_nodes = soup.select("dl, .key-details, .job-meta, .job-summary, table")
        hay = " ".join(self._clean_text_node(n) for n in haystack_nodes) if haystack_nodes else blob

        closing = self._parse_date_soft(hay)
        if closing and re.search(r"posted|advertised", hay, flags=re.I):
            # try to extract both, best-effort
            try:
                dates = re.findall(r"([0-9]{1,2}\s+\w+\s+[0-9]{4}|\w+\s+[0-9]{1,2},?\s+[0-9]{4})", hay)
                if len(dates) >= 2:
                    posted = to_date_iso(dates[0])
                    closing = to_date_iso(dates[1])
            except Exception:
                pass

        return {
            "description_html": meta["html"],
            "employment_type": clean_text(self._extract_with_patterns(blob, EMPLOYMENT_PATTERNS)),
            "work_arrangement": None,
            "location": clean_text(self._extract_with_patterns(blob, LOCATION_PATTERNS)),
            "salary": clean_text(self._extract_with_patterns(blob, SALARY_PATTERNS)),
            "band": clean_text(find_first(BAND_PATTERNS, blob) or None),
            "posted_date": posted,
            "closing_date": closing,
        }

    def fetch(self) -> List[JobRecord]:
        results: List[JobRecord] = []
        next_url = self.start_url
        visited = set()

        while next_url and next_url not in visited:
            visited.add(next_url)
            r = get(next_url, session=self.session)
            soup = BeautifulSoup(r.text, "html.parser")

            for a in self._find_listing_cards(soup):
                title = clean_text(a.get_text(" ", strip=True)) or "(untitled)"
                href = a.get("href", "").strip()
                if not href:
                    continue
                link = urljoin(next_url, href)
                # Scope guard
                host = urlparse(link).netloc.lower()
                if ("applynow.net.au" not in host) and ("/applynow/" not in link) and ("/job" not in link):
                    continue

                detail = {}
                try:
                    detail = self._parse_detail(link)
                    self._sleep()
                except Exception:
                    logging.debug("ApplyNow detail error: %s", link, exc_info=True)
                    detail = {k: None for k in (
                        "description_html", "employment_type", "work_arrangement",
                        "location", "salary", "band", "posted_date", "closing_date"
                    )}

                results.append(JobRecord(
                    council=self.council_name,
                    title=title,
                    link=link,
                    posted_date=detail.get("posted_date"),
                    closing_date=detail.get("closing_date"),
                    salary=detail.get("salary"),
                    band=detail.get("band"),
                    employment_type=detail.get("employment_type"),
                    work_arrangement=detail.get("work_arrangement"),
                    location=detail.get("location"),
                    description_html=detail.get("description_html"),
                    scrape_date=now_iso(),
                    source_engine=self.engine_name
                ))

            # Simple pagination discovery
            nxt = None
            for a in soup.select("a[rel='next']"):
                if a.get("href"):
                    nxt = urljoin(next_url, a.get("href"))
                    break
            if not nxt:
                for a in soup.find_all("a"):
                    if (a.get_text(" ", strip=True) or "").strip().lower() in {"next", "older", "more jobs"} and a.get("href"):
                        nxt = urljoin(next_url, a.get("href"))
                        break
            next_url = nxt

        return results

# -------- Generic HTML fallback --------

class GenericHTMLAdapter(BaseAdapter):
    engine_name = "generic"

    def fetch(self) -> List[JobRecord]:
        r = get(self.start_url, session=self.session)
        soup = BeautifulSoup(r.text, "htmlparser") if False else BeautifulSoup(r.text, "html.parser")  # guard
        anchors = soup.select("a[href]")
        jobs: List[JobRecord] = []
        seen = set()

        for a in anchors:
            href = urljoin(self.start_url, a.get("href") or "")
            if not href or href in seen:
                continue
            seen.add(href)
            if not re.search(r"/job|/vacanc|/careers|/employment|/opportun", href, flags=re.I):
                continue
            try:
                jr = self._parse_detail(href)
                if jr:
                    jobs.append(jr)
                self._sleep()
            except Exception:
                # keep quiet; generic is noisy by nature
                pass
        return jobs

    def _parse_detail(self, url: str) -> Optional[JobRecord]:
        r = get(url, session=self.session)
        soup = BeautifulSoup(r.text, "html.parser")
        title_node = soup.select_one("h1, h2, .title, .job-title")
        if not title_node:
            return None
        title = clean_text(title_node.get_text(" ", strip=True))
        text = soup.get_text(" ", strip=True)
        closing = find_first([r"(?i)Closing\s*(?:Date)?\s*[:\-]\s*([^\n\r]+)"], text)
        posted = find_first([r"(?i)Posted\s*(?:on|date)?\s*[:\-]\s*([^\n\r]+)"], text)
        salary = find_first([r"(?i)(?:Salary|Remuneration|Package)\s*[:\-]\s*([^\n\r]+)"], text)
        band = find_first([r"(?i)\bBand\s*\d+\w?\b"], text)
        employment_type = find_first([r"(?i)(?:Employment Type|Work Type)\s*[:\-]\s*([^\n\r]+)"], text)
        location = find_first([r"(?i)(?:Location|Based at)\s*[:\-]\s*([^\n\r]+)"], text)
        desc_html = html_of(soup.select_one("main") or soup.select_one("#content") or soup)

        return JobRecord(
            council=self.council_name,
            title=title or "(untitled)",
            link=url,
            posted_date=to_date_iso(posted),
            closing_date=to_date_iso(closing),
            salary=clean_text(salary),
            band=clean_text(band),
            employment_type=clean_text(employment_type),
            work_arrangement=None,
            location=clean_text(location),
            description_html=desc_html,
            scrape_date=now_iso(),
            source_engine=self.engine_name
        )

# -------- Router / council catalogue --------

def infer_vendor_from_url(url: str) -> str:
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.lower()
    if "pulsesoftware.com" in host:
        return "pulse"
    if "careers.pageuppeople.com" in host or host.endswith("bendigo.vic.gov.au"):
        return "pageup"
    if "applynow.net.au" in host:
        return "applynow"
    if "scouttalent" in host or "bigredsky" in host or "mercury.com.au" in host or "centralgoldfieldscareers.com.au" in host:
        return "scout"
    if "recruitment.wyndham.vic.gov.au" in host:
        return "generic"
    return "generic"

def pick_adapter(council_name: str, url: str, vendor_hint: Optional[str], session: requests.Session, delay: float) -> BaseAdapter:
    vendor = (vendor_hint or infer_vendor_from_url(url) or "generic").lower()
    if vendor in {"pulse", "pulse_rcm"}:
        return PulseRCMAdapter(council_name, url, session=session, delay=delay)
    if vendor == "pageup":
        return PageUpAdapter(council_name, url, session=session, delay=delay)
    if vendor in {"scout", "bigredsky"}:
        return ScoutAdapter(council_name, url, session=session, delay=delay)
    if vendor == "applynow":
        return ApplyNowAdapter(council_name, url, session=session, delay=delay)
    return GenericHTMLAdapter(council_name, url, session=session, delay=delay)

# -------- Default council list (fallback) --------

DEFAULT_COUNCILS = [
    ("City of Ballarat", "https://ballarat.pulsesoftware.com/Pulse/jobs", "pulse"),
    ("Golden Plains Shire", "https://www.goldenplains.vic.gov.au/council/about-council/careers", "generic"),
    ("Moorabool Shire", "https://www.moorabool.vic.gov.au/About-Council/Careers", "generic"),
    ("Hepburn Shire", "https://www.hepburn.vic.gov.au/work-with-us/current-vacancies", "generic"),
    ("Central Goldfields Shire", "https://centralgoldfieldscareers.com.au/Vacancies", "scout"),
    ("Pyrenees Shire", "https://www.pyrenees.vic.gov.au/Your-Council/Careers/Current-vacancies", "generic"),
    ("Ararat Rural City", "https://www.ararat.vic.gov.au/council/careers/jobs-ararat-rural-city", "generic"),
    ("City of Greater Geelong", "https://careers.pageuppeople.com/887/cw/en/listing/", "pageup"),
    ("Wyndham City", "https://recruitment.wyndham.vic.gov.au/careers/latest-jobs", "generic"),
    ("Surf Coast Shire", "https://www.surfcoast.vic.gov.au/council/careers/current-vacancies", "generic"),
    ("City of Greater Bendigo", "https://careers.bendigo.vic.gov.au/en/listing/", "pageup"),
    ("Macedon Ranges Shire", "https://macedon-ranges-ext-shire-portal.applynow.net.au/", "applynow"),
]

# -------- Registry loaders --------

def load_councils_from_file(path: str) -> List[Tuple[str, str, Optional[str]]]:
    """
    Accepts:
      - YAML (preferred): { version: 1, councils: [ {name, vendor?, starts: [..], active?} ] }
      - JSON (legacy): [ { "name": "...", "url": "..." } ]  OR  [ ["name", "url"], ... ]
    Returns list of (name, url, vendor_hint)
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    if path.lower().endswith((".yaml", ".yml")):
        if yaml is None:
            raise RuntimeError("PyYAML is not installed; cannot read YAML registries.")
        data = yaml.safe_load(raw)
        councils = data.get("councils") if isinstance(data, dict) else data
        out: List[Tuple[str, str, Optional[str]]] = []
        for c in councils or []:
            if not c.get("active", True):
                continue
            name = c["name"]
            vendor = c.get("vendor")
            starts = c.get("starts") or []
            for u in starts:
                out.append((name, u, vendor))
        return out

    # JSON
    data = json.loads(raw)
    out: List[Tuple[str, str, Optional[str]]] = []
    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict) and "name" in row and "url" in row:
                out.append((row["name"], row["url"], row.get("vendor")))
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                out.append((row[0], row[1], row[2] if len(row) >= 3 else None))
    return out

# -------- Runner --------

def scrape_all(registry: List[Tuple[str, str, Optional[str]]], delay: float) -> List[JobRecord]:
    session = make_session()
    all_jobs: List[JobRecord] = []
    for name, url, vendor_hint in registry:
        try:
            adapter = pick_adapter(name, url, vendor_hint, session=session, delay=delay)
            logging.info("Scraping %s via %s → %s", name, adapter.engine_name, url)
            jobs = adapter.fetch()
            all_jobs.extend(jobs)
        except Exception:
            logging.exception("Failed council: %s (%s)", name, url)
    return all_jobs

def dedupe_by_link(jobs: List[JobRecord], existing_keys: Optional[set[tuple]] = None) -> List[JobRecord]:
    seen = set(existing_keys or [])
    out: List[JobRecord] = []
    for j in jobs:
        key = (j.council, j.link)
        if key in seen:
            continue
        seen.add(key)
        out.append(j)
    return out

def load_existing_keys(out_path: str) -> set[tuple]:
    keys: set[tuple] = set()
    if not out_path or out_path in ("-", ""):
        return keys
    if not os.path.exists(out_path):
        return keys
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    keys.add((obj.get("council"), obj.get("link")))
                except Exception:
                    continue
    except Exception:
        logging.debug("Failed reading existing output for dedupe; proceeding without.", exc_info=True)
    return keys

def main():
    parser = argparse.ArgumentParser(description="Bandsight council job scraper")
    parser.add_argument("--councils", help="Path to YAML/JSON registry (see README). If omitted, uses built-in defaults.", default=None)
    parser.add_argument("--out", help="Output JSONL file (default stdout)", default="-")
    parser.add_argument("--append", help="Append to output file instead of overwrite (default: true)", action="store_true", default=True)
    parser.add_argument("--no-append", dest="append", action="store_false")
    parser.add_argument("--delay", help="Seconds to sleep between detail requests", type=float, default=0.3)
    parser.add_argument("--log", help="Log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.councils:
        registry = load_councils_from_file(args.councils)
    else:
        registry = DEFAULT_COUNCILS

    jobs = scrape_all(registry, delay=args.delay)

    # Dedupe (including against existing file when appending)
    existing = load_existing_keys(args.out) if (args.out not in ("-", "", None) and args.append) else set()
    jobs = dedupe_by_link(jobs, existing_keys=existing)

    # Write JSONL
    if args.out in ("-", "", None):
        sink = sys.stdout
        for j in jobs:
            sink.write(json.dumps(asdict(j), ensure_ascii=False) + "\n")
        sink.flush()
    else:
        mode = "a" if args.append else "w"
        with open(args.out, mode, encoding="utf-8") as f:
            for j in jobs:
                f.write(json.dumps(asdict(j), ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()
