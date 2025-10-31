#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py
Aggregates local-government job ads into a normalised schema.

Outputs JSON Lines (one object per line).

Usage:
  python src/scraper.py --out data/jobs.jsonl
  python src/scraper.py --councils data/councils.yaml --out data/jobs.jsonl
  python src/scraper.py --councils data/councils.yaml --out data/jobs_history.jsonl --append --delay 0.3

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
  "source_engine": "pageup|pulse_rcm|scout|generic"
}
"""

import argparse
import datetime as dt
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import tz
from dateutil.parser import parse as dateparse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# -------- Config --------

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
    if not x:
        return None
    t = re.sub(r"\s+", " ", x).strip()
    return t or None

def to_date_iso(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
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

def html_of(node: Optional[BeautifulSoup]) -> Optional[str]:
    if not node:
        return None
    return str(node)

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    retry=retry_if_exception_type((requests.RequestException,))
)
def get(url: str, **kw) -> requests.Response:
    kw.setdefault("headers", HEADERS)
    kw.setdefault("timeout", (10, 20))
    resp = requests.get(url, **kw)
    resp.raise_for_status()
    return resp

# -------- Base adapter --------

class BaseAdapter:
    engine_name = "generic"
    def __init__(self, council_name: str, start_url: str):
        self.council_name = council_name
        self.start_url = start_url

    def fetch(self) -> List[JobRecord]:
        """Return list of JobRecord."""
        raise NotImplementedError

# -------- Pulse Software (RCM) adapter (e.g., Ballarat) --------

class PulseRCMAdapter(BaseAdapter):
    engine_name = "pulse_rcm"

    def fetch(self) -> List[JobRecord]:
        # WebServices root inference
        base = self.start_url.rstrip("/")
        if "/WebServices" in base:
            ws = base
        else:
            root = base.split("/Pulse")[0] if "/Pulse" in base else base
            ws = urljoin(root + "/", "WebServices/")
        jobs_url = urljoin(ws, "RCM/Jobs/Jobs?internalOnly=public")
        r = get(jobs_url)
        data = r.json()
        jobs = data.get("Jobs") or []
        out: List[JobRecord] = []
        for j in jobs:
            info = j.get("JobInfo") or {}
            title = clean_text(info.get("Title"))
            link_id = j.get("LinkId")
            slug = re.sub(r"[^\w\-]+", "-", (title or "").lower()).strip("-")
            details_link = urljoin(base.split("/Pulse")[0] + "/", f"Pulse/job/{link_id}/{slug}?source=public")

            desc_html = None
            try:
                dr = get(details_link)
                dsoup = BeautifulSoup(dr.text, "html.parser")
                main = dsoup.select_one(".pulse-container") or dsoup.select_one("#main-content") or dsoup
                desc_html = html_of(main)
            except Exception:
                logging.exception("Pulse details fetch failed for %s", details_link)

            posted = clean_text(info.get("PostedDate") or j.get("PostedDate"))
            close = clean_text(info.get("ClosingDate"))
            salary = clean_text(info.get("Compensation"))
            band = find_first([r"(Band\s*\d+\w?)"], " ".join([title or "", salary or "", desc_html or ""]))
            employment_type = clean_text(info.get("EmploymentType"))
            work_arrangement = clean_text(info.get("WorkArrangement"))
            location = clean_text(info.get("Location"))

            out.append(JobRecord(
                council=self.council_name,
                title=title or "(untitled)",
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

# -------- PageUp People adapter (e.g., City of Greater Geelong) --------

class PageUpAdapter(BaseAdapter):
    engine_name = "pageup"

    def fetch(self) -> List[JobRecord]:
        listing_url = self.start_url
        if "/listing" not in listing_url:
            parts = urlparse(listing_url)
            path = parts.path
            prefix = re.sub(r"/en/.*", "/en/listing/", path)
            listing_url = f"{parts.scheme}://{parts.netloc}{prefix}"

        r = get(listing_url)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("article, .job, .job-search-result, .job-list-item, .job-link")
        if not rows:
            rows = soup.select("a[href*='/job/']")
        jobs: List[JobRecord] = []

        links_seen = set()
        for node in rows:
            a = node if node.name == "a" else node.select_one("a[href*='/job/']")
            if not a:
                continue
            href = urljoin(listing_url, a.get("href"))
            if href in links_seen:
                continue
            links_seen.add(href)

            try:
                jr = self._parse_job_page(href)
                if jr:
                    jobs.append(jr)
            except Exception:
                logging.exception("PageUp item failed: %s", href)

        return jobs

    def _parse_job_page(self, url: str) -> Optional[JobRecord]:
        r = get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        title = clean_text(
            (soup.select_one("h1") or soup.select_one("h2") or soup.select_one(".job-title")).get_text(" ", strip=True)
            if soup.select_one("h1, h2, .job-title") else None
        )

        text = soup.get_text(" ", strip=True)

        salary = find_first([
            r"(?i)(?:Salary|Classification|Remuneration)\s*[:\-]\s*([^|•\n\r]+)",
            r"(?i)\bBand\s*\d+\w?\b[^|•\n\r]*"
        ], text)

        band = find_first([r"(?i)\bBand\s*\d+\w?\b"], text)
        posted = find_first([r"(?i)(?:Posted on|Advertised|Publication date)\s*[:\-]\s*([^\n\r]+)"], text)
        closing = find_first([r"(?i)(?:Closes|Closing|Applications close)\s*[:\-]\s*([^\n\r]+)"], text)
        employment_type = find_first([r"(?i)(?:Work type|Employment Type)\s*[:\-]\s*([^\n\r|•]+)"], text)
        location = find_first([r"(?i)(?:Location)\s*[:\-]\s*([^\n\r|•]+)"], text)

        main = (soup.select_one("main") or
                soup.select_one("#content") or
                soup.select_one(".job-description") or
                soup.select_one(".content") or soup)
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

# -------- Scout/BigRedSky-ish adapter (e.g., centralgoldfieldscareers.com.au) --------

class ScoutAdapter(BaseAdapter):
    engine_name = "scout"

    def fetch(self) -> List[JobRecord]:
        r = get(self.start_url)
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("a[href*='/Vacancies/']")
        if not items:
            items = soup.select("a[href*='/title/']")
        links = []
        for a in items:
            href = urljoin(self.start_url, a.get("href"))
            if re.search(r"/Vacancies/\d+", href) and href not in links:
                links.append(href)

        jobs: List[JobRecord] = []
        for href in links:
            try:
                jobs.append(self._parse(href))
            except Exception:
                logging.exception("Scout parse failed: %s", href)
        return jobs

    def _parse(self, url: str) -> JobRecord:
        r = get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        title = clean_text(soup.select_one("h1, h2, .job-title").get_text(" ", strip=True)
                           if soup.select_one("h1, h2, .job-title") else None)
        text = soup.get_text(" ", strip=True)

        closing = find_first([r"(?i)Closing\s*(?:Date)?\s*[:\-]\s*([^\n\r]+)"], text)
        posted = find_first([r"(?i)(?:Posted|Advertised)\s*[:\-]\s*([^\n\r]+)"], text)
        salary = find_first([r"(?i)(?:Salary|Remuneration)\s*[:\-]\s*([^\n\r]+)"], text)
        band = find_first([r"(?i)\bBand\s*\d+\w?\b"], text)
        employment_type = find_first([r"(?i)(?:Work\s*Type|Employment\s*Type)\s*[:\-]\s*([^\n\r]+)"], text)
        location = find_first([r"(?i)(?:Location)\s*[:\-]\s*([^\n\r]+)"], text)

        content = (soup.select_one(".job") or soup.select_one("#content") or soup)
        desc_html = html_of(content)

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

# -------- Generic HTML fallback --------

class GenericHTMLAdapter(BaseAdapter):
    engine_name = "generic"

    def fetch(self) -> List[JobRecord]:
        r = get(self.start_url)
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = soup.select("a[href]")
        jobs: List[JobRecord] = []
        seen = set()
        for a in anchors:
            href = urljoin(self.start_url, a.get("href"))
            if href in seen:
                continue
            seen.add(href)

            # Only follow likely job detail links
            if not re.search(r"/job|/vacanc|/careers|/employment|/opportun", href, flags=re.I):
                continue

            # Skip obvious noise (profiles, sitemap, help etc.)
            if re.search(r"/(mysubmissions|profile|sitemap|info/help)\b", href, flags=re.I):
                continue

            try:
                jr = self._parse_detail(href)
                if jr:
                    jobs.append(jr)
            except Exception:
                # soft-fail
                pass
        return jobs

    def _parse_detail(self, url: str) -> Optional[JobRecord]:
        r = get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        title_node = soup.select_one("h1, h2, .title, .job-title")
        if not title_node:
            return None
        title = clean_text(title_node.get_text(" ", strip=True))
        text = soup.get_text(" ", strip=True)
        closing = find_first([r"(?i)Closing\s*(?:Date)?\s*[:\-]\s*([^\n\r]+)"], text)
        posted = find_first([r"(?i)Posted\s*(?:on|date)?\s*[:\-]\s*([^\n\r]+)"], text)
        salary = find_first([r"(?i)(?:Salary|Remuneration)\s*[:\-]\s*([^\n\r]+)"], text)
        band = find_first([r"(?i)\bBand\s*\d+\w?\b"], text)
        employment_type = find_first([r"(?i)(?:Employment Type|Work Type)\s*[:\-]\s*([^\n\r]+)"], text)
        location = find_first([r"(?i)(?:Location)\s*[:\-]\s*([^\n\r]+)"], text)
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

# -------- Adapter router --------

def pick_adapter(council_name: str, url: str) -> BaseAdapter:
    host = urlparse(url).netloc.lower()

    if "pulsesoftware.com" in host:
        return PulseRCMAdapter(council_name, url)
    if "careers.pageuppeople.com" in host:
        return PageUpAdapter(council_name, url)
    if "scouttalent" in host or "bigredsky" in host or "mercury" in host:
        return ScoutAdapter(council_name, url)
    if "centralgoldfieldscareers.com.au" in host:
        return ScoutAdapter(council_name, url)
    # Wyndham’s recruitment domain (PageUp-themed UI), treat as generic detail crawler
    if "recruitment.wyndham.vic.gov.au" in host:
        return GenericHTMLAdapter(council_name, url)
    return GenericHTMLAdapter(council_name, url)

# -------- Default councils (fallback if no registry passed) --------

DEFAULT_COUNCILS = [
    ("City of Ballarat", "https://ballarat.pulsesoftware.com/Pulse/jobs"),
    ("Central Goldfields Shire", "https://centralgoldfieldscareers.com.au/Vacancies/"),
    ("Wyndham City", "https://recruitment.wyndham.vic.gov.au/careers/latest-jobs"),
]

# -------- Registry loader (YAML/JSON) --------

def load_registry(path: str) -> List[Tuple[str, str]]:
    """
    Accepts either:
      A) YAML:
         version: 1
         councils:
           - name: ...
             active: true
             starts: [url1, url2, ...]
           - ...
      B) JSON (legacy):
         [{"name": "...", "url": "..."}, ...]
    Returns: List[(council_name, start_url)]
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Registry not found: {path}")

    text = p.read_text(encoding="utf-8")

    data: Any = None
    # Prefer YAML based on extension; otherwise try JSON then YAML
    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # requires PyYAML
        except Exception as e:
            raise RuntimeError("PyYAML not installed. Add 'PyYAML' to requirements.txt.") from e
        data = yaml.safe_load(text)
    else:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            try:
                import yaml
            except Exception as e:
                raise RuntimeError("Registry is not valid JSON and PyYAML is not installed.") from e
            data = yaml.safe_load(text)

    out: List[Tuple[str, str]] = []

    # YAML dict schema
    if isinstance(data, dict) and "councils" in data:
        for row in data.get("councils") or []:
            if not row:
                continue
            name = (row.get("name") or "").strip()
            active = row.get("active", True)
            starts = row.get("starts") or []
            if not name or not active:
                continue
            for u in starts:
                u = (u or "").strip()
                if u:
                    out.append((name, u))
        return out

    # Legacy JSON list schema
    if isinstance(data, list):
        for row in data:
            if not row:
                continue
            name = (row.get("name") or "").strip()
            url = (row.get("url") or "").strip()
            if name and url:
                out.append((name, url))
        return out

    raise RuntimeError(f"Unrecognised registry schema in {path}")

# -------- Runner --------

def scrape_all(councils: List[Tuple[str, str]], inter_council_delay: float = 0.0) -> List[JobRecord]:
    all_jobs: List[JobRecord] = []
    for idx, (name, url) in enumerate(councils, 1):
        try:
            adapter = pick_adapter(name, url)
            logging.info("(%02d/%02d) %s via %s :: %s", idx, len(councils), name, adapter.engine_name, url)
            jobs = adapter.fetch()
            all_jobs.extend(jobs)
        except Exception:
            logging.exception("Failed council: %s (%s)", name, url)
        if inter_council_delay > 0 and idx < len(councils):
            time.sleep(inter_council_delay)
    return dedupe_by_link(all_jobs)

def dedupe_by_link(jobs: List[JobRecord]) -> List[JobRecord]:
    seen = set()
    out: List[JobRecord] = []
    for j in jobs:
        key = (j.council, j.link)
        if key in seen:
            continue
        seen.add(key)
        out.append(j)
    return out

def main():
    parser = argparse.ArgumentParser(description="Bandsight council job scraper")
    parser.add_argument("--councils", help="Path to YAML/JSON registry (see README)", default=None)
    parser.add_argument("--out", help="Output JSONL file (default stdout)", default="-")
    parser.add_argument("--append", help="Append to output file instead of overwrite", action="store_true")
    parser.add_argument("--delay", help="Seconds to sleep between councils (float)", type=float, default=0.0)
    parser.add_argument("--log", help="Log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.councils:
        councils = load_registry(args.councils)
    else:
        councils = DEFAULT_COUNCILS

    logging.info("Loaded %d council start URLs", len(councils))
    for n, (nm, u) in enumerate(councils[:10], 1):
        logging.debug(" [%02d] %s -> %s", n, nm, u)

    jobs = scrape_all(councils, inter_council_delay=args.delay)

    # Write JSONL
    if args.out in ("-", "", None):
        sink = sys.stdout
        close_after = False
    else:
        mode = "a" if args.append else "w"
        sink = open(args.out, mode, encoding="utf-8")
        close_after = True

    try:
        for j in jobs:
            obj = asdict(j)
            sink.write(json.dumps(obj, ensure_ascii=False) + "\n")
    finally:
        if close_after:
            sink.close()

    logging.info("Wrote %d records", len(jobs))

if __name__ == "__main__":
    main()
