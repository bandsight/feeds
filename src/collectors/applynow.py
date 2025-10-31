# -*- coding: utf-8 -*-
"""
ApplyNowAdapter
---------------
Adapter for portals hosted on *.applynow.net.au (e.g. Macedon Ranges).
Designed to normalize records to Bandsight's shared schema.

Returns a list[dict] with keys:
  title, link, posted_date, closing_date, salary, band,
  employment_type, work_arrangement, location, description_html
"""

from __future__ import annotations
import re
import time
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dateutil import parser as dateparser

DEFAULT_TIMEOUT = 20
HEADERS = {
    "User-Agent": "BandsightScraper/1.0 (+https://github.com/bandsight)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DATE_PATTERNS = [
    r"(applications\s+close|closing|closes)\s*[:\-]\s*(?P<date>.+)$",
    r"(posted|advertised)\s*[:\-]\s*(?P<date>.+)$",
]

EMPLOYMENT_PATTERNS = [
    r"\b(full[-\s]?time)\b",
    r"\b(part[-\s]?time)\b",
    r"\b(casual)\b",
    r"\b(temporary|fixed[-\s]?term)\b",
    r"\b(contract)\b",
]

LOCATION_PATTERNS = [
    r"\b(location|based\s+at|work\s*location)\s*[:\-]\s*(?P<loc>.+)",
]

SALARY_PATTERNS = [
    r"\b(salary|remuneration|package|band)\s*[:\-]\s*(?P<sal>.+)",
    r"\b(\$[0-9][\d,]*(?:\s*-\s*\$?[0-9][\d,]*)?)\b",
]

BAND_PATTERNS = [
    r"\bband\s*([0-9]{1,2})\b",
    r"\b(grade|classification)\s*([0-9A-Z]{1,3})\b",
]


class ApplyNowAdapter:
    """
    Scrapes job listing pages and detail pages for ApplyNow portals.

    Typical listing root:
      https://{tenant}.applynow.net.au/

    Also works if you pass a deeper /Jobs or /ApplyNow listing URL.
    """

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers.update(HEADERS)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((requests.RequestException,))
    )
    def _get(self, url: str) -> requests.Response:
        resp = self.session.get(url, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp

    def _clean_text(self, node: Optional[Tag]) -> str:
        if not node:
            return ""
        return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

    def _parse_date_soft(self, text: str) -> Optional[str]:
        text = (text or "").strip()
        if not text:
            return None
        # Try direct parse
        try:
            dt = dateparser.parse(text, dayfirst=True, fuzzy=True)
            if dt:
                return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
        # Try pattern-led parse (e.g. "Closes: 12 Nov 2025")
        for patt in DATE_PATTERNS:
            m = re.search(patt, text, flags=re.I)
            if m and m.groupdict().get("date"):
                try:
                    dt = dateparser.parse(m.group("date"), dayfirst=True, fuzzy=True)
                    if dt:
                        return dt.strftime("%Y-%m-%d")
                except Exception:
                    continue
        return None

    def _extract_with_patterns(self, text: str, patterns: list[str]) -> Optional[str]:
        t = (text or "")
        for patt in patterns:
            m = re.search(patt, t, flags=re.I)
            if m:
                # prefer named groups 'loc'/'sal', else full match
                gd = m.groupdict() if hasattr(m, "groupdict") else {}
                for key in ("loc", "sal"):
                    if key in gd and gd[key]:
                        return gd[key].strip()
                return m.group(0).strip()
        return None

    def _find_listing_cards(self, soup: BeautifulSoup) -> list[Tag]:
        """
        ApplyNow variants differ, so try a few resilient selectors.
        """
        cards = []
        # Common patterns we’ve seen on ApplyNow tenants:
        selectors = [
            # UL/LI list with anchors
            "ul li a[href*='applynow'], ul li a[href*='/jobs/'], ul li a[href*='/job/']",
            # Card layouts
            "article a[href*='applynow'], .job-list a[href*='applynow'], .job a[href*='applynow']",
            # Generic anchors inside obvious containers
            "a[href*='applynow.net.au/']",
            "a[href*='/applynow/']",
        ]
        for sel in selectors:
            for a in soup.select(sel):
                if isinstance(a, Tag) and a.get("href"):
                    cards.append(a)
            if cards:
                break
        # Deduplicate by href
        seen = set()
        uniq = []
        for a in cards:
            href = a.get("href")
            if href and href not in seen:
                seen.add(href)
                uniq.append(a)
        return uniq

    def _guess_employment_type(self, blob: str) -> Optional[str]:
        val = self._extract_with_patterns(blob, EMPLOYMENT_PATTERNS)
        return val.title() if val else None

    def _guess_location(self, blob: str) -> Optional[str]:
        val = self._extract_with_patterns(blob, LOCATION_PATTERNS)
        if val:
            return re.sub(r"(?i)\b(location|based at|work location)\s*[:\-]\s*", "", val).strip()
        return None

    def _guess_salary(self, blob: str) -> Optional[str]:
        val = self._extract_with_patterns(blob, SALARY_PATTERNS)
        return val

    def _guess_band(self, blob: str) -> Optional[str]:
        for patt in BAND_PATTERNS:
            m = re.search(patt, blob, flags=re.I)
            if m:
                return m.group(0).strip()
        return None

    def _best_absolute(self, base: str, href: str) -> str:
        # Some tenants return relative paths (/applynow/XXXX); some return absolute.
        return urljoin(base, href)

    def _detail_selectors(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Try to consolidate textual fields from detail page into a single blob
        and pull HTML of the main description.
        """
        # Likely content containers
        candidates = [
            "main",
            "article",
            ".content",
            ".container",
            "#content",
            ".job, .job-details, .job-detail",
        ]
        html_node = None
        for sel in candidates:
            node = soup.select_one(sel)
            if node and self._clean_text(node):
                html_node = node
                break
        if not html_node:
            html_node = soup.body or soup

        blob = self._clean_text(html_node)
        return {"blob": blob, "html": str(html_node)}

    def _extract_dates_from_detail(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:
        # Scan common label/value rows for dates
        text = self._clean_text(soup)
        posted = None
        closing = None

        # Look for explicit labels first
        labels = soup.select("dl, .key-details, .job-meta, .job-summary, table")
        hay = " ".join(self._clean_text(n) for n in labels) if labels else text
        # Generic date parsing from combined meta
        # Try to find 'Closing' / 'Closes'
        closing = self._parse_date_soft(hay)
        # If we accidentally grabbed a posted date as 'closing', try a second pass:
        if closing and re.search(r"posted|advertised", hay, flags=re.I):
            # Try to pull a second date occurrence for posted
            # (best-effort, ApplyNow often only shows one of these)
            try:
                dates = list(dateparser.parse(h, fuzzy=True, dayfirst=True)
                             for h in re.findall(r"([0-9]{1,2}\s+\w+\s+[0-9]{4}|\w+\s+[0-9]{1,2},?\s+[0-9]{4})", hay))
                if len(dates) >= 2:
                    posted = dates[0].strftime("%Y-%m-%d")
                    closing = dates[1].strftime("%Y-%m-%d")
            except Exception:
                pass

        # As a fallback, leave posted/closing None (pipeline can accept)
        return {"posted": posted, "closing": closing}

    def _parse_detail_page(self, url: str) -> Dict[str, Optional[str]]:
        resp = self._get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        meta = self._detail_selectors(soup)
        date_meta = self._extract_dates_from_detail(soup)

        blob = meta["blob"]
        return {
            "description_html": meta["html"],
            "employment_type": self._guess_employment_type(blob),
            "work_arrangement": None,  # Typically not explicit on ApplyNow
            "location": self._guess_location(blob),
            "salary": self._guess_salary(blob),
            "band": self._guess_band(blob),
            "posted_date": date_meta.get("posted"),
            "closing_date": date_meta.get("closing"),
        }

    def _paginate(self, base_url: str, soup: BeautifulSoup) -> Optional[str]:
        """
        Try to discover a 'Next' pagination link. ApplyNow variants differ;
        handle both rel=next and 'Next' anchor text, plus numeric pagers.
        """
        # rel=next
        for a in soup.select("a[rel='next']"):
            href = a.get("href")
            if href:
                return urljoin(base_url, href)

        # text-based
        for a in soup.find_all("a"):
            if self._clean_text(a).lower() in {"next", "older", "more jobs"}:
                href = a.get("href")
                if href:
                    return urljoin(base_url, href)

        # numeric pager: find current page and pick next sibling anchor
        pager = soup.select_one(".pagination, .pager, .pages")
        if pager:
            current = pager.select_one(".active, .current")
            if current and current.find_next("a"):
                return urljoin(base_url, current.find_next("a").get("href"))
            # else, find the highest number and try +1 (rare)
        return None

    def scrape(self, start_url: str) -> List[Dict]:
        """
        Entry point. Accepts the careers root (e.g. https://macedon-ranges-ext-shire-portal.applynow.net.au/)
        and walks listings (with simple pagination) collecting job records.
        """
        results: List[Dict] = []
        next_url = start_url

        visited = set()
        while next_url and next_url not in visited:
            visited.add(next_url)
            resp = self._get(next_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            cards = self._find_listing_cards(soup)
            for a in cards:
                title = self._clean_text(a)
                href = a.get("href", "").strip()
                if not href:
                    continue
                link = self._best_absolute(next_url, href)

                # Basic guard: only follow into same tenant/domain or clear job path
                if "applynow.net.au" not in urlparse(link).netloc and "/applynow/" not in link and "/job" not in link:
                    continue

                detail = {}
                try:
                    detail = self._parse_detail_page(link)
                    # Be kind to hosts
                    time.sleep(0.5)
                except Exception:
                    # still return a minimal record
                    detail = {
                        "description_html": None,
                        "employment_type": None,
                        "work_arrangement": None,
                        "location": None,
                        "salary": None,
                        "band": None,
                        "posted_date": None,
                        "closing_date": None,
                    }

                results.append({
                    "title": title or None,
                    "link": link,
                    "posted_date": detail.get("posted_date"),
                    "closing_date": detail.get("closing_date"),
                    "salary": detail.get("salary"),
                    "band": detail.get("band"),
                    "employment_type": detail.get("employment_type"),
                    "work_arrangement": detail.get("work_arrangement"),
                    "location": detail.get("location"),
                    "description_html": detail.get("description_html"),
                })

            # Try next page
            next_url = self._paginate(next_url, soup)

        return results
