# src/collectors/pulse_api.py
import requests
from urllib.parse import urljoin, quote
from bs4 import BeautifulSoup

def dedupe_by_link(rows):
    seen = set()
    out = []
    for r in rows:
        if r["link"] in seen:
            continue
        seen.add(r["link"])
        out.append(r)
    return out

def guess_band(title: str) -> str:
    tl = title.lower()
    for b in range(1, 9):
        if f"band {b}" in tl:
            return f"Band {b}"
    return "Unknown"

def collect_pulse_api(start_url: str, council_name: str) -> list[dict]:
    """
    Use the Pulse JSON API to get job listings for the council.
    """
    # Derive WebServices base
    root = start_url.split("/Pulse")[0]
    api_root = urljoin(root + "/", "WebServices/")
    api_jobs_url = urljoin(api_root, "RCM/Jobs/Jobs")

    params = {
        "internalOnly": "false",
        "workArrangement": "",
        "employmentType": ""
    }
    headers = {
        "Accept": "application/json",
        "Referer": start_url,
        "User-Agent": "BandsightPulseCollector/1.0"
    }

    resp = requests.get(api_jobs_url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    items = []
    for row in data.get("Jobs", []):
        ji = row.get("JobInfo", {}) or {}
        title = ji.get("Title", "").strip()
        link_id = row.get("LinkId") or ""
        slug = "-".join(title.split()).replace("/", "-").replace("&", "and")
        detail_link = f"{root}/Pulse/job/{link_id}/{quote(slug)}?source=public"

        items.append({
            "title": title,
            "link": detail_link,
            "description": "",  # optional: fetch detail page if you like
            "category": guess_band(title),
            "salary": ji.get("Compensation", "").strip(),
            "closing": ji.get("ClosingDate", "").strip(),
            "council": council_name,
            "location": ji.get("Location", "").strip(),
            "employment_type": ji.get("EmploymentType", "").strip(),
            "work_arrangement": ji.get("WorkArrangement", "").strip(),
            "department": ji.get("Department", "").strip(),
        })

    return dedupe_by_link(items)
