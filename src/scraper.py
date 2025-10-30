import re
import requests
import yaml
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

BASE_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = BASE_DIR / "docs"
DATA_DIR = BASE_DIR / "data"
RSS_FILE = DOCS_DIR / "feed.xml"
COUNCIL_REGISTRY = DATA_DIR / "councils.yaml"

def load_registry():
    return yaml.safe_load(COUNCIL_REGISTRY.read_text(encoding="utf-8"))["councils"]

def fetch(url):
    headers = {"User-Agent": "BandsightJobScraper/1.0"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text

def clean(text):
    return re.sub(r"\s+", " ", text.strip())

def collect_generic(url):
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = clean(link.get_text())
        if any(x in href.lower() for x in ["job", "vacanc", "career"]) and text:
            jobs.append({
                "title": text,
                "link": requests.compat.urljoin(url, href),
                "description": "Auto-collected listing from council careers page.",
                "category": "General",
                "salary": "",
                "closing": "",
                "council": "Unknown"
            })
    return jobs

def collect_pageup(url):
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    for li in soup.select("li.job"):
        title = clean(li.get_text())
        a = li.find("a", href=True)
        if a:
            link = requests.compat.urljoin(url, a["href"])
            jobs.append({
                "title": title,
                "link": link,
                "description": "PageUp job listing",
                "category": "Unknown",
                "salary": "",
                "closing": "",
                "council": "City of Greater Geelong"
            })
    return jobs

def collect_pulse(url):
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    for row in soup.select("div.job-item"):
        title = clean(row.get_text())
        a = row.find("a", href=True)
        if a:
            jobs.append({
                "title": title,
                "link": requests.compat.urljoin(url, a["href"]),
                "description": "Pulse Software job",
                "category": "Unknown",
                "salary": "",
                "closing": "",
                "council": "City of Ballarat"
            })
    return jobs

VENDOR_COLLECTORS = {
    "generic": collect_generic,
    "pageup": collect_pageup,
    "pulse": collect_pulse,
}

def append_to_rss(items):
    if RSS_FILE.exists():
        tree = ET.parse(RSS_FILE)
        root = tree.getroot()
        channel = root.find("channel")
    else:
        rss = ET.Element("rss", version="2.0")
        channel = ET.SubElement(rss, "channel")
        ET.SubElement(channel, "title").text = "Bandsight – Victorian Council Jobs Feed"
        ET.SubElement(channel, "link").text = "https://bandsight.github.io/feeds/feed.xml"
        ET.SubElement(channel, "description").text = "Automatic jobs feed by Bandsight."
        ET.SubElement(channel, "language").text = "en-au"
        root = rss

    ET.SubElement(channel, "lastBuildDate").text = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")

    for job in items:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = job["title"]
        ET.SubElement(item, "link").text = job["link"]
        ET.SubElement(item, "guid", isPermaLink="false").text = str(hash(job["link"]))
        ET.SubElement(item, "pubDate").text = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
        ET.SubElement(item, "description").text = job["description"]
        ET.SubElement(item, "category").text = job["category"]
        ET.SubElement(item, "salary").text = job["salary"]
        ET.SubElement(item, "closing").text = job["closing"]
        ET.SubElement(item, "council").text = job.get("council", "")

    ET.ElementTree(root).write(RSS_FILE, encoding="utf-8", xml_declaration=True)

def main():
    registry = load_registry()
    all_items = []
    for c in registry:
        if not c.get("active"):
            continue
        vendor = c.get("vendor")
        for start_url in c.get("starts", []):
            collector = VENDOR_COLLECTORS.get(vendor, collect_generic)
            try:
                results = collector(start_url)
                for job in results:
                    job["council"] = c["name"]
                all_items.extend(results)
                print(f"✅ {c['name']}: {len(results)} jobs")
            except Exception as e:
                print(f"❌ {c['name']} failed: {e}")
    append_to_rss(all_items)

if __name__ == "__main__":
    main()
