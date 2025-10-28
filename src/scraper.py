#!/usr/bin/env python3
import json, re, time, hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_FILE = REPO_ROOT / "src" / "config.json"
STATE_FILE = REPO_ROOT / "state.json"
OUTPUT_FEED = REPO_ROOT / "docs" / "feed.xml"

def load_config() -> Dict:
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    cfg.setdefault("max_items", 300)
    cfg.setdefault("user_agent", "BandsightRSSBot/1.0 (+contact: feeds@bandsight.example)")
    cfg.setdefault("request_timeout", 25)
    cfg.setdefault("sleep_between_requests_seconds", 1)
    return cfg

def load_state() -> Dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"seen": {}}

def save_state(state: Dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

def http_get(url: str, ua: str, timeout: int) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout)
        if r.status_code == 200 and r.text:
            return r.text
        return None
    except requests.RequestException:
        return None

def parse_relative_date(txt: str) -> Optional[datetime]:
    s = (txt or "").strip().lower()
    now = datetime.now(timezone.utc)
    m = re.search(r'(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago', s)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if "minute" in unit:
        return now - timedelta(minutes=n)
    if "hour" in unit:
        return now - timedelta(hours=n)
    if "day" in unit:
        return now - timedelta(days=n)
    return None

def parse_advertised_line(txt: str) -> Optional[datetime]:
    s = (txt or "").strip()
    s = re.sub(r'^\s*Advertised:\s*', '', s, flags=re.I)
    s = re.sub(r'\b(AEDT|AEST)\b', '', s).strip()
    fmts = ["%d %b %Y %I:%M %p", "%d %b %Y %H:%M", "%d %b %Y"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def absolute_url(page_url: str, href: str) -> str:
    if not href: return page_url
    if href.startswith(("http://","https://")): return href
    if href.startswith("//"): return "https:" + href
    from urllib.parse import urljoin
    return urljoin(page_url, href)

def text_or_none(el) -> Optional[str]:
    if el:
        t = el.get_text(strip=True)
        return t or None
    return None

def extract_jsonld_jobposting(html: str) -> Optional[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue
        blocks = data if isinstance(data, list) else [data]
        for b in blocks:
            if isinstance(b, dict) and b.get("@type") == "JobPosting":
                return b
            if isinstance(b, dict) and isinstance(b.get("@graph"), list):
                for g in b["@graph"]:
                    if isinstance(g, dict) and g.get("@type") == "JobPosting":
                        return g
    return None

def collect_from_listing(src: Dict, cfg: Dict) -> List[Dict]:
    html = http_get(src["url"], cfg["user_agent"], cfg["request_timeout"])
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    sels = src.get("selectors", {}).get("listing", {})
    container = sels.get("card") or sels.get("row") or "a"
    items = []
    for node in soup.select(container):
        a = node.select_one(sels.get("title", "a"))
        if not a: 
            continue
        href_attr = sels.get("href_attr","href")
        href = a.get(href_attr)
        title = text_or_none(a) or ""
        date_el = node.select_one(sels.get("date",""))
        sum_el  = node.select_one(sels.get("summary",""))
        items.append({
            "title": title.strip(),
            "url": absolute_url(src["url"], href or src["url"]),
            "summary": (text_or_none(sum_el) or "").strip(),
            "rel_date": (text_or_none(date_el) or "").strip(),
            "source": src["name"]
        })
    return items

def enrich_with_detail(item: Dict, src: Dict, cfg: Dict) -> Dict:
    detail_cfg = src.get("selectors", {}).get("detail", {})
    html = http_get(item["url"], cfg["user_agent"], cfg["request_timeout"])
    if not html:
        return item
    soup = BeautifulSoup(html, "html.parser")

    if detail_cfg.get("jsonld"):
        jp = extract_jsonld_jobposting(html)
        if jp and jp.get("datePosted"):
            item["posted_dt"] = jp["datePosted"]
            return item

    adv_sel = detail_cfg.get("advertised")
    if adv_sel:
        adv_el = soup.select_one(adv_sel)
        adv_txt = text_or_none(adv_el) or ""
        dt = parse_advertised_line(adv_txt) or parse_advertised_line(adv_el.get("datetime","") if adv_el else "")
        if dt:
            item["posted_dt"] = dt.isoformat()
    return item

def normalise_pubdate(item: Dict) -> datetime:
    if item.get("posted_dt"):
        try:
            return datetime.fromisoformat(item["posted_dt"].replace("Z","+00:00")).astimezone(timezone.utc)
        except Exception:
            pass
    rd = item.get("rel_date","")
    dt = parse_relative_date(rd)
    return dt or datetime.now(timezone.utc)

def hash_guid(*parts: str) -> str:
    return hashlib.sha1(("||".join(p or "" for p in parts)).encode("utf-8")).hexdigest()

def build_rss(cfg: Dict, items: List[Dict], seen: Dict[str, Dict]) -> str:
    from xml.sax.saxutils import escape
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    head = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>{escape(cfg["feed_title"])}</title>
  <link>{escape(cfg["feed_link"])}</link>
  <description>{escape(cfg["feed_description"])}</description>
  <language>en-au</language>
  <lastBuildDate>{now}</lastBuildDate>
"""
    body = []
    for it in items[: cfg["max_items"] ]:
        guid = hash_guid(it.get("source",""), it.get("url",""), it.get("title",""))
        it["guid"] = guid
        if guid not in seen:
            seen[guid] = {"first_seen": datetime.utcnow().isoformat(timespec='seconds') + "Z"}
        title = escape(it.get("title",""))
        link  = escape(it.get("url",""))
        desc  = escape(it.get("summary","") or it.get("source",""))
        pub   = normalise_pubdate(it).strftime("%a, %d %b %Y %H:%M:%S +0000")
        cat   = escape(it.get("source",""))
        body.append(f"""  <item>
    <title>{title}</title>
    <link>{link}</link>
    <guid isPermaLink="false">{guid}</guid>
    <pubDate>{pub}</pubDate>
    <description>{desc}</description>
    <category>{cat}</category>
  </item>
""")
    tail = "</channel>\n</rss>\n"
    return head + "".join(body) + tail

def main():
    cfg = load_config()
    state = load_state()
    seen = state.get("seen", {})
    all_items = []

    for src in cfg.get("sources", []):
        items = collect_from_listing(src, cfg)
        if src.get("strategy") == "listing_plus_detail":
            enriched = []
            for it in items:
                enriched.append(enrich_with_detail(it, src, cfg))
                time.sleep(cfg["sleep_between_requests_seconds"])
            items = enriched
        all_items.extend(items)
        time.sleep(cfg["sleep_between_requests_seconds"])

    all_items.sort(key=lambda i: normalise_pubdate(i), reverse=True)
    xml = build_rss(cfg, all_items, seen)

    OUTPUT_FEED.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FEED.write_text(xml, encoding="utf-8")

    if len(seen) > 3000:
        ordered = sorted(seen.items(), key=lambda kv: kv[1]["first_seen"], reverse=True)[:3000]
        seen = dict(ordered)
    state["seen"] = seen
    save_state(state)
    print(f"Wrote {OUTPUT_FEED} with {min(len(all_items), cfg['max_items'])} items.")

if __name__ == "__main__":
    main()
