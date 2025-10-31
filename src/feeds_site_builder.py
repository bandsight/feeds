#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
feeds_site_builder.py
Builds an RSS 2.0 feed from a JSONL history produced by scraper.py

Usage:
  python src/feeds_site_builder.py --in data/jobs_history.jsonl --out feeds/feed.xml --days 45
"""

import argparse
import datetime as dt
import hashlib
import html
import json
from email.utils import formatdate
from pathlib import Path
from typing import Dict, Iterable, List, Optional

AUS_TZ = dt.timezone(dt.timedelta(hours=11))  # Melbourne AEDT

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input JSONL (scraper output)")
    ap.add_argument("--out", dest="outp", required=True, help="Output RSS XML path")
    ap.add_argument("--title", default="Bandsight – Victorian Council Jobs Feed")
    ap.add_argument("--link", default="https://bandsight.github.io/feeds/feed.xml")
    ap.add_argument("--desc", default="Automatically updated jobs feed by Bandsight.")
    ap.add_argument("--max_items", type=int, default=300)
    ap.add_argument("--days", type=int, default=45, help="Only include jobs within last N days")
    return ap.parse_args()

def read_jsonl(path: Path) -> Iterable[Dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def within_window(rec: Dict, days: int) -> bool:
    if days <= 0:
        return True
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(days=days)
    pd = rec.get("posted_date")
    if pd:
        try:
            d = dt.datetime.fromisoformat(pd + "T00:00:00+11:00").astimezone(dt.UTC)
            return d >= cutoff
        except Exception:
            pass
    sd = rec.get("scrape_date")
    if sd:
        try:
            d = dt.datetime.fromisoformat(sd)
            if d.tzinfo is None:
                d = d.replace(tzinfo=AUS_TZ)
            d = d.astimezone(dt.UTC)
            return d >= cutoff
        except Exception:
            pass
    return True

def sanitize_text(x: Optional[str]) -> str:
    return "" if not x else html.escape(x, quote=False)

def item_guid(rec: Dict) -> str:
    base = (rec.get("council") or "") + "|" + (rec.get("link") or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def as_rfc2822(dt_utc: dt.datetime) -> str:
    return formatdate(dt_utc.timestamp(), usegmt=True)

def pubdate_for(rec: Dict) -> str:
    pd = rec.get("posted_date")
    if pd:
        try:
            d = dt.datetime.fromisoformat(pd + "T00:00:00+11:00").astimezone(dt.UTC)
            return as_rfc2822(d)
        except Exception:
            pass
    sd = rec.get("scrape_date")
    if sd:
        try:
            d = dt.datetime.fromisoformat(sd)
            if d.tzinfo is None:
                d = d.replace(tzinfo=AUS_TZ)
            return as_rfc2822(d.astimezone(dt.UTC))
        except Exception:
            pass
    return as_rfc2822(dt.datetime.now(dt.UTC))

def build(items: List[Dict], title: str, link: str, desc: str, max_items: int) -> str:
    now = as_rfc2822(dt.datetime.now(dt.UTC))
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0">',
        "<channel>",
        f"<title>{sanitize_text(title)}</title>",
        f"<link>{sanitize_text(link)}</link>",
        f"<description>{sanitize_text(desc)}</description>",
        "<language>en-au</language>",
        f"<lastBuildDate>{now}</lastBuildDate>",
    ]

    items_sorted = sorted(
        items,
        key=lambda r: (r.get("posted_date") or r.get("scrape_date") or ""),
        reverse=True,
    )
    for rec in items_sorted[:max_items]:
        council = rec.get("council") or ""
        title = rec.get("title") or "(untitled)"
        link = rec.get("link") or ""
        band = rec.get("band") or ""
        salary = rec.get("salary") or ""
        closing = rec.get("closing_date") or ""
        desc_html = rec.get("description_html") or ""
        summary = desc_html or sanitize_text(" — ".join([p for p in [council, salary, band] if p]).strip(" —"))

        parts += [
            "<item>",
            f"<title>{sanitize_text(title)}</title>",
            f"<link>{sanitize_text(link)}</link>",
            f"<guid isPermaLink=\"false\">{item_guid(rec)}</guid>",
            f"<pubDate>{pubdate_for(rec)}</pubDate>",
        ]
        if band:
            parts.append(f"<category>{sanitize_text(band)}</category>")
        if salary:
            parts.append(f"<salary>{sanitize_text(salary)}</salary>")
        if closing:
            parts.append(f"<closing>{sanitize_text(closing)}</closing>")
        if council:
            parts.append(f"<council>{sanitize_text(council)}</council>")
        parts.append(f"<description>{sanitize_text(summary[:4000])}</description>")
        parts.append("</item>")

    parts += [f"<lastBuildDate>{now}</lastBuildDate>", "</channel>", "</rss>"]
    return "\n".join(parts)

def main():
    args = parse_args()
    rows = [r for r in read_jsonl(Path(args.inp)) if within_window(r, args.days)]
    xml = build(rows, args.title, args.link, args.desc, args.max_items)
    Path(args.outp).write_text(xml, encoding="utf-8")
    print(f"Wrote RSS with {min(len(rows), args.max_items)} items to {args.outp}")

if __name__ == "__main__":
    main()
