# src/scraper.py (snippet)
from collectors.pulse_api import collect_pulse_api

VENDOR_COLLECTORS = {
    "generic": collect_generic,
    "pageup": collect_pageup,
    "applynow": collect_applynow,
    "pulse_api": collect_pulse_api,
}

def main():
    registry = load_registry()
    all_items = []
    for c in registry:
        if not c.get("active"):
            continue
        vendor = c["vendor"]
        collector = VENDOR_COLLECTORS.get(vendor)
        if not collector:
            print(f"[warn] No collector for vendor '{vendor}' in {c['name']}")
            continue
        for start_url in c.get("starts", []):
            print(f"→ Scanning {c['name']} ({vendor}) @ {start_url}")
            try:
                results = collector(start_url, c["name"])
                print(f"← Found {len(results)} items for {c['name']}")
                all_items.extend(results)
            except Exception as e:
                print(f"[error] {c['name']} (@{start_url}): {e}")
    append_to_rss(all_items)
