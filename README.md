# Bandsight Feeds

Automated RSS feed hosted on GitHub Pages and built by GitHub Actions.

- Live feed (after Pages is enabled): `https://bandsight.github.io/feeds/feed.xml`
- Scraper config: `src/config.json`
- Scraper code: `src/scraper.py`

## Deploy
1) Upload this folder to your GitHub repo named **feeds** (public).
2) Settings → Pages → Deploy from branch: `main` / `/docs` → Save.
3) Actions → **Build RSS** → **Run workflow**.
