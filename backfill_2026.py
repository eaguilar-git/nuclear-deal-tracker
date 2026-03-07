"""
backfill_2026.py
─────────────────
One-time backfill: scrapes nuclear deal news from Jan 1 - today 2026
using Google News RSS (date-filtered) + the same Claude extraction
pipeline as the daily scraper.

Run once, then delete or ignore. The daily scraper takes over afterward.
"""

import os
import json
import hashlib
import datetime
import time
import feedparser
import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials

# CONFIG
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEAL_KEYWORDS = [
    "agreement", "contract", "mou", "memorandum", "partnership", "deal",
    "signed", "award", "license", "permit", "approval", "deployment",
    "construction", "build", "order", "purchase", "invest", "fund",
    "smr", "small modular reactor", "advanced reactor", "nuclear plant",
    "natrium", "xe-100", "bwrx-300", "aurora", "evinci", "kairos",
    "terrestrial", "nuscale", "last energy", "radiant", "oklo",
    "haleu", "enrichment", "fuel supply", "power purchase",
    "utility", "offtake", "data center", "hyperscaler", "microsoft",
    "amazon", "google", "meta", "ai", "industrial heat",
]

QUERIES = [
    "nuclear reactor deal agreement 2026",
    "small modular reactor SMR contract 2026",
    "nuclear power plant construction agreement 2026",
    "advanced nuclear energy partnership 2026",
    "nuclear energy data center agreement 2026",
    "TerraPower Natrium 2026",
    "X-energy Xe-100 2026",
    "GE Hitachi BWRX-300 2026",
    "Oklo Aurora reactor 2026",
    "Kairos Power 2026",
    "NuScale Power 2026",
    "nuclear fuel HALEU agreement 2026",
    "nuclear power purchase agreement 2026",
    "nuclear energy investment fund 2026",
    "nuclear reactor license approval 2026",
]

SYSTEM_PROMPT = """You are an expert analyst tracking nuclear energy deals and agreements.
Extract structured deal data from the article text provided.

If this article describes a concrete nuclear energy deal, agreement, MOU, contract,
partnership, investment, license, permit, or deployment announcement, extract the details.

Respond ONLY with a JSON object. No explanation, no markdown.

JSON schema:
{
  "is_deal": true/false,
  "company": "primary company/developer (string)",
  "reactor": "reactor type/model if mentioned, else null",
  "project": "project name or location if mentioned, else null",
  "location": "city/state/country if mentioned, else null",
  "partners": "other parties involved (string or null)",
  "deal": "one-sentence description of what was agreed/announced",
  "significance": "Deployment" | "Market development" | "Signaling",
  "summary": "2-3 sentence summary of the deal and its importance"
}

Significance definitions:
- Deployment: Physical construction, site prep, concrete commitments with timeline/funding
- Market development: PPAs, offtake agreements, fuel supply deals, licensing milestones
- Signaling: MOUs, LOIs, feasibility studies, expressions of interest

If the article does NOT describe a nuclear deal, return: {"is_deal": false}"""


def build_gnews_url(query):
    import urllib.parse
    q = urllib.parse.quote(query)
    return (
        f"https://news.google.com/rss/search?q={q}"
        f"+after:2025-12-31+before:2026-12-31"
        f"&hl=en-US&gl=US&ceid=US:en"
    )


def fetch_article_text(url, timeout=10):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; NuclearScraper/1.0)"}
        resp = requests.get(url, headers=headers, timeout=timeout)
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "aside", "header"]):
            tag.decompose()
        article = soup.find("article") or soup.find(class_=["article", "content", "post"])
        text = article.get_text(separator=" ", strip=True) if article else soup.get_text(separator=" ", strip=True)
        return text[:4000]
    except Exception:
        return ""


def is_relevant(title, summary):
    text = (title + " " + summary).lower()
    return any(kw in text for kw in DEAL_KEYWORDS)


def extract_deal(client, title, body):
    prompt = f"Article title: {title}\n\nArticle text:\n{body}"
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        data = json.loads(text)
        return data if data.get("is_deal") else None
    except Exception as e:
        print(f"  Claude extraction error: {e}")
        return None


def article_id(url):
    return hashlib.md5(url.encode()).hexdigest()[:12]


def connect_sheet():
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]
    creds_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


def load_seen_ids(sh):
    try:
        seen_ws = sh.worksheet("Seen")
        vals = seen_ws.col_values(1)
        return set(vals[1:])
    except Exception:
        return set()


def mark_seen(sh, ids):
    try:
        seen_ws = sh.worksheet("Seen")
    except Exception:
        seen_ws = sh.add_worksheet("Seen", rows=10000, cols=2)
        seen_ws.append_row(["id", "scraped_at"])
    now = datetime.datetime.utcnow().isoformat()
    rows = [[i, now] for i in ids]
    if rows:
        seen_ws.append_rows(rows)


def append_deals(sh, rows):
    try:
        deals_ws = sh.worksheet("Deals")
    except Exception:
        deals_ws = sh.add_worksheet("Deals", rows=10000, cols=13)
        deals_ws.append_row([
            "id", "date", "company", "reactor", "project",
            "location", "partners", "deal", "significance",
            "summary", "source", "scraped_at"
        ])
    if rows:
        deals_ws.append_rows(rows)
        print(f"  Appended {len(rows)} new deals to sheet")


def main():
    print("=" * 60)
    print("Nuclear Deal Backfill - 2026")
    print("=" * 60)

    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    sh = connect_sheet()
    seen_ids = load_seen_ids(sh)
    print(f"Already seen: {len(seen_ids)} articles")

    all_articles = {}

    print(f"\nFetching {len(QUERIES)} Google News queries...")
    for query in QUERIES:
        url = build_gnews_url(query)
        try:
            feed = feedparser.parse(url)
            count = 0
            for entry in feed.entries:
                link = entry.get("link", "")
                if not link or link in all_articles:
                    continue
                pub = entry.get("published", "")
                all_articles[link] = {
                    "title": entry.get("title", ""),
                    "date": pub,
                    "summary": entry.get("summary", ""),
                }
                count += 1
            print(f"  [{count:3d} new] {query[:55]}")
            time.sleep(0.5)
        except Exception as e:
            print(f"  [ERROR] {query[:55]}: {e}")

    print(f"\nTotal unique articles: {len(all_articles)}")

    candidates = {
        url: art for url, art in all_articles.items()
        if is_relevant(art["title"], art["summary"])
    }
    print(f"After keyword filter: {len(candidates)}")

    new_candidates = {
        url: art for url, art in candidates.items()
        if article_id(url) not in seen_ids
    }
    print(f"After dedup: {len(new_candidates)} new to process")

    if not new_candidates:
        print("Nothing new. Done.")
        return

    new_deal_rows = []
    new_seen_ids = []
    scraped_at = datetime.datetime.utcnow().isoformat()
    batch_size = 20

    print(f"\nProcessing {len(new_candidates)} articles...\n")
    for i, (url, art) in enumerate(new_candidates.items(), 1):
        aid = article_id(url)
        title = art["title"]
        print(f"[{i:3d}/{len(new_candidates)}] {title[:70]}")

        body = fetch_article_text(url)
        if not body:
            print("         skipping (no body)")
            new_seen_ids.append(aid)
            continue

        deal = extract_deal(anthropic_client, title, body)
        new_seen_ids.append(aid)

        if deal:
            raw_date = art["date"]
            try:
                parsed = feedparser._parse_date(raw_date)
                date_str = f"{parsed.tm_year}-{parsed.tm_mon:02d}-{parsed.tm_mday:02d}" if parsed else raw_date[:10]
            except Exception:
                date_str = raw_date[:10] if raw_date else ""

            row = [
                aid, date_str,
                deal.get("company", ""), deal.get("reactor") or "",
                deal.get("project") or "", deal.get("location") or "",
                deal.get("partners") or "", deal.get("deal", ""),
                deal.get("significance", ""), deal.get("summary", ""),
                url, scraped_at,
            ]
            new_deal_rows.append(row)
            print(f"         DEAL: {deal.get('company','')} - {deal.get('deal','')[:60]}")
        else:
            print(f"         not a deal")

        if len(new_deal_rows) > 0 and len(new_deal_rows) % batch_size == 0:
            append_deals(sh, new_deal_rows[-batch_size:])
            mark_seen(sh, new_seen_ids[-batch_size:])

        time.sleep(0.3)

    remainder = len(new_deal_rows) % batch_size
    if remainder:
        append_deals(sh, new_deal_rows[-remainder:])

    mark_seen(sh, new_seen_ids)

    print(f"\n{'='*60}")
    print(f"Backfill complete.")
    print(f"  Articles processed : {len(new_candidates)}")
    print(f"  Deals found        : {len(new_deal_rows)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
