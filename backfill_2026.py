"""
backfill_2026.py
─────────────────
One-time backfill: scrapes nuclear deal news from Jan 1 - today 2026
using Google News RSS + Claude extraction.

Compatible with updated scraper schema.

Run once, then disable. The daily scraper takes over afterward.
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


# ───────────────── CONFIG ─────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

DEAL_COLUMNS = [
    "id",
    "date",
    "company",
    "reactor",
    "project",
    "location",
    "country",
    "developer_country",
    "partners",
    "deal",
    "significance",
    "reactor_capacity_mw",
    "deployment_year",
    "summary",
    "source",
    "scraped_at",
]

DEAL_KEYWORDS = [
    "agreement","contract","mou","memorandum","partnership","deal",
    "signed","award","license","permit","approval","deployment",
    "construction","build","order","purchase","invest","fund",
    "smr","small modular reactor","advanced reactor","nuclear plant",
    "natrium","xe-100","bwrx-300","aurora","evinci","kairos",
    "nuscale","last energy","radiant","oklo",
    "haleu","fuel supply","power purchase","ppa",
    "offtake","data center","hyperscaler",
    "microsoft","amazon","google","meta","ai","industrial heat",
]

QUERIES = [
    "nuclear reactor deal agreement 2026",
    "small modular reactor contract 2026",
    "advanced nuclear partnership 2026",
    "nuclear data center power agreement 2026",
    "TerraPower Natrium reactor project 2026",
    "X-energy Xe-100 reactor 2026",
    "GE Hitachi BWRX-300 reactor project",
    "Oklo Aurora reactor project",
    "Kairos Power reactor project",
    "NuScale SMR deployment",
    "nuclear fuel HALEU agreement",
    "nuclear power purchase agreement",
]


SYSTEM_PROMPT = """
You are an expert analyst tracking nuclear energy deals and agreements.

Extract structured deal data from the article.

If the article describes a nuclear energy agreement, contract,
partnership, investment, licensing milestone, reactor order,
or deployment announcement, extract details.

Return ONLY JSON.

Schema:

{
  "is_deal": true/false,
  "company": "...",
  "reactor": "...",
  "project": "...",
  "location": "...",
  "partners": "...",
  "deal": "...",
  "significance": "...",
  "summary": "..."
}

Significance:

Deployment
  construction, reactor order, financial close

Market development
  licensing milestones, contracts, supply agreements

Signaling
  MOUs, LOIs, feasibility studies
"""


# ───────────────── HELPERS ─────────────────


def article_id(url):
    return hashlib.md5(url.encode()).hexdigest()[:12]


def build_gnews_url(query):
    import urllib.parse
    q = urllib.parse.quote(query)

    return (
        f"https://news.google.com/rss/search?q={q}"
        f"+after:2025-12-31+before:2026-12-31"
        f"&hl=en-US&gl=US&ceid=US:en"
    )


def clean(text):
    if not text:
        return ""
    return " ".join(text.split())


def is_relevant(title, summary):
    text = (title + " " + summary).lower()
    return any(k in text for k in DEAL_KEYWORDS)


def fetch_article_text(url):

    try:
        headers = {"User-Agent": "Mozilla/5.0"}

        r = requests.get(url, headers=headers, timeout=15)

        soup = BeautifulSoup(r.text, "html.parser")

        for tag in soup(["script","style","nav","footer","header","aside"]):
            tag.decompose()

        article = (
            soup.find("article")
            or soup.find("main")
            or soup.body
        )

        text = article.get_text(separator=" ", strip=True)

        text = clean(text)

        return text[:6500]

    except Exception:
        return ""


def parse_date(raw):

    if not raw:
        return ""

    try:
        parsed = feedparser._parse_date(raw)

        if parsed:
            return f"{parsed.tm_year}-{parsed.tm_mon:02d}-{parsed.tm_mday:02d}"

    except Exception:
        pass

    return raw[:10]


# ───────────────── CLAUDE EXTRACTION ─────────────────


def extract_deal(client, title, body):

    prompt = f"Article title: {title}\n\nArticle text:\n{body}"

    try:

        resp = client.messages.create(
            model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514"),
            max_tokens=900,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        text = resp.content[0].text.strip()

        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        data = json.loads(text)

        if data.get("is_deal"):
            return data

        return None

    except Exception as e:

        print("   Claude extraction error:", e)

        return None


# ───────────────── GOOGLE SHEETS ─────────────────


def connect_sheet():

    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]

    creds_info = json.loads(creds_json)

    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=SCOPES
    )

    gc = gspread.authorize(creds)

    return gc.open_by_key(sheet_id)


def load_seen_ids(sh):

    try:

        ws = sh.worksheet("Seen")

        vals = ws.col_values(1)

        return set(vals[1:])

    except Exception:

        return set()


def mark_seen(sh, ids):

    try:

        ws = sh.worksheet("Seen")

    except Exception:

        ws = sh.add_worksheet("Seen", rows=10000, cols=2)

        ws.append_row(["id","scraped_at"])

    now = datetime.datetime.utcnow().isoformat()

    rows = [[i,now] for i in ids]

    ws.append_rows(rows)


def append_deals(sh, rows):

    try:

        ws = sh.worksheet("Deals")

    except Exception:

        ws = sh.add_worksheet("Deals", rows=10000, cols=len(DEAL_COLUMNS))

        ws.append_row(DEAL_COLUMNS)

    ws.append_rows(rows)


# ───────────────── MAIN ─────────────────


def main():

    print("="*60)
    print("Nuclear Deal Backfill 2026")
    print("="*60)

    client = anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"]
    )

    sh = connect_sheet()

    seen = load_seen_ids(sh)

    print("Seen articles:", len(seen))

    articles = {}

    print("\nFetching Google News feeds...")

    for query in QUERIES:

        url = build_gnews_url(query)

        feed = feedparser.parse(url)

        new_count = 0

        for entry in feed.entries:

            link = entry.get("link")

            if not link:
                continue

            if link in articles:
                continue

            articles[link] = {
                "title": entry.get("title",""),
                "summary": entry.get("summary",""),
                "date": entry.get("published","")
            }

            new_count += 1

        print(f"[{new_count:3d}] {query}")

        time.sleep(0.5)

    print("\nTotal articles:", len(articles))

    candidates = {
        url:a for url,a in articles.items()
        if is_relevant(a["title"],a["summary"])
    }

    print("After keyword filter:", len(candidates))

    new_articles = {
        url:a for url,a in candidates.items()
        if article_id(url) not in seen
    }

    print("After dedup:", len(new_articles))

    if not new_articles:
        print("Nothing to process")
        return

    rows = []
    new_ids = []

    scraped_at = datetime.datetime.utcnow().isoformat()

    for i,(url,a) in enumerate(new_articles.items(),1):

        title = a["title"]

        print(f"[{i}/{len(new_articles)}] {title[:70]}")

        body = fetch_article_text(url)

        if not body:
            print("   skipping (no body)")
            new_ids.append(article_id(url))
            continue

        deal = extract_deal(client,title,body)

        new_ids.append(article_id(url))

        if deal:

            row = [
                article_id(url),
                parse_date(a["date"]),
                deal.get("company","unknown"),
                deal.get("reactor","unknown"),
                deal.get("project","unknown"),
                deal.get("location","unknown"),
                "unknown",
                "unknown",
                deal.get("partners","unknown"),
                deal.get("deal",""),
                deal.get("significance","unknown"),
                "unknown",
                "unknown",
                deal.get("summary",""),
                url,
                scraped_at
            ]

            rows.append(row)

            print("   DEAL:", deal.get("company"), "-", deal.get("deal"))

        else:

            print("   not a deal")

        time.sleep(0.3)

    if rows:

        append_deals(sh,rows)

    mark_seen(sh,new_ids)

    print("\nBackfill complete")
    print("Deals found:",len(rows))


if __name__ == "__main__":
    main()
