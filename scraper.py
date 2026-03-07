"""
nuclear_scraper.py
──────────────────
Daily scraper for nuclear deal/announcement data.

Pipeline:
  1. Fetch RSS feeds from major nuclear news sources
  2. Filter articles by deal-signal keywords
  3. Send each candidate to Claude API for structured extraction
  4. Deduplicate against a "seen" tab in the Google Sheet
  5. Append new rows to the "Deals" tab

Required env vars:
  ANTHROPIC_API_KEY          — Anthropic API key
  GOOGLE_SERVICE_ACCOUNT_JSON — Full JSON of your service account credentials
  GOOGLE_SHEET_ID            — The ID from your sheet URL:
                                docs.google.com/spreadsheets/d/<SHEET_ID>/edit

Dependencies:
  pip install anthropic feedparser requests beautifulsoup4 gspread google-auth
"""

import os
import json
import hashlib
import datetime
import feedparser
import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ─────────────────────────────────────────────────────────────────────

# RSS/news feeds to monitor
FEEDS = [
    {"name": "World Nuclear News",       "url": "https://www.world-nuclear-news.org/rss"},
    {"name": "NEI",                      "url": "https://www.nei.org/rss/news"},
    {"name": "DOE Nuclear Energy",       "url": "https://www.energy.gov/ne/rss.xml"},
    {"name": "ANS Nuclear Newswire",     "url": "https://www.ans.org/news/feed/"},
    {"name": "Nuclear Engineering Intl", "url": "https://www.neimagazine.com/rss"},
]

# Keywords that flag an article as a potential deal/milestone
SIGNAL_KEYWORDS = [
    "agreement", "contract", "partnership", "deal", "mou", "memorandum",
    "deploy", "deployment", "construct", "construction permit", "license",
    "funding", "investment", "grant", "award", "selected", "signed",
    "smr", "advanced reactor", "microreactor", "natrium", "xe-100", "bwrx",
    "aurora", "evinci", "kairos", "nuscale", "last energy", "terrapower",
    "x-energy", "ge hitachi", "oklo", "westinghouse", "newcleo", "radiant",
    "dow", "microsoft", "google", "amazon", "data center",
    "haleu", "doe", "ardp", "nrc", "license application",
    "power purchase", "ppa", "offtake",
]

# Google Sheets tab names
SHEET_DEALS = "Deals"
SHEET_SEEN  = "Seen"

# Column order for the Deals sheet
DEAL_COLUMNS = [
    "id", "date", "company", "reactor", "project",
    "location", "partners", "deal", "significance",
    "summary", "source", "scraped_at",
]

# ── CLAUDE EXTRACTION PROMPT ───────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a nuclear energy industry analyst assistant.
Given a news article title, URL, and text, extract structured deal data.
Respond ONLY with a valid JSON object — no markdown fences, no preamble.

If the article does NOT describe a concrete deal, partnership, funding round,
licensing milestone, or deployment announcement between identifiable parties:
  {"skip": true, "reason": "brief reason"}

If it IS relevant:
{
  "skip": false,
  "company": "primary reactor developer (e.g. TerraPower, X-energy, GE Hitachi, Oklo, Westinghouse, Kairos, NuScale, Last Energy)",
  "reactor": "reactor technology name (Natrium, Xe-100, BWRX-300, Aurora, eVinci, KP-FHR, VOYGR, or best match)",
  "project": "project or site name, or 'Program-level' if no specific site",
  "location": "City, State or Country",
  "partners": "key partner(s), comma-separated",
  "deal": "short deal label, max 8 words",
  "date": "YYYY-MM-DD (use article publication date)",
  "significance": "Signaling | Market development | Deployment",
  "source": "full article URL",
  "summary": "1-2 sentence factual summary"
}

Significance:
- Signaling        = MOU, letter of intent, site study, feasibility work
- Market development = Funded contract, licensing step, EPC selection, design award
- Deployment       = Construction permit, commercial operation agreement, financial close
"""

# ── GOOGLE SHEETS HELPERS ──────────────────────────────────────────────────────

def get_gsheet_client():
    """Authenticate with Google Sheets using a service account JSON stored in env."""
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON env var not set")
    
    creds_dict = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def ensure_sheets(spreadsheet):
    """Create Deals and Seen tabs if they don't exist; add headers to Deals."""
    sheet_names = [ws.title for ws in spreadsheet.worksheets()]
    
    # Deals sheet
    if SHEET_DEALS not in sheet_names:
        ws = spreadsheet.add_worksheet(title=SHEET_DEALS, rows=1000, cols=20)
        ws.append_row(DEAL_COLUMNS, value_input_option="RAW")
        print(f"  Created sheet: {SHEET_DEALS}")
    
    # Seen hashes sheet (single column)
    if SHEET_SEEN not in sheet_names:
        ws = spreadsheet.add_worksheet(title=SHEET_SEEN, rows=5000, cols=2)
        ws.append_row(["hash", "title"], value_input_option="RAW")
        print(f"  Created sheet: {SHEET_SEEN}")


def load_seen_hashes(spreadsheet) -> set:
    """Load all previously seen article hashes from the Seen sheet."""
    ws = spreadsheet.worksheet(SHEET_SEEN)
    records = ws.get_all_values()
    # Skip header row; first column is hash
    return {row[0] for row in records[1:] if row}


def save_seen_hashes(spreadsheet, new_entries: list[tuple]):
    """Append new (hash, title) rows to the Seen sheet."""
    if not new_entries:
        return
    ws = spreadsheet.worksheet(SHEET_SEEN)
    for entry in new_entries:
        ws.append_row(list(entry), value_input_option="RAW")


def load_existing_deals(spreadsheet) -> list[dict]:
    """Load existing deals to get current max ID."""
    ws = spreadsheet.worksheet(SHEET_DEALS)
    records = ws.get_all_records()
    return records


def append_deals(spreadsheet, deals: list[dict]):
    """Append new deal rows to the Deals sheet."""
    ws = spreadsheet.worksheet(SHEET_DEALS)
    for deal in deals:
        row = [str(deal.get(col, "")) for col in DEAL_COLUMNS]
        ws.append_row(row, value_input_option="USER_ENTERED")
    print(f"  → Appended {len(deals)} row(s) to '{SHEET_DEALS}'")


# ── SCRAPING HELPERS ───────────────────────────────────────────────────────────

def entry_hash(title: str, link: str) -> str:
    return hashlib.md5(f"{title}|{link}".encode()).hexdigest()


def is_relevant(title: str, summary: str) -> bool:
    text = (title + " " + summary).lower()
    return any(kw in text for kw in SIGNAL_KEYWORDS)


def fetch_article_text(url: str) -> str:
    """Grab cleaned article body text."""
    try:
        resp = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["nav", "footer", "script", "style", "aside", "header"]):
            tag.decompose()
        article = soup.find("article") or soup.find("main") or soup.body
        if article:
            return article.get_text(separator=" ", strip=True)[:3000]
    except Exception as e:
        print(f"    ⚠ Fetch error: {e}")
    return ""


def scrape_feeds() -> list[dict]:
    candidates = []
    for feed_cfg in FEEDS:
        print(f"  Fetching: {feed_cfg['name']} ...")
        try:
            feed = feedparser.parse(feed_cfg["url"])
            for entry in feed.entries[:25]:
                title   = getattr(entry, "title", "")
                link    = getattr(entry, "link", "")
                summary = BeautifulSoup(
                    getattr(entry, "summary", ""), "html.parser"
                ).get_text()[:600]
                pub     = getattr(entry, "published", "")
                
                if is_relevant(title, summary):
                    candidates.append({
                        "title":   title,
                        "link":    link,
                        "summary": summary,
                        "pub":     pub,
                        "feed":    feed_cfg["name"],
                    })
        except Exception as e:
            print(f"    ⚠ Feed error ({feed_cfg['name']}): {e}")
    
    print(f"  {len(candidates)} relevant candidates found across all feeds")
    return candidates


# ── CLAUDE EXTRACTION ──────────────────────────────────────────────────────────

def extract_deal(client: anthropic.Anthropic, article: dict) -> dict | None:
    body = fetch_article_text(article["link"])
    
    user_msg = f"""Title: {article['title']}
URL: {article['link']}
Published: {article['pub']}
Summary: {article['summary']}
Body: {body[:2000] if body else 'Not available'}
"""
    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=700,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        print(f"    ⚠ Claude error: {e}")
        return None


# ── MAIN ───────────────────────────────────────────────────────────────────────

def run():
    today = datetime.date.today().isoformat()
    now   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    
    print(f"\n{'='*60}")
    print(f"Nuclear Deal Scraper — {today}")
    print(f"{'='*60}\n")

    # ── Connect to Google Sheets ──
    print("Connecting to Google Sheets...")
    gc = get_gsheet_client()
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID env var not set")
    
    spreadsheet = gc.open_by_key(sheet_id)
    ensure_sheets(spreadsheet)
    
    seen_hashes    = load_seen_hashes(spreadsheet)
    existing_deals = load_existing_deals(spreadsheet)
    next_id        = max((int(d.get("id", 0)) for d in existing_deals if d.get("id")), default=0) + 1
    
    print(f"  Loaded {len(seen_hashes)} seen hashes, {len(existing_deals)} existing deals\n")

    # ── Init Claude ──
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # ── Scrape ──
    candidates = scrape_feeds()
    print()

    new_deals      = []
    new_seen_rows  = []

    for article in candidates:
        h = entry_hash(article["title"], article["link"])
        
        if h in seen_hashes:
            print(f"  ↩ Seen: {article['title'][:65]}")
            continue

        print(f"  ⟳ Extracting: {article['title'][:65]}")
        result = extract_deal(client, article)
        
        # Always mark as seen to avoid re-processing
        new_seen_rows.append((h, article["title"][:200]))

        if result is None:
            print("    ✗ Extraction failed")
            continue

        if result.get("skip"):
            print(f"    ↷ Skipped ({result.get('reason', '')})")
            continue

        required = ["company", "reactor", "deal", "date", "significance", "source"]
        if not all(result.get(k) for k in required):
            print(f"    ✗ Incomplete: {result}")
            continue

        result["id"]         = next_id
        result["scraped_at"] = now
        next_id += 1
        new_deals.append(result)
        
        sig_icon = {"Deployment": "🟢", "Market development": "🟡", "Signaling": "🔵"}.get(result["significance"], "⚪")
        print(f"    {sig_icon} NEW [{result['significance']}] {result['company']} — {result['deal']}")

    # ── Write to Sheets ──
    print()
    if new_deals:
        append_deals(spreadsheet, new_deals)
        print(f"✅ Added {len(new_deals)} new deal(s) to Google Sheet")
    else:
        print("— No new deals found today")

    if new_seen_rows:
        save_seen_hashes(spreadsheet, new_seen_rows)
        print(f"  Logged {len(new_seen_rows)} new seen hash(es)")

    print(f"\nDone — {today}\n")


if __name__ == "__main__":
    run()
