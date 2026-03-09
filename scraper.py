"""
nuclear_scraper.py
──────────────────
Daily scraper for nuclear deal/announcement data.

Pipeline:
  1. Fetch RSS feeds from major nuclear news sources
  2. Filter articles by deal-signal keywords
  3. Send each candidate to Claude API for structured extraction
  4. Optionally allow Claude web search to fill missing details
  5. Deduplicate against a "seen" tab in the Google Sheet
  6. Append new rows to the "Deals" tab

Required env vars:
  ANTHROPIC_API_KEY
  GOOGLE_SERVICE_ACCOUNT_JSON
  GOOGLE_SHEET_ID

Optional env vars:
  CLAUDE_MODEL                 default: claude-sonnet-4-20250514
  ENABLE_CLAUDE_WEB_SEARCH     true / false (default false)
  CLAUDE_WEB_SEARCH_MAX_USES   default: 3
"""

import os
import re
import json
import hashlib
import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials


# ── CONFIG ─────────────────────────────────────────────────────────────────────

FEEDS = [
    {"name": "World Nuclear News",       "url": "https://www.world-nuclear-news.org/rss"},
    {"name": "NEI",                      "url": "https://www.nei.org/rss/news"},
    {"name": "DOE Nuclear Energy",       "url": "https://www.energy.gov/ne/rss.xml"},
    {"name": "ANS Nuclear Newswire",     "url": "https://www.ans.org/news/feed/"},
    {"name": "Nuclear Engineering Intl", "url": "https://www.neimagazine.com/rss"},
    {"name": "Nucnet",                   "url": "https://www.nucnet.org/news/rss"},
    {"name": "Power Magazine Nuclear",   "url": "https://www.powermag.com/category/nuclear/feed/"},
]

SIGNAL_KEYWORDS = [
    "agreement", "contract", "partnership", "deal", "mou", "memorandum",
    "deployment", "deploy", "reactor order", "reactor sale",
    "construction", "construction permit", "construction start",
    "license", "licensing", "licence", "approval", "permit",
    "funding", "investment", "grant", "award", "selected", "signed",
    "financial close", "epc", "supply agreement", "purchase",
    "ppa", "power purchase", "offtake", "energy supply",
    "smr", "advanced reactor", "microreactor",
    "natrium", "xe-100", "bwrx", "bwrx-300", "aurora", "evinci",
    "kairos", "kp-fhr", "voygr", "last energy", "newcleo",
    "terrapower", "x-energy", "oklo", "ge hitachi", "westinghouse",
    "data center", "ai power", "industrial heat",
    "doe", "ardp", "haleu", "nrc", "site selection", "feasibility",
    "letter of intent", "loi", "demonstration project", "commercial agreement",
]

SHEET_DEALS = "Deals"
SHEET_SEEN = "Seen"

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

REACTOR_MAP = {
    "terrapower": "Natrium",
    "x-energy": "Xe-100",
    "ge hitachi": "BWRX-300",
    "geh": "BWRX-300",
    "oklo": "Aurora",
    "westinghouse": "unknown",
    "nuscale": "VOYGR",
    "kairos": "KP-FHR",
    "last energy": "PWR-20",
    "radiant": "Kaleidos",
    "newcleo": "LFR-AS-30",
}

DEVELOPER_COUNTRY_MAP = {
    "terrapower": "United States",
    "x-energy": "United States",
    "ge hitachi": "United States",
    "geh": "United States",
    "oklo": "United States",
    "westinghouse": "United States",
    "nuscale": "United States",
    "kairos": "United States",
    "last energy": "United States",
    "radiant": "United States",
    "newcleo": "France/Italy/United Kingdom",
}

SIGNIFICANCE_VALUES = {"signaling", "market development", "deployment"}


SYSTEM_PROMPT = """
You are an expert nuclear energy industry analyst building a structured
database of global nuclear project announcements, reactor deployments,
financing events, and commercial agreements.

You receive a news article title, URL, publication date, summary, and article text.

Your job is to extract structured project/deal information.

You may:
• infer missing details from context
• use your general knowledge of nuclear developers and reactors
• infer reactor type from developer (example: TerraPower → Natrium)
• infer location if the site is mentioned indirectly
• infer partners if clearly referenced
• use the article URL if the source field is missing
• if web search is available, use it only when needed to fill important gaps

If a field cannot be determined, return "unknown".

If the article does NOT describe a nuclear project development,
deployment milestone, commercial agreement, licensing step,
or financing activity, return:

{"skip": true, "reason": "brief reason"}

Otherwise return:

{
  "skip": false,
  "company": "...",
  "reactor": "...",
  "project": "...",
  "location": "...",
  "country": "...",
  "developer_country": "...",
  "partners": "...",
  "deal": "...",
  "date": "...",
  "significance": "...",
  "reactor_capacity_mw": "...",
  "deployment_year": "...",
  "summary": "...",
  "source": "..."
}

Rules:
• Never leave a field blank — use "unknown"
• "deal" must be concise, max 8 words
• "summary" must be factual and concise, 1–2 sentences
• "date" should be YYYY-MM-DD
• "significance" must be one of: Signaling, Market development, Deployment
• return valid JSON only
• no markdown
• no explanation

Significance categories:
• Signaling = MOU, LOI, feasibility studies, early exploration
• Market development = contracts, EPC selection, licensing steps, supply agreements, funded partnerships
• Deployment = reactor orders, construction permits, financial close, construction start, commercial operation
"""


SEED_DEALS = [
    {
        "id": 1,
        "date": "2024-10-15",
        "company": "TerraPower",
        "reactor": "Natrium",
        "project": "Kemmerer Plant",
        "location": "Kemmerer, Wyoming, United States",
        "country": "United States",
        "developer_country": "United States",
        "partners": "PacifiCorp, DOE",
        "deal": "Construction permit application accepted",
        "significance": "Deployment",
        "reactor_capacity_mw": "345",
        "deployment_year": "unknown",
        "summary": "TerraPower submitted a construction permit application to the NRC for its Natrium reactor demonstration at the Kemmerer, Wyoming site.",
        "source": "https://www.world-nuclear-news.org/Articles/TerraPower-submits-construction-permit-application",
        "scraped_at": "seed",
    },
    {
        "id": 2,
        "date": "2024-09-20",
        "company": "X-energy",
        "reactor": "Xe-100",
        "project": "Dow Seadrift",
        "location": "Seadrift, Texas, United States",
        "country": "United States",
        "developer_country": "United States",
        "partners": "Dow",
        "deal": "MOU for industrial heat",
        "significance": "Signaling",
        "reactor_capacity_mw": "unknown",
        "deployment_year": "unknown",
        "summary": "X-energy and Dow signed an MOU to explore deploying Xe-100 reactors at Dow's Seadrift facility.",
        "source": "https://x-energy.com/news/dow-x-energy-mou",
        "scraped_at": "seed",
    },
]


# ── UTILS ──────────────────────────────────────────────────────────────────────

def getenv_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def clean_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split()).strip()


def extract_json_object(text: str) -> str:
    if not text:
        return ""
    text = text.replace("```json", "").replace("```", "").strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return ""
    return text[start:end + 1]


def parse_pub_date(raw: str) -> str:
    if not raw:
        return "unknown"

    try:
        dt = parsedate_to_datetime(raw)
        return dt.date().isoformat()
    except Exception:
        pass

    # fallback patterns
    patterns = [
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{4}/\d{2}/\d{2})",
    ]
    for pattern in patterns:
        m = re.search(pattern, raw)
        if m:
            return m.group(1).replace("/", "-")

    return "unknown"


def entry_hash(title: str, link: str) -> str:
    clean_title = clean_text(title).lower()
    clean_link = clean_text(link)
    return hashlib.md5(f"{clean_title}|{clean_link}".encode()).hexdigest()


def infer_country(location: str) -> str:
    if not location or location == "unknown":
        return "unknown"
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if not parts:
        return "unknown"
    return parts[-1]


def infer_from_company(company: str):
    if not company or company == "unknown":
        return "unknown", "unknown"

    name = company.lower()
    reactor = "unknown"
    dev_country = "unknown"

    for key, value in REACTOR_MAP.items():
        if key in name and reactor == "unknown":
            reactor = value

    for key, value in DEVELOPER_COUNTRY_MAP.items():
        if key in name and dev_country == "unknown":
            dev_country = value

    return reactor, dev_country


def normalize_significance(value: str) -> str:
    if not value:
        return "unknown"
    val = value.strip().lower()
    if val == "signaling":
        return "Signaling"
    if val == "market development":
        return "Market development"
    if val == "deployment":
        return "Deployment"
    return "unknown"


def is_relevant(title: str, summary: str) -> bool:
    text = clean_text(f"{title} {summary}").lower()
    return any(kw in text for kw in SIGNAL_KEYWORDS)


def normalize_result(result: dict, article: dict, now: str) -> dict:
    if not isinstance(result, dict):
        return result

    # Default all fields used in sheet
    for col in DEAL_COLUMNS:
        if col not in result or result[col] in [None, ""]:
            result[col] = "unknown"

    result["source"] = result["source"] if result["source"] != "unknown" else article["link"]
    result["summary"] = (
        result["summary"]
        if result["summary"] != "unknown"
        else (article.get("summary", "")[:250] or "unknown")
    )

    if result["date"] == "unknown":
        result["date"] = parse_pub_date(article.get("pub", ""))

    result["significance"] = normalize_significance(result.get("significance", ""))

    inferred_reactor, inferred_dev_country = infer_from_company(result.get("company", "unknown"))

    if result["reactor"] == "unknown" and inferred_reactor != "unknown":
        result["reactor"] = inferred_reactor

    if result["developer_country"] == "unknown" and inferred_dev_country != "unknown":
        result["developer_country"] = inferred_dev_country

    if result["country"] == "unknown":
        result["country"] = infer_country(result.get("location", "unknown"))

    result["scraped_at"] = now

    # Soft cleanup
    for key, value in result.items():
        if isinstance(value, str):
            result[key] = clean_text(value) or "unknown"

    return result


# ── GOOGLE SHEETS HELPERS ──────────────────────────────────────────────────────

def get_gsheet_client():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON env var not set")

    creds_dict = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def ensure_sheets(spreadsheet):
    sheet_names = [ws.title for ws in spreadsheet.worksheets()]

    if SHEET_DEALS not in sheet_names:
        ws = spreadsheet.add_worksheet(title=SHEET_DEALS, rows=2000, cols=max(20, len(DEAL_COLUMNS) + 4))
        ws.append_row(DEAL_COLUMNS, value_input_option="RAW")
        print(f"  Created sheet: {SHEET_DEALS}")
    else:
        ws = spreadsheet.worksheet(SHEET_DEALS)
        first_row = ws.row_values(1)
        if first_row != DEAL_COLUMNS:
            if not first_row:
                ws.insert_row(DEAL_COLUMNS, 1, value_input_option="RAW")
            else:
                ws.update("A1", [DEAL_COLUMNS], value_input_option="RAW")
            print(f"  Updated headers on {SHEET_DEALS}")

    if SHEET_SEEN not in sheet_names:
        ws = spreadsheet.add_worksheet(title=SHEET_SEEN, rows=10000, cols=3)
        ws.append_row(["hash", "title", "source"], value_input_option="RAW")
        print(f"  Created sheet: {SHEET_SEEN}")


def load_seen_hashes(spreadsheet) -> set:
    ws = spreadsheet.worksheet(SHEET_SEEN)
    records = ws.get_all_values()
    return {row[0] for row in records[1:] if row and row[0]}


def save_seen_hashes(spreadsheet, new_entries: list):
    if not new_entries:
        return
    ws = spreadsheet.worksheet(SHEET_SEEN)
    ws.append_rows(new_entries, value_input_option="RAW")


def load_existing_deals(spreadsheet) -> list:
    ws = spreadsheet.worksheet(SHEET_DEALS)
    return ws.get_all_records()


def append_deals(spreadsheet, deals: list):
    if not deals:
        return
    ws = spreadsheet.worksheet(SHEET_DEALS)
    rows = []
    for deal in deals:
        row = [str(deal.get(col, "unknown")) for col in DEAL_COLUMNS]
        rows.append(row)
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"  → Appended {len(deals)} row(s) to '{SHEET_DEALS}'")


# ── SCRAPING HELPERS ───────────────────────────────────────────────────────────

def fetch_article_text(url: str) -> str:
    try:
        resp = requests.get(
            url,
            timeout=20,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; NuclearDealTracker/1.0)"
            },
        )
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["nav", "footer", "script", "style", "aside", "header", "noscript", "form"]):
            tag.decompose()

        article = (
            soup.find("article")
            or soup.find("main")
            or soup.find(attrs={"role": "main"})
            or soup.body
        )

        if not article:
            return ""

        text = article.get_text(separator=" ", strip=True)
        text = clean_text(text)

        return text[:7000]

    except Exception as e:
        print(f"    ⚠ Fetch error: {e}")
        return ""


def scrape_feeds() -> list:
    candidates = []

    for feed_cfg in FEEDS:
        print(f"  Fetching: {feed_cfg['name']} ...", end=" ")
        try:
            feed = feedparser.parse(feed_cfg["url"])
            n_entries = len(feed.entries)
            n_matched = 0

            for entry in feed.entries[:50]:
                title = clean_text(getattr(entry, "title", ""))
                link = clean_text(getattr(entry, "link", ""))

                summary_html = getattr(entry, "summary", "") or getattr(entry, "description", "")
                summary = clean_text(BeautifulSoup(summary_html, "html.parser").get_text())[:800]

                pub = (
                    getattr(entry, "published", "")
                    or getattr(entry, "updated", "")
                    or getattr(entry, "created", "")
                )

                if not title or not link:
                    continue

                if is_relevant(title, summary):
                    n_matched += 1
                    candidates.append(
                        {
                            "title": title,
                            "link": link,
                            "summary": summary,
                            "pub": pub,
                            "feed": feed_cfg["name"],
                        }
                    )

            print(f"{n_entries} entries, {n_matched} matched")

        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\n  TOTAL: {len(candidates)} candidate articles across all feeds")
    return candidates


# ── CLAUDE EXTRACTION ──────────────────────────────────────────────────────────

def extract_deal(client: anthropic.Anthropic, article: dict):
    body = fetch_article_text(article["link"])

    user_msg = f"""Title: {article['title']}
URL: {article['link']}
Published: {article.get('pub', 'unknown')}
Feed: {article.get('feed', 'unknown')}
Summary: {article.get('summary', 'unknown')}
Body: {body[:4000] if body else 'Not available'}

Use the article first.
If important fields are still missing and web search is available, you may search the web.
Return JSON only.
"""

    model = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")
    enable_web_search = getenv_bool("ENABLE_CLAUDE_WEB_SEARCH", False)
    web_search_max_uses = int(os.getenv("CLAUDE_WEB_SEARCH_MAX_USES", "3"))

    kwargs = {
        "model": model,
        "max_tokens": 1200,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_msg}],
    }

    if enable_web_search:
        kwargs["tools"] = [
            {
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": web_search_max_uses,
            }
        ]

    try:
        response = client.messages.create(**kwargs)

        # Join text blocks only
        text_parts = []
        for block in response.content:
            if getattr(block, "type", None) == "text":
                text_parts.append(block.text)

        raw_text = "\n".join(text_parts).strip()
        raw_json = extract_json_object(raw_text)

        if not raw_json:
            print(f"    ⚠ Claude returned non-JSON content: {raw_text[:200]}")
            return None

        parsed = json.loads(raw_json)
        print(f"    Claude parsed: {json.dumps(parsed)[:180]}")
        return parsed

    except Exception as e:
        print(f"    ⚠ Claude error: {e}")
        return None


# ── MAIN ───────────────────────────────────────────────────────────────────────

def run():
    today = datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'=' * 70}")
    print(f"Nuclear Deal Scraper — {today}")
    print(f"{'=' * 70}\n")

    print("Connecting to Google Sheets...")
    gc = get_gsheet_client()

    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID env var not set")

    spreadsheet = gc.open_by_key(sheet_id)
    ensure_sheets(spreadsheet)

    seen_hashes = load_seen_hashes(spreadsheet)
    existing_deals = load_existing_deals(spreadsheet)
    next_id = max((int(d.get("id", 0)) for d in existing_deals if d.get("id")), default=0) + 1

    print(f"  Loaded {len(seen_hashes)} seen hashes, {len(existing_deals)} existing deals\n")

    # Seed if empty
    if len(existing_deals) == 0:
        print("  Sheet is empty — loading seed data...")
        seed_rows = []
        seed_seen_rows = []

        for deal in SEED_DEALS:
            d = dict(deal)
            if d.get("scraped_at") == "seed":
                d["scraped_at"] = now
            seed_rows.append(d)
            seed_seen_rows.append([
                entry_hash(d["deal"], d["source"]),
                d["deal"][:200],
                d["source"],
            ])

        append_deals(spreadsheet, seed_rows)
        save_seen_hashes(spreadsheet, seed_seen_rows)
        seen_hashes.update({row[0] for row in seed_seen_rows})
        next_id = len(seed_rows) + 1
        print(f"  ✅ Seeded {len(seed_rows)} baseline deals\n")

    print("Initializing Claude...")
    if "ANTHROPIC_API_KEY" not in os.environ:
        raise ValueError("ANTHROPIC_API_KEY env var not set")

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    print("Scanning RSS feeds...")
    candidates = scrape_feeds()
    print()

    new_deals = []
    new_seen_rows = []

    for article in candidates:
        h = entry_hash(article["title"], article["link"])

        if h in seen_hashes:
            print(f"  ↩ Seen: {article['title'][:90]}")
            continue

        print(f"  ⟳ Extracting: {article['title'][:90]}")
        result = extract_deal(client, article)

        # Mark as seen whether extraction works or not, so the scraper doesn't loop forever
        new_seen_rows.append([h, article["title"][:200], article["link"]])

        if result is None:
            print("    ✗ Extraction failed")
            continue

        if result.get("skip"):
            print(f"    ↷ Skipped ({result.get('reason', '')})")
            continue

        result = normalize_result(result, article, now)

        required = ["company", "deal", "date"]
        if not all(result.get(k) and result.get(k) != "unknown" for k in required):
            print(f"    ✗ Missing critical fields: {result}")
            continue

        result["id"] = next_id
        next_id += 1
        new_deals.append(result)

        sig_icon = {
            "Deployment": "🟢",
            "Market development": "🟡",
            "Signaling": "🔵",
        }.get(result["significance"], "⚪")

        print(f"    {sig_icon} NEW [{result['significance']}] {result['company']} — {result['deal']}")

    print()
    if new_deals:
        append_deals(spreadsheet, new_deals)
        print(f"✅ Added {len(new_deals)} new deal(s) to Google Sheet")
    else:
        print("— No new deals found in this run")

    if new_seen_rows:
        save_seen_hashes(spreadsheet, new_seen_rows)
        print(f"  Logged {len(new_seen_rows)} new seen hash(es)")

    print(f"\nDone — {today}\n")


if __name__ == "__main__":
    run()
