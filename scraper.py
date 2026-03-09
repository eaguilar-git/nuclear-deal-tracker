"""
nuclear_scraper.py
"""

import os
import re
import json
import hashlib
import datetime
from email.utils import parsedate_to_datetime

import feedparser
import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials

FEEDS = [
    {"name": "World Nuclear News",       "url": "https://www.world-nuclear-news.org/rss"},
    {"name": "NEI",                      "url": "https://www.nei.org/rss/news"},
    {"name": "DOE Nuclear Energy",       "url": "https://www.energy.gov/ne/rss.xml"},
    {"name": "ANS Nuclear Newswire",     "url": "https://www.ans.org/news/feed/"},
    {"name": "Nuclear Engineering Intl", "url": "https://www.neimagazine.com/rss"},
    {"name": "Nucnet",                   "url": "https://www.nucnet.org/news/rss"},
    {"name": "Power Magazine Nuclear",   "url": "https://www.powermag.com/category/nuclear/feed/"},
    {"name": "TerraPower",               "url": "https://www.terrapower.com/feed/"},
    {"name": "X-energy",                 "url": "https://x-energy.com/news/feed/"},
    {"name": "NuScale",                  "url": "https://www.nuscalepower.com/news/feed/"},
]

SIGNAL_KEYWORDS = [
    "agreement","contract","partnership","deal","mou","memorandum",
    "deployment","deploy","reactor order","reactor sale",
    "construction","construction permit","construction start",
    "license","licensing","approval","permit",
    "funding","investment","grant","award","selected","signed",
    "financial close","epc","supply agreement","purchase",
    "ppa","power purchase","offtake","energy supply",
    "smr","advanced reactor","microreactor",
    "natrium","xe-100","bwrx","bwrx-300","aurora","evinci",
    "kairos","voygr","last energy","newcleo",
    "terrapower","x-energy","oklo","ge hitachi","westinghouse",
    "data center","ai power","industrial heat",
]

SHEET_DEALS = "Deals"
SHEET_SEEN  = "Seen"

DEAL_COLUMNS = [
    "id","date","company","reactor","project",
    "location","country","developer_country","partners",
    "deal","significance","reactor_capacity_mw","deployment_year",
    "summary","source","scraped_at",
]

SYSTEM_PROMPT = """You are a nuclear energy industry analyst.
Extract structured deal data from the article. Return JSON only - no markdown fences, no explanation.

If NOT a concrete nuclear deal/partnership/funding/licensing milestone: {"skip": true}

If IS a deal:
{
  "skip": false,
  "company": "primary reactor developer",
  "reactor": "reactor technology name",
  "project": "project or site name",
  "location": "City, State or region",
  "country": "deployment country",
  "developer_country": "developer country",
  "partners": "key partners comma-separated",
  "deal": "short label max 8 words",
  "date": "YYYY-MM-DD",
  "significance": "Signaling | Market development | Deployment",
  "reactor_capacity_mw": "numeric MW or unknown",
  "deployment_year": "expected year or unknown",
  "summary": "1-2 sentence factual summary",
  "source": "article URL"
}

Significance: Signaling=MOU/LOI/study, Market development=funded contract/license/EPC award, Deployment=construction permit/financial close/commercial operation"""


def clean_text(t):
    return " ".join(t.split()).strip() if t else ""

def entry_hash(title, link):
    return hashlib.md5(f"{title}|{link}".encode()).hexdigest()

def is_relevant(title, summary):
    text = (title + " " + summary).lower()
    return any(k in text for k in SIGNAL_KEYWORDS)

def fetch_article_text(url):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["nav","footer","script","style","header","aside"]):
            tag.decompose()
        article = soup.find("article") or soup.find("main") or soup.body
        return clean_text(article.get_text(separator=" ", strip=True))[:6000] if article else ""
    except Exception as e:
        print("fetch error:", e)
        return ""

def extract_deal(client, article):
    body = fetch_article_text(article["link"])
    user_msg = f"""Title: {article['title']}
URL: {article['link']}
Published: {article.get('pub','')}
Summary: {article.get('summary','')}
Body: {body[:3000]}"""
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        # strip any accidental markdown fences
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        print("Claude error:", e)
        return None

def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def ensure_sheets(spreadsheet):
    names = [ws.title for ws in spreadsheet.worksheets()]
    if SHEET_DEALS not in names:
        ws = spreadsheet.add_worksheet(title=SHEET_DEALS, rows=2000, cols=20)
        ws.append_row(DEAL_COLUMNS)
    if SHEET_SEEN not in names:
        ws = spreadsheet.add_worksheet(title=SHEET_SEEN, rows=10000, cols=3)
        ws.append_row(["hash","title","source"])

def load_seen_hashes(spreadsheet):
    return {v[0] for v in spreadsheet.worksheet(SHEET_SEEN).get_all_values()[1:]}

def save_seen(spreadsheet, rows):
    if rows:
        spreadsheet.worksheet(SHEET_SEEN).append_rows(rows)

def append_deals(spreadsheet, deals):
    if not deals:
        return
    rows = [[d.get(c,"unknown") for c in DEAL_COLUMNS] for d in deals]
    spreadsheet.worksheet(SHEET_DEALS).append_rows(rows)
    print(f"  -> Appended {len(rows)} rows to sheet")

def scrape_feeds():
    candidates = []
    for feed_cfg in FEEDS:
        print(f"  Fetching: {feed_cfg['name']} ...", end=" ", flush=True)
        try:
            feed = feedparser.parse(feed_cfg["url"])
            matched = 0
            for entry in feed.entries[:80]:
                title   = clean_text(entry.get("title",""))
                link    = entry.get("link","")
                summary = BeautifulSoup(entry.get("summary",""),"html.parser").get_text()
                pub     = entry.get("published","")
                if not title or not link:
                    continue
                if is_relevant(title, summary):
                    matched += 1
                    candidates.append({"title":title,"link":link,"summary":summary,"pub":pub})
            print(f"{len(feed.entries)} entries, {matched} matched")
        except Exception as e:
            print(f"ERROR: {e}")
    print(f"\n  TOTAL: {len(candidates)} candidates")
    return candidates

def run():
    today = datetime.date.today().isoformat()
    now   = datetime.datetime.utcnow().isoformat()
    print(f"\n{'='*60}\nNuclear Deal Scraper — {today}\n{'='*60}\n")

    gc    = get_gsheet_client()
    sheet = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])
    ensure_sheets(sheet)

    seen     = load_seen_hashes(sheet)
    existing = sheet.worksheet(SHEET_DEALS).get_all_records()
    next_id  = len(existing) + 1
    print(f"  {len(seen)} seen, {len(existing)} existing deals\n")

    client     = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    candidates = scrape_feeds()
    print()

    new_deals, new_seen = [], []

    for article in candidates:
        h = entry_hash(article["title"], article["link"])
        if h in seen:
            continue
        print(f"  Analyzing: {article['title'][:75]}")
        result = extract_deal(client, article)
        new_seen.append([h, article["title"], article["link"]])
        if not result or result.get("skip"):
            print("    -> Skipped")
            continue
        try:
            if int(result.get("date","0")[:4]) < 2024:
                print("    -> Too old")
                continue
        except Exception:
            pass
        result["id"]         = next_id
        result["scraped_at"] = now
        next_id += 1
        new_deals.append(result)
        print(f"    NEW [{result.get('significance')}] {result.get('company')} - {result.get('deal')}")

    print()
    append_deals(sheet, new_deals)
    save_seen(sheet, new_seen)
    print(f"Done: {len(new_deals)} new deals | {today}\n")

if __name__ == "__main__":
    run()
