"""
nuclear_scraper.py
──────────────────
Daily scraper for nuclear deal/announcement data.
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


# ───────────────── FEEDS ─────────────────

FEEDS = [

    {"name": "World Nuclear News", "url": "https://www.world-nuclear-news.org/rss"},
    {"name": "NEI", "url": "https://www.nei.org/rss/news"},
    {"name": "DOE Nuclear Energy", "url": "https://www.energy.gov/ne/rss.xml"},
    {"name": "ANS Nuclear Newswire", "url": "https://www.ans.org/news/feed/"},
    {"name": "Nuclear Engineering Intl", "url": "https://www.neimagazine.com/rss"},
    {"name": "Nucnet", "url": "https://www.nucnet.org/news/rss"},
    {"name": "Power Magazine Nuclear", "url": "https://www.powermag.com/category/nuclear/feed/"},

    # company announcements
    {"name": "TerraPower", "url": "https://www.terrapower.com/feed/"},
    {"name": "X-energy", "url": "https://x-energy.com/news/feed/"},
    {"name": "NuScale", "url": "https://www.nuscalepower.com/news/feed/"},
]


# ───────────────── KEYWORDS ─────────────────

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


# ───────────────── SHEET STRUCTURE ─────────────────

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
"scraped_at"
]


# ───────────────── HELPERS ─────────────────

def clean_text(text):
    if not text:
        return ""
    return " ".join(text.split()).strip()


def entry_hash(title, link):
    return hashlib.md5(f"{title}|{link}".encode()).hexdigest()


def parse_pub_date(raw):

    if not raw:
        return "unknown"

    try:
        dt = parsedate_to_datetime(raw)
        return dt.date().isoformat()
    except:
        pass

    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)

    return "unknown"


def is_relevant(title, summary):

    text = (title + " " + summary).lower()

    return any(k in text for k in SIGNAL_KEYWORDS)


# ───────────────── ARTICLE SCRAPER ─────────────────

def fetch_article_text(url):

    try:

        r = requests.get(
            url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )

        soup = BeautifulSoup(r.text,"html.parser")

        for tag in soup(["nav","footer","script","style","header","aside"]):
            tag.decompose()

        article = soup.find("article") or soup.find("main") or soup.body

        if not article:
            return ""

        text = article.get_text(separator=" ",strip=True)

        text = clean_text(text)

        return text[:9000]

    except Exception as e:

        print("fetch error:",e)

        return ""


# ───────────────── CLAUDE PROMPT ─────────────────

SYSTEM_PROMPT = """
You are a nuclear energy industry analyst.

Extract structured deal data from the article.

Return JSON only.

Schema:

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

If the article does not describe a nuclear deal:

{"skip": true}
"""


# ───────────────── CLAUDE EXTRACTION ─────────────────

def extract_deal(client, article):

    body = fetch_article_text(article["link"])

    user_msg = f"""
Title: {article['title']}
URL: {article['link']}
Published: {article.get('pub','')}
Summary: {article.get('summary','')}
Body: {body[:4000]}
"""

    try:

        resp = client.messages.create(
            model=os.getenv("CLAUDE_MODEL","claude-sonnet-4-20250514"),
            max_tokens=1600,
            system=SYSTEM_PROMPT,
            messages=[{"role":"user","content":user_msg}]
        )

        text = resp.content[0].text.strip()

        text = text.replace("```json","").replace("```","")

        start = text.find("{")
        end = text.rfind("}")+1

        text = text[start:end]

        data = json.loads(text)

        return data

    except Exception as e:

        print("Claude error:",e)

        return None


# ───────────────── GOOGLE SHEETS ─────────────────

def get_gsheet_client():

    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    creds = Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    return gspread.authorize(creds)


def ensure_sheets(spreadsheet):

    names=[ws.title for ws in spreadsheet.worksheets()]

    if SHEET_DEALS not in names:

        ws=spreadsheet.add_worksheet(title=SHEET_DEALS,rows=2000,cols=20)

        ws.append_row(DEAL_COLUMNS)

    if SHEET_SEEN not in names:

        ws=spreadsheet.add_worksheet(title=SHEET_SEEN,rows=10000,cols=3)

        ws.append_row(["hash","title","source"])


def load_seen_hashes(spreadsheet):

    ws=spreadsheet.worksheet(SHEET_SEEN)

    vals=ws.get_all_values()

    return {v[0] for v in vals[1:]}


def save_seen(spreadsheet,rows):

    if not rows:
        return

    spreadsheet.worksheet(SHEET_SEEN).append_rows(rows)


def append_deals(spreadsheet,deals):

    if not deals:
        return

    rows=[]

    for d in deals:
        rows.append([d.get(c,"unknown") for c in DEAL_COLUMNS])

    spreadsheet.worksheet(SHEET_DEALS).append_rows(rows)


# ───────────────── RSS SCRAPER ─────────────────

def scrape_feeds():

    candidates=[]

    for feed_cfg in FEEDS:

        print("Fetching:",feed_cfg["name"])

        try:

            feed=feedparser.parse(feed_cfg["url"])

            for entry in feed.entries[:80]:

                title=clean_text(entry.get("title",""))

                link=entry.get("link","")

                summary=BeautifulSoup(
                    entry.get("summary",""),
                    "html.parser"
                ).get_text()

                pub=entry.get("published","")

                if not title or not link:
                    continue

                # relaxed filtering
                if not is_relevant(title,summary):

                    if len(candidates)>5:
                        continue

                candidates.append({
                    "title":title,
                    "link":link,
                    "summary":summary,
                    "pub":pub
                })

        except Exception as e:

            print("feed error:",e)

    print("Candidates:",len(candidates))

    return candidates


# ───────────────── MAIN ─────────────────

def run():

    today=datetime.date.today().isoformat()

    now=datetime.datetime.utcnow().isoformat()

    print("Nuclear Deal Scraper",today)

    gc=get_gsheet_client()

    sheet=gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

    ensure_sheets(sheet)

    seen=load_seen_hashes(sheet)

    existing=sheet.worksheet(SHEET_DEALS).get_all_records()

    next_id=len(existing)+1

    client=anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"]
    )

    candidates=scrape_feeds()

    new_deals=[]

    new_seen=[]

    for article in candidates:

        h=entry_hash(article["title"],article["link"])

        if h in seen:
            continue

        print("Analyzing:",article["title"][:80])

        result=extract_deal(client,article)

        new_seen.append([h,article["title"],article["link"]])

        if not result:
            continue

        if result.get("skip"):
            continue

        if result.get("date")!="unknown":

            try:
                year=int(result["date"][:4])

                if year<2024:
                    print("Skipping old deal")
                    continue
            except:
                pass

        result["id"]=next_id
        result["scraped_at"]=now

        next_id+=1

        new_deals.append(result)

        print("NEW:",result.get("company"),"-",result.get("deal"))

    append_deals(sheet,new_deals)

    save_seen(sheet,new_seen)

    print("Added",len(new_deals),"new deals")


if __name__=="__main__":
    run()
