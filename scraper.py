"""
nuclear_scraper.py  —  Relational version
==========================================
- Extracts deals from RSS feeds using Claude (Anthropic)
- Looks up existing Companies / Reactors / Projects by name
- Creates new rows in those tables if not found
- Appends Announcements with full relational IDs
- Flags unresolved items in a 'Review' tab
"""

import os
import re
import json
import time
import hashlib
import datetime
from email.utils import parsedate_to_datetime

import feedparser
import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials


# ─── FEEDS ────────────────────────────────────────────────────────────────────

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
    "agreement", "contract", "partnership", "deal", "mou", "memorandum",
    "deployment", "deploy", "reactor order", "reactor sale",
    "construction", "construction permit", "construction start",
    "license", "licensing", "approval", "permit",
    "funding", "investment", "grant", "award", "selected", "signed",
    "financial close", "epc", "supply agreement", "purchase",
    "ppa", "power purchase", "offtake", "energy supply",
    "smr", "advanced reactor", "microreactor",
    "natrium", "xe-100", "bwrx", "bwrx-300", "aurora", "evinci",
    "kairos", "voygr", "last energy", "newcleo",
    "terrapower", "x-energy", "oklo", "ge hitachi", "westinghouse",
    "data center", "ai power", "industrial heat",
]

# ─── SHEET TABS ───────────────────────────────────────────────────────────────

TAB_COMPANIES     = "Companies"
TAB_REACTORS      = "Reactors"
TAB_PROJECTS      = "Projects"
TAB_ANNOUNCEMENTS = "Announcements"
TAB_SEEN          = "Seen"
TAB_REVIEW        = "Review"

# Column headers for each tab (must match the relational Excel exactly)
COMPANY_COLS = [
    "company_id", "Company Name", "Type", "Year Founded",
    "Headquarters", "Capital Raised", "Description", "Website",
]
REACTOR_COLS = [
    "reactor_id", "Reactor Name", "Company (Developer)", "Category",
    "Capacity", "Coolant", "Fuel", "Reactor Type",
    "Development Stage", "Regulatory Status",
]
PROJECT_COLS = [
    "project_id", "Project Name", "Company (Developer)", "Reactor",
    "Site", "State", "Project Type", "Notes",
]
ANNOUNCEMENT_COLS = [
    "ann_id", "Date", "Company", "Reactor", "Project",
    "Deal / Announcement", "Partners", "Location", "Country",
    "Significance", "Type", "Capital Sources", "NRC Status",
    "Timeline", "Summary", "Source", "scraped_at",
]
REVIEW_COLS = [
    "review_id", "scraped_at", "article_title", "article_url",
    "raw_company", "raw_reactor", "raw_project",
    "issue", "raw_json",
]


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def clean(text):
    return " ".join(str(text).split()).strip() if text else ""

def entry_hash(title, link):
    return hashlib.md5(f"{title}|{link}".encode()).hexdigest()

def is_relevant(title, summary):
    text = (title + " " + summary).lower()
    return any(k in text for k in SIGNAL_KEYWORDS)

def normalize(s):
    """Lowercase, strip punctuation for fuzzy matching."""
    return re.sub(r"[^a-z0-9 ]", "", str(s).lower().strip())

def best_match(query, candidates):
    """
    Try to find a match for `query` in a list of (id, name) tuples.
    Returns the matching id, or None.
    Strategy: exact → normalized exact → substring
    """
    if not query or not candidates:
        return None
    q = clean(query)
    qn = normalize(q)
    # 1. exact match
    for cid, name in candidates:
        if clean(name) == q:
            return cid
    # 2. normalized exact
    for cid, name in candidates:
        if normalize(name) == qn:
            return cid
    # 3. substring (query contained in name, or name in query)
    for cid, name in candidates:
        nn = normalize(name)
        if qn and (qn in nn or nn in qn) and len(qn) >= 4:
            return cid
    return None


# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────

def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def ensure_tabs(spreadsheet):
    existing = [ws.title for ws in spreadsheet.worksheets()]
    defaults = {
        TAB_COMPANIES:     COMPANY_COLS,
        TAB_REACTORS:      REACTOR_COLS,
        TAB_PROJECTS:      PROJECT_COLS,
        TAB_ANNOUNCEMENTS: ANNOUNCEMENT_COLS,
        TAB_SEEN:          ["hash", "title", "source"],
        TAB_REVIEW:        REVIEW_COLS,
    }
    for name, headers in defaults.items():
        if name not in existing:
            ws = spreadsheet.add_worksheet(title=name, rows=2000, cols=len(headers) + 2)
            ws.append_row(headers)
            print(f"  Created tab: {name}")


def load_tab(spreadsheet, tab_name):
    """Return list of dicts (one per row, using header row as keys)."""
    ws = spreadsheet.worksheet(tab_name)
    time.sleep(1)  # avoid read quota bursts
    return ws.get_all_records()


def load_lookup(spreadsheet, tab_name, id_col, name_col):
    """Return list of (id, name) tuples for matching."""
    records = load_tab(spreadsheet, tab_name)
    return [(r[id_col], r[name_col]) for r in records if r.get(id_col) and r.get(name_col)]


# ── In-memory ID counters (loaded once at startup, incremented locally) ────────
_id_counters = {}

def init_id_counter(spreadsheet, tab_name, id_col):
    """Load the current max ID from the sheet once and cache it."""
    records = load_tab(spreadsheet, tab_name)
    ids = [int(r[id_col]) for r in records if str(r.get(id_col, "")).lstrip('-').isdigit()]
    _id_counters[tab_name] = max(ids, default=0)

def next_id(spreadsheet, tab_name, id_col):
    """Return next ID using in-memory counter — no extra API call."""
    if tab_name not in _id_counters:
        init_id_counter(spreadsheet, tab_name, id_col)
    _id_counters[tab_name] += 1
    return _id_counters[tab_name]


# ── Cached worksheet handles ───────────────────────────────────────────────────
_ws_cache = {}

def get_ws(spreadsheet, tab_name):
    """Return cached worksheet object to avoid repeated metadata fetches."""
    if tab_name not in _ws_cache:
        _ws_cache[tab_name] = spreadsheet.worksheet(tab_name)
        time.sleep(0.5)
    return _ws_cache[tab_name]


def append_row(spreadsheet, tab_name, cols, values_dict):
    """Append a row to a tab, ordered by cols list."""
    row = [values_dict.get(c, "") for c in cols]
    get_ws(spreadsheet, tab_name).append_row(row)
    time.sleep(0.5)  # avoid write quota bursts


def load_seen_hashes(spreadsheet):
    records = load_tab(spreadsheet, TAB_SEEN)
    return {r["hash"] for r in records if r.get("hash")}


def save_seen(spreadsheet, rows):
    if rows:
        get_ws(spreadsheet, TAB_SEEN).append_rows(rows)
        time.sleep(0.5)


# ─── UPSERT LOGIC ─────────────────────────────────────────────────────────────

def upsert_company(spreadsheet, company_name, company_lookup):
    """Find or create company. Returns company_id."""
    if not company_name:
        return None
    cid = best_match(company_name, company_lookup)
    if cid:
        return cid
    # Create new company row
    new_id = next_id(spreadsheet, TAB_COMPANIES, "company_id")
    append_row(spreadsheet, TAB_COMPANIES, COMPANY_COLS, {
        "company_id":     new_id,
        "Company Name":   clean(company_name),
        "Type":           "Unknown",
        "Year Founded":   "",
        "Headquarters":   "",
        "Capital Raised": "",
        "Description":    "Auto-created by scraper — please review and enrich.",
        "Website":        "",
    })
    company_lookup.append((new_id, company_name))
    print(f"    ✚ New company created: {company_name} (id={new_id})")
    return new_id


def upsert_reactor(spreadsheet, reactor_name, company_name, reactor_lookup):
    """Find or create reactor. Returns reactor_id."""
    if not reactor_name or reactor_name.lower() in ("unknown", "tbd", "n/a", ""):
        return None
    rid = best_match(reactor_name, reactor_lookup)
    if rid:
        return rid
    new_id = next_id(spreadsheet, TAB_REACTORS, "reactor_id")
    append_row(spreadsheet, TAB_REACTORS, REACTOR_COLS, {
        "reactor_id":        new_id,
        "Reactor Name":      clean(reactor_name),
        "Company (Developer)": clean(company_name) if company_name else "",
        "Category":          "Unknown",
        "Capacity":          "",
        "Coolant":           "",
        "Fuel":              "",
        "Reactor Type":      "",
        "Development Stage": "",
        "Regulatory Status": "Auto-created by scraper — please review and enrich.",
    })
    reactor_lookup.append((new_id, reactor_name))
    print(f"    ✚ New reactor created: {reactor_name} (id={new_id})")
    return new_id


def upsert_project(spreadsheet, project_name, company_name, reactor_name, project_lookup):
    """Find or create project. Returns project_id."""
    if not project_name or project_name.lower() in ("unknown", "tbd", "program-level", ""):
        return None
    pid = best_match(project_name, project_lookup)
    if pid:
        return pid
    new_id = next_id(spreadsheet, TAB_PROJECTS, "project_id")
    append_row(spreadsheet, TAB_PROJECTS, PROJECT_COLS, {
        "project_id":          new_id,
        "Project Name":        clean(project_name),
        "Company (Developer)": clean(company_name) if company_name else "",
        "Reactor":             clean(reactor_name) if reactor_name else "",
        "Site":                "",
        "State":               "",
        "Project Type":        "",
        "Notes":               "Auto-created by scraper — please review and enrich.",
    })
    project_lookup.append((new_id, project_name))
    print(f"    ✚ New project created: {project_name} (id={new_id})")
    return new_id


# ─── ARTICLE FETCHER ──────────────────────────────────────────────────────────

def fetch_article_text(url):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["nav", "footer", "script", "style", "header", "aside"]):
            tag.decompose()
        article = soup.find("article") or soup.find("main") or soup.body
        return clean(article.get_text(separator=" ", strip=True))[:9000] if article else ""
    except Exception as e:
        print(f"    fetch error: {e}")
        return ""


# ─── CLAUDE EXTRACTION ────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a nuclear energy industry analyst.

Extract structured deal data from the article. Return JSON only — no markdown, no explanation.

If the article does NOT describe a concrete nuclear deal, partnership, funding, licensing milestone,
or deployment announcement: {"skip": true}

If it IS relevant:
{
  "skip": false,
  "company": "primary reactor developer or lead entity",
  "reactor": "reactor technology name (e.g. Xe-100, Natrium, BWRX-300, Aurora) or 'unknown'",
  "project": "specific project or site name, or 'Program-level' if no specific site",
  "location": "City, State",
  "country": "country of deployment",
  "developer_country": "country of the reactor developer",
  "partners": "key partners, comma-separated",
  "deal": "short deal label, max 8 words",
  "date": "YYYY-MM-DD",
  "significance": "Signaling | Market development | Deployment",
  "type": "deal category (e.g. PPA, MOU, ARDP Award, License Extension, New Build, Restart)",
  "capital_sources": "funding sources and amounts if mentioned, else 'Not disclosed'",
  "nrc_status": "NRC regulatory status if mentioned, else 'unknown'",
  "timeline": "expected operation timeline if mentioned, else 'unknown'",
  "summary": "1-2 sentence factual summary",
  "source": "full article URL"
}

Significance levels:
- Signaling = MOU, letter of intent, site study, feasibility, early announcement
- Market development = Funded contract, licensing step, EPC award, design selection, DOE award
- Deployment = Construction permit, financial close, commercial operation, restart, PPA with firm date"""


def extract_deal(client, article, company_names, reactor_names, project_names):
    """Call Claude with context about existing entities to aid matching."""
    body = fetch_article_text(article["link"])

    context = f"""
Known companies: {', '.join(company_names[:40])}
Known reactors: {', '.join(reactor_names[:30])}
Known projects: {', '.join(project_names[:40])}

Use the exact spelling from the known lists above when the article refers to them.
If you identify a genuinely new company/reactor/project not in the lists, use your best name for it.
"""

    user_msg = f"""Title: {article['title']}
URL: {article['link']}
Published: {article.get('pub', '')}
Summary: {article.get('summary', '')}
Body: {body[:3500]}

{context}"""

    try:
        resp = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        # Strip markdown fences if present
        raw = re.sub(r"^```json\s*|\s*```$", "", raw, flags=re.DOTALL).strip()
        return json.loads(raw)
    except Exception as e:
        print(f"    Claude error: {e}")
        return None


# ─── RSS SCRAPER ──────────────────────────────────────────────────────────────

def scrape_feeds():
    candidates = []
    for feed_cfg in FEEDS:
        print(f"  Fetching: {feed_cfg['name']} ...", end=" ", flush=True)
        try:
            feed = feedparser.parse(feed_cfg["url"])
            matched = 0
            for entry in feed.entries[:80]:
                title   = clean(entry.get("title", ""))
                link    = entry.get("link", "")
                summary = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text()
                pub     = entry.get("published", "")
                if not title or not link:
                    continue
                if is_relevant(title, summary):
                    matched += 1
                    candidates.append({"title": title, "link": link, "summary": summary, "pub": pub})
            print(f"{len(feed.entries)} entries, {matched} matched")
        except Exception as e:
            print(f"ERROR: {e}")
    print(f"\n  TOTAL: {len(candidates)} candidates\n")
    return candidates


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    today = datetime.date.today().isoformat()
    now   = datetime.datetime.utcnow().isoformat()

    print(f"\n{'='*65}")
    print(f"Nuclear Deal Scraper (Relational) — {today}")
    print(f"{'='*65}\n")

    gc    = get_gsheet_client()
    sheet = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])
    ensure_tabs(sheet)

    # ── Load lookup tables ───────────────────────────────────────────
    print("  Loading lookup tables...")
    company_lookup = load_lookup(sheet, TAB_COMPANIES, "company_id", "Company Name")
    reactor_lookup = load_lookup(sheet, TAB_REACTORS,  "reactor_id", "Reactor Name")
    project_lookup = load_lookup(sheet, TAB_PROJECTS,  "project_id", "Project Name")
    seen_hashes    = load_seen_hashes(sheet)

    company_names = [name for _, name in company_lookup]
    reactor_names = [name for _, name in reactor_lookup]
    project_names = [name for _, name in project_lookup]

    existing_ann   = load_tab(sheet, TAB_ANNOUNCEMENTS)
    next_ann_id    = max([int(r["ann_id"]) for r in existing_ann if str(r.get("ann_id","")).isdigit()], default=0) + 1

    # ── Pre-cache worksheet handles & ID counters to avoid per-upsert API hits ──
    _id_counters[TAB_COMPANIES]     = max([int(cid) for cid, _ in company_lookup], default=0)
    _id_counters[TAB_REACTORS]      = max([int(rid) for rid, _ in reactor_lookup], default=0)
    _id_counters[TAB_PROJECTS]      = max([int(pid) for pid, _ in project_lookup], default=0)
    _id_counters[TAB_ANNOUNCEMENTS] = next_ann_id - 1
    for tab in [TAB_COMPANIES, TAB_REACTORS, TAB_PROJECTS, TAB_ANNOUNCEMENTS, TAB_SEEN, TAB_REVIEW]:
        get_ws(sheet, tab)  # pre-warm the cache

    print(f"  {len(company_lookup)} companies, {len(reactor_lookup)} reactors, {len(project_lookup)} projects")
    print(f"  {len(seen_hashes)} seen hashes, {len(existing_ann)} existing announcements\n")

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # ── Scrape feeds ─────────────────────────────────────────────────
    candidates = scrape_feeds()

    new_seen     = []
    new_ann      = []
    review_rows  = []
    review_id    = 1

    for article in candidates:
        h = entry_hash(article["title"], article["link"])
        if h in seen_hashes:
            continue

        print(f"  ⟳ {article['title'][:80]}")
        result = extract_deal(client, article, company_names, reactor_names, project_names)

        new_seen.append([h, article["title"], article["link"]])

        if not result or result.get("skip"):
            print("    ↷ Skipped (not a deal)")
            continue

        # Drop pre-2024 deals
        try:
            if int(result.get("date", "0")[:4]) < 2024:
                print("    ↷ Too old")
                continue
        except Exception:
            pass

        company_name = clean(result.get("company", ""))
        reactor_name = clean(result.get("reactor", ""))
        project_name = clean(result.get("project", ""))

        # ── Upsert into lookup tables ─────────────────────────────
        issues = []

        company_id = upsert_company(sheet, company_name, company_lookup)
        if not company_id:
            issues.append("no company")

        reactor_id = upsert_reactor(sheet, reactor_name, company_name, reactor_lookup)
        # reactor_id can be None (not every deal names a specific reactor)

        project_id = upsert_project(sheet, project_name, company_name, reactor_name, project_lookup)
        # project_id can be None (program-level deals ok)

        # ── Flag for review if company missing ────────────────────
        if issues:
            review_rows.append({
                "review_id":    review_id,
                "scraped_at":   now,
                "article_title":article["title"],
                "article_url":  article["link"],
                "raw_company":  company_name,
                "raw_reactor":  reactor_name,
                "raw_project":  project_name,
                "issue":        "; ".join(issues),
                "raw_json":     json.dumps(result)[:500],
            })
            review_id += 1
            print(f"    ⚠ Flagged for review: {'; '.join(issues)}")
            continue

        # ── Build announcement row ────────────────────────────────
        sig_icon = {
            "Deployment":         "🟢",
            "Market development": "🟡",
            "Signaling":          "🔵",
        }.get(result.get("significance", ""), "⚪")

        ann = {
            "ann_id":            next_id(sheet, TAB_ANNOUNCEMENTS, "ann_id"),
            "Date":              result.get("date", ""),
            "Company":           company_name,
            "Reactor":           reactor_name if reactor_name.lower() not in ("unknown","tbd","") else "",
            "Project":           project_name if project_name.lower() not in ("unknown","tbd","") else "",
            "Deal / Announcement": result.get("deal", ""),
            "Partners":          result.get("partners", ""),
            "Location":          result.get("location", ""),
            "Country":           result.get("country", ""),
            "Significance":      result.get("significance", ""),
            "Type":              result.get("type", ""),
            "Capital Sources":   result.get("capital_sources", ""),
            "NRC Status":        result.get("nrc_status", ""),
            "Timeline":          result.get("timeline", ""),
            "Summary":           result.get("summary", ""),
            "Source":            result.get("source", article["link"]),
            "scraped_at":        now,
        }
        new_ann.append(ann)

        # Keep lookups fresh for subsequent articles in same run
        if company_name and company_name not in company_names:
            company_names.append(company_name)
        if reactor_name and reactor_name not in reactor_names:
            reactor_names.append(reactor_name)
        if project_name and project_name not in project_names:
            project_names.append(project_name)

        print(f"    {sig_icon} NEW [{result.get('significance')}] {company_name} — {result.get('deal','')}")

    # ── Write to sheet ────────────────────────────────────────────────
    print()
    if new_ann:
        rows = [[a.get(c, "") for c in ANNOUNCEMENT_COLS] for a in new_ann]
        sheet.worksheet(TAB_ANNOUNCEMENTS).append_rows(rows)
        print(f"  ✅ Appended {len(new_ann)} new announcement(s)")
    else:
        print("  — No new announcements to append")

    if review_rows:
        rrows = [[r.get(c, "") for c in REVIEW_COLS] for r in review_rows]
        sheet.worksheet(TAB_REVIEW).append_rows(rrows)
        print(f"  ⚠  {len(review_rows)} item(s) flagged for review in '{TAB_REVIEW}' tab")

    save_seen(sheet, new_seen)
    print(f"\n  Done — {len(new_ann)} added, {len(review_rows)} for review | {today}\n")


if __name__ == "__main__":
    run()
