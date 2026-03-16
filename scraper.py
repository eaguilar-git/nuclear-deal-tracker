"""
nuclear_scraper.py  —  v2.2
===============================================
Scrapes nuclear industry RSS feeds, extracts structured deal data using
Claude, and writes to the 4-tab Google Sheet schema:
  Programs → Projects → Reactors → Announcements

Changes vs v2.1:
  - Added `Unlinked` column (separate from `Needs Review`)
    · Needs Review = content is questionable (low confidence, ambiguous extraction)
    · Unlinked     = deal has no matched project/program/reactor (needs entity assignment)
  - Review tab only receives rows where content is questionable, not merely unlinked

Changes vs v2:
  - Added missing `from email.utils import parsedate_to_datetime`
  - Fixed Deal ID sequencing: base computed once before loop, incremented
    by counter — avoids all-same-ID bug when batch-writing multiple deals
  - nuclear_only pre-filter for hyperscaler/finance feeds (suppresses noise)
  - Retry logic with back-off for Google Sheets 429 rate limit errors
  - Pass 1 body capped at 4,000 chars; Pass 2 uses full 9,000
  - MIN_YEAR filter applied before Pass 2 to save API calls
  - Robust date parsing with fallback to published_parsed tuple
  - Improved fetch: timeout handling, redirect following, figure tag removal
  - Cleaner logging output

Environment variables required
-------------------------------
  ANTHROPIC_API_KEY
  GOOGLE_SHEET_ID
  GOOGLE_SERVICE_ACCOUNT_JSON
  ANTHROPIC_MODEL  (optional, defaults to claude-haiku-4-5-20251001)
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


# ─── CONFIG ───────────────────────────────────────────────────────────────────

MODEL            = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
ESCALATION_MODEL = "claude-sonnet-4-6"
BODY_CHAR_LIMIT  = 9000
PASS1_CHAR_LIMIT = 4000
MIN_YEAR         = 2024

TAB_PROGRAMS      = "Programs"
TAB_PROJECTS      = "Projects"
TAB_REACTORS      = "Reactors"
TAB_ANNOUNCEMENTS = "Announcements"
TAB_SEEN          = "Seen"
TAB_REVIEW        = "Review"

ANNOUNCEMENT_COLS = [
    "Deal ID", "Reactor ID", "Project ID", "Program ID",
    "Deal Type", "Significance", "Impact", "Announcement Date",
    "Partners", "Deal Summary",
    "Capital Value (low)", "Capital Value (high)", "Capital Unit",
    "Value Type", "Capital Source",
    "USD Val low (calc.)", "USD Val high (calc.)",
    "Source URL", "Confidence", "Needs Review", "Unlinked",
]

REVIEW_COLS = ["Review ID", "Scraped At", "Article Title", "Article URL",
               "Reason", "Raw Extraction"]

SEEN_COLS = ["hash", "title", "url", "scraped_at"]


# ─── RSS FEEDS ────────────────────────────────────────────────────────────────
# nuclear_only=True: pre-filter entries to nuclear keywords before deal check
# (used for high-volume feeds covering many non-nuclear topics)

FEEDS = [
    # ── Tier 1: Industry trade press ──────────────────────────────────────────
    {"name": "World Nuclear News",       "url": "https://www.world-nuclear-news.org/rss"},
    {"name": "ANS Nuclear Newswire",     "url": "https://www.ans.org/news/feed"},
    {"name": "NEI News",                 "url": "https://www.nei.org/rss"},
    {"name": "Power Magazine Nuclear",   "url": "https://www.powermag.com/category/nuclear/feed/"},
    {"name": "Power Engineering",        "url": "https://www.power-eng.com/feed/"},
    {"name": "Utility Dive",             "url": "https://www.utilitydive.com/feeds/news/"},
    {"name": "Neutron Bytes",            "url": "https://neutronbytes.com/feed/"},
    {"name": "Nuclear Engineering Intl", "url": "https://www.neimagazine.com/rss"},
    {"name": "Canary Media",             "url": "https://www.canarymedia.com/rss"},
    {"name": "Latitude Media",           "url": "https://www.latitudemedia.com/feed"},
    {"name": "Atomic Insights",          "url": "https://atomicinsights.com/feed"},
    {"name": "NucNet",                   "url": "https://nucnet.org/feed.rss"},

    # ── Tier 2: Government & regulatory ───────────────────────────────────────
    {"name": "DOE Nuclear Energy",       "url": "https://www.energy.gov/ne/rss.xml"},
    {"name": "DOE News",                 "url": "https://www.energy.gov/news/rss.xml"},
    {"name": "NRC News",                 "url": "https://www.nrc.gov/reading-rm/doc-collections/news/rss.xml"},
    {"name": "NRC Press Releases",       "url": "https://www.nrc.gov/reading-rm/doc-collections/press-releases/rss.xml"},
    {"name": "IAEA Nuclear Power",       "url": "https://www.iaea.org/feeds/topical/nuclear-power.xml"},
    {"name": "IAEA Newscenter",          "url": "https://www.iaea.org/newscenter/feed"},

    # ── Tier 3: Company newsrooms ──────────────────────────────────────────────
    {"name": "Holtec News",              "url": "https://holtecinternational.com/feed/"},
    {"name": "NANO Nuclear IR",          "url": "https://ir.nanonuclearenergy.com/rss/news-releases.xml"},
    {"name": "Helion Energy",            "url": "https://www.helionenergy.com/feed/"},
    {"name": "TerraPower News",          "url": "https://www.terrapower.com/news/feed/"},
    {"name": "Kairos Power",             "url": "https://kairospower.com/news/feed/"},
    {"name": "Oklo IR",                  "url": "https://ir.oklo.com/news-releases/rss"},
    {"name": "X-energy News",            "url": "https://x-energy.com/news/feed/"},
    {"name": "Commonwealth Fusion",      "url": "https://cfs.energy/news/feed/"},
    {"name": "GE Vernova Newsroom",      "url": "https://www.gevernova.com/news/press-releases/rss"},
    {"name": "Westinghouse Newsroom",    "url": "https://www.westinghousenuclear.com/about/news/rss"},
    {"name": "NuScale IR",               "url": "https://ir.nuscalepower.com/news-releases/rss"},
    {"name": "TVA Newsroom",             "url": "https://www.tva.com/rss/news"},
    {"name": "Duke Energy News",         "url": "https://news.duke-energy.com/rss/all.rss"},
    {"name": "Dominion Energy News",     "url": "https://news.dominionenergy.com/press-releases/rss"},
    {"name": "Constellation IR",         "url": "https://ir.constellationenergy.com/news-releases/rss"},
    {"name": "Southern Company News",    "url": "https://www.southerncompany.com/news/rss"},
    # Hyperscalers / finance — nuclear_only=True filters out non-nuclear noise
    {"name": "Google Blog",              "url": "https://blog.google/rss/",                       "nuclear_only": True},
    {"name": "Microsoft On the Issues",  "url": "https://blogs.microsoft.com/on-the-issues/feed/","nuclear_only": True},
    {"name": "Meta Newsroom",            "url": "https://about.fb.com/rss/",                      "nuclear_only": True},
    {"name": "Amazon About",             "url": "https://www.aboutamazon.com/news/rss",           "nuclear_only": True},
    {"name": "Brookfield IR",            "url": "https://bam.brookfield.com/news-releases/rss",   "nuclear_only": True},
]

DEAL_KEYWORDS = [
    "agreement", "deal", "contract", "ppa", "power purchase",
    "mou", "memorandum", "partnership", "collaboration",
    "investment", "funding", "loan", "grant", "award",
    "financing", "license renewal", "license extension",
    "restart", "new build", "construction permit",
    "offtake", "signed", "announced", "selected",
    "smr", "small modular", "advanced reactor",
]

NUCLEAR_KEYWORDS = [
    "nuclear", "reactor", "smr", "fission", "fusion",
    "uranium", "atomic", "nrc", "doe nuclear",
]


# ─── PROMPTS ──────────────────────────────────────────────────────────────────

PASS1_SYSTEM = """You are a nuclear industry analyst. Decide whether a news article
describes a concrete nuclear energy deal, agreement, financing event, license action,
or deployment milestone to track in a deal database.
Respond with JSON only. No prose, no markdown fences."""

PASS1_USER_TMPL = """Article title: {title}
Article text:
{body}

Does this article describe a trackable nuclear deal or milestone?
Trackable: signed/announced agreement (PPA, MOU, JDA, EPC), financing event
(equity, debt, grant, DOE award), license action (renewal, restart approval,
construction permit filed/approved), deployment milestone (COL filing, site
permit, construction start), significant offtake or supply commitment.

NOT trackable: opinion/analysis/retrospective with no new concrete action,
policy speculation, earnings reports with no specific deal, articles only
referencing prior deals with no new development.

Respond with exactly this JSON:
{{"include": true/false, "reason": "one sentence"}}"""


PASS2_SYSTEM = """You are a nuclear industry deal analyst. Extract structured data from
nuclear energy news articles. Return only valid JSON, no prose, no markdown fences.

SIGNIFICANCE (pick highest applicable):
- "Deployment"          → construction, licensing, restart
- "Financing"           → capital commitment with specific dollar amount
- "Offtake"             → binding/near-binding PPA or supply agreement
- "Market development"  → preorders, site options, feasibility with capital at risk
- "Signaling"           → non-binding MOUs, intent without committed capital
- "Policy / Regulatory" → government action, legislation, NRC decisions

IMPACT:
- "High"   → >$500M or >500 MW or landmark first-of-kind
- "Medium" → $50M–$500M or 50–500 MW or notable
- "Low"    → <$50M or <50 MW or early-stage

DEAL TYPES (use exactly one):
PPA, MOU, JDA, EPC, Financing, Grant, License, COLA Filing, ARDP Award,
Collaboration, Preorder, DoD Contract, State Legislation / Grant,
NRC License Renewal, NRC SLR Approval, SLR Application,
Early Site Permit Application, Strategic Partnership, Announcement,
Master Power Agreement, Funding Agreement, Other

CAPITAL UNIT: USD millions | USD billions | USD thousands |
EUR millions | EUR billions | GBP millions | GBP billions |
JPY billions | CAD millions | CAD billions

VALUE TYPE: Exact | Up to | At least | Range
CAPITAL SOURCE: Equity | Debt | Grant | PPA | Mixed | Undisclosed
CONFIDENCE: High | Medium | Low

ENTITY ATTACHMENT (one should be set; default to project_id):
- project_id  if deal ties to a specific project in reference list
- program_id  if program-level deal with no specific project match
- reactor_id  if a specific reactor unit is explicitly named
- All null only if truly unidentifiable (set needs_review = true)"""


PASS2_USER_TMPL = """Article title: {title}
Article URL: {url}
Article date: {date}

Article text:
{body}

Known programs:
{programs}

Known projects:
{projects}

Known reactors:
{reactors}

Existing announcement fingerprints (avoid duplicating):
{fingerprints}

Extract and return this JSON (null for unknown fields):
{{
  "deal_type":           "string",
  "significance":        "string",
  "impact":              "High|Medium|Low",
  "announcement_date":   "YYYY-MM-DD or YYYY-MM or YYYY",
  "partners":            "comma-separated counterparties",
  "deal_summary":        "1-2 sentence factual description",
  "capital_value_low":   number or null,
  "capital_value_high":  number or null,
  "capital_unit":        "string or null",
  "value_type":          "Exact|Up to|At least|Range or null",
  "capital_source":      "string",
  "project_id":          "PROJ-XXX or null",
  "program_id":          "PROG-XXX or null",
  "reactor_id":          "REAC-XXX or null",
  "confidence":          "High|Medium|Low",
  "needs_review":        true/false,
  "is_duplicate":        true/false,
  "duplicate_reason":    "string or null"
}}"""


# ─── FX RATES ─────────────────────────────────────────────────────────────────

FX_TO_USD = {
    "USD millions":  1.0,     "USD billions":  1000.0,  "USD thousands": 0.001,
    "EUR millions":  1.08,    "EUR billions":  1080.0,
    "GBP millions":  1.27,    "GBP billions":  1270.0,
    "JPY billions":  0.0067,
    "CAD millions":  0.74,    "CAD billions":  740.0,
}

def to_usd_millions(value, unit):
    if value is None or not unit:
        return None
    rate = FX_TO_USD.get(unit)
    return round(float(value) * rate, 2) if rate is not None else None


# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────

def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def load_tab(spreadsheet, tab_name, retries=3):
    """Load a sheet tab as list of dicts, with retry on 429."""
    for attempt in range(retries):
        try:
            ws = spreadsheet.worksheet(tab_name)
            time.sleep(0.5)
            return ws.get_all_records()
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < retries - 1:
                wait = 20 * (attempt + 1)
                print(f"    Rate limited — waiting {wait}s...")
                time.sleep(wait)
            else:
                raise


def ensure_tab(spreadsheet, tab_name, headers):
    existing = [ws.title for ws in spreadsheet.worksheets()]
    if tab_name not in existing:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=2000, cols=len(headers) + 2)
        ws.append_row(headers)
        print(f"  Created tab: {tab_name}")
    return spreadsheet.worksheet(tab_name)


def append_rows_batch(spreadsheet, tab_name, rows, retries=3):
    if not rows:
        return
    for attempt in range(retries):
        try:
            ws = spreadsheet.worksheet(tab_name)
            headers = ws.row_values(1)
            values = [[str(row.get(h, "") or "") for h in headers] for row in rows]
            ws.append_rows(values, value_input_option="RAW")
            time.sleep(1)
            return
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < retries - 1:
                wait = 20 * (attempt + 1)
                print(f"    Rate limited on write — waiting {wait}s...")
                time.sleep(wait)
            else:
                raise


def get_next_deal_num(spreadsheet):
    """Read Announcements once and return the next integer. Call ONCE before loop."""
    try:
        ws = spreadsheet.worksheet(TAB_ANNOUNCEMENTS)
        existing = [v for v in ws.col_values(1)[1:] if v.startswith("DEAL-")]
        nums = []
        for v in existing:
            try:
                nums.append(int(v.split("-")[1]))
            except Exception:
                pass
        return max(nums) + 1 if nums else 1
    except Exception:
        return 1


def load_seen_hashes(spreadsheet):
    try:
        ws = spreadsheet.worksheet(TAB_SEEN)
        return set(ws.col_values(1)[1:])
    except Exception:
        return set()


# ─── REFERENCE DATA ───────────────────────────────────────────────────────────

def load_reference_data(spreadsheet):
    print("  Loading Programs...")
    programs = load_tab(spreadsheet, TAB_PROGRAMS)
    program_lookup = [(r["Program ID"], r["Program Name"])
                      for r in programs if r.get("Program ID")]

    print("  Loading Projects...")
    projects = load_tab(spreadsheet, TAB_PROJECTS)
    project_lookup = [(r["Project ID"], r["Project Name"], r.get("Primary Lead", ""))
                      for r in projects if r.get("Project ID")]

    print("  Loading Reactors...")
    reactors = load_tab(spreadsheet, TAB_REACTORS)
    reactor_lookup = [(r["Reactor ID"], r["Unit Name / Number"], r.get("Project ID (opt.)", ""))
                      for r in reactors if r.get("Reactor ID")]

    print("  Loading existing Announcements for deduplication...")
    try:
        announcements = load_tab(spreadsheet, TAB_ANNOUNCEMENTS)
        fingerprints = [
            f"{r.get('Project ID','')}/{r.get('Program ID','')}"
            f"/{r.get('Deal Type','')}/{str(r.get('Deal Summary',''))[:80]}"
            for r in announcements
        ]
    except Exception:
        fingerprints = []

    return {
        "program_lookup": program_lookup,
        "project_lookup": project_lookup,
        "reactor_lookup": reactor_lookup,
        "fingerprints":   fingerprints,
    }


# ─── UTILITIES ────────────────────────────────────────────────────────────────

def clean(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()

def entry_hash(title, url):
    return hashlib.md5(f"{title}{url}".encode()).hexdigest()

def has_nuclear_keyword(text):
    t = text.lower()
    return any(kw in t for kw in NUCLEAR_KEYWORDS)

def is_deal_candidate(title, summary):
    t = (title + " " + summary).lower()
    return any(kw in t for kw in DEAL_KEYWORDS)

def format_reference_list(items, max_items=60):
    lines = []
    for item in items[:max_items]:
        if len(item) == 2:
            lines.append(f"  {item[0]}: {item[1]}")
        elif len(item) == 3:
            lines.append(f"  {item[0]}: {item[1]} (lead: {item[2]})")
    if len(items) > max_items:
        lines.append(f"  ... and {len(items) - max_items} more")
    return "\n".join(lines) if lines else "  (none)"

def parse_entry_date(entry):
    """Safely parse RSS entry date to YYYY-MM-DD string."""
    if entry.get("published"):
        try:
            return parsedate_to_datetime(entry.published).strftime("%Y-%m-%d")
        except Exception:
            pass
    if entry.get("published_parsed"):
        try:
            return datetime.date(*entry.published_parsed[:3]).isoformat()
        except Exception:
            pass
    return entry.get("published", "")[:10]


# ─── ARTICLE FETCHER ──────────────────────────────────────────────────────────

def fetch_article_text(url):
    """Fetch and clean article body, capped at BODY_CHAR_LIMIT chars."""
    try:
        r = requests.get(
            url, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NuclearDealBot/2.1)"},
            allow_redirects=True,
        )
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header",
                         "aside", "form", "iframe", "noscript", "figure"]):
            tag.decompose()
        body = (
            soup.find("article") or
            soup.find("main") or
            soup.find(class_=re.compile(r"article|content|story|post|body", re.I)) or
            soup.body
        )
        text = clean(body.get_text(separator=" ")) if body else ""
        return text[:BODY_CHAR_LIMIT]
    except requests.exceptions.Timeout:
        print("    Fetch timeout")
        return ""
    except Exception as e:
        print(f"    Fetch error: {e}")
        return ""


# ─── CLAUDE CALLS ─────────────────────────────────────────────────────────────

def call_claude(client, system, user_msg, model=None):
    """Single Claude call, returns parsed JSON dict or None."""
    use_model = model or MODEL
    try:
        resp = client.messages.create(
            model=use_model,
            max_tokens=1400,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.DOTALL).strip()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"    JSON parse error ({use_model}): {e}")
        return None
    except anthropic.APIStatusError as e:
        print(f"    API error ({use_model}): {e.status_code} {e.message}")
        return None
    except Exception as e:
        print(f"    Claude error ({use_model}): {e}")
        return None


def unwrap_result(result):
    """If Claude returned a list, unwrap first dict element."""
    if isinstance(result, list):
        return result[0] if result and isinstance(result[0], dict) else None
    return result


def pass1_include(client, title, body):
    user_msg = PASS1_USER_TMPL.format(title=title, body=body[:PASS1_CHAR_LIMIT])
    result = call_claude(client, PASS1_SYSTEM, user_msg)
    if not result:
        return False, "claude error"
    return bool(result.get("include", False)), result.get("reason", "")


def pass2_extract(client, article, ref_data, model=None):
    user_msg = PASS2_USER_TMPL.format(
        title=article["title"],
        url=article["link"],
        date=article.get("date", "unknown"),
        body=article.get("body", "")[:BODY_CHAR_LIMIT],
        programs=format_reference_list(ref_data["program_lookup"]),
        projects=format_reference_list(ref_data["project_lookup"]),
        reactors=format_reference_list(ref_data["reactor_lookup"]),
        fingerprints=(
            "\n".join(f"  {f}" for f in ref_data["fingerprints"][-60:])
            or "  (none)"
        ),
    )
    result = call_claude(client, PASS2_SYSTEM, user_msg, model=model)
    return unwrap_result(result)


# ─── RSS SCRAPER ──────────────────────────────────────────────────────────────

def scrape_feeds():
    """Fetch all RSS feeds and return deduplicated candidate articles."""
    candidates = []
    failed = []

    for feed_cfg in FEEDS:
        name         = feed_cfg["name"]
        nuclear_only = feed_cfg.get("nuclear_only", False)
        print(f"  Fetching: {name} ...", end=" ", flush=True)
        try:
            feed = feedparser.parse(feed_cfg["url"])
            if feed.bozo and not feed.entries:
                print("SKIP (unreadable)")
                failed.append(name)
                continue

            matched = 0
            for entry in feed.entries[:80]:
                title   = clean(entry.get("title", ""))
                link    = entry.get("link", "")
                summary = clean(
                    BeautifulSoup(entry.get("summary", ""), "html.parser").get_text()
                )
                if not title or not link:
                    continue
                # Nuclear pre-filter for high-volume non-nuclear feeds
                if nuclear_only and not has_nuclear_keyword(title + " " + summary):
                    continue
                if is_deal_candidate(title, summary):
                    candidates.append({
                        "title":   title,
                        "link":    link,
                        "summary": summary,
                        "date":    parse_entry_date(entry),
                        "source":  name,
                    })
                    matched += 1

            print(f"{matched} candidates")

        except Exception as e:
            print(f"ERROR: {e}")
            failed.append(name)

    if failed:
        print(f"\n  ⚠ {len(failed)} feeds skipped: {', '.join(failed)}")

    # Deduplicate by URL
    seen_links = set()
    unique = []
    for c in candidates:
        if c["link"] not in seen_links:
            seen_links.add(c["link"])
            unique.append(c)
    return unique


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    today = datetime.date.today().isoformat()
    now   = datetime.datetime.utcnow().isoformat()

    print(f"\n{'='*65}")
    print(f"Nuclear Deal Scraper v2.1 — {today}")
    print(f"{'='*65}\n")

    # Clients
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    gc     = get_gsheet_client()
    sheet  = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

    # Ensure operational tabs
    ensure_tab(sheet, TAB_SEEN,   SEEN_COLS)
    ensure_tab(sheet, TAB_REVIEW, REVIEW_COLS)

    # Load reference data
    print("Loading reference data...")
    ref = load_reference_data(sheet)
    seen_hashes = load_seen_hashes(sheet)
    print(
        f"  {len(ref['program_lookup'])} programs, "
        f"{len(ref['project_lookup'])} projects, "
        f"{len(ref['reactor_lookup'])} reactors, "
        f"{len(ref['fingerprints'])} existing deals\n"
    )

    # Compute base Deal ID ONCE before loop to avoid all-same-ID bug
    base_deal_num = get_next_deal_num(sheet)
    print(f"  Next Deal ID: DEAL-{base_deal_num:03d}\n")

    # Scrape
    print("Scraping feeds...")
    candidates = scrape_feeds()
    print(f"\n  {len(candidates)} total candidates after dedup\n")

    # Counters
    new_seen    = []
    new_ann     = []
    review_rows = []
    review_num  = 1
    skipped     = 0
    duplicates  = 0
    already_seen_count = 0

    for article in candidates:
        h = entry_hash(article["title"], article["link"])

        if h in seen_hashes:
            already_seen_count += 1
            continue

        print(f"  ⟳ {article['title'][:80]}")

        # MIN_YEAR filter before Pass 2 to save API cost
        year_str = article.get("date", "")[:4]
        try:
            if year_str and int(year_str) < MIN_YEAR:
                print(f"    ↷ Too old ({year_str})")
                new_seen.append({"hash": h, "title": article["title"],
                                 "url": article["link"], "scraped_at": now})
                continue
        except ValueError:
            pass

        # Fetch full article body
        body = fetch_article_text(article["link"])
        article["body"] = body if body else article["summary"]

        # Pass 1: include/exclude
        include, reason = pass1_include(client, article["title"], article["body"])
        if not include:
            print(f"    ↷ Excluded: {reason}")
            new_seen.append({"hash": h, "title": article["title"],
                             "url": article["link"], "scraped_at": now})
            skipped += 1
            continue

        print(f"    ✓ Included: {reason}")

        # Pass 2: structured extraction
        result = pass2_extract(client, article, ref)

        # Escalate on low confidence
        if result and result.get("confidence") == "Low":
            print(f"    ↑ Escalating to {ESCALATION_MODEL}")
            escalated = pass2_extract(client, article, ref, model=ESCALATION_MODEL)
            if escalated:
                result = escalated

        if not result:
            print("    ✗ Extraction failed → Review")
            review_rows.append({
                "Review ID":     f"REV-{review_num:03d}",
                "Scraped At":    now,
                "Article Title": article["title"],
                "Article URL":   article["link"],
                "Reason":        "extraction failed",
                "Raw Extraction": "",
            })
            review_num += 1
            new_seen.append({"hash": h, "title": article["title"],
                             "url": article["link"], "scraped_at": now})
            continue

        # Duplicate check
        if result.get("is_duplicate"):
            print(f"    ≡ Duplicate: {result.get('duplicate_reason', '')}")
            duplicates += 1
            new_seen.append({"hash": h, "title": article["title"],
                             "url": article["link"], "scraped_at": now})
            continue

        # Assign Deal ID: base + count of deals already added this run
        deal_id = f"DEAL-{base_deal_num + len(new_ann):03d}"

        # USD conversion
        cap_low  = result.get("capital_value_low")
        cap_high = result.get("capital_value_high")
        cap_unit = result.get("capital_unit")
        usd_low  = to_usd_millions(cap_low,  cap_unit)
        usd_high = to_usd_millions(cap_high, cap_unit)

        # Separate concerns:
        # - unlinked     = no entity match (needs someone to assign a project/program)
        # - needs_review = content is questionable (low confidence or Claude flagged it)
        unlinked = not any([result.get("project_id"),
                            result.get("program_id"),
                            result.get("reactor_id")])
        needs_review = (
            result.get("needs_review", False) or
            result.get("confidence") == "Low"
        )

        ann = {
            "Deal ID":              deal_id,
            "Reactor ID":           result.get("reactor_id") or "",
            "Project ID":           result.get("project_id") or "",
            "Program ID":           result.get("program_id") or "",
            "Deal Type":            result.get("deal_type", ""),
            "Significance":         result.get("significance", ""),
            "Impact":               result.get("impact", ""),
            "Announcement Date":    result.get("announcement_date", article.get("date", "")),
            "Partners":             result.get("partners", ""),
            "Deal Summary":         result.get("deal_summary", ""),
            "Capital Value (low)":  cap_low  if cap_low  is not None else "",
            "Capital Value (high)": cap_high if cap_high is not None else "",
            "Capital Unit":         cap_unit or "",
            "Value Type":           result.get("value_type", "") or "",
            "Capital Source":       result.get("capital_source", ""),
            "USD Val low (calc.)":  usd_low  if usd_low  is not None else "",
            "USD Val high (calc.)": usd_high if usd_high is not None else "",
            "Source URL":           article["link"],
            "Confidence":           result.get("confidence", "Medium"),
            "Needs Review":         "Yes" if needs_review else "No",
            "Unlinked":             "Yes" if unlinked else "No",
        }
        new_ann.append(ann)

        # Update in-memory fingerprints for same-run dedup
        ref["fingerprints"].append(
            f"{ann['Project ID']}/{ann['Program ID']}"
            f"/{ann['Deal Type']}/{ann['Deal Summary'][:80]}"
        )

        conf_icon     = {"High": "🟢", "Medium": "🟡", "Low": "🔴"}.get(ann["Confidence"], "⚪")
        entity_str    = ann["Project ID"] or ann["Program ID"] or ann["Reactor ID"] or "⚠ unlinked"
        review_flag   = " → REVIEW" if needs_review else ""
        unlinked_flag = " → UNLINKED" if unlinked else ""
        print(f"    ✚ {deal_id} | {ann['Deal Type']} | {ann['Significance']} | {ann['Confidence']} {conf_icon}{review_flag}{unlinked_flag}")
        print(f"      Entity: {entity_str}")

        # Only send to Review tab when content is questionable (not just unlinked)
        if needs_review:
            review_rows.append({
                "Review ID":     f"REV-{review_num:03d}",
                "Scraped At":    now,
                "Article Title": article["title"],
                "Article URL":   article["link"],
                "Reason":        f"confidence={result.get('confidence')}; needs_review={result.get('needs_review')}",
                "Raw Extraction": json.dumps(result),
            })
            review_num += 1

        new_seen.append({"hash": h, "title": article["title"],
                         "url": article["link"], "scraped_at": now})
        time.sleep(0.5)

    # Batch writes
    print(f"\n{'─'*65}")
    print(f"Run summary — {today}")
    print(f"  Total candidates     : {len(candidates)}")
    print(f"  Already seen (skip)  : {already_seen_count}")
    print(f"  Excluded (pass 1)    : {skipped}")
    print(f"  Duplicates           : {duplicates}")
    print(f"  New deals written    : {len(new_ann)}")
    print(f"  Sent to Review       : {len(review_rows)}")

    if new_ann:
        print(f"\nWriting {len(new_ann)} announcements...")
        append_rows_batch(sheet, TAB_ANNOUNCEMENTS, new_ann)
        print("  ✓ Done.")

    if review_rows:
        print(f"Writing {len(review_rows)} review items...")
        append_rows_batch(sheet, TAB_REVIEW, review_rows)
        print("  ✓ Done.")

    if new_seen:
        print(f"Logging {len(new_seen)} seen hashes...")
        append_rows_batch(sheet, TAB_SEEN, new_seen)
        print("  ✓ Done.")

    print(f"\n✓ Scraper complete — {today}\n")


if __name__ == "__main__":
    run()
