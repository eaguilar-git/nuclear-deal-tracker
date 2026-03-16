"""
nuclear_scraper.py  —  v2 (relational schema)
===============================================
Scrapes nuclear industry RSS feeds, extracts structured deal data using
Claude, and writes to the new 4-tab Google Sheet schema:
  Programs → Projects → Reactors → Announcements

Architecture
------------
1. Load reference data  — read Programs + Projects from Sheet at startup
2. Scrape feeds         — fetch RSS entries, filter by deal keywords
3. Fetch article text   — pull full body (~8k chars) for each candidate
4. Two-pass extraction  — Pass 1: include/exclude decision
                          Pass 2: structured field extraction (if included)
5. Deduplicate          — fingerprint against existing Announcements rows
6. Write output         — append to Announcements; flag low-confidence rows

Environment variables required
-------------------------------
  ANTHROPIC_API_KEY
  GOOGLE_SHEET_ID          (new sheet: 1K_Be4gnuxUrWaTR_L_2FKwHH9l6q9UYgtfkFBDLznBA)
  GOOGLE_SERVICE_ACCOUNT_JSON  (service account JSON as a string)
  ANTHROPIC_MODEL          (optional, defaults to claude-haiku-4-5-20251001)
"""

import os
import re
import json
import time
import hashlib
import datetime

import feedparser
import requests
from bs4 import BeautifulSoup
import anthropic
import gspread
from google.oauth2.service_account import Credentials


# ─── CONFIG ───────────────────────────────────────────────────────────────────

MODEL           = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
ESCALATION_MODEL = "claude-sonnet-4-6"   # used when confidence is low
BODY_CHAR_LIMIT = 9000                   # article body text cap
MIN_YEAR        = 2024                   # drop deals announced before this

# Tab names (must match Google Sheet exactly)
TAB_PROGRAMS      = "Programs"
TAB_PROJECTS      = "Projects"
TAB_REACTORS      = "Reactors"
TAB_ANNOUNCEMENTS = "Announcements"
TAB_SEEN          = "Seen"        # dedup hash log
TAB_REVIEW        = "Review"      # low-confidence / needs-human-review

# Announcements column order (must match sheet col order exactly)
ANNOUNCEMENT_COLS = [
    "Deal ID",
    "Reactor ID",
    "Project ID",
    "Program ID",
    "Deal Type",
    "Significance",
    "Impact",
    "Announcement Date",
    "Partners",
    "Deal Summary",
    "Capital Value (low)",
    "Capital Value (high)",
    "Capital Unit",
    "Value Type",
    "Capital Source",
    "USD Val low (calc.)",
    "USD Val high (calc.)",
    "Source URL",
    "Confidence",
    "Needs Review",
]

REVIEW_COLS = [
    "Review ID",
    "Scraped At",
    "Article Title",
    "Article URL",
    "Reason",
    "Raw Extraction",
]

SEEN_COLS = ["hash", "title", "url", "scraped_at"]

# ─── RSS FEEDS ────────────────────────────────────────────────────────────────
# Three tiers:
#   1. Industry trade press       — primary deal coverage
#   2. Government / regulatory    — NRC, DOE, IAEA official announcements
#   3. Company newsrooms          — press releases from companies in our tracker

FEEDS = [

    # ── Tier 1: Industry trade press ─────────────────────────────────────────
    {"name": "World Nuclear News",      "url": "https://www.world-nuclear-news.org/rss"},
    {"name": "ANS Nuclear Newswire",    "url": "https://www.ans.org/news/feed"},
    {"name": "NEI News",                "url": "https://www.nei.org/rss"},
    {"name": "Power Magazine – Nuclear","url": "https://www.powermag.com/category/nuclear/feed/"},
    {"name": "Power Engineering",       "url": "https://www.power-eng.com/feed/"},
    {"name": "Utility Dive",            "url": "https://www.utilitydive.com/feeds/news/"},
    {"name": "Neutron Bytes",           "url": "https://neutronbytes.com/feed/"},
    {"name": "Nuclear Engineering Intl","url": "https://www.neimagazine.com/rss"},
    {"name": "Canary Media",            "url": "https://www.canarymedia.com/rss"},
    {"name": "Latitude Media",          "url": "https://www.latitudemedia.com/feed"},
    {"name": "Atomic Insights",         "url": "https://atomicinsights.com/feed"},
    {"name": "NucNet",                  "url": "https://nucnet.org/feed.rss"},

    # ── Tier 2: Government & regulatory ──────────────────────────────────────
    {"name": "DOE Nuclear Energy",      "url": "https://www.energy.gov/ne/rss.xml"},
    {"name": "DOE News",                "url": "https://www.energy.gov/news/rss.xml"},
    {"name": "NRC News",                "url": "https://www.nrc.gov/reading-rm/doc-collections/news/rss.xml"},
    {"name": "NRC Press Releases",      "url": "https://www.nrc.gov/reading-rm/doc-collections/press-releases/rss.xml"},
    {"name": "IAEA Nuclear Power",      "url": "https://www.iaea.org/feeds/topical/nuclear-power.xml"},
    {"name": "IAEA Newscenter",         "url": "https://www.iaea.org/newscenter/feed"},

    # ── Tier 3: Company newsrooms ─────────────────────────────────────────────
    # Developers / vendors
    {"name": "Holtec News",             "url": "https://holtecinternational.com/feed/"},
    {"name": "NANO Nuclear IR",         "url": "https://ir.nanonuclearenergy.com/rss/news-releases.xml"},
    {"name": "Helion Energy",           "url": "https://www.helionenergy.com/feed/"},
    {"name": "TerraPower News",         "url": "https://www.terrapower.com/news/feed/"},
    {"name": "Kairos Power",            "url": "https://kairospower.com/news/feed/"},
    {"name": "Oklo IR",                 "url": "https://ir.oklo.com/news-releases/rss"},
    {"name": "X-energy News",           "url": "https://x-energy.com/news/feed/"},
    {"name": "Commonwealth Fusion",     "url": "https://cfs.energy/news/feed/"},
    {"name": "GE Vernova Newsroom",     "url": "https://www.gevernova.com/news/press-releases/rss"},
    {"name": "Westinghouse Newsroom",   "url": "https://www.westinghousenuclear.com/about/news/rss"},
    {"name": "NuScale IR",              "url": "https://ir.nuscalepower.com/news-releases/rss"},
    # Utilities
    {"name": "TVA Newsroom",            "url": "https://www.tva.com/rss/news"},
    {"name": "Duke Energy News",        "url": "https://news.duke-energy.com/rss/all.rss"},
    {"name": "Dominion Energy News",    "url": "https://news.dominionenergy.com/press-releases/rss"},
    {"name": "Constellation IR",        "url": "https://ir.constellationenergy.com/news-releases/rss"},
    {"name": "Southern Company News",   "url": "https://www.southerncompany.com/news/rss"},
    # Tech / hyperscalers
    {"name": "Google Blog",             "url": "https://blog.google/rss/",              "nuclear_only": True},
    {"name": "Microsoft On the Issues", "url": "https://blogs.microsoft.com/on-the-issues/feed/", "nuclear_only": True},
    {"name": "Meta Newsroom",           "url": "https://about.fb.com/rss/",             "nuclear_only": True},
    {"name": "Amazon About",            "url": "https://www.aboutamazon.com/news/rss",  "nuclear_only": True},
    # Finance
    {"name": "Brookfield IR",           "url": "https://bam.brookfield.com/news-releases/rss", "nuclear_only": True},
]

# Keywords that flag an article as a potential deal
DEAL_KEYWORDS = [
    "agreement", "deal", "contract", "ppa", "power purchase",
    "mou", "memorandum", "partnership", "collaboration",
    "investment", "funding", "loan", "grant", "award",
    "financing", "license renewal", "license extension",
    "restart", "new build", "construction permit",
    "offtake", "signed", "announced", "selected",
    "smr", "small modular", "advanced reactor",
]

# Extra nuclear topic keywords used to pre-filter noisy broad-topic feeds
# (feeds tagged nuclear_only=True must pass at least one of these)
NUCLEAR_KEYWORDS = [
    "nuclear", "reactor", "smr", "uranium", "fission", "fusion",
    "atomic", "nrc", "doe nuclear", "advanced reactor", "small modular",
    "natrium", "ap1000", "bwrx", "xe-100", "kairos", "terrapower",
    "holtec", "oklo", "x-energy", "commonwealth fusion", "helion",
    "palisades", "diablo", "three mile", "vogtle", "surry",
]


# ─── PROMPTS ──────────────────────────────────────────────────────────────────

PASS1_SYSTEM = """You are a nuclear industry analyst. Your job is to read a news article
and decide whether it describes a concrete nuclear energy deal, agreement, financing event,
license action, or deployment milestone that should be tracked in a deal database.

Respond with JSON only. No prose, no markdown fences."""

PASS1_USER_TMPL = """Article title: {title}
Article text:
{body}

Does this article describe a trackable nuclear deal or milestone?
A trackable item is one of:
- A signed or announced agreement (PPA, MOU, JDA, EPC, collaboration)
- A financing event (equity, debt, grant, DOE award)
- A license action (renewal, restart approval, construction permit filed/approved)
- A deployment milestone (COL filing, site permit, construction start)
- A significant offtake or supply commitment

NOT trackable:
- Opinion pieces, analysis, retrospectives
- Policy speculation without a concrete action
- Earnings reports with no specific deal
- International news with no US nexus (unless a major framework)
- Articles that only mention existing/prior deals with no new development

Respond with exactly this JSON:
{{"include": true/false, "reason": "one sentence"}}"""


PASS2_SYSTEM = """You are a nuclear industry deal analyst. Extract structured data from
nuclear energy news articles. Return only valid JSON, no prose, no markdown fences.

SIGNIFICANCE CATEGORIES (pick the highest applicable):
- "Deployment"        → concrete construction, licensing, restart activity
- "Financing"         → capital commitment with a specific dollar amount
- "Offtake"           → binding/near-binding PPA or supply agreement
- "Market development"→ preorders, site options, feasibility studies with capital at risk
- "Signaling"         → non-binding MOUs, collaborations, intent without committed capital
- "Policy / Regulatory" → government action, legislation, NRC decisions

IMPACT LEVELS:
- "High"   → >$500M or >500 MW or landmark first-of-kind
- "Medium" → $50M–$500M or 50–500 MW or notable but not landmark
- "Low"    → <$50M or <50 MW or early-stage/feasibility

DEAL TYPES:
PPA, MOU, JDA, EPC, Financing, Grant, License, COLA Filing, ARDP Award,
Collaboration, Preorder, DoD Contract, State Legislation / Grant,
NRC License Renewal, NRC SLR Approval, SLR Application,
Early Site Permit Application, Strategic Partnership, Announcement,
Master Power Agreement, Funding Agreement, Other

CAPITAL UNIT OPTIONS: USD millions, USD billions, USD thousands,
EUR millions, EUR billions, GBP millions, GBP billions, JPY billions,
CAD millions, CAD billions

VALUE TYPE OPTIONS: Exact, Up to, At least, Range

CAPITAL SOURCE OPTIONS: Equity, Debt, Grant, PPA, Mixed, Undisclosed

CONFIDENCE: High / Medium / Low
- High   → all key fields clearly stated in article
- Medium → some fields inferred or partially stated
- Low    → significant gaps or ambiguity

ATTACHMENT RULE:
- Set project_id if the deal can be tied to a specific project in the reference list
- Set program_id only if it is clearly a program-level deal with no specific project
- Set reactor_id only if a specific reactor unit is named
- Default to project_id whenever possible"""


PASS2_USER_TMPL = """Article title: {title}
Article URL: {url}
Article date: {date}

Article text:
{body}

Known programs (match by name similarity):
{programs}

Known projects (match by name similarity):
{projects}

Known reactors (match by name similarity):
{reactors}

Existing announcements fingerprints (to avoid duplicates):
{fingerprints}

Extract the deal and return this JSON (use null for unknown fields):
{{
  "deal_type":         "string from allowed list",
  "significance":      "string from allowed list",
  "impact":            "High|Medium|Low",
  "announcement_date": "YYYY-MM-DD or YYYY-MM or YYYY",
  "partners":          "comma-separated counterparties (not the lead)",
  "deal_summary":      "1-2 sentence description of what was agreed",
  "capital_value_low": number or null,
  "capital_value_high": number or null,
  "capital_unit":      "string from allowed list or null",
  "value_type":        "Exact|Up to|At least|Range or null",
  "capital_source":    "string from allowed list",
  "project_id":        "PROJ-XXX if matched, else null",
  "program_id":        "PROG-XXX if matched and no project match, else null",
  "reactor_id":        "REAC-XXX if a specific unit named, else null",
  "confidence":        "High|Medium|Low",
  "needs_review":      true/false,
  "is_duplicate":      true/false,
  "duplicate_reason":  "string or null"
}}"""


# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────

def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def load_tab(spreadsheet, tab_name):
    """Return list of dicts keyed by header row."""
    ws = spreadsheet.worksheet(tab_name)
    time.sleep(0.5)
    return ws.get_all_records()


def ensure_tab(spreadsheet, tab_name, headers):
    """Create tab with headers if it doesn't exist."""
    existing = [ws.title for ws in spreadsheet.worksheets()]
    if tab_name not in existing:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=2000, cols=len(headers) + 2)
        ws.append_row(headers)
        print(f"  Created tab: {tab_name}")
    return spreadsheet.worksheet(tab_name)


def append_rows_batch(spreadsheet, tab_name, rows):
    """Append a list of row-dicts to a tab in one batch."""
    if not rows:
        return
    ws = spreadsheet.worksheet(tab_name)
    # Get headers to order values correctly
    headers = ws.row_values(1)
    values = [[str(row.get(h, "") or "") for h in headers] for row in rows]
    ws.append_rows(values, value_input_option="RAW")
    time.sleep(1)


def next_id(spreadsheet, tab_name, id_prefix, id_col=0):
    """Return next sequential ID string like DEAL-042."""
    ws = spreadsheet.worksheet(tab_name)
    all_vals = ws.col_values(id_col + 1)[1:]  # skip header
    existing = [v for v in all_vals if v.startswith(id_prefix + "-")]
    if not existing:
        return f"{id_prefix}-001"
    nums = []
    for v in existing:
        try:
            nums.append(int(v.split("-")[1]))
        except Exception:
            pass
    return f"{id_prefix}-{(max(nums) + 1):03d}" if nums else f"{id_prefix}-001"


# ─── REFERENCE DATA LOADER ────────────────────────────────────────────────────

def load_reference_data(spreadsheet):
    """
    Load Programs, Projects, Reactors, and existing Announcement fingerprints.
    Returns dicts and lists used for matching and deduplication.
    """
    print("  Loading Programs...")
    programs = load_tab(spreadsheet, TAB_PROGRAMS)
    program_lookup = [(r["Program ID"], r["Program Name"]) for r in programs if r.get("Program ID")]

    print("  Loading Projects...")
    projects = load_tab(spreadsheet, TAB_PROJECTS)
    project_lookup = [(r["Project ID"], r["Project Name"], r.get("Primary Lead","")) for r in projects if r.get("Project ID")]

    print("  Loading Reactors...")
    reactors = load_tab(spreadsheet, TAB_REACTORS)
    reactor_lookup = [(r["Reactor ID"], r["Unit Name / Number"], r.get("Project ID (opt.)","")) for r in reactors if r.get("Reactor ID")]

    print("  Loading existing Announcements for deduplication...")
    try:
        announcements = load_tab(spreadsheet, TAB_ANNOUNCEMENTS)
        # Fingerprint = project_id + deal_type + summary (first 80 chars)
        fingerprints = [
            f"{r.get('Project ID','')}/{r.get('Program ID','')}/{r.get('Deal Type','')}/{str(r.get('Deal Summary',''))[:80]}"
            for r in announcements
        ]
    except Exception:
        fingerprints = []

    return {
        "program_lookup":  program_lookup,
        "project_lookup":  project_lookup,
        "reactor_lookup":  reactor_lookup,
        "fingerprints":    fingerprints,
    }


def load_seen_hashes(spreadsheet):
    """Return set of already-processed article hashes."""
    try:
        ws = spreadsheet.worksheet(TAB_SEEN)
        return set(ws.col_values(1)[1:])
    except Exception:
        return set()


# ─── UTILITIES ────────────────────────────────────────────────────────────────

def clean(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def entry_hash(title, url):
    return hashlib.md5(f"{title}{url}".encode()).hexdigest()


def is_deal_candidate(title, summary):
    text = (title + " " + summary).lower()
    return any(kw in text for kw in DEAL_KEYWORDS)


def format_reference_list(items, max_items=60):
    """Format a lookup list as a compact string for the prompt."""
    lines = []
    for item in items[:max_items]:
        if len(item) == 2:
            lines.append(f"  {item[0]}: {item[1]}")
        elif len(item) == 3:
            lines.append(f"  {item[0]}: {item[1]} (lead: {item[2]})")
    if len(items) > max_items:
        lines.append(f"  ... and {len(items) - max_items} more")
    return "\n".join(lines) if lines else "  (none)"


# ─── ARTICLE FETCHER ──────────────────────────────────────────────────────────

def fetch_article_text(url):
    """Fetch and clean article body text, capped at BODY_CHAR_LIMIT chars."""
    try:
        r = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        # Remove noise
        for tag in soup(["script", "style", "nav", "footer", "header",
                         "aside", "form", "iframe", "noscript"]):
            tag.decompose()
        # Prefer article body elements
        body = (
            soup.find("article") or
            soup.find("main") or
            soup.find(class_=re.compile(r"article|content|story|post", re.I)) or
            soup.body
        )
        text = clean(body.get_text(separator=" ")) if body else ""
        return text[:BODY_CHAR_LIMIT]
    except Exception as e:
        print(f"    Fetch error: {e}")
        return ""


# ─── CLAUDE CALLS ─────────────────────────────────────────────────────────────

def call_claude(client, system, user_msg, model=None):
    """Single Claude API call, returns parsed JSON or None."""
    use_model = model or MODEL
    try:
        resp = client.messages.create(
            model=use_model,
            max_tokens=1200,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"^```json\s*|\s*```$", "", raw, flags=re.DOTALL).strip()
        return json.loads(raw)
    except Exception as e:
        print(f"    Claude error ({use_model}): {e}")
        return None


def pass1_include(client, title, body):
    """Pass 1: should this article be included?"""
    user_msg = PASS1_USER_TMPL.format(title=title, body=body[:4000])
    result = call_claude(client, PASS1_SYSTEM, user_msg)
    if not result:
        return False, "claude error"
    return result.get("include", False), result.get("reason", "")


def pass2_extract(client, article, ref_data, model=None):
    """Pass 2: extract structured deal fields."""
    user_msg = PASS2_USER_TMPL.format(
        title=article["title"],
        url=article["link"],
        date=article.get("date", "unknown"),
        body=article.get("body", "")[:BODY_CHAR_LIMIT],
        programs=format_reference_list(ref_data["program_lookup"]),
        projects=format_reference_list(ref_data["project_lookup"]),
        reactors=format_reference_list(ref_data["reactor_lookup"]),
        fingerprints="\n".join(f"  {f}" for f in ref_data["fingerprints"][-50:]) or "  (none)",
    )
    return call_claude(client, PASS2_SYSTEM, user_msg, model=model)


# ─── RSS SCRAPER ──────────────────────────────────────────────────────────────

def scrape_feeds():
    """Fetch RSS feeds and return candidate articles."""
    candidates = []
    failed = []
    for feed_cfg in FEEDS:
        print(f"  Fetching: {feed_cfg['name']} ...", end=" ", flush=True)
        try:
            feed = feedparser.parse(feed_cfg["url"])
            if feed.bozo and not feed.entries:
                print("SKIP (unreadable)")
                failed.append(feed_cfg["name"])
                continue
            # Company newsrooms have fewer but more targeted entries — read all
            limit = feed_cfg.get("limit", 80)
            nuclear_only = feed_cfg.get("nuclear_only", False)
            matched = 0
            for entry in feed.entries[:limit]:
                title   = clean(entry.get("title", ""))
                link    = entry.get("link", "")
                summary = clean(BeautifulSoup(entry.get("summary", ""), "html.parser").get_text())
                # For noisy broad-topic feeds, require a nuclear keyword first
                if nuclear_only:
                    text_lower = (title + " " + summary).lower()
                    if not any(kw in text_lower for kw in NUCLEAR_KEYWORDS):
                        continue
                # Parse date
                date = ""
                if entry.get("published"):
                    try:
                        date = parsedate_to_datetime(entry.published).strftime("%Y-%m-%d")
                    except Exception:
                        date = entry.get("published", "")[:10]
                if not title or not link:
                    continue
                if is_deal_candidate(title, summary):
                    candidates.append({
                        "title":   title,
                        "link":    link,
                        "summary": summary,
                        "date":    date,
                        "source":  feed_cfg["name"],
                    })
                    matched += 1
            print(f"{matched} candidates")
        except Exception as e:
            print(f"ERROR: {e}")
            failed.append(feed_cfg["name"])
    if failed:
        print(f"\n  ⚠ {len(failed)} feeds skipped: {', '.join(failed)}")
    # Deduplicate by link
    seen_links = set()
    unique = []
    for c in candidates:
        if c["link"] not in seen_links:
            seen_links.add(c["link"])
            unique.append(c)
    return unique


# ─── CURRENCY CONVERSION STUB ─────────────────────────────────────────────────

# Approximate FX rates to USD (to be replaced with live API call later)
FX_TO_USD = {
    "USD millions":  1.0,
    "USD billions":  1000.0,
    "USD thousands": 0.001,
    "EUR millions":  1.08,
    "EUR billions":  1080.0,
    "GBP millions":  1.27,
    "GBP billions":  1270.0,
    "JPY billions":  0.0067,
    "CAD millions":  0.74,
    "CAD billions":  740.0,
}

def to_usd_millions(value, unit):
    """Convert capital value to USD millions. Returns None if not possible."""
    if value is None or not unit:
        return None
    rate = FX_TO_USD.get(unit)
    if rate is None:
        return None
    return round(float(value) * rate, 2)


# ─── MAIN RUN ─────────────────────────────────────────────────────────────────

def run():
    today = datetime.date.today().isoformat()
    now   = datetime.datetime.utcnow().isoformat()

    print(f"\n{'='*65}")
    print(f"Nuclear Deal Scraper v2 — {today}")
    print(f"{'='*65}\n")

    # ── Clients ──────────────────────────────────────────────────────
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    gc     = get_gsheet_client()
    sheet  = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

    # ── Ensure operational tabs exist ────────────────────────────────
    ensure_tab(sheet, TAB_SEEN,   SEEN_COLS)
    ensure_tab(sheet, TAB_REVIEW, REVIEW_COLS)

    # ── Load reference data ──────────────────────────────────────────
    print("Loading reference data...")
    ref = load_reference_data(sheet)
    seen_hashes = load_seen_hashes(sheet)
    print(f"  {len(ref['program_lookup'])} programs, {len(ref['project_lookup'])} projects, "
          f"{len(ref['reactor_lookup'])} reactors, {len(ref['fingerprints'])} existing deals\n")

    # ── Scrape feeds ─────────────────────────────────────────────────
    print("Scraping feeds...")
    candidates = scrape_feeds()
    print(f"\n  {len(candidates)} total candidates after dedup\n")

    # ── Compute base Deal ID number once before loop ──────────────────
    first_deal_id = next_id(sheet, TAB_ANNOUNCEMENTS, "DEAL")
    base_deal_num = int(first_deal_id.split("-")[1]) - 1

    # ── Process articles ─────────────────────────────────────────────
    new_seen     = []
    new_ann      = []
    review_rows  = []
    review_id    = 1
    deal_counter = 0
    skipped      = 0
    duplicates   = 0

    for article in candidates:
        h = entry_hash(article["title"], article["link"])

        # Skip already-processed articles
        if h in seen_hashes:
            continue

        print(f"  ⟳ {article['title'][:80]}")

        # ── Fetch full article body ───────────────────────────────
        body = fetch_article_text(article["link"])
        article["body"] = body or article["summary"]

        # ── Pass 1: include/exclude ───────────────────────────────
        include, reason = pass1_include(client, article["title"], article["body"])
        if not include:
            print(f"    ↷ Excluded: {reason}")
            new_seen.append({
                "hash": h, "title": article["title"],
                "url": article["link"], "scraped_at": now
            })
            skipped += 1
            continue

        print(f"    ✓ Included: {reason}")

        # ── Date filter ───────────────────────────────────────────
        year_str = article.get("date", "")[:4]
        try:
            if year_str and int(year_str) < MIN_YEAR:
                print(f"    ↷ Too old ({year_str})")
                new_seen.append({
                    "hash": h, "title": article["title"],
                    "url": article["link"], "scraped_at": now
                })
                continue
        except Exception:
            pass

        # ── Pass 2: extract structured fields ────────────────────
        result = pass2_extract(client, article, ref)

        # If Claude returned a list instead of a dict, unwrap or discard
        if isinstance(result, list):
            result = result[0] if result and isinstance(result[0], dict) else None

        # Escalate to stronger model if low confidence
        if result and result.get("confidence") == "Low":
            print(f"    ↑ Escalating to {ESCALATION_MODEL} (low confidence)")
            result_escalated = pass2_extract(client, article, ref, model=ESCALATION_MODEL)
            if isinstance(result_escalated, list):
                result_escalated = result_escalated[0] if result_escalated and isinstance(result_escalated[0], dict) else None
            if result_escalated:
                result = result_escalated

        if not result:
            print("    ✗ Extraction failed — sending to Review")
            review_rows.append({
                "Review ID":     f"REV-{review_id:03d}",
                "Scraped At":    now,
                "Article Title": article["title"],
                "Article URL":   article["link"],
                "Reason":        "extraction failed",
                "Raw Extraction": "",
            })
            review_id += 1
            new_seen.append({
                "hash": h, "title": article["title"],
                "url": article["link"], "scraped_at": now
            })
            continue

        # ── Duplicate check ───────────────────────────────────────
        if result.get("is_duplicate"):
            print(f"    ≡ Duplicate: {result.get('duplicate_reason','')}")
            duplicates += 1
            new_seen.append({
                "hash": h, "title": article["title"],
                "url": article["link"], "scraped_at": now
            })
            continue

        # ── Assign Deal ID ────────────────────────────────────────
        deal_counter += 1
        deal_id = f"DEAL-{(base_deal_num + deal_counter):03d}"

        # ── USD conversion ────────────────────────────────────────
        cap_low  = result.get("capital_value_low")
        cap_high = result.get("capital_value_high")
        cap_unit = result.get("capital_unit")
        usd_low  = to_usd_millions(cap_low, cap_unit)
        usd_high = to_usd_millions(cap_high, cap_unit)

        # ── Build announcement row ────────────────────────────────
        needs_review = result.get("needs_review", False) or result.get("confidence") == "Low"

        ann = {
            "Deal ID":               deal_id,
            "Reactor ID":            result.get("reactor_id") or "",
            "Project ID":            result.get("project_id") or "",
            "Program ID":            result.get("program_id") or "",
            "Deal Type":             result.get("deal_type", ""),
            "Significance":          result.get("significance", ""),
            "Impact":                result.get("impact", ""),
            "Announcement Date":     result.get("announcement_date", article.get("date", "")),
            "Partners":              result.get("partners", ""),
            "Deal Summary":          result.get("deal_summary", ""),
            "Capital Value (low)":   cap_low if cap_low is not None else "",
            "Capital Value (high)":  cap_high if cap_high is not None else "",
            "Capital Unit":          cap_unit or "",
            "Value Type":            result.get("value_type", "") or "",
            "Capital Source":        result.get("capital_source", ""),
            "USD Val low (calc.)":   usd_low if usd_low is not None else "",
            "USD Val high (calc.)":  usd_high if usd_high is not None else "",
            "Source URL":            article["link"],
            "Confidence":            result.get("confidence", "Medium"),
            "Needs Review":          "Yes" if needs_review else "No",
        }
        new_ann.append(ann)

        # Update in-memory fingerprints so later articles in same run dedup correctly
        fp = f"{ann['Project ID']}/{ann['Program ID']}/{ann['Deal Type']}/{ann['Deal Summary'][:80]}"
        ref["fingerprints"].append(fp)

        conf_icon = "🟡" if result.get("confidence") == "Medium" else ("🔴" if result.get("confidence") == "Low" else "🟢")
        review_flag = " → REVIEW" if needs_review else ""
        print(f"    ✚ {deal_id} | {ann['Deal Type']} | {ann['Significance']} | conf={ann['Confidence']} {conf_icon}{review_flag}")
        print(f"      Project: {ann['Project ID'] or ann['Program ID'] or '⚠ unlinked'}")

        # Mark as seen
        new_seen.append({
            "hash": h, "title": article["title"],
            "url": article["link"], "scraped_at": now
        })

        # Low-confidence also gets a Review row for human inspection
        if needs_review:
            review_rows.append({
                "Review ID":     f"REV-{review_id:03d}",
                "Scraped At":    now,
                "Article Title": article["title"],
                "Article URL":   article["link"],
                "Reason":        f"confidence={result.get('confidence')}; needs_review={result.get('needs_review')}",
                "Raw Extraction": json.dumps(result),
            })
            review_id += 1

        time.sleep(0.5)   # gentle rate limiting

    # ── Batch write to Sheets ─────────────────────────────────────────
    print(f"\n{'─'*65}")
    print(f"Run summary:")
    print(f"  Candidates processed : {len(candidates)}")
    print(f"  Excluded (pass 1)    : {skipped}")
    print(f"  Duplicates           : {duplicates}")
    print(f"  New deals            : {len(new_ann)}")
    print(f"  Sent to Review       : {len(review_rows)}")

    if new_ann:
        print(f"\nWriting {len(new_ann)} announcements to Sheet...")
        append_rows_batch(sheet, TAB_ANNOUNCEMENTS, new_ann)
        print("  Done.")

    if review_rows:
        print(f"Writing {len(review_rows)} review items...")
        append_rows_batch(sheet, TAB_REVIEW, review_rows)
        print("  Done.")

    if new_seen:
        print(f"Logging {len(new_seen)} seen hashes...")
        append_rows_batch(sheet, TAB_SEEN, new_seen)
        print("  Done.")

    print(f"\n✓ Scraper complete — {today}\n")


if __name__ == "__main__":
    run()
