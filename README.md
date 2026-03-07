# Nuclear Deal Scraper → Google Sheets

Daily pipeline: monitors 5 major nuclear news RSS feeds → Claude extracts structured deal data → appends new rows to Google Sheets automatically via GitHub Actions.

---

## Architecture

```
RSS Feeds (5 sources)
       ↓
  Keyword filter
       ↓
  Claude Sonnet  ←  article title + body text
       ↓
  Structured JSON
       ↓
  Google Sheets  ←  appends new rows to "Deals" tab
                    tracks seen articles in "Seen" tab
```

---

## Sources monitored

| Source | Type |
|---|---|
| World Nuclear News | Daily global industry news |
| Nuclear Energy Institute (NEI) | U.S. policy & industry |
| DOE Office of Nuclear Energy | Federal funding & programs |
| ANS Nuclear Newswire | Technical & industry news |
| Nuclear Engineering International | Global project news |

---

## One-time setup (20 minutes)

### Step 1 — Create a Google Sheet

1. Go to [sheets.google.com](https://sheets.google.com) → create a new blank sheet
2. Name it **"Nuclear Deal Tracker"**
3. Copy the Sheet ID from the URL:
   ```
   https://docs.google.com/spreadsheets/d/  <<<SHEET_ID>>>  /edit
   ```

### Step 2 — Create a Google Service Account

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a new project (or use an existing one)
3. Enable the **Google Sheets API**:
   - APIs & Services → Enable APIs → search "Google Sheets API" → Enable
4. Create a Service Account:
   - APIs & Services → Credentials → Create Credentials → Service Account
   - Name it `nuclear-scraper` → Create and Continue → Done
5. Generate a JSON key:
   - Click your new service account → Keys tab → Add Key → Create new key → JSON
   - Download the `.json` file — **keep this secret**

### Step 3 — Share the sheet with the service account

1. Open your Google Sheet
2. Share → paste the service account email (looks like `nuclear-scraper@your-project.iam.gserviceaccount.com`)
3. Give it **Editor** access

### Step 4 — Seed the sheet with existing data (run once locally)

```bash
pip install -r requirements.txt

export GOOGLE_SERVICE_ACCOUNT_JSON=$(cat your-service-account-key.json)
export GOOGLE_SHEET_ID=your_sheet_id_here

python seed_sheet.py
```

This creates the `Deals` and `Seen` tabs with headers and loads the 6 seed deals.

### Step 5 — Create a GitHub repo and add secrets

1. Create a new GitHub repo, push these files
2. Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret name | Value |
|---|---|
| `ANTHROPIC_API_KEY` | Your key from [console.anthropic.com](https://console.anthropic.com) |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | The full contents of your service account `.json` file |
| `GOOGLE_SHEET_ID` | The sheet ID from Step 1 |

### Step 6 — Enable GitHub Actions

The workflow runs automatically at **7:00 AM UTC daily**.

Manual trigger: Actions tab → "Daily Nuclear Deal Scraper" → Run workflow

---

## Google Sheet structure

### Deals tab

| Column | Description |
|---|---|
| id | Auto-incrementing integer |
| date | Publication date (YYYY-MM-DD) |
| company | Reactor developer |
| reactor | Technology name |
| project | Project / site name |
| location | City, State |
| partners | Key partner(s) |
| deal | Short deal label |
| significance | Signaling / Market development / Deployment |
| summary | 1-2 sentence description |
| source | Article URL |
| scraped_at | UTC timestamp of scrape run |

### Seen tab

Tracks article hashes to avoid duplicates across daily runs.

---

## Connecting to the React tracker

Replace the static `deals` array with a live fetch from Google Sheets.

**Option A — Published CSV (simplest, no auth)**

1. In Google Sheets: File → Share → Publish to web → Deals tab → CSV → Publish
2. Copy the published CSV URL
3. In your React component:

```javascript
import Papa from "papaparse";
const [deals, setDeals] = useState([]);

useEffect(() => {
  fetch("YOUR_PUBLISHED_CSV_URL")
    .then(r => r.text())
    .then(csv => {
      const { data } = Papa.parse(csv, { header: true, skipEmptyLines: true });
      setDeals(data);
    });
}, []);
```

**Option B — Google Sheets API (requires API key)**

```javascript
const SHEET_ID = "your_sheet_id";
const API_KEY  = "your_google_api_key";  // Read-only key, safe to use in frontend

useEffect(() => {
  fetch(`https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/Deals?key=${API_KEY}`)
    .then(r => r.json())
    .then(data => {
      const [headers, ...rows] = data.values;
      setDeals(rows.map(row =>
        Object.fromEntries(headers.map((h, i) => [h, row[i] ?? ""]))
      ));
    });
}, []);
```

---

## Cost estimate

- ~$0.01–0.05/day (Claude Sonnet, typically 10–30 extractions/day)
- GitHub Actions: free tier (2,000 min/month; this job runs ~2 min/day)
- Google Sheets API: free

---

## Running locally

```bash
pip install -r requirements.txt

export ANTHROPIC_API_KEY=sk-ant-...
export GOOGLE_SERVICE_ACCOUNT_JSON=$(cat your-key.json)
export GOOGLE_SHEET_ID=your_sheet_id

python scraper.py
```
