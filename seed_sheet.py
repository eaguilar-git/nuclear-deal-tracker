"""
seed_sheet.py
─────────────
One-time script to populate the Google Sheet with the 6 existing seed deals.
Run once before deploying the daily scraper.

Usage:
    export GOOGLE_SERVICE_ACCOUNT_JSON='...'
    export GOOGLE_SHEET_ID='...'
    python seed_sheet.py
"""

import os
import json
import gspread
from google.oauth2.service_account import Credentials

SEED_DEALS = [
    {
        "id": 1, "date": "2023-08-12", "company": "TerraPower", "reactor": "Natrium",
        "project": "Kemmerer Demonstration", "location": "Kemmerer, Wyoming",
        "partners": "Bechtel", "deal": "EPC partnership",
        "significance": "Market development", "source": "https://terrapower.com",
        "summary": "Bechtel selected as EPC contractor for the Natrium project.", "scraped_at": "seed",
    },
    {
        "id": 2, "date": "2024-04-10", "company": "TerraPower", "reactor": "Natrium",
        "project": "Kemmerer Demonstration", "location": "Kemmerer, Wyoming",
        "partners": "DOE", "deal": "ARDP demonstration funding",
        "significance": "Deployment", "source": "https://energy.gov",
        "summary": "DOE selected TerraPower for the ARDP program supporting Natrium demonstration.", "scraped_at": "seed",
    },
    {
        "id": 3, "date": "2024-05-15", "company": "Oklo", "reactor": "Aurora",
        "project": "INL Aurora", "location": "Idaho Falls, Idaho",
        "partners": "DOE", "deal": "Site agreement",
        "significance": "Market development", "source": "https://oklo.com",
        "summary": "Oklo secured a site use agreement at Idaho National Laboratory.", "scraped_at": "seed",
    },
    {
        "id": 4, "date": "2026-01-10", "company": "GE Hitachi", "reactor": "BWRX-300",
        "project": "Clinch River", "location": "Oak Ridge, Tennessee",
        "partners": "TVA", "deal": "Site feasibility study",
        "significance": "Signaling", "source": "https://gevernova.com",
        "summary": "TVA evaluating BWRX-300 deployment at Clinch River.", "scraped_at": "seed",
    },
    {
        "id": 5, "date": "2026-01-26", "company": "Westinghouse", "reactor": "eVinci",
        "project": "Program-level", "location": "United States",
        "partners": "US DoD", "deal": "Microreactor demonstration partnership",
        "significance": "Market development", "source": "https://westinghousenuclear.com",
        "summary": "Westinghouse collaborating with the Department of Defense on microreactor demonstrations.", "scraped_at": "seed",
    },
    {
        "id": 6, "date": "2026-03-01", "company": "X-energy", "reactor": "Xe-100",
        "project": "Dow Seadrift", "location": "Seadrift, Texas",
        "partners": "Dow", "deal": "Industrial deployment agreement",
        "significance": "Deployment", "source": "https://x-energy.com",
        "summary": "Dow signed agreement to deploy Xe-100 reactors at Seadrift.", "scraped_at": "seed",
    },
]

DEAL_COLUMNS = [
    "id", "date", "company", "reactor", "project",
    "location", "partners", "deal", "significance",
    "summary", "source", "scraped_at",
]

def main():
    sa_json  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    sheet_id = os.environ["GOOGLE_SHEET_ID"]

    creds = Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)

    sheet_names = [ws.title for ws in spreadsheet.worksheets()]

    # ── Deals sheet ──
    if "Deals" not in sheet_names:
        ws = spreadsheet.add_worksheet(title="Deals", rows=1000, cols=20)
    else:
        ws = spreadsheet.worksheet("Deals")
        ws.clear()

    ws.append_row(DEAL_COLUMNS, value_input_option="RAW")
    for deal in SEED_DEALS:
        ws.append_row([str(deal.get(c, "")) for c in DEAL_COLUMNS], value_input_option="USER_ENTERED")

    # ── Seen sheet ──
    if "Seen" not in sheet_names:
        seen_ws = spreadsheet.add_worksheet(title="Seen", rows=5000, cols=2)
    else:
        seen_ws = spreadsheet.worksheet("Seen")
        seen_ws.clear()

    seen_ws.append_row(["hash", "title"], value_input_option="RAW")

    print(f"✅ Seeded {len(SEED_DEALS)} deals into Google Sheet: {sheet_id}")
    print(f"   Sheet URL: https://docs.google.com/spreadsheets/d/{sheet_id}/edit")


if __name__ == "__main__":
    main()
