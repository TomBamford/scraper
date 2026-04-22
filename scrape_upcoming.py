"""
Copart Upcoming Lots Scraper — California only
===============================================
Scrapes Copart for CA vehicles going up for auction in the next 7 days
and writes upcoming.csv to this repo.

The Car Lookup page in your web app reads this file directly from GitHub.

Install:
    pip install requests pandas

Run manually:
    python scrape_upcoming.py

Runs automatically via GitHub Actions (.github/workflows/scrape_upcoming.yml)
once a day — the file is committed and pushed automatically.
"""

import os, re, time, random
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

# ─────────────────────────────────────────────────────────────
# CONFIG — edit these if needed
# ─────────────────────────────────────────────────────────────
UPCOMING_DAYS     = 7        # how many days ahead to look
UPCOMING_STATE    = "CA"     # state filter — California
UPCOMING_YEAR_MIN = 2000
UPCOMING_YEAR_MAX = 2027
UPCOMING_CSV      = "upcoming.csv"   # output file name

ROWS_PER_PAGE = 100
MAX_PAGES     = 9999
REQUEST_DELAY = 1.5   # seconds between page requests

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU",
}

# ─────────────────────────────────────────────────────────────
# COPART API
# ─────────────────────────────────────────────────────────────
SEARCH_URL = "https://www.copart.com/public/lots/search-results"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.copart.com",
    "Referer": "https://www.copart.com/vehicleFinderSearch",
}


def build_payload(page: int) -> dict:
    """Build the POST body for Copart's search API — upcoming CA lots."""
    today     = datetime.now(timezone.utc)
    cutoff    = today + timedelta(days=UPCOMING_DAYS)
    today_ms  = int(today.timestamp() * 1000)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    return {
        "query": ["*"],
        "filter": {
            "ODM":  ["odometer_reading_received:[0 TO 9999999]"],
            "YEAR": [f"lot_year:[{UPCOMING_YEAR_MIN} TO {UPCOMING_YEAR_MAX}]"],
            "MISC": [
                "#VehicleTypeCode:VEHTYPE_V",
                f'#LocState:\"{UPCOMING_STATE}\"',
            ],
            "SALE_DATE": [f"auction_date_type:[{today_ms} TO {cutoff_ms}]"],
        },
        "sort": ["auction_date_type asc"],
        "page": page,
        "size": ROWS_PER_PAGE,
        "watchListOnly": False,
        "freeFormSearch": False,
    }


# ─────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    """Create a session and seed cookies just like a real browser."""
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get("https://www.copart.com/", timeout=15)
        time.sleep(1.2)
        s.get("https://www.copart.com/vehicleFinderSearch", timeout=15)
        time.sleep(0.8)
    except Exception as e:
        print(f"  [WARN] Session seed: {e}")
    return s


def fetch_page(session: requests.Session, payload: dict) -> dict | None:
    """POST to the Copart search API and return parsed JSON."""
    for attempt in range(3):
        try:
            resp = session.post(SEARCH_URL, json=payload, timeout=25)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data") or data.get("returnValue") or data
            elif resp.status_code == 403:
                print(f"  [WARN] 403 — blocked (attempt {attempt+1})")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"  [WARN] HTTP {resp.status_code} (attempt {attempt+1})")
        except Exception as e:
            print(f"  [WARN] Request error: {e} (attempt {attempt+1})")
        time.sleep(REQUEST_DELAY * (attempt + 2))
    return None


def extract_lots(data: dict) -> list[dict]:
    try:
        results = data.get("results", {}) or data.get("lotSearchResults", {}) or {}
        return list(results.get("content") or data.get("content") or [])
    except Exception as e:
        print(f"  [WARN] extract_lots: {e}")
        return []


def total_pages(data: dict) -> int:
    try:
        results = data.get("results", {}) or data.get("lotSearchResults", {}) or {}
        tp = results.get("totalPages") or data.get("totalPages") or 1
        return max(1, int(tp))
    except Exception:
        return 1


# ─────────────────────────────────────────────────────────────
# FIELD EXTRACTION
# ─────────────────────────────────────────────────────────────

def gf(lot: dict, *keys) -> str:
    """Get first non-empty value from a lot dict by trying multiple keys."""
    for k in keys:
        v = lot.get(k)
        if v is not None and str(v).strip() not in ("", "None", "null"):
            return str(v).strip()
    return ""


def normalise_date(raw: str) -> str:
    """Convert any date format to YYYY-MM-DD."""
    if not raw:
        return ""
    if re.fullmatch(r"\d{10,13}", raw.strip()):
        ts = int(raw.strip())
        if ts > 1e11:
            ts //= 1000
        try:
            return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    return ""


def cap(s: str) -> str:
    return s.strip().title() if s else ""


def clean_price(val) -> int:
    s = str(val).replace("$", "").replace(",", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return int(float(m.group(1))) if m else 0


def clean_odo(val: str) -> str:
    s = str(val).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m:
        return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"


def lot_to_raw(lot: dict) -> dict:
    """Map Copart API lot fields → our internal dict."""
    raw_date = gf(lot, "auctionDate", "saleDate", "soldDate", "auctionDateType")
    date     = normalise_date(raw_date)
    location = gf(lot, "auctionLocationName", "yardName", "location", "facilityName")
    state    = gf(lot, "stateCode", "locationStateCode", "state")

    if not state and location:
        sm = re.search(r"\b([A-Z]{2})$", location.strip().upper())
        if sm and sm.group(1) in US_STATES:
            state = sm.group(1)

    return {
        "year":     gf(lot, "year", "modelYear", "lotYear"),
        "make":     gf(lot, "makerDescription", "make", "makeDescription"),
        "model":    gf(lot, "modelDescription", "model", "modelDesc"),
        "trim":     gf(lot, "seriesDesc", "series", "trim"),
        "vin":      gf(lot, "vehicleId", "vin"),
        "lot":      gf(lot, "lotNumberStr", "lotNumber", "lotNum", "ln"),
        "damage":   gf(lot, "primaryDamage", "damageDescription", "damage"),
        "price":    gf(lot, "currentBid", "salePrice", "highBid", "currentBidAmount"),
        "odometer": gf(lot, "odometer", "odometerReading", "odometerReadingReceived"),
        "location": location,
        "state":    state,
        "date":     date,
        "type":     gf(lot, "titleTypeCode", "lotType", "titleType"),
    }


def build_record(raw: dict) -> dict | None:
    """Validate and build a clean upcoming lot record. Returns None to skip."""
    state    = raw.get("state", "").strip().upper()
    location = raw.get("location", "").strip()

    # Infer state from location string if missing
    if not state and location:
        sm = re.search(r"\b([A-Z]{2})$", location.upper())
        if sm and sm.group(1) in US_STATES:
            state = sm.group(1)

    if state != UPCOMING_STATE:
        return None  # California only

    year  = str(raw.get("year", "")).strip()
    make  = cap(raw.get("make", ""))
    model = cap(raw.get("model", ""))
    if not year or not make or not model:
        return None  # skip incomplete lots

    lot        = str(raw.get("lot",  "")).strip()
    vin        = str(raw.get("vin",  "")).strip().upper()
    trim       = str(raw.get("trim", "")).strip()
    title_type = str(raw.get("type", "")).strip()
    damage     = cap(raw.get("damage", "")) or "Unknown"
    sale_date  = str(raw.get("date", "")).strip()
    bid        = clean_price(raw.get("price", ""))
    odo        = clean_odo(raw.get("odometer", ""))

    # Only include lots auctioning within the next UPCOMING_DAYS days
    if sale_date:
        try:
            sale_dt = datetime.strptime(sale_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            now     = datetime.now(timezone.utc)
            cutoff  = now + timedelta(days=UPCOMING_DAYS)
            if not (now <= sale_dt <= cutoff):
                return None
        except Exception:
            pass  # keep if we can't parse the date

    return {
        "lot":         lot,
        "vin":         vin,
        "year":        year,
        "make":        make,
        "model":       model,
        "trim":        trim,
        "title_type":  title_type,
        "damage":      damage,
        "location":    cap(location),
        "sale_date":   sale_date,
        "current_bid": bid,
        "odometer":    odo,
        "url":         f"https://www.copart.com/lot/{lot}" if lot else "",
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    t0 = time.time()

    print("=" * 60)
    print(f"  Copart Upcoming Lots Scraper")
    print(f"  State  : {UPCOMING_STATE} (California)")
    print(f"  Window : next {UPCOMING_DAYS} days")
    print(f"  Years  : {UPCOMING_YEAR_MIN}–{UPCOMING_YEAR_MAX}")
    print("=" * 60)

    session   = get_session()
    all_lots  = []
    total_pg  = None

    for page in range(0, MAX_PAGES):
        payload = build_payload(page)
        print(f"  Page {page+1}{f'/{total_pg}' if total_pg else ''}...", end=" ", flush=True)
        data = fetch_page(session, payload)

        if data is None:
            print("no data — stopping")
            break

        lots = extract_lots(data)
        if not lots:
            print("empty — done")
            break

        if total_pg is None:
            total_pg = total_pages(data)
            print(f"(total pages: {total_pg})", end=" ")

        all_lots.extend(lots)
        print(f"{len(lots)} lots (running total: {len(all_lots)})")

        if page + 1 >= total_pg:
            print(f"  Reached last page ({total_pg})")
            break

        time.sleep(REQUEST_DELAY + random.uniform(0, 0.4))

    # Build clean records
    records   = []
    seen_lots = set()
    skipped   = 0

    for lot in all_lots:
        raw = lot_to_raw(lot)
        rec = build_record(raw)
        if rec is None:
            skipped += 1
            continue
        if rec["lot"] and rec["lot"] in seen_lots:
            skipped += 1
            continue
        records.append(rec)
        if rec["lot"]:
            seen_lots.add(rec["lot"])

    # Write CSV
    cols = [
        "lot", "vin", "year", "make", "model", "trim",
        "title_type", "damage", "location", "sale_date",
        "current_bid", "odometer", "url"
    ]

    if not records:
        print("\n  [INFO] No upcoming CA lots found — writing empty upcoming.csv")
        pd.DataFrame(columns=cols).to_csv(UPCOMING_CSV, index=False)
    else:
        df = pd.DataFrame(records)
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df[cols].sort_values("sale_date")
        df.to_csv(UPCOMING_CSV, index=False)

    elapsed = int(time.time() - t0)
    print("\n" + "=" * 60)
    print(f"  CA lots written : {len(records)}")
    print(f"  Skipped         : {skipped}")
    print(f"  Output file     : {UPCOMING_CSV}")
    print(f"  Elapsed         : {elapsed}s")
    print("=" * 60)
    print("\nDone. Commit and push upcoming.csv to GitHub.")


if __name__ == "__main__":
    main()
