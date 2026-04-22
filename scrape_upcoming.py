"""
Copart Upcoming Lots Scraper — California only
===============================================
Uses ScraperAPI to bypass Copart's Imperva/Incapsula bot protection,
which blocks direct requests from GitHub Actions IPs.

Setup (one time):
  1. Sign up free at https://www.scraperapi.com — 5,000 free calls/month
  2. Copy your API key from the dashboard
  3. In your GitHub repo → Settings → Secrets and variables → Actions
     → New repository secret → Name: SCRAPER_API_KEY → paste your key

Install:
    pip install requests pandas

Run manually (set env var first):
    export SCRAPER_API_KEY=your_key_here
    python scrape_upcoming.py

Runs automatically via GitHub Actions once a day.
"""

import os, re, time, random
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
UPCOMING_DAYS     = 7
UPCOMING_STATE    = "CA"
UPCOMING_YEAR_MIN = 2000
UPCOMING_YEAR_MAX = 2027
UPCOMING_CSV      = "upcoming.csv"

ROWS_PER_PAGE = 100
MAX_PAGES     = 500
REQUEST_DELAY = 1.5

# ScraperAPI key — read from environment variable (set as GitHub secret)
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU",
}

COPART_SEARCH_URL = "https://www.copart.com/public/lots/search-results"

# ─────────────────────────────────────────────────────────────
# SCRAPERAPI WRAPPER
# Sends any request through ScraperAPI's rotating proxy + browser pool,
# which handles cookies, bot detection, and JavaScript rendering.
# ─────────────────────────────────────────────────────────────

def scraper_api_post(url: str, json_body: dict, session_number: int = 1) -> requests.Response | None:
    """
    POST request routed through ScraperAPI.
    ScraperAPI handles Imperva/Incapsula automatically.
    session_number keeps the same proxy IP across pages (important for cookies).
    """
    if not SCRAPER_API_KEY:
        raise RuntimeError(
            "SCRAPER_API_KEY environment variable not set.\n"
            "Sign up free at https://www.scraperapi.com and add it as a GitHub secret."
        )

    # ScraperAPI POST endpoint
    proxy_url = "https://api.scraperapi.com/"

    params = {
        "api_key":        SCRAPER_API_KEY,
        "url":            url,
        "session_number": session_number,   # sticky session = same IP = cookies persist
        "render":         "false",          # no JS rendering needed — it's a JSON API
        "country_code":   "us",             # use US IPs
        "premium":        "false",
    }

    headers = {
        "Content-Type": "application/json",
        "Accept":        "application/json, text/plain, */*",
        "Origin":        "https://www.copart.com",
        "Referer":       "https://www.copart.com/lotSearchResults",
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                proxy_url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=60,   # ScraperAPI can be slow — give it time
            )
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 403:
                print(f"    ScraperAPI 403 (attempt {attempt+1}) — retrying...")
                time.sleep(5 * (attempt + 1))
            elif resp.status_code == 429:
                print(f"    Rate limited (attempt {attempt+1}) — waiting 15s...")
                time.sleep(15)
            else:
                print(f"    ScraperAPI HTTP {resp.status_code} (attempt {attempt+1})")
                print(f"    Response: {resp.text[:300]}")
                time.sleep(5)
        except requests.exceptions.Timeout:
            print(f"    Timeout (attempt {attempt+1})")
            time.sleep(10)
        except Exception as e:
            print(f"    Error: {e} (attempt {attempt+1})")
            time.sleep(5)
    return None


# ─────────────────────────────────────────────────────────────
# PAYLOAD
# ─────────────────────────────────────────────────────────────

def build_payload(page: int) -> dict:
    today     = datetime.now(timezone.utc)
    cutoff    = today + timedelta(days=UPCOMING_DAYS)
    today_ms  = int(today.timestamp() * 1000)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    return {
        "query": ["*"],
        "filter": {
            "YEAR": [f"lot_year:[{UPCOMING_YEAR_MIN} TO {UPCOMING_YEAR_MAX}]"],
            "MISC": [
                "#VehicleTypeCode:VEHTYPE_V",
                f'#LocState:"{UPCOMING_STATE}"',
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
# PARSING
# ─────────────────────────────────────────────────────────────

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


def gf(lot: dict, *keys) -> str:
    for k in keys:
        v = lot.get(k)
        if v is not None and str(v).strip() not in ("", "None", "null"):
            return str(v).strip()
    return ""


def normalise_date(raw: str) -> str:
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
    raw_date = gf(lot, "auctionDate", "saleDate", "auctionDateType", "soldDate")
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
        "date":     normalise_date(raw_date),
        "type":     gf(lot, "titleTypeCode", "lotType", "titleType"),
    }


def build_record(raw: dict) -> dict | None:
    state    = raw.get("state", "").strip().upper()
    location = raw.get("location", "").strip()

    if not state and location:
        sm = re.search(r"\b([A-Z]{2})$", location.upper())
        if sm and sm.group(1) in US_STATES:
            state = sm.group(1)

    if state != UPCOMING_STATE:
        return None

    year  = str(raw.get("year", "")).strip()
    make  = cap(raw.get("make", ""))
    model = cap(raw.get("model", ""))
    if not year or not make or not model:
        return None

    lot        = str(raw.get("lot",  "")).strip()
    vin        = str(raw.get("vin",  "")).strip().upper()
    trim       = str(raw.get("trim", "")).strip()
    title_type = str(raw.get("type", "")).strip()
    damage     = cap(raw.get("damage", "")) or "Unknown"
    sale_date  = str(raw.get("date", "")).strip()
    bid        = clean_price(raw.get("price", ""))
    odo        = clean_odo(raw.get("odometer", ""))

    if sale_date:
        try:
            sale_dt = datetime.strptime(sale_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            now     = datetime.now(timezone.utc)
            cutoff  = now + timedelta(days=UPCOMING_DAYS)
            if not (now <= sale_dt <= cutoff):
                return None
        except Exception:
            pass

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
    print(f"  Copart Upcoming Lots Scraper (via ScraperAPI)")
    print(f"  State  : {UPCOMING_STATE} (California)")
    print(f"  Window : next {UPCOMING_DAYS} days")
    print(f"  Date   : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Key    : {'set ✓' if SCRAPER_API_KEY else 'MISSING — set SCRAPER_API_KEY'}")
    print("=" * 60)

    if not SCRAPER_API_KEY:
        print("\nERROR: SCRAPER_API_KEY not set.")
        print("Sign up free at https://www.scraperapi.com")
        print("Then add it as a GitHub Actions secret named SCRAPER_API_KEY")
        raise SystemExit(1)

    # Use a fixed session number so ScraperAPI keeps the same IP + cookies
    # across all page requests — important for Copart's session validation
    SESSION_NUM = random.randint(1, 9999)

    all_lots = []
    total_pg = None

    for page in range(0, MAX_PAGES):
        print(f"\n  Page {page+1}{f'/{total_pg}' if total_pg else ''}...", end=" ", flush=True)

        payload = build_payload(page)
        resp    = scraper_api_post(COPART_SEARCH_URL, payload, SESSION_NUM)

        if resp is None:
            print("failed — stopping")
            break

        try:
            raw = resp.json()
            data = raw.get("data") or raw.get("returnValue") or raw
        except Exception as e:
            print(f"JSON parse error: {e}")
            print(f"Response text: {resp.text[:400]}")
            break

        # Debug first page
        if page == 0:
            print(f"\n  Response keys: {list(data.keys())[:10]}")

        lots = extract_lots(data)
        if not lots:
            print("empty — done")
            break

        if total_pg is None:
            total_pg = total_pages(data)
            print(f"total pages: {total_pg}", end=" ")

        all_lots.extend(lots)
        print(f"→ {len(lots)} lots (total: {len(all_lots)})")

        if page + 1 >= total_pg:
            print(f"  Reached last page ({total_pg})")
            break

        time.sleep(REQUEST_DELAY + random.uniform(0, 0.5))

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
        print("\n  [INFO] No CA lots found — writing empty upcoming.csv")
        if all_lots:
            print("  [DEBUG] Raw lots received but all filtered out. Sample raw lot:")
            sample = lot_to_raw(all_lots[0])
            for k, v in sample.items():
                print(f"    {k}: {v}")
        pd.DataFrame(columns=cols).to_csv(UPCOMING_CSV, index=False)
    else:
        df = pd.DataFrame(records)
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df[cols].sort_values("sale_date")
        df.to_csv(UPCOMING_CSV, index=False)
        print(f"\n  Sample output (first 3 rows):")
        print(df.head(3).to_string())

    elapsed = int(time.time() - t0)
    print("\n" + "=" * 60)
    print(f"  Raw lots received : {len(all_lots)}")
    print(f"  CA lots written   : {len(records)}")
    print(f"  Skipped           : {skipped}")
    print(f"  Output            : {UPCOMING_CSV}")
    print(f"  Elapsed           : {elapsed}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
