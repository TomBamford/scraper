"""
Copart Upcoming Lots Scraper — California only
===============================================
Scrapes Copart for CA vehicles going up for auction in the next 7 days
and writes upcoming.csv to this repo.

Uses Copart's public lotSearchResults API — the same endpoint their
website uses, which works without a browser session.

Install:
    pip install requests pandas

Run manually:
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
REQUEST_DELAY = 2.0

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU",
}

# ─────────────────────────────────────────────────────────────
# COPART PUBLIC API  — no login required
# ─────────────────────────────────────────────────────────────
# This is the endpoint Copart's own website calls for its search results page.
# It accepts a JSON search criteria and returns paginated lot data.
SEARCH_URL = "https://www.copart.com/public/lots/search-results"

# Rotate user agents to reduce blocking
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Origin": "https://www.copart.com",
        "Referer": "https://www.copart.com/lotSearchResults",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Connection": "keep-alive",
    }


def build_payload(page: int) -> dict:
    """
    Mirrors exactly what Copart's own search page sends.
    Filters: CA state, automobiles only, years 2000-2027, upcoming auction date.
    """
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
# SESSION — seeds cookies like a real browser visit
# ─────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    s = requests.Session()

    # Step 1: Hit the homepage to get initial cookies
    try:
        print("  Seeding session cookies from Copart homepage...")
        r = s.get(
            "https://www.copart.com/",
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=20,
        )
        print(f"  Homepage: {r.status_code}, cookies: {list(s.cookies.keys())}")
        time.sleep(random.uniform(1.5, 2.5))
    except Exception as e:
        print(f"  [WARN] Homepage seed failed: {e}")

    # Step 2: Hit the search results page to get the search-specific cookies
    try:
        print("  Seeding search page cookies...")
        r = s.get(
            "https://www.copart.com/lotSearchResults",
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.copart.com/",
            },
            timeout=20,
        )
        print(f"  Search page: {r.status_code}, cookies: {list(s.cookies.keys())}")
        time.sleep(random.uniform(1.0, 2.0))
    except Exception as e:
        print(f"  [WARN] Search page seed failed: {e}")

    return s


# ─────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────

def fetch_page(session: requests.Session, page: int) -> dict | None:
    payload = build_payload(page)
    headers = get_headers()

    for attempt in range(4):
        try:
            resp = session.post(
                SEARCH_URL,
                json=payload,
                headers=headers,
                timeout=30,
            )

            print(f"    Status: {resp.status_code}", end=" ")

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    # Copart wraps in data.data, data.returnValue, or returns directly
                    return data.get("data") or data.get("returnValue") or data
                except Exception as e:
                    print(f"JSON parse error: {e}")
                    print(f"    Response text: {resp.text[:300]}")
                    return None

            elif resp.status_code == 403:
                print(f"403 — blocked, waiting {10 * (attempt+1)}s...")
                time.sleep(10 * (attempt + 1))
            elif resp.status_code == 429:
                print(f"429 — rate limited, waiting 30s...")
                time.sleep(30)
            else:
                print(f"unexpected status")
                print(f"    Response: {resp.text[:200]}")
                time.sleep(5)

        except requests.exceptions.Timeout:
            print(f"  [WARN] Timeout on page {page} attempt {attempt+1}")
            time.sleep(5)
        except Exception as e:
            print(f"  [WARN] Error: {e}")
            time.sleep(5)

    return None


def extract_lots(data: dict) -> list[dict]:
    try:
        results = (
            data.get("results", {})
            or data.get("lotSearchResults", {})
            or {}
        )
        content = results.get("content") or data.get("content") or []
        return list(content)
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
    print(f"  Copart Upcoming Lots Scraper")
    print(f"  State  : {UPCOMING_STATE} (California)")
    print(f"  Window : next {UPCOMING_DAYS} days")
    print(f"  Date   : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    session  = get_session()
    all_lots = []
    total_pg = None

    for page in range(0, MAX_PAGES):
        print(f"\n  Page {page+1}{f'/{total_pg}' if total_pg else ''}...", end=" ", flush=True)
        data = fetch_page(session, page)

        if data is None:
            print("no data — stopping")
            break

        # Debug: show raw keys on first page
        if page == 0:
            print(f"\n  Response keys: {list(data.keys())[:8]}")

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
        print("  [DEBUG] Raw lots received:", len(all_lots))
        if all_lots:
            print("  [DEBUG] Sample raw lot keys:", list(all_lots[0].keys())[:15])
            sample = lot_to_raw(all_lots[0])
            print("  [DEBUG] Sample mapped:", sample)
        pd.DataFrame(columns=cols).to_csv(UPCOMING_CSV, index=False)
    else:
        df = pd.DataFrame(records)
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df[cols].sort_values("sale_date")
        df.to_csv(UPCOMING_CSV, index=False)
        print(f"\n  Sample output:")
        print(df.head(3).to_string())

    elapsed = int(time.time() - t0)
    print("\n" + "=" * 60)
    print(f"  Raw lots received : {len(all_lots)}")
    print(f"  CA lots written   : {len(records)}")
    print(f"  Skipped           : {skipped}")
    print(f"  Output file       : {UPCOMING_CSV}")
    print(f"  Elapsed           : {elapsed}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
