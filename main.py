"""
Salvage auction scraper — Copart.com internal search API
=========================================================
Copart powers its own website with a JSON REST API. This scraper
calls that API directly with requests (no browser, no Playwright).
Picks a random make each run and pages through sold lots 2008-2015.

Install:
    pip install requests pandas

Run:
    python scraper_copart.py
"""

import os, re, time, random, json
import requests
import pandas as pd

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
YEAR_MIN = 2008
YEAR_MAX = 2015
MIN_PRICE = 500
REQUIRE_PRICE = True

MASTER_CSV  = "master.csv"
WEBSITE_CSV = "cars.csv"
RESUME_FROM_MASTER = True

ROWS_PER_PAGE  = 100     # Copart supports up to 100
MAX_PAGES      = 9999
REQUEST_DELAY  = 1.2     # seconds between requests

EXCLUDED_MAKES = {"LAND ROVER"}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU",
}

# Copart make names (as they appear in the API's makerDescription field)
ALL_MAKES = [
    "ACURA","ALFA ROMEO","AUDI","BENTLEY","BMW","BUICK","CADILLAC",
    "CHEVROLET","CHRYSLER","DODGE","FERRARI","FIAT","FORD","GMC",
    "HONDA","HUMMER","HYUNDAI","INFINITI","JAGUAR","JEEP","KIA",
    "LEXUS","LINCOLN","MAZDA","MERCEDES-BENZ","MERCURY",
    "MINI","MITSUBISHI","NISSAN","OLDSMOBILE","PONTIAC","PORSCHE",
    "RAM","SAAB","SATURN","SCION","SMART","SUBARU","SUZUKI",
    "TESLA","TOYOTA","VOLKSWAGEN","VOLVO",
]

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

def copart_payload(make: str, page: int) -> dict:
    """Build the POST body for Copart's search endpoint."""
    return {
        "query": ["*"],
        "filter": {
            "MAKE": [make],
            "YEAR": [str(y) for y in range(YEAR_MIN, YEAR_MAX + 1)],
            "COUNTRY_CODE": ["US"],
            # Sold/completed lots only
            "SALE_STATUS": ["SALE_UPCOMING", "ON_SALE", "SALE_FUTURE",
                            "BIDDING_CLOSED", "SALE_TODAY"],
        },
        "sort": None,
        "page": page,
        "size": ROWS_PER_PAGE,
        "watchListOnly": False,
        "freeFormSearch": False,
    }

def copart_payload_v2(make: str, page: int) -> dict:
    """Alternative payload format used by some Copart API versions."""
    return {
        "searchCriteria": {
            "query": [make],
            "filter": {
                "YEAR": {
                    "from": str(YEAR_MIN),
                    "to": str(YEAR_MAX),
                },
                "COUNTRY_CODE": ["US"],
            },
            "sort": {"auction_date_type": "asc"},
            "page": page,
            "size": ROWS_PER_PAGE,
        }
    }


def fetch_page(session: requests.Session, make: str, page: int) -> dict | None:
    """POST to Copart search API and return parsed JSON, or None on failure."""
    for attempt in range(3):
        try:
            resp = session.post(
                SEARCH_URL,
                json=copart_payload(make, page),
                timeout=20,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 403:
                print(f"  [WARN] 403 on page {page} — Copart may require a session cookie")
                return None
            else:
                print(f"  [WARN] HTTP {resp.status_code} on page {page}, attempt {attempt+1}")
        except Exception as e:
            print(f"  [WARN] Request error page {page}: {e}")
        time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


def get_session() -> requests.Session:
    """Create a requests session, seeding it with a real browser-like cookie."""
    s = requests.Session()
    s.headers.update(HEADERS)
    # Hit the homepage first to get cookies/CSRF token just like a browser would
    try:
        s.get("https://www.copart.com/", timeout=15)
        time.sleep(1.0)
    except Exception:
        pass
    return s


# ─────────────────────────────────────────────────────────────
# DATA EXTRACTION
# ─────────────────────────────────────────────────────────────
def extract_lots(data: dict) -> list[dict]:
    """Pull lot records out of Copart API response."""
    lots = []
    try:
        # Response structure: data.results.content[] or data.data.results.content[]
        content = (
            data.get("data", {}).get("results", {}).get("content")
            or data.get("results", {}).get("content")
            or data.get("content")
            or []
        )
        for item in content:
            lots.append(item)
    except Exception as e:
        print(f"  [WARN] extract_lots: {e}")
    return lots


def total_pages(data: dict) -> int:
    try:
        total = (
            data.get("data", {}).get("results", {}).get("totalPages")
            or data.get("results", {}).get("totalPages")
            or data.get("totalPages")
            or 1
        )
        return int(total)
    except Exception:
        return 1


def lot_to_raw(lot: dict) -> dict:
    """Map Copart lot fields to our internal raw dict."""
    # Copart field names vary slightly by API version — check multiple keys
    def g(*keys):
        for k in keys:
            v = lot.get(k)
            if v is not None and str(v).strip() not in ("", "None"):
                return str(v).strip()
        return ""

    year = g("year", "YEAR", "modelYear")
    make = g("make", "MAKE", "makerDescription", "make_desc")
    model = g("model", "MODEL", "modelDescription", "model_desc")
    vin = g("vin", "VIN", "vehicleId")
    lot_num = g("lotNumberStr", "lotNumber", "LOT_NUMBER", "lot_number", "ln")
    damage = g("primaryDamage", "PRIMARY_DAMAGE", "damageDescription", "damage_desc")
    price = g("currentBid", "CURRENT_BID", "salePrice", "sale_price", "highBid", "high_bid")
    odometer = g("odometer", "ODOMETER", "odometerReading", "odom")
    location = g("location", "LOCATION", "yardName", "yard_name", "auctionLocationName")
    state_code = g("stateCode", "STATE_CODE", "stateName", "state")
    date = g("saleDate", "SALE_DATE", "auctionDate", "auction_date", "soldDate")
    trim = g("trim", "TRIM", "seriesDesc", "series")
    source = "Copart"

    # Normalise date to YYYY-MM-DD
    if date and re.search(r"\d", date):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", date)
        if m:
            date = m.group(1)
        else:
            m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", date)
            if m:
                date = f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"

    # State from location string if not given directly
    if not state_code and location:
        sm = re.search(r"\b([A-Z]{2})$", location.strip().upper())
        if sm and sm.group(1) in US_STATES:
            state_code = sm.group(1)

    return {
        "year": year, "make": make, "model": model, "trim": trim,
        "vin": vin, "lot": lot_num, "damage": damage,
        "price": price, "odometer": odometer,
        "location": location, "state": state_code,
        "date": date, "source": source,
        "url": f"https://www.copart.com/lot/{lot_num}" if lot_num else "",
    }


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def nw(t) -> str:
    return re.sub(r"\s+", " ", str(t)).strip()

def year_ok(y) -> bool:
    try:
        return YEAR_MIN <= int(str(y).strip()) <= YEAR_MAX
    except Exception:
        return False

def clean_price(t) -> int:
    s = str(t).replace("$", "").replace(",", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return int(float(m.group(1))) if m else 0

def clean_odo(t: str) -> str:
    s = str(t).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m:
        return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"

def infer_type(text: str) -> str:
    t = text.upper()
    if any(x in t for x in ["PICKUP","F-150","F150","SILVERADO","RAM 1","TUNDRA","RANGER","TACOMA","FRONTIER","COLORADO","F-250","F-350"]):
        return "Truck"
    if any(x in t for x in ["SUV","EXPLORER","ESCAPE","ROGUE","EQUINOX","TAHOE","SUBURBAN","RAV4","CR-V","CX-5","SORENTO","SPORTAGE","PILOT","HIGHLANDER","PATHFINDER","TRAVERSE","EXPEDITION","4RUNNER","SANTA FE","TUCSON","OUTLANDER","DURANGO"]):
        return "SUV"
    if any(x in t for x in ["COUPE","MUSTANG","CHALLENGER","CAMARO","BRZ","CORVETTE"]):
        return "Coupe"
    if any(x in t for x in ["VAN","TRANSIT","ODYSSEY","SIENNA","PACIFICA","CARAVAN","ECONOLINE","EXPRESS","SAVANA","PROMASTER"]):
        return "Van"
    return "Sedan"

def load_existing_keys() -> set:
    if not RESUME_FROM_MASTER or not os.path.exists(MASTER_CSV):
        return set()
    try:
        df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
        for c in ("vin", "lot"):
            if c not in df.columns:
                df[c] = ""
        keys = set((df["vin"].str.strip() + "|" + df["lot"].str.strip()).tolist())
        keys.discard("|")
        print(f"[resume] {len(keys)} existing records loaded")
        return keys
    except Exception as e:
        print(f"[WARN] load keys: {e}")
        return set()

def build_record(raw: dict, existing_keys: set, seen_keys: set, stats: dict):
    year = str(raw.get("year", "")).strip()
    if not year_ok(year):
        stats["year_out_of_range"] += 1
        return None

    price = clean_price(raw.get("price", ""))
    if REQUIRE_PRICE and price < MIN_PRICE:
        stats["price_below_min"] += 1
        return None

    make  = str(raw.get("make", "")).strip().title()
    model = str(raw.get("model", "")).strip().title()
    if not make or not model:
        stats["incomplete"] += 1
        return None

    if make.upper() in EXCLUDED_MAKES:
        stats["excluded_make"] += 1
        return None

    state = str(raw.get("state", "")).strip().upper()
    if state and state not in US_STATES:
        stats["non_us"] = stats.get("non_us", 0) + 1
        return None

    vin = str(raw.get("vin", "")).strip().upper()
    lot = str(raw.get("lot", "")).strip()
    if not vin and not lot:
        stats["incomplete"] += 1
        return None

    key = f"{vin}|{lot}"
    if key in seen_keys:
        stats["duplicate"] += 1
        return None
    if key in existing_keys:
        stats["existing"] += 1
        return None

    location = str(raw.get("location", "")).strip().title()

    seen_keys.add(key)
    existing_keys.add(key)
    stats["kept"] += 1

    return {
        "vin":      vin,
        "year":     year,
        "make":     make,
        "model":    model,
        "trim":     str(raw.get("trim", "")).strip(),
        "type":     infer_type(f"{make} {model} {raw.get('trim','')}"),
        "damage":   str(raw.get("damage", "")).strip().title(),
        "price":    price,
        "odometer": clean_odo(raw.get("odometer", "")),
        "lot":      lot,
        "date":     str(raw.get("date",  "")).strip(),
        "location": location,
        "state":    state,
        "source":   str(raw.get("source", "Copart")).strip(),
        "url":      str(raw.get("url",   "")).strip(),
    }


# ─────────────────────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────────────────────
def write_csvs(data: list) -> int:
    cols_m = ["vin","year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","source","url"]
    cols_w = ["year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","url"]

    master = (pd.read_csv(MASTER_CSV, dtype=str).fillna("")
              if os.path.exists(MASTER_CSV) else pd.DataFrame(columns=cols_m))

    if data:
        ndf = pd.DataFrame(data)
        for c in cols_m:
            if c not in ndf.columns:
                ndf[c] = ""
        ndf = ndf[cols_m]
        ndf["price"] = pd.to_numeric(ndf["price"], errors="coerce").fillna(0).astype(int)
        master = pd.concat([master, ndf], ignore_index=True)

    if not master.empty:
        master["price"] = pd.to_numeric(master["price"], errors="coerce").fillna(0).astype(int)
        master["_yr"]   = pd.to_numeric(master["year"],  errors="coerce")
        master = master[master["price"] >= MIN_PRICE]
        master = master[master["_yr"].between(YEAR_MIN, YEAR_MAX, inclusive="both")]
        master = master[~master["make"].str.upper().isin(EXCLUDED_MAKES)]
        master = master.drop_duplicates(subset=["vin","lot"], keep="first")
        master = master.drop(columns=["_yr"])

    master.to_csv(MASTER_CSV, index=False)
    site = master.copy()
    for c in cols_w:
        if c not in site.columns:
            site[c] = ""
    site.reindex(columns=cols_w, fill_value="").to_csv(WEBSITE_CSV, index=False)
    return len(master)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    existing_keys = load_existing_keys()
    seen_keys: set = set()
    stats = {k: 0 for k in [
        "price_below_min","year_out_of_range","excluded_make",
        "duplicate","existing","incomplete","non_us","kept"
    ]}

    # Pick a random make
    make = random.choice([m for m in ALL_MAKES if m not in EXCLUDED_MAKES])

    print("=" * 55)
    print(f"  Copart API scraper")
    print(f"  Selected make : {make}")
    print(f"  Year range    : {YEAR_MIN}–{YEAR_MAX}")
    print(f"  Min price     : ${MIN_PRICE}")
    print(f"  Existing recs : {len(existing_keys)}")
    print("=" * 55)

    session = get_session()
    results = []
    total_pg = None

    for page in range(0, MAX_PAGES):
        print(f"\n  Page {page+1}{f'/{total_pg}' if total_pg else ''}...")
        data = fetch_page(session, make, page)

        if data is None:
            print("  API returned no data — stopping")
            break

        # Debug: show raw response structure on first page
        if page == 0:
            keys_seen = list(data.keys())[:10]
            print(f"  Response keys: {keys_seen}")

        lots = extract_lots(data)
        if not lots:
            print("  No lots in response — done")
            break

        if total_pg is None:
            total_pg = total_pages(data)
            print(f"  Total pages: {total_pg}")

        page_kept = 0
        for lot in lots:
            raw = lot_to_raw(lot)
            rec = build_record(raw, existing_keys, seen_keys, stats)
            if rec:
                results.append(rec)
                page_kept += 1

        print(f"  Lots: {len(lots)} received, {page_kept} kept (total {len(results)})")

        if page + 1 >= total_pg:
            print("  Reached last page")
            break

        time.sleep(REQUEST_DELAY + random.uniform(0, 0.5))

    total = write_csvs(results)

    print("\n" + "=" * 55)
    print(f"  Make scraped :  {make}")
    print(f"  New rows     :  {len(results)}")
    print(f"  Master total :  {total}")
    print(f"  Skip stats   :  {stats}")
    print(f"  Elapsed      :  {int(time.time()-t0)}s")
    print("=" * 55)
    print("\nRun again to scrape a different random make.")


if __name__ == "__main__":
    main()
