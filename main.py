"""
Copart Historical Sales Harvester
===================================
Combines two free public data sources to build a historical comp database
with real final auction prices:

  SOURCE 1 — Rebrowser GitHub (rebrowser/copart-dataset)
    Free daily parquet snapshots of Copart lots: year, make, model,
    damage, mileage, location, title type, repair cost.
    Updated daily. No login needed. ~1,000 rows per day file.

  SOURCE 2 — AutoBidCar public API (autobidcar.com)
    Free API: given a VIN → returns the car's detail page URL.
    The detail page shows the final hammer price Copart sold the car for.
    Rate limit: 100 requests/minute (no key needed).

Pipeline:
  1. Download last N days of parquet files from Rebrowser
  2. Filter for California lots (or all US — configurable)
  3. For each lot with a VIN, call AutoBidCar API → get detail page URL
  4. Scrape detail page → extract final sold price
  5. Combine everything → write cars.csv in your repo format

Install:
    pip install requests pandas pyarrow beautifulsoup4

Run:
    python harvest_history.py

Put this in .github/workflows/ to run weekly and grow your comps database.
"""

import os, re, time, random, io
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
DAYS_TO_FETCH    = 30          # fetch last N days of Rebrowser data
STATE_FILTER     = "CA"        # filter lots to this state ("" = all US)
MIN_YEAR         = 2015        # skip older vehicles
OUTPUT_CSV       = "cars.csv"  # appends to existing file
MASTER_CSV       = "master.csv"

# AutoBidCar rate limit is 100/min — stay safely under
API_DELAY        = 0.8         # seconds between AutoBidCar API calls
SCRAPE_DELAY     = 1.2         # seconds between page fetches
MAX_VIN_LOOKUPS  = 500         # max VIN lookups per run (to stay within rate limits)

# Rebrowser GitHub raw base URL for parquet files
REBROWSER_BASE = (
    "https://raw.githubusercontent.com/rebrowser/copart-dataset"
    "/main/auction-listings/data/"
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ─────────────────────────────────────────────────────────────
# STEP 1 — DOWNLOAD REBROWSER PARQUET FILES
# ─────────────────────────────────────────────────────────────

def download_rebrowser_lots(days: int, state_filter: str, min_year: int) -> pd.DataFrame:
    """
    Download the last N days of Copart parquet snapshots from the
    Rebrowser GitHub repo. Returns a filtered DataFrame.
    """
    session = requests.Session()
    session.headers["User-Agent"] = random.choice(USER_AGENTS)

    all_frames = []
    today = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  STEP 1: Downloading Rebrowser Copart data ({days} days)")
    print(f"{'='*60}")

    for i in range(days):
        date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        url  = REBROWSER_BASE + date + ".parquet"

        try:
            r = session.get(url, timeout=20)
            if r.status_code == 404:
                continue  # file doesn't exist for this date
            if r.status_code != 200:
                print(f"  [{date}] HTTP {r.status_code} — skipping")
                continue

            df = pd.read_parquet(io.BytesIO(r.content))

            # Apply filters immediately to save memory
            if state_filter:
                df = df[df.get("locationState", pd.Series(dtype=str)) == state_filter]
            if min_year:
                df = df[pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce") >= min_year]

            rows = len(df)
            print(f"  [{date}] {rows} rows after filter", end="")
            if rows > 0:
                all_frames.append(df)
                print(f" ✓")
            else:
                print()

        except Exception as e:
            print(f"  [{date}] Error: {e}")

        time.sleep(0.3)

    if not all_frames:
        print("  No data downloaded.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\n  Total rows downloaded: {len(combined)}")
    return combined


# ─────────────────────────────────────────────────────────────
# STEP 2 — LOAD EXISTING VINs (skip already-priced ones)
# ─────────────────────────────────────────────────────────────

def load_existing_vins() -> set:
    """Load VINs already in master.csv to avoid re-fetching."""
    existing = set()
    for fname in [MASTER_CSV, OUTPUT_CSV]:
        if os.path.exists(fname):
            try:
                df = pd.read_csv(fname, dtype=str, usecols=lambda c: c in ["vin"])
                existing.update(df["vin"].dropna().str.strip().str.upper().tolist())
            except Exception:
                pass
    existing.discard("")
    print(f"\n  Existing VINs in database: {len(existing):,}")
    return existing


# ─────────────────────────────────────────────────────────────
# STEP 3 — AUTOBIDCAR VIN LOOKUP + PRICE SCRAPE
# ─────────────────────────────────────────────────────────────

def get_autobidcar_url(vin: str, session: requests.Session) -> str | None:
    """
    Call AutoBidCar's free check API to get the car's detail page URL.
    Returns URL string or None if not found.
    """
    try:
        r = session.get(
            f"https://autobidcar.com/api/check/{vin}",
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("full_url")
        elif r.status_code == 404:
            return None  # not in database
        else:
            print(f"    AutoBidCar API {r.status_code} for {vin}")
            return None
    except Exception as e:
        print(f"    AutoBidCar error for {vin}: {e}")
        return None


def scrape_final_price(car_url: str, session: requests.Session) -> int | None:
    """
    Scrape the AutoBidCar car detail page and extract the final sold price.
    AutoBidCar shows the Copart/IAAI final hammer price.
    """
    try:
        r = session.get(car_url, timeout=20)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Method 1: Look for __NEXT_DATA__ JSON (Next.js)
        next_data = soup.find("script", {"id": "__NEXT_DATA__"})
        if next_data:
            import json
            try:
                data = json.loads(next_data.string)
                page_props = data.get("props", {}).get("pageProps", {})

                # Common field names AutoBidCar uses
                for field in ["finalBid", "final_bid", "soldPrice", "sold_price",
                               "lastBid", "last_bid", "highBid", "high_bid", "price"]:
                    val = page_props.get(field) or page_props.get("car", {}).get(field)
                    if val and str(val).replace(".", "").isdigit():
                        price = int(float(str(val).replace(",", "")))
                        if price > 50:  # sanity check
                            return price

                # Try nested car object
                car = page_props.get("car", {})
                for field in ["finalBid", "soldPrice", "highBid", "price", "bid"]:
                    val = car.get(field)
                    if val and str(val).replace(".", "").isdigit():
                        price = int(float(str(val).replace(",", "")))
                        if price > 50:
                            return price
            except Exception:
                pass

        # Method 2: Search HTML for price patterns near "final", "sold", "bid" keywords
        text = soup.get_text(" ", strip=True)

        # Pattern: "Final Bid: $4,250" or "Sold for $4,250" or "Winning Bid $4,250"
        patterns = [
            r'(?:final\s*bid|sold\s*(?:for|price)|winning\s*bid|hammer\s*price)[^\d]{0,20}(\d[\d,]+)',
            r'(\d[\d,]+)\s*(?:USD|usd|\$)',
            r'\$\s*(\d[\d,]+)',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for m in matches:
                price = int(m.replace(",", ""))
                if 200 <= price <= 200000:  # realistic salvage range
                    return price

        # Method 3: Look for specific HTML elements with price class
        for el in soup.find_all(class_=re.compile(r'price|bid|sold', re.I)):
            t = el.get_text(strip=True)
            m = re.search(r'(\d[\d,]+)', t)
            if m:
                price = int(m.group(1).replace(",", ""))
                if 200 <= price <= 200000:
                    return price

    except Exception as e:
        print(f"    Scrape error {car_url}: {e}")

    return None


def enrich_with_prices(df: pd.DataFrame, existing_vins: set) -> pd.DataFrame:
    """
    For each lot with a VIN, look up final price via AutoBidCar.
    Returns enriched DataFrame with 'final_price' column.
    """
    print(f"\n{'='*60}")
    print(f"  STEP 3: AutoBidCar VIN price lookup")
    print(f"{'='*60}")

    session = requests.Session()
    session.headers["User-Agent"] = random.choice(USER_AGENTS)
    session.headers["Accept"] = "text/html,application/json,*/*"

    # Seed session
    try:
        session.get("https://autobidcar.com/", timeout=10)
        time.sleep(1.0)
    except Exception:
        pass

    df = df.copy()
    df["final_price"] = None

    # Only look up VINs we don't already have
    vin_col = None
    for c in ["vin", "VIN", "vehicleId"]:
        if c in df.columns:
            vin_col = c
            break

    if not vin_col:
        print("  No VIN column found — skipping price lookup")
        return df

    # Get unique VINs that are new and valid
    mask = (
        df[vin_col].notna() &
        (df[vin_col] != "[PREMIUM]") &
        (df[vin_col].str.len() == 17) &
        (~df[vin_col].str.upper().isin(existing_vins))
    )
    candidates = df[mask][vin_col].unique()[:MAX_VIN_LOOKUPS]

    print(f"  VINs to look up: {len(candidates):,} (capped at {MAX_VIN_LOOKUPS})")

    price_map = {}  # vin → final_price
    found = 0
    checked = 0

    for vin in candidates:
        checked += 1
        if checked % 50 == 0:
            pct = checked / len(candidates) * 100
            print(f"  Progress: {checked}/{len(candidates)} ({pct:.0f}%) — {found} prices found")

        # Step A: get car URL
        car_url = get_autobidcar_url(vin, session)
        time.sleep(API_DELAY + random.uniform(0, 0.3))

        if not car_url:
            continue

        # Step B: scrape final price from car page
        price = scrape_final_price(car_url, session)
        time.sleep(SCRAPE_DELAY + random.uniform(0, 0.4))

        if price:
            price_map[vin.upper()] = price
            found += 1

    print(f"\n  Final prices found: {found} / {checked} VINs checked")

    # Map prices back to DataFrame
    df["final_price"] = df[vin_col].str.upper().map(price_map)

    return df


# ─────────────────────────────────────────────────────────────
# STEP 4 — NORMALIZE + WRITE cars.csv
# ─────────────────────────────────────────────────────────────

def normalize_to_cars_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map Rebrowser column names → your cars.csv schema:
    year, make, model, trim, type, damage, price, odometer, lot, date, location, state, url
    """
    def col(df, *names):
        for n in names:
            if n in df.columns:
                return df[n]
        return pd.Series("", index=df.index)

    out = pd.DataFrame()

    out["year"]     = pd.to_numeric(col(df, "year"), errors="coerce").fillna(0).astype(int)
    out["make"]     = col(df, "make").str.strip().str.title()
    out["model"]    = col(df, "modelGroup", "modelDetail", "model").str.strip().str.title()
    out["trim"]     = col(df, "trim").str.strip()
    out["type"]     = col(df, "saleTitleType").str.strip()
    out["damage"]   = col(df, "damageDescription").str.strip().str.title()
    out["odometer"] = pd.to_numeric(col(df, "mileage"), errors="coerce").fillna(0).astype(int)
    out["lot"]      = col(df, "lotId").str.strip()
    out["vin"]      = col(df, "vin").str.strip().str.upper()
    out["location"] = (col(df, "yardName").str.strip()
                       .where(col(df, "yardName") != "",
                              col(df, "locationCity").str.strip() + ", " + col(df, "locationState").str.strip()))
    out["state"]    = col(df, "locationState").str.strip().str.upper()
    out["repair_cost"] = pd.to_numeric(col(df, "repairCost"), errors="coerce").fillna(0).astype(int)
    out["url"]      = col(df, "listingUrl").str.strip()

    # Use final_price (real sold price) if available, else 0
    if "final_price" in df.columns:
        out["price"] = pd.to_numeric(df["final_price"], errors="coerce").fillna(0).astype(int)
    else:
        out["price"] = 0

    # Normalize date
    raw_date = col(df, "saleDate")
    out["date"] = raw_date.apply(lambda v: normalize_date(str(v)) if pd.notna(v) and v != "" else "")

    # Only keep rows with enough info
    out = out[
        (out["year"] >= MIN_YEAR) &
        out["make"].notna() & (out["make"] != "") &
        out["model"].notna() & (out["model"] != "")
    ]

    return out


def normalize_date(raw: str) -> str:
    import re
    if not raw or raw in ("NaT", "None", "nan", ""):
        return ""
    # Already ISO
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    return ""


def write_csvs(new_df: pd.DataFrame):
    """Append new records to master.csv and rebuild cars.csv."""
    print(f"\n{'='*60}")
    print(f"  STEP 4: Writing output files")
    print(f"{'='*60}")

    cols_master = ["vin","year","make","model","trim","type","damage",
                   "price","odometer","lot","date","location","state",
                   "repair_cost","url"]
    cols_site   = ["year","make","model","trim","type","damage",
                   "price","odometer","lot","date","location","state","url"]

    # Load existing master
    if os.path.exists(MASTER_CSV):
        master = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
    else:
        master = pd.DataFrame(columns=cols_master)

    # Ensure all columns exist in new_df
    for c in cols_master:
        if c not in new_df.columns:
            new_df[c] = ""

    new_df = new_df[cols_master].copy()
    new_df["price"] = pd.to_numeric(new_df["price"], errors="coerce").fillna(0).astype(int)

    # Concatenate and deduplicate by lot + vin
    combined = pd.concat([master, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["lot"], keep="first")

    # Stats
    with_price = (pd.to_numeric(combined["price"], errors="coerce").fillna(0) > 0).sum()
    print(f"  Master records   : {len(combined):,}")
    print(f"  With final price : {with_price:,}")
    print(f"  Without price    : {len(combined) - with_price:,}")

    combined.to_csv(MASTER_CSV, index=False)
    print(f"  Written: {MASTER_CSV}")

    # Site CSV (no VINs, only records with prices)
    site = combined[pd.to_numeric(combined["price"], errors="coerce").fillna(0) > 0].copy()
    for c in cols_site:
        if c not in site.columns:
            site[c] = ""
    site[cols_site].to_csv(OUTPUT_CSV, index=False)
    print(f"  Written: {OUTPUT_CSV} ({len(site):,} priced records)")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print("=" * 60)
    print("  Copart Historical Sales Harvester")
    print(f"  Date     : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  State    : {STATE_FILTER or 'All US'}")
    print(f"  Days     : last {DAYS_TO_FETCH} days of Rebrowser data")
    print(f"  Max VINs : {MAX_VIN_LOOKUPS} price lookups per run")
    print("=" * 60)

    # 1. Download Rebrowser lot data
    lots_df = download_rebrowser_lots(DAYS_TO_FETCH, STATE_FILTER, MIN_YEAR)
    if lots_df.empty:
        print("No lots downloaded. Exiting.")
        return

    print(f"\n  Lots after filter: {len(lots_df):,}")
    print(f"  Columns: {list(lots_df.columns[:10])}")

    # 2. Load existing VINs
    existing_vins = load_existing_vins()

    # 3. Look up final prices via AutoBidCar
    lots_df = enrich_with_prices(lots_df, existing_vins)

    # 4. Normalize to cars.csv schema
    print(f"\n  Normalizing columns...")
    norm_df = normalize_to_cars_csv(lots_df)
    print(f"  Normalized records: {len(norm_df):,}")

    # 5. Write output
    write_csvs(norm_df)

    elapsed = int(time.time() - t0)
    print(f"\n  Total time: {elapsed}s ({elapsed//60}m {elapsed%60}s)")
    print("=" * 60)
    print("\nDone. Commit cars.csv and master.csv to GitHub.")


if __name__ == "__main__":
    main()
