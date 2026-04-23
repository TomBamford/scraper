"""
Copart Historical Sales Harvester
=================================
Optimized version with:

- Rebrowser daily Copart parquet ingestion
- LOT-first price lookup
- VIN fallback lookup
- Caches for lot/VIN URLs and prices
- Higher safe caps
- Concurrent detail-page scraping

INSTALL
-------
pip install requests pandas pyarrow beautifulsoup4

RUN
---
python harvest_history.py
"""

import io
import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DAYS_TO_FETCH    = 30
STATE_FILTER     = "CA"        # "" = all US
MIN_YEAR         = 2015
MIN_PRICE        = 500

OUTPUT_CSV       = "cars.csv"
MASTER_CSV       = "master.csv"

RESUME_FROM_MASTER = True

# Maxed safe performance settings
API_DELAY        = 0.6         # keep AutoBidCar around 100/min
LOT_DELAY        = 0.2
SCRAPE_TIMEOUT   = 20
SCRAPE_WORKERS   = 10          # 12 is possible, 10 is safer

MAX_LOT_LOOKUPS  = 3000
MAX_VIN_LOOKUPS  = 1200

VIN_URL_CACHE_FILE   = "vin_url_cache.json"
PRICE_CACHE_FILE     = "price_cache.json"
LOT_URL_CACHE_FILE   = "lot_url_cache.json"
LOT_PRICE_CACHE_FILE = "lot_price_cache.json"

REBROWSER_BASE = (
    "https://raw.githubusercontent.com/rebrowser/copart-dataset"
    "/main/auction-listings/data/"
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

PRICE_KEYS = {
    "finalBid", "final_bid", "soldPrice", "sold_price", "lastBid",
    "last_bid", "highBid", "high_bid", "price", "bid", "auctionPrice",
    "auction_price", "winningBid", "winning_bid", "hammerPrice",
    "hammer_price", "currentBid", "current_bid"
}


# ─────────────────────────────────────────────
# HTTP / CACHE HELPERS
# ─────────────────────────────────────────────
def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/json,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def load_json_cache(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_json_cache(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def maybe_sleep(base: float):
    time.sleep(base + random.uniform(0, 0.15))


# ─────────────────────────────────────────────
# STEP 1 — DOWNLOAD REBROWSER PARQUET FILES
# ─────────────────────────────────────────────
def download_rebrowser_lots(days: int, state_filter: str, min_year: int) -> pd.DataFrame:
    session = build_session()
    all_frames = []
    today = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  STEP 1: Downloading Rebrowser Copart data ({days} days)")
    print(f"{'='*60}")

    for i in range(days):
        date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        url = REBROWSER_BASE + date + ".parquet"

        try:
            r = session.get(url, timeout=25)
            if r.status_code == 404:
                continue
            if r.status_code != 200:
                print(f"  [{date}] HTTP {r.status_code} — skipping")
                continue

            df = pd.read_parquet(io.BytesIO(r.content))

            if state_filter:
                df = df[df.get("locationState", pd.Series(dtype=str)) == state_filter]

            if min_year:
                df = df[pd.to_numeric(df.get("year", pd.Series(dtype=float)), errors="coerce") >= min_year]

            rows = len(df)
            print(f"  [{date}] {rows} rows after filter", end="")
            if rows > 0:
                all_frames.append(df)
                print(" ✓")
            else:
                print()

        except Exception as e:
            print(f"  [{date}] Error: {e}")

        time.sleep(0.2)

    if not all_frames:
        print("  No data downloaded.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\n  Total rows downloaded: {len(combined):,}")
    return combined


# ─────────────────────────────────────────────
# STEP 2 — LOAD EXISTING KEYS
# ─────────────────────────────────────────────
def load_existing_keys() -> tuple[set, set]:
    existing_vins = set()
    existing_lots = set()

    if not RESUME_FROM_MASTER:
        return existing_vins, existing_lots

    for fname in [MASTER_CSV, OUTPUT_CSV]:
        if os.path.exists(fname):
            try:
                df = pd.read_csv(fname, dtype=str).fillna("")
                if "vin" in df.columns:
                    existing_vins.update(df["vin"].str.strip().str.upper().tolist())
                if "lot" in df.columns:
                    existing_lots.update(df["lot"].str.strip().tolist())
            except Exception:
                pass

    existing_vins.discard("")
    existing_lots.discard("")
    print(f"\n  Existing VINs : {len(existing_vins):,}")
    print(f"  Existing Lots : {len(existing_lots):,}")
    return existing_vins, existing_lots


# ─────────────────────────────────────────────
# PRICE EXTRACTION HELPERS
# ─────────────────────────────────────────────
def _coerce_price(value):
    if value is None:
        return None
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s or not re.fullmatch(r"\d+(?:\.\d+)?", s):
        return None
    price = int(float(s))
    if 200 <= price <= 200000:
        return price
    return None


def _walk_for_price(obj):
    best = None

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in PRICE_KEYS:
                p = _coerce_price(v)
                if p:
                    return p
            nested = _walk_for_price(v)
            if nested and (best is None or nested > best):
                best = nested

    elif isinstance(obj, list):
        for item in obj:
            nested = _walk_for_price(item)
            if nested and (best is None or nested > best):
                best = nested

    return best


def _extract_price_from_json_scripts(soup: BeautifulSoup):
    for script in soup.find_all("script"):
        txt = script.string or script.get_text() or ""
        txt = txt.strip()
        if not txt:
            continue

        if script.get("id") == "__NEXT_DATA__":
            try:
                data = json.loads(txt)
                p = _walk_for_price(data)
                if p:
                    return p
            except Exception:
                pass

        if script.get("type") in {"application/json", "application/ld+json"}:
            try:
                data = json.loads(txt)
                p = _walk_for_price(data)
                if p:
                    return p
            except Exception:
                pass

        for key in PRICE_KEYS:
            m = re.search(rf'"{re.escape(key)}"\s*:\s*"?(?P<val>\d[\d,\.]*)"?', txt, re.IGNORECASE)
            if m:
                p = _coerce_price(m.group("val"))
                if p:
                    return p

            m = re.search(rf"{re.escape(key)}\s*:\s*\"?(?P<val>\d[\d,\.]*)\"?", txt, re.IGNORECASE)
            if m:
                p = _coerce_price(m.group("val"))
                if p:
                    return p

    return None


def _extract_price_from_html_text(text: str):
    patterns = [
        r'(?:final\s*bid|sold\s*(?:for|price)|winning\s*bid|hammer\s*price|last\s*bid)[^\d]{0,30}\$?\s*(\d[\d,]+)',
        r'(?:copart|iaai)[^\d]{0,30}(?:sold|bid|price)[^\d]{0,30}\$?\s*(\d[\d,]+)',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for m in matches:
            p = _coerce_price(m)
            if p:
                return p
    return None


def _extract_price_from_price_elements(soup: BeautifulSoup):
    for el in soup.find_all(attrs={"class": re.compile(r'price|bid|sold', re.I)}):
        t = el.get_text(" ", strip=True)
        if not t:
            continue
        m = re.search(r'(\d[\d,]+)', t)
        if m:
            p = _coerce_price(m.group(1))
            if p:
                return p
    return None


def scrape_final_price_from_url(page_url: str, session: requests.Session, price_cache: dict) -> int | None:
    if page_url in price_cache:
        val = price_cache[page_url]
        return int(val) if str(val).isdigit() else None

    try:
        r = session.get(page_url, timeout=SCRAPE_TIMEOUT)
        if r.status_code != 200:
            price_cache[page_url] = ""
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        p = _extract_price_from_json_scripts(soup)
        if p:
            price_cache[page_url] = p
            return p

        text = soup.get_text(" ", strip=True)
        p = _extract_price_from_html_text(text)
        if p:
            price_cache[page_url] = p
            return p

        p = _extract_price_from_price_elements(soup)
        if p:
            price_cache[page_url] = p
            return p

    except Exception as e:
        print(f"    Scrape error {page_url}: {e}")

    price_cache[page_url] = ""
    return None


# ─────────────────────────────────────────────
# LOT LOOKUP
# ─────────────────────────────────────────────
def get_carsbidshistory_lot_search_url(lot: str) -> str:
    return f"https://carsbidshistory.com/findbylot?lot={lot}"


def get_autobidcar_lot_search_url(lot: str) -> str:
    return f"https://autobidcar.com/en/search-result?search={lot}"


def _find_candidate_detail_url_from_search_page(search_url: str, session: requests.Session) -> str | None:
    try:
        r = session.get(search_url, timeout=SCRAPE_TIMEOUT)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue

            if href.startswith("/"):
                if "carsbidshistory.com" in search_url:
                    href = "https://carsbidshistory.com" + href
                elif "autobidcar.com" in search_url:
                    href = "https://autobidcar.com" + href

            if "carsbidshistory.com" in href and re.search(r"/make/\d+-[^/]+/\d+-[^/]+/(19|20)\d{2}_", href):
                return href

            if "autobidcar.com" in href and (re.search(r"/car/", href) or re.search(r"/details/", href)):
                return href

    except Exception:
        return None

    return None


def scrape_price_by_lot(lot: str, session: requests.Session, lot_url_cache: dict, lot_price_cache: dict) -> tuple[int | None, str | None]:
    lot = str(lot).strip()
    if not lot:
        return None, None

    if lot in lot_price_cache:
        val = lot_price_cache[lot]
        url = lot_url_cache.get(lot) or None
        return (int(val) if str(val).isdigit() else None), url

    page_url = lot_url_cache.get(lot, "")

    if not page_url:
        search_url = get_carsbidshistory_lot_search_url(lot)
        page_url = _find_candidate_detail_url_from_search_page(search_url, session)

        if not page_url:
            search_url = get_autobidcar_lot_search_url(lot)
            page_url = _find_candidate_detail_url_from_search_page(search_url, session)

        lot_url_cache[lot] = page_url or ""

    if not page_url:
        lot_price_cache[lot] = ""
        return None, None

    price = scrape_final_price_from_url(page_url, session, lot_price_cache)
    if price:
        lot_price_cache[lot] = price
        return price, page_url

    lot_price_cache[lot] = ""
    return None, page_url


def _lookup_price_for_lot(lot: str, page_url: str, price_cache: dict) -> tuple[str, int | None]:
    session = build_session()
    try:
        price = scrape_final_price_from_url(page_url, session, price_cache)
        return lot, price
    finally:
        session.close()


# ─────────────────────────────────────────────
# VIN LOOKUP
# ─────────────────────────────────────────────
def get_autobidcar_url(vin: str, session: requests.Session, vin_url_cache: dict) -> str | None:
    vin = vin.upper().strip()

    if vin in vin_url_cache:
        cached = vin_url_cache[vin]
        return cached or None

    try:
        r = session.get(f"https://autobidcar.com/api/check/{vin}", timeout=15)

        if r.status_code == 200:
            data = r.json() if "application/json" in r.headers.get("content-type", "") else {}
            url = data.get("full_url") or data.get("url") or data.get("car_url")
            vin_url_cache[vin] = url or ""
            return url or None

        if r.status_code == 404:
            vin_url_cache[vin] = ""
            return None

        print(f"    AutoBidCar API {r.status_code} for {vin}")
        vin_url_cache[vin] = ""
        return None

    except Exception as e:
        print(f"    AutoBidCar error for {vin}: {e}")
        return None


def _lookup_price_for_vin(vin: str, car_url: str, price_cache: dict) -> tuple[str, int | None]:
    session = build_session()
    try:
        price = scrape_final_price_from_url(car_url, session, price_cache)
        return vin, price
    finally:
        session.close()


# ─────────────────────────────────────────────
# ENRICH WITH PRICES
# ─────────────────────────────────────────────
def enrich_with_prices(df: pd.DataFrame, existing_vins: set, existing_lots: set) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print("  STEP 3: Price lookup by LOT first, VIN second")
    print(f"{'='*60}")

    vin_url_cache = load_json_cache(VIN_URL_CACHE_FILE)
    price_cache = load_json_cache(PRICE_CACHE_FILE)
    lot_url_cache = load_json_cache(LOT_URL_CACHE_FILE)
    lot_price_cache = load_json_cache(LOT_PRICE_CACHE_FILE)

    session = build_session()

    try:
        session.get("https://autobidcar.com/", timeout=10)
        time.sleep(1.0)
    except Exception:
        pass

    df = df.copy()
    df["final_price"] = None
    df["price_source"] = ""
    df["matched_url"] = None

    vin_col = None
    for c in ["vin", "VIN", "vehicleId"]:
        if c in df.columns:
            vin_col = c
            break

    lot_col = None
    for c in ["lotId", "lot", "lot_id"]:
        if c in df.columns:
            lot_col = c
            break

    vin_series = df[vin_col].astype(str).str.strip().str.upper() if vin_col else pd.Series("", index=df.index)
    lot_series = df[lot_col].astype(str).str.strip() if lot_col else pd.Series("", index=df.index)

    # Phase 1: lot lookup candidates
    lot_candidates = []
    if lot_col:
        lot_candidates = (
            lot_series[
                lot_series.notna() &
                (lot_series != "") &
               existing_with_price = set()

if os.path.exists(MASTER_CSV):
    df_old = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
    df_old["price"] = pd.to_numeric(df_old["price"], errors="coerce").fillna(0)
    existing_with_price = set(df_old[df_old["price"] > 0]["lot"].astype(str))

lot_candidates = (
    lot_series[
        lot_series.notna() &
        (lot_series != "") &
        (~lot_series.isin(existing_with_price))
    ]
    .drop_duplicates()
    .tolist()
)
            ]
            .drop_duplicates()
            .tolist()
        )[:MAX_LOT_LOOKUPS]

    print(f"  Lots to look up: {len(lot_candidates):,} (capped at {MAX_LOT_LOOKUPS})")

    lot_to_url = {}
    lot_to_price = {}
    lot_price_pending = {}

    for idx, lot in enumerate(lot_candidates, start=1):
        price, url = scrape_price_by_lot(lot, session, lot_url_cache, lot_price_cache)

        if url:
            lot_to_url[lot] = url
        if price:
            lot_to_price[lot] = price
        elif url:
            lot_price_pending[lot] = url

        if idx % 100 == 0 or idx == len(lot_candidates):
            print(f"  Lot lookup progress: {idx}/{len(lot_candidates)} — prices found: {len(lot_to_price)}")

        maybe_sleep(LOT_DELAY)

    # Concurrent second-pass detail scrapes for lot URLs that resolved but didn't price immediately
    if lot_price_pending:
        with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as ex:
            futures = [
                ex.submit(_lookup_price_for_lot, lot, url, lot_price_cache)
                for lot, url in lot_price_pending.items()
            ]
            done = 0
            for fut in as_completed(futures):
                lot, price = fut.result()
                done += 1
                if price:
                    lot_to_price[lot] = price
                if done % 100 == 0 or done == len(futures):
                    print(f"  Lot detail scrape progress: {done}/{len(futures)} — prices found: {len(lot_to_price)}")

    print(f"\n  Lot-based prices found: {len(lot_to_price)} / {len(lot_candidates)}")

    # Phase 2: VIN fallback candidates
    vin_candidates = []
    if vin_col:
        vin_candidates = (
            vin_series[
                vin_series.notna() &
                (vin_series != "") &
                (vin_series != "[PREMIUM]") &
                (vin_series.str.len() == 17) &
                (~vin_series.isin(existing_vins))
            ]
            .drop_duplicates()
            .tolist()
        )[:MAX_VIN_LOOKUPS]

    print(f"  VINs to look up: {len(vin_candidates):,} (capped at {MAX_VIN_LOOKUPS})")

    vin_to_url = {}
    for idx, vin in enumerate(vin_candidates, start=1):
        url = get_autobidcar_url(vin, session, vin_url_cache)
        if url:
            vin_to_url[vin] = url

        if idx % 100 == 0 or idx == len(vin_candidates):
            print(f"  VIN lookup progress: {idx}/{len(vin_candidates)} — URLs found: {len(vin_to_url)}")

        maybe_sleep(API_DELAY)

    vin_to_price = {}
    if vin_to_url:
        with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as ex:
            futures = [
                ex.submit(_lookup_price_for_vin, vin, url, price_cache)
                for vin, url in vin_to_url.items()
            ]
            done = 0
            for fut in as_completed(futures):
                vin, price = fut.result()
                done += 1
                if price:
                    vin_to_price[vin] = price
                if done % 100 == 0 or done == len(futures):
                    print(f"  VIN detail scrape progress: {done}/{len(futures)} — prices found: {len(vin_to_price)}")

    print(f"\n  VIN-based prices found: {len(vin_to_price)} / {len(vin_to_url)}")

    # Map back, lot first
    df["final_price"] = lot_series.map(lot_to_price)
    df["price_source"] = df["final_price"].notna().map(lambda x: "lot" if x else "")
    df["matched_url"] = lot_series.map(lot_to_url)

    vin_prices = vin_series.map(vin_to_price)
    vin_urls = vin_series.map(vin_to_url)
    needs_vin = df["final_price"].isna() & vin_prices.notna()

    df.loc[needs_vin, "final_price"] = vin_prices[needs_vin]
    df.loc[needs_vin, "price_source"] = "vin"
    df.loc[needs_vin, "matched_url"] = vin_urls[needs_vin]

    save_json_cache(VIN_URL_CACHE_FILE, vin_url_cache)
    save_json_cache(PRICE_CACHE_FILE, price_cache)
    save_json_cache(LOT_URL_CACHE_FILE, lot_url_cache)
    save_json_cache(LOT_PRICE_CACHE_FILE, lot_price_cache)

    return df


# ─────────────────────────────────────────────
# NORMALIZE + WRITE OUTPUT
# ─────────────────────────────────────────────
def normalize_date(raw: str) -> str:
    if not raw or raw in ("NaT", "None", "nan", ""):
        return ""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    return ""


def normalize_to_cars_csv(df: pd.DataFrame) -> pd.DataFrame:
    def col(df, *names):
        for n in names:
            if n in df.columns:
                return df[n]
        return pd.Series("", index=df.index)

    out = pd.DataFrame()

    out["year"]     = pd.to_numeric(col(df, "year"), errors="coerce").fillna(0).astype(int)
    out["make"]     = col(df, "make").astype(str).str.strip().str.title()
    out["model"]    = col(df, "modelGroup", "modelDetail", "model").astype(str).str.strip().str.title()
    out["trim"]     = col(df, "trim").astype(str).str.strip()
    out["type"]     = col(df, "saleTitleType").astype(str).str.strip()
    out["damage"]   = col(df, "damageDescription").astype(str).str.strip().str.title()
    out["odometer"] = pd.to_numeric(col(df, "mileage"), errors="coerce").fillna(0).astype(int)
    out["lot"]      = col(df, "lotId", "lot").astype(str).str.strip()
    out["vin"]      = col(df, "vin").astype(str).str.strip().str.upper()
    out["location"] = (
        col(df, "yardName").astype(str).str.strip().where(
            col(df, "yardName").astype(str).str.strip() != "",
            col(df, "locationCity").astype(str).str.strip() + ", " + col(df, "locationState").astype(str).str.strip()
        )
    )
    out["state"]    = col(df, "locationState").astype(str).str.strip().str.upper()
    out["repair_cost"] = pd.to_numeric(col(df, "repairCost"), errors="coerce").fillna(0).astype(int)
    out["url"]      = col(df, "listingUrl").astype(str).str.strip()
    out["matched_url"] = col(df, "matched_url").astype(str).str.strip()
    out["price_source"] = col(df, "price_source").astype(str).str.strip()

    if "final_price" in df.columns:
        out["price"] = pd.to_numeric(df["final_price"], errors="coerce").fillna(0).astype(int)
    else:
        out["price"] = 0

    raw_date = col(df, "saleDate")
    out["date"] = raw_date.apply(lambda v: normalize_date(str(v)) if pd.notna(v) and v != "" else "")

    out = out[
        (out["year"] >= MIN_YEAR) &
        out["make"].notna() & (out["make"] != "") &
        out["model"].notna() & (out["model"] != "")
    ]

    return out


def write_csvs(new_df: pd.DataFrame):
    print(f"\n{'='*60}")
    print("  STEP 4: Writing output files")
    print(f"{'='*60}")

    cols_master = [
        "vin", "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state",
        "repair_cost", "url", "matched_url", "price_source"
    ]
    cols_site = [
        "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state", "url", "matched_url"
    ]

    if os.path.exists(MASTER_CSV):
        master = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
    else:
        master = pd.DataFrame(columns=cols_master)

    for c in cols_master:
        if c not in new_df.columns:
            new_df[c] = ""

    new_df = new_df[cols_master].copy()
    new_df["price"] = pd.to_numeric(new_df["price"], errors="coerce").fillna(0).astype(int)

    combined = pd.concat([master, new_df], ignore_index=True)

    if "lot" in combined.columns and combined["lot"].astype(str).str.strip().ne("").any():
        combined = combined.drop_duplicates(subset=["lot"], keep="first")
    else:
        combined = combined.drop_duplicates(subset=["vin"], keep="first")

    with_price = (pd.to_numeric(combined["price"], errors="coerce").fillna(0) > 0).sum()
    print(f"  Master records   : {len(combined):,}")
    print(f"  With final price : {with_price:,}")
    print(f"  Without price    : {len(combined) - with_price:,}")

    combined.to_csv(MASTER_CSV, index=False)
    print(f"  Written: {MASTER_CSV}")

    site = combined[pd.to_numeric(combined["price"], errors="coerce").fillna(0) > 0].copy()
    for c in cols_site:
        if c not in site.columns:
            site[c] = ""
    site[cols_site].to_csv(OUTPUT_CSV, index=False)
    print(f"  Written: {OUTPUT_CSV} ({len(site):,} priced records)")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    t0 = time.time()
    print("=" * 60)
    print("  Copart Historical Sales Harvester")
    print(f"  Date         : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  State        : {STATE_FILTER or 'All US'}")
    print(f"  Days         : last {DAYS_TO_FETCH} days")
    print(f"  Min year     : {MIN_YEAR}")
    print(f"  Min price    : {MIN_PRICE}")
    print(f"  Max lots     : {MAX_LOT_LOOKUPS}")
    print(f"  Max VINs     : {MAX_VIN_LOOKUPS}")
    print(f"  Workers      : {SCRAPE_WORKERS}")
    print("=" * 60)

    lots_df = download_rebrowser_lots(DAYS_TO_FETCH, STATE_FILTER, MIN_YEAR)
    if lots_df.empty:
        print("No lots downloaded. Exiting.")
        return

    print(f"\n  Lots after filter: {len(lots_df):,}")
    print(f"  Columns sample: {list(lots_df.columns[:12])}")

    existing_vins, existing_lots = load_existing_keys()

    lots_df = enrich_with_prices(lots_df, existing_vins, existing_lots)

    print(f"\n  Normalizing columns...")
    norm_df = normalize_to_cars_csv(lots_df)
    print(f"  Normalized records: {len(norm_df):,}")

    write_csvs(norm_df)

    elapsed = int(time.time() - t0)
    print(f"\n  Total time: {elapsed}s ({elapsed // 60}m {elapsed % 60}s)")
    print("=" * 60)
    print("\nDone. Commit cars.csv and master.csv to GitHub.")


if __name__ == "__main__":
    main()
