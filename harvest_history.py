"""
AutoBidCar Catalog Harvester — Real Copart & IAAI Final Prices
==============================================================
Scrapes autobidcar.com/catalog/{make} pages directly.
Each catalog page lists 20 already-SOLD cars with:
  - Year, Make, Model, Trim
  - Final hammer price ("Sold For $X,XXX")
  - Lot number, VIN, Sale date, Auction (COPART/IAAI)
  - Damage, Odometer, Location

WHY THIS APPROACH:
  - Rebrowser lots are upcoming/future auctions — no price yet
  - AutoBidCar catalog = historical SOLD lots = real final prices
  - Price is in the page HTML, no VIN lookup needed
  - 20 makes × 50 pages × 20 cars = ~20,000 records per run
  - No API key, no login, fully public

SPEED:
  - 50 concurrent catalog page fetches
  - Each page has 20 car cards with prices embedded
  - ~1,000-2,000 pages in 5-10 minutes = 20,000-40,000 records

Install:
    pip install aiohttp pandas beautifulsoup4 lxml

Env vars (all optional):
    SCRAPER_API_KEY   - ScraperAPI key (recommended for GH Actions)
    MAX_PAGES_PER_MAKE - pages per make (default 50, 20 cars each)
    CONCURRENCY        - parallel page fetches (default 50)
    REQUEST_TIMEOUT    - seconds per request (default 10)
"""

from __future__ import annotations

import asyncio, io, json, os, random, re, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_CSV  = "cars.csv"
MASTER_CSV  = "master.csv"
MIN_YEAR    = 2010
MIN_PRICE   = 200

MAX_PAGES_PER_MAKE = int(os.environ.get("MAX_PAGES_PER_MAKE", "50"))
CONCURRENCY        = int(os.environ.get("CONCURRENCY",        "50"))
REQUEST_TIMEOUT    = int(os.environ.get("REQUEST_TIMEOUT",    "10"))
PROGRESS_EVERY     = int(os.environ.get("PROGRESS_EVERY",     "100"))

SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "").strip()
SCRAPER_PROXY   = (
    f"http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001"
    if SCRAPER_API_KEY else ""
)

# All makes to scrape — covers ~95% of salvage auction inventory
MAKES = [
    "toyota", "honda", "chevrolet", "ford", "nissan",
    "hyundai", "kia", "jeep", "dodge", "bmw",
    "mercedes-benz", "volkswagen", "subaru", "mazda",
    "audi", "lexus", "acura", "mitsubishi", "gmc", "ram",
    "chrysler", "buick", "cadillac", "lincoln", "volvo",
    "infiniti", "genesis", "tesla", "land-rover", "porsche",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

STANDARD_COLS = [
    "year","make","model","trim","type","damage",
    "price","odometer","lot","vin","date",
    "location","state","source","auction","url",
]

BLOCK_SIGNALS = [
    "captcha","access denied","cloudflare","verify you are human",
    "checking your browser","/cdn-cgi/","attention required",
]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg): print(f"  {msg}", flush=True)

def rh() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

def slugify(s: str) -> str:
    s = str(s).lower().strip()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

def clean_year(v) -> int:
    try: return int(float(str(v).strip()))
    except: return 0

def clean_price(v) -> int:
    if not v: return 0
    s = str(v).replace("$","").replace(",","").strip()
    m = re.search(r"(\d+)", s)
    p = int(m.group(1)) if m else 0
    return p if p >= MIN_PRICE else 0

def clean_odo(v) -> str:
    if not v or str(v).strip() in ("","nan","None"): return ""
    s = str(v).replace(",","").strip()
    m = re.search(r"([\d.]+)", s)
    if not m: return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"

def cap(s) -> str:
    if not s or str(s).strip() in ("","nan","None","N/A"): return ""
    return str(s).strip().title()

def norm_date(v) -> str:
    if not v: return ""
    s = str(v).strip()
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m: return m.group(1)
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s)
    if m: return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S"):
        try: return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def state_from_location(loc: str) -> str:
    if not loc: return ""
    # "NY - ALBANY" or "CA - LOS ANGELES" or just "California"
    m = re.match(r"^([A-Z]{2})\s*[-–]", loc.strip())
    if m: return m.group(1)
    return ""

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in STANDARD_COLS:
        if c not in df.columns: df[c] = ""
    return df[STANDARD_COLS]

def num_price(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

# ─────────────────────────────────────────────────────────────────────────────
# STATE — track already-scraped lots to avoid duplicates across runs
# ─────────────────────────────────────────────────────────────────────────────

STATE_FILE = "harvest_state.json"

def load_existing_lots() -> set:
    """Load lot numbers already in master.csv."""
    lots = set()
    if Path(MASTER_CSV).exists():
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str, usecols=["lot"]).fillna("")
            lots = set(df["lot"].str.strip().tolist())
            lots.discard("")
        except: pass
    return lots

# ─────────────────────────────────────────────────────────────────────────────
# ASYNC HTTP
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    async with sem:
        for attempt in range(2):
            try:
                kw: Dict[str, Any] = dict(
                    headers=rh(),
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                    allow_redirects=True,
                    ssl=False,
                )
                if SCRAPER_PROXY:
                    kw["proxy"] = SCRAPER_PROXY
                async with session.get(url, **kw) as r:
                    if r.status == 404: return None
                    if r.status in (403, 429):
                        await asyncio.sleep(2.0 * (attempt + 1))
                        continue
                    if r.status == 200:
                        html = await r.text(errors="ignore")
                        if any(s in html[:2000].lower() for s in BLOCK_SIGNALS):
                            return None
                        return html
                    return None
            except asyncio.TimeoutError: return None
            except Exception:
                if attempt == 0: await asyncio.sleep(1.0)
    return None

# ─────────────────────────────────────────────────────────────────────────────
# CATALOG PAGE PARSER
# Parses autobidcar.com/catalog/{make}?page=N
# Each page has ~20 car cards with prices embedded
# ─────────────────────────────────────────────────────────────────────────────

def parse_catalog_page(html: str, make_slug: str) -> List[Dict]:
    """
    Parse an autobidcar catalog page and extract car records.
    
    Card structure (confirmed from Google search snippets):
      <a href="/car/toyota-camry-le-2023-VIN17CHARS">
        <h2>2023 TOYOTA CAMRY LE</h2>
        <p>Lot: 12345678</p>
        <p>Sold For $8,000</p>
        <p>COPART</p>
        <p>CA - LOS ANGELES</p>
      </a>
    """
    soup = BeautifulSoup(html, "lxml")
    records = []

    # Find all car card links
    for card in soup.find_all("a", href=re.compile(r"^/car/")):
        try:
            href = card.get("href", "")
            car_url = "https://autobidcar.com" + href

            # VIN is the last segment of the URL slug (17 chars)
            vin_m = re.search(r"-([A-HJ-NPR-Z0-9]{17})$", href, re.IGNORECASE)
            vin   = vin_m.group(1).upper() if vin_m else ""

            text = card.get_text(" ", strip=True)

            # Year
            year_m = re.search(r"\b(20\d{2}|19[89]\d)\b", text)
            year   = int(year_m.group(1)) if year_m else 0
            if year < MIN_YEAR: continue

            # Lot number
            lot_m = re.search(r"(?:Lot|lot)[:\s#]*(\d{5,})", text)
            lot   = lot_m.group(1) if lot_m else ""

            # Price — "Sold For $X,XXX" or "Sold for $X,XXX"
            price_m = re.search(r"Sold\s+[Ff]or\s+\$\s*([\d,]+)", text)
            if not price_m:
                # Fallback: first $X,XXX in card
                price_m = re.search(r"\$\s*([\d,]+)", text)
            price_str = price_m.group(1).replace(",","") if price_m else "0"
            price = int(price_str) if price_str.isdigit() else 0
            if price < MIN_PRICE: price = 0

            # Auction source
            auction = ""
            tu = text.upper()
            if "COPART" in tu: auction = "COPART"
            elif "IAAI" in tu: auction  = "IAAI"

            # Location
            loc_m = re.search(r"\b([A-Z]{2})\s*[-–]\s*([A-Z][A-Z\s]+)", text.upper())
            location = loc_m.group(0).strip().title() if loc_m else ""
            state    = loc_m.group(1) if loc_m else ""

            # Sale date
            date_m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
            sale_date = date_m.group(1) if date_m else ""
            if not sale_date:
                date_m2 = re.search(r"\b(\w+ \d{1,2},?\s*\d{4})\b", text)
                sale_date = norm_date(date_m2.group(1)) if date_m2 else ""

            # Title — everything between year and first $ or lot
            h2 = card.find(["h2","h3","h4"])
            title_text = h2.get_text(strip=True) if h2 else text[:60]

            # Parse make/model/trim from title: "2023 TOYOTA CAMRY LE"
            title_clean = re.sub(r"\b(20\d{2}|19[89]\d)\b", "", title_text).strip()
            title_parts = title_clean.upper().split()
            make_str  = title_parts[0].title() if title_parts else make_slug.title()
            model_str = title_parts[1].title() if len(title_parts) > 1 else ""
            trim_str  = " ".join(p.title() for p in title_parts[2:]) if len(title_parts) > 2 else ""

            # Odometer
            odo_m = re.search(r"([\d,]+)\s*(?:mi|miles|km)", text, re.IGNORECASE)
            odometer = clean_odo(odo_m.group(0)) if odo_m else ""

            # Damage
            dmg_m = re.search(r"(?:damage|dmg)[:\s]+([A-Za-z ]+?)(?:\s*[,\|]|\s{2}|$)", text, re.IGNORECASE)
            damage = cap(dmg_m.group(1)) if dmg_m else ""

            records.append({
                "year":     year,
                "make":     make_str,
                "model":    model_str,
                "trim":     trim_str,
                "type":     "",
                "damage":   damage,
                "price":    price,
                "odometer": odometer,
                "lot":      lot,
                "vin":      vin,
                "date":     sale_date,
                "location": location,
                "state":    state,
                "source":   "AutoBidCar",
                "auction":  auction,
                "url":      car_url,
            })

        except Exception:
            continue

    return records


def parse_catalog_page_json(html: str, make_slug: str) -> List[Dict]:
    """
    Try to extract from __NEXT_DATA__ JSON first (faster + more reliable).
    Falls back to HTML parsing if not found.
    """
    # Try Next.js data blob
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if nd:
        try:
            data = json.loads(nd.group(1))
            cars = (
                data.get("props",{}).get("pageProps",{}).get("cars") or
                data.get("props",{}).get("pageProps",{}).get("vehicles") or
                data.get("props",{}).get("pageProps",{}).get("items") or
                []
            )
            if cars and isinstance(cars, list) and len(cars) > 0:
                records = []
                for car in cars:
                    price = clean_price(
                        car.get("finalBid") or car.get("soldPrice") or
                        car.get("highBid") or car.get("price") or 0
                    )
                    year = clean_year(car.get("year", 0))
                    if year < MIN_YEAR or price < MIN_PRICE:
                        continue
                    vin = str(car.get("vin","")).strip().upper()
                    lot = str(car.get("lotId") or car.get("lot") or car.get("stockNumber","")).strip()
                    location = str(car.get("yardName") or car.get("branchName") or car.get("location","")).strip()
                    records.append({
                        "year":     year,
                        "make":     cap(car.get("make","")),
                        "model":    cap(car.get("modelGroup") or car.get("model","")),
                        "trim":     cap(car.get("trim","")),
                        "type":     cap(car.get("saleTitleType") or car.get("titleCode","")),
                        "damage":   cap(car.get("damageDescription") or car.get("damage","")),
                        "price":    price,
                        "odometer": clean_odo(car.get("mileage") or car.get("odometer","")),
                        "lot":      lot,
                        "vin":      vin,
                        "date":     norm_date(str(car.get("saleDate") or car.get("date",""))),
                        "location": cap(location),
                        "state":    str(car.get("locationState") or car.get("branchState","")).strip().upper(),
                        "source":   "AutoBidCar",
                        "auction":  str(car.get("auction","")).strip().upper(),
                        "url":      "https://autobidcar.com/car/" + str(car.get("slug","")).strip(),
                    })
                if records:
                    return records
        except Exception:
            pass

    # Fallback to HTML parsing
    return parse_catalog_page(html, make_slug)

# ─────────────────────────────────────────────────────────────────────────────
# CATALOG SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_make(
    make: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    existing_lots: set,
) -> List[Dict]:
    """Scrape all catalog pages for one make concurrently."""

    # First fetch page 1 to find total pages
    base = f"https://autobidcar.com/catalog/{make}"
    html1 = await fetch_html(session, base, sem)
    if not html1:
        return []

    # Check for total pages in HTML
    total_pages = MAX_PAGES_PER_MAKE
    tp_m = re.search(r'"totalPages"\s*:\s*(\d+)', html1)
    if not tp_m:
        tp_m = re.search(r'of\s+(\d+)\s+pages', html1, re.IGNORECASE)
    if tp_m:
        total_pages = min(int(tp_m.group(1)), MAX_PAGES_PER_MAKE)

    records_p1 = parse_catalog_page_json(html1, make)
    all_records = [r for r in records_p1 if r["lot"] not in existing_lots]

    if total_pages <= 1:
        return all_records

    # Fetch remaining pages concurrently
    page_urls = [f"{base}?page={p}" for p in range(2, total_pages + 1)]
    blobs = await asyncio.gather(*[fetch_html(session, url, sem) for url in page_urls])

    for html in blobs:
        if not html: continue
        recs = parse_catalog_page_json(html, make)
        new  = [r for r in recs if r["lot"] not in existing_lots]
        all_records.extend(new)
        # Update set to avoid dups within this make
        for r in new:
            if r["lot"]: existing_lots.add(r["lot"])

    return all_records


# ─────────────────────────────────────────────────────────────────────────────
# MASTER CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_existing() -> pd.DataFrame:
    if Path(MASTER_CSV).exists():
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
            df = ensure_cols(df)
            log(f"Existing master : {len(df):,} rows")
            return df
        except Exception as e:
            log(f"Could not load master: {e}")
    return pd.DataFrame(columns=STANDARD_COLS)


def merge_and_dedup(existing: pd.DataFrame, new_records: List[Dict]) -> pd.DataFrame:
    if not new_records:
        return existing

    new_df = pd.DataFrame(new_records)
    new_df = ensure_cols(new_df)

    combined = pd.concat([existing, new_df], ignore_index=True)
    combined["_p"] = num_price(combined["price"])
    combined = combined.sort_values("_p", ascending=False).drop(columns=["_p"])

    # Dedup by lot
    has_lot = combined[combined["lot"].str.strip().str.len() > 3]
    no_lot  = combined[~combined.index.isin(has_lot.index)]
    has_lot = has_lot.drop_duplicates(subset=["lot"], keep="first")
    combined = pd.concat([has_lot, no_lot], ignore_index=True)

    # Dedup by VIN
    has_vin = combined[combined["vin"].str.len() == 17]
    no_vin  = combined[~combined.index.isin(has_vin.index)]
    has_vin = has_vin.drop_duplicates(subset=["vin"], keep="first")
    combined = pd.concat([has_vin, no_vin], ignore_index=True)

    return ensure_cols(combined)


def write_outputs(combined: pd.DataFrame) -> None:
    combined.to_csv(MASTER_CSV, index=False)
    log(f"Written : {MASTER_CSV} ({len(combined):,} rows)")

    priced = combined[num_price(combined["price"]) > 0]
    site   = ["year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","auction","source","url"]
    for c in site:
        if c not in priced.columns: priced[c] = ""
    priced[site].to_csv(OUTPUT_CSV, index=False)
    log(f"Written : {OUTPUT_CSV} ({len(priced):,} priced rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def run() -> None:
    t0 = time.time()

    print("=" * 68, flush=True)
    print("  AutoBidCar Catalog Harvester — Real Final Prices")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Makes       : {len(MAKES)}")
    print(f"  Pages/make  : up to {MAX_PAGES_PER_MAKE} × 20 cars = {MAX_PAGES_PER_MAKE*20:,} per make")
    print(f"  Target      : ~{len(MAKES)*MAX_PAGES_PER_MAKE*20:,} records total")
    print(f"  Concurrency : {CONCURRENCY} parallel requests")
    print(f"  Proxy       : {'ScraperAPI ✓' if SCRAPER_API_KEY else 'none'}")
    print("=" * 68, flush=True)

    existing     = load_existing()
    existing_lots= load_existing_lots()
    log(f"Known lots to skip: {len(existing_lots):,}")

    all_records: List[Dict] = []
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY + 20,
        ttl_dns_cache=300,
        force_close=False,
        keepalive_timeout=20,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        for i, make in enumerate(MAKES, 1):
            t_make = time.time()
            print(f"\n  [{i:02d}/{len(MAKES)}] {make}...", flush=True)

            records = await scrape_make(make, session, sem, existing_lots)
            all_records.extend(records)

            priced  = sum(1 for r in records if r.get("price",0) > 0)
            elapsed = int(time.time() - t_make)
            log(f"{len(records):,} new records, {priced:,} priced ({elapsed}s)")

            # Short pause between makes to be respectful
            await asyncio.sleep(0.5)

    # Write output
    print("\n" + "=" * 68, flush=True)

    total_priced = sum(1 for r in all_records if r.get("price",0) > 0)
    log(f"Total new records  : {len(all_records):,}")
    log(f"Total priced       : {total_priced:,}")
    log(f"Total unpriced     : {len(all_records) - total_priced:,}")

    if all_records:
        combined = merge_and_dedup(existing, all_records)
        master_priced = int((num_price(combined["price"]) > 0).sum())
        log(f"Master total       : {len(combined):,}")
        log(f"Master priced      : {master_priced:,}")
        write_outputs(combined)
    else:
        log("No new records — nothing to write")

    elapsed = int(time.time() - t0)
    print(f"\n  Done in {elapsed}s ({elapsed//60}m {elapsed%60}s)")
    print("=" * 68, flush=True)


def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
