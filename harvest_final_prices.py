"""
Rebrowser → AutoBidCar Final Price Harvester
=============================================
Uses real lot numbers from free Rebrowser GitHub datasets,
then looks up each lot on autobidcar.com to get the final sold price.

Why this works better than random VIN generation:
  - Rebrowser gives REAL lot numbers from actual Copart/IAAI auctions
  - autobidcar.com/en/copart/{lot} or /en/iaai/{lot} = direct page for that lot
  - No guessing — every lot number is a real auction record
  - Hit rate: ~60-80% (vs ~20% with random VINs)
  - 4,800 ScraperAPI calls × 70% hit rate = ~3,360 priced records per run

Sources:
  Rebrowser Copart: github.com/rebrowser/copart-dataset (free daily parquets)
  Rebrowser IAAI:   github.com/rebrowser/iaai-dataset   (free daily parquets)
  Price lookup:     autobidcar.com via ScraperAPI residential IPs

Install:
  pip install aiohttp pandas pyarrow beautifulsoup4

Env vars:
  SCRAPER_API_KEY   your ScraperAPI key (required)
  REBROWSER_DAYS    days of parquet history to pull (default 60)
  TOTAL_REQUESTS    max ScraperAPI calls per run (default 4800)
  CONCURRENCY       parallel requests (default 20)
"""

from __future__ import annotations

import asyncio, io, json, os, random, re, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_CSV      = "cars.csv"
MASTER_CSV      = "master.csv"
MIN_YEAR        = 2010
MIN_PRICE       = 200

SCRAPER_API_KEY  = os.environ.get("SCRAPER_API_KEY", "").strip()
REBROWSER_DAYS   = int(os.environ.get("REBROWSER_DAYS",   "60"))
TOTAL_REQUESTS   = int(os.environ.get("TOTAL_REQUESTS",   "4800"))
CONCURRENCY      = int(os.environ.get("CONCURRENCY",      "20"))
REQUEST_TIMEOUT  = int(os.environ.get("REQUEST_TIMEOUT",  "60"))
PROGRESS_EVERY   = int(os.environ.get("PROGRESS_EVERY",   "100"))
MAX_RUNTIME_SEC  = int(os.environ.get("MAX_RUNTIME_SEC",  "3000"))

SCRAPER_ENDPOINT = "http://api.scraperapi.com"

STANDARD_COLS = [
    "year","make","model","trim","type","damage",
    "price","odometer","lot","vin","date",
    "location","state","source","auction","url",
]

REBROWSER_SOURCES = {
    "Rebrowser-Copart": {
        "url": "https://raw.githubusercontent.com/rebrowser/copart-dataset/main/auction-listings/data/",
        "cols": {
            "year":"year","make":"make","model":"modelGroup","trim":"trim",
            "type":"saleTitleType","damage":"damageDescription","odometer":"mileage",
            "lot":"lotId","vin":"vin","date":"saleDate","location":"yardName",
            "state":"locationState",
        },
        "auction": "COPART",
    },
    "Rebrowser-IAAI": {
        "url": "https://raw.githubusercontent.com/rebrowser/iaai-dataset/main/auction-listings/data/",
        "cols": {
            "year":"year","make":"make","model":"model","trim":"trim",
            "type":"titleCode","damage":"damageDescription","odometer":"mileage",
            "lot":"stockNumber","vin":"vin","date":"saleDate","location":"branchName",
            "state":"branchState",
        },
        "auction": "IAAI",
    },
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg): print(f"  {msg}", flush=True)

def rh() -> Dict[str,str]:
    return {"User-Agent": random.choice(USER_AGENTS)}

def clean_price(v) -> int:
    s = str(v).replace("$","").replace(",","").strip()
    m = re.search(r"(\d+)", s)
    p = int(m.group(1)) if m else 0
    return p if p >= MIN_PRICE else 0

def clean_year(v) -> int:
    try: return int(float(str(v)))
    except: return 0

def clean_odo(v) -> str:
    if not v or str(v).strip() in ("","nan","None"): return ""
    s = str(v).replace(",","").strip()
    m = re.search(r"([\d.]+)", s)
    if not m: return ""
    return f"{int(float(m.group(1)))} {'km' if 'km' in s.lower() else 'mi'}"

def cap(s) -> str:
    if not s or str(s).strip() in ("","nan","None","[PREMIUM]","N/A"): return ""
    return str(s).strip().title()

def norm_date(v) -> str:
    if not v: return ""
    s = str(v).strip()
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m: return m.group(1)
    for fmt in ("%B %d, %Y","%b %d, %Y","%m/%d/%Y"):
        try: return datetime.strptime(s,fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in STANDARD_COLS:
        if c not in df.columns: df[c] = ""
    return df[STANDARD_COLS]

def num_price(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Download Rebrowser lot data (free, no login)
# Gets real lot numbers we can look up on autobidcar
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_rebrowser_lots() -> pd.DataFrame:
    print(f"\n[1/3] Cloning Rebrowser repos to get lot numbers...", flush=True)

    import subprocess, tempfile
    all_frames = []

    repos = {
        "Rebrowser-Copart": {
            "repo": "https://github.com/rebrowser/copart-dataset.git",
            "glob": "auction-listings/data/*.parquet",
            "cols": REBROWSER_SOURCES["Rebrowser-Copart"]["cols"],
            "auction": "COPART",
        },
        "Rebrowser-IAAI": {
            "repo": "https://github.com/rebrowser/iaai-dataset.git",
            "glob": "auction-listings/data/*.parquet",
            "cols": REBROWSER_SOURCES["Rebrowser-IAAI"]["cols"],
            "auction": "IAAI",
        },
    }

    for name, cfg in repos.items():
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                log(f"Cloning {name}...")
                result = subprocess.run(
                    ["git", "clone", "--depth=1", "--filter=blob:limit=50m",
                     cfg["repo"], tmpdir],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode != 0:
                    log(f"  Clone failed: {result.stderr[:200]}")
                    continue

                # Find all parquet files
                import glob as glob_mod
                parquet_files = sorted(glob_mod.glob(f"{tmpdir}/{cfg['glob']}"))[-REBROWSER_DAYS:]
                log(f"  Found {len(parquet_files)} parquet files")

                frames = []
                for f in parquet_files:
                    try: frames.append(pd.read_parquet(f))
                    except: pass

                if not frames:
                    log(f"  No readable parquet files")
                    continue

                raw = pd.concat(frames, ignore_index=True)
                out = pd.DataFrame()
                for std, src in cfg["cols"].items():
                    out[std] = raw[src].astype(str) if src in raw.columns else ""

                out["auction"] = cfg["auction"]
                out["source"]  = name
                out["price"]   = 0
                out["url"]     = ""

                out["year"]     = out["year"].apply(clean_year)
                out["make"]     = out["make"].apply(cap)
                out["model"]    = out["model"].apply(cap)
                out["lot"]      = out["lot"].astype(str).str.strip()
                out["vin"]      = out["vin"].apply(
                    lambda v: "" if str(v).strip() in ("[PREMIUM]","nan","None") else str(v).strip().upper()
                )
                out["date"]     = out["date"].apply(norm_date)
                out["damage"]   = out["damage"].apply(cap)
                out["location"] = out["location"].apply(cap)
                out["state"]    = out["state"].astype(str).str.strip().str.upper()
                out["odometer"] = out["odometer"].apply(clean_odo)

                out = out[
                    (out["year"] >= MIN_YEAR) &
                    out["make"].ne("") &
                    out["lot"].str.len().ge(5)
                ]
                out = ensure_cols(out)
                log(f"  {name}: {len(out):,} lots")
                all_frames.append(out)

        except subprocess.TimeoutExpired:
            log(f"  Clone timed out for {name}")
        except Exception as e:
            log(f"  Error: {e}")

    if not all_frames:
        return pd.DataFrame(columns=STANDARD_COLS)

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["lot"], keep="first")
    log(f"Total unique lots: {len(combined):,}")
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Filter out lots we already have prices for
# ─────────────────────────────────────────────────────────────────────────────

def load_existing() -> Tuple[pd.DataFrame, set, set]:
    if Path(MASTER_CSV).exists():
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
            df = ensure_cols(df)
            priced_lots = set(
                df[num_price(df["price"]) > 0]["lot"].str.strip().tolist()
            )
            all_lots = set(df["lot"].str.strip().tolist())
            priced_lots.discard(""); all_lots.discard("")
            log(f"Existing master  : {len(df):,} rows, {len(priced_lots):,} already priced")
            return df, priced_lots, all_lots
        except Exception as e:
            log(f"Could not load master: {e}")
    return pd.DataFrame(columns=STANDARD_COLS), set(), set()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Look up each lot on autobidcar via ScraperAPI
# URL: autobidcar.com/en/copart/{lot} or autobidcar.com/en/iaai/{lot}
# ─────────────────────────────────────────────────────────────────────────────

SOLD_RE  = re.compile(r"Sold\s+for:\s*\$\s*([\d,]+)", re.I)
FINAL_RE = re.compile(
    r"(?:Final\s*Bid|Sale\s*Price|Sold\s*[Ff]or|Winning\s*Bid|Last\s*Bid)"
    r"\s*[:\-]?\s*\$?\s*([\d,]+)", re.I)

def extract_price(html: str) -> int:
    if not html: return 0
    if "page not found" in html[:500].lower(): return 0
    if "Sold for: $null" in html: return 0
    if "not found" in html[:200].lower(): return 0

    for m in SOLD_RE.finditer(html[:8000]):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p

    for m in FINAL_RE.finditer(html):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p

    nd = re.search(
        r'"(?:finalBid|soldPrice|highBid|salePrice|price)"\s*:\s*"?([\d.]+)"?',
        html[:10000]
    )
    if nd:
        p = int(float(nd.group(1)))
        if p >= MIN_PRICE: return p

    return 0

def extract_record(html: str, row: pd.Series, url: str) -> Optional[Dict]:
    """Extract full record from autobidcar lot page, enriched with Rebrowser metadata."""
    price = extract_price(html)
    if price == 0: return None

    # Try to get extra fields from autobidcar page
    lot_m  = re.search(r"(?:Lot|LOT)[:\s#]*(\d{5,})", html)
    vin_m  = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", html)
    date_m = re.search(r"(\d{4}-\d{2}-\d{2})", html[:5000])
    dmg_m  = re.search(r'"damage[^"]*"\s*:\s*"([^"]+)"', html, re.I)
    loc_m  = re.search(r'"(?:yardName|location|branchName)"\s*:\s*"([^"]+)"', html)

    # Prefer autobidcar data, fall back to Rebrowser metadata
    return {
        "year":     clean_year(row.get("year", 0)),
        "make":     cap(str(row.get("make",""))),
        "model":    cap(str(row.get("model",""))),
        "trim":     cap(str(row.get("trim",""))),
        "type":     cap(str(row.get("type",""))),
        "damage":   cap(dmg_m.group(1)) if dmg_m else cap(str(row.get("damage",""))),
        "price":    price,
        "odometer": clean_odo(str(row.get("odometer",""))),
        "lot":      str(row.get("lot","")),
        "vin":      vin_m.group(1).upper() if vin_m else str(row.get("vin","")),
        "date":     date_m.group(1) if date_m else str(row.get("date","")),
        "location": cap(loc_m.group(1)) if loc_m else cap(str(row.get("location",""))),
        "state":    str(row.get("state","")),
        "source":   "AutoBidCar-Lot",
        "auction":  str(row.get("auction","")),
        "url":      url,
    }

async def fetch_scraperapi(
    session: aiohttp.ClientSession,
    target_url: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    async with sem:
        try:
            params = {
                "api_key":      SCRAPER_API_KEY,
                "url":          target_url,
                "country_code": "us",
                "render":       "false",
            }
            async with session.get(
                SCRAPER_ENDPOINT,
                params=params,
                headers=rh(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as r:
                if r.status == 200:
                    return await r.text(errors="ignore")
                if r.status == 404: return None
                if r.status == 403:
                    log("ScraperAPI 403 — check API key")
                    return None
                if r.status == 429:
                    await asyncio.sleep(5.0)
                    return None
                return None
        except asyncio.TimeoutError: return None
        except Exception: return None


async def enrich_prices(
    lots_df: pd.DataFrame,
    priced_lots: set,
) -> List[Dict]:
    print(f"\n[3/3] Looking up prices on autobidcar via ScraperAPI...", flush=True)

    # Filter to unpriced lots only, up to TOTAL_REQUESTS
    unpriced = lots_df[~lots_df["lot"].isin(priced_lots)].copy()
    unpriced = unpriced.head(TOTAL_REQUESTS)
    log(f"Unpriced lots to look up: {len(unpriced):,}")
    log(f"API calls available:      {TOTAL_REQUESTS:,}")

    all_records: List[Dict] = []
    total_req  = 0
    total_hits = 0
    total_miss = 0
    api_errors = 0
    t0 = time.time()
    lock = asyncio.Lock()
    sem  = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 10, ttl_dns_cache=300)

    async def do_one(row: pd.Series) -> None:
        nonlocal total_req, total_hits, total_miss, api_errors

        lot     = str(row.get("lot","")).strip()
        auction = str(row.get("auction","")).strip().upper()

        # Build autobidcar lot URL
        if auction == "IAAI":
            url = f"https://autobidcar.com/en/iaai/{lot}"
        else:
            url = f"https://autobidcar.com/en/copart/{lot}"

        html = await fetch_scraperapi(session, url, sem)

        async with lock:
            total_req += 1
            if html is None:
                api_errors += 1
            else:
                rec = extract_record(html, row, url)
                if rec:
                    all_records.append(rec)
                    total_hits += 1
                else:
                    total_miss += 1

            if total_req % PROGRESS_EVERY == 0:
                elapsed = int(time.time() - t0)
                rate    = total_req / max(elapsed, 1) * 60
                hit_pct = total_hits / max(total_req, 1) * 100
                rem     = len(unpriced) - total_req
                eta     = int(rem / max(rate, 1))
                print(
                    f"  [{total_req:>5}/{len(unpriced)}]"
                    f"  hits={total_hits:>4}"
                    f"  miss={total_miss:>4}"
                    f"  err={api_errors:>3}"
                    f"  hit%={hit_pct:.1f}%"
                    f"  {rate:.0f}/min"
                    f"  ETA={eta}m",
                    flush=True,
                )

            # Save checkpoint every 500 hits
            if total_hits > 0 and total_hits % 500 == 0:
                pd.DataFrame(all_records).to_csv(
                    f"checkpoint_{total_hits}.csv", index=False
                )
                log(f"Checkpoint: {total_hits} records saved")

    async with aiohttp.ClientSession(connector=connector) as session:
        rows = [row for _, row in unpriced.iterrows()]
        BATCH = CONCURRENCY * 5

        for i in range(0, len(rows), BATCH):
            if time.time() - t0 > MAX_RUNTIME_SEC:
                log(f"Time limit {MAX_RUNTIME_SEC}s reached — saving {total_hits} records")
                break
            batch = rows[i:i + BATCH]
            await asyncio.gather(*[do_one(r) for r in batch])

    elapsed  = int(time.time() - t0)
    hit_pct  = total_hits / max(total_req, 1) * 100
    rate     = total_req / max(elapsed, 1) * 60
    log(f"\nLookup complete:")
    log(f"  Lots checked   : {total_req:,}")
    log(f"  Prices found   : {total_hits:,} ({hit_pct:.1f}% hit rate)")
    log(f"  No price       : {total_miss:,}")
    log(f"  API errors     : {api_errors:,}")
    log(f"  Rate           : {rate:.0f} req/min")
    log(f"  Time           : {elapsed}s ({elapsed//60}m {elapsed%60}s)")

    return all_records


# ─────────────────────────────────────────────────────────────────────────────
# WRITE OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def write_outputs(existing: pd.DataFrame, new_records: List[Dict], lots_df: pd.DataFrame) -> None:
    # Start with existing master
    frames = [existing] if not existing.empty else []

    # Add newly priced records
    if new_records:
        new_df = pd.DataFrame(new_records)
        new_df = ensure_cols(new_df)
        frames.append(new_df)

    # Also add unpriced Rebrowser lots (useful metadata even without price)
    if not lots_df.empty:
        frames.append(ensure_cols(lots_df))

    if not frames:
        log("Nothing to write.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined["_p"] = num_price(combined["price"])
    combined = combined.sort_values("_p", ascending=False).drop(columns=["_p"])

    # Dedup — prefer rows with prices
    for key, ml in [("lot",4), ("vin",17)]:
        has = combined[combined[key].astype(str).str.len() >= ml]
        no  = combined[~combined.index.isin(has.index)]
        has = has.drop_duplicates(subset=[key], keep="first")
        combined = pd.concat([has, no], ignore_index=True)

    combined = ensure_cols(combined)
    combined.to_csv(MASTER_CSV, index=False)
    log(f"Written : {MASTER_CSV} ({len(combined):,} rows)")

    priced = combined[num_price(combined["price"]) > 0]
    site_cols = ["year","make","model","trim","type","damage",
                 "price","odometer","lot","date","location","state","auction","source","url"]
    for c in site_cols:
        if c not in priced.columns: priced[c] = ""
    priced[site_cols].to_csv(OUTPUT_CSV, index=False)
    log(f"Written : {OUTPUT_CSV} ({len(priced):,} priced rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def run() -> None:
    t0 = time.time()
    print("=" * 68, flush=True)
    print("  Rebrowser → AutoBidCar Final Price Harvester")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  ScraperAPI key : {'set ✓' if SCRAPER_API_KEY else '✗ MISSING'}")
    print(f"  Rebrowser days : {REBROWSER_DAYS}")
    print(f"  Max API calls  : {TOTAL_REQUESTS:,}")
    print(f"  Concurrency    : {CONCURRENCY}")
    print(f"  Est. hit rate  : ~60-80% (real lot numbers)")
    print(f"  Est. records   : ~{int(TOTAL_REQUESTS * 0.70):,}")
    print("=" * 68, flush=True)

    if not SCRAPER_API_KEY:
        print("\n  ERROR: SCRAPER_API_KEY not set.")
        print("  Add it as a GitHub secret named SCRAPER_API_KEY")
        return

    # Step 1: Get real lot numbers from Rebrowser
    lots_df = await fetch_rebrowser_lots()
    if lots_df.empty:
        log("No lots fetched from Rebrowser. Exiting.")
        return
    log(f"Lots available: {len(lots_df):,}")

    # Step 2: Load existing master, find unpriced lots
    print(f"\n[2/3] Loading existing master...", flush=True)
    existing, priced_lots, all_lots = load_existing()

    new_lots = lots_df[~lots_df["lot"].isin(all_lots)]
    log(f"New lots not in master: {len(new_lots):,}")

    # Step 3: Look up prices on autobidcar
    new_records = await enrich_prices(lots_df, priced_lots)

    # Write output
    print("\n" + "=" * 68, flush=True)
    write_outputs(existing, new_records, lots_df)

    elapsed = int(time.time() - t0)
    print(f"\n  Total runtime : {elapsed}s ({elapsed//60}m {elapsed%60}s)")
    print("=" * 68, flush=True)


def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
