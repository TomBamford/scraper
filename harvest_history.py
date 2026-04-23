"""
Rebrowser → Stat.vin Final Price Harvester  (FAST ASYNC VERSION)
================================================================
Hits 2,000–5,000 price lookups per run using:

  1. Rebrowser GitHub  → free daily Copart + IAAI lot parquets (30 days)
  2. stat.vin          → real final hammer prices via:
                         https://stat.vin/vin-decoding/{make}/{model}/{vin}

Speed strategy:
  - All parquet downloads: concurrent (asyncio + aiohttp)
  - All stat.vin lookups:  concurrent semaphore pool
  - Default 25 concurrent stat.vin requests  → ~5,000 lookups in ~10 min
  - Rotating batches via state file so each run gets a fresh slice of unpriced rows
  - ScraperAPI proxy support if stat.vin blocks direct GH Actions IPs

Install:
    pip install aiohttp pandas pyarrow beautifulsoup4

Env vars (all optional):
    SCRAPER_API_KEY         - ScraperAPI key (bypasses blocks, recommended)
    REBROWSER_DAYS          - days of parquet files to fetch (default 30)
    STATVIN_CONCURRENCY     - parallel stat.vin requests (default 25)
    STATVIN_LOOKUPS_PER_RUN - max VINs to price per run (default 3000)
    SAVE_STATVIN_DEBUG      - set to "1" to save HTML to debug/ folder
"""

from __future__ import annotations

import asyncio, io, json, os, random, re, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  (override with env vars)
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_CSV   = "cars.csv"
MASTER_CSV   = "master.csv"
STATE_FILE   = "statvin_state.json"        # tracks which VINs we've already tried
DEBUG_DIR    = "debug"

MIN_YEAR     = 2012
MIN_PRICE    = 200

REBROWSER_DAYS        = int(os.environ.get("REBROWSER_DAYS", "30"))
REBROWSER_CONCURRENCY = int(os.environ.get("REBROWSER_CONCURRENCY", "15"))
PARQUET_TIMEOUT       = int(os.environ.get("PARQUET_TIMEOUT", "25"))

STATVIN_CONCURRENCY     = int(os.environ.get("STATVIN_CONCURRENCY", "25"))
STATVIN_LOOKUPS_PER_RUN = int(os.environ.get("STATVIN_LOOKUPS_PER_RUN", "3000"))
STATVIN_TIMEOUT         = int(os.environ.get("STATVIN_TIMEOUT", "18"))
STATVIN_DELAY_MIN       = float(os.environ.get("STATVIN_DELAY_MIN", "0.15"))
STATVIN_DELAY_MAX       = float(os.environ.get("STATVIN_DELAY_MAX", "0.55"))
RETRIES                 = int(os.environ.get("RETRIES", "3"))

SAVE_DEBUG = os.environ.get("SAVE_STATVIN_DEBUG", "0").strip() == "1"

# ScraperAPI proxy — routes requests through rotating residential IPs
# Highly recommended for GitHub Actions since stat.vin blocks datacenter IPs
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "").strip()
# Build proxy URL if key is set
SCRAPER_PROXY = (
    f"http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001"
    if SCRAPER_API_KEY else
    (os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "").strip()
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

STANDARD_COLS = [
    "year", "make", "model", "trim", "type", "damage",
    "price", "odometer", "lot", "vin", "date",
    "location", "state", "source", "auction", "url",
]

REBROWSER_SOURCES = {
    "Rebrowser-Copart": {
        "url":  "https://raw.githubusercontent.com/rebrowser/copart-dataset/main/auction-listings/data/",
        "cols": {
            "year": "year", "make": "make", "model": "modelGroup",
            "trim": "trim", "type": "saleTitleType", "damage": "damageDescription",
            "odometer": "mileage", "lot": "lotId", "vin": "vin",
            "date": "saleDate", "location": "yardName",
            "state": "locationState", "url": "listingUrl",
        },
        "auction": "COPART",
    },
    "Rebrowser-IAAI": {
        "url":  "https://raw.githubusercontent.com/rebrowser/iaai-dataset/main/auction-listings/data/",
        "cols": {
            "year": "year", "make": "make", "model": "model",
            "trim": "trim", "type": "titleCode", "damage": "damageDescription",
            "odometer": "mileage", "lot": "stockNumber", "vin": "vin",
            "date": "saleDate", "location": "branchName",
            "state": "branchState", "url": "listingUrl",
        },
        "auction": "IAAI",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print("  " + msg, flush=True)

def rh() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

def clean_price(val: Any) -> int:
    if pd.isna(val): return 0
    s = str(val).replace("$", "").replace(",", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    p = int(float(m.group(1))) if m else 0
    return p if p >= MIN_PRICE else 0

def clean_year(val: Any) -> int:
    try: return int(float(str(val).strip()))
    except: return 0

def clean_odo(val: Any) -> str:
    if pd.isna(val) or not str(val).strip(): return ""
    s = str(val).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m: return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"

def cap(s: Any) -> str:
    if not s or str(s).strip() in ("", "nan", "None", "[PREMIUM]", "N/A"): return ""
    return str(s).strip().title()

def norm_date(val: Any) -> str:
    if not val: return ""
    s = str(val).strip()
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m: return m.group(1)
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s)
    if m: return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def slugify(s: str) -> str:
    """Convert 'Mercedes-Benz' → 'mercedes-benz', 'RAV 4' → 'rav-4'"""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in STANDARD_COLS:
        if c not in df.columns:
            df[c] = ""
    return df[STANDARD_COLS]

def num_price(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)

# ─────────────────────────────────────────────────────────────────────────────
# STATE FILE  — tracks already-attempted VINs so we don't retry them
# ─────────────────────────────────────────────────────────────────────────────

def load_state() -> Dict[str, Any]:
    p = Path(STATE_FILE)
    if p.exists():
        try: return json.loads(p.read_text())
        except: pass
    return {"attempted_vins": [], "next_idx": 0}

def save_state(state: Dict[str, Any]) -> None:
    Path(STATE_FILE).write_text(json.dumps(state, indent=2))

# ─────────────────────────────────────────────────────────────────────────────
# ASYNC HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

BLOCKED_SIGNALS = [
    "captcha", "access denied", "cloudflare", "verify you are human",
    "checking your browser", "/cdn-cgi/", "attention required",
    "error 403", "error 429",
]

async def fetch_bytes_async(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    timeout: int = 20,
) -> Optional[bytes]:
    """Fetch binary (for parquet files)."""
    async with sem:
        for attempt in range(RETRIES):
            try:
                async with session.get(
                    url, headers=rh(),
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    ssl=False,
                ) as r:
                    if r.status == 200:
                        return await r.read()
                    if r.status in (403, 404, 429):
                        if attempt < RETRIES - 1:
                            await asyncio.sleep(1.0 * (attempt + 1))
                        return None
            except Exception:
                if attempt < RETRIES - 1:
                    await asyncio.sleep(0.8 * (attempt + 1))
    return None

async def fetch_html_async(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    timeout: int = 18,
) -> Optional[str]:
    """Fetch HTML page (for stat.vin). Returns None if blocked."""
    async with sem:
        await asyncio.sleep(random.uniform(STATVIN_DELAY_MIN, STATVIN_DELAY_MAX))
        for attempt in range(RETRIES):
            try:
                kw: Dict[str, Any] = dict(
                    headers=rh(),
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    allow_redirects=True,
                    ssl=False,
                )
                if SCRAPER_PROXY:
                    kw["proxy"] = SCRAPER_PROXY
                async with session.get(url, **kw) as r:
                    if r.status == 404:
                        return None   # VIN not in database
                    text = await r.text(errors="ignore")
                    if r.status == 200:
                        low = text.lower()
                        if any(sig in low for sig in BLOCKED_SIGNALS):
                            return None
                        return text
                    if r.status in (403, 429):
                        wait = 2.0 * (attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                    return None
            except asyncio.TimeoutError:
                if attempt < RETRIES - 1:
                    await asyncio.sleep(1.0)
            except Exception:
                if attempt < RETRIES - 1:
                    await asyncio.sleep(0.8 * (attempt + 1))
    return None


# ─────────────────────────────────────────────────────────────────────────────
# REBROWSER FETCHER
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_rebrowser(name: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    print(f"\n  Fetching {name} ({REBROWSER_DAYS} days, {REBROWSER_CONCURRENCY} concurrent)...")

    today = datetime.now(timezone.utc)
    urls = [
        cfg["url"] + (today - timedelta(days=i)).strftime("%Y-%m-%d") + ".parquet"
        for i in range(REBROWSER_DAYS)
    ]

    sem = asyncio.Semaphore(REBROWSER_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=REBROWSER_CONCURRENCY * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        blobs = await asyncio.gather(*[
            fetch_bytes_async(session, url, sem, timeout=PARQUET_TIMEOUT)
            for url in urls
        ])

    frames = []
    for blob in blobs:
        if blob:
            try: frames.append(pd.read_parquet(io.BytesIO(blob)))
            except: pass

    if not frames:
        log(f"No data for {name}")
        return pd.DataFrame(columns=STANDARD_COLS)

    raw = pd.concat(frames, ignore_index=True)
    out = pd.DataFrame()
    for std, src in cfg["cols"].items():
        out[std] = raw[src].astype(str) if src in raw.columns else ""

    out["year"]     = out["year"].apply(clean_year)
    out["make"]     = out["make"].apply(cap)
    out["model"]    = out["model"].apply(cap)
    out["trim"]     = out["trim"].apply(cap)
    out["type"]     = out["type"].apply(cap)
    out["damage"]   = out["damage"].apply(cap)
    out["odometer"] = out["odometer"].apply(clean_odo)
    out["price"]    = 0
    out["lot"]      = out["lot"].astype(str).str.strip()
    out["vin"]      = out["vin"].apply(
        lambda v: "" if str(v).strip() in ("[PREMIUM]", "nan", "None") else str(v).strip().upper()
    )
    out["date"]     = out["date"].apply(norm_date)
    out["location"] = out["location"].apply(cap)
    out["state"]    = out["state"].astype(str).str.strip().str.upper()
    out["url"]      = out["url"].apply(
        lambda v: "" if str(v).strip() == "[PREMIUM]" else str(v).strip()
    )
    out["auction"]  = cfg["auction"]
    out["source"]   = name

    out = out[(out["year"] >= MIN_YEAR) & out["make"].ne("") & out["model"].ne("")]
    out = ensure_cols(out)
    log(f"{name}: {len(out):,} rows from {len(frames)} files")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# STAT.VIN PRICE EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

PRICE_PATTERNS = [
    r"Final\s*Bid\s*[:\-]?\s*\$?\s*([\d,]+)",
    r"Sale\s*Price\s*[:\-]?\s*\$?\s*([\d,]+)",
    r"Sold\s*[Ff]or\s*[:\-]?\s*\$?\s*([\d,]+)",
    r"Winning\s*Bid\s*[:\-]?\s*\$?\s*([\d,]+)",
    r"Last\s*Bid\s*[:\-]?\s*\$?\s*([\d,]+)",
    r"Purchase\s*Price\s*[:\-]?\s*\$?\s*([\d,]+)",
    r"\$\s*([\d,]{3,})",            # fallback: any $X,XXX amount
]

def extract_price(html: str) -> int:
    """Extract the final hammer price from a stat.vin page."""
    soup  = BeautifulSoup(html, "html.parser")
    text  = soup.get_text("\n", strip=True)

    for pat in PRICE_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            try:
                val = int(m.group(1).replace(",", ""))
                if MIN_PRICE <= val <= 500_000:
                    return val
            except:
                pass

    # Also try JSON-LD / structured data embedded in page
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            for key in ("price", "highBid", "finalBid", "soldPrice", "salePrice"):
                val = data.get(key)
                if val:
                    p = int(float(str(val).replace(",", "")))
                    if p >= MIN_PRICE:
                        return p
        except:
            pass

    # Try __NEXT_DATA__ (Next.js)
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if nd:
        try:
            data = json.loads(nd.group(1))
            raw_str = json.dumps(data)
            for key in ("finalBid", "soldPrice", "highBid", "salePrice", "price"):
                m = re.search(rf'"{key}"\s*:\s*"?([\d.]+)"?', raw_str)
                if m:
                    p = int(float(m.group(1)))
                    if p >= MIN_PRICE:
                        return p
        except:
            pass

    return 0


def build_statvin_urls(vin: str, make: str, model: str, lot: str, auction: str) -> List[str]:
    """
    Build stat.vin URL variants to try for this vehicle.
    Primary format: https://stat.vin/vin-decoding/{make}/{model}/{vin}
    """
    urls = []
    make_s  = slugify(make)
    model_s = slugify(model)

    if vin and len(vin) == 17:
        # Best URL: full make/model/vin path (as shown in screenshot)
        if make_s and model_s:
            urls.append(f"https://stat.vin/vin-decoding/{make_s}/{model_s}/{vin}")
        # Fallback: make only
        if make_s:
            urls.append(f"https://stat.vin/vin-decoding/{make_s}/{vin}")
        # Fallback: VIN only
        urls.append(f"https://stat.vin/vin-decoding/{vin}")

    # Lot-based URLs (no VIN needed)
    if lot and lot.strip() not in ("", "nan", "None"):
        if auction == "COPART":
            urls.append(f"https://stat.vin/en/copart/{lot}")
        elif auction == "IAAI":
            urls.append(f"https://stat.vin/en/iaai/{lot}")

    return urls


async def lookup_one(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    row: pd.Series,
) -> Tuple[str, int, str]:
    """Try stat.vin URLs for one vehicle. Returns (vin, price, source_url)."""
    vin     = str(row.get("vin",     "")).strip().upper()
    lot     = str(row.get("lot",     "")).strip()
    auction = str(row.get("auction", "")).strip().upper()
    make    = str(row.get("make",    "")).strip()
    model   = str(row.get("model",   "")).strip()

    urls = build_statvin_urls(vin, make, model, lot, auction)

    for url in urls:
        html = await fetch_html_async(session, url, sem, timeout=STATVIN_TIMEOUT)
        if not html:
            continue

        if SAVE_DEBUG:
            key = vin or lot or "unknown"
            Path(DEBUG_DIR).mkdir(exist_ok=True)
            Path(DEBUG_DIR, f"{key}.html").write_text(html[:300_000], errors="ignore")

        price = extract_price(html)
        if price > 0:
            return vin, price, url

    return vin, 0, ""


async def enrich_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main price enrichment — runs STATVIN_LOOKUPS_PER_RUN concurrent lookups.
    Uses a state file to rotate through unpriced rows across runs.
    """
    if df.empty:
        return df

    df = df.copy()
    df["price"] = num_price(df["price"])

    # Candidates: unpriced rows with a valid VIN or lot number
    mask = (
        (df["price"] == 0) &
        (
            (df["vin"].str.len() == 17) |
            (df["lot"].str.strip().str.len() >= 5)
        )
    )
    candidates = df[mask].copy()

    if candidates.empty:
        log("No unpriced candidates found.")
        return df

    # Load state — skip VINs we've already attempted this cycle
    state = load_state()
    attempted = set(state.get("attempted_vins", []))
    next_idx   = int(state.get("next_idx", 0))

    # Filter out already-attempted
    fresh = candidates[~candidates["vin"].isin(attempted)]
    if len(fresh) < STATVIN_LOOKUPS_PER_RUN // 2:
        # Reset cycle if we've gone through most candidates
        log("Resetting attempted VINs cache (full cycle complete).")
        attempted = set()
        fresh = candidates
        next_idx = 0

    # Take a rotating batch starting at next_idx
    fresh_list = fresh.to_dict("records")
    start = next_idx % max(len(fresh_list), 1)
    batch_rows = fresh_list[start:start + STATVIN_LOOKUPS_PER_RUN]
    if len(batch_rows) < STATVIN_LOOKUPS_PER_RUN and start > 0:
        batch_rows += fresh_list[:max(0, STATVIN_LOOKUPS_PER_RUN - len(batch_rows))]

    batch_df = pd.DataFrame(batch_rows).head(STATVIN_LOOKUPS_PER_RUN)

    log(f"Unpriced candidates : {len(candidates):,}")
    log(f"Already attempted   : {len(attempted):,}")
    log(f"This batch          : {len(batch_df):,} lookups")
    log(f"Concurrency         : {STATVIN_CONCURRENCY}")
    log(f"Proxy               : {'ScraperAPI' if SCRAPER_API_KEY else ('custom' if SCRAPER_PROXY else 'none')}")

    t_start = time.time()

    # Run concurrent lookups
    sem = asyncio.Semaphore(STATVIN_CONCURRENCY)
    connector = aiohttp.TCPConnector(
        limit       = STATVIN_CONCURRENCY * 3,
        ttl_dns_cache = 300,
        force_close = False,
        keepalive_timeout = 30,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [lookup_one(session, sem, pd.Series(row)) for row in batch_rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Apply results back to df
    price_map: Dict[str, Tuple[int, str]] = {}
    hits = 0
    errors = 0

    for res in results:
        if isinstance(res, Exception):
            errors += 1
            continue
        vin, price, url = res
        if price > 0:
            price_map[vin] = (price, url)
            hits += 1

    # Update df with prices
    for idx, row in df.iterrows():
        vin = str(row["vin"]).strip().upper()
        if vin in price_map and df.at[idx, "price"] == 0:
            p, u = price_map[vin]
            df.at[idx, "price"] = p
            if u:
                df.at[idx, "url"] = u

    elapsed = int(time.time() - t_start)
    rate = len(batch_df) / max(elapsed, 1) * 60
    log(f"Prices found        : {hits:,} / {len(batch_df):,}")
    log(f"Errors              : {errors:,}")
    log(f"Time                : {elapsed}s  ({rate:.0f} lookups/min)")

    # Save state
    new_attempted = list(attempted | {str(r.get("vin","")) for r in batch_rows if r.get("vin")})
    new_idx = (start + len(batch_df)) % max(len(fresh_list), 1)
    save_state({"attempted_vins": new_attempted[-50_000:], "next_idx": new_idx,
                "last_run": datetime.now(timezone.utc).isoformat()})

    return df


# ─────────────────────────────────────────────────────────────────────────────
# MASTER CSV  merge + dedup
# ─────────────────────────────────────────────────────────────────────────────

def load_existing() -> pd.DataFrame:
    if Path(MASTER_CSV).exists():
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
            df = ensure_cols(df)
            log(f"Existing master     : {len(df):,} rows")
            return df
        except Exception as e:
            log(f"Could not read master: {e}")
    return pd.DataFrame(columns=STANDARD_COLS)


def merge_and_dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Combine existing + new, dedup by lot then vin, prefer priced rows."""
    combined = pd.concat([existing, new], ignore_index=True)
    combined = ensure_cols(combined)
    combined["_p"] = num_price(combined["price"])
    combined = combined.sort_values("_p", ascending=False)
    combined = combined.drop(columns=["_p"])

    # Dedup by lot
    has_lot = combined[combined["lot"].str.strip().ne("") & (combined["lot"].str.len() > 3)]
    no_lot  = combined[~combined.index.isin(has_lot.index)]
    has_lot = has_lot.drop_duplicates(subset=["lot"], keep="first")
    combined = pd.concat([has_lot, no_lot], ignore_index=True)

    # Dedup by VIN
    has_vin = combined[combined["vin"].str.len().eq(17)]
    no_vin  = combined[~combined.index.isin(has_vin.index)]
    has_vin = has_vin.drop_duplicates(subset=["vin"], keep="first")
    combined = pd.concat([has_vin, no_vin], ignore_index=True)

    return ensure_cols(combined)


def write_outputs(combined: pd.DataFrame) -> None:
    combined.to_csv(MASTER_CSV, index=False)
    log(f"Written : {MASTER_CSV} ({len(combined):,} total rows)")

    priced = combined[num_price(combined["price"]) > 0].copy()
    site_cols = ["year","make","model","trim","type","damage",
                 "price","odometer","lot","date","location","state","auction","source","url"]
    for c in site_cols:
        if c not in priced.columns: priced[c] = ""
    priced[site_cols].to_csv(OUTPUT_CSV, index=False)
    log(f"Written : {OUTPUT_CSV} ({len(priced):,} priced rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def run_async() -> None:
    t0 = time.time()

    print("=" * 68)
    print("  Rebrowser → Stat.vin Final Price Harvester")
    print(f"  Date         : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Parquet days : {REBROWSER_DAYS}")
    print(f"  Batch size   : {STATVIN_LOOKUPS_PER_RUN:,} VIN lookups")
    print(f"  Concurrency  : {STATVIN_CONCURRENCY} parallel requests")
    print(f"  Proxy        : {'ScraperAPI ✓' if SCRAPER_API_KEY else ('custom' if SCRAPER_PROXY else 'none (direct)')}")
    print("=" * 68)

    # ── Step 1: Rebrowser parquets ──────────────────────────────────────────
    print("\n[1/3] Downloading Rebrowser Copart + IAAI parquets...")
    rb_copart, rb_iaai = await asyncio.gather(
        fetch_rebrowser("Rebrowser-Copart", REBROWSER_SOURCES["Rebrowser-Copart"]),
        fetch_rebrowser("Rebrowser-IAAI",   REBROWSER_SOURCES["Rebrowser-IAAI"]),
    )

    frames = [df for df in (rb_copart, rb_iaai) if not df.empty]
    if not frames:
        print("  No Rebrowser data fetched. Exiting.")
        return

    incoming = pd.concat(frames, ignore_index=True)
    incoming = ensure_cols(incoming)
    print(f"  Combined incoming : {len(incoming):,} rows")

    # ── Step 2: Load existing master + merge to find truly unpriced ─────────
    print("\n[2/3] Loading existing master + merging...")
    existing = load_existing()

    # Pre-enrich: copy prices from existing master into incoming
    if not existing.empty:
        vin_price = existing[num_price(existing["price"]) > 0].set_index("vin")["price"].to_dict()
        lot_price = existing[num_price(existing["price"]) > 0].set_index("lot")["price"].to_dict()
        def fill_price(row):
            if row["price"] > 0:
                return row
            if row["vin"] in vin_price:
                row["price"] = int(vin_price[row["vin"]])
            elif row["lot"] in lot_price:
                row["price"] = int(lot_price[row["lot"]])
            return row
        incoming["price"] = num_price(incoming["price"])
        incoming = incoming.apply(fill_price, axis=1)
        pre_filled = int((num_price(incoming["price"]) > 0).sum())
        log(f"Pre-filled from master  : {pre_filled:,} prices")

    # ── Step 3: Stat.vin enrichment ─────────────────────────────────────────
    print("\n[3/3] Stat.vin price enrichment...")
    incoming = await enrich_prices(incoming)

    # ── Write outputs ────────────────────────────────────────────────────────
    print("\n" + "=" * 68)
    combined = merge_and_dedup(existing, incoming)

    total      = len(combined)
    with_price = int((num_price(combined["price"]) > 0).sum())
    new_rows   = total - len(existing)

    print(f"  New rows added   : {new_rows:,}")
    print(f"  Master total     : {total:,}")
    print(f"  With price       : {with_price:,}")
    print(f"  Without price    : {total - with_price:,}")

    # Source breakdown
    for src, grp in incoming.groupby("source"):
        priced = int((num_price(grp["price"]) > 0).sum())
        print(f"  {src:<30} {len(grp):>7,} rows  {priced:>7,} priced")

    write_outputs(combined)

    elapsed = int(time.time() - t0)
    print(f"\n  Done in {elapsed}s ({elapsed // 60}m {elapsed % 60}s)")
    print("=" * 68)


def main() -> None:
    asyncio.run(run_async())


if __name__ == "__main__":
    main()
