"""
Rebrowser → Stat.vin Final Price Harvester  (SPEED OPTIMIZED)
=============================================================
Target: 3,000-5,000 VIN lookups in under 15 minutes.

Key speed fixes vs previous version:
  - Concurrency: 100 simultaneous requests (was 25)
  - Timeout: 8s per request (was 18s)
  - Retries: 1 (was 3) — fail fast, move on
  - URL strategy: 1 best URL per VIN (was up to 4 sequential)
  - No per-request sleep delays (was 0.15-0.55s each)
  - Pre-fill uses vectorized pandas ops (was row-by-row apply)
  - BeautifulSoup parsing: lxml parser (was html.parser, 3-5x faster)
  - Progress logged every 250 completions so you can watch it run

Install:
    pip install aiohttp pandas pyarrow beautifulsoup4 lxml

Env vars:
    SCRAPER_API_KEY          ScraperAPI key (recommended for GH Actions)
    REBROWSER_DAYS           days of parquets to fetch (default 30)
    STATVIN_CONCURRENCY      parallel requests (default 100)
    STATVIN_LOOKUPS_PER_RUN  VINs to attempt per run (default 4000)
    STATVIN_TIMEOUT          per-request timeout seconds (default 8)
"""

from __future__ import annotations

import asyncio, io, json, os, random, re, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

try:
    from bs4 import BeautifulSoup
    BS4 = True
except ImportError:
    BS4 = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_CSV  = "cars.csv"
MASTER_CSV  = "master.csv"
STATE_FILE  = "statvin_state.json"
MIN_YEAR    = 2012
MIN_PRICE   = 200

REBROWSER_DAYS        = int(os.environ.get("REBROWSER_DAYS",        "30"))
REBROWSER_CONCURRENCY = int(os.environ.get("REBROWSER_CONCURRENCY", "20"))
PARQUET_TIMEOUT       = int(os.environ.get("PARQUET_TIMEOUT",       "20"))

STATVIN_CONCURRENCY     = int(os.environ.get("STATVIN_CONCURRENCY",     "100"))
STATVIN_LOOKUPS_PER_RUN = int(os.environ.get("STATVIN_LOOKUPS_PER_RUN", "4000"))
STATVIN_TIMEOUT         = int(os.environ.get("STATVIN_TIMEOUT",         "8"))
STATVIN_RETRIES         = int(os.environ.get("STATVIN_RETRIES",         "1"))
PROGRESS_EVERY          = int(os.environ.get("PROGRESS_EVERY",          "250"))

SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "").strip()
SCRAPER_PROXY   = (
    f"http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001"
    if SCRAPER_API_KEY else
    (os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "").strip()
)

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
            "state":"locationState","url":"listingUrl",
        },
        "auction": "COPART",
    },
    "Rebrowser-IAAI": {
        "url": "https://raw.githubusercontent.com/rebrowser/iaai-dataset/main/auction-listings/data/",
        "cols": {
            "year":"year","make":"make","model":"model","trim":"trim",
            "type":"titleCode","damage":"damageDescription","odometer":"mileage",
            "lot":"stockNumber","vin":"vin","date":"saleDate","location":"branchName",
            "state":"branchState","url":"listingUrl",
        },
        "auction": "IAAI",
    },
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
]

# Price patterns to search for in HTML text
PRICE_RE = re.compile(
    r"(?:Final\s*Bid|Sale\s*Price|Sold\s*[Ff]or|Winning\s*Bid|Last\s*Bid|Purchase\s*Price)"
    r"\s*[:\-]?\s*\$?\s*([\d,]+)",
    re.IGNORECASE,
)
DOLLAR_RE = re.compile(r"\$\s*([\d,]{3,7})")  # fallback: any $X,XXX

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"  {msg}", flush=True)

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
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def clean_year(v: Any) -> int:
    try: return int(float(str(v).strip()))
    except: return 0

def clean_price(v: Any) -> int:
    if pd.isna(v): return 0
    s = str(v).replace("$","").replace(",","").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    p = int(float(m.group(1))) if m else 0
    return p if p >= MIN_PRICE else 0

def clean_odo(v: Any) -> str:
    if pd.isna(v) or not str(v).strip(): return ""
    s = str(v).replace(",","").strip()
    m = re.search(r"([\d.]+)", s)
    if not m: return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"

def cap(s: Any) -> str:
    if not s or str(s).strip() in ("","nan","None","[PREMIUM]","N/A"): return ""
    return str(s).strip().title()

def norm_date(v: Any) -> str:
    if not v: return ""
    s = str(v).strip()
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m: return m.group(1)
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s)
    if m: return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in STANDARD_COLS:
        if c not in df.columns:
            df[c] = ""
    return df[STANDARD_COLS]

def num_price(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

def load_state() -> Dict[str, Any]:
    p = Path(STATE_FILE)
    if p.exists():
        try: return json.loads(p.read_text())
        except: pass
    return {"attempted": [], "next_idx": 0}

def save_state(state: Dict[str, Any]) -> None:
    Path(STATE_FILE).write_text(json.dumps(state, indent=2))

# ─────────────────────────────────────────────────────────────────────────────
# PRICE EXTRACTOR  (regex-first, no DOM parsing unless needed)
# ─────────────────────────────────────────────────────────────────────────────

def extract_price(html: str) -> int:
    """
    Fast price extraction — tries regex directly on raw HTML first,
    only falls back to BS4 text extraction if needed.
    """
    # Fast path: regex on raw HTML (no parsing overhead)
    for m in PRICE_RE.finditer(html):
        try:
            p = int(m.group(1).replace(",", ""))
            if MIN_PRICE <= p <= 500_000:
                return p
        except: pass

    # Check __NEXT_DATA__ JSON blob (common in Next.js apps like stat.vin)
    nd = re.search(r'"(?:finalBid|soldPrice|highBid|salePrice|price)"\s*:\s*"?([\d.]+)"?', html)
    if nd:
        try:
            p = int(float(nd.group(1)))
            if p >= MIN_PRICE:
                return p
        except: pass

    # Slower fallback: strip HTML tags then scan for dollar amounts
    if BS4:
        try:
            text = BeautifulSoup(html, "lxml").get_text("\n")
        except:
            text = re.sub(r"<[^>]+>", " ", html)
    else:
        text = re.sub(r"<[^>]+>", " ", html)

    for m in PRICE_RE.finditer(text):
        try:
            p = int(m.group(1).replace(",", ""))
            if MIN_PRICE <= p <= 500_000:
                return p
        except: pass

    # Last resort: any $X,XXX amount
    for m in DOLLAR_RE.finditer(text):
        try:
            p = int(m.group(1).replace(",", ""))
            if MIN_PRICE <= p <= 500_000:
                return p
        except: pass

    return 0

# ─────────────────────────────────────────────────────────────────────────────
# STAT.VIN URL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def best_statvin_url(vin: str, make: str, model: str, lot: str, auction: str) -> str:
    """
    Return the single BEST URL to try for this vehicle.
    Priority: full make/model/vin path → lot-based fallback

    URL format confirmed from screenshot:
    https://stat.vin/vin-decoding/{make}/{model}/{vin}
    """
    vin  = (vin  or "").strip().upper()
    make = slugify(make  or "")
    model= slugify(model or "")
    lot  = (lot  or "").strip()
    auction = (auction or "").strip().upper()

    if len(vin) == 17:
        if make and model:
            return f"https://stat.vin/vin-decoding/{make}/{model}/{vin}"
        if make:
            return f"https://stat.vin/vin-decoding/{make}/{vin}"
        return f"https://stat.vin/vin-decoding/{vin}"

    # No valid VIN — fall back to lot
    if lot and len(lot) >= 5:
        if auction == "COPART":
            return f"https://stat.vin/en/copart/{lot}"
        if auction == "IAAI":
            return f"https://stat.vin/en/iaai/{lot}"

    return ""

# ─────────────────────────────────────────────────────────────────────────────
# ASYNC HTTP
# ─────────────────────────────────────────────────────────────────────────────

BLOCK_SIGNALS = ["captcha","access denied","cloudflare","verify you are human",
                 "checking your browser","/cdn-cgi/","attention required"]

async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    """Single HTML fetch — fast timeout, minimal retries."""
    async with sem:
        for attempt in range(STATVIN_RETRIES + 1):
            try:
                kw: Dict[str, Any] = dict(
                    headers=rh(),
                    timeout=aiohttp.ClientTimeout(total=STATVIN_TIMEOUT),
                    allow_redirects=True,
                    ssl=False,
                )
                if SCRAPER_PROXY:
                    kw["proxy"] = SCRAPER_PROXY

                async with session.get(url, **kw) as r:
                    if r.status == 404:
                        return None
                    if r.status in (403, 429):
                        if attempt < STATVIN_RETRIES:
                            await asyncio.sleep(2.0)
                            continue
                        return None
                    if r.status == 200:
                        html = await r.text(errors="ignore")
                        low  = html[:2000].lower()
                        if any(sig in low for sig in BLOCK_SIGNALS):
                            return None
                        return html
                    return None
            except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError):
                return None   # timeout = fail fast, don't retry
            except Exception:
                if attempt < STATVIN_RETRIES:
                    await asyncio.sleep(0.5)
                else:
                    return None
    return None

async def fetch_bytes(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> Optional[bytes]:
    """Binary fetch for parquet files."""
    async with sem:
        for attempt in range(3):
            try:
                async with session.get(
                    url, headers=rh(),
                    timeout=aiohttp.ClientTimeout(total=PARQUET_TIMEOUT),
                ) as r:
                    if r.status == 200: return await r.read()
                    if r.status in (403, 404): return None
                    await asyncio.sleep(1.0 * (attempt + 1))
            except Exception:
                if attempt < 2: await asyncio.sleep(0.8)
    return None

# ─────────────────────────────────────────────────────────────────────────────
# REBROWSER FETCHER
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_rebrowser(name: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    print(f"  Fetching {name} — {REBROWSER_DAYS} days concurrent...", flush=True)

    today = datetime.now(timezone.utc)
    urls  = [
        cfg["url"] + (today - timedelta(days=i)).strftime("%Y-%m-%d") + ".parquet"
        for i in range(REBROWSER_DAYS)
    ]

    sem = asyncio.Semaphore(REBROWSER_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=REBROWSER_CONCURRENCY * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        blobs = await asyncio.gather(*[
            fetch_bytes(session, url, sem) for url in urls
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

    # Vectorized cleanup
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
        lambda v: "" if str(v).strip() in ("[PREMIUM]","nan","None") else str(v).strip().upper()
    )
    out["date"]     = out["date"].apply(norm_date)
    out["location"] = out["location"].apply(cap)
    out["state"]    = out["state"].astype(str).str.strip().str.upper()
    out["url"]      = out["url"].apply(
        lambda v: "" if str(v).strip() in ("[PREMIUM]","nan","None") else str(v).strip()
    )
    out["auction"]  = cfg["auction"]
    out["source"]   = name

    out = out[(out["year"] >= MIN_YEAR) & out["make"].ne("") & out["model"].ne("")]
    out = ensure_cols(out)
    log(f"{name}: {len(out):,} rows from {len(frames)}/{REBROWSER_DAYS} files")
    return out

# ─────────────────────────────────────────────────────────────────────────────
# STAT.VIN ENRICHMENT  — the fast core loop
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_prices(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["price"] = num_price(df["price"])

    # Candidates: rows with no price but a valid VIN or lot
    mask = (
        (df["price"] == 0) &
        (df["vin"].str.len().eq(17) | df["lot"].str.strip().str.len().ge(5))
    )
    candidates = df[mask]
    if candidates.empty:
        log("No unpriced candidates.")
        return df

    # Rotating state — skip already-tried VINs
    state     = load_state()
    attempted = set(state.get("attempted", []))
    next_idx  = int(state.get("next_idx", 0))

    fresh = candidates[~candidates["vin"].isin(attempted)]
    if len(fresh) < STATVIN_LOOKUPS_PER_RUN // 4:
        log("Resetting attempted cache — full cycle done.")
        attempted = set()
        fresh     = candidates
        next_idx  = 0

    total_fresh = len(fresh)
    start  = next_idx % max(total_fresh, 1)
    rows   = fresh.iloc[start : start + STATVIN_LOOKUPS_PER_RUN]
    if len(rows) < STATVIN_LOOKUPS_PER_RUN:
        rows = pd.concat([rows, fresh.iloc[:max(0, STATVIN_LOOKUPS_PER_RUN - len(rows))]])
    rows = rows.head(STATVIN_LOOKUPS_PER_RUN)

    log(f"Unpriced candidates : {len(candidates):,}")
    log(f"Already attempted   : {len(attempted):,}")
    log(f"This batch          : {len(rows):,}")
    log(f"Concurrency         : {STATVIN_CONCURRENCY} simultaneous requests")
    log(f"Timeout             : {STATVIN_TIMEOUT}s per request")
    log(f"Proxy               : {'ScraperAPI ✓' if SCRAPER_API_KEY else 'none (direct)'}")
    log(f"Expected runtime    : ~{len(rows) // STATVIN_CONCURRENCY * STATVIN_TIMEOUT // 60 + 1}m")

    t0 = time.time()
    done_count  = 0
    hit_count   = 0

    # Build (idx, url) pairs — one URL per VIN, computed upfront
    tasks_meta = []
    for idx, row in rows.iterrows():
        url = best_statvin_url(
            vin=str(row.get("vin","")),
            make=str(row.get("make","")),
            model=str(row.get("model","")),
            lot=str(row.get("lot","")),
            auction=str(row.get("auction","")),
        )
        if url:
            tasks_meta.append((idx, url))

    log(f"Valid URLs built    : {len(tasks_meta):,}")

    # Semaphore + connector sized for max throughput
    sem = asyncio.Semaphore(STATVIN_CONCURRENCY)
    connector = aiohttp.TCPConnector(
        limit=STATVIN_CONCURRENCY + 20,
        ttl_dns_cache=300,
        force_close=False,
        keepalive_timeout=15,
        enable_cleanup_closed=True,
    )

    results: Dict[int, Tuple[int, str]] = {}   # idx → (price, url)
    lock = asyncio.Lock()

    async def do_one(idx: int, url: str) -> None:
        nonlocal done_count, hit_count
        html = await fetch_html(session, url, sem)
        price = extract_price(html) if html else 0

        async with lock:
            done_count += 1
            if price > 0:
                results[idx] = (price, url)
                hit_count += 1
            if done_count % PROGRESS_EVERY == 0:
                elapsed = int(time.time() - t0)
                rate    = done_count / max(elapsed, 1) * 60
                pct     = done_count / len(tasks_meta) * 100
                print(
                    f"  [{done_count:>5}/{len(tasks_meta)}]"
                    f"  {pct:.0f}%  hits={hit_count}"
                    f"  rate={rate:.0f}/min  elapsed={elapsed}s",
                    flush=True,
                )

    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*[do_one(idx, url) for idx, url in tasks_meta])

    # Apply prices back to dataframe
    for idx, (price, url) in results.items():
        if df.at[idx, "price"] == 0:
            df.at[idx, "price"] = price
            if url:
                df.at[idx, "url"] = url

    elapsed = int(time.time() - t0)
    rate    = len(tasks_meta) / max(elapsed, 1) * 60
    log(f"Prices found    : {hit_count:,} / {len(tasks_meta):,} ({hit_count/max(len(tasks_meta),1)*100:.1f}%)")
    log(f"Total time      : {elapsed}s  ({rate:.0f} lookups/min)")

    # Save state
    tried_vins = [str(rows.iloc[i].get("vin","")) for i in range(len(rows)) if rows.iloc[i].get("vin")]
    new_attempted = list(attempted | set(tried_vins))
    new_idx = (start + len(rows)) % max(total_fresh, 1)
    save_state({
        "attempted": new_attempted[-100_000:],
        "next_idx": new_idx,
        "last_run": datetime.now(timezone.utc).isoformat(),
    })

    return df

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


def prefill_from_master(incoming: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    """Vectorized price prefill — copy known prices from master into new rows."""
    if existing.empty:
        return incoming

    priced = existing[num_price(existing["price"]) > 0]
    if priced.empty:
        return incoming

    vin_price = priced.dropna(subset=["vin"]).set_index("vin")["price"].to_dict()
    lot_price = priced.dropna(subset=["lot"]).set_index("lot")["price"].to_dict()

    incoming = incoming.copy()
    incoming["price"] = num_price(incoming["price"])

    # Vectorized VIN lookup
    mask_no_price = incoming["price"] == 0
    vin_mapped    = incoming.loc[mask_no_price, "vin"].map(vin_price)
    incoming.loc[mask_no_price & vin_mapped.notna(), "price"] = vin_mapped.dropna().astype(int)

    # Vectorized lot lookup for still-unpriced rows
    mask_no_price = incoming["price"] == 0
    lot_mapped    = incoming.loc[mask_no_price, "lot"].map(lot_price)
    incoming.loc[mask_no_price & lot_mapped.notna(), "price"] = lot_mapped.dropna().astype(int)

    filled = int((num_price(incoming["price"]) > 0).sum())
    log(f"Pre-filled from master  : {filled:,} prices (vectorized)")
    return incoming


def merge_and_dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, new], ignore_index=True)
    combined = ensure_cols(combined)
    combined["_p"] = num_price(combined["price"])
    combined = combined.sort_values("_p", ascending=False).drop(columns=["_p"])

    for key, min_len in [("lot", 4), ("vin", 17)]:
        has = combined[combined[key].str.strip().str.len() >= min_len]
        no  = combined[~combined.index.isin(has.index)]
        has = has.drop_duplicates(subset=[key], keep="first")
        combined = pd.concat([has, no], ignore_index=True)

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
    print("  Rebrowser → Stat.vin  |  SPEED OPTIMIZED")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Concurrency : {STATVIN_CONCURRENCY}   Timeout : {STATVIN_TIMEOUT}s   Batch : {STATVIN_LOOKUPS_PER_RUN:,}")
    print(f"  Proxy       : {'ScraperAPI ✓' if SCRAPER_API_KEY else 'none — direct requests'}")
    print("=" * 68, flush=True)

    # Step 1: Rebrowser (both sources in parallel)
    print("\n[1/3] Rebrowser parquets (Copart + IAAI simultaneously)...")
    rb_copart, rb_iaai = await asyncio.gather(
        fetch_rebrowser("Rebrowser-Copart", REBROWSER_SOURCES["Rebrowser-Copart"]),
        fetch_rebrowser("Rebrowser-IAAI",   REBROWSER_SOURCES["Rebrowser-IAAI"]),
    )

    frames = [df for df in (rb_copart, rb_iaai) if not df.empty]
    if not frames:
        print("  No Rebrowser data. Exiting.")
        return

    incoming = pd.concat(frames, ignore_index=True)
    incoming = ensure_cols(incoming)
    print(f"  Total incoming rows : {len(incoming):,}")

    # Step 2: Pre-fill from master
    print("\n[2/3] Loading master + pre-filling known prices...")
    existing = load_existing()
    incoming = prefill_from_master(incoming, existing)

    # Step 3: stat.vin enrichment
    print("\n[3/3] Stat.vin price enrichment...")
    incoming = await enrich_prices(incoming)

    # Merge + write
    print("\n" + "=" * 68)
    combined   = merge_and_dedup(existing, incoming)
    total      = len(combined)
    with_price = int((num_price(combined["price"]) > 0).sum())

    print(f"  Master total     : {total:,}")
    print(f"  With price       : {with_price:,}")
    print(f"  Without price    : {total - with_price:,}")
    print(f"  New rows added   : {total - len(existing):,}")
    for src, grp in incoming.groupby("source"):
        p = int((num_price(grp["price"]) > 0).sum())
        print(f"  {src:<28} {len(grp):>7,} rows  {p:>6,} priced")

    write_outputs(combined)

    elapsed = int(time.time() - t0)
    print(f"\n  Total runtime : {elapsed}s  ({elapsed // 60}m {elapsed % 60}s)")
    print("=" * 68, flush=True)


def main() -> None:
    asyncio.run(run())

if __name__ == "__main__":
    main()
