"""
AutoBidCar ScraperAPI Harvester — Max Efficiency
=================================================
Uses all 5,000 free ScraperAPI requests in one run.
At ~20% hit rate = ~1,000 real Copart/IAAI priced records per run.

ScraperAPI routes through real residential IPs → autobidcar serves pages.
Free tier: 5,000 requests/month at scraperapi.com

Setup:
  1. Sign up free at scraperapi.com
  2. Copy your API key
  3. Add GitHub secret: SCRAPER_API_KEY = your key
  4. Run this workflow once a month to use all free credits

Install:
  pip install aiohttp pandas beautifulsoup4 lxml

Env vars:
  SCRAPER_API_KEY   your ScraperAPI key (required)
  TOTAL_REQUESTS    how many API calls to make (default 4800, leaves buffer)
  CONCURRENCY       parallel requests (default 20, ScraperAPI recommends ≤20)
  REQUEST_TIMEOUT   seconds per request (default 60, ScraperAPI can be slow)
"""

from __future__ import annotations

import asyncio, json, os, random, re, time
from datetime import datetime, timezone
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
TOTAL_REQUESTS   = int(os.environ.get("TOTAL_REQUESTS",  "4800"))  # leave 200 buffer from 5000
CONCURRENCY      = int(os.environ.get("CONCURRENCY",     "20"))    # ScraperAPI recommended max
REQUEST_TIMEOUT  = int(os.environ.get("REQUEST_TIMEOUT", "60"))    # ScraperAPI needs longer timeout
PROGRESS_EVERY   = int(os.environ.get("PROGRESS_EVERY",  "100"))
MAX_RUNTIME_SEC  = int(os.environ.get("MAX_RUNTIME_SEC", "3000"))  # 50 min max

# ScraperAPI endpoint — POST your target URL as a parameter
SCRAPER_ENDPOINT = "http://api.scraperapi.com"

STANDARD_COLS = [
    "year","make","model","trim","type","damage",
    "price","odometer","lot","vin","date",
    "location","state","source","auction","url",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ─────────────────────────────────────────────────────────────────────────────
# VIN GENERATOR  (real WMI + valid check digit)
# ─────────────────────────────────────────────────────────────────────────────
VIN_VALS = {
    "A":1,"B":2,"C":3,"D":4,"E":5,"F":6,"G":7,"H":8,
    "J":1,"K":2,"L":3,"M":4,"N":5,"P":7,"R":9,
    "S":2,"T":3,"U":4,"V":5,"W":6,"X":7,"Y":8,"Z":9,
    **{str(i):i for i in range(10)},
}
VIN_WEIGHTS = [8,7,6,5,4,3,2,10,0,9,8,7,6,5,4,3,2]

def vin_check_digit(v: str) -> str:
    t = sum(VIN_VALS.get(c, 0) * VIN_WEIGHTS[i] for i, c in enumerate(v))
    r = t % 11
    return "X" if r == 10 else str(r)

def make_vin(wmi8: str, year_code: str, plant: str, seq: int) -> str:
    s   = str(seq).zfill(6)
    p   = wmi8 + "0" + year_code + plant + s
    chk = vin_check_digit(p)
    return wmi8 + chk + year_code + plant + s

YEAR_CODES = {
    2010:"A",2011:"B",2012:"C",2013:"D",2014:"E",
    2015:"F",2016:"G",2017:"H",2018:"J",2019:"K",
    2020:"L",2021:"M",2022:"N",2023:"P",2024:"R",2025:"S",
}

# Real WMI patterns: (wmi8, make_slug, model_slug, plant, year_min, year_max)
VIN_PATTERNS = [
    # Toyota
    ("4T1BF1FK","toyota","camry","E",2015,2024),
    ("4T1C11AK","toyota","camry","E",2019,2024),
    ("4T1K61AK","toyota","camry","E",2018,2024),
    ("5TDDKRFH","toyota","highlander","0",2014,2023),
    ("5TFCZ5AN","toyota","tacoma","Z",2016,2024),
    ("5TFAX5GN","toyota","tacoma","Z",2016,2024),
    ("2T3RFREV","toyota","rav4","F",2013,2023),
    ("JTMRFREV","toyota","rav4","J",2013,2023),
    # Honda
    ("1HGCR2F3","honda","accord","A",2013,2024),
    ("1HGCR2E5","honda","accord","A",2013,2024),
    ("2HGFC2F5","honda","civic","8",2012,2024),
    ("2HGFC2F8","honda","civic","8",2016,2024),
    ("5FNYF6H9","honda","pilot","B",2016,2024),
    ("19XFC2F5","honda","civic","8",2016,2024),
    # Chevrolet
    ("1G1ZD5ST","chevrolet","malibu","F",2013,2024),
    ("1GNSCBKC","chevrolet","tahoe","R",2015,2024),
    ("2GNALAEK","chevrolet","equinox","6",2018,2024),
    ("2GNALFEK","chevrolet","equinox","6",2018,2024),
    ("1GCRYDED","chevrolet","silverado","Z",2014,2024),
    ("1GCUKREC","chevrolet","silverado","Z",2014,2024),
    # Ford
    ("1FA6P8AM","ford","mustang","F",2015,2024),
    ("1FA6P8TH","ford","mustang","F",2015,2024),
    ("1FMCU9GX","ford","escape","A",2013,2024),
    ("1FTEW1EP","ford","f-150","F",2015,2024),
    ("1FTFW1ET","ford","f-150","F",2015,2024),
    ("3FADP4BJ","ford","fiesta","E",2014,2019),
    # Nissan
    ("1N4AL3AP","nissan","altima","C",2013,2024),
    ("1N4BL4CV","nissan","altima","C",2019,2024),
    ("5N1AZ2MH","nissan","murano","B",2015,2024),
    ("3N1AB7AP","nissan","sentra","C",2013,2022),
    ("JN8AT2MT","nissan","rogue","T",2014,2024),
    # Hyundai
    ("5NPE34AF","hyundai","sonata","H",2015,2024),
    ("5NPD84LF","hyundai","sonata","H",2015,2024),
    ("5NMS3CAD","hyundai","santa-fe","H",2019,2024),
    ("KM8J3CA4","hyundai","tucson","U",2016,2024),
    ("5NMS23AD","hyundai","santa-fe","H",2013,2018),
    # Kia
    ("5XXGT4L3","kia","optima","U",2014,2020),
    ("5XYPGDA5","kia","sorento","U",2016,2024),
    ("KNDPM3AC","kia","sportage","U",2017,2024),
    ("KNDJP3A5","kia","soul","U",2014,2024),
    # Jeep
    ("1C4RJFBG","jeep","grand-cherokee","A",2014,2024),
    ("1C4HJXEG","jeep","wrangler","A",2018,2024),
    ("1C4NJDBB","jeep","compass","A",2017,2024),
    ("1C4PJLCB","jeep","cherokee","A",2014,2023),
    # BMW
    ("WBA3A5G5","bmw","3-series","N",2012,2024),
    ("WBA8E9G5","bmw","3-series","N",2017,2024),
    ("5UXWX9C5","bmw","x3","L",2013,2024),
    ("5UXKR0C5","bmw","x5","L",2014,2024),
    # Mercedes
    ("4JGBF2FE","mercedes-benz","gle","A",2016,2024),
    ("WDDZF4KB","mercedes-benz","e-class","A",2017,2024),
    ("4JGDA5HB","mercedes-benz","gl","A",2013,2019),
    # Dodge/RAM
    ("1C6RR7LT","ram","1500","D",2013,2024),
    ("3C6RR7LT","ram","1500","D",2014,2024),
    ("2C3CDXBG","dodge","challenger","B",2013,2024),
    ("2C3CDXCT","dodge","charger","B",2015,2024),
    ("1C4SDJCT","dodge","durango","B",2014,2024),
    # Subaru
    ("4S3BWAC6","subaru","legacy","6",2018,2024),
    ("4S4BSANC","subaru","outback","6",2018,2024),
    ("JF2SKAEC","subaru","forester","H",2019,2024),
    ("JF1GPAC6","subaru","impreza","F",2012,2023),
    # GMC
    ("1GKKNKLS","gmc","acadia","Z",2017,2024),
    ("1GTVKNEH","gmc","sierra","Z",2019,2024),
    ("1GKKVPKD","gmc","yukon","R",2015,2024),
    # Mazda
    ("JM3KFBCM","mazda","cx-5","U",2013,2024),
    ("JM1GL1VM","mazda","mazda6","U",2014,2021),
    # Volkswagen
    ("1VWBS7A3","volkswagen","passat","A",2012,2022),
    ("3VWF17AT","volkswagen","jetta","H",2016,2024),
    # Lexus
    ("2T2BZMCA","lexus","rx","F",2016,2024),
    ("JTJBARBZ","lexus","nx","U",2015,2024),
    # Tesla
    ("5YJ3E1EA","tesla","model-3","F",2017,2024),
    ("5YJSA1E2","tesla","model-s","P",2012,2024),
]

def generate_vins(n: int, seen: set) -> List[Tuple[str,str,str,str]]:
    out = []
    tries = 0
    while len(out) < n and tries < n * 20:
        tries += 1
        pat = random.choice(VIN_PATTERNS)
        wmi8, make, model, plant, y_min, y_max = pat
        year = random.randint(y_min, y_max)
        yc   = YEAR_CODES.get(year, "N")
        seq  = random.randint(100000, 999999)
        try:
            vin = make_vin(wmi8, yc, plant, seq)
            if vin not in seen:
                out.append((vin, make, model, str(year)))
                seen.add(vin)
        except Exception:
            pass
    return out

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg): print(f"  {msg}", flush=True)

def rh() -> Dict[str, str]:
    return {"User-Agent": random.choice(USER_AGENTS)}

def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower().strip()).strip("-")

def clean_price(v) -> int:
    s = str(v).replace("$","").replace(",","").strip()
    m = re.search(r"(\d+)", s)
    p = int(m.group(1)) if m else 0
    return p if p >= MIN_PRICE else 0

def clean_year(v) -> int:
    try: return int(float(str(v)))
    except: return 0

def clean_odo(v) -> str:
    if not v: return ""
    s = str(v).replace(",","").strip()
    m = re.search(r"([\d.]+)", s)
    if not m: return ""
    return f"{int(float(m.group(1)))} {'km' if 'km' in s.lower() else 'mi'}"

def cap(s) -> str:
    if not s or str(s).strip() in ("","nan","None","N/A"): return ""
    return str(s).strip().title()

def norm_date(v) -> str:
    if not v: return ""
    s = str(v).strip()
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m: return m.group(1)
    for fmt in ("%B %d, %Y","%b %d, %Y","%m/%d/%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in STANDARD_COLS:
        if c not in df.columns: df[c] = ""
    return df[STANDARD_COLS]

def num_price(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

# ─────────────────────────────────────────────────────────────────────────────
# PRICE EXTRACTORS
# ─────────────────────────────────────────────────────────────────────────────

SOLD_RE  = re.compile(r"Sold\s+for:\s*\$\s*([\d,]+)", re.I)
FINAL_RE = re.compile(
    r"(?:Final\s*Bid|Sale\s*Price|Sold\s*[Ff]or|Winning\s*Bid|Last\s*Bid)"
    r"\s*[:\-]?\s*\$?\s*([\d,]+)", re.I)

def extract_price(html: str) -> int:
    if not html: return 0
    if "page not found" in html[:500].lower(): return 0
    if "Sold for: $null" in html: return 0

    # autobidcar "Sold for: $X,XXX" in title/meta
    for m in SOLD_RE.finditer(html[:5000]):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p

    # bidhistory / stat.vin patterns
    for m in FINAL_RE.finditer(html):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p

    # JSON data blob
    nd = re.search(
        r'"(?:finalBid|soldPrice|highBid|salePrice|price)"\s*:\s*"?([\d.]+)"?',
        html[:10000]
    )
    if nd:
        p = int(float(nd.group(1)))
        if p >= MIN_PRICE: return p

    return 0

def extract_record(html: str, vin: str, make: str, model: str, year: str, url: str) -> Optional[Dict]:
    price = extract_price(html)
    if price == 0: return None

    lot_m  = re.search(r"(?:Lot|LOT)[:\s#]*(\d{5,})", html)
    odo_m  = re.search(r"([\d,]+)\s*(?:mi|miles|km)\b", html[:5000], re.I)
    dmg_m  = re.search(r'"damage[^"]*"\s*:\s*"([^"]+)"', html, re.I)
    loc_m  = re.search(r'"(?:yardName|location|branchName)"\s*:\s*"([^"]+)"', html)
    date_m = re.search(r"(\d{4}-\d{2}-\d{2})", html[:5000])
    auc    = "COPART" if "COPART" in html[:3000].upper() else ("IAAI" if "IAAI" in html[:3000].upper() else "")
    state_m= re.search(r'"(?:locationState|branchState)"\s*:\s*"([A-Z]{2})"', html)

    return {
        "year":     clean_year(year),
        "make":     cap(make.replace("-"," ")),
        "model":    cap(model.replace("-"," ")),
        "trim":     "",
        "type":     "",
        "damage":   cap(dmg_m.group(1)) if dmg_m else "",
        "price":    price,
        "odometer": clean_odo(odo_m.group(1)) if odo_m else "",
        "lot":      lot_m.group(1) if lot_m else "",
        "vin":      vin,
        "date":     date_m.group(1) if date_m else "",
        "location": cap(loc_m.group(1)) if loc_m else "",
        "state":    state_m.group(1) if state_m else "",
        "source":   "ScraperAPI-AutoBidCar",
        "auction":  auc,
        "url":      url,
    }

# ─────────────────────────────────────────────────────────────────────────────
# SCRAPERAPI FETCH
# Sends requests through ScraperAPI's residential IP pool
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_scraperapi(
    session: aiohttp.ClientSession,
    target_url: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    """Fetch a URL through ScraperAPI's residential IP pool."""
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
                if r.status == 404: return None
                if r.status == 200:
                    return await r.text(errors="ignore")
                if r.status == 403:
                    log(f"ScraperAPI 403 — check your API key")
                    return None
                if r.status == 429:
                    await asyncio.sleep(5.0)
                    return None
                return None
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            return None

# ─────────────────────────────────────────────────────────────────────────────
# MASTER CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_existing() -> Tuple[pd.DataFrame, set]:
    if Path(MASTER_CSV).exists():
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
            df = ensure_cols(df)
            seen = set(df["vin"].str.strip().str.upper())
            seen.discard("")
            log(f"Existing master : {len(df):,} rows")
            return df, seen
        except Exception as e:
            log(f"Could not load master: {e}")
    return pd.DataFrame(columns=STANDARD_COLS), set()

def write_outputs(existing: pd.DataFrame, new_records: List[Dict]) -> None:
    if not new_records:
        log("No new records."); return

    new_df   = pd.DataFrame(new_records)
    new_df   = ensure_cols(new_df)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined["_p"] = num_price(combined["price"])
    combined = combined.sort_values("_p", ascending=False).drop(columns=["_p"])

    for key, ml in [("lot",4),("vin",17)]:
        has = combined[combined[key].str.len() >= ml]
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
    print("  AutoBidCar Harvester — ScraperAPI Max Efficiency")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  API key       : {'set ✓' if SCRAPER_API_KEY else '✗ MISSING — set SCRAPER_API_KEY'}")
    print(f"  Total calls   : {TOTAL_REQUESTS:,} (uses all free tier credits)")
    print(f"  Concurrency   : {CONCURRENCY}")
    print(f"  Est. records  : ~{int(TOTAL_REQUESTS * 0.20):,} (at 20% hit rate)")
    print(f"  Est. runtime  : ~{TOTAL_REQUESTS // CONCURRENCY * 3 // 60 + 1}m")
    print("=" * 68, flush=True)

    if not SCRAPER_API_KEY:
        print("\n  ERROR: SCRAPER_API_KEY not set.")
        print("  1. Sign up free at scraperapi.com")
        print("  2. Add as GitHub secret: SCRAPER_API_KEY")
        return

    existing, seen_vins = load_existing()
    all_records: List[Dict] = []
    total_req   = 0
    total_hits  = 0
    total_miss  = 0
    api_errors  = 0

    lock = asyncio.Lock()
    sem  = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY + 10,
        ttl_dns_cache=300,
    )

    async def do_one(vin: str, make: str, model: str, year: str) -> None:
        nonlocal total_req, total_hits, total_miss, api_errors

        target_url = f"https://autobidcar.com/car/{slugify(make)}-{slugify(model)}-{year}-{vin}"
        html = await fetch_scraperapi(session, target_url, sem)

        async with lock:
            total_req += 1

            if html is None:
                api_errors += 1
            else:
                rec = extract_record(html, vin, make, model, year, target_url)
                if rec:
                    all_records.append(rec)
                    seen_vins.add(vin)
                    total_hits += 1
                else:
                    total_miss += 1

            if total_req % PROGRESS_EVERY == 0:
                elapsed  = int(time.time() - t0)
                rate     = total_req / max(elapsed, 1) * 60
                hit_pct  = total_hits / max(total_req, 1) * 100
                rem      = TOTAL_REQUESTS - total_req
                eta_min  = int(rem / max(rate, 1))
                print(
                    f"  [{total_req:>5}/{TOTAL_REQUESTS}]"
                    f"  hits={total_hits:>4}"
                    f"  miss={total_miss:>4}"
                    f"  err={api_errors:>3}"
                    f"  hit%={hit_pct:.1f}%"
                    f"  {rate:.0f}/min"
                    f"  ETA={eta_min}m",
                    flush=True,
                )

            # Save checkpoint every 500 hits so we don't lose data
            if total_hits > 0 and total_hits % 500 == 0 and len(all_records) >= 500:
                checkpoint_path = Path(f"checkpoint_{total_hits}.csv")
                pd.DataFrame(all_records).to_csv(checkpoint_path, index=False)
                log(f"Checkpoint saved: {checkpoint_path}")

    async with aiohttp.ClientSession(connector=connector) as session:
        BATCH = CONCURRENCY * 5
        while total_req < TOTAL_REQUESTS:
            if time.time() - t0 > MAX_RUNTIME_SEC:
                log(f"Time limit reached — saving {total_hits} priced records")
                break

            remaining = TOTAL_REQUESTS - total_req
            batch_size = min(BATCH, remaining)
            batch = generate_vins(batch_size, seen_vins)
            if not batch:
                break

            await asyncio.gather(*[do_one(v, m, mo, y) for v, m, mo, y in batch])

    # Final summary
    elapsed  = int(time.time() - t0)
    hit_pct  = total_hits / max(total_req, 1) * 100
    rate     = total_req / max(elapsed, 1) * 60

    print("\n" + "=" * 68, flush=True)
    log(f"Total API calls  : {total_req:,} / {TOTAL_REQUESTS:,}")
    log(f"Priced records   : {total_hits:,} ({hit_pct:.1f}% hit rate)")
    log(f"Misses (no price): {total_miss:,}")
    log(f"API errors       : {api_errors:,}")
    log(f"Rate             : {rate:.0f} req/min")
    log(f"Time             : {elapsed}s ({elapsed//60}m {elapsed%60}s)")

    write_outputs(existing, all_records)
    print("=" * 68, flush=True)

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
