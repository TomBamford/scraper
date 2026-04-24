"""
AutoBidCar Harvester via Tor IP Rotation
=========================================
Uses Tor to route requests through rotating residential-like IPs,
bypassing the datacenter IP blocks that cause 0% hit rate on GitHub Actions.

How it works:
  1. GitHub Actions installs tor (free, built into ubuntu-latest)
  2. Tor runs as a SOCKS5 proxy on localhost:9050
  3. Every 30 requests we send NEWNYM signal → new Tor circuit → new exit IP
  4. Requests look like they come from real residential IPs worldwide
  5. autobidcar, bidhistory, stat.vin can't block all Tor exit nodes

Speed:
  - Tor is slower than direct (~2-5s/request) but it actually WORKS
  - 10 concurrent requests through Tor = ~200-300/min
  - 2000 requests in ~10 min = ~400-600 priced records (20-30% hit rate)

Install (handled by workflow):
  sudo apt-get install -y tor
  pip install aiohttp[socks] aiohttp pandas beautifulsoup4 lxml stem

Sources tried in order:
  1. autobidcar.com/car/{make}-{model}-{year}-{VIN}   (has "Sold for: $X")
  2. en.bidhistory.org/cars/{VIN}/                     (free, no login)
  3. stat.vin/vin-decoding/{make}/{model}/{VIN}        (fallback)
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

CONCURRENCY     = int(os.environ.get("CONCURRENCY",     "10"))   # keep low for Tor
TARGET_HITS     = int(os.environ.get("TARGET_HITS",     "2000"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))   # Tor is slower
PROGRESS_EVERY  = int(os.environ.get("PROGRESS_EVERY",  "50"))
MAX_RUNTIME_SEC = int(os.environ.get("MAX_RUNTIME_SEC", "480"))
ROTATE_EVERY    = int(os.environ.get("ROTATE_EVERY",    "30"))   # new Tor circuit every N requests

TOR_SOCKS       = "socks5://127.0.0.1:9050"
TOR_CONTROL_PORT= 9051
TOR_CONTROL_PASS= os.environ.get("TOR_CONTROL_PASS", "")

STANDARD_COLS = [
    "year","make","model","trim","type","damage",
    "price","odometer","lot","vin","date",
    "location","state","source","auction","url",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
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
    t = sum(VIN_VALS.get(c,0) * VIN_WEIGHTS[i] for i,c in enumerate(v))
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
    ("4T1BF1FK","toyota","camry","E",2015,2024),
    ("4T1C11AK","toyota","camry","E",2019,2024),
    ("5TDDKRFH","toyota","highlander","0",2014,2023),
    ("5TFCZ5AN","toyota","tacoma","Z",2016,2024),
    ("2T3RFREV","toyota","rav4","F",2013,2023),
    ("1HGCR2F3","honda","accord","A",2013,2024),
    ("2HGFC2F5","honda","civic","8",2012,2024),
    ("5FNYF6H9","honda","pilot","B",2016,2024),
    ("1G1ZD5ST","chevrolet","malibu","F",2013,2024),
    ("1GNSCBKC","chevrolet","tahoe","R",2015,2024),
    ("2GNALAEK","chevrolet","equinox","6",2018,2024),
    ("1GCRYDED","chevrolet","silverado","Z",2014,2024),
    ("1FA6P8AM","ford","mustang","F",2015,2024),
    ("1FMCU9GX","ford","escape","A",2013,2024),
    ("1FTEW1EP","ford","f-150","F",2015,2024),
    ("1N4AL3AP","nissan","altima","C",2013,2024),
    ("5N1AZ2MH","nissan","murano","B",2015,2024),
    ("5NPE34AF","hyundai","sonata","H",2015,2024),
    ("5NMS3CAD","hyundai","santa-fe","H",2019,2024),
    ("KM8J3CA4","hyundai","tucson","U",2016,2024),
    ("5XXGT4L3","kia","optima","U",2014,2020),
    ("5XYPGDA5","kia","sorento","U",2016,2024),
    ("1C4RJFBG","jeep","grand-cherokee","A",2014,2024),
    ("1C4HJXEG","jeep","wrangler","A",2018,2024),
    ("WBA3A5G5","bmw","3-series","N",2012,2024),
    ("5UXWX9C5","bmw","x3","L",2013,2024),
    ("4JGBF2FE","mercedes-benz","gle","A",2016,2024),
    ("1C6RR7LT","ram","1500","D",2013,2024),
    ("2C3CDXBG","dodge","challenger","B",2013,2024),
    ("2C3CDXCT","dodge","charger","B",2015,2024),
    ("4S4BSANC","subaru","outback","6",2018,2024),
    ("JF2SKAEC","subaru","forester","H",2019,2024),
    ("1GKVKNEH","gmc","sierra","Z",2019,2024),
    ("1GKKNKLS","gmc","acadia","Z",2017,2024),
]

def generate_vins(n: int, seen: set) -> List[Tuple[str,str,str,str]]:
    out = []
    tries = 0
    while len(out) < n and tries < n * 15:
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
# TOR CIRCUIT ROTATION
# ─────────────────────────────────────────────────────────────────────────────

def rotate_tor_circuit() -> bool:
    """Send NEWNYM to Tor control port to get a new exit node."""
    try:
        import socket
        s = socket.socket()
        s.connect(("127.0.0.1", TOR_CONTROL_PORT))
        s.send(b"AUTHENTICATE\r\n")
        resp = s.recv(128).decode()
        if "250" in resp:
            s.send(b"SIGNAL NEWNYM\r\n")
            s.recv(128)
            s.close()
            return True
        s.close()
    except Exception:
        pass
    return False

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg): print(f"  {msg}", flush=True)

def rh() -> Dict[str,str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

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
# PRICE EXTRACTORS  (one per site)
# ─────────────────────────────────────────────────────────────────────────────

SOLD_RE   = re.compile(r"Sold\s+for:\s*\$\s*([\d,]+)", re.I)
FINAL_RE  = re.compile(
    r"(?:Final\s*Bid|Sale\s*Price|Sold\s*[Ff]or|Winning\s*Bid|Last\s*Bid)"
    r"\s*[:\-]?\s*\$?\s*([\d,]+)", re.I)

def extract_autobidcar(html: str) -> int:
    if "Sold for: $null" in html or "page not found" in html[:500].lower():
        return 0
    for m in SOLD_RE.finditer(html[:5000]):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p
    nd = re.search(r'"(?:finalBid|soldPrice|highBid|price)"\s*:\s*"?([\d.]+)"?', html[:8000])
    if nd:
        p = int(float(nd.group(1)))
        if p >= MIN_PRICE: return p
    return 0

def extract_bidhistory(html: str) -> int:
    if "not found" in html[:1000].lower(): return 0
    for m in FINAL_RE.finditer(html):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p
    # bidhistory.org puts price in JSON-LD
    nd = re.search(r'"price"\s*:\s*"?([\d.]+)"?', html)
    if nd:
        p = int(float(nd.group(1)))
        if p >= MIN_PRICE: return p
    return 0

def build_urls(vin: str, make: str, model: str, year: str) -> List[Tuple[str,str]]:
    """Return list of (url, site_name) to try for this VIN."""
    m = slugify(make)
    mo = slugify(model)
    return [
        (f"https://autobidcar.com/car/{m}-{mo}-{year}-{vin}", "autobidcar"),
        (f"https://en.bidhistory.org/cars/{vin}/",             "bidhistory"),
        (f"https://stat.vin/vin-decoding/{m}/{mo}/{vin}",      "statvin"),
    ]

def extract_price_any(html: str, site: str) -> int:
    if site == "autobidcar": return extract_autobidcar(html)
    if site == "bidhistory": return extract_bidhistory(html)
    # stat.vin fallback
    for m in FINAL_RE.finditer(html):
        p = int(m.group(1).replace(",",""))
        if MIN_PRICE <= p <= 500_000: return p
    return 0

# ─────────────────────────────────────────────────────────────────────────────
# ASYNC HTTP via Tor SOCKS5
# ─────────────────────────────────────────────────────────────────────────────

BLOCK_SIGNALS = ["captcha","access denied","cloudflare","verify you are human","/cdn-cgi/"]

async def fetch_via_tor(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    async with sem:
        try:
            async with session.get(
                url,
                headers=rh(),
                proxy=TOR_SOCKS,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as r:
                if r.status == 404: return None
                if r.status in (403, 429):
                    await asyncio.sleep(3.0)
                    return None
                if r.status == 200:
                    html = await r.text(errors="ignore")
                    if any(s in html[:2000].lower() for s in BLOCK_SIGNALS):
                        return None
                    return html
        except (asyncio.TimeoutError, aiohttp.ClientError):
            return None
        except Exception:
            return None
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
    print("  AutoBidCar / BidHistory Harvester  (via Tor IP rotation)")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Tor proxy     : {TOR_SOCKS}")
    print(f"  Concurrency   : {CONCURRENCY}  (keep low for Tor)")
    print(f"  Target hits   : {TARGET_HITS:,}")
    print(f"  Circuit rotate: every {ROTATE_EVERY} requests")
    print(f"  Timeout       : {REQUEST_TIMEOUT}s/request")
    print("=" * 68, flush=True)

    existing, seen_vins = load_existing()
    all_records: List[Dict] = []
    total_req = 0
    total_hits = 0

    lock = asyncio.Lock()
    sem  = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY + 5,
        ttl_dns_cache=60,
        force_close=True,   # important for Tor — don't reuse connections across circuits
    )

    async def do_one(vin: str, make: str, model: str, year: str) -> None:
        nonlocal total_req, total_hits

        urls = build_urls(vin, make, model, year)
        found = False

        for url, site in urls:
            html = await fetch_via_tor(session, url, sem)
            if not html:
                continue
            price = extract_price_any(html, site)
            if price > 0:
                # Extract extra fields from the page
                lot_m  = re.search(r"(?:Lot|LOT)[:\s#]*(\d{5,})", html)
                odo_m  = re.search(r"([\d,]+)\s*(?:mi|miles|km)\b", html[:5000], re.I)
                dmg_m  = re.search(r'"damage[^"]*"\s*:\s*"([^"]+)"', html, re.I)
                loc_m  = re.search(r'"(?:yardName|location|branchName)"\s*:\s*"([^"]+)"', html)
                date_m = re.search(r"(\d{4}-\d{2}-\d{2})", html[:5000])
                auc    = "COPART" if "COPART" in html[:3000].upper() else ("IAAI" if "IAAI" in html[:3000].upper() else "")

                rec = {
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
                    "state":    "",
                    "source":   f"Tor-{site}",
                    "auction":  auc,
                    "url":      url,
                }

                async with lock:
                    all_records.append(rec)
                    seen_vins.add(vin)
                    total_hits += 1
                found = True
                break  # got a price — no need to try other sites

        async with lock:
            total_req += 1

            # Rotate Tor circuit periodically
            if total_req % ROTATE_EVERY == 0:
                rotated = rotate_tor_circuit()
                await asyncio.sleep(2.0 if rotated else 0.5)

            if total_req % PROGRESS_EVERY == 0:
                elapsed = int(time.time() - t0)
                rate    = total_req / max(elapsed, 1) * 60
                hit_pct = total_hits / max(total_req, 1) * 100
                print(
                    f"  [{total_req:>6}]  hits={total_hits:>4}  "
                    f"hit%={hit_pct:.1f}%  rate={rate:.0f}/min  {elapsed}s",
                    flush=True,
                )

    async with aiohttp.ClientSession(connector=connector) as session:
        BATCH = CONCURRENCY * 5
        while total_hits < TARGET_HITS:
            if time.time() - t0 > MAX_RUNTIME_SEC:
                log(f"Time limit {MAX_RUNTIME_SEC}s reached — saving results")
                break

            batch = generate_vins(BATCH, seen_vins)
            if not batch:
                break

            await asyncio.gather(*[do_one(v,m,mo,y) for v,m,mo,y in batch])

            # If Tor is totally blocked too, stop early
            if total_req >= 200 and total_hits == 0:
                log("0 hits after 200 requests — Tor may be blocked too")
                log("Consider adding SCRAPER_API_KEY secret (~$10/mo fixes this permanently)")
                break

    print("\n" + "="*68, flush=True)
    elapsed  = int(time.time() - t0)
    hit_pct  = total_hits / max(total_req, 1) * 100
    log(f"Total requests : {total_req:,}")
    log(f"Prices found   : {total_hits:,} ({hit_pct:.1f}% hit rate)")
    log(f"Time           : {elapsed}s ({elapsed//60}m {elapsed%60}s)")

    write_outputs(existing, all_records)
    print("="*68, flush=True)

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
