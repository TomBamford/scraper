from __future__ import annotations

import asyncio
import io
import json
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

"""
Rebrowser -> Stat.vin Final Price Harvester
==========================================

This version:
- downloads Rebrowser Copart + IAAI parquet snapshots
- normalizes and dedupes them
- resolves final sale prices from Stat.vin using direct pages only
- avoids Stat.vin /vehicle-search because it is commonly blocked

Lookup strategy:
1) VIN page:  https://stat.vin/cars/<VIN>
2) Copart lot: https://stat.vin/en/copart/<LOT>
3) IAAI lot:   https://stat.vin/en/iaai/<LOT>

Install:
    pip install aiohttp pandas pyarrow beautifulsoup4 lxml fastparquet

Run:
    python harvest_history_stat_vin.py

Optional env vars:
    REBROWSER_DAYS=30
    REBROWSER_CONCURRENCY=10
    STATVIN_CONCURRENCY=4
    STATVIN_LOOKUPS_PER_RUN=2000
    STATVIN_TIMEOUT=20
    SCRAPER_PROXY=http://user:pass@host:port
"""

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

OUTPUT_CSV = "cars.csv"
MASTER_CSV = "master.csv"
CHECKPOINT_DIR = "checkpoints"
DEBUG_DIR = "debug"
LOOKUP_BATCH_STATE_FILE = os.environ.get("LOOKUP_BATCH_STATE_FILE", "statvin_lookup_state.json")

MIN_YEAR = 2012
MIN_PRICE = 200
REBROWSER_DAYS = int(os.environ.get("REBROWSER_DAYS", "30"))
REBROWSER_CONCURRENCY = int(os.environ.get("REBROWSER_CONCURRENCY", "10"))
STATVIN_CONCURRENCY = int(os.environ.get("STATVIN_CONCURRENCY", "4"))
STATVIN_LOOKUPS_PER_RUN = int(os.environ.get("STATVIN_LOOKUPS_PER_RUN", "2000"))
STATVIN_TIMEOUT = int(os.environ.get("STATVIN_TIMEOUT", "20"))
PARQUET_TIMEOUT = int(os.environ.get("PARQUET_TIMEOUT", "20"))
RETRIES = int(os.environ.get("RETRIES", "3"))
MIN_PRICED_ROWS_TO_WRITE = int(os.environ.get("MIN_PRICED_ROWS_TO_WRITE", "100"))
SAVE_STATVIN_DEBUG = os.environ.get("SAVE_STATVIN_DEBUG", "0").strip() == "1"

SCRAPER_PROXY = (
    os.environ.get("SCRAPER_PROXY")
    or os.environ.get("HTTPS_PROXY")
    or os.environ.get("HTTP_PROXY")
    or ""
).strip()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

STANDARD_COLS = [
    "year", "make", "model", "trim", "type", "damage",
    "price", "odometer", "lot", "vin", "date",
    "location", "state", "source", "auction", "url"
]

REBROWSER_SOURCES = {
    "Rebrowser-Copart": {
        "url": "https://raw.githubusercontent.com/rebrowser/copart-dataset/main/auction-listings/data/",
        "cols": {
            "year": "year",
            "make": "make",
            "model": "modelGroup",
            "trim": "trim",
            "type": "saleTitleType",
            "damage": "damageDescription",
            "odometer": "mileage",
            "lot": "lotId",
            "vin": "vin",
            "date": "saleDate",
            "location": "yardName",
            "state": "locationState",
            "url": "listingUrl",
        },
        "auction": "COPART",
    },
    "Rebrowser-IAAI": {
        "url": "https://raw.githubusercontent.com/rebrowser/iaai-dataset/main/auction-listings/data/",
        "cols": {
            "year": "year",
            "make": "make",
            "model": "model",
            "trim": "trim",
            "type": "titleCode",
            "damage": "damageDescription",
            "odometer": "mileage",
            "lot": "stockNumber",
            "vin": "vin",
            "date": "saleDate",
            "location": "branchName",
            "state": "branchState",
            "url": "listingUrl",
        },
        "auction": "IAAI",
    },
}

# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------

def log(msg: str) -> None:
    print(f"  {msg}")


def rh() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def save_json(path: str | Path, data: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_json(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_debug_text(name: str, content: str) -> None:
    ensure_dir(DEBUG_DIR)
    Path(DEBUG_DIR, name).write_text(content, encoding="utf-8", errors="ignore")


def save_debug_html(name: str, html: str) -> None:
    ensure_dir(DEBUG_DIR)
    Path(DEBUG_DIR, f"{name}.html").write_text(html[:500000], encoding="utf-8", errors="ignore")


def clean_price(val: Any) -> int:
    if pd.isna(val):
        return 0
    s = str(val).replace("$", "").replace(",", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    p = int(float(m.group(1))) if m else 0
    return p if p >= MIN_PRICE else 0


def clean_year(val: Any) -> int:
    try:
        return int(float(str(val).strip()))
    except Exception:
        return 0


def clean_odo(val: Any) -> str:
    if pd.isna(val) or not str(val).strip():
        return ""
    s = str(val).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m:
        return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"


def cap(s: Any) -> str:
    if not s or str(s).strip() in ("", "nan", "None", "[PREMIUM]", "N/A"):
        return ""
    return str(s).strip().title()


def norm_date(val: Any) -> str:
    if not val:
        return ""
    s = str(val).strip()

    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"

    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%a %d %b %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    return ""


def ensure_standard_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in STANDARD_COLS:
        if col not in df.columns:
            df[col] = ""
    return df[STANDARD_COLS]


def to_int_price_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = {c: rec.get(c, "") for c in STANDARD_COLS}
    out["year"] = clean_year(out.get("year", 0))
    out["make"] = cap(out.get("make", ""))
    out["model"] = cap(out.get("model", ""))
    out["trim"] = cap(out.get("trim", ""))
    out["type"] = cap(out.get("type", ""))
    out["damage"] = cap(out.get("damage", ""))
    out["price"] = clean_price(out.get("price", 0))
    out["odometer"] = clean_odo(out.get("odometer", ""))
    out["lot"] = str(out.get("lot", "")).strip()
    out["vin"] = str(out.get("vin", "")).strip().upper()
    out["date"] = norm_date(out.get("date", ""))
    out["location"] = cap(out.get("location", ""))
    out["state"] = str(out.get("state", "")).strip().upper()
    out["source"] = str(out.get("source", "")).strip()
    out["auction"] = str(out.get("auction", "")).strip().upper()
    out["url"] = str(out.get("url", "")).strip()
    return out


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return ensure_standard_cols(df)

    out = ensure_standard_cols(df.copy())
    out["year"] = out["year"].apply(clean_year)
    out["make"] = out["make"].apply(cap)
    out["model"] = out["model"].apply(cap)
    out["trim"] = out["trim"].apply(cap)
    out["type"] = out["type"].apply(cap)
    out["damage"] = out["damage"].apply(cap)
    out["price"] = out["price"].apply(clean_price)
    out["odometer"] = out["odometer"].apply(clean_odo)
    out["lot"] = out["lot"].astype(str).str.strip()
    out["vin"] = out["vin"].astype(str).str.strip().str.upper()
    out["date"] = out["date"].apply(norm_date)
    out["location"] = out["location"].apply(cap)
    out["state"] = out["state"].astype(str).str.strip().str.upper()
    out["source"] = out["source"].astype(str).str.strip()
    out["auction"] = out["auction"].astype(str).str.strip().str.upper()
    out["url"] = out["url"].astype(str).str.strip()
    return out


def rotated_slice(items: List[int], state_file: str, batch_size: int) -> List[int]:
    if not items:
        return []
    state = load_json(state_file)
    idx = int(state.get("next_index", 0)) if str(state.get("next_index", "")).isdigit() else 0
    idx %= len(items)
    out = items[idx:idx + batch_size]
    if len(out) < batch_size:
        out += items[: max(0, batch_size - len(out))]
    next_idx = (idx + batch_size) % len(items)
    save_json(state_file, {"next_index": next_idx, "updated_at": datetime.now(timezone.utc).isoformat()})
    return out


# ------------------------------------------------------------------
# HTTP
# ------------------------------------------------------------------

async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int,
    retries: int = RETRIES,
) -> Optional[str]:
    for attempt in range(retries):
        try:
            await asyncio.sleep(random.uniform(0.3, 1.0))
            async with session.get(
                url,
                headers=rh(),
                proxy=SCRAPER_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                text = await resp.text(errors="ignore")
                if resp.status == 200:
                    low = text.lower()
                    blocked_signals = [
                        "captcha",
                        "access denied",
                        "forbidden",
                        "cloudflare",
                        "attention required",
                        "/cdn-cgi/",
                        "verify you are human",
                        "checking your browser",
                    ]
                    if any(sig in low for sig in blocked_signals):
                        print(f"[BLOCKED] {url}")
                        return None
                    return text
                if resp.status in (403, 429):
                    print(f"[HTTP {resp.status}] {url}")
                    await asyncio.sleep(1.2 * (attempt + 1))
                    continue
                print(f"[HTTP {resp.status}] {url}")
                return None
        except Exception as e:
            if attempt == retries - 1:
                print(f"[ERROR] {url} -> {e}")
                return None
            await asyncio.sleep(0.8 * (attempt + 1))
    return None


async def fetch_bytes(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int,
    sem: asyncio.Semaphore,
    retries: int = RETRIES,
) -> Optional[bytes]:
    async with sem:
        for attempt in range(retries):
            try:
                async with session.get(
                    url,
                    headers=rh(),
                    proxy=SCRAPER_PROXY or None,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    if resp.status in (403, 429):
                        await asyncio.sleep(1.2 * (attempt + 1))
                        continue
                    return None
            except Exception:
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(0.7 * (attempt + 1))
    return None


# ------------------------------------------------------------------
# REBROWSER
# ------------------------------------------------------------------

async def fetch_rebrowser_async(name: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    print(f"Fetching {name} ({REBROWSER_DAYS} days)...")

    today = datetime.now(timezone.utc)
    date_strs = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(REBROWSER_DAYS)]
    urls = [cfg["url"] + ds + ".parquet" for ds in date_strs]

    sem = asyncio.Semaphore(REBROWSER_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=max(REBROWSER_CONCURRENCY * 2, 20), ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        blobs = await asyncio.gather(*[
            fetch_bytes(session, url, timeout=PARQUET_TIMEOUT, sem=sem, retries=RETRIES)
            for url in urls
        ])

    frames: List[pd.DataFrame] = []
    for blob in blobs:
        if not blob:
            continue
        try:
            frames.append(pd.read_parquet(io.BytesIO(blob)))
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=STANDARD_COLS)

    raw = pd.concat(frames, ignore_index=True)
    out = pd.DataFrame()

    for std, src in cfg["cols"].items():
        out[std] = raw[src].astype(str) if src in raw.columns else ""

    out["year"] = out["year"].apply(clean_year)
    out["make"] = out["make"].apply(cap)
    out["model"] = out["model"].apply(cap)
    out["trim"] = out["trim"].apply(cap)
    out["type"] = out["type"].apply(cap)
    out["damage"] = out["damage"].apply(cap)
    out["odometer"] = out["odometer"].apply(clean_odo)
    out["price"] = 0
    out["lot"] = out["lot"].astype(str).str.strip()
    out["vin"] = out["vin"].apply(lambda v: "" if str(v).strip() in ("[PREMIUM]", "nan", "None") else str(v).strip().upper())
    out["date"] = out["date"].apply(norm_date)
    out["location"] = out["location"].apply(cap)
    out["state"] = out["state"].astype(str).str.strip().str.upper()
    out["url"] = out["url"].apply(lambda v: "" if str(v).strip() == "[PREMIUM]" else str(v).strip())
    out["auction"] = cfg["auction"]
    out["source"] = name

    out = out[(out["year"] >= MIN_YEAR) & out["make"].ne("") & out["model"].ne("")]
    out = ensure_standard_cols(out)
    print(f"{name}: {len(out):,} rows")
    return out


# ------------------------------------------------------------------
# STAT.VIN
# ------------------------------------------------------------------

class StatVinClient:
    def __init__(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> None:
        self.session = session
        self.sem = sem

    async def lookup_price(self, vin: str, lot: str, auction: str) -> Tuple[int, str]:
        vin = (vin or "").strip().upper()
        lot = str(lot or "").strip()
        auction = (auction or "").strip().upper()

        urls: List[str] = []

        if vin and len(vin) == 17:
            urls.append(f"https://stat.vin/cars/{vin}")

        if lot:
            if auction == "COPART":
                urls.append(f"https://stat.vin/en/copart/{lot}")
            elif auction == "IAAI":
                urls.append(f"https://stat.vin/en/iaai/{lot}")

        for i, url in enumerate(urls, start=1):
            async with self.sem:
                html = await fetch_text(self.session, url, timeout=STATVIN_TIMEOUT, retries=RETRIES)
            if not html:
                continue

            if SAVE_STATVIN_DEBUG and i == 1:
                safe_key = vin or lot or "unknown"
                save_debug_html(f"statvin_{safe_key}", html)

            if not self._page_matches(html, vin=vin, lot=lot, auction=auction):
                continue

            price = self._e
