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

Pipeline:
1. Download recent Rebrowser Copart and IAAI parquet snapshots.
2. Normalize and combine them.
3. Try to resolve final prices from direct Stat.vin pages only.
4. Always write snapshot CSVs for the current run.
5. Write master.csv and cars.csv when possible.

Notes:
- This version avoids Stat.vin search endpoints because they are commonly blocked.
- It uses direct pages only:
    https://stat.vin/cars/<VIN>
    https://stat.vin/en/copart/<LOT>
    https://stat.vin/en/iaai/<LOT>
- It is written to be GitHub Actions safe from a syntax perspective:
  no multiline f-strings, no fragile string formatting.
"""

OUTPUT_CSV = "cars.csv"
MASTER_CSV = "master.csv"
INCOMING_CSV = "incoming_latest.csv"
UNRESOLVED_CSV = "statvin_unresolved.csv"
DEBUG_DIR = "debug"
LOOKUP_BATCH_STATE_FILE = os.environ.get("LOOKUP_BATCH_STATE_FILE", "statvin_lookup_state.json")

MIN_YEAR = 2012
MIN_PRICE = 200
REBROWSER_DAYS = int(os.environ.get("REBROWSER_DAYS", "30"))
REBROWSER_CONCURRENCY = int(os.environ.get("REBROWSER_CONCURRENCY", "10"))
STATVIN_CONCURRENCY = int(os.environ.get("STATVIN_CONCURRENCY", "2"))
STATVIN_LOOKUPS_PER_RUN = int(os.environ.get("STATVIN_LOOKUPS_PER_RUN", "500"))
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
    "location", "state", "source", "auction", "url",
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


def log(msg: str) -> None:
    print("  " + msg)


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


def save_debug_html(name: str, html: str) -> None:
    ensure_dir(DEBUG_DIR)
    Path(DEBUG_DIR, name + ".html").write_text(html[:500000], encoding="utf-8", errors="ignore")


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
    return str(int(float(m.group(1)))) + " " + unit


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
        return m.group(3) + "-" + m.group(1).zfill(2) + "-" + m.group(2).zfill(2)

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
    raw_idx = state.get("next_index", 0)
    idx = int(raw_idx) if str(raw_idx).isdigit() else 0
    idx = idx % len(items)
    out = items[idx:idx + batch_size]
    if len(out) < batch_size:
        out += items[: max(0, batch_size - len(out))]
    next_idx = (idx + batch_size) % len(items)
    save_json(state_file, {"next_index": next_idx, "updated_at": datetime.now(timezone.utc).isoformat()})
    return out


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
                        print("[BLOCKED] " + url)
                        return None
                    return text
                if resp.status in (403, 429):
                    print("[HTTP " + str(resp.status) + "] " + url)
                    await asyncio.sleep(1.2 * (attempt + 1))
                    continue
                print("[HTTP " + str(resp.status) + "] " + url)
                return None
        except Exception as e:
            if attempt == retries - 1:
                print("[ERROR] " + url + " -> " + str(e))
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


async def fetch_rebrowser_async(name: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    print("Fetching " + name + " (" + str(REBROWSER_DAYS) + " days)...")

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
    print(name + ": " + format(len(out), ",") + " rows")
    return out


class StatVinClient:
    def __init__(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> None:
        self.session = session
        self.sem = sem

    def _make_slug_from_vin_page_hint(self, vin: str) -> str:
        return ""

    async def lookup_price(self, vin: str, lot: str, auction: str, make_slug: str = "") -> Tuple[int, str]:
        vin = (vin or "").strip().upper()
        lot = str(lot or "").strip()
        auction = (auction or "").strip().upper()
        make_slug = (make_slug or "").strip().lower()

        urls: List[str] = []
        if vin and len(vin) == 17:
            if make_slug:
                urls.append("https://stat.vin/vin-decoding/" + make_slug + "/" + vin)
            urls.append("https://stat.vin/vin-decoding/" + vin)
        if lot:
            if auction == "COPART":
                urls.append("https://stat.vin/en/copart/" + lot)
            elif auction == "IAAI":
                urls.append("https://stat.vin/en/iaai/" + lot)

        for idx, url in enumerate(urls):
            async with self.sem:
                html = await fetch_text(self.session, url, timeout=STATVIN_TIMEOUT, retries=RETRIES)
            if not html:
                continue
            if SAVE_STATVIN_DEBUG and idx == 0:
                safe_key = vin or lot or "unknown"
                save_debug_html("statvin_" + safe_key, html)
            if not self._page_matches(html, vin, lot, auction):
                continue
            price = self._extract_price_from_html(html)
            if price > 0:
                return price, url

        return 0, ""

    def _page_matches(self, html: str, vin: str, lot: str, auction: str) -> bool:
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        upper = re.sub(r"\s+", " ", text).upper()
        if vin and vin in upper:
            return True
        if lot and lot in upper:
            return True
        if auction and auction in upper and "FINAL BID" in upper:
            return True
        return False

    def _extract_price_from_html(self, html: str) -> int:
        text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
        patterns = [
            r"Final\s*Bid\s*:?\s*\$?\s*([\d,]+)",
            r"Sale\s*Price\s*:?\s*\$?\s*([\d,]+)",
            r"Sold\s*for\s*:?\s*\$?\s*([\d,]+)",
            r"Winning\s*Bid\s*:?\s*\$?\s*([\d,]+)",
            r"Amount\s*:\s*\$?\s*([\d,]+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if not m:
                continue
            try:
                value = int(m.group(1).replace(",", ""))
                if value >= MIN_PRICE:
                    return value
            except Exception:
                pass
        return 0


async def enrich_from_statvin(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = ensure_standard_cols(df.copy())
    out["vin"] = out["vin"].astype(str).str.strip().str.upper()
    out["lot"] = out["lot"].astype(str).str.strip()
    out["price"] = to_int_price_series(out["price"])

    mask = (out["price"] == 0) & ((out["vin"].str.len() == 17) | out["lot"].str.len().ge(5))
    target_indexes = list(out[mask].index)
    if not target_indexes:
        return out

    run_indexes = rotated_slice(target_indexes, LOOKUP_BATCH_STATE_FILE, min(STATVIN_LOOKUPS_PER_RUN, len(target_indexes)))
    run_targets = out.loc[run_indexes].copy()

    log("Stat.vin lookup candidates available: " + format(len(target_indexes), ","))
    log("Stat.vin lookup batch this run: " + format(len(run_targets), ","))

    connector = aiohttp.TCPConnector(limit=max(STATVIN_CONCURRENCY * 2, 20), ttl_dns_cache=300)
    sem = asyncio.Semaphore(STATVIN_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        client = StatVinClient(session=session, sem=sem)

        async def one(idx: int, row: pd.Series) -> Tuple[int, int, str]:
            vin = str(row.get("vin", "")).strip().upper()
            lot = str(row.get("lot", "")).strip()
            auction = str(row.get("auction", "")).strip().upper()
            make = str(row.get("make", "")).strip().lower().replace(" ", "-")
            if make == "mercedes-benz":
                make = "mercedes-benz"
            price, matched_url = await client.lookup_price(vin=vin, lot=lot, auction=auction, make_slug=make)
            return idx, price, matched_url

        tasks = [one(idx, row) for idx, row in run_targets.iterrows()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    hit_count = 0
    unresolved_count = 0
    for res in results:
        if isinstance(res, Exception):
            unresolved_count += 1
            continue
        idx, price, matched_url = res
        if price > 0:
            out.at[idx, "price"] = int(price)
            out.at[idx, "url"] = matched_url
            hit_count += 1
        else:
            unresolved_count += 1

    log("Stat.vin prices recovered this run: " + format(hit_count, ","))
    log("Stat.vin unresolved this run: " + format(unresolved_count, ","))
    return out


def load_existing() -> pd.DataFrame:
    if os.path.exists(MASTER_CSV):
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
            df = ensure_standard_cols(df)
            log("Existing master: " + format(len(df), ",") + " rows")
            return df
        except Exception as e:
            log("Could not read master: " + str(e))
    return pd.DataFrame(columns=STANDARD_COLS)


def dedupe_combined(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return ensure_standard_cols(df)

    combined = normalize_dataframe(df)
    combined["_price_num"] = to_int_price_series(combined["price"])
    combined["_vin_len"] = combined["vin"].astype(str).str.len()
    combined["_lot_len"] = combined["lot"].astype(str).str.len()

    combined = combined.sort_values(
        by=["_price_num", "_vin_len", "_lot_len"],
        ascending=[False, False, False],
        kind="mergesort",
    )

    has_lot = combined[combined["lot"].astype(str).str.strip().ne("") & combined["lot"].astype(str).str.len().gt(3)]
    no_lot = combined[~combined.index.isin(has_lot.index)]
    has_lot = has_lot.drop_duplicates(subset=["lot"], keep="first")
    combined = pd.concat([has_lot, no_lot], ignore_index=True)

    has_vin = combined[combined["vin"].astype(str).str.strip().ne("") & combined["vin"].astype(str).str.len().eq(17)]
    no_vin = combined[~combined.index.isin(has_vin.index)]
    has_vin = has_vin.drop_duplicates(subset=["vin"], keep="first")
    combined = pd.concat([has_vin, no_vin], ignore_index=True)

    combined = combined.drop(columns=["_price_num", "_vin_len", "_lot_len"], errors="ignore")
    return ensure_standard_cols(combined)


def merge_preview(existing: pd.DataFrame, frames: List[pd.DataFrame]) -> Tuple[pd.DataFrame, int]:
    if frames:
        new_data = pd.concat(frames, ignore_index=True)
    else:
        new_data = pd.DataFrame(columns=STANDARD_COLS)

    new_data = normalize_dataframe(new_data)
    before_count = len(existing)
    combined = pd.concat([existing, new_data], ignore_index=True)
    combined = dedupe_combined(combined)
    actual_new = len(combined) - before_count
    return combined, actual_new


def write_snapshot_csvs(incoming: pd.DataFrame) -> None:
    incoming = ensure_standard_cols(incoming)
    incoming.to_csv(INCOMING_CSV, index=False)
    log("Written: " + INCOMING_CSV + " (" + format(len(incoming), ",") + " rows)")

    unresolved = incoming[to_int_price_series(incoming["price"]) == 0].copy()
    unresolved.to_csv(UNRESOLVED_CSV, index=False)
    log("Written: " + UNRESOLVED_CSV + " (" + format(len(unresolved), ",") + " unresolved rows)")


def write_outputs(combined: pd.DataFrame, incoming: pd.DataFrame) -> None:
    combined = ensure_standard_cols(combined)
    incoming = ensure_standard_cols(incoming)

    combined.to_csv(MASTER_CSV, index=False)
    log("Written: " + MASTER_CSV + " (" + format(len(combined), ",") + " rows)")

    priced = combined[to_int_price_series(combined["price"]) > 0].copy()
    site_cols = [
        "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state",
        "auction", "source", "url",
    ]
    for c in site_cols:
        if c not in priced.columns:
            priced[c] = ""
    priced[site_cols].to_csv(OUTPUT_CSV, index=False)
    log("Written: " + OUTPUT_CSV + " (" + format(len(priced), ",") + " priced rows)")

    write_snapshot_csvs(incoming)


def main() -> None:
    t0 = time.time()

    print("=" * 68)
    print("  Rebrowser -> Stat.vin Final Price Harvester")
    print("  Date              : " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    print("  Rebrowser days    : " + str(REBROWSER_DAYS))
    print("  Stat.vin batch    : " + str(STATVIN_LOOKUPS_PER_RUN))
    print("  Stat.vin concur   : " + str(STATVIN_CONCURRENCY))
    print("  Proxy             : " + ("enabled" if SCRAPER_PROXY else "disabled"))
    print("=" * 68)

    existing = load_existing()

    print("")
    print("[1/3] Rebrowser refresh - Copart")
    rb_copart = asyncio.run(fetch_rebrowser_async("Rebrowser-Copart", REBROWSER_SOURCES["Rebrowser-Copart"]))

    print("")
    print("[2/3] Rebrowser refresh - IAAI")
    rb_iaai = asyncio.run(fetch_rebrowser_async("Rebrowser-IAAI", REBROWSER_SOURCES["Rebrowser-IAAI"]))

    frames = [df for df in (rb_copart, rb_iaai) if not df.empty]
    incoming = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=STANDARD_COLS)
    incoming = ensure_standard_cols(incoming)

    print("")
    print("[3/3] Stat.vin final price enrichment")
    incoming = asyncio.run(enrich_from_statvin(incoming))

    combined, actual_new = merge_preview(existing, [incoming])

    print("")
    print("=" * 68)
    print("Actual new rows added this run: " + format(actual_new, ","))

    total = len(combined)
    with_price = int((to_int_price_series(combined["price"]) > 0).sum())
    print("Master total: " + format(total, ","))
    print("Rows with price: " + format(with_price, ","))
    print("Rows without price: " + format(total - with_price, ","))

    if not incoming.empty:
        for src, grp in incoming.groupby("source"):
            priced = int((to_int_price_series(grp["price"]) > 0).sum())
            print("Incoming " + str(src) + ": " + format(len(grp), ",") + " rows, " + format(priced, ",") + " priced")

    write_snapshot_csvs(incoming)

    if with_price < MIN_PRICED_ROWS_TO_WRITE:
        print("Too few priced rows found (" + str(with_price) + " < " + str(MIN_PRICED_ROWS_TO_WRITE) + "); not updating master.csv or cars.csv.")
        elapsed = int(time.time() - t0)
        print("")
        print("Done in " + str(elapsed) + "s (" + str(elapsed // 60) + "m " + str(elapsed % 60) + "s)")
        print("=" * 68)
        return

    if actual_new == 0:
        print("No new rows found in master merge. Snapshot CSVs were still written.")
        elapsed = int(time.time() - t0)
        print("")
        print("Done in " + str(elapsed) + "s (" + str(elapsed // 60) + "m " + str(elapsed % 60) + "s)")
        print("=" * 68)
        return

    write_outputs(combined, incoming)

    elapsed = int(time.time() - t0)
    print("")
    print("Done in " + str(elapsed) + "s (" + str(elapsed // 60) + "m " + str(elapsed % 60) + "s)")
    print("=" * 68)


if __name__ == "__main__":
    main()
