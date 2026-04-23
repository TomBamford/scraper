"""
Copart & IAAI Final Price Harvester
===================================
High-throughput, target-driven scraper designed to add roughly
2,000-5,000 NEW rows to the CSV on each run.

Features:
- Target-driven collection (actual deduped additions)
- Async concurrent AutoBidCar catalog scraping
- Async concurrent AutoBidCar detail scraping
- Async concurrent Rebrowser parquet downloads
- Periodic checkpoint CSV writes
- Rotating make order between runs
- Optional proxy support
- Optional Kaggle Manheim backfill
- Safe merge + dedupe before final write

Install:
    pip install aiohttp aiodns requests pandas pyarrow beautifulsoup4

Optional:
    pip install kaggle

Env vars:
    KAGGLE_USERNAME=...
    KAGGLE_KEY=...
    HTTP_PROXY=http://user:pass@host:port
    HTTPS_PROXY=http://user:pass@host:port
    SCRAPER_PROXY=http://user:pass@host:port   # optional override
    ROTATION_STATE_FILE=rotation_state.json    # optional
    ENABLE_KAGGLE=true                         # optional
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import random
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

OUTPUT_CSV = "cars.csv"
MASTER_CSV = "master.csv"

CHECKPOINT_DIR = "checkpoints"
CHECKPOINT_EVERY_NEW_ROWS = 500
ROTATION_STATE_FILE = os.environ.get("ROTATION_STATE_FILE", "rotation_state.json")

MIN_YEAR = 2012
MIN_PRICE = 200

TARGET_NEW_MIN = 2000
TARGET_NEW_MAX = 5000

MAX_PAGES_PER_MAKE = 220
MAX_EMPTY_PAGES_PER_MAKE = 5
CATALOG_BATCH_SIZE = 10

AUTO_CONCURRENCY = 20
DETAIL_CONCURRENCY = 12
REBROWSER_CONCURRENCY = 10

ENRICH_DETAILS = True
DETAILS_LIMIT_PER_RUN = 1200

REBROWSER_DAYS = 30
MAX_AUTOBIDCAR_PASSES = 4

CATALOG_TIMEOUT = 20
DETAIL_TIMEOUT = 20
PARQUET_TIMEOUT = 20
RETRIES = 3

ENABLE_KAGGLE = os.environ.get("ENABLE_KAGGLE", "").strip().lower() in {"1", "true", "yes", "on"}

MAKES_TO_SCRAPE = [
    "toyota", "honda", "chevrolet", "ford", "nissan",
    "hyundai", "kia", "jeep", "dodge", "bmw",
    "mercedes-benz", "volkswagen", "subaru", "mazda",
    "audi", "lexus", "acura", "mitsubishi", "infiniti", "volvo",
    "gmc", "cadillac", "chrysler", "buick", "lincoln",
    "mini", "porsche", "land-rover", "jaguar", "tesla",
    "ram", "fiat", "alfa-romeo", "genesis", "suzuki",
    "saab", "saturn", "pontiac", "scion", "mercury",
]

KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME", "")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY", "")

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


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"  {msg}")


def rh() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
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

    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    return ""


def state_from_location(loc: str) -> str:
    if not loc:
        return ""
    m = re.match(r"^([A-Z]{2})\s*[-–]", loc.strip())
    return m.group(1) if m else ""


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


def rotated_makes(makes: List[str], state_file: str) -> List[str]:
    state = load_json(state_file)
    idx = int(state.get("next_index", 0)) if str(state.get("next_index", "")).isdigit() else 0
    idx = idx % len(makes) if makes else 0
    rotated = makes[idx:] + makes[:idx]
    next_idx = (idx + 7) % len(makes) if makes else 0
    save_json(state_file, {"next_index": next_idx, "updated_at": datetime.now(timezone.utc).isoformat()})
    return rotated


def chunked(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ─────────────────────────────────────────────────────────────
# CHECKPOINTING
# ─────────────────────────────────────────────────────────────

class CheckpointManager:
    def __init__(self, existing: pd.DataFrame) -> None:
        self.existing = ensure_standard_cols(existing.copy())
        self.frames: List[pd.DataFrame] = []
        self.last_checkpoint_at = 0
        ensure_dir(CHECKPOINT_DIR)

    def add_frame(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        self.frames.append(ensure_standard_cols(df.copy()))

    def merge_preview(self) -> Tuple[pd.DataFrame, int]:
        if self.frames:
            new_data = pd.concat(self.frames, ignore_index=True)
        else:
            new_data = pd.DataFrame(columns=STANDARD_COLS)

        new_data = normalize_dataframe(new_data)
        before_count = len(self.existing)

        combined = pd.concat([self.existing, new_data], ignore_index=True)
        combined = dedupe_combined(combined)
        actual_new = len(combined) - before_count
        return combined, actual_new

    def maybe_checkpoint(self, force: bool = False) -> None:
        combined, actual_new = self.merge_preview()
        if not force and actual_new - self.last_checkpoint_at < CHECKPOINT_EVERY_NEW_ROWS:
            return

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        preview_path = Path(CHECKPOINT_DIR) / f"preview_{ts}.csv"
        delta_path = Path(CHECKPOINT_DIR) / f"delta_{ts}.csv"

        combined.to_csv(preview_path, index=False)

        if self.frames:
            delta = pd.concat(self.frames, ignore_index=True)
            delta = ensure_standard_cols(delta)
            delta.to_csv(delta_path, index=False)

        self.last_checkpoint_at = actual_new
        log(f"Checkpoint written at +{actual_new:,} actual new rows")


# ─────────────────────────────────────────────────────────────
# ASYNC HTTP
# ─────────────────────────────────────────────────────────────

async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int,
    retries: int = RETRIES,
) -> Optional[str]:
    for attempt in range(retries):
        try:
            async with session.get(
                url,
                headers=rh(),
                proxy=SCRAPER_PROXY or None,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    return await resp.text()

                if resp.status in (403, 429):
                    await asyncio.sleep(1.2 * (attempt + 1))
                    continue

                return None
        except Exception:
            if attempt == retries - 1:
                return None
            await asyncio.sleep(0.7 * (attempt + 1))
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


# ─────────────────────────────────────────────────────────────
# AUTOBIDCAR
# ─────────────────────────────────────────────────────────────

def parse_catalog_page(html: str, make: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    cars: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for card in soup.find_all("a", href=re.compile(r"^/car/")):
        try:
            href = card.get("href", "")
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)

            url = "https://autobidcar.com" + href
            vin_match = re.search(r"-([A-HJ-NPR-Z0-9]{17})$", href, re.IGNORECASE)
            vin = vin_match.group(1).upper() if vin_match else ""

            text = card.get_text(" ", strip=True)

            year_m = re.search(r"\b(20\d{2}|19\d{2})\b", text)
            year = int(year_m.group(1)) if year_m else 0
            if year < MIN_YEAR:
                continue

            h4 = card.find("h4")
            title = h4.get_text(strip=True) if h4 else ""

            lot_m = re.search(r"Lot\s*#?\s*(\d+)", text, re.IGNORECASE)
            lot = lot_m.group(1) if lot_m else ""

            price_m = re.search(r"Sold\s+For\s+\$\s*([\d,]+)", text, re.IGNORECASE)
            if not price_m:
                price_m = re.search(r"\$\s*([\d,]+)", text)
            price = int(price_m.group(1).replace(",", "")) if price_m else 0
            if price < MIN_PRICE:
                price = 0

            auction = ""
            upper = text.upper()
            if "COPART" in upper:
                auction = "COPART"
            elif "IAAI" in upper:
                auction = "IAAI"

            cars.append({
                "vin": vin,
                "url": url,
                "year": year,
                "title": title,
                "lot": lot,
                "price": price,
                "auction": auction,
                "make_slug": make,
            })
        except Exception:
            continue

    return cars


async def fetch_catalog_page(
    session: aiohttp.ClientSession,
    make: str,
    page_num: int,
    sem: asyncio.Semaphore,
) -> List[Dict[str, Any]]:
    base_url = f"https://autobidcar.com/catalog/{make}"
    url = base_url if page_num == 1 else f"{base_url}?page={page_num}"

    async with sem:
        html = await fetch_text(session, url, timeout=CATALOG_TIMEOUT, retries=RETRIES)
        if not html:
            return []
        return parse_catalog_page(html, make)


async def scrape_car_detail_async(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with sem:
        html = await fetch_text(session, url, timeout=DETAIL_TIMEOUT, retries=RETRIES)
        if not html:
            return {}

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        price = 0
        price_m = re.search(r"\|\s*\$\s*([\d,]+)", html)
        if price_m:
            price = int(price_m.group(1).replace(",", ""))

        if price < MIN_PRICE:
            p2 = re.search(r"(?:Price|Sold For)\s*[\n\r\s]*\$\s*([\d,]+)", text, re.IGNORECASE)
            if p2:
                price = int(p2.group(1).replace(",", ""))

        if price < MIN_PRICE:
            price = 0

        def extract_field(label: str) -> str:
            m = re.search(rf"{re.escape(label)}\s*[\n\r]+\s*([^\n\r]+)", text, re.IGNORECASE)
            return m.group(1).strip() if m else ""

        damage = extract_field("Primary Damage")
        odometer = extract_field("Odometer")
        location = extract_field("Location")
        title_type = extract_field("Document Type")
        sale_date = extract_field("Sale Date")

        auction = ""
        upper = text[:2500].upper()
        if "COPART" in upper:
            auction = "COPART"
        elif "IAAI" in upper:
            auction = "IAAI"

        lot_m = re.search(r"Lot\s*#?\s*(\d{5,})", text)
        lot = lot_m.group(1) if lot_m else ""

        h1 = soup.find("h1")
        h1_text = h1.get_text(strip=True) if h1 else ""

        year_m = re.search(r"\b(20\d{2}|19\d{2})\b", h1_text)
        year_detail = int(year_m.group(1)) if year_m else 0

        vin_m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", html)
        vin = vin_m.group(1).upper() if vin_m else ""

        return {
            "price": price,
            "damage": damage,
            "odometer": odometer,
            "location": location,
            "type": title_type,
            "date": norm_date(sale_date),
            "auction": auction,
            "lot": lot,
            "vin": vin,
            "year_detail": year_detail,
            "h1": h1_text,
        }


def build_autobidcar_record(
    make_slug: str,
    car: Dict[str, Any],
    detail: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    detail = detail or {}

    price = detail.get("price") or car.get("price", 0)
    lot = detail.get("lot") or car.get("lot", "")
    vin = (detail.get("vin") or car.get("vin", "")).upper()
    damage = detail.get("damage", "")
    odometer = detail.get("odometer", "")
    location = detail.get("location", "")
    title_type = detail.get("type", "")
    sale_date = detail.get("date", "")
    auction = detail.get("auction") or car.get("auction", "")
    h1 = detail.get("h1", car.get("title", ""))

    h1_clean = re.sub(r"\s*[-–]\s*[A-HJ-NPR-Z0-9]{17}.*$", "", h1, flags=re.IGNORECASE).strip()
    parts = h1_clean.upper().split()

    year_h = int(parts[0]) if parts and parts[0].isdigit() else car.get("year", 0)
    make_h = parts[1].title() if len(parts) > 1 else make_slug.title()
    model_h = parts[2].title() if len(parts) > 2 else ""
    trim_h = " ".join(p.title() for p in parts[3:]) if len(parts) > 3 else ""

    rec = {
        "year": year_h or car.get("year", 0),
        "make": make_h,
        "model": model_h,
        "trim": trim_h,
        "type": cap(title_type),
        "damage": cap(damage),
        "price": int(price or 0),
        "odometer": clean_odo(odometer),
        "lot": str(lot).strip(),
        "vin": str(vin).strip().upper(),
        "date": norm_date(sale_date),
        "location": cap(location),
        "state": state_from_location(location.upper() if location else ""),
        "source": "AutoBidCar",
        "auction": str(auction).strip().upper(),
        "url": car.get("url", ""),
    }
    return normalize_record(rec)


async def scrape_make_until_target(
    make: str,
    session: aiohttp.ClientSession,
    existing_vins: set[str],
    existing_lots: set[str],
    remaining_needed: int,
    hard_cap_remaining: int,
    detail_budget_remaining: List[int],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    empty_pages = 0
    page_num = 1

    catalog_sem = asyncio.Semaphore(AUTO_CONCURRENCY)
    detail_sem = asyncio.Semaphore(DETAIL_CONCURRENCY)

    while page_num <= MAX_PAGES_PER_MAKE:
        if len(records) >= hard_cap_remaining:
            break

        batch_end = min(page_num + CATALOG_BATCH_SIZE - 1, MAX_PAGES_PER_MAKE)
        tasks = [fetch_catalog_page(session, make, p, catalog_sem) for p in range(page_num, batch_end + 1)]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        batch_cars: List[Dict[str, Any]] = []
        for result in batch_results:
            if isinstance(result, list) and result:
                batch_cars.extend(result)

        if not batch_cars:
            empty_pages += (batch_end - page_num + 1)
            if empty_pages >= MAX_EMPTY_PAGES_PER_MAKE:
                break
            page_num = batch_end + 1
            continue

        seen_urls: set[str] = set()
        unique_new: List[Dict[str, Any]] = []

        for car in batch_cars:
            url = str(car.get("url", "")).strip()
            vin = str(car.get("vin", "")).strip().upper()
            lot = str(car.get("lot", "")).strip()

            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            if vin and vin in existing_vins:
                continue
            if lot and lot in existing_lots:
                continue

            unique_new.append(car)

        if not unique_new:
            empty_pages += 1
            if empty_pages >= MAX_EMPTY_PAGES_PER_MAKE:
                break
            page_num = batch_end + 1
            continue

        empty_pages = 0

        room_left = hard_cap_remaining - len(records)
        if room_left <= 0:
            break
        unique_new = unique_new[:room_left]

        detail_map: Dict[str, Dict[str, Any]] = {}
        if ENRICH_DETAILS and detail_budget_remaining[0] > 0:
            needs_detail = [
                c for c in unique_new
                if (not c.get("price")) or (not c.get("lot")) or (not c.get("vin"))
            ]
            if needs_detail:
                detail_budget = min(detail_budget_remaining[0], len(needs_detail))
                detail_tasks = [scrape_car_detail_async(session, c["url"], detail_sem) for c in needs_detail[:detail_budget]]
                detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

                for car, detail in zip(needs_detail[:detail_budget], detail_results):
                    detail_map[car["url"]] = detail if isinstance(detail, dict) else {}

                detail_budget_remaining[0] -= detail_budget

        for car in unique_new:
            rec = build_autobidcar_record(make, car, detail_map.get(car["url"], {}))
            records.append(rec)

            vin = rec.get("vin", "")
            lot = rec.get("lot", "")
            if vin:
                existing_vins.add(vin)
            if lot:
                existing_lots.add(lot)

            if len(records) >= hard_cap_remaining:
                break

        if len(records) >= remaining_needed:
            break

        page_num = batch_end + 1

    return records


async def scrape_autobidcar_targeted(
    makes_to_scrape: List[str],
    existing_vins: set[str],
    existing_lots: set[str],
    target_min: int,
    target_max: int,
    checkpoints: CheckpointManager,
) -> List[Dict[str, Any]]:
    connector = aiohttp.TCPConnector(limit=max(AUTO_CONCURRENCY + DETAIL_CONCURRENCY + 10, 40), ttl_dns_cache=300)
    all_records: List[Dict[str, Any]] = []
    detail_budget_remaining = [DETAILS_LIMIT_PER_RUN]

    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        await fetch_text(session, "https://autobidcar.com/", timeout=10, retries=2)

        for pass_num in range(1, MAX_AUTOBIDCAR_PASSES + 1):
            print(f"AutoBidCar pass {pass_num}...")
            made_progress = False

            for make in makes_to_scrape:
                if len(all_records) >= target_max:
                    break

                remaining_needed = max(0, target_min - len(all_records))
                hard_cap_remaining = max(0, target_max - len(all_records))
                if hard_cap_remaining <= 0:
                    break

                recs = await scrape_make_until_target(
                    make=make,
                    session=session,
                    existing_vins=existing_vins,
                    existing_lots=existing_lots,
                    remaining_needed=remaining_needed if remaining_needed > 0 else hard_cap_remaining,
                    hard_cap_remaining=hard_cap_remaining,
                    detail_budget_remaining=detail_budget_remaining,
                )

                if recs:
                    made_progress = True
                    df = pd.DataFrame([normalize_record(r) for r in recs])
                    checkpoints.add_frame(df)
                    checkpoints.maybe_checkpoint()

                print(f"{make}: +{len(recs)} new")
                all_records.extend(recs)
                print(f"AutoBidCar running total: {len(all_records)} new")

                if len(all_records) >= target_min:
                    break

            if len(all_records) >= target_min or not made_progress:
                break

    return all_records[:target_max]


# ─────────────────────────────────────────────────────────────
# REBROWSER
# ─────────────────────────────────────────────────────────────

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


def enrich_prices(df: pd.DataFrame, price_map: Dict[str, int]) -> pd.DataFrame:
    if df.empty or "vin" not in df.columns or "price" not in df.columns:
        return df

    out = df.copy()
    out["vin"] = out["vin"].astype(str).str.strip().str.upper()
    out["price"] = to_int_price_series(out["price"])
    mask = (out["price"] == 0) & out["vin"].ne("") & (out["vin"].str.len() == 17)
    out.loc[mask, "price"] = out.loc[mask, "vin"].map(price_map).fillna(0).astype(int)
    return out


# ─────────────────────────────────────────────────────────────
# KAGGLE
# ─────────────────────────────────────────────────────────────

def fetch_kaggle_manheim() -> pd.DataFrame:
    if not ENABLE_KAGGLE:
        log("Skipping Kaggle (ENABLE_KAGGLE not set)")
        return pd.DataFrame(columns=STANDARD_COLS)

    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        log("Skipping Kaggle (missing KAGGLE_USERNAME or KAGGLE_KEY)")
        return pd.DataFrame(columns=STANDARD_COLS)

    log("Fetching Kaggle Manheim dataset...")
    try:
        import kaggle  # type: ignore

        path = Path("/tmp/kgl_manheim")
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            "syedanwarafridi/vehicle-sales-data",
            path=str(path),
            unzip=True,
            quiet=True,
        )

        csv = next(path.glob("**/*.csv"), None)
        if not csv:
            return pd.DataFrame(columns=STANDARD_COLS)

        df = pd.read_csv(csv, dtype=str).fillna("")

        out = pd.DataFrame()
        out["year"] = df.get("year", "").apply(clean_year)
        out["make"] = df.get("make", "").apply(cap)
        out["model"] = df.get("model", "").apply(cap)
        out["trim"] = df.get("trim", "").apply(cap)
        out["type"] = ""
        out["damage"] = ""
        out["price"] = df.get("sellingprice", "0").apply(clean_price)
        out["odometer"] = df.get("odometer", "").apply(clean_odo)
        out["lot"] = df.get("vin", "").astype(str).str.strip()
        out["vin"] = df.get("vin", "").astype(str).str.strip().str.upper()
        out["date"] = df.get("saledate", "").apply(norm_date)
        out["location"] = df.get("state", "").astype(str).str.title()
        out["state"] = df.get("state", "").astype(str).str.strip().str.upper()
        out["source"] = "Kaggle-Manheim"
        out["auction"] = "MANHEIM"
        out["url"] = ""

        out = out[(out["price"] > 0) & (out["year"] >= MIN_YEAR)]
        out = ensure_standard_cols(out)
        log(f"Kaggle rows with price: {len(out):,}")
        return out
    except Exception as e:
        log(f"Kaggle error: {e}")
        return pd.DataFrame(columns=STANDARD_COLS)


# ─────────────────────────────────────────────────────────────
# DEDUPE / MERGE / WRITE
# ─────────────────────────────────────────────────────────────

def load_existing() -> Tuple[pd.DataFrame, set[str], set[str]]:
    if os.path.exists(MASTER_CSV):
        try:
            df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
            df = ensure_standard_cols(df)

            vins = set(df["vin"].astype(str).str.strip().str.upper())
            lots = set(df["lot"].astype(str).str.strip())
            vins.discard("")
            lots.discard("")

            log(f"Existing master: {len(df):,} rows, {len(vins):,} VINs, {len(lots):,} lots")
            return df, vins, lots
        except Exception as e:
            log(f"Could not read master: {e}")

    return pd.DataFrame(columns=STANDARD_COLS), set(), set()


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


def write_outputs(combined: pd.DataFrame) -> None:
    combined = ensure_standard_cols(combined)
    combined.to_csv(MASTER_CSV, index=False)
    log(f"Written: {MASTER_CSV} ({len(combined):,} rows)")

    priced = combined[to_int_price_series(combined["price"]) > 0].copy()
    site_cols = [
        "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state",
        "auction", "source", "url"
    ]
    for c in site_cols:
        if c not in priced.columns:
            priced[c] = ""
    priced[site_cols].to_csv(OUTPUT_CSV, index=False)
    log(f"Written: {OUTPUT_CSV} ({len(priced):,} priced rows)")


def build_price_map_from_frames(frames: List[pd.DataFrame]) -> Dict[str, int]:
    price_map: Dict[str, int] = {}
    for df in frames:
        if df.empty:
            continue
        subset = ensure_standard_cols(df.copy())
        subset["vin"] = subset["vin"].astype(str).str.strip().str.upper()
        subset["price"] = to_int_price_series(subset["price"])

        for _, row in subset.iterrows():
            vin = row["vin"]
            price = int(row["price"])
            if vin and len(vin) == 17 and price > 0:
                if vin not in price_map or price > price_map[vin]:
                    price_map[vin] = price
    return price_map


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    t0 = time.time()

    print("=" * 68)
    print("  Copart & IAAI Final Price Harvester")
    print(f"  Date         : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Target new   : {TARGET_NEW_MIN}-{TARGET_NEW_MAX}")
    print(f"  Proxy        : {'enabled' if SCRAPER_PROXY else 'disabled'}")
    print(f"  Kaggle       : {'enabled' if ENABLE_KAGGLE else 'disabled'}")
    print("=" * 68)

    existing, existing_vins, existing_lots = load_existing()
    checkpoints = CheckpointManager(existing)
    all_frames: List[pd.DataFrame] = []

    ordered_makes = rotated_makes(MAKES_TO_SCRAPE, ROTATION_STATE_FILE)
    log(f"Rotated make order starts with: {', '.join(ordered_makes[:8])}")

    print("\n[1/4] AutoBidCar — targeted collection")
    abc_records = asyncio.run(
        scrape_autobidcar_targeted(
            makes_to_scrape=ordered_makes,
            existing_vins=existing_vins,
            existing_lots=existing_lots,
            target_min=TARGET_NEW_MIN,
            target_max=TARGET_NEW_MAX,
            checkpoints=checkpoints,
        )
    )

    if abc_records:
        abc_df = pd.DataFrame([normalize_record(r) for r in abc_records])
        abc_df = ensure_standard_cols(abc_df)
        all_frames.append(abc_df)
        checkpoints.add_frame(abc_df)
        checkpoints.maybe_checkpoint(force=True)

        abc_priced = (to_int_price_series(abc_df["price"]) > 0).sum()
        log(f"AutoBidCar collected: {len(abc_df):,} rows, {abc_priced:,} priced")
    else:
        log("AutoBidCar collected no rows")

    combined_preview, actual_new_preview = merge_preview(existing, all_frames)
    print(f"After AutoBidCar merge preview: +{actual_new_preview:,} actual new rows")

    if actual_new_preview < TARGET_NEW_MIN:
        print("\n[2/4] Rebrowser — Copart")
        rb_copart = asyncio.run(fetch_rebrowser_async("Rebrowser-Copart", REBROWSER_SOURCES["Rebrowser-Copart"]))

        if not rb_copart.empty:
            price_map = build_price_map_from_frames(all_frames)
            rb_copart = enrich_prices(rb_copart, price_map)
            all_frames.append(rb_copart)
            checkpoints.add_frame(rb_copart)
            checkpoints.maybe_checkpoint(force=True)

    if actual_new_preview < TARGET_NEW_MIN:
        print("\n[3/4] Rebrowser — IAAI")
        rb_iaai = asyncio.run(fetch_rebrowser_async("Rebrowser-IAAI", REBROWSER_SOURCES["Rebrowser-IAAI"]))

        if not rb_iaai.empty:
            price_map = build_price_map_from_frames(all_frames)
            rb_iaai = enrich_prices(rb_iaai, price_map)
            all_frames.append(rb_iaai)
            checkpoints.add_frame(rb_iaai)
            checkpoints.maybe_checkpoint(force=True)

        combined_preview, actual_new_preview = merge_preview(existing, all_frames)
        print(f"After Rebrowser merge preview: +{actual_new_preview:,} actual new rows")

    if actual_new_preview < TARGET_NEW_MIN:
        print("\n[4/4] Kaggle — Manheim")
        manheim = fetch_kaggle_manheim()
        if not manheim.empty:
            all_frames.append(manheim)
            checkpoints.add_frame(manheim)
            checkpoints.maybe_checkpoint(force=True)
            combined_preview, actual_new_preview = merge_preview(existing, all_frames)
            print(f"After Kaggle merge preview: +{actual_new_preview:,} actual new rows")
        else:
            log("Kaggle contributed no rows")

    combined, actual_new = merge_preview(existing, all_frames)

    print("\n" + "=" * 68)
    print(f"Actual new rows added this run: {actual_new:,}")

    if actual_new == 0:
        print("No new rows found. Nothing written.")
        elapsed = int(time.time() - t0)
        print(f"\nDone in {elapsed}s ({elapsed // 60}m {elapsed % 60}s)")
        print("=" * 68)
        return

    if actual_new < TARGET_NEW_MIN:
        print(f"Warning: target minimum not reached. Added {actual_new:,}, target min is {TARGET_NEW_MIN:,}.")
    elif actual_new > TARGET_NEW_MAX:
        print(f"Warning: final deduped additions exceed target max ({actual_new:,} > {TARGET_NEW_MAX:,}).")

    total = len(combined)
    with_price = (to_int_price_series(combined["price"]) > 0).sum()
    print(f"Master total: {total:,}")
    print(f"Rows with price: {with_price:,}")
    print(f"Rows without price: {total - with_price:,}")

    if all_frames:
        incoming = pd.concat(all_frames, ignore_index=True)
        incoming = ensure_standard_cols(incoming)
        for src, grp in incoming.groupby("source"):
            priced = (to_int_price_series(grp["price"]) > 0).sum()
            print(f"Incoming {src}: {len(grp):,} rows, {priced:,} priced")

    write_outputs(combined)
    checkpoints.maybe_checkpoint(force=True)

    elapsed = int(time.time() - t0)
    print(f"\nDone in {elapsed}s ({elapsed // 60}m {elapsed % 60}s)")
    print("=" * 68)
    print("\nCommit cars.csv, master.csv, checkpoints/, and rotation_state.json as needed.")


if __name__ == "__main__":
    main()
