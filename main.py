"""
Salvage auction history scraper — Poctra.com
============================================
Poctra archives Copart & IAAI US sales since 2017.
No login required. No API. Free public browse.

URL patterns:
  Make list:    https://poctra.com/makes
  Model list:   https://poctra.com/{MAKE}-for-sale-1   (e.g. TOYT-for-sale-1)
  Model page:   https://poctra.com/{MAKE}-{MODEL}-for-sale-{N}
  Detail page:  https://poctra.com/car/{LOT_OR_ID}/{SLUG}

Run:
    pip install playwright pandas
    playwright install chromium
    python scraper_poctra.py
"""

import asyncio
import os
import re
import time
import random

import pandas as pd
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://poctra.com"

YEAR_MIN = 2008
YEAR_MAX = 2015
MIN_PRICE = 500

MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

RESUME_FROM_MASTER = True

# How many listing pages to crawl per make/model combo (each page ~20 cars)
MAX_LISTING_PAGES = 50

MODEL_WORKERS = 3
DETAIL_WORKERS = 5

NAV_TIMEOUT_MS = 30_000
MAX_RETRIES = 3
RETRY_DELAY_MS = 1_500

# Leave empty to scrape all; populate to narrow scope
TARGET_MAKES: set = set()   # e.g. {"TOYT", "HOND"}
TARGET_MODELS: set = set()  # e.g. {"CAMRY", "CIVIC"}

EXCLUDED_MAKES = {"LAND ROVER", "LNDR"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DAMAGE_PATTERNS = [
    "FRONT END", "REAR END", "SIDE", "ROLLOVER", "ALL OVER",
    "WATER/FLOOD", "FLOOD", "MECHANICAL", "MINOR DENT", "NORMAL WEAR",
    "UNDERCARRIAGE", "STRIPPED", "BURN", "TOP/ROOF", "HAIL",
    "VANDALISM", "BIOHAZARD", "FIRE",
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def nw(text: str) -> str:
    """Normalize whitespace."""
    return re.sub(r"\s+", " ", str(text)).strip()


def clean_price(text) -> int:
    s = str(text).replace("$", "").replace(",", "").strip()
    if not s or any(x in s.lower() for x in ["no sale", "not sold", "n/a"]):
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return int(float(m.group(1))) if m else 0


def clean_odometer(text: str) -> str:
    s = str(text).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m:
        return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"


def year_in_range(y) -> bool:
    try:
        return YEAR_MIN <= int(str(y).strip()) <= YEAR_MAX
    except Exception:
        return False


def infer_type(text: str) -> str:
    t = text.upper()
    if any(x in t for x in ["PICKUP", "F-150", "F150", "SILVERADO", "RAM 1", "TUNDRA",
                              "RANGER", "TACOMA", "FRONTIER", "COLORADO", "CANYON"]):
        return "Truck"
    if any(x in t for x in ["SUV", "EXPLORER", "ESCAPE", "ROGUE", "EQUINOX", "TAHOE",
                              "SUBURBAN", "RAV4", "CR-V", "CX-5", "XC90", "SORENTO",
                              "SPORTAGE", "PILOT", "HIGHLANDER", "PATHFINDER", "TRAVERSE",
                              "EXPEDITION", "4RUNNER", "SANTA FE", "TUCSON", "OUTLANDER"]):
        return "SUV"
    if any(x in t for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "BRZ", "86"]):
        return "Coupe"
    if any(x in t for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA", "CARAVAN",
                              "ECONOLINE", "EXPRESS", "SAVANA", "MINIVAN"]):
        return "Van"
    return "Sedan"


def parse_location_state(raw: str):
    raw = str(raw).strip()
    for sep in [", ", " - ", " – "]:
        if sep in raw:
            parts = raw.rsplit(sep, 1)
            if len(parts) == 2 and len(parts[1]) == 2 and parts[1].isalpha():
                return parts[0].strip(), parts[1].strip().upper()
    m = re.match(r"^(.*?)\s+([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return raw, ""


def load_existing_keys(path=MASTER_CSV):
    if not RESUME_FROM_MASTER or not os.path.exists(path):
        return set()
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        if "vin" not in df.columns:
            df["vin"] = ""
        if "lot" not in df.columns:
            df["lot"] = ""
        keys = set((df["vin"].str.strip() + "|" + df["lot"].str.strip()).tolist())
        keys.discard("|")
        return keys
    except Exception as e:
        print(f"[WARN] Could not load existing keys: {e}")
        return set()


def build_record(raw: dict, existing_keys: set, seen_keys: set, stats: dict):
    price = clean_price(raw.get("price", ""))
    if price < MIN_PRICE:
        stats["price_below_min"] += 1
        return None

    year = str(raw.get("year", "")).strip()
    make = str(raw.get("make", "")).strip()
    model = str(raw.get("model", "")).strip()

    if not year_in_range(year):
        stats["year_out_of_range"] += 1
        return None

    if not make or not model:
        stats["incomplete"] += 1
        return None

    if make.upper() in EXCLUDED_MAKES:
        stats["excluded_make"] += 1
        return None

    vin = str(raw.get("vin", "")).strip().upper()
    lot = str(raw.get("lot", "")).strip()

    if not vin and not lot:
        stats["incomplete"] += 1
        return None

    key = f"{vin}|{lot}"
    if key in seen_keys:
        stats["duplicate"] += 1
        return None
    if key in existing_keys:
        stats["existing"] += 1
        return None

    loc_raw = str(raw.get("location", "")).strip()
    state = str(raw.get("state", "")).strip()
    if loc_raw and not state:
        loc_raw, state = parse_location_state(loc_raw)

    record = {
        "vin": vin,
        "year": year,
        "make": make.title(),
        "model": model.title(),
        "trim": str(raw.get("trim", "")).strip(),
        "type": infer_type(f"{make} {model} {raw.get('trim', '')}"),
        "damage": str(raw.get("damage", "")).strip(),
        "price": price,
        "odometer": clean_odometer(raw.get("odometer", "")),
        "lot": lot,
        "date": str(raw.get("date", "")).strip(),
        "location": loc_raw,
        "state": state.upper(),
        "source": str(raw.get("source", "Poctra/Copart-IAAI")).strip(),
        "url": str(raw.get("url", "")).strip(),
    }

    seen_keys.add(key)
    existing_keys.add(key)
    stats["kept"] += 1
    return record


# ─────────────────────────────────────────────
# BROWSER HELPERS
# ─────────────────────────────────────────────
async def new_context(browser):
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    # Block images/fonts/media to speed up
    await ctx.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in {"image", "font", "media", "stylesheet"}
        else route.continue_()
    )
    return ctx


async def goto_resilient(page, url: str, wait_until="domcontentloaded"):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, wait_until=wait_until, timeout=NAV_TIMEOUT_MS)
            await page.wait_for_timeout(random.randint(400, 900))
            return True
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"[ERROR] gave up on {url}: {e}")
                return False
            delay = RETRY_DELAY_MS * attempt + random.randint(200, 600)
            print(f"[WARN] retry {attempt}/{MAX_RETRIES} for {url}")
            await page.wait_for_timeout(delay)
    return False


async def safe_text(loc) -> str:
    try:
        return nw(await loc.inner_text(timeout=4000))
    except Exception:
        return ""


# ─────────────────────────────────────────────
# STEP 1: Discover make codes from /makes page
# ─────────────────────────────────────────────
async def discover_makes(browser) -> list[dict]:
    """
    Returns list of dicts: {"code": "TOYT", "name": "Toyota", "url": "..."}
    Poctra's /makes page has links like /TOYT-for-sale-1
    """
    ctx = await new_context(browser)
    page = await ctx.new_page()
    makes = []
    try:
        ok = await goto_resilient(page, f"{BASE_URL}/makes")
        if not ok:
            return []
        # Each make is an <a> with href like /TOYT-for-sale-1
        links = await page.locator("a[href]").all()
        for link in links:
            try:
                href = await link.get_attribute("href") or ""
                m = re.match(r"^/([A-Z0-9]{2,6})-for-sale-\d+$", href)
                if m:
                    code = m.group(1)
                    name = nw(await link.inner_text())
                    makes.append({
                        "code": code,
                        "name": name or code,
                        "url": BASE_URL + href,
                    })
            except Exception:
                pass
        # Deduplicate by code
        seen = set()
        unique = []
        for mk in makes:
            if mk["code"] not in seen:
                seen.add(mk["code"])
                unique.append(mk)
        print(f"[makes] Found {len(unique)} make codes")
        return unique
    finally:
        await page.close()
        await ctx.close()


# ─────────────────────────────────────────────
# STEP 2: Discover model URLs for each make
# ─────────────────────────────────────────────
async def discover_models_for_make(page, make: dict) -> list[str]:
    """
    From the make's first listing page, find model sub-pages.
    Poctra sidebar/nav has links like /TOYT-CAMRY-for-sale-1
    """
    ok = await goto_resilient(page, f"{BASE_URL}/{make['code']}-for-sale-1")
    if not ok:
        return []

    model_urls = set()
    links = await page.locator("a[href]").all()
    for link in links:
        try:
            href = await link.get_attribute("href") or ""
            # Model page pattern: /MAKE-MODEL-for-sale-N (make code is known)
            pattern = rf"^/({re.escape(make['code'])}-[A-Z0-9_\-]+-for-sale)-\d+$"
            m = re.match(pattern, href, re.IGNORECASE)
            if m:
                base = m.group(1)
                model_urls.add(f"{BASE_URL}/{base}-1")
        except Exception:
            pass

    # Also include the make-level URL itself as a fallback
    model_urls.add(f"{BASE_URL}/{make['code']}-for-sale-1")
    return sorted(model_urls)


# ─────────────────────────────────────────────
# STEP 3: Collect detail page URLs from listing pages
# ─────────────────────────────────────────────
async def collect_detail_urls_from_listing(page, listing_base_url: str) -> list[str]:
    """
    Paginate through listing pages and collect detail URLs.
    Detail URL pattern: /car/{id}/{slug}
    """
    detail_urls = []
    seen = set()
    found_out_of_range = 0  # if we see many out-of-range years, stop early

    for page_num in range(1, MAX_LISTING_PAGES + 1):
        url = re.sub(r"-for-sale-\d+$", f"-for-sale-{page_num}", listing_base_url)
        ok = await goto_resilient(page, url)
        if not ok:
            break

        # Check if page actually has content (Poctra returns 200 for empty pages)
        body = await safe_text(page.locator("body"))
        if "no vehicles" in body.lower() or "no results" in body.lower():
            break

        links = await page.locator("a[href]").all()
        page_details = []
        page_years = []

        for link in links:
            try:
                href = await link.get_attribute("href") or ""
                # Detail: /car/12345678/2012-toyota-camry or similar
                if re.match(r"^/car/[^/]+/", href):
                    full = BASE_URL + href if href.startswith("/") else href
                    if full not in seen:
                        seen.add(full)
                        # Extract year from URL slug
                        yr_m = re.search(r"/((?:19|20)\d{2})-", href)
                        if yr_m:
                            yr = int(yr_m.group(1))
                            page_years.append(yr)
                            if year_in_range(yr):
                                page_details.append(full)
                        else:
                            # No year in URL — keep it, will filter at detail stage
                            page_details.append(full)
            except Exception:
                pass

        detail_urls.extend(page_details)

        # Early stop: if we've gone past our year range
        if page_years:
            min_yr = min(page_years)
            if min_yr > YEAR_MAX:
                # Poctra lists newest first — once all on page are too new keep going
                pass
            if min_yr < YEAR_MIN and max(page_years) < YEAR_MIN:
                print(f"  [listing] all years < {YEAR_MIN} on page {page_num}, stopping")
                break

        if not page_details:
            break

        print(f"  [listing] {url} -> {len(page_details)} detail URLs (total {len(detail_urls)})")

    return detail_urls


# ─────────────────────────────────────────────
# STEP 4: Scrape a single detail page
# ─────────────────────────────────────────────
async def scrape_detail(page, url: str) -> dict | None:
    ok = await goto_resilient(page, url)
    if not ok:
        return None

    # Wait for main content
    try:
        await page.wait_for_selector("h1, .car-title, .vehicle-title, table", timeout=8000)
    except Exception:
        pass

    body = await safe_text(page.locator("body"))
    if not body:
        return None

    raw = {
        "url": url, "title": "", "year": "", "make": "", "model": "",
        "trim": "", "damage": "", "price": "", "odometer": "", "lot": "",
        "date": "", "location": "", "state": "", "source": "", "vin": "",
    }

    # ── Title ──────────────────────────────────────────────────────
    for sel in ["h1", ".car-title", ".vehicle-title", "title"]:
        try:
            if sel == "title":
                t = await page.title()
            else:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    t = await safe_text(loc)
                else:
                    continue
            if t:
                raw["title"] = nw(t)
                break
        except Exception:
            pass

    # ── Year from URL (most reliable on Poctra) ────────────────────
    yr_url = re.search(r"/((?:19|20)\d{2})-", url)
    if yr_url:
        raw["year"] = yr_url.group(1)

    # ── Year/Make/Model from title ─────────────────────────────────
    title = raw["title"]
    title_m = re.match(r"(\d{4})\s+(\w[\w\s-]*)", title)
    if title_m:
        if not raw["year"]:
            raw["year"] = title_m.group(1)
        rest = title_m.group(2).strip()
        parts = rest.split()
        if parts:
            raw["make"] = parts[0]
        if len(parts) > 1:
            raw["model"] = " ".join(parts[1:3])  # model is usually 1-2 words

    # ── Structured field extraction via Poctra's label:value pattern ─
    # Poctra detail pages use a definition table with rows like:
    #   <td class="label">VIN</td><td class="value">1HGCM82633A123456</td>
    # OR a two-column <table> with alternating label/value cells.
    field_map = {
        # label text (upper) -> raw dict key
        "VIN": "vin",
        "VIN NUMBER": "vin",
        "LOT": "lot",
        "LOT #": "lot",
        "LOT NUMBER": "lot",
        "STOCK #": "lot",
        "ODOMETER": "odometer",
        "MILEAGE": "odometer",
        "MILES": "odometer",
        "PRIMARY DAMAGE": "damage",
        "DAMAGE": "damage",
        "SECONDARY DAMAGE": "trim",   # repurpose trim field if needed
        "SALE DATE": "date",
        "AUCTION DATE": "date",
        "DATE": "date",
        "LOCATION": "location",
        "YARD": "location",
        "AUCTION": "source",
        "SOURCE": "source",
        "TRIM": "trim",
        "SERIES": "trim",
        "MAKE": "make",
        "MODEL": "model",
        "YEAR": "year",
        "LAST BID": "price",
        "FINAL BID": "price",
        "SOLD FOR": "price",
        "SALE PRICE": "price",
        "BID": "price",
        "WINNING BID": "price",
        "HAMMER PRICE": "price",
        "PRICE": "price",
    }

    # Try to read all table rows
    try:
        rows = await page.locator("tr").all()
        for row in rows:
            cells = await row.locator("td, th").all()
            if len(cells) >= 2:
                label = nw(await safe_text(cells[0])).upper().rstrip(":")
                value = nw(await safe_text(cells[1]))
                if label in field_map and value:
                    key = field_map[label]
                    if not raw.get(key):
                        raw[key] = value
    except Exception:
        pass

    # Try dl/dt/dd pattern
    try:
        dts = await page.locator("dt").all()
        dds = await page.locator("dd").all()
        for dt, dd in zip(dts, dds):
            label = nw(await safe_text(dt)).upper().rstrip(":")
            value = nw(await safe_text(dd))
            if label in field_map and value:
                key = field_map[label]
                if not raw.get(key):
                    raw[key] = value
    except Exception:
        pass

    # Try div/span label:value pairs (common in card-style layouts)
    try:
        items = await page.locator(
            ".detail-item, .info-item, .spec-item, .car-spec, .vehicle-spec, [class*='detail']"
        ).all()
        for item in items:
            txt = nw(await safe_text(item))
            if ":" in txt:
                parts = txt.split(":", 1)
                label = parts[0].strip().upper()
                value = parts[1].strip()
                if label in field_map and value:
                    key = field_map[label]
                    if not raw.get(key):
                        raw[key] = value
    except Exception:
        pass

    # ── Regex fallbacks on full body text ─────────────────────────

    if not raw["vin"]:
        m = re.search(r"(?:VIN)[^\w]*([A-HJ-NPR-Z0-9]{17})\b", body, re.IGNORECASE)
        if m:
            raw["vin"] = m.group(1).upper()
        else:
            m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", body)
            if m:
                raw["vin"] = m.group(1).upper()

    if not raw["lot"]:
        m = re.search(r"(?:Lot|LOT|Stock)[^\w#]*[#]?\s*([A-Z0-9\-]{5,})", body, re.IGNORECASE)
        if m:
            raw["lot"] = m.group(1)

    if not raw["lot"]:
        # Extract from URL: /car/12345678/slug → lot = 12345678
        m = re.search(r"/car/([^/]+)/", url)
        if m:
            raw["lot"] = m.group(1)

    if not raw["price"]:
        for pattern in [
            r"\bLast\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bFinal\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bSold\s+(?:for|price)?\b[^\d$]*\$?\s*([\d,]+)",
            r"\bSale\s+Price\b[^\d$]*\$?\s*([\d,]+)",
            r"\bWinning\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bHammer\b[^\d$]*\$?\s*([\d,]+)",
            r"\$([\d,]{3,})\b",
        ]:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                p = clean_price(m.group(1))
                if p > 0:
                    raw["price"] = str(p)
                    break

    if not raw["odometer"]:
        m = re.search(r"(\d[\d,]*)\s*(?:miles?|mi|km)", body, re.IGNORECASE)
        if m:
            raw["odometer"] = m.group(0).strip()

    if not raw["damage"]:
        body_up = body.upper()
        for dmg in DAMAGE_PATTERNS:
            if dmg in body_up:
                raw["damage"] = dmg.title()
                break

    if not raw["date"]:
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", body)
        if m:
            raw["date"] = m.group(1)
        else:
            m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", body)
            if m:
                raw["date"] = m.group(1)

    if not raw["source"]:
        b_lower = body.lower()
        if "copart" in b_lower:
            raw["source"] = "Copart"
        elif "iaai" in b_lower or "insurance auto" in b_lower:
            raw["source"] = "IAAI"

    if not raw["year"]:
        m = re.search(r"\b((?:19|20)\d{2})\b", body)
        if m:
            raw["year"] = m.group(1)

    return raw


# ─────────────────────────────────────────────
# WORKERS
# ─────────────────────────────────────────────
async def model_worker(worker_id, browser, in_q, detail_q, detail_seen, detail_lock):
    """Reads model listing URLs, paginates them, feeds detail URLs to detail_q."""
    ctx = await new_context(browser)
    page = await ctx.new_page()
    try:
        while True:
            item = await in_q.get()
            if item is None:
                in_q.task_done()
                break
            listing_url = item
            try:
                urls = await collect_detail_urls_from_listing(page, listing_url)
                added = 0
                async with detail_lock:
                    for u in urls:
                        if u not in detail_seen:
                            detail_seen.add(u)
                            await detail_q.put(u)
                            added += 1
                print(f"[model W{worker_id}] {listing_url} → +{added} detail URLs")
            except Exception as e:
                print(f"[model W{worker_id}] error for {listing_url}: {e}")
            finally:
                in_q.task_done()
    finally:
        await page.close()
        await ctx.close()


async def detail_worker(worker_id, browser, detail_q, results, results_lock,
                        existing_keys, seen_keys, stats):
    """Reads detail URLs, scrapes them, appends valid records to results."""
    ctx = await new_context(browser)
    page = await ctx.new_page()
    try:
        while True:
            url = await detail_q.get()
            if url is None:
                detail_q.task_done()
                break
            try:
                raw = await scrape_detail(page, url)
                if raw:
                    async with results_lock:
                        rec = build_record(raw, existing_keys, seen_keys, stats)
                        if rec:
                            results.append(rec)
                            if len(results) % 50 == 0:
                                print(f"[detail W{worker_id}] ✓ kept {len(results)} records so far")
            except Exception as e:
                print(f"[detail W{worker_id}] error for {url}: {e}")
            finally:
                detail_q.task_done()
    finally:
        await page.close()
        await ctx.close()


# ─────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────
def write_csvs(all_data: list) -> int:
    internal_cols = [
        "vin", "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state", "source", "url"
    ]
    website_cols = [
        "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state", "url"
    ]

    if os.path.exists(MASTER_CSV):
        master_df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
    else:
        master_df = pd.DataFrame(columns=internal_cols)

    if all_data:
        new_df = pd.DataFrame(all_data)
        for col in internal_cols:
            if col not in new_df.columns:
                new_df[col] = ""
        new_df = new_df[internal_cols]
        new_df["price"] = pd.to_numeric(new_df["price"], errors="coerce").fillna(0).astype(int)
        master_df = pd.concat([master_df, new_df], ignore_index=True)

    if not master_df.empty:
        master_df["price"] = pd.to_numeric(master_df["price"], errors="coerce").fillna(0).astype(int)
        master_df["year_num"] = pd.to_numeric(master_df["year"], errors="coerce")
        master_df = master_df[master_df["price"] >= MIN_PRICE]
        master_df = master_df[master_df["year_num"].between(YEAR_MIN, YEAR_MAX, inclusive="both")]
        make_col = master_df.get("make", pd.Series(dtype=str)).str.upper()
        master_df = master_df[~make_col.isin(EXCLUDED_MAKES)]
        master_df = master_df.drop_duplicates(subset=["vin", "lot"], keep="first")
        master_df = master_df.drop(columns=["year_num"])

    master_df.to_csv(MASTER_CSV, index=False)

    website_df = master_df.copy()
    for col in website_cols:
        if col not in website_df.columns:
            website_df[col] = ""
    website_df = website_df.reindex(columns=website_cols, fill_value="")
    website_df.to_csv(WEBSITE_CSV, index=False)

    return len(master_df)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    t0 = time.time()
    existing_keys = load_existing_keys()
    seen_keys: set = set()
    stats = {k: 0 for k in [
        "price_below_min", "year_out_of_range", "excluded_make",
        "duplicate", "existing", "incomplete", "kept"
    ]}

    print(f"Year range:       {YEAR_MIN}–{YEAR_MAX}")
    print(f"Min price:        ${MIN_PRICE}")
    print(f"Resume from CSV:  {RESUME_FROM_MASTER}")
    print(f"Existing records: {len(existing_keys)}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        # ── 1. Discover makes ───────────────────────────────────────
        all_makes = await discover_makes(browser)
        if TARGET_MAKES:
            all_makes = [m for m in all_makes if m["code"].upper() in TARGET_MAKES]
        print(f"Makes to process: {len(all_makes)}")

        # ── 2. Discover model listing URLs ──────────────────────────
        model_listing_urls: list[str] = []
        ctx = await new_context(browser)
        page = await ctx.new_page()
        try:
            for mk in all_makes:
                models = await discover_models_for_make(page, mk)
                if TARGET_MODELS:
                    models = [
                        u for u in models
                        if any(tm in u.upper() for tm in TARGET_MODELS)
                    ]
                model_listing_urls.extend(models)
                print(f"  {mk['name']:20s} → {len(models)} model URLs")
        finally:
            await page.close()
            await ctx.close()

        model_listing_urls = list(dict.fromkeys(model_listing_urls))
        print(f"\nTotal model listing URLs: {len(model_listing_urls)}")

        # ── 3. Parallel crawl of listing pages + detail scraping ────
        model_q: asyncio.Queue = asyncio.Queue()
        for u in model_listing_urls:
            await model_q.put(u)

        detail_q: asyncio.Queue = asyncio.Queue()
        detail_seen: set = set()
        detail_lock = asyncio.Lock()

        results: list = []
        results_lock = asyncio.Lock()

        # Start model workers (feed detail_q)
        model_tasks = [
            asyncio.create_task(
                model_worker(i + 1, browser, model_q, detail_q, detail_seen, detail_lock)
            )
            for i in range(MODEL_WORKERS)
        ]

        # Start detail workers (consume detail_q)
        detail_tasks = [
            asyncio.create_task(
                detail_worker(i + 1, browser, detail_q, results, results_lock,
                              existing_keys, seen_keys, stats)
            )
            for i in range(DETAIL_WORKERS)
        ]

        # Wait for model workers to finish
        await model_q.join()
        for _ in model_tasks:
            await model_q.put(None)
        await asyncio.gather(*model_tasks, return_exceptions=True)

        # Wait for detail workers to drain
        await detail_q.join()
        for _ in detail_tasks:
            await detail_q.put(None)
        await asyncio.gather(*detail_tasks, return_exceptions=True)

        await browser.close()

    total = write_csvs(results)

    print("\n" + "=" * 60)
    print("DONE")
    print(f"New rows this run:    {len(results)}")
    print(f"Total in master CSV:  {total}")
    print(f"Skip stats:           {stats}")
    print(f"Elapsed:              {int(time.time() - t0)}s")


if __name__ == "__main__":
    asyncio.run(main())
