"""
Salvage auction history scraper — en.bidfax.info
=================================================
Source: BidFax (bidfax.info) — free Copart & IAAI US auction history since 2018.
No login required. Data is US-only (Copart/IAAI US lots).

Confirmed URL structure:
  Make list:    https://en.bidfax.info/                     → links like /toyota/ /honda/
  Model list:   https://en.bidfax.info/toyota/              → links like /toyota/camry/
  Listing page: https://en.bidfax.info/toyota/camry/        → paginated with ?page=2
  Detail page:  https://en.bidfax.info/toyota/camry/6676964-toyota-camry-base-2009-silver-24l-4-vin-4t1be46k99u404572.html

Confirmed detail page fields (from live search snippet):
  Year, Make, Model, Trim, Color, Engine, VIN, Lot, Odometer,
  Primary Damage, Location, Sale Date, Final Bid / Last Bid

Install:
    pip install playwright pandas
    playwright install chromium

Usage:
    python scraper_bidfax.py

Quick test (narrow scope):
    Set TARGET_MAKES = {"toyota"} and TARGET_MODELS = {"camry"} below.
"""

import asyncio
import os
import re
import time
import random

import pandas as pd
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
BASE_URL = "https://en.bidfax.info"

YEAR_MIN = 2008
YEAR_MAX = 2015
MIN_PRICE = 500
REQUIRE_PRICE = True        # False = keep rows even with no price found

MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"
RESUME_FROM_MASTER = True

MAX_LISTING_PAGES = 9999    # per make/model combo — stops early on year range
LISTING_WORKERS  = 3        # parallel workers crawling listing pages
DETAIL_WORKERS   = 6        # parallel workers scraping detail pages

NAV_TIMEOUT_MS = 35_000
MAX_RETRIES    = 3
RETRY_MS       = 2_000

# Leave empty to scrape everything; populate to narrow
TARGET_MAKES:  set = set()  # lowercase, e.g. {"toyota", "honda"}
TARGET_MODELS: set = set()  # lowercase substring, e.g. {"camry", "civic"}

EXCLUDED_MAKES = {"land-rover", "land rover"}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU","AS","MP",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Hardcoded make slugs from en.bidfax.info — avoids scraping the homepage
ALL_MAKES = [
    "acura", "alfa-romeo", "audi", "bentley", "bmw", "buick", "cadillac",
    "chevrolet", "chrysler", "dodge", "ferrari", "fiat", "ford", "gmc",
    "honda", "hummer", "hyundai", "infiniti", "jaguar", "jeep", "kia",
    "land-rover", "lexus", "lincoln", "mazda", "mercedes-benz", "mercury",
    "mini", "mitsubishi", "nissan", "oldsmobile", "pontiac", "porsche",
    "ram", "saab", "saturn", "scion", "smart", "subaru", "suzuki",
    "tesla", "toyota", "volkswagen", "volvo",
]


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def nw(t) -> str:
    return re.sub(r"\s+", " ", str(t)).strip()


def year_ok(y) -> bool:
    try:
        return YEAR_MIN <= int(str(y).strip()) <= YEAR_MAX
    except Exception:
        return False


def clean_price(t) -> int:
    s = str(t).replace("$", "").replace(",", "").strip()
    if not s:
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return int(float(m.group(1))) if m else 0


def clean_odo(t: str) -> str:
    s = str(t).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m:
        return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"


def is_us_location(location: str) -> bool:
    """Return True if location ends with a known US state code."""
    location = location.strip().upper()
    # "HOUSTON, TX" or "LAS VEGAS NV"
    m = re.search(r"\b([A-Z]{2})$", location)
    if m:
        return m.group(1) in US_STATES
    return False


def parse_state(location: str):
    raw = str(location).strip()
    m = re.match(r"^(.*?),?\s*([A-Z]{2})$", raw.strip().upper())
    if m:
        return m.group(1).strip().title(), m.group(2)
    return raw.title(), ""


def infer_type(text: str) -> str:
    t = text.upper()
    if any(x in t for x in ["PICKUP","F-150","F150","SILVERADO","RAM 1","TUNDRA",
                              "RANGER","TACOMA","FRONTIER","COLORADO","F-250","F-350"]):
        return "Truck"
    if any(x in t for x in ["SUV","EXPLORER","ESCAPE","ROGUE","EQUINOX","TAHOE","SUBURBAN",
                              "RAV4","CR-V","CX-5","XC90","SORENTO","SPORTAGE","PILOT",
                              "HIGHLANDER","PATHFINDER","TRAVERSE","EXPEDITION","4RUNNER",
                              "SANTA FE","TUCSON","OUTLANDER","DURANGO","TRAILBLAZER"]):
        return "SUV"
    if any(x in t for x in ["COUPE","MUSTANG","CHALLENGER","CAMARO","BRZ","86","CORVETTE"]):
        return "Coupe"
    if any(x in t for x in ["VAN","TRANSIT","ODYSSEY","SIENNA","PACIFICA","CARAVAN",
                              "ECONOLINE","EXPRESS","SAVANA","PROMASTER"]):
        return "Van"
    return "Sedan"


def load_existing_keys(path=MASTER_CSV) -> set:
    if not RESUME_FROM_MASTER or not os.path.exists(path):
        return set()
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        for col in ("vin", "lot"):
            if col not in df.columns:
                df[col] = ""
        keys = set((df["vin"].str.strip() + "|" + df["lot"].str.strip()).tolist())
        keys.discard("|")
        print(f"[resume] {len(keys)} existing records loaded")
        return keys
    except Exception as e:
        print(f"[WARN] Could not load existing keys: {e}")
        return set()


def build_record(raw: dict, existing_keys: set, seen_keys: set, stats: dict):
    year = str(raw.get("year", "")).strip()
    if not year_ok(year):
        stats["year_out_of_range"] += 1
        return None

    price = clean_price(raw.get("price", ""))
    if REQUIRE_PRICE and price < MIN_PRICE:
        stats["price_below_min"] += 1
        return None

    make  = str(raw.get("make", "")).strip().title()
    model = str(raw.get("model", "")).strip().title()
    if not make or not model:
        stats["incomplete"] += 1
        return None

    if make.lower().replace(" ", "-") in EXCLUDED_MAKES:
        stats["excluded_make"] += 1
        return None

    # US-only filter
    location = str(raw.get("location", "")).strip()
    if location and not is_us_location(location):
        stats["non_us"] = stats.get("non_us", 0) + 1
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

    loc_str, state = parse_state(location)

    seen_keys.add(key)
    existing_keys.add(key)
    stats["kept"] += 1

    return {
        "vin":      vin,
        "year":     year,
        "make":     make,
        "model":    model,
        "trim":     str(raw.get("trim",  "")).strip(),
        "type":     infer_type(f"{make} {model} {raw.get('trim', '')}"),
        "damage":   str(raw.get("damage","")).strip(),
        "price":    price,
        "odometer": clean_odo(raw.get("odometer", "")),
        "lot":      lot,
        "date":     str(raw.get("date",  "")).strip(),
        "location": loc_str,
        "state":    state,
        "source":   str(raw.get("source","Copart/IAAI")).strip(),
        "url":      str(raw.get("url",   "")).strip(),
    }


# ─────────────────────────────────────────────────────────────
# BROWSER HELPERS
# ─────────────────────────────────────────────────────────────
async def new_ctx(browser):
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://en.bidfax.info/",
        },
    )
    await ctx.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in {"image", "font", "media"}
        else route.continue_()
    )
    return ctx


async def goto_safe(page, url: str) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            await page.wait_for_timeout(random.randint(400, 900))
            return True
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"[ERROR] gave up: {url}  ({e})")
                return False
            await page.wait_for_timeout(RETRY_MS * attempt + random.randint(0, 600))
    return False


async def body_text(page) -> str:
    try:
        return nw(await page.locator("body").inner_text(timeout=8000))
    except Exception:
        return ""


async def get_links(page, pattern: str) -> list[str]:
    """Return all href values matching a regex pattern."""
    try:
        hrefs = await page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href]')).map(a => a.href)"
        )
        return [h for h in hrefs if re.search(pattern, h)]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# STEP 1: Discover model URLs for each make
# ─────────────────────────────────────────────────────────────
async def discover_model_urls(browser) -> list[dict]:
    makes = ALL_MAKES
    if TARGET_MAKES:
        makes = [m for m in makes if m in {t.lower() for t in TARGET_MAKES}]

    entries = []
    ctx = await new_ctx(browser)
    page = await ctx.new_page()

    try:
        for make_slug in makes:
            url = f"{BASE_URL}/{make_slug}/"
            ok = await goto_safe(page, url)
            if not ok:
                # No models page — add make directly as a listing URL
                entries.append({
                    "make": make_slug,
                    "model": make_slug,
                    "listing_url": url,
                })
                continue

            # Find model links: /toyota/camry/ pattern
            model_links = await get_links(
                page,
                rf"en\.bidfax\.info/{re.escape(make_slug)}/[^/]+/$"
            )

            if not model_links:
                # No sub-models — treat the make page itself as the listing
                entries.append({
                    "make": make_slug,
                    "model": make_slug,
                    "listing_url": url,
                })
            else:
                for link in dict.fromkeys(model_links):
                    model_slug = link.rstrip("/").split("/")[-1]
                    if TARGET_MODELS and not any(
                        t.lower() in model_slug.lower() for t in TARGET_MODELS
                    ):
                        continue
                    entries.append({
                        "make": make_slug,
                        "model": model_slug,
                        "listing_url": link if link.startswith("http") else BASE_URL + link,
                    })

            print(f"  [{make_slug}] {len([e for e in entries if e['make']==make_slug])} model URLs")

    finally:
        await page.close()
        await ctx.close()

    # Deduplicate
    seen: set = set()
    unique = []
    for e in entries:
        if e["listing_url"] not in seen:
            seen.add(e["listing_url"])
            unique.append(e)

    print(f"\n[discover] {len(unique)} model listing URLs")
    return unique


# ─────────────────────────────────────────────────────────────
# STEP 2: Collect detail page URLs from listing pages
# ─────────────────────────────────────────────────────────────
async def collect_detail_urls(page, entry: dict) -> list[str]:
    """
    Paginate listing pages and collect detail page URLs.
    BidFax listing URL: en.bidfax.info/toyota/camry/
    Pagination:         en.bidfax.info/toyota/camry/?page=2
    Detail URL pattern: en.bidfax.info/toyota/camry/6676964-...-vin-XXXX.html
    """
    base = entry["listing_url"].rstrip("/") + "/"
    detail_urls = []
    seen: set = set()
    consecutive_out_of_range = 0

    for pg in range(1, MAX_LISTING_PAGES + 1):
        url = base if pg == 1 else f"{base}?page={pg}"
        ok = await goto_safe(page, url)
        if not ok:
            break

        txt = await body_text(page)
        if not txt:
            break

        # Stop conditions
        if any(x in txt.lower() for x in ["no results", "no vehicles", "page not found", "404"]):
            break

        # Collect detail links: ends with .html, contains a year in slug
        links = await get_links(
            page,
            rf"en\.bidfax\.info/{re.escape(entry['make'])}/{re.escape(entry['model'])}/\d+"
        )

        page_detail_urls = []
        page_years = []

        for link in links:
            if link in seen or not link.endswith(".html"):
                continue
            # Extract year from URL slug: "...6676964-toyota-camry-base-2009-silver..."
            yr_m = re.search(r"-(\d{4})-", link)
            if yr_m:
                yr = int(yr_m.group(1))
                page_years.append(yr)
                if year_ok(yr):
                    seen.add(link)
                    page_detail_urls.append(link)
            else:
                # Year not in URL — keep and filter at detail stage
                seen.add(link)
                page_detail_urls.append(link)

        detail_urls.extend(page_detail_urls)

        # Early stop: multiple consecutive pages entirely outside year range
        if page_years:
            if all(y < YEAR_MIN or y > YEAR_MAX for y in page_years):
                consecutive_out_of_range += 1
                if consecutive_out_of_range >= 3:
                    print(f"  [listing] stopping early — years out of range at page {pg}")
                    break
            else:
                consecutive_out_of_range = 0

        print(f"  [listing] {entry['make']}/{entry['model']} p{pg}: +{len(page_detail_urls)} (total {len(detail_urls)})")

        if not page_detail_urls and pg > 1:
            break

    return detail_urls


# ─────────────────────────────────────────────────────────────
# STEP 3: Scrape a detail page
# ─────────────────────────────────────────────────────────────
async def scrape_detail(page, url: str, make_slug: str, model_slug: str) -> dict | None:
    ok = await goto_safe(page, url)
    if not ok:
        return None

    # Wait for main content block
    try:
        await page.wait_for_selector("h1, .car-info, table, .lot-info", timeout=6000)
    except Exception:
        pass

    txt = await body_text(page)
    if not txt:
        return None

    raw: dict = {
        "url":    url,
        "make":   make_slug.replace("-", " ").title(),
        "model":  model_slug.replace("-", " ").title(),
        "source": "Copart/IAAI",
    }

    # ── Structured field extraction via table label→value rows ────
    # BidFax detail pages use a two-column table: label | value
    field_map = {
        "VIN": "vin", "VIN NUMBER": "vin",
        "LOT": "lot", "LOT #": "lot", "LOT NUMBER": "lot", "STOCK": "lot",
        "YEAR": "year",
        "MAKE": "make",
        "MODEL": "model",
        "SERIES": "trim", "TRIM": "trim", "BODY STYLE": "trim",
        "PRIMARY DAMAGE": "damage", "DAMAGE": "damage",
        "ODOMETER": "odometer", "MILEAGE": "odometer", "MILES": "odometer",
        "SALE DATE": "date", "AUCTION DATE": "date", "DATE": "date",
        "LOCATION": "location", "AUCTION LOCATION": "location",
        "LAST BID": "price", "FINAL BID": "price", "SALE PRICE": "price",
        "SOLD FOR": "price", "WINNING BID": "price",
        "AUCTION": "source",
    }

    try:
        rows = await page.locator("tr").all()
        for row in rows:
            cells = await row.locator("td, th").all()
            if len(cells) >= 2:
                label = nw(await cells[0].inner_text()).upper().rstrip(":")
                value = nw(await cells[1].inner_text())
                if label in field_map and value and not raw.get(field_map[label]):
                    raw[field_map[label]] = value
    except Exception:
        pass

    # Try dl/dt/dd
    try:
        dts = await page.locator("dt").all()
        dds = await page.locator("dd").all()
        for dt, dd in zip(dts, dds):
            label = nw(await dt.inner_text()).upper().rstrip(":")
            value = nw(await dd.inner_text())
            if label in field_map and value and not raw.get(field_map[label]):
                raw[field_map[label]] = value
    except Exception:
        pass

    # Try .info-label/.info-value or similar paired divs
    try:
        items = await page.locator(
            "[class*='label'], [class*='key'], [class*='title']"
        ).all()
        for item in items:
            label = nw(await item.inner_text()).upper().rstrip(":")
            if label not in field_map:
                continue
            # Try next sibling
            sib_text = await item.evaluate(
                "el => el.nextElementSibling ? el.nextElementSibling.innerText : ''"
            )
            value = nw(sib_text)
            if value and not raw.get(field_map[label]):
                raw[field_map[label]] = value
    except Exception:
        pass

    # ── Regex fallbacks on full body text ────────────────────────
    if not raw.get("vin"):
        m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", txt)
        if m:
            raw["vin"] = m.group(1).upper()

    if not raw.get("lot"):
        m = re.search(r"(?:Lot|LOT|Stock)[^\w#]*#?\s*(\d{6,})", txt, re.IGNORECASE)
        if m:
            raw["lot"] = m.group(1)

    if not raw.get("year"):
        # From URL slug
        yr_m = re.search(r"-(\d{4})-", url)
        if yr_m:
            raw["year"] = yr_m.group(1)
        else:
            # From title
            m = re.search(r"\b((?:19|20)\d{2})\b", txt)
            if m:
                raw["year"] = m.group(1)

    if not raw.get("price"):
        for pat in [
            r"\bLast\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bFinal\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bSold\s+(?:for)?\b[^\d$]*\$?\s*([\d,]+)",
            r"\bSale\s+Price\b[^\d$]*\$?\s*([\d,]+)",
            r"\$([\d,]{3,})\b",
        ]:
            m = re.search(pat, txt, re.IGNORECASE)
            if m:
                raw["price"] = m.group(1)
                break

    if not raw.get("odometer"):
        m = re.search(r"([\d,]+)\s*(?:miles?|mi|km)\b", txt, re.IGNORECASE)
        if m:
            raw["odometer"] = m.group(0).strip()

    if not raw.get("date"):
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", txt)
        if m:
            raw["date"] = m.group(1)
        else:
            m = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", txt)
            if m:
                raw["date"] = m.group(1)

    if not raw.get("source"):
        tl = txt.lower()
        if "copart" in tl:
            raw["source"] = "Copart"
        elif "iaai" in tl or "insurance auto" in tl:
            raw["source"] = "IAAI"

    return raw


# ─────────────────────────────────────────────────────────────
# WORKERS
# ─────────────────────────────────────────────────────────────
async def listing_worker(wid, browser, model_q, detail_q, detail_seen, det_lock):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    try:
        while True:
            entry = await model_q.get()
            if entry is None:
                model_q.task_done()
                break
            try:
                urls = await collect_detail_urls(page, entry)
                added = 0
                async with det_lock:
                    for u in urls:
                        if u not in detail_seen:
                            detail_seen.add(u)
                            await detail_q.put({
                                "url": u,
                                "make": entry["make"],
                                "model": entry["model"],
                            })
                            added += 1
                print(f"[listing W{wid}] {entry['make']}/{entry['model']} → {added} detail URLs queued")
            except Exception as e:
                print(f"[listing W{wid}] error: {e}")
            finally:
                model_q.task_done()
    finally:
        await page.close()
        await ctx.close()


async def detail_worker(wid, browser, detail_q, results, res_lock,
                        existing_keys, seen_keys, stats):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    try:
        while True:
            item = await detail_q.get()
            if item is None:
                detail_q.task_done()
                break
            try:
                raw = await scrape_detail(page, item["url"], item["make"], item["model"])
                if raw:
                    async with res_lock:
                        rec = build_record(raw, existing_keys, seen_keys, stats)
                        if rec:
                            results.append(rec)
                            if len(results) % 50 == 0:
                                print(f"[detail W{wid}] ✓ {len(results)} records kept")
            except Exception as e:
                print(f"[detail W{wid}] error {item['url']}: {e}")
            finally:
                detail_q.task_done()
    finally:
        await page.close()
        await ctx.close()


# ─────────────────────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────────────────────
def write_csvs(all_data: list) -> int:
    cols_m = ["vin","year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","source","url"]
    cols_w = ["year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","url"]

    master = (pd.read_csv(MASTER_CSV, dtype=str).fillna("")
              if os.path.exists(MASTER_CSV)
              else pd.DataFrame(columns=cols_m))

    if all_data:
        new_df = pd.DataFrame(all_data)
        for c in cols_m:
            if c not in new_df.columns:
                new_df[c] = ""
        new_df = new_df[cols_m]
        new_df["price"] = pd.to_numeric(new_df["price"], errors="coerce").fillna(0).astype(int)
        master = pd.concat([master, new_df], ignore_index=True)

    if not master.empty:
        master["price"] = pd.to_numeric(master["price"], errors="coerce").fillna(0).astype(int)
        master["_yr"]   = pd.to_numeric(master["year"],  errors="coerce")
        master = master[master["price"] >= MIN_PRICE]
        master = master[master["_yr"].between(YEAR_MIN, YEAR_MAX, inclusive="both")]
        master = master[~master["make"].str.lower().str.replace(" ", "-").isin(EXCLUDED_MAKES)]
        master = master.drop_duplicates(subset=["vin","lot"], keep="first")
        master = master.drop(columns=["_yr"])

    master.to_csv(MASTER_CSV, index=False)

    site = master.copy()
    for c in cols_w:
        if c not in site.columns:
            site[c] = ""
    site.reindex(columns=cols_w, fill_value="").to_csv(WEBSITE_CSV, index=False)
    return len(master)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
async def main():
    t0 = time.time()
    existing_keys = load_existing_keys()
    seen_keys: set = set()
    stats = {k: 0 for k in [
        "price_below_min","year_out_of_range","excluded_make",
        "duplicate","existing","incomplete","non_us","kept"
    ]}

    print(f"Target site:   en.bidfax.info (US Copart + IAAI)")
    print(f"Year range:    {YEAR_MIN}–{YEAR_MAX}")
    print(f"Min price:     ${MIN_PRICE}  (require={REQUIRE_PRICE})")
    print(f"Existing:      {len(existing_keys)} records in {MASTER_CSV}")
    if TARGET_MAKES:
        print(f"Target makes:  {TARGET_MAKES}")
    if TARGET_MODELS:
        print(f"Target models: {TARGET_MODELS}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        # Step 1 — discover model listing URLs
        model_entries = await discover_model_urls(browser)

        # Step 2+3 — parallel crawl
        model_q:  asyncio.Queue = asyncio.Queue()
        detail_q: asyncio.Queue = asyncio.Queue()
        det_lock  = asyncio.Lock()
        detail_seen: set = set()
        results:  list  = []
        res_lock  = asyncio.Lock()

        for e in model_entries:
            await model_q.put(e)

        l_tasks = [
            asyncio.create_task(
                listing_worker(i+1, browser, model_q, detail_q, detail_seen, det_lock)
            )
            for i in range(LISTING_WORKERS)
        ]
        d_tasks = [
            asyncio.create_task(
                detail_worker(i+1, browser, detail_q, results, res_lock,
                              existing_keys, seen_keys, stats)
            )
            for i in range(DETAIL_WORKERS)
        ]

        await model_q.join()
        for _ in l_tasks:
            await model_q.put(None)
        await asyncio.gather(*l_tasks, return_exceptions=True)

        await detail_q.join()
        for _ in d_tasks:
            await detail_q.put(None)
        await asyncio.gather(*d_tasks, return_exceptions=True)

        await browser.close()

    total = write_csvs(results)

    print("\n" + "="*60)
    print(f"New rows this run:   {len(results)}")
    print(f"Total in master CSV: {total}")
    print(f"Skip stats:          {stats}")
    print(f"Elapsed:             {int(time.time()-t0)}s")


if __name__ == "__main__":
    asyncio.run(main())
