"""
Salvage auction scraper — en.bidfax.info
========================================
Picks a random make each run, crawls all its model listing pages,
then scrapes each detail page for the full record.

Install:
    pip install playwright pandas
    playwright install chromium

Run:
    python scraper_bidfax.py
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
REQUIRE_PRICE = True

MASTER_CSV  = "master.csv"
WEBSITE_CSV = "cars.csv"
RESUME_FROM_MASTER = True

MAX_LISTING_PAGES = 9999   # stops early when years go out of range
DETAIL_WORKERS    = 5

NAV_TIMEOUT_MS = 40_000
MAX_RETRIES    = 3
RETRY_MS       = 2_500

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","VI","GU",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Full list of makes on en.bidfax.info
ALL_MAKES = [
    "acura", "alfa-romeo", "audi", "bentley", "bmw", "buick", "cadillac",
    "chevrolet", "chrysler", "dodge", "ferrari", "fiat", "ford", "gmc",
    "honda", "hummer", "hyundai", "infiniti", "jaguar", "jeep", "kia",
    "land-rover", "lexus", "lincoln", "mazda", "mercedes-benz", "mercury",
    "mini", "mitsubishi", "nissan", "oldsmobile", "pontiac", "porsche",
    "ram", "saab", "saturn", "scion", "smart", "subaru", "suzuki",
    "tesla", "toyota", "volkswagen", "volvo",
]

EXCLUDED_MAKES = {"land-rover"}


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
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return int(float(m.group(1))) if m else 0

def clean_odo(t: str) -> str:
    s = str(t).replace(",", "").strip()
    m = re.search(r"([\d.]+)", s)
    if not m:
        return ""
    unit = "km" if "km" in s.lower() else "mi"
    return f"{int(float(m.group(1)))} {unit}"

def is_us(location: str) -> bool:
    m = re.search(r"\b([A-Z]{2})$", location.strip().upper())
    return bool(m and m.group(1) in US_STATES)

def parse_state(location: str):
    raw = location.strip().upper()
    m = re.match(r"^(.*?),?\s*([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip().title(), m.group(2)
    return location.strip().title(), ""

def infer_type(text: str) -> str:
    t = text.upper()
    if any(x in t for x in ["PICKUP","F-150","F150","SILVERADO","RAM 1","TUNDRA","RANGER","TACOMA","FRONTIER","COLORADO","F-250","F-350"]):
        return "Truck"
    if any(x in t for x in ["SUV","EXPLORER","ESCAPE","ROGUE","EQUINOX","TAHOE","SUBURBAN","RAV4","CR-V","CX-5","SORENTO","SPORTAGE","PILOT","HIGHLANDER","PATHFINDER","TRAVERSE","EXPEDITION","4RUNNER","SANTA FE","TUCSON","OUTLANDER","DURANGO"]):
        return "SUV"
    if any(x in t for x in ["COUPE","MUSTANG","CHALLENGER","CAMARO","BRZ","CORVETTE"]):
        return "Coupe"
    if any(x in t for x in ["VAN","TRANSIT","ODYSSEY","SIENNA","PACIFICA","CARAVAN","ECONOLINE","EXPRESS","SAVANA","PROMASTER"]):
        return "Van"
    return "Sedan"

def load_existing_keys() -> set:
    if not RESUME_FROM_MASTER or not os.path.exists(MASTER_CSV):
        return set()
    try:
        df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
        for c in ("vin", "lot"):
            if c not in df.columns:
                df[c] = ""
        keys = set((df["vin"].str.strip() + "|" + df["lot"].str.strip()).tolist())
        keys.discard("|")
        print(f"[resume] {len(keys)} existing records")
        return keys
    except Exception as e:
        print(f"[WARN] load keys: {e}")
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

    make  = str(raw.get("make",  "")).strip().title()
    model = str(raw.get("model", "")).strip().title()
    if not make or not model:
        stats["incomplete"] += 1
        return None

    if raw.get("make_slug", "") in EXCLUDED_MAKES:
        stats["excluded_make"] += 1
        return None

    location = str(raw.get("location", "")).strip()
    if location and not is_us(location):
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
        "trim":     str(raw.get("trim",    "")).strip(),
        "type":     infer_type(f"{make} {model} {raw.get('trim','')}"),
        "damage":   str(raw.get("damage",  "")).strip(),
        "price":    price,
        "odometer": clean_odo(raw.get("odometer", "")),
        "lot":      lot,
        "date":     str(raw.get("date",    "")).strip(),
        "location": loc_str,
        "state":    state,
        "source":   str(raw.get("source",  "Copart/IAAI")).strip(),
        "url":      str(raw.get("url",     "")).strip(),
    }


# ─────────────────────────────────────────────────────────────
# BROWSER
# ─────────────────────────────────────────────────────────────
async def new_ctx(browser):
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": BASE_URL + "/",
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
            await page.wait_for_timeout(random.randint(500, 1200))
            return True
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"  [ERROR] gave up: {url} ({e})")
                return False
            await page.wait_for_timeout(RETRY_MS * attempt + random.randint(0, 800))
    return False

async def all_internal_links(page, prefix: str) -> list[str]:
    """
    Return deduplicated absolute hrefs that start with `prefix`.
    Works regardless of whether href is absolute or relative.
    """
    try:
        hrefs = await page.evaluate(
            """(prefix) => {
                return Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(h => h.startsWith(prefix));
            }""",
            prefix,
        )
        return list(dict.fromkeys(hrefs))
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# STEP 1 — Discover model URLs for a make
# ─────────────────────────────────────────────────────────────
async def get_model_urls(page, make_slug: str) -> list[str]:
    """
    Load en.bidfax.info/{make}/ and collect all sub-model links.
    Pattern: https://en.bidfax.info/toyota/camry/
    """
    make_url = f"{BASE_URL}/{make_slug}/"
    ok = await goto_safe(page, make_url)
    if not ok:
        return [make_url]

    prefix = f"{BASE_URL}/{make_slug}/"
    links = await all_internal_links(page, prefix)

    # Keep only links that are ONE level deeper than the make URL
    # e.g. /toyota/camry/  but NOT /toyota/camry/some-detail.html
    model_links = []
    for link in links:
        path = link.replace(BASE_URL, "").strip("/")
        parts = path.split("/")
        # Exactly 2 parts: make/model
        if len(parts) == 2 and parts[0] == make_slug:
            model_links.append(link if link.endswith("/") else link + "/")

    model_links = list(dict.fromkeys(model_links))

    if not model_links:
        # No sub-models found — use the make page itself
        print(f"  [{make_slug}] no sub-model links found, using make page directly")
        return [make_url]

    print(f"  [{make_slug}] {len(model_links)} models: {[m.split('/')[-2] for m in model_links[:8]]}{'...' if len(model_links)>8 else ''}")
    return model_links


# ─────────────────────────────────────────────────────────────
# STEP 2 — Collect detail page URLs from a listing
# ─────────────────────────────────────────────────────────────
async def get_detail_urls(page, listing_url: str, make_slug: str) -> list[str]:
    """
    Paginate through en.bidfax.info/toyota/camry/?page=N
    and collect detail page URLs (end with .html).
    Stops when years fall consistently outside range.
    """
    detail_urls: list[str] = []
    seen: set = set()
    out_of_range_streak = 0

    for pg in range(1, MAX_LISTING_PAGES + 1):
        url = listing_url if pg == 1 else f"{listing_url}?page={pg}"
        ok = await goto_safe(page, url)
        if not ok:
            break

        # Check for empty/end page
        try:
            body = nw(await page.locator("body").inner_text(timeout=6000))
        except Exception:
            break

        if any(x in body.lower() for x in ["page not found", "no results", "404"]):
            break

        # Collect .html links under this listing prefix
        prefix = listing_url.rstrip("/") + "/"
        links = await all_internal_links(page, prefix)
        html_links = [l for l in links if l.endswith(".html") and l not in seen]

        if not html_links:
            # Try broader: any .html link on the page under the make
            make_prefix = f"{BASE_URL}/{make_slug}/"
            all_links = await all_internal_links(page, make_prefix)
            html_links = [l for l in all_links if l.endswith(".html") and l not in seen]

        if not html_links and pg > 1:
            break

        # Year filter from URL slugs
        page_urls = []
        page_years = []
        for link in html_links:
            seen.add(link)
            yr_m = re.search(r"-(\d{4})[^/]*\.html$", link)
            if yr_m:
                yr = int(yr_m.group(1))
                page_years.append(yr)
                if year_ok(yr):
                    page_urls.append(link)
            else:
                # No year in URL — include and filter at detail stage
                page_urls.append(link)

        detail_urls.extend(page_urls)

        # Early stop: 3 consecutive pages all outside year range
        if page_years:
            if all(not year_ok(y) for y in page_years):
                out_of_range_streak += 1
                if out_of_range_streak >= 3:
                    print(f"    stopping early — years out of range at page {pg}")
                    break
            else:
                out_of_range_streak = 0

        print(f"    page {pg}: +{len(page_urls)} detail URLs (total {len(detail_urls)})")

        if not html_links:
            break

    return detail_urls


# ─────────────────────────────────────────────────────────────
# STEP 3 — Scrape a detail page
# ─────────────────────────────────────────────────────────────
FIELD_MAP = {
    "VIN": "vin", "VIN NUMBER": "vin",
    "LOT": "lot", "LOT #": "lot", "LOT NUMBER": "lot", "STOCK": "lot", "STOCK #": "lot",
    "YEAR": "year",
    "MAKE": "make", "MODEL": "model",
    "SERIES": "trim", "TRIM": "trim", "BODY STYLE": "trim",
    "PRIMARY DAMAGE": "damage", "DAMAGE": "damage", "SECONDARY DAMAGE": "damage2",
    "ODOMETER": "odometer", "MILEAGE": "odometer", "MILES": "odometer",
    "SALE DATE": "date", "AUCTION DATE": "date", "DATE": "date",
    "LOCATION": "location", "AUCTION LOCATION": "location", "YARD": "location",
    "LAST BID": "price", "FINAL BID": "price", "SALE PRICE": "price",
    "SOLD FOR": "price", "WINNING BID": "price", "BID": "price",
    "AUCTION": "source", "SOURCE": "source",
}

async def scrape_detail(page, url: str, make_slug: str) -> dict | None:
    ok = await goto_safe(page, url)
    if not ok:
        return None

    try:
        await page.wait_for_selector("h1, table, .car-info, dl", timeout=5000)
    except Exception:
        pass

    try:
        body = nw(await page.locator("body").inner_text(timeout=8000))
    except Exception:
        return None

    if not body or len(body) < 50:
        return None

    raw: dict = {"url": url, "make_slug": make_slug}

    # ── Strategy A: table rows (label | value) ───────────────
    try:
        for row in await page.locator("tr").all():
            cells = await row.locator("td, th").all()
            if len(cells) >= 2:
                lbl = nw(await cells[0].inner_text()).upper().rstrip(":")
                val = nw(await cells[1].inner_text())
                if lbl in FIELD_MAP and val and not raw.get(FIELD_MAP[lbl]):
                    raw[FIELD_MAP[lbl]] = val
    except Exception:
        pass

    # ── Strategy B: dl / dt+dd pairs ─────────────────────────
    try:
        dts = await page.locator("dt").all()
        dds = await page.locator("dd").all()
        for dt, dd in zip(dts, dds):
            lbl = nw(await dt.inner_text()).upper().rstrip(":")
            val = nw(await dd.inner_text())
            if lbl in FIELD_MAP and val and not raw.get(FIELD_MAP[lbl]):
                raw[FIELD_MAP[lbl]] = val
    except Exception:
        pass

    # ── Strategy C: any element whose text is a known label,
    #               grab its next sibling's text ──────────────
    try:
        for lbl_key in FIELD_MAP:
            if raw.get(FIELD_MAP[lbl_key]):
                continue
            els = await page.get_by_text(re.compile(rf"^{re.escape(lbl_key)}:?$", re.IGNORECASE)).all()
            for el in els:
                sib = await el.evaluate("e => e.nextElementSibling ? e.nextElementSibling.innerText : ''")
                val = nw(sib)
                if val:
                    raw[FIELD_MAP[lbl_key]] = val
                    break
    except Exception:
        pass

    # ── Regex fallbacks on body text ─────────────────────────
    if not raw.get("vin"):
        m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", body)
        if m:
            raw["vin"] = m.group(1).upper()

    if not raw.get("lot"):
        m = re.search(r"(?:lot|stock)\s*#?\s*:?\s*(\d{5,})", body, re.IGNORECASE)
        if m:
            raw["lot"] = m.group(1)

    if not raw.get("year"):
        yr_m = re.search(r"-(\d{4})[^/]*\.html", url)
        if yr_m:
            raw["year"] = yr_m.group(1)
        else:
            m = re.search(r"\b(20(?:0[89]|1[0-5]))\b", body)
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
            m = re.search(pat, body, re.IGNORECASE)
            if m:
                raw["price"] = m.group(1)
                break

    if not raw.get("odometer"):
        m = re.search(r"([\d,]+)\s*(?:miles?|mi)\b", body, re.IGNORECASE)
        if m:
            raw["odometer"] = m.group(0).strip()

    if not raw.get("date"):
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", body)
        if m:
            raw["date"] = m.group(1)

    if not raw.get("source"):
        bl = body.lower()
        raw["source"] = "Copart" if "copart" in bl else ("IAAI" if "iaai" in bl else "Copart/IAAI")

    if not raw.get("make"):
        raw["make"] = make_slug.replace("-", " ").title()

    if not raw.get("model"):
        # Try to pull from URL slug: /toyota/camry/123-toyota-camry-le-2012...
        m = re.search(rf"/{re.escape(make_slug)}/([^/]+)/", url)
        if m:
            raw["model"] = m.group(1).replace("-", " ").title()

    return raw


# ─────────────────────────────────────────────────────────────
# DETAIL WORKER
# ─────────────────────────────────────────────────────────────
async def detail_worker(wid, browser, q, results, lock, existing_keys, seen_keys, stats):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    try:
        while True:
            item = await q.get()
            if item is None:
                q.task_done()
                break
            try:
                raw = await scrape_detail(page, item["url"], item["make_slug"])
                if raw:
                    async with lock:
                        rec = build_record(raw, existing_keys, seen_keys, stats)
                        if rec:
                            results.append(rec)
                            if len(results) % 25 == 0:
                                print(f"  [detail W{wid}] ✓ {len(results)} records kept")
            except Exception as e:
                print(f"  [detail W{wid}] error {item['url']}: {e}")
            finally:
                q.task_done()
    finally:
        await page.close()
        await ctx.close()


# ─────────────────────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────────────────────
def write_csvs(data: list) -> int:
    cols_m = ["vin","year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","source","url"]
    cols_w = ["year","make","model","trim","type","damage",
              "price","odometer","lot","date","location","state","url"]

    master = (pd.read_csv(MASTER_CSV, dtype=str).fillna("")
              if os.path.exists(MASTER_CSV) else pd.DataFrame(columns=cols_m))

    if data:
        ndf = pd.DataFrame(data)
        for c in cols_m:
            if c not in ndf.columns:
                ndf[c] = ""
        ndf = ndf[cols_m]
        ndf["price"] = pd.to_numeric(ndf["price"], errors="coerce").fillna(0).astype(int)
        master = pd.concat([master, ndf], ignore_index=True)

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

    # Pick a random make each run
    available = [m for m in ALL_MAKES if m not in EXCLUDED_MAKES]
    make_slug = random.choice(available)

    print("=" * 55)
    print(f"  en.bidfax.info scraper")
    print(f"  Selected make : {make_slug.upper()}")
    print(f"  Year range    : {YEAR_MIN}–{YEAR_MAX}")
    print(f"  Min price     : ${MIN_PRICE}")
    print(f"  Existing recs : {len(existing_keys)}")
    print("=" * 55)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await new_ctx(browser)
        nav_page = await ctx.new_page()

        # Step 1: get model URLs
        print(f"\n[1] Discovering models for '{make_slug}'...")
        model_urls = await get_model_urls(nav_page, make_slug)
        print(f"    → {len(model_urls)} model listing(s)")

        # Step 2: collect detail URLs from every model listing
        print(f"\n[2] Collecting detail URLs...")
        all_detail_items: list[dict] = []
        seen_detail: set = set()

        for model_url in model_urls:
            model_name = model_url.rstrip("/").split("/")[-1]
            print(f"  {make_slug}/{model_name}")
            urls = await get_detail_urls(nav_page, model_url, make_slug)
            for u in urls:
                if u not in seen_detail:
                    seen_detail.add(u)
                    all_detail_items.append({"url": u, "make_slug": make_slug})

        await nav_page.close()
        await ctx.close()

        print(f"\n    → {len(all_detail_items)} detail pages to scrape")

        if not all_detail_items:
            print("\n[WARN] No detail URLs found. The site may have blocked the scraper.")
            print("       Try running again — a different make may work better.")
            await browser.close()
            return

        # Step 3: scrape detail pages in parallel
        print(f"\n[3] Scraping detail pages ({DETAIL_WORKERS} workers)...")
        q: asyncio.Queue = asyncio.Queue()
        for item in all_detail_items:
            await q.put(item)

        results: list = []
        lock = asyncio.Lock()

        workers = [
            asyncio.create_task(
                detail_worker(i+1, browser, q, results, lock, existing_keys, seen_keys, stats)
            )
            for i in range(DETAIL_WORKERS)
        ]

        await q.join()
        for _ in workers:
            await q.put(None)
        await asyncio.gather(*workers, return_exceptions=True)
        await browser.close()

    total = write_csvs(results)

    print("\n" + "=" * 55)
    print(f"  Make scraped :  {make_slug.upper()}")
    print(f"  New rows     :  {len(results)}")
    print(f"  Master total :  {total}")
    print(f"  Skip stats   :  {stats}")
    print(f"  Elapsed      :  {int(time.time()-t0)}s")
    print("=" * 55)
    print("\nRun again to scrape a different random make.")


if __name__ == "__main__":
    asyncio.run(main())
