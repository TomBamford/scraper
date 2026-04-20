"""
Salvage auction history scraper — Poctra.com  (v3 — correct URL patterns)
=========================================================================
URL patterns confirmed from live search results:
  Makes list:     https://poctra.com/makes
                  → Full name makes: TOYOTA, HONDA, CHEVROLET, etc.
  Model listing:  https://poctra.com/TOYOTA-CAMRY-for-sale-1
                  https://poctra.com/TOYOTA-CAMRY_LE-for-sale-1  (trim uses underscore)
  Detail page:    https://poctra.com/lot/{LOT_NUMBER}/{SLUG}
                  (lot number links from listing rows)

Listing row format (confirmed from search snippet):
  "2008 TOYOTA CAMRY · Lot No: 39966187 · 0 MI · Pri/Sec Dmg: WATER/FLOOD · HOUSTON, TX · [price or 'Auction data not found']"

Strategy:
  1. Build make→model URL list from hardcoded makes (JS-rendered /makes page is unreliable)
  2. For each model listing page, parse rows in <table> or <div> cards directly
  3. Filter by year range on listing page (no detail page needed for most fields)
  4. Only visit detail pages to get VIN + price (since lot/year/damage/location are in listing)
  5. Skip rows where "Auction data not found" to avoid wasting requests

Install:
    pip install playwright pandas
    playwright install chromium
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
BASE_URL = "https://poctra.com"

YEAR_MIN = 2008
YEAR_MAX = 2015
MIN_PRICE = 500
REQUIRE_PRICE = True     # if True, skip "Auction data not found" rows entirely

MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"
RESUME_FROM_MASTER = True

MAX_PAGES_PER_MODEL = 9999
LISTING_WORKERS = 4
DETAIL_WORKERS = 6

NAV_TIMEOUT_MS = 35_000
MAX_RETRIES = 3
RETRY_MS = 2_000

TARGET_MAKES: set = set()    # e.g. {"TOYOTA", "HONDA"} — empty = all
TARGET_MODELS: set = set()   # substring match e.g. {"CAMRY"} — empty = all

EXCLUDED_MAKES = {"LAND ROVER"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# All makes confirmed on poctra.com/makes (full display names, no short codes)
ALL_POCTRA_MAKES = [
    "ACURA", "ALFA ROMEO", "ASTON MARTIN", "AUDI", "BENTLEY", "BMW",
    "BUICK", "CADILLAC", "CHEVROLET", "CHRYSLER", "DODGE", "FERRARI",
    "FIAT", "FORD", "FREIGHTLINER", "GEO", "GMC", "HARLEY-DAVIDSON",
    "HONDA", "HUMMER", "HYUNDAI", "ISUZU", "JAGUAR", "JEEP", "KIA",
    "LAND ROVER", "LEXUS", "LINCOLN", "MASERATI", "MAZDA",
    "MERCEDES-BENZ", "MERCURY", "MINI", "MITSUBISHI", "NISSAN",
    "OLDSMOBILE", "PLYMOUTH", "PONTIAC", "PORSCHE", "RAM", "ROLLS-ROYCE",
    "SAAB", "SATURN", "SCION", "SMART", "SUBARU", "SUZUKI", "TESLA",
    "TOYOTA", "VOLKSWAGEN", "VOLVO", "YAMAHA",
]

DAMAGE_NORMALIZE = {
    "WATER/FLOOD": "Water/Flood", "FLOOD": "Water/Flood",
    "FRONT END": "Front End", "REAR END": "Rear End", "SIDE": "Side",
    "ROLLOVER": "Rollover", "ALL OVER": "All Over",
    "MECHANICAL": "Mechanical", "MINOR DENT/SCRATCHES": "Minor Dent/Scratches",
    "MINOR DENT": "Minor Dent/Scratches", "NORMAL WEAR": "Normal Wear",
    "UNDERCARRIAGE": "Undercarriage", "STRIPPED": "Stripped",
    "BURN": "Burn", "TOP/ROOF": "Top/Roof", "HAIL": "Hail",
    "VANDALISM": "Vandalism", "BIOHAZARD/CHEMICAL": "Biohazard/Chemical",
    "FIRE": "Burn",
}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def nw(t) -> str:
    return re.sub(r"\s+", " ", str(t)).strip()


def make_to_slug(make: str) -> str:
    """'MERCEDES-BENZ' stays 'MERCEDES-BENZ', 'ALFA ROMEO' → 'ALFA-ROMEO'"""
    return make.strip().upper().replace(" ", "-")


def year_ok(y) -> bool:
    try:
        return YEAR_MIN <= int(str(y).strip()) <= YEAR_MAX
    except Exception:
        return False


def clean_price(t) -> int:
    s = str(t).replace("$", "").replace(",", "").strip()
    if not s or any(x in s.lower() for x in ["no sale", "not sold", "n/a", "not found"]):
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


def normalize_damage(raw: str) -> str:
    raw_up = raw.strip().upper()
    for key, val in DAMAGE_NORMALIZE.items():
        if key in raw_up:
            return val
    return raw.strip().title()


def infer_type(text: str) -> str:
    t = text.upper()
    if any(x in t for x in ["PICKUP","F-150","F150","SILVERADO","RAM 1","TUNDRA",
                              "RANGER","TACOMA","FRONTIER","COLORADO","CANYON","F-250","F-350"]):
        return "Truck"
    if any(x in t for x in ["SUV","EXPLORER","ESCAPE","ROGUE","EQUINOX","TAHOE","SUBURBAN",
                              "RAV4","CR-V","CX-5","XC90","SORENTO","SPORTAGE","PILOT",
                              "HIGHLANDER","PATHFINDER","TRAVERSE","EXPEDITION","4RUNNER",
                              "SANTA FE","TUCSON","OUTLANDER","DURANGO","TRAILBLAZER"]):
        return "SUV"
    if any(x in t for x in ["COUPE","MUSTANG","CHALLENGER","CAMARO","BRZ","86","CORVETTE"]):
        return "Coupe"
    if any(x in t for x in ["VAN","TRANSIT","ODYSSEY","SIENNA","PACIFICA","CARAVAN",
                              "ECONOLINE","EXPRESS","SAVANA","MINIVAN","PROMASTER"]):
        return "Van"
    return "Sedan"


def parse_state(location: str):
    raw = str(location).strip()
    m = re.match(r"^(.*?),\s*([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip().title(), m.group(2)
    m = re.match(r"^(.*?)\s+([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip().title(), m.group(2)
    return raw.title(), ""


def load_existing_keys(path=MASTER_CSV) -> set:
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
        print(f"[resume] Loaded {len(keys)} existing keys from {path}")
        return keys
    except Exception as e:
        print(f"[WARN] Could not load existing keys: {e}")
        return set()


def build_record(raw: dict, existing_keys: set, seen_keys: set, stats: dict):
    price = clean_price(raw.get("price", ""))
    if REQUIRE_PRICE and price < MIN_PRICE:
        stats["price_below_min"] += 1
        return None

    year = str(raw.get("year", "")).strip()
    if not year_ok(year):
        stats["year_out_of_range"] += 1
        return None

    make = str(raw.get("make", "")).strip().title()
    model = str(raw.get("model", "")).strip().title()
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
    state = str(raw.get("state", "")).strip().upper()
    if loc_raw and not state:
        loc_raw, state = parse_state(loc_raw)

    damage = normalize_damage(raw.get("damage", "")) if raw.get("damage") else ""

    seen_keys.add(key)
    existing_keys.add(key)
    stats["kept"] += 1

    return {
        "vin": vin,
        "year": year,
        "make": make,
        "model": model,
        "trim": str(raw.get("trim", "")).strip(),
        "type": infer_type(f"{make} {model} {raw.get('trim', '')}"),
        "damage": damage,
        "price": price,
        "odometer": clean_odo(raw.get("odometer", "")),
        "lot": lot,
        "date": str(raw.get("date", "")).strip(),
        "location": loc_raw,
        "state": state,
        "source": str(raw.get("source", "Copart/IAAI")).strip(),
        "url": str(raw.get("url", "")).strip(),
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
            await page.wait_for_timeout(random.randint(300, 800))
            return True
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"[ERROR] gave up: {url}  ({e})")
                return False
            await page.wait_for_timeout(RETRY_MS * attempt + random.randint(0, 500))
    return False


async def get_body_text(page) -> str:
    try:
        return nw(await page.locator("body").inner_text(timeout=8000))
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────
# STEP 1: Build model listing URL list
# ─────────────────────────────────────────────────────────────
async def discover_models(browser) -> list[dict]:
    makes = [m for m in ALL_POCTRA_MAKES if not TARGET_MAKES or m.upper() in {t.upper() for t in TARGET_MAKES}]

    model_entries = []
    ctx = await new_ctx(browser)
    page = await ctx.new_page()

    try:
        for make in makes:
            slug = make_to_slug(make)
            make_url = f"{BASE_URL}/{slug}-for-sale-1"
            ok = await goto_safe(page, make_url)

            # Always include the bare make listing
            model_entries.append({"make": make, "model": make, "base_url": make_url})

            if not ok:
                continue

            # Collect model sub-links from the sidebar
            found_models = set()
            links = await page.locator("a[href]").all()
            for link in links:
                try:
                    href = (await link.get_attribute("href") or "").strip()
                    # Pattern: /MAKE-MODEL-for-sale-N  where MODEL has no spaces (uses underscore or dash)
                    m = re.match(
                        rf"^/{re.escape(slug)}-(.+?)-for-sale-\d+$",
                        href, re.IGNORECASE
                    )
                    if not m:
                        continue
                    model_slug = m.group(1)
                    # Skip if it's just a page number variant of the make itself
                    if not model_slug:
                        continue
                    model_display = model_slug.replace("_", " ").replace("-", " ").strip()
                    base_url = f"{BASE_URL}/{slug}-{model_slug}-for-sale-1"
                    if base_url not in found_models:
                        found_models.add(base_url)
                        model_entries.append({
                            "make": make,
                            "model": model_display,
                            "base_url": base_url,
                        })
                except Exception:
                    pass

            print(f"  [{make}] {len(found_models)} model variants")

    finally:
        await page.close()
        await ctx.close()

    if TARGET_MODELS:
        filtered = []
        for e in model_entries:
            if any(t.upper() in e["model"].upper() for t in TARGET_MODELS) or e["model"] == e["make"]:
                filtered.append(e)
        model_entries = filtered

    # Deduplicate
    seen: set = set()
    unique = []
    for e in model_entries:
        if e["base_url"] not in seen:
            seen.add(e["base_url"])
            unique.append(e)

    print(f"\n[discover] {len(unique)} listing URLs to crawl")
    return unique


# ─────────────────────────────────────────────────────────────
# STEP 2: Parse listing page rows
# ─────────────────────────────────────────────────────────────
def parse_row(row_text: str, make: str, model_hint: str) -> dict | None:
    """
    Parse one car row from Poctra listing page.
    Confirmed format from search snippets:
      "2008 TOYOTA CAMRY · Lot No: 39966187 · 47,931 MI · Pri/Sec Dmg: VANDALISM · HOUSTON, TX · $3,500"
    """
    row_text = nw(row_text)
    if not row_text:
        return None

    # Must start with a 4-digit year
    y_m = re.match(r"^((?:19|20)\d{2})\b", row_text)
    if not y_m:
        return None
    year = y_m.group(1)
    if not year_ok(year):
        return None

    # Lot number is required
    lot_m = re.search(r"\bLot\s*No[:\s]*(\d{6,})", row_text, re.IGNORECASE)
    if not lot_m:
        return None
    lot = lot_m.group(1)

    raw = {
        "year": year,
        "make": make,
        "model": model_hint,
        "lot": lot,
        "price": "0",
        "source": "Copart/IAAI",
    }

    # Odometer: "47,931 MI"
    odo_m = re.search(r"([\d,]+)\s*MI\b", row_text, re.IGNORECASE)
    if odo_m:
        raw["odometer"] = odo_m.group(0).strip()

    # Primary damage: "Pri/Sec Dmg: FRONT END"
    dmg_m = re.search(r"Pri/Sec\s+Dmg:\s*([A-Z/\s,\-]+?)(?:\s*[·|]|\s{2,}|,\s*[A-Z]{2}\b|$)", row_text, re.IGNORECASE)
    if dmg_m:
        raw["damage"] = dmg_m.group(1).strip().rstrip(",·|").strip()

    # Location: "HOUSTON, TX" — 2-letter state code helps identify it
    loc_m = re.search(r"\b([A-Z][A-Z\s]+,\s*[A-Z]{2})\b", row_text)
    if loc_m:
        raw["location"] = loc_m.group(1).strip()

    # Price
    price_m = re.search(r"\$\s*([\d,]+)", row_text)
    if price_m:
        raw["price"] = price_m.group(1)

    # Model from row text (strip make prefix)
    after_year = row_text[y_m.end():].strip()
    make_up = make.upper()
    if after_year.upper().startswith(make_up):
        rest = after_year[len(make_up):].strip()
        # Model is text up to first bullet/separator
        mod_m = re.match(r"^([A-Z0-9][A-Z0-9\s\-]*?)(?:\s*·|\s{3,}|$)", rest, re.IGNORECASE)
        if mod_m:
            raw["model"] = mod_m.group(1).strip()

    return raw


async def scrape_listing_pages(page, entry: dict) -> list[dict]:
    base_url = entry["base_url"]
    make = entry["make"]
    model_hint = entry.get("model", make)
    rows_out = []
    seen_lots: set = set()
    early_stop_count = 0

    for page_num in range(1, MAX_PAGES_PER_MODEL + 1):
        url = re.sub(r"-for-sale-\d+$", f"-for-sale-{page_num}", base_url)
        ok = await goto_safe(page, url)
        if not ok:
            break

        body = await get_body_text(page)
        if not body:
            break
        if any(x in body.lower() for x in ["no vehicles", "no results", "page not found", "404"]):
            break

        # Extract row texts: try table rows first, then text splitting
        row_texts = []
        try:
            trs = await page.locator("table tr").all()
            for tr in trs:
                t = nw(await tr.inner_text())
                if re.match(r"^\d{4}\s+[A-Z]", t):
                    row_texts.append(t)
        except Exception:
            pass

        if not row_texts:
            for seg in re.split(r"\n+", body):
                seg = nw(seg)
                if re.match(r"^\d{4}\s+[A-Z]", seg):
                    row_texts.append(seg)

        if not row_texts:
            break

        page_new = 0
        page_years = []

        for rt in row_texts:
            parsed = parse_row(rt, make, model_hint)
            if parsed is None:
                # Still track years for early-stop
                y_m = re.match(r"^(\d{4})\b", rt)
                if y_m:
                    try:
                        page_years.append(int(y_m.group(1)))
                    except Exception:
                        pass
                continue

            page_years.append(int(parsed["year"]))
            lot = parsed["lot"]
            if lot not in seen_lots:
                seen_lots.add(lot)
                parsed["url"] = f"{BASE_URL}/{make_to_slug(make)}-{model_hint.replace(' ','-')}/{lot}"
                rows_out.append(parsed)
                page_new += 1

        # Early stop: two consecutive pages entirely before YEAR_MIN
        if page_years and max(page_years) < YEAR_MIN:
            early_stop_count += 1
            if early_stop_count >= 2:
                break
        else:
            early_stop_count = 0

        print(f"  [listing] page {page_num}: +{page_new} (total {len(rows_out)})  {url}")

        if page_new == 0 and page_num > 1:
            break

    return rows_out


# ─────────────────────────────────────────────────────────────
# STEP 3: Scrape detail page for VIN + price
# ─────────────────────────────────────────────────────────────
async def scrape_detail(page, lot: str, make: str, model: str) -> dict:
    result = {"vin": "", "price": "", "date": "", "source": ""}
    slug = make_to_slug(make)
    model_slug = model.upper().replace(" ", "-")

    # Poctra detail page URL patterns to try
    urls = [
        f"{BASE_URL}/{slug}-{model_slug}/{lot}",
        f"{BASE_URL}/lot/{lot}",
        f"{BASE_URL}/{slug}/{lot}",
    ]

    for url in urls:
        ok = await goto_safe(page, url)
        if not ok:
            continue
        title = await page.title()
        if "404" in title.upper() or "not found" in title.lower():
            continue
        body = await get_body_text(page)
        if not body or lot not in body:
            continue

        # VIN: 17-char alphanumeric
        vin_m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", body)
        if vin_m:
            result["vin"] = vin_m.group(1).upper()

        # Price
        for pat in [
            r"\bLast\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bFinal\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\bSold\s+(?:for|price)?\b[^\d$]*\$?\s*([\d,]+)",
            r"\bSale\s+Price\b[^\d$]*\$?\s*([\d,]+)",
            r"\bWinning\s+Bid\b[^\d$]*\$?\s*([\d,]+)",
            r"\$([\d,]{3,})",
        ]:
            pm = re.search(pat, body, re.IGNORECASE)
            if pm:
                result["price"] = pm.group(1)
                break

        # Date
        dm = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", body)
        if dm:
            result["date"] = dm.group(1)

        bl = body.lower()
        if "copart" in bl:
            result["source"] = "Copart"
        elif "iaai" in bl:
            result["source"] = "IAAI"

        if result["vin"] or result["price"]:
            break

    return result


# ─────────────────────────────────────────────────────────────
# WORKERS
# ─────────────────────────────────────────────────────────────
async def listing_worker(wid, browser, model_q, row_q, row_lock):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    try:
        while True:
            entry = await model_q.get()
            if entry is None:
                model_q.task_done()
                break
            try:
                rows = await scrape_listing_pages(page, entry)
                async with row_lock:
                    for r in rows:
                        await row_q.put(r)
                print(f"[listing W{wid}] {entry['make']} {entry['model']} → {len(rows)} rows")
            except Exception as e:
                print(f"[listing W{wid}] error: {e}")
            finally:
                model_q.task_done()
    finally:
        await page.close()
        await ctx.close()


async def detail_worker(wid, browser, row_q, results, res_lock, existing_keys, seen_keys, stats):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    try:
        while True:
            raw = await row_q.get()
            if raw is None:
                row_q.task_done()
                break
            try:
                lot = raw.get("lot", "")
                needs_detail = (
                    REQUIRE_PRICE and clean_price(raw.get("price", "")) < MIN_PRICE
                ) or not raw.get("vin")

                if needs_detail and lot:
                    detail = await scrape_detail(page, lot, raw.get("make",""), raw.get("model",""))
                    if detail.get("vin"):
                        raw["vin"] = detail["vin"]
                    if detail.get("price") and not raw.get("price"):
                        raw["price"] = detail["price"]
                    if detail.get("date"):
                        raw["date"] = detail["date"]
                    if detail.get("source"):
                        raw["source"] = detail["source"]

                async with res_lock:
                    rec = build_record(raw, existing_keys, seen_keys, stats)
                    if rec:
                        results.append(rec)
                        if len(results) % 50 == 0:
                            print(f"[detail W{wid}] ✓ {len(results)} records kept")
            except Exception as e:
                print(f"[detail W{wid}] error: {e}")
            finally:
                row_q.task_done()
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

    master = pd.read_csv(MASTER_CSV, dtype=str).fillna("") if os.path.exists(MASTER_CSV) \
        else pd.DataFrame(columns=cols_m)

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
        master["_yr"] = pd.to_numeric(master["year"], errors="coerce")
        master = master[master["price"] >= MIN_PRICE]
        master = master[master["_yr"].between(YEAR_MIN, YEAR_MAX, inclusive="both")]
        master = master[~master["make"].str.upper().isin(EXCLUDED_MAKES)]
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
    stats = {k: 0 for k in ["price_below_min","year_out_of_range","excluded_make",
                              "duplicate","existing","incomplete","kept"]}

    print(f"Year range:    {YEAR_MIN}–{YEAR_MAX}")
    print(f"Min price:     ${MIN_PRICE}  (require_price={REQUIRE_PRICE})")
    print(f"Existing:      {len(existing_keys)} records")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        model_entries = await discover_models(browser)

        model_q: asyncio.Queue = asyncio.Queue()
        for e in model_entries:
            await model_q.put(e)

        row_q: asyncio.Queue = asyncio.Queue()
        row_lock = asyncio.Lock()
        results: list = []
        res_lock = asyncio.Lock()

        l_tasks = [
            asyncio.create_task(listing_worker(i+1, browser, model_q, row_q, row_lock))
            for i in range(LISTING_WORKERS)
        ]
        d_tasks = [
            asyncio.create_task(detail_worker(i+1, browser, row_q, results, res_lock,
                                               existing_keys, seen_keys, stats))
            for i in range(DETAIL_WORKERS)
        ]

        await model_q.join()
        for _ in l_tasks:
            await model_q.put(None)
        await asyncio.gather(*l_tasks, return_exceptions=True)

        await row_q.join()
        for _ in d_tasks:
            await row_q.put(None)
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
