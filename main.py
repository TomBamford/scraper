import asyncio
import os
import re
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
from playwright.async_api import async_playwright


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://carsbidshistory.com/"
MAKE_LIST_PAGES = 35          # public make index shows 35 pages
YEAR_MIN = 2008
YEAR_MAX = 2015
MIN_PRICE = 500

MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

RESUME_FROM_MASTER = True

MAKE_WORKERS = 4
MODEL_WORKERS = 8
DETAIL_WORKERS = 12

# Optional narrowing. Leave empty to crawl all.
TARGET_MAKES = set()          # e.g. {"TOYOTA", "HONDA", "FORD"}
TARGET_MODELS = set()         # e.g. {"CAMRY", "CIVIC", "ACCORD"}

EXCLUDED_MAKES = {"LAND ROVER"}

BMW_MODEL_MAP = {
    "1ER": "1 Series", "2ER": "2 Series", "3ER": "3 Series",
    "4ER": "4 Series", "5ER": "5 Series", "6ER": "6 Series",
    "7ER": "7 Series", "8ER": "8 Series",
    "X1": "X1", "X2": "X2", "X3": "X3", "X4": "X4",
    "X5": "X5", "X6": "X6", "X7": "X7",
    "Z3": "Z3", "Z4": "Z4",
    "M2": "M2", "M3": "M3", "M4": "M4", "M5": "M5", "M6": "M6",
    "I3": "i3", "I4": "i4", "I7": "i7", "IX": "iX",
}

MULTI_WORD_MAKES = {
    "LAND ROVER", "ALFA ROMEO", "ASTON MARTIN",
    "MERCEDES BENZ", "MERCEDES-BENZ", "ROLLS ROYCE", "GENERAL MOTORS",
}

DAMAGE_PATTERNS = [
    "FRONT END",
    "REAR END",
    "SIDE",
    "ROLLOVER",
    "ALL OVER",
    "WATER/FLOOD",
    "MECHANICAL",
    "MINOR DENT/SCRATCHES",
    "NORMAL WEAR",
    "UNDERCARRIAGE",
    "STRIPPED",
    "BURN",
    "TOP/ROOF",
    "HAIL",
    "VANDALISM",
    "BIOHAZARD/CHEMICAL",
    "BURN - INTERIOR",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()


def clean_price(text) -> int:
    text = str(text).replace("$", "").replace(",", "").strip()
    if not text:
        return 0
    if any(x in text.lower() for x in ["no sale", "not sold", "n/a"]):
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return int(float(m.group(1))) if m else 0


def clean_odometer(text: str) -> str:
    text = str(text).replace(",", "").replace('"', "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if not m:
        return ""
    return str(int(float(m.group(1)))) + " mi"


def normalize_make_model(make: str, model: str):
    make = str(make).strip().upper()
    model = str(model).strip()
    if make == "BMW":
        model = BMW_MODEL_MAP.get(model.upper(), model)
    return make.title(), model


def split_title_with_optional_year(title_line: str):
    """
    Handles:
      '2010 TOYOTA PRIUS'
      'TOYOTA PRIUS'
    """
    title_line = normalize_whitespace(title_line)
    if not title_line:
        return "", "", ""

    parts = title_line.split()
    year = ""

    if parts and re.fullmatch(r"\d{4}", parts[0]):
        year = parts[0]
        rest_parts = parts[1:]
    else:
        rest_parts = parts

    if not rest_parts:
        return year, "", ""

    rest = " ".join(rest_parts).strip()
    matched_make = None

    for mk in sorted(MULTI_WORD_MAKES, key=len, reverse=True):
        if rest.upper().startswith(mk + " ") or rest.upper() == mk:
            matched_make = mk
            break

    if matched_make:
        make = matched_make.title()
        model = rest[len(matched_make):].strip()
    else:
        make = rest_parts[0].title() if len(rest_parts) > 0 else ""
        model = " ".join(rest_parts[1:]) if len(rest_parts) > 1 else ""

    make, model = normalize_make_model(make, model)
    return year, make, model


def infer_type(title: str) -> str:
    t = str(title).upper()
    if any(x in t for x in ["PICKUP", "F-150", "F150", "SILVERADO", "RAM ", "TUNDRA", "RANGER", "TACOMA", "FRONTIER"]):
        return "Truck"
    if any(x in t for x in ["SUV", "EXPLORER", "ESCAPE", "ROGUE", "EQUINOX", "TAHOE", "SUBURBAN",
                              "RAV4", "CR-V", "CX-5", "XC90", "SORENTO", "SPORTAGE", "PILOT",
                              "HIGHLANDER", "PATHFINDER", "TRAVERSE", "EXPEDITION", "SOUL"]):
        return "SUV"
    if any(x in t for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "86", "BRZ"]):
        return "Coupe"
    if any(x in t for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA", "CARAVAN", "ECONOLINE", "EXPRESS 35"]):
        return "Van"
    return "Sedan"


def parse_location_state(raw: str):
    raw = str(raw).strip()
    for sep in [", ", " - ", " – ", " / "]:
        if sep in raw:
            parts = raw.rsplit(sep, 1)
            if len(parts[1]) == 2 and parts[1].isalpha():
                return parts[0].strip(), parts[1].strip().upper()
    m = re.match(r"^(.*?)\s+([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return raw, ""


def is_excluded(make: str) -> bool:
    return str(make).strip().upper() in EXCLUDED_MAKES


def year_in_range(year_text: str) -> bool:
    try:
        y = int(str(year_text).strip())
        return YEAR_MIN <= y <= YEAR_MAX
    except Exception:
        return False


def canonical_url(url: str) -> str:
    return url.rstrip("/")


def path_segments(url: str):
    return [x for x in urlparse(url).path.strip("/").split("/") if x]


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

    if raw.get("title") and (not year or not make or not model):
        yr, mk, mo = split_title_with_optional_year(raw["title"])
        if yr and not year:
            year = yr
        if mk and not make:
            make = mk
        if mo and not model:
            model = mo

    if not year_in_range(year):
        stats["year_out_of_range"] += 1
        return None

    if is_excluded(make):
        stats["excluded_make"] += 1
        return None

    if TARGET_MAKES and make.upper() not in TARGET_MAKES:
        stats["filtered_make"] += 1
        return None

    if TARGET_MODELS and model.upper() not in TARGET_MODELS:
        stats["filtered_model"] += 1
        return None

    vin = str(raw.get("vin", "")).strip()
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
        "source": str(raw.get("source", "")).strip(),
        "url": str(raw.get("url", "")).strip(),
    }

    seen_keys.add(key)
    existing_keys.add(key)
    stats["kept"] += 1
    return record


# ─────────────────────────────────────────────
# URL CLASSIFIERS
# ─────────────────────────────────────────────
def is_make_url(url: str) -> bool:
    segs = path_segments(url)
    return len(segs) == 2 and segs[0] == "make" and re.match(r"^\d+-", segs[1]) is not None


def is_model_url(url: str) -> bool:
    segs = path_segments(url)
    return len(segs) == 3 and segs[0] == "make" and re.match(r"^\d+-", segs[1]) and re.match(r"^\d+-", segs[2])


def is_detail_url(url: str) -> bool:
    segs = path_segments(url)
    if len(segs) != 4 or segs[0] != "make":
        return False
    if not re.match(r"^\d+-", segs[1]) or not re.match(r"^\d+-", segs[2]):
        return False
    return re.match(r"^(19|20)\d{2}_", segs[3]) is not None


def detail_year_from_url(url: str):
    segs = path_segments(url)
    if len(segs) != 4:
        return ""
    m = re.match(r"^((?:19|20)\d{2})_", segs[3])
    return m.group(1) if m else ""


# ─────────────────────────────────────────────
# BROWSER EXTRACTION
# ─────────────────────────────────────────────
async def new_context(browser):
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1400, "height": 1000},
    )
    await context.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in {"image", "font", "media"}
        else route.continue_()
    )
    return context


async def goto_fast(page, url: str, timeout=20000):
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    await page.wait_for_timeout(200)


async def extract_links(page, url: str):
    await goto_fast(page, url)
    hrefs = await page.evaluate("""
    () => Array.from(document.querySelectorAll("a[href]"))
      .map(a => a.href)
      .filter(Boolean)
    """)
    out = []
    for h in hrefs:
        if h.startswith("https://carsbidshistory.com/"):
            out.append(canonical_url(h))
    return list(dict.fromkeys(out))


async def extract_detail_data(page, url: str):
    await goto_fast(page, url)
    text = await page.locator("body").inner_text()

    title = ""
    for sel in ["h1", "title"]:
        try:
            if sel == "title":
                title = await page.title()
            else:
                if await page.locator(sel).count() > 0:
                    title = (await page.locator(sel).first.inner_text()).strip()
            if title:
                break
        except Exception:
            pass

    raw = {
        "title": "",
        "year": "",
        "make": "",
        "model": "",
        "trim": "",
        "damage": "",
        "price": "",
        "odometer": "",
        "lot": "",
        "date": "",
        "location": "",
        "state": "",
        "source": "",
        "vin": "",
        "url": url,
    }

    norm_title = normalize_whitespace(title)
    if norm_title:
        raw["title"] = norm_title

    m = re.search(r"\b((?:19|20)\d{2})\b", norm_title or text)
    if m:
        raw["year"] = m.group(1)

    yr, mk, mo = split_title_with_optional_year(norm_title)
    if mk:
        raw["make"] = mk
    if mo:
        raw["model"] = mo
    if yr and not raw["year"]:
        raw["year"] = yr

    # URL fallback is very reliable on this site
    year_from_url = detail_year_from_url(url)
    if year_from_url and not raw["year"]:
        raw["year"] = year_from_url

    m = re.search(r"\bVIN\b[:\s]+([A-HJ-NPR-Z0-9]{11,17})\b", text, re.IGNORECASE)
    if m:
        raw["vin"] = m.group(1)

    m = re.search(r"\bLot\b[:\s]+([A-Z0-9-]+)\b", text, re.IGNORECASE)
    if m:
        raw["lot"] = m.group(1)
    else:
        # URL often contains lot before VIN
        segs = path_segments(url)
        if len(segs) == 4:
            parts = segs[3].split("_")
            if len(parts) >= 3:
                maybe_lot = parts[-2]
                if re.fullmatch(r"\d{6,}", maybe_lot):
                    raw["lot"] = maybe_lot

    m = re.search(r"\bOdometer\b[:\s]+([\d,.]+)", text, re.IGNORECASE)
    if m:
        raw["odometer"] = m.group(1)

    for dmg in DAMAGE_PATTERNS:
        if dmg in text.upper():
            raw["damage"] = dmg.title()
            break

    # Prefer "Last Bid"
    m = re.search(r"\bLast Bid\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if m:
        raw["price"] = m.group(1)
    else:
        m = re.search(r"\bSold Status\b.*?\b([\d,]+(?:\.\d+)?)\b", text, re.IGNORECASE | re.DOTALL)
        if m:
            raw["price"] = m.group(1)

    # Auction/source
    if "Copart" in text:
        raw["source"] = "Copart"
    elif "IAAI" in text:
        raw["source"] = "IAAI"

    # Date
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    if m:
        raw["date"] = m.group(1)

    # Location
    m = re.search(r"\bBuyer Location\b[:\s]+([^\n]+)", text, re.IGNORECASE)
    if m:
        raw["location"] = normalize_whitespace(m.group(1))
    else:
        m = re.search(r"\bLocation\b[:\s]+([^\n]+)", text, re.IGNORECASE)
        if m:
            raw["location"] = normalize_whitespace(m.group(1))

    return raw


# ─────────────────────────────────────────────
# DISCOVERY
# ─────────────────────────────────────────────
async def seed_make_urls(browser):
    make_urls = set()
    context = await new_context(browser)
    page = await context.new_page()
    try:
        for p in range(1, MAKE_LIST_PAGES + 1):
            url = urljoin(BASE_URL, f"make/page-{p}")
            try:
                links = await extract_links(page, url)
                for link in links:
                    if is_make_url(link):
                        make_urls.add(link)
                print(f"[seed] make page {p}: total makes so far {len(make_urls)}")
            except Exception as e:
                print(f"[seed] make page {p} error: {e}")
    finally:
        await page.close()
        await context.close()
    return sorted(make_urls)


async def make_worker(worker_id, browser, in_q, model_urls, model_urls_lock, seen_make_urls):
    context = await new_context(browser)
    page = await context.new_page()
    try:
        while True:
            url = await in_q.get()
            if url is None:
                in_q.task_done()
                break

            try:
                links = await extract_links(page, url)
                discovered = 0
                async with model_urls_lock:
                    for link in links:
                        if is_model_url(link) and link not in model_urls:
                            model_urls.add(link)
                            discovered += 1
                print(f"[make W{worker_id}] {url} -> +{discovered} models")
            except Exception as e:
                print(f"[make W{worker_id}] error {url}: {e}")
            finally:
                in_q.task_done()
    finally:
        await page.close()
        await context.close()


async def crawl_model_pages(browser, model_urls):
    """
    For each model page, crawl pagination within that model section and collect detail URLs.
    """
    detail_urls = set()
    seen_model_pages = set()
    q = asyncio.Queue()
    for u in model_urls:
        await q.put(canonical_url(u))
        seen_model_pages.add(canonical_url(u))

    detail_lock = asyncio.Lock()
    model_lock = asyncio.Lock()

    async def worker(worker_id):
        context = await new_context(browser)
        page = await context.new_page()
        try:
            while True:
                url = await q.get()
                if url is None:
                    q.task_done()
                    break

                try:
                    links = await extract_links(page, url)

                    new_detail = 0
                    new_model_pages = 0

                    async with detail_lock:
                        for link in links:
                            if is_detail_url(link):
                                y = detail_year_from_url(link)
                                if year_in_range(y) and link not in detail_urls:
                                    detail_urls.add(link)
                                    new_detail += 1

                    # pagination / related model pages inside same model root
                    root = "/".join(path_segments(url)[:3])
                    root_prefix = urljoin(BASE_URL, root + "/")
                    async with model_lock:
                        for link in links:
                            if link.startswith(root_prefix) and not is_detail_url(link) and link not in seen_model_pages:
                                seen_model_pages.add(link)
                                await q.put(link)
                                new_model_pages += 1

                    print(f"[model W{worker_id}] {url} -> +{new_detail} details, +{new_model_pages} pages")

                except Exception as e:
                    print(f"[model W{worker_id}] error {url}: {e}")
                finally:
                    q.task_done()
        finally:
            await page.close()
            await context.close()

    tasks = [asyncio.create_task(worker(i + 1)) for i in range(MODEL_WORKERS)]
    await q.join()
    for _ in tasks:
        await q.put(None)
    await asyncio.gather(*tasks, return_exceptions=True)

    return sorted(detail_urls)


async def scrape_detail_urls(browser, detail_urls, existing_keys, stats):
    data = []
    seen_keys = set()
    q = asyncio.Queue()
    for u in detail_urls:
        await q.put(u)

    data_lock = asyncio.Lock()

    async def worker(worker_id):
        context = await new_context(browser)
        page = await context.new_page()
        try:
            while True:
                url = await q.get()
                if url is None:
                    q.task_done()
                    break
                try:
                    raw = await extract_detail_data(page, url)
                    async with data_lock:
                        record = build_record(raw, existing_keys, seen_keys, stats)
                        if record:
                            data.append(record)
                            if len(data) % 100 == 0:
                                print(f"[detail W{worker_id}] kept {len(data)}")
                except Exception as e:
                    print(f"[detail W{worker_id}] error {url}: {e}")
                finally:
                    q.task_done()
        finally:
            await page.close()
            await context.close()

    tasks = [asyncio.create_task(worker(i + 1)) for i in range(DETAIL_WORKERS)]
    await q.join()
    for _ in tasks:
        await q.put(None)
    await asyncio.gather(*tasks, return_exceptions=True)

    return data


# ─────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────
def write_csvs(all_data: list):
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
        for col in ["price", "make", "vin", "lot", "year"]:
            if col not in master_df.columns:
                master_df[col] = ""

        master_df["price"] = pd.to_numeric(master_df["price"], errors="coerce").fillna(0).astype(int)
        master_df["year_num"] = pd.to_numeric(master_df["year"], errors="coerce")
        master_df = master_df[master_df["price"] >= MIN_PRICE]
        master_df = master_df[master_df["year_num"].between(YEAR_MIN, YEAR_MAX, inclusive="both")]
        master_df = master_df[~master_df["make"].str.upper().isin(EXCLUDED_MAKES)]
        master_df = master_df.drop_duplicates(subset=["vin", "lot"], keep="first")
        master_df = master_df.drop(columns=["year_num"])

    master_df.to_csv(MASTER_CSV, index=False)

    website_df = master_df.copy()
    for col in website_cols:
        if col not in website_df.columns:
            website_df[col] = ""
    website_df = website_df[website_df[website_cols[0:]].columns.intersection(website_cols)]
    website_df = website_df.reindex(columns=website_cols, fill_value="")
    website_df.to_csv(WEBSITE_CSV, index=False)

    return len(master_df)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    run_start = time.time()
    existing_keys = load_existing_keys(MASTER_CSV)

    stats = {
        "price_below_min": 0,
        "year_out_of_range": 0,
        "excluded_make": 0,
        "filtered_make": 0,
        "filtered_model": 0,
        "duplicate": 0,
        "existing": 0,
        "incomplete": 0,
        "kept": 0,
    }

    print(f"Year range:        {YEAR_MIN}-{YEAR_MAX}")
    print(f"Minimum price:     ${MIN_PRICE}")
    print(f"Resume existing:   {RESUME_FROM_MASTER}")
    print(f"Existing records:  {len(existing_keys)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # Step 1: discover make URLs
        make_urls = await seed_make_urls(browser)
        print(f"Discovered make URLs: {len(make_urls)}")

        # Step 2: discover model URLs from make pages
        make_q = asyncio.Queue()
        for u in make_urls:
            await make_q.put(u)

        model_urls = set()
        model_urls_lock = asyncio.Lock()
        seen_make_urls = set(make_urls)

        make_tasks = [
            asyncio.create_task(
                make_worker(i + 1, browser, make_q, model_urls, model_urls_lock, seen_make_urls)
            )
            for i in range(MAKE_WORKERS)
        ]
        await make_q.join()
        for _ in make_tasks:
            await make_q.put(None)
        await asyncio.gather(*make_tasks, return_exceptions=True)

        model_urls = sorted(model_urls)
        print(f"Discovered model URLs: {len(model_urls)}")

        # Optional model narrowing by slug text
        if TARGET_MODELS:
            filtered = []
            for u in model_urls:
                seg = path_segments(u)[-1]
                label = seg.split("-", 1)[-1].replace("-", " ").upper()
                if any(t in label for t in TARGET_MODELS):
                    filtered.append(u)
            model_urls = filtered
            print(f"After TARGET_MODELS filter: {len(model_urls)} model URLs")

        # Step 3: crawl model pages and discover detail URLs
        detail_urls = await crawl_model_pages(browser, model_urls)
        print(f"Discovered detail URLs in year range: {len(detail_urls)}")

        # Step 4: scrape detail pages
        all_data = await scrape_detail_urls(browser, detail_urls, existing_keys, stats)

        await browser.close()

    total_master = write_csvs(all_data)

    print("\n" + "=" * 60)
    print("DONE")
    print(f"New rows this run:      {len(all_data)}")
    print(f"Total in {MASTER_CSV}: {total_master}")
    print(f"Skip stats:            {stats}")
    print(f"Elapsed seconds:       {int(time.time() - run_start)}")


if __name__ == "__main__":
    asyncio.run(main())
