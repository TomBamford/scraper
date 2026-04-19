from playwright.sync_api import sync_playwright
import pandas as pd
import re
import os
import time

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
BASE_URL        = "https://autoconduct.com/auction-prices/"
MASTER_CSV      = "master.csv"
WEBSITE_CSV     = "cars.csv"

TARGET_ROWS     = 150          # new rows per run — accumulates in master.csv over time
MIN_PRICE       = 500          # skip lots below this
MAX_PRICE       = 10_000       # skip lots above this
MAX_EMPTY_PAGES = 10           # safety stop

EXCLUDED_MAKES  = {"LAND ROVER"}

# Makes to iterate through — autoconduct is organised by /make/model/
# The scraper discovers models automatically from the make index page.
TARGET_MAKES = [
    "toyota", "honda", "ford", "chevrolet", "bmw",
    "mercedes-benz", "tesla", "jeep", "dodge", "nissan",
    "hyundai", "kia", "audi", "volkswagen", "subaru",
    "mazda", "gmc", "ram", "lexus", "acura",
]

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
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


def clean_price(text):
    text = str(text).replace("$", "").replace(",", "").strip()
    if not text:
        return 0
    if any(x in text.lower() for x in ["no sale", "not sold", "n/a"]):
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return int(float(m.group(1))) if m else 0


def clean_odometer(text):
    """Return numeric miles string, stripping ' mi', commas, quotes."""
    text = str(text).replace(",", "").replace('"', "").strip()
    m = re.search(r"(\d+)", text)
    return m.group(1) + " mi" if m else ""


def normalize_make_model(make, model):
    make = make.strip().upper()
    model = model.strip()
    if make == "BMW":
        model = BMW_MODEL_MAP.get(model.upper(), model)
    return make.title(), model


def split_title(title_line):
    title_line = str(title_line).strip()
    parts = title_line.split()
    if not parts or not re.match(r"^\d{4}$", parts[0]):
        return "", "", ""
    year = parts[0]
    rest = " ".join(parts[1:]).strip()
    matched_make = None
    for mk in sorted(MULTI_WORD_MAKES, key=len, reverse=True):
        if rest.upper().startswith(mk + " ") or rest.upper() == mk:
            matched_make = mk
            break
    if matched_make:
        make = matched_make.title()
        model = rest[len(matched_make):].strip()
    else:
        make = parts[1].title() if len(parts) > 1 else ""
        model = " ".join(parts[2:]) if len(parts) > 2 else ""
    make, model = normalize_make_model(make, model)
    return year, make, model


def infer_type(title):
    t = str(title).upper()
    if any(x in t for x in ["PICKUP", "F-150", "F150", "SILVERADO", "RAM ", "TUNDRA", "RANGER", "TACOMA", "FRONTIER"]):
        return "Truck"
    if any(x in t for x in ["SUV", "EXPLORER", "ESCAPE", "ROGUE", "EQUINOX", "TAHOE", "SUBURBAN",
                              "RAV4", "CR-V", "CX-5", "XC90", "SORENTO", "SPORTAGE", "PILOT",
                              "HIGHLANDER", "PATHFINDER", "TRAVERSE", "EXPEDITION"]):
        return "SUV"
    if any(x in t for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "86", "BRZ"]):
        return "Coupe"
    if any(x in t for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA", "CARAVAN"]):
        return "Van"
    return "Sedan"


def parse_location_state(raw):
    raw = str(raw).strip()
    # "City, STATE" or "City - STATE"
    for sep in [", ", " - ", " – "]:
        if sep in raw:
            parts = raw.rsplit(sep, 1)
            if len(parts[1]) == 2 and parts[1].isalpha():
                return parts[0].strip(), parts[1].strip().upper()
    m = re.match(r"^(.*?)\s+([A-Z]{2})$", raw)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return raw, ""


def is_excluded(make):
    return str(make).strip().upper() in EXCLUDED_MAKES


def load_existing_keys(path=MASTER_CSV):
    keys = set()
    if not os.path.exists(path):
        return keys
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        for _, row in df.iterrows():
            vin = row.get("vin", "").strip()
            lot = row.get("lot", "").strip()
            if vin or lot:
                keys.add(f"{vin}|{lot}")
    except Exception as e:
        print(f"[WARN] Could not load existing keys: {e}")
    return keys


def scroll_to_bottom(page, steps=4, delay=400):
    last = 0
    for _ in range(steps):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(delay)
        h = page.evaluate("document.body.scrollHeight")
        if h == last:
            break
        last = h


# ─────────────────────────────────────────────
#  AUTOCONDUCT SCRAPING LOGIC
# ─────────────────────────────────────────────

def get_model_urls(page, make_slug):
    """
    Visit /auction-prices/<make>/ and collect all model sub-page links.
    e.g. /auction-prices/toyota/camry/
    """
    url = f"{BASE_URL}{make_slug}/"
    urls = []
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(1500)
        scroll_to_bottom(page)
        links = page.locator(f"a[href*='/auction-prices/{make_slug}/']")
        count = links.count()
        for i in range(count):
            try:
                href = links.nth(i).get_attribute("href") or ""
                # only model-level links (exactly 4 path segments)
                clean = href.rstrip("/")
                parts = [p for p in clean.split("/") if p]
                if len(parts) == 3 and parts[0] == "auction-prices":
                    full = "https://autoconduct.com" + href if href.startswith("/") else href
                    if full not in urls:
                        urls.append(full)
            except Exception:
                pass
    except Exception as e:
        print(f"[WARN] Could not load make page {url}: {e}")
    return urls


def extract_rows_from_model_page(page, model_url, make_hint, model_hint):
    """
    Load a model results page and extract all table rows.
    Returns list of dicts with raw field values.
    """
    rows = []
    try:
        page.goto(model_url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(1500)
        scroll_to_bottom(page)

        # Try to find a data table — autoconduct renders an HTML table with auction results
        # Common selectors: table, .auction-table, [class*="table"], [class*="results"]
        table = None
        for sel in ["table", ".auction-results", "[class*='auction']", "[class*='table']", "[class*='results']"]:
            if page.locator(sel).count() > 0:
                table = sel
                break

        if table:
            rows = extract_table_rows(page, table, make_hint, model_hint)
        else:
            # Fallback: parse full page text
            rows = extract_rows_from_text(page, make_hint, model_hint)

    except Exception as e:
        print(f"[WARN] Error loading {model_url}: {e}")

    return rows


def extract_table_rows(page, table_sel, make_hint, model_hint):
    """Extract rows from an HTML table element."""
    rows = []
    try:
        # Get all header cells to map column positions
        headers = []
        header_cells = page.locator(f"{table_sel} th")
        for i in range(header_cells.count()):
            headers.append(header_cells.nth(i).inner_text().strip().lower())

        # Map header names to our field names
        col_map = {}
        for i, h in enumerate(headers):
            if any(x in h for x in ["year", "yr"]):
                col_map["year"] = i
            elif any(x in h for x in ["make"]):
                col_map["make"] = i
            elif any(x in h for x in ["model"]):
                col_map["model"] = i
            elif any(x in h for x in ["trim"]):
                col_map["trim"] = i
            elif any(x in h for x in ["damage", "primary"]):
                col_map["damage"] = i
            elif any(x in h for x in ["price", "sale", "bid", "sold"]):
                col_map["price"] = i
            elif any(x in h for x in ["odometer", "odo", "mile", "mileage"]):
                col_map["odometer"] = i
            elif any(x in h for x in ["lot"]):
                col_map["lot"] = i
            elif any(x in h for x in ["vin"]):
                col_map["vin"] = i
            elif any(x in h for x in ["date", "auction date", "sale date"]):
                col_map["date"] = i
            elif any(x in h for x in ["location", "loc", "city", "yard"]):
                col_map["location"] = i
            elif any(x in h for x in ["state"]):
                col_map["state"] = i
            elif any(x in h for x in ["source", "auction", "auction house"]):
                col_map["source"] = i
            elif any(x in h for x in ["title", "vehicle", "car", "name"]):
                col_map["title"] = i

        tr_list = page.locator(f"{table_sel} tbody tr")
        count = tr_list.count()

        for i in range(count):
            try:
                cells = tr_list.nth(i).locator("td")
                cell_count = cells.count()
                if cell_count < 2:
                    continue

                def cell(key):
                    idx = col_map.get(key, -1)
                    if idx < 0 or idx >= cell_count:
                        return ""
                    return cells.nth(idx).inner_text().strip()

                # Build raw row — fill from table then infer what we can
                raw = {
                    "vin":      cell("vin"),
                    "year":     cell("year"),
                    "make":     cell("make") or make_hint,
                    "model":    cell("model") or model_hint,
                    "trim":     cell("trim"),
                    "damage":   cell("damage"),
                    "price_raw":cell("price"),
                    "odometer": cell("odometer"),
                    "lot":      cell("lot"),
                    "date":     cell("date"),
                    "location": cell("location"),
                    "state":    cell("state"),
                    "source":   cell("source"),
                    "title":    cell("title"),
                }

                # If we got a combined title/vehicle column, parse it
                if raw["title"] and (not raw["year"] or not raw["make"] or not raw["model"]):
                    yr, mk, mo = split_title(raw["title"])
                    if yr: raw["year"]  = yr
                    if mk: raw["make"]  = mk
                    if mo: raw["model"] = mo

                # If location contains state (e.g. "Dallas, TX")
                if raw["location"] and not raw["state"]:
                    loc, st = parse_location_state(raw["location"])
                    raw["location"] = loc
                    raw["state"]    = st

                rows.append(raw)
            except Exception as e:
                print(f"[WARN] Row parse error: {e}")

    except Exception as e:
        print(f"[WARN] Table extract error: {e}")

    return rows


def extract_rows_from_text(page, make_hint, model_hint):
    """
    Fallback: parse visible text for price/date/damage patterns.
    Used when no <table> is found (card-layout pages).
    """
    rows = []
    try:
        # Look for card/list item containers
        card_selectors = [
            ".auction-card", ".result-card", ".vehicle-card",
            "[class*='card']", "[class*='item']", "[class*='listing']",
            "article", ".row-item",
        ]
        cards = None
        for sel in card_selectors:
            loc = page.locator(sel)
            if loc.count() > 2:
                cards = loc
                break

        if not cards:
            return rows

        count = cards.count()
        for i in range(count):
            try:
                text = cards.nth(i).inner_text()
                raw = parse_text_block(text, make_hint, model_hint)
                if raw:
                    rows.append(raw)
            except Exception:
                pass
    except Exception as e:
        print(f"[WARN] Text extract error: {e}")
    return rows


def parse_text_block(text, make_hint, model_hint):
    """Parse a freeform text block from a card element."""
    raw = {
        "vin": "", "year": "", "make": make_hint, "model": model_hint,
        "trim": "", "damage": "", "price_raw": "", "odometer": "",
        "lot": "", "date": "", "location": "", "state": "", "source": "", "title": "",
    }

    # Year
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        raw["year"] = m.group(1)

    # Title / full vehicle name
    m = re.search(r"\b(19\d{2}|20\d{2})\s+[A-Za-z][\w ./-]{2,50}", text)
    if m:
        raw["title"] = m.group(0).strip()
        yr, mk, mo = split_title(raw["title"])
        if yr: raw["year"]  = yr
        if mk: raw["make"]  = mk
        if mo: raw["model"] = mo

    # VIN
    m = re.search(r"\b([A-HJ-NPR-Z0-9]{11,17})\b", text)
    if m:
        raw["vin"] = m.group(1)

    # Lot
    m = re.search(r"[Ll]ot\s*#?\s*([A-Za-z0-9-]+)", text)
    if m:
        raw["lot"] = m.group(1)

    # Price
    for pattern in [r"\$\s?([\d,]+)", r"[Ss]old\s+[Ff]or\s+\$?\s?([\d,]+)",
                    r"[Ff]inal\s+[Bb]id\s+\$?\s?([\d,]+)", r"[Pp]rice\s+\$?\s?([\d,]+)"]:
        m = re.search(pattern, text)
        if m:
            raw["price_raw"] = m.group(1)
            break

    # Odometer
    m = re.search(r"([\d,]+)\s*mi(?:les?)?", text, re.IGNORECASE)
    if m:
        raw["odometer"] = m.group(1).replace(",", "") + " mi"

    # Damage
    for pattern in [r"[Dd]amage\s*[:\-]\s*([A-Za-z &/]+)",
                    r"[Pp]rimary\s+[Dd]amage\s*[:\-]\s*([A-Za-z &/]+)"]:
        m = re.search(pattern, text)
        if m:
            raw["damage"] = m.group(1).strip()
            break

    # Date
    m = re.search(r"([A-Za-z]+ \d{1,2},?\s*\d{4}|\d{1,2}/\d{1,2}/\d{2,4})", text)
    if m:
        raw["date"] = m.group(1).strip()

    # Location / state
    m = re.search(r"([A-Za-z ]+),\s*([A-Z]{2})\b", text)
    if m:
        raw["location"] = m.group(1).strip()
        raw["state"]    = m.group(2).strip()

    return raw if raw["price_raw"] or raw["vin"] else None


def paginate_and_collect(page, model_url, make_hint, model_hint, existing_keys, seen, all_data, skip_counts):
    """
    For a given model URL, iterate through all result pages collecting rows.
    Stops when TARGET_ROWS is reached or no next page found.
    """
    page_num = 1
    empty_pages = 0

    # Autoconduct pagination: try appending ?page=N or /page/N/ or clicking Next
    current_url = model_url

    while True:
        if len(all_data) >= TARGET_ROWS:
            break

        print(f"  Page {page_num} — {model_url}")
        before = len(all_data)

        raw_rows = extract_rows_from_model_page(page, current_url, make_hint, model_hint)
        print(f"  Found {len(raw_rows)} raw rows")

        for raw in raw_rows:
            if len(all_data) >= TARGET_ROWS:
                break

            # Clean and validate
            price = clean_price(raw.get("price_raw", ""))
            if price < MIN_PRICE or price > MAX_PRICE:
                skip_counts["price_range"] += 1
                continue

            make = raw.get("make", make_hint).strip()
            if is_excluded(make):
                skip_counts["excluded_make"] += 1
                continue

            vin = raw.get("vin", "").strip()
            lot = raw.get("lot", "").strip()
            key = f"{vin}|{lot}"

            if key in seen:
                skip_counts["duplicate"] += 1
                continue
            if key != "|" and key in existing_keys:
                skip_counts["existing"] += 1
                continue

            year  = raw.get("year", "").strip()
            model = raw.get("model", model_hint).strip()

            if not year or not make or not model:
                skip_counts["incomplete"] += 1
                continue

            # Infer type from make+model
            car_type = infer_type(f"{make} {model} {raw.get('trim','')}")

            loc_raw = raw.get("location", "")
            state   = raw.get("state", "")
            if loc_raw and not state:
                loc_raw, state = parse_location_state(loc_raw)

            record = {
                "vin":      vin,
                "year":     year,
                "make":     make.title(),
                "model":    model.title(),
                "trim":     raw.get("trim", "").strip(),
                "type":     car_type,
                "damage":   raw.get("damage", "").strip(),
                "price":    price,
                "odometer": clean_odometer(raw.get("odometer", "")),
                "lot":      lot,
                "date":     raw.get("date", "").strip(),
                "location": loc_raw.strip(),
                "state":    state.strip().upper(),
                "source":   raw.get("source", "").strip(),
            }

            seen.add(key)
            if key != "|":
                existing_keys.add(key)
            all_data.append(record)
            skip_counts["kept"] += 1
            print(f"    KEEP {len(all_data)}/{TARGET_ROWS}: {year} {make} {model} | ${price}")

        added = len(all_data) - before
        if added == 0:
            empty_pages += 1
            if empty_pages >= MAX_EMPTY_PAGES:
                print("  Too many empty pages, moving on.")
                break
        else:
            empty_pages = 0

        # Try to go to next page
        moved = try_next_page(page, page_num)
        if not moved:
            break
        page_num += 1
        current_url = page.url  # update after navigation


def try_next_page(page, current_num):
    """Try various strategies to click to the next page."""
    next_num = str(current_num + 1)
    selectors = [
        f"a:has-text('{next_num}')",
        f"button:has-text('{next_num}')",
        "a:has-text('Next')",
        "button:has-text('Next')",
        "a[rel='next']",
        "[aria-label='Next page']",
        "[aria-label='next']",
        ".pagination .next a",
        ".next-page",
    ]
    for sel in selectors:
        loc = page.locator(sel)
        if loc.count() > 0:
            try:
                loc.first.click()
                page.wait_for_load_state("domcontentloaded", timeout=10000)
                page.wait_for_timeout(1000)
                return True
            except Exception:
                pass

    # Fallback: try URL parameter ?page=N
    try:
        current_url = page.url
        if "page=" in current_url:
            new_url = re.sub(r"page=\d+", f"page={current_num+1}", current_url)
        elif "?" in current_url:
            new_url = current_url + f"&page={current_num+1}"
        else:
            new_url = current_url.rstrip("/") + f"?page={current_num+1}"

        page.goto(new_url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(2000)
        # If page content is same as before (no results), return False
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    all_data = []
    seen = set()
    existing_keys = load_existing_keys(MASTER_CSV)

    skip_counts = {
        "price_range":   0,
        "excluded_make": 0,
        "duplicate":     0,
        "existing":      0,
        "incomplete":    0,
        "kept":          0,
    }

    RUN_START = time.time()
    MAX_RUN_SECONDS = 45 * 60  # stop after 45 min — well inside the 55 min GitHub limit

    print(f"Existing records: {len(existing_keys)}")
    print(f"Target new rows:  {TARGET_ROWS}")
    print(f"Price filter:     ${MIN_PRICE} – ${MAX_PRICE}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        for make_slug in TARGET_MAKES:
            if len(all_data) >= TARGET_ROWS:
                break
            if (time.time() - RUN_START) > MAX_RUN_SECONDS:
                print("45 min limit reached — stopping early to allow clean commit.")
                break

            print(f"\n{'='*50}")
            print(f"Make: {make_slug.upper()}")

            model_urls = get_model_urls(page, make_slug)
            print(f"Found {len(model_urls)} models")

            if not model_urls:
                # Fallback: go straight to the make page and scrape it
                model_urls = [f"{BASE_URL}{make_slug}/"]

            for model_url in model_urls:
                if len(all_data) >= TARGET_ROWS:
                    break
                if (time.time() - RUN_START) > MAX_RUN_SECONDS:
                    break

                # Extract model hint from URL: .../toyota/camry/ → "Camry"
                slug_parts = model_url.rstrip("/").split("/")
                model_hint = slug_parts[-1].replace("-", " ").title() if slug_parts else ""
                make_hint  = make_slug.replace("-", " ").title()

                print(f"\n  Model: {model_hint}")
                paginate_and_collect(
                    page, model_url, make_hint, model_hint,
                    existing_keys, seen, all_data, skip_counts
                )

        browser.close()

    # ─── Write CSVs ───────────────────────────────
    internal_cols = ["vin","year","make","model","trim","type","damage",
                     "price","odometer","lot","date","location","state","source"]
    website_cols  = ["year","make","model","trim","type","damage",
                     "price","odometer","lot","date","location","state"]

    # Load / merge master
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
        master_df = master_df[master_df["price"].between(MIN_PRICE, MAX_PRICE)]
        master_df = master_df[~master_df["make"].str.upper().isin(EXCLUDED_MAKES)]
        master_df = master_df.drop_duplicates(subset=["vin", "lot"], keep="first")

    master_df.to_csv(MASTER_CSV, index=False)

    website_df = master_df.copy()
    for col in website_cols:
        if col not in website_df.columns:
            website_df[col] = ""
    website_df = website_df[website_cols]
    website_df.to_csv(WEBSITE_CSV, index=False)

    print(f"\n{'='*50}")
    print("DONE")
    print(f"New rows this run:      {len(all_data)}")
    print(f"Total in {MASTER_CSV}: {len(master_df)}")
    print(f"Skip stats: {skip_counts}")


if __name__ == "__main__":
    main()
 
 
if __name__ == "__main__":
    main()
