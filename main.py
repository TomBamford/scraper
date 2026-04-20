from playwright.sync_api import sync_playwright
import pandas as pd
import re
import os
import time

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://autoconduct.com/auction-prices/"
MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

TARGET_ROWS = 150
MIN_PRICE = 500
MAX_PRICE = 10_000

MAX_RUN_SECONDS = 20 * 60
MAX_PAGES = 40
MAX_EMPTY_PAGES = 2

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
]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def clean_price(text):
    text = str(text).replace("$", "").replace(",", "").strip()
    if not text:
        return 0
    if any(x in text.lower() for x in ["no sale", "not sold", "n/a"]):
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return int(float(m.group(1))) if m else 0


def clean_odometer(text):
    text = str(text).replace(",", "").replace('"', "").strip()
    m = re.search(r"(\d+)", text)
    return m.group(1) + " mi" if m else ""


def normalize_make_model(make, model):
    make = make.strip().upper()
    model = model.strip()
    if make == "BMW":
        model = BMW_MODEL_MAP.get(model.upper(), model)
    return make.title(), model


def split_title_with_optional_year(title_line):
    """
    Handles both:
      '2021 KIA K5 LXS'
      'KIA K5 LXS'
    """
    title_line = str(title_line).strip()
    if not title_line:
        return "", "", ""

    parts = title_line.split()
    year = ""

    if parts and re.match(r"^\d{4}$", parts[0]):
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


def infer_type(title):
    t = str(title).upper()
    if any(x in t for x in ["PICKUP", "F-150", "F150", "SILVERADO", "RAM ", "TUNDRA", "RANGER", "TACOMA", "FRONTIER"]):
        return "Truck"
    if any(x in t for x in ["SUV", "EXPLORER", "ESCAPE", "ROGUE", "EQUINOX", "TAHOE", "SUBURBAN",
                              "RAV4", "CR-V", "CX-5", "XC90", "SORENTO", "SPORTAGE", "PILOT",
                              "HIGHLANDER", "PATHFINDER", "TRAVERSE", "EXPEDITION", "SOUL"]):
        return "SUV"
    if any(x in t for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "86", "BRZ"]):
        return "Coupe"
    if any(x in t for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA", "CARAVAN"]):
        return "Van"
    return "Sedan"


def parse_location_state(raw):
    raw = str(raw).strip()
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
    if not os.path.exists(path):
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


def build_paged_url(base_url, page_num):
    if page_num <= 1:
        return base_url
    if "?" in base_url:
        return f"{base_url}&page={page_num}"
    return f"{base_url}?page={page_num}"


def goto_fast(page, url, timeout=12000):
    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    page.wait_for_timeout(300)


def first_nonempty_text(locator):
    try:
        count = min(locator.count(), 10)
    except Exception:
        return ""

    for i in range(count):
        try:
            txt = locator.nth(i).inner_text().strip()
            if txt:
                return txt
        except Exception:
            pass
    return ""


# ─────────────────────────────────────────────
# CARD PARSING
# ─────────────────────────────────────────────
def parse_card_text(text):
    raw = {
        "vin": "",
        "year": "",
        "make": "",
        "model": "",
        "trim": "",
        "damage": "",
        "price_raw": "",
        "odometer": "",
        "lot": "",
        "date": "",
        "location": "",
        "state": "",
        "source": "",
        "title": "",
    }

    text = str(text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    # Title line: first line that is not VIN/date/price/damage/year-only/source
    for line in lines:
        upper = line.upper()
        if (
            "VIN" in upper
            or "$" in upper
            or "COPART" == upper
            or "IAAI" == upper
            or re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", line)
            or re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", line)
        ):
            continue

        # If it's mostly title-like text, take it
        if len(line.split()) >= 2 and not re.fullmatch(r"\d{4}", line):
            raw["title"] = line
            break

    if raw["title"]:
        yr, mk, mo = split_title_with_optional_year(raw["title"])
        if yr:
            raw["year"] = yr
        if mk:
            raw["make"] = mk
        if mo:
            raw["model"] = mo

    # VIN
    m = re.search(r"\bVIN:\s*([A-HJ-NPR-Z0-9]{11,17})\b", text, re.IGNORECASE)
    if m:
        raw["vin"] = m.group(1)
    else:
        m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", text)
        if m:
            raw["vin"] = m.group(1)

    # Year
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        raw["year"] = m.group(1)

    # Odometer: supports "15872 mil" from screenshot
    m = re.search(r"([\d,]+)\s*(?:mil|mi|miles?)\b", text, re.IGNORECASE)
    if m:
        raw["odometer"] = m.group(1).replace(",", "") + " mi"

    # Damage
    upper_text = text.upper()
    for dmg in DAMAGE_PATTERNS:
        if dmg in upper_text:
            raw["damage"] = dmg.title()
            break

    # Price
    for pattern in [
        r"\$\s*([\d,]+)",
        r"[Ss]old\s+[Ff]or\s+\$?\s*([\d,]+)",
        r"[Ff]inal\s+[Bb]id\s+\$?\s*([\d,]+)",
        r"[Pp]rice\s+\$?\s*([\d,]+)",
    ]:
        m = re.search(pattern, text)
        if m:
            raw["price_raw"] = m.group(1)
            break

    # Date
    m = re.search(r"(\d{2}\.\d{2}\.\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|[A-Za-z]+ \d{1,2},?\s*\d{4})", text)
    if m:
        raw["date"] = m.group(1).strip()

    # Location/state if present
    m = re.search(r"([A-Za-z .'-]+),\s*([A-Z]{2})\b", text)
    if m:
        raw["location"] = m.group(1).strip()
        raw["state"] = m.group(2).strip()

    # Auction source
    if "COPART" in upper_text:
        raw["source"] = "Copart"
    elif "IAAI" in upper_text:
        raw["source"] = "IAAI"

    return raw if raw["title"] or raw["vin"] or raw["price_raw"] else None


def extract_rows_from_cards(page):
    """
    Built for the card layout shown in the screenshot.
    """
    rows = []
    try:
        candidate_selectors = [
            "article",
            "[class*='card']",
            "[class*='Card']",
            "[class*='listing']",
            "[class*='Listing']",
            "[class*='vehicle']",
            "[class*='Vehicle']",
            "[class*='result']",
            "[class*='Result']",
        ]

        cards = None
        best_count = 0

        for sel in candidate_selectors:
            try:
                loc = page.locator(sel)
                count = loc.count()
                if count > best_count:
                    best_count = count
                    cards = loc
            except Exception:
                pass

        if not cards or best_count == 0:
            return rows

        count = min(cards.count(), 80)

        for i in range(count):
            try:
                card = cards.nth(i)
                card_text = card.inner_text().strip()
                if not card_text:
                    continue

                upper = card_text.upper()

                # Skip non-vehicle containers
                if "VIN" not in upper and "$" not in upper:
                    continue

                raw = parse_card_text(card_text)
                if not raw:
                    continue

                # Try to improve title using DOM title-like elements inside card
                title_text = first_nonempty_text(
                    card.locator("h1, h2, h3, h4, [class*='title'], [class*='Title'], [class*='name'], [class*='Name']")
                )
                if title_text:
                    raw["title"] = title_text.strip()
                    yr, mk, mo = split_title_with_optional_year(raw["title"])
                    if yr and not raw["year"]:
                        raw["year"] = yr
                    if mk:
                        raw["make"] = mk
                    if mo:
                        raw["model"] = mo

                rows.append(raw)
            except Exception:
                pass

    except Exception as e:
        print(f"[WARN] Card extract error: {e}")

    return rows


def extract_rows_from_page(page, url):
    try:
        goto_fast(page, url)
        return extract_rows_from_cards(page)
    except Exception as e:
        print(f"[WARN] Error loading {url}: {e}")
        return []


# ─────────────────────────────────────────────
# RECORD FILTERING
# ─────────────────────────────────────────────
def build_record(raw, existing_keys, seen, skip_counts):
    price = clean_price(raw.get("price_raw", ""))
    if price < MIN_PRICE or price > MAX_PRICE:
        skip_counts["price_range"] += 1
        return None

    make = raw.get("make", "").strip()
    model = raw.get("model", "").strip()
    year = raw.get("year", "").strip()

    if raw.get("title") and (not year or not make or not model):
        yr, mk, mo = split_title_with_optional_year(raw["title"])
        if yr and not year:
            year = yr
        if mk and not make:
            make = mk
        if mo and not model:
            model = mo

    if is_excluded(make):
        skip_counts["excluded_make"] += 1
        return None

    vin = raw.get("vin", "").strip()
    lot = raw.get("lot", "").strip()

    if not vin and not lot:
        skip_counts["incomplete"] += 1
        return None

    key = f"{vin}|{lot}"
    if key in seen:
        skip_counts["duplicate"] += 1
        return None
    if key in existing_keys:
        skip_counts["existing"] += 1
        return None

    if not year or not make or not model:
        skip_counts["incomplete"] += 1
        return None

    loc_raw = raw.get("location", "").strip()
    state = raw.get("state", "").strip()
    if loc_raw and not state:
        loc_raw, state = parse_location_state(loc_raw)

    record = {
        "vin": vin,
        "year": year,
        "make": make.title(),
        "model": model.title(),
        "trim": raw.get("trim", "").strip(),
        "type": infer_type(f"{make} {model} {raw.get('trim', '')}"),
        "damage": raw.get("damage", "").strip(),
        "price": price,
        "odometer": clean_odometer(raw.get("odometer", "")),
        "lot": lot,
        "date": raw.get("date", "").strip(),
        "location": loc_raw,
        "state": state.upper(),
        "source": raw.get("source", "").strip(),
    }

    seen.add(key)
    existing_keys.add(key)
    skip_counts["kept"] += 1
    return record


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    all_data = []
    seen = set()
    existing_keys = load_existing_keys(MASTER_CSV)

    skip_counts = {
        "price_range": 0,
        "excluded_make": 0,
        "duplicate": 0,
        "existing": 0,
        "incomplete": 0,
        "kept": 0,
    }

    run_start = time.time()

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
            viewport={"width": 1400, "height": 1000},
        )
        page = context.new_page()

        # Block heavy assets but keep CSS/js
        page.route(
            "**/*",
            lambda route, request: (
                route.abort()
                if request.resource_type in {"image", "font", "media"}
                else route.continue_()
            ),
        )

        empty_pages = 0

        for page_num in range(1, MAX_PAGES + 1):
            if len(all_data) >= TARGET_ROWS:
                break

            if (time.time() - run_start) > MAX_RUN_SECONDS:
                print("Runtime limit reached, stopping early.")
                break

            url = build_paged_url(BASE_URL, page_num)
            print(f"\nPage {page_num}: {url}")

            raw_rows = extract_rows_from_page(page, url)
            print(f"Found {len(raw_rows)} raw rows")

            if not raw_rows:
                empty_pages += 1
                if empty_pages >= MAX_EMPTY_PAGES:
                    print("Too many empty pages, stopping.")
                    break
                continue

            empty_pages = 0

            for raw in raw_rows:
                if len(all_data) >= TARGET_ROWS:
                    break

                record = build_record(raw, existing_keys, seen, skip_counts)
                if record:
                    all_data.append(record)
                    print(
                        f"  KEEP {len(all_data)}/{TARGET_ROWS}: "
                        f"{record['year']} {record['make']} {record['model']} | ${record['price']}"
                    )

        browser.close()

    # ─── Write CSVs ───────────────────────────────
    internal_cols = [
        "vin", "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state", "source"
    ]
    website_cols = [
        "year", "make", "model", "trim", "type", "damage",
        "price", "odometer", "lot", "date", "location", "state"
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
        if "price" not in master_df.columns:
            master_df["price"] = 0
        if "make" not in master_df.columns:
            master_df["make"] = ""
        if "vin" not in master_df.columns:
            master_df["vin"] = ""
        if "lot" not in master_df.columns:
            master_df["lot"] = ""

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

    print(f"\n{'=' * 50}")
    print("DONE")
    print(f"New rows this run:      {len(all_data)}")
    print(f"Total in {MASTER_CSV}: {len(master_df)}")
    print(f"Skip stats: {skip_counts}")
    print(f"Elapsed seconds:        {int(time.time() - run_start)}")


if __name__ == "__main__":
    main()
