from playwright.sync_api import sync_playwright
import pandas as pd
import re
import os

URL = "https://bidstreamline.com/catalog"

# Stop when this many NEW matching rows are collected
TARGET_ROWS = 500

# Optional hard cap. Set to None to keep going until target or no more pages.
MAX_PAGES = None

DETAIL_LOOKUPS = True  # True = open detail pages for better price/date/damage extraction

MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

# Keep only sold cars with a usable price
REQUIRE_PRICE = True
REQUIRE_SOLD_DATE = False
SKIP_UNSOLD = True
MIN_PRICE = 500  # only count prices >= 500 as valid

# Only keep California lots
STATE_FILTER = {"CA", "CALIFORNIA"}

# Safety stop: if this many pages in a row add 0 rows, stop
MAX_EMPTY_PAGES = 15

BMW_MODEL_MAP = {
    "1ER": "1 Series",
    "2ER": "2 Series",
    "3ER": "3 Series",
    "4ER": "4 Series",
    "5ER": "5 Series",
    "6ER": "6 Series",
    "7ER": "7 Series",
    "8ER": "8 Series",
    "X1": "X1",
    "X2": "X2",
    "X3": "X3",
    "X4": "X4",
    "X5": "X5",
    "X6": "X6",
    "X7": "X7",
    "Z3": "Z3",
    "Z4": "Z4",
    "M2": "M2",
    "M3": "M3",
    "M4": "M4",
    "M5": "M5",
    "M6": "M6",
    "I3": "i3",
    "I4": "i4",
    "I7": "i7",
    "IX": "iX",
}

MULTI_WORD_MAKES = {
    "LAND ROVER",
    "ALFA ROMEO",
    "ASTON MARTIN",
    "MERCEDES BENZ",
    "ROLLS ROYCE",
    "GENERAL MOTORS",
}


def clean_price(text):
    text = str(text).replace("$", "").replace(",", "").strip()

    if not text:
        return 0

    lowered = text.lower()
    if "no sale recorded" in lowered or "not sold" in lowered:
        return 0

    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        try:
            return int(float(match.group(1)))
        except Exception:
            return 0

    return 0


def normalize_make_model(make, model):
    make = make.strip()
    model = model.strip()

    if make == "BMW":
        model = BMW_MODEL_MAP.get(model.upper(), model)

    return make, model


def normalize_state(state):
    s = str(state).strip().upper()
    state_map = {
        "CALIFORNIA": "CA",
        "CA": "CA",
    }
    return state_map.get(s, s)


def is_allowed_state(state):
    normalized = normalize_state(state)
    allowed = {normalize_state(x) for x in STATE_FILTER}
    return normalized in allowed


def split_title(title_line):
    parts = title_line.split()
    if not parts or not re.match(r"^\d{4}$", parts[0]):
        return "", "", ""

    year = parts[0]
    rest = " ".join(parts[1:]).strip()

    matched_make = None
    for make in sorted(MULTI_WORD_MAKES, key=len, reverse=True):
        if rest.startswith(make + " ") or rest == make:
            matched_make = make
            break

    if matched_make:
        make = matched_make
        model = rest[len(matched_make):].strip()
    else:
        make = parts[1] if len(parts) > 1 else ""
        model = " ".join(parts[2:]) if len(parts) > 2 else ""

    make, model = normalize_make_model(make, model)
    return year, make, model


def extract_trim_and_type(make, model, title):
    full_name = title.strip()
    base = f"{make} {model}".strip()

    trim = ""
    if full_name.startswith(base):
        trim = full_name[len(base):].strip()

    title_upper = title.upper()

    if any(x in title_upper for x in ["PICKUP", "F150", "F-150", "SILVERADO", "RAM ", "TUNDRA"]):
        car_type = "Truck"
    elif any(
        x in title_upper
        for x in [
            "SUV",
            "SPORTAGE",
            "EXPLORER",
            "ESCAPE",
            "ROGUE",
            "EQUINOX",
            "TAHOE",
            "SUBURBAN",
            "RAV4",
            "CR-V",
            "CX-5",
            "XC90",
            "DISCOVERY",
            "SORRENTO",
            "SORENTO",
        ]
    ):
        car_type = "SUV"
    elif any(x in title_upper for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "86", "BRZ"]):
        car_type = "Coupe"
    elif any(x in title_upper for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA"]):
        car_type = "Van"
    else:
        car_type = "Sedan"

    return trim, car_type


def parse_location_state(line):
    line = str(line).strip()
    if " - " in line:
        parts = line.rsplit(" - ", 1)
        return parts[0].strip(), parts[1].strip()

    match = re.match(r"^(.*?)[,\s]+([A-Z]{2})$", line)
    if match:
        return match.group(1).strip(), match.group(2).strip()

    return line, ""


def get_state_from_text(text):
    text = str(text)

    patterns = [
        r"\bCalifornia\b",
        r"\bCA\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return normalize_state(match.group(0))

    match = re.search(r"Location\s*[:\n]\s*([A-Za-z0-9 .,&/\-]+)\s*-\s*([A-Z]{2})", text, re.IGNORECASE)
    if match:
        return normalize_state(match.group(2))

    return ""


def scroll_page(page, steps=10, delay=800):
    last_height = 0
    for _ in range(steps):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(delay)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def get_listing_urls(page):
    urls = set()
    try:
        links = page.locator("a[href*='/car/']")
        count = links.count()

        for i in range(count):
            try:
                href = links.nth(i).get_attribute("href")
                if href and "/car/" in href:
                    if not href.startswith("http"):
                        href = "https://bidstreamline.com" + href
                    urls.add(href)
            except Exception:
                pass
    except Exception:
        pass

    return list(urls)


def extract_detail_fields(detail_page):
    result = {
        "damage": "",
        "odometer": "",
        "date": "",
        "location": "",
        "state": "",
        "price": 0,
        "vin": "",
        "lot": "",
        "title": "",
    }

    try:
        text = detail_page.locator("body").inner_text(timeout=15000)

        vin_match = re.search(r"VIN\s*[:#]?\s*([A-HJ-NPR-Z0-9]{11,17})", text, re.IGNORECASE)
        if vin_match:
            result["vin"] = vin_match.group(1).strip()

        lot_match = re.search(r"Lot\s*#?\s*[:#]?\s*([A-Za-z0-9-]+)", text, re.IGNORECASE)
        if lot_match:
            result["lot"] = lot_match.group(1).strip()

        title_match = re.search(r"\b(19\d{2}|20\d{2})\s+[A-Za-z0-9 .&/\-]+\b", text)
        if title_match:
            result["title"] = title_match.group(0).strip()

        damage_patterns = [
            r"Primary Damage\s*[:\n]\s*([A-Za-z0-9 /&-]+)",
            r"Damage\s*[:\n]\s*([A-Za-z0-9 /&-]+)",
        ]
        for pattern in damage_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result["damage"] = match.group(1).strip()
                break

        odo_match = re.search(r"Odometer\s*[:\n]\s*([0-9,]+(?:\s*mi)?)", text, re.IGNORECASE)
        if odo_match:
            result["odometer"] = odo_match.group(1).strip()

        if "Not yet sold" in text:
            result["date"] = "Not yet sold"
        else:
            date_patterns = [
                r"Sale Date\s*[:\n]\s*([A-Za-z0-9, /:-]+)",
                r"Auction Date\s*[:\n]\s*([A-Za-z0-9, /:-]+)",
                r"Date Sold\s*[:\n]\s*([A-Za-z0-9, /:-]+)",
            ]
            for pattern in date_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    result["date"] = match.group(1).strip()
                    break

        loc_match = re.search(r"Location\s*[:\n]\s*([A-Za-z0-9 .,&/\-]+)", text, re.IGNORECASE)
        if loc_match:
            loc_raw = loc_match.group(1).strip()
            loc, st = parse_location_state(loc_raw)
            result["location"] = loc
            result["state"] = normalize_state(st)

        if not result["state"]:
            result["state"] = get_state_from_text(text)

        price_patterns = [
            r"Sold For\s*[:\n]\s*\$?\s*([0-9,]+(?:\.\d{2})?)",
            r"Sale Price\s*[:\n]\s*\$?\s*([0-9,]+(?:\.\d{2})?)",
            r"Final Bid\s*[:\n]\s*\$?\s*([0-9,]+(?:\.\d{2})?)",
            r"Winning Bid\s*[:\n]\s*\$?\s*([0-9,]+(?:\.\d{2})?)",
            r"Sold Amount\s*[:\n]\s*\$?\s*([0-9,]+(?:\.\d{2})?)",
        ]

        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result["price"] = clean_price(match.group(1))
                if result["price"] > 0:
                    break

    except Exception:
        pass

    return result


def scrape_detail_fields(browser, url):
    empty = {
        "damage": "",
        "odometer": "",
        "date": "",
        "location": "",
        "state": "",
        "price": 0,
        "vin": "",
        "lot": "",
        "title": "",
    }

    if not url:
        return empty

    detail_page = browser.new_page()
    try:
        detail_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        detail_page.wait_for_timeout(2500)
        return extract_detail_fields(detail_page)
    except Exception:
        return empty
    finally:
        detail_page.close()


def merge_detail_into_data(data, details):
    if details.get("price", 0) > 0:
        data["price"] = details["price"]

    for field in ["damage", "odometer", "date", "location", "state"]:
        if details.get(field):
            data[field] = details[field]

    if details.get("vin") and not data["vin"]:
        data["vin"] = details["vin"]

    if details.get("lot") and not data["lot"]:
        data["lot"] = details["lot"]

    if details.get("title") and not data["title"]:
        data["title"] = details["title"]

    if data["title"] and (not data["year"] or not data["make"] or not data["model"]):
        year, make, model = split_title(data["title"])
        data["year"] = year
        data["make"] = make
        data["model"] = model
        trim, car_type = extract_trim_and_type(make, model, " ".join(data["title"].split()[1:]))
        data["trim"] = trim
        data["type"] = car_type

    if data["state"]:
        data["state"] = normalize_state(data["state"])

    if data["date"] and "Not yet sold" not in str(data["date"]):
        data["sold"] = True

    return data


def go_to_next_page(page, current_page_num):
    next_page_num = str(current_page_num + 1)

    selectors = [
        f"button:has-text('{next_page_num}')",
        f"a:has-text('{next_page_num}')",
        "button:has-text('→')",
        "a:has-text('→')",
        "button:has-text('Next')",
        "a:has-text('Next')",
        "[aria-label='Next']",
        "[rel='next']",
    ]

    for selector in selectors:
        btn = page.locator(selector)
        if btn.count() > 0:
            try:
                btn.first.click()
                page.wait_for_load_state("networkidle", timeout=15000)
                page.wait_for_timeout(2000)
                return True
            except Exception as e:
                print(f"Could not click selector {selector}: {e}")

    return False


def load_existing_keys(csv_path=MASTER_CSV):
    existing_keys = set()

    if not os.path.exists(csv_path):
        return existing_keys

    try:
        existing_df = pd.read_csv(csv_path, dtype=str).fillna("")
        for _, row in existing_df.iterrows():
            vin = row.get("vin", "").strip()
            lot = row.get("lot", "").strip()
            if vin and lot:
                existing_keys.add(f"{vin}_{lot}")
    except Exception as e:
        print(f"Could not load existing keys from {csv_path}: {e}")

    return existing_keys


def build_data_from_url(browser, url):
    data = {
        "vin": "",
        "year": "",
        "make": "",
        "model": "",
        "trim": "",
        "type": "",
        "damage": "",
        "price": 0,
        "odometer": "",
        "lot": "",
        "date": "",
        "location": "",
        "state": "",
        "sold": False,
        "url": url,
        "title": "",
    }

    details = scrape_detail_fields(browser, url)
    data = merge_detail_into_data(data, details)
    return data


def reached_page_limit(page_num):
    return MAX_PAGES is not None and page_num > MAX_PAGES


def main():
    all_data = []
    seen = set()
    existing_keys = load_existing_keys(MASTER_CSV)
    empty_pages_in_a_row = 0

    skip_counts = {
        "no_vin": 0,
        "duplicate": 0,
        "existing": 0,
        "wrong_state": 0,
        "unsold": 0,
        "no_date": 0,
        "no_price": 0,
        "low_price": 0,
        "kept": 0,
        "detail_fail": 0,
    }

    print(f"Loaded {len(existing_keys)} existing keys from {MASTER_CSV}")
    print(f"Filtering to states: {STATE_FILTER}")
    print(f"Minimum valid price: {MIN_PRICE}")
    print(f"Target new rows this run: {TARGET_ROWS}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Opening site...")
        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        page_num = 1

        while True:
            if reached_page_limit(page_num):
                print(f"Reached MAX_PAGES={MAX_PAGES}, stopping.")
                break

            if len(all_data) >= TARGET_ROWS:
                print(f"Reached TARGET_ROWS={TARGET_ROWS}, stopping.")
                break

            print(f"\nScraping page {page_num}")
            rows_before_page = len(all_data)

            try:
                scroll_page(page)
                urls = get_listing_urls(page)
                print(f"Found {len(urls)} detail urls")
            except Exception as e:
                print(f"Failed to collect urls on page {page_num}: {e}")
                urls = []

            page_seen_urls = set()

            for url in urls:
                if len(all_data) >= TARGET_ROWS:
                    print(f"Reached TARGET_ROWS={TARGET_ROWS} mid-page, stopping.")
                    break

                if not url or url in page_seen_urls:
                    continue
                page_seen_urls.add(url)

                try:
                    data = build_data_from_url(browser, url)

                    if not data["vin"]:
                        skip_counts["no_vin"] += 1
                        continue

                    if not is_allowed_state(data["state"]):
                        skip_counts["wrong_state"] += 1
                        continue

                    key = f"{data['vin']}_{data['lot']}"
                    if key in seen:
                        skip_counts["duplicate"] += 1
                        continue
                    if key in existing_keys:
                        skip_counts["existing"] += 1
                        continue

                    if SKIP_UNSOLD and "Not yet sold" in str(data["date"]):
                        skip_counts["unsold"] += 1
                        continue

                    if REQUIRE_SOLD_DATE and not data["date"]:
                        skip_counts["no_date"] += 1
                        continue

                    if REQUIRE_PRICE:
                        if data["price"] <= 0:
                            skip_counts["no_price"] += 1
                            continue
                        if data["price"] < MIN_PRICE:
                            skip_counts["low_price"] += 1
                            continue

                    seen.add(key)
                    all_data.append(data)
                    skip_counts["kept"] += 1

                    print(
                        f"KEEP {len(all_data)}/{TARGET_ROWS}: "
                        f"{data['year']} {data['make']} {data['model']} | "
                        f"STATE={data['state']} | VIN={data['vin']} | LOT={data['lot']} | PRICE={data['price']}"
                    )

                except Exception as e:
                    skip_counts["detail_fail"] += 1
                    print(f"Error parsing detail url {url}: {e}")

            rows_added_this_page = len(all_data) - rows_before_page
            print(f"Rows added on page {page_num}: {rows_added_this_page}")

            if rows_added_this_page == 0:
                empty_pages_in_a_row += 1
                print(f"Empty pages in a row: {empty_pages_in_a_row}/{MAX_EMPTY_PAGES}")
            else:
                empty_pages_in_a_row = 0

            if len(all_data) >= TARGET_ROWS:
                print(f"Reached TARGET_ROWS={TARGET_ROWS}, stopping.")
                break

            if empty_pages_in_a_row >= MAX_EMPTY_PAGES:
                print("Too many pages in a row with no new matching rows, stopping.")
                break

            moved = go_to_next_page(page, page_num)
            if not moved:
                print("No next button found, stopping.")
                break

            page_num += 1

        browser.close()

    internal_columns = [
        "vin",
        "year",
        "make",
        "model",
        "trim",
        "type",
        "damage",
        "price",
        "odometer",
        "lot",
        "date",
        "location",
        "state",
    ]

    website_columns = [
        "year",
        "make",
        "model",
        "trim",
        "type",
        "damage",
        "price",
        "odometer",
        "lot",
        "date",
        "location",
        "state",
    ]

    if os.path.exists(MASTER_CSV):
        master_df = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
    else:
        master_df = pd.DataFrame(columns=internal_columns)

    if all_data:
        new_df = pd.DataFrame(all_data)

        for col in internal_columns:
            if col not in new_df.columns:
                new_df[col] = ""

        new_df = new_df[internal_columns].copy()
        new_df["price"] = pd.to_numeric(new_df["price"], errors="coerce").fillna(0)
        new_df = new_df[new_df["price"] >= MIN_PRICE].copy()

        master_df = pd.concat([master_df, new_df], ignore_index=True)

    if not master_df.empty:
        master_df["price"] = pd.to_numeric(master_df["price"], errors="coerce").fillna(0)
        master_df = master_df[master_df["price"] >= MIN_PRICE].copy()
        master_df = master_df.drop_duplicates(subset=["vin", "lot"], keep="first")

    master_df.to_csv(MASTER_CSV, index=False)

    website_df = master_df.copy()
    for col in website_columns:
        if col not in website_df.columns:
            website_df[col] = ""

    website_df = website_df[website_columns].copy()
    website_df["price"] = pd.to_numeric(website_df["price"], errors="coerce").fillna(0)
    website_df = website_df[website_df["price"] >= MIN_PRICE].copy()
    website_df.to_csv(WEBSITE_CSV, index=False)

    print("\nDONE")
    print(f"New rows added this run: {len(all_data)}")
    print(f"Total rows in {MASTER_CSV}: {len(master_df)}")
    print(f"Saved {MASTER_CSV} and {WEBSITE_CSV}")
    print("Skip stats:", skip_counts)


if __name__ == "__main__":
    main()
