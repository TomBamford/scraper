from playwright.sync_api import sync_playwright
import pandas as pd
import re
import os

URL = "https://bidstreamline.com/catalog"
TARGET_ROWS = 500


MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

MIN_PRICE = 500
MAX_PRICE = 9999
EXCLUDED_MAKES = {"LAND ROVER"}


def clean_price(text):
    text = str(text).replace("$", "").replace(",", "").strip()
    if not text:
        return 0
    if "no sale recorded" in text.lower() or "not sold" in text.lower():
        return 0

    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        return 0

    try:
        return int(float(match.group(1)))
    except Exception:
        return 0


def split_title(title_line):
    title_line = str(title_line).strip()
    match = re.match(r"^(\d{4})\s+(.+)$", title_line)
    if not match:
        return "", "", ""

    year = match.group(1)
    rest = match.group(2).strip()

    multi_word_makes = [
        "LAND ROVER",
        "ALFA ROMEO",
        "ASTON MARTIN",
        "MERCEDES BENZ",
        "ROLLS ROYCE",
        "GENERAL MOTORS",
    ]

    for make in sorted(multi_word_makes, key=len, reverse=True):
        if rest.startswith(make + " ") or rest == make:
            model = rest[len(make):].strip()
            return year, make, model

    parts = rest.split()
    make = parts[0] if parts else ""
    model = " ".join(parts[1:]) if len(parts) > 1 else ""
    return year, make, model


def extract_trim(make, model, title):
    full_name = str(title).strip()
    base = f"{make} {model}".strip()

    if full_name.startswith(base):
        return full_name[len(base):].strip()
    return ""


def parse_location_state(line):
    line = str(line).strip()
    if " - " in line:
        parts = line.rsplit(" - ", 1)
        return parts[0].strip(), parts[1].strip()
    return line, ""


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

    return list(urls)


def extract_detail_fields(detail_page):
    result = {
        "vin": "",
        "year": "",
        "make": "",
        "model": "",
        "trim": "",
        "damage": "",
        "price": 0,
        "odometer": "",
        "lot": "",
        "date": "",
        "location": "",
        "state": "",
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
            title = title_match.group(0).strip()
            year, make, model = split_title(title)
            result["year"] = year
            result["make"] = make
            result["model"] = model
            result["trim"] = extract_trim(make, model, " ".join(title.split()[1:]))

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
            result["state"] = st

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


def scrape_detail(browser, url):
    page = browser.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2500)
        return extract_detail_fields(page)
    except Exception:
        return {
            "vin": "",
            "year": "",
            "make": "",
            "model": "",
            "trim": "",
            "damage": "",
            "price": 0,
            "odometer": "",
            "lot": "",
            "date": "",
            "location": "",
            "state": "",
        }
    finally:
        page.close()


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
            except Exception:
                pass

    return False


def load_existing_keys(csv_path):
    existing_keys = set()
    if not os.path.exists(csv_path):
        return existing_keys

    try:
        df = pd.read_csv(csv_path, dtype=str).fillna("")
        for _, row in df.iterrows():
            vin = row.get("vin", "").strip()
            lot = row.get("lot", "").strip()
            if vin and lot:
                existing_keys.add(f"{vin}_{lot}")
    except Exception:
        pass

    return existing_keys


def is_valid_row(data):
    make = str(data["make"]).strip().upper()
    price = int(data["price"]) if str(data["price"]).isdigit() else int(float(data["price"] or 0))

    if not data["vin"]:
        return False
    if make in EXCLUDED_MAKES:
        return False
    if "Not yet sold" in str(data["date"]):
        return False
    if price < MIN_PRICE:
        return False
    if price >= 10000:
        return False

    return True


def main():
    all_data = []
    seen = set()
    existing_keys = load_existing_keys(MASTER_CSV)
    empty_pages = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Opening site...")
        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        page_num = 1

        while len(all_data) < TARGET_ROWS:
            print(f"\nScraping page {page_num}")
            before_count = len(all_data)

            try:
                scroll_page(page)
                urls = get_listing_urls(page)
                print(f"Found {len(urls)} detail urls")
            except Exception as e:
                print(f"Failed on page {page_num}: {e}")
                urls = []

            page_seen = set()

            for url in urls:
                if len(all_data) >= TARGET_ROWS:
                    break
                if not url or url in page_seen:
                    continue

                page_seen.add(url)
                data = scrape_detail(browser, url)
                data["url"] = url

                key = f"{data['vin']}_{data['lot']}"
                if key in seen or key in existing_keys:
                    continue

                if not is_valid_row(data):
                    continue

                seen.add(key)
                all_data.append(data)

                print(
                    f"KEEP {len(all_data)}/{TARGET_ROWS}: "
                    f"{data['year']} {data['make']} {data['model']} | "
                    f"VIN={data['vin']} | LOT={data['lot']} | PRICE={data['price']}"
                )

            added_this_page = len(all_data) - before_count
            print(f"Rows added on page {page_num}: {added_this_page}")

            if added_this_page == 0:
                empty_pages += 1
                print(f"Empty pages in a row: {empty_pages}/{MAX_EMPTY_PAGES}")
            else:
                empty_pages = 0

            if len(all_data) >= TARGET_ROWS:
                print("Reached target.")
                break

    
            moved = go_to_next_page(page, page_num)
            if not moved:
                print("No next page found, stopping.")
                break

            page_num += 1

        browser.close()

    columns = [
        "vin",
        "year",
        "make",
        "model",
        "trim",
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
        master_df = pd.DataFrame(columns=columns)

    if all_data:
        new_df = pd.DataFrame(all_data)
        for col in columns:
            if col not in new_df.columns:
                new_df[col] = ""

        new_df = new_df[columns].copy()
        new_df["price"] = pd.to_numeric(new_df["price"], errors="coerce").fillna(0)
        new_df["make"] = new_df["make"].astype(str).str.strip()

        new_df = new_df[
            (new_df["price"] >= MIN_PRICE)
            & (new_df["price"] < 10000)
            & (~new_df["make"].str.upper().isin(EXCLUDED_MAKES))
        ].copy()

        master_df = pd.concat([master_df, new_df], ignore_index=True)

    if not master_df.empty:
        master_df["price"] = pd.to_numeric(master_df["price"], errors="coerce").fillna(0)
        master_df["make"] = master_df["make"].astype(str).str.strip()

        master_df = master_df[
            (master_df["price"] >= MIN_PRICE)
            & (master_df["price"] < 10000)
            & (~master_df["make"].str.upper().isin(EXCLUDED_MAKES))
        ].copy()

        master_df = master_df.drop_duplicates(subset=["vin", "lot"], keep="first")

    master_df.to_csv(MASTER_CSV, index=False)

    website_df = master_df[[
        "year",
        "make",
        "model",
        "trim",
        "damage",
        "price",
        "odometer",
        "lot",
        "date",
        "location",
        "state",
    ]].copy()
    website_df.to_csv(WEBSITE_CSV, index=False)

    print("\nDONE")
    print(f"New rows added this run: {len(all_data)}")
    print(f"Total rows in {MASTER_CSV}: {len(master_df)}")
    print(f"Saved {MASTER_CSV} and {WEBSITE_CSV}")


if __name__ == "__main__":
    main()
