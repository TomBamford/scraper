from playwright.sync_api import sync_playwright
import pandas as pd
import re

URL = "https://bidstreamline.com/catalog"
MAX_PAGES = 2
DETAIL_LOOKUPS = True  # False = faster, but less detail

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
    text = text.replace("$", "").replace(",", "").strip()
    if "No sale recorded" in text or text == "":
        return 0
    try:
        return int(float(text))
    except Exception:
        return 0


def normalize_make_model(make, model):
    make = make.strip()
    model = model.strip()

    if make == "BMW":
        model = BMW_MODEL_MAP.get(model.upper(), model)

    return make, model


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
    elif any(x in title_upper for x in ["SUV", "SPORTAGE", "EXPLORER", "ESCAPE", "ROGUE", "EQUINOX", "TAHOE", "SUBURBAN", "RAV4", "CR-V", "CX-5", "XC90", "DISCOVERY", "SORRENTO", "SORENTO"]):
        car_type = "SUV"
    elif any(x in title_upper for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "86", "BRZ"]):
        car_type = "Coupe"
    elif any(x in title_upper for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA"]):
        car_type = "Van"
    else:
        car_type = "Sedan"

    return trim, car_type


def parse_location_state(line):
    line = line.strip()
    if " - " in line:
        parts = line.rsplit(" - ", 1)
        return parts[0].strip(), parts[1].strip()
    return line, ""


def parse_card(text):
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    data = {
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
        "url": "",
        "title": "",
        "vin": "",
    }

    saw_location_label = False
    saw_odometer_label = False
    saw_sale_date_label = False

    for line in lines:
        if "VIN:" in line:
            data["vin"] = line.replace("VIN:", "").strip()

        elif line.startswith("Lot #"):
            data["lot"] = line.replace("Lot #", "").strip()

        elif "$" in line or "No sale recorded" in line:
            data["price"] = clean_price(line)

        elif line in ["IAAI", "COPART"]:
            pass

        elif re.match(r"^\d{4}\s+", line):
            data["title"] = line
            year, make, model = split_title(line)
            data["year"] = year
            data["make"] = make
            data["model"] = model
            trim, car_type = extract_trim_and_type(make, model, " ".join(line.split()[1:]))
            data["trim"] = trim
            data["type"] = car_type

        elif line.upper() == "LOCATION":
            saw_location_label = True

        elif saw_location_label:
            loc, st = parse_location_state(line)
            data["location"] = loc
            data["state"] = st
            saw_location_label = False

        elif line.upper() == "ODOMETER":
            saw_odometer_label = True

        elif saw_odometer_label:
            data["odometer"] = line
            saw_odometer_label = False

        elif line.upper() == "SALE DATE":
            saw_sale_date_label = True

        elif saw_sale_date_label:
            data["date"] = line
            if "Not yet sold" not in line:
                data["sold"] = True
            saw_sale_date_label = False

        elif "Not yet sold" in line:
            data["date"] = "Not yet sold"
            data["sold"] = False

        elif (
            re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", line)
            or re.search(r"\b[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\b", line)
        ):
            data["date"] = line
            data["sold"] = True

    if data["date"] == "":
        data["sold"] = False

    return data


def get_card_url(card):
    try:
        links = card.locator("a")
        count = links.count()
        for i in range(count):
            href = links.nth(i).get_attribute("href")
            if href and "/car/" in href:
                if href.startswith("http"):
                    return href
                return "https://bidstreamline.com" + href
    except Exception:
        pass
    return ""


def extract_detail_fields(detail_page):
    result = {
        "damage": "",
        "odometer": "",
        "date": "",
        "location": "",
        "state": "",
    }

    try:
        text = detail_page.locator("body").inner_text(timeout=10000)

        damage_patterns = [
            r"Primary Damage\s*[:\n]\s*([A-Za-z0-9 /&-]+)",
            r"Damage\s*[:\n]\s*([A-Za-z0-9 /&-]+)",
        ]
        for pattern in damage_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result["damage"] = match.group(1).strip()
                break

        odo_match = re.search(r"Odometer\s*[:\n]\s*([0-9,]+\s*mi)", text, re.IGNORECASE)
        if odo_match:
            result["odometer"] = odo_match.group(1).strip()

        if "Not yet sold" in text:
            result["date"] = "Not yet sold"
        else:
            date_match = re.search(r"Sale Date\s*[:\n]\s*([A-Za-z0-9, /-]+)", text, re.IGNORECASE)
            if date_match:
                result["date"] = date_match.group(1).strip()

        loc_match = re.search(r"Location\s*[:\n]\s*([A-Za-z0-9 .,&/-]+)", text, re.IGNORECASE)
        if loc_match:
            loc_raw = loc_match.group(1).strip()
            loc, st = parse_location_state(loc_raw)
            result["location"] = loc
            result["state"] = st

    except Exception:
        pass

    return result


def scrape_detail_fields(browser, url):
    if not url:
        return {
            "damage": "",
            "odometer": "",
            "date": "",
            "location": "",
            "state": "",
        }

    detail_page = browser.new_page()
    try:
        detail_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        detail_page.wait_for_timeout(2500)
        return extract_detail_fields(detail_page)
    except Exception:
        return {
            "damage": "",
            "odometer": "",
            "date": "",
            "location": "",
            "state": "",
        }
    finally:
        detail_page.close()


def go_to_next_page(page, current_page_num):
    next_page_num = str(current_page_num + 1)

    selectors = [
        f"button:has-text('{next_page_num}')",
        f"a:has-text('{next_page_num}')",
        "button:has-text('→')",
        "a:has-text('→')",
        "button:has-text('Next')",
        "a:has-text('Next')",
    ]

    for selector in selectors:
        btn = page.locator(selector)
        if btn.count() > 0:
            try:
                btn.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(2000)
                return True
            except Exception as e:
                print(f"Could not click selector {selector}: {e}")

    return False


def main():
    all_data = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Opening site...")
        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        for page_num in range(MAX_PAGES):
            print(f"Scraping page {page_num + 1}")

            cards = page.locator("div").filter(has_text="VIN:")
            count = cards.count()
            print(f"Found {count} cards")

            for i in range(count):
                try:
                    card = cards.nth(i)
                    text = card.inner_text()
                    data = parse_card(text)

                    if not data["vin"]:
                        continue

                    data["url"] = get_card_url(card)

                    key = f"{data['vin']}_{data['lot']}"
                    if key in seen:
                        continue

                    if DETAIL_LOOKUPS and data["url"]:
                        details = scrape_detail_fields(browser, data["url"])
                        if details["damage"]:
                            data["damage"] = details["damage"]
                        if details["odometer"]:
                            data["odometer"] = details["odometer"]
                        if details["date"]:
                            data["date"] = details["date"]
                        if details["location"]:
                            data["location"] = details["location"]
                        if details["state"]:
                            data["state"] = details["state"]

                    if not data["date"] or "Not yet sold" in data["date"]:
                        continue
                    if data["price"] <= 0:
                        continue

                    seen.add(key)
                    all_data.append(data)

                except Exception as e:
                    print("Error parsing card:", e)

            if page_num < MAX_PAGES - 1:
                moved = go_to_next_page(page, page_num + 1)
                if not moved:
                    print("No next button found, stopping.")
                    break

        browser.close()

    if not all_data:
        print("No data collected.")
        return

    df = pd.DataFrame(all_data)

    output_columns = [
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

    for col in output_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[output_columns].copy()
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    clean_df = df[df["price"] > 0].copy()

    clean_df.to_csv("cars.csv", index=False)

    print("\nDONE")
    print(f"Clean rows: {len(clean_df)}")
    print("Saved to cars.csv")


if __name__ == "__main__":
    main()