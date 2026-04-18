from playwright.sync_api import sync_playwright
import pandas as pd
import re
import time

URL = "https://bidstreamline.com/catalog"
MAX_PAGES = 5  # change this to scrape more pages


def clean_price(text):
    text = text.replace("$", "").replace(",", "").strip()
    if "No sale recorded" in text or text == "":
        return 0
    try:
        return int(text)
    except:
        return 0


def parse_card(text):
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    data = {
        "title": "",
        "year": "",
        "make": "",
        "model": "",
        "vin": "",
        "lot": "",
        "price": 0,
        "auction": ""
    }

    for line in lines:
        if "VIN:" in line:
            data["vin"] = line.replace("VIN:", "").strip()

        elif "Lot #" in line:
            data["lot"] = line.replace("Lot #", "").strip()

        elif "$" in line or "No sale recorded" in line:
            data["price"] = clean_price(line)

        elif re.match(r"\d{4} ", line):
            data["title"] = line
            parts = line.split()
            data["year"] = parts[0]
            if len(parts) > 1:
                data["make"] = parts[1]
            if len(parts) > 2:
                data["model"] = " ".join(parts[2:])

        elif line in ["IAAI", "COPART"]:
            data["auction"] = line

    return data


def main():
    all_data = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Opening site...")
        page.goto(URL)
        page.wait_for_timeout(5000)

        for page_num in range(MAX_PAGES):
            print(f"Scraping page {page_num + 1}")

            cards = page.locator("div").filter(has_text="VIN:")
            count = cards.count()
            print(f"Found {count} cards")

            for i in range(count):
                try:
                    text = cards.nth(i).inner_text()
                    data = parse_card(text)

                    # remove empty results
                    if not data["vin"]:
                        continue

                    # deduplicate
                    key = data["vin"] + str(data["lot"])
                    if key in seen:
                        continue
                    seen.add(key)

                    all_data.append(data)

                except Exception as e:
                    print("Error parsing card:", e)

            # go to next page
            next_btn = page.locator("button:has-text('Next'), a:has-text('Next')")

            if next_btn.count() == 0:
                print("No next button found, stopping.")
                break

            try:
                next_btn.first.click()
                page.wait_for_timeout(4000)
            except:
                print("Could not click next, stopping.")
                break

        browser.close()

    if not all_data:
        print("No data collected.")
        return

    df = pd.DataFrame(all_data)

    # save raw data
    df.to_csv("raw.csv", index=False)

    # clean data
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    clean_df = df[df["price"] > 0]

    clean_df.to_csv("cars.csv", index=False)

    print(f"\nDONE")
    print(f"Raw rows: {len(df)}")
    print(f"Clean rows: {len(clean_df)}")
    print("Saved to cars.csv")


if __name__ == "__main__":
    main()