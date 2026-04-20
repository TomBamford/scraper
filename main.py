import asyncio
import json
import re
import time
from playwright.async_api import async_playwright

BASE_URL = "https://poctra.com"
START_URL = f"{BASE_URL}/TOYOTA-CAMRY-for-sale-1"

YEAR_MIN = 2008
YEAR_MAX = 2015
MAX_PAGES = 10


def year_ok(y):
    try:
        return YEAR_MIN <= int(y) <= YEAR_MAX
    except:
        return False


def extract_from_json(data):
    """
    Try to normalize Poctra API response into rows.
    You MUST inspect actual response shape — this handles common patterns.
    """
    rows = []

    # handle list directly
    if isinstance(data, list):
        for item in data:
            rows.append(item)

    # handle dict with results
    elif isinstance(data, dict):
        for key in ["data", "results", "items", "vehicles", "lots"]:
            if key in data and isinstance(data[key], list):
                rows.extend(data[key])

    return rows


async def capture_api(page, collected):
    async def handle_response(response):
        try:
            url = response.url

            # filter only XHR/fetch
            if response.request.resource_type not in ["xhr", "fetch"]:
                return

            # only keep Poctra-related endpoints
            if "poctra" not in url.lower():
                return

            text = await response.text()

            # must be JSON
            if not text.startswith("{") and not text.startswith("["):
                return

            data = json.loads(text)

            rows = extract_from_json(data)

            if rows:
                print(f"[API] {url} → {len(rows)} rows")
                collected.extend(rows)

        except Exception:
            pass

    page.on("response", handle_response)


async def scrape():
    t0 = time.time()
    collected = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await capture_api(page, collected)

        for i in range(1, MAX_PAGES + 1):
            url = re.sub(r"-for-sale-\d+$", f"-for-sale-{i}", START_URL)
            print(f"[+] Visiting {url}")

            await page.goto(url, wait_until="networkidle")
            await page.wait_for_timeout(3000)  # allow XHR to fire

        await browser.close()

    print(f"\nCaptured raw rows: {len(collected)}")

    # basic filtering
    cleaned = []
    for r in collected:
        year = r.get("year") or r.get("Year") or ""

        if not year_ok(year):
            continue

        cleaned.append({
            "year": year,
            "make": r.get("make") or r.get("Make"),
            "model": r.get("model") or r.get("Model"),
            "price": r.get("price") or r.get("Price"),
            "lot": r.get("lot") or r.get("lotNumber"),
        })

    print(f"Filtered rows: {len(cleaned)}")

    # preview
    for c in cleaned[:10]:
        print(c)

    print(f"\nDone in {round(time.time() - t0, 2)}s")


if __name__ == "__main__":
    asyncio.run(scrape())
if __name__ == "__main__":
    asyncio.run(main())
