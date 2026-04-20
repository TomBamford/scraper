import asyncio
import os
import re
import time
from typing import Optional, Tuple

import pandas as pd
from playwright.async_api import async_playwright


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://autoconduct.com/auction-prices/"
MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

MIN_PRICE = 500

# Parallel workers. 4-8 is usually a good range.
WORKERS = 6

# Fallback if pagination count cannot be detected.
# 49,000 cars / ~24 per page ≈ ~2042 pages, so 2500 is a safe ceiling.
MAX_PAGE_GUESS = 4000

# If True, skip VIN|LOT pairs already present in master.csv
RESUME_FROM_MASTER = True

# Stop after this many consecutive completely empty pages
# when pagination count cannot be determined reliably.
MAX_CONSECUTIVE_EMPTY_PAGES = 200

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

TITLE_BAD_WORDS = {"VIN", "COPART", "IAAI", "AUTOCONDUCT"}


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
    m = re.search(r"(\d+)", text)
    return m.group(1) + " mi" if m else ""


def normalize_make_model(make: str, model: str) -> Tuple[str, str]:
    make = str(make).strip().upper()
    model = str(model).strip()
    if make == "BMW":
        model = BMW_MODEL_MAP.get(model.upper(), model)
    return make.title(), model


def split_title_with_optional_year(title_line: str) -> Tuple[str, str, str]:
    """
    Handles:
      '2021 KIA K5 LXS'
      'KIA K5 LXS'
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
    if any(x in t for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA", "CARAVAN"]):
        return "Van"
    return "Sedan"


def parse_location_state(raw: str) -> Tuple[str, str]:
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


def is_excluded(make: str) -> bool:
    return str(make).strip().upper() in EXCLUDED_MAKES


def build_paged_url(page_num: int) -> str:
    if page_num <= 1:
        return BASE_URL
    return f"{BASE_URL}?page={page_num}"


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
        return keys
    except Exception as e:
        print(f"[WARN] Could not load existing keys: {e}")
        return set()


def extract_card_title_from_lines(lines) -> str:
    for line in lines:
        s = normalize_whitespace(line)
        upper = s.upper()

        if not s:
            continue
        if any(bad in upper for bad in TITLE_BAD_WORDS):
            continue
        if "$" in s:
            continue
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", s):
            continue
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
            continue
        if re.fullmatch(r"\d{4}", s):
            continue
        if len(s.split()) < 2:
            continue
        if re.search(r"\b(?:mil|mi|miles?)\b", s, re.IGNORECASE):
            continue
        if any(dmg in upper for dmg in DAMAGE_PATTERNS):
            continue
        return s
    return ""


def parse_card_payload(payload: dict) -> Optional[dict]:
    text = str(payload.get("text", "")).strip()
    title_hint = normalize_whitespace(payload.get("title", ""))

    if not text:
        return None

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

    lines = [normalize_whitespace(line) for line in text.splitlines() if normalize_whitespace(line)]

    raw["title"] = title_hint or extract_card_title_from_lines(lines)

    if raw["title"]:
        yr, mk, mo = split_title_with_optional_year(raw["title"])
        if yr:
            raw["year"] = yr
        if mk:
            raw["make"] = mk
        if mo:
            raw["model"] = mo

    m = re.search(r"\bVIN:\s*([A-HJ-NPR-Z0-9]{11,17})\b", text, re.IGNORECASE)
    if m:
        raw["vin"] = m.group(1)
    else:
        m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", text)
        if m:
            raw["vin"] = m.group(1)

    m = re.search(r"\b(?:LOT|LOT#|LOT #)\s*[:#]?\s*([A-Z0-9-]+)\b", text, re.IGNORECASE)
    if m:
        raw["lot"] = m.group(1)

    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        raw["year"] = m.group(1)

    m = re.search(r"([\d,]+)\s*(?:mil|mi|miles?)\b", text, re.IGNORECASE)
    if m:
        raw["odometer"] = m.group(1).replace(",", "") + " mi"

    upper_text = text.upper()
    for dmg in DAMAGE_PATTERNS:
        if dmg in upper_text:
            raw["damage"] = dmg.title()
            break

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

    m = re.search(r"(\d{2}\.\d{2}\.\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|[A-Za-z]+ \d{1,2},?\s*\d{4})", text)
    if m:
        raw["date"] = m.group(1).strip()

    m = re.search(r"([A-Za-z .'-]+),\s*([A-Z]{2})\b", text)
    if m:
        raw["location"] = m.group(1).strip()
        raw["state"] = m.group(2).strip()

    if "COPART" in upper_text:
        raw["source"] = "Copart"
    elif "IAAI" in upper_text:
        raw["source"] = "IAAI"

    if raw["title"] or raw["vin"] or raw["price_raw"]:
        return raw
    return None


def build_record(raw: dict, existing_keys: set, seen: set, skip_counts: dict) -> Optional[dict]:
    price = clean_price(raw.get("price_raw", ""))
    if price < MIN_PRICE:
        skip_counts["price_below_min"] += 1
        return None

    make = str(raw.get("make", "")).strip()
    model = str(raw.get("model", "")).strip()
    year = str(raw.get("year", "")).strip()

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

    vin = str(raw.get("vin", "")).strip()
    lot = str(raw.get("lot", "")).strip()

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
    }

    seen.add(key)
    existing_keys.add(key)
    skip_counts["kept"] += 1
    return record


# ─────────────────────────────────────────────
# PAGE EXTRACTION
# ─────────────────────────────────────────────
async def goto_fast(page, url: str, timeout=15000):
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    await page.wait_for_timeout(200)


async def get_total_pages(page) -> int:
    await goto_fast(page, BASE_URL)
    try:
        max_page = await page.evaluate("""
        () => {
            const nums = [];
            const anchors = Array.from(document.querySelectorAll("a, button, span"));
            for (const el of anchors) {
                const txt = (el.textContent || "").trim();
                if (/^\\d+$/.test(txt)) nums.push(parseInt(txt, 10));
            }
            return nums.length ? Math.max(...nums) : null;
        }
        """)
        if isinstance(max_page, int) and max_page > 0:
            return max_page
    except Exception:
        pass
    return MAX_PAGE_GUESS


async def extract_cards_from_page(page, page_num: int) -> list:
    url = build_paged_url(page_num)
    await goto_fast(page, url)

    payloads = await page.evaluate("""
    () => {
        const selectors = [
            "article",
            "[class*='card']",
            "[class*='Card']",
            "[class*='listing']",
            "[class*='Listing']",
            "[class*='vehicle']",
            "[class*='Vehicle']",
            "[class*='result']",
            "[class*='Result']"
        ];

        let best = [];
        for (const sel of selectors) {
            const nodes = Array.from(document.querySelectorAll(sel));
            if (nodes.length > best.length) best = nodes;
        }

        const results = [];
        for (const node of best) {
            const text = (node.innerText || "").trim();
            if (!text) continue;

            const upper = text.toUpperCase();
            if (!upper.includes("VIN") && !text.includes("$")) continue;

            let title = "";
            const titleNode = node.querySelector("h1, h2, h3, h4, [class*='title'], [class*='Title'], [class*='name'], [class*='Name']");
            if (titleNode) title = (titleNode.innerText || "").trim();

            results.push({ text, title });
        }

        return results;
    }
    """)

    rows = []
    for payload in payloads:
        raw = parse_card_payload(payload)
        if raw:
            rows.append(raw)
    return rows


# ─────────────────────────────────────────────
# WORKER
# ─────────────────────────────────────────────
async def worker(worker_id: int, browser, page_numbers: asyncio.Queue, all_data: list,
                 seen: set, existing_keys: set, skip_counts: dict,
                 data_lock: asyncio.Lock, progress: dict):
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1400, "height": 1000},
    )

    await context.route(
        "**/*",
        lambda route: route.abort() if route.request.resource_type in {"image", "font", "media"} else route.continue_()
    )

    page = await context.new_page()

    try:
        while True:
            page_num = await page_numbers.get()
            if page_num is None:
                page_numbers.task_done()
                break

            try:
                raw_rows = await extract_cards_from_page(page, page_num)
                print(f"[W{worker_id}] Page {page_num}: found {len(raw_rows)} raw rows")

                if not raw_rows:
                    async with data_lock:
                        progress["consecutive_empty"] += 1
                    page_numbers.task_done()
                    continue

                async with data_lock:
                    progress["consecutive_empty"] = 0

                kept_here = 0
                new_records = []

                async with data_lock:
                    for raw in raw_rows:
                        record = build_record(raw, existing_keys, seen, skip_counts)
                        if record:
                            new_records.append(record)
                            kept_here += 1

                    all_data.extend(new_records)
                    progress["pages_done"] += 1
                    progress["rows_kept"] = len(all_data)

                print(f"[W{worker_id}] Page {page_num}: kept {kept_here}")

            except Exception as e:
                print(f"[W{worker_id}] Page {page_num}: ERROR {e}")

            page_numbers.task_done()

    finally:
        await page.close()
        await context.close()


# ─────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────
def write_csvs(all_data: list):
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
        for col in ["price", "make", "vin", "lot"]:
            if col not in master_df.columns:
                master_df[col] = ""

        master_df["price"] = pd.to_numeric(master_df["price"], errors="coerce").fillna(0).astype(int)
        master_df = master_df[master_df["price"] >= MIN_PRICE]
        master_df = master_df[~master_df["make"].str.upper().isin(EXCLUDED_MAKES)]
        master_df = master_df.drop_duplicates(subset=["vin", "lot"], keep="first")

    master_df.to_csv(MASTER_CSV, index=False)

    website_df = master_df.copy()
    for col in website_cols:
        if col not in website_df.columns:
            website_df[col] = ""
    website_df = website_df[website_cols]
    website_df.to_csv(WEBSITE_CSV, index=False)

    return len(master_df)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    run_start = time.time()

    all_data = []
    seen = set()
    existing_keys = load_existing_keys(MASTER_CSV)

    skip_counts = {
        "price_below_min": 0,
        "excluded_make": 0,
        "duplicate": 0,
        "existing": 0,
        "incomplete": 0,
        "kept": 0,
    }

    progress = {
        "pages_done": 0,
        "rows_kept": 0,
        "consecutive_empty": 0,
    }

    print(f"Existing records: {len(existing_keys)}")
    print(f"Minimum price:    ${MIN_PRICE}")
    print(f"Workers:          {WORKERS}")
    print(f"Resume existing:  {RESUME_FROM_MASTER}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        page_for_count = await browser.new_page()
        try:
            total_pages = await get_total_pages(page_for_count)
        finally:
            await page_for_count.close()

        if not isinstance(total_pages, int) or total_pages <= 0:
            total_pages = MAX_PAGE_GUESS

        print(f"Total pages target: {total_pages}")

        q = asyncio.Queue()
        for page_num in range(1, total_pages + 1):
            await q.put(page_num)

        for _ in range(WORKERS):
            await q.put(None)

        data_lock = asyncio.Lock()

        tasks = [
            asyncio.create_task(
                worker(
                    worker_id=i + 1,
                    browser=browser,
                    page_numbers=q,
                    all_data=all_data,
                    seen=seen,
                    existing_keys=existing_keys,
                    skip_counts=skip_counts,
                    data_lock=data_lock,
                    progress=progress,
                )
            )
            for i in range(WORKERS)
        ]

        await q.join()
        await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    total_master = write_csvs(all_data)

    print("\n" + "=" * 50)
    print("DONE")
    print(f"New rows this run:      {len(all_data)}")
    print(f"Total in {MASTER_CSV}: {total_master}")
    print(f"Skip stats:            {skip_counts}")
    print(f"Elapsed seconds:       {int(time.time() - run_start)}")


if __name__ == "__main__":
    asyncio.run(main())
