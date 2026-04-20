import asyncio
import os
import re
import time
import random
from urllib.parse import urljoin, urlparse

import pandas as pd
from playwright.async_api import async_playwright


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://carsbidshistory.com/"
MAKE_LIST_PAGES = 35

YEAR_MIN = 2008
YEAR_MAX = 2015
MIN_PRICE = 500

MASTER_CSV = "master.csv"
WEBSITE_CSV = "cars.csv"

RESUME_FROM_MASTER = True

MAKE_WORKERS = 2
MODEL_WORKERS = 4
DETAIL_WORKERS = 6

NAV_TIMEOUT_MS = 30000
MAX_RETRIES = 3
RETRY_BASE_DELAY_MS = 1200

TARGET_MAKES = set()
TARGET_MODELS = set()

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
    "FRONT END", "REAR END", "SIDE", "ROLLOVER", "ALL OVER",
    "WATER/FLOOD", "MECHANICAL", "MINOR DENT/SCRATCHES", "NORMAL WEAR",
    "UNDERCARRIAGE", "STRIPPED", "BURN", "TOP/ROOF", "HAIL",
    "VANDALISM", "BIOHAZARD/CHEMICAL", "BURN - INTERIOR",
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
    if any(x in text.lower() for x in ["no sale", "not sold", "n/a", "-"]):
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
    if any(x in t for x in ["PICKUP", "F-150", "F150", "SILVERADO", "RAM ", "TUNDRA", "RANGER",
                              "TACOMA", "FRONTIER"]):
        return "Truck"
    if any(x in t for x in ["SUV", "EXPLORER", "ESCAPE", "ROGUE", "EQUINOX", "TAHOE", "SUBURBAN",
                              "RAV4", "CR-V", "CX-5", "XC90", "SORENTO", "SPORTAGE", "PILOT",
                              "HIGHLANDER", "PATHFINDER", "TRAVERSE", "EXPEDITION", "SOUL"]):
        return "SUV"
    if any(x in t for x in ["COUPE", "MUSTANG", "CHALLENGER", "CAMARO", "86", "BRZ"]):
        return "Coupe"
    if any(x in t for x in ["VAN", "TRANSIT", "ODYSSEY", "SIENNA", "PACIFICA", "CARAVAN",
                              "ECONOLINE", "EXPRESS 35"]):
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

    # FIX 1: allow records with only lot OR only vin — original required BOTH to be non-empty
    # but the condition `not vin and not lot` was correct; the real issue is upstream
    # extraction. We keep this but add a fallback lot from URL always.
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
    return (len(segs) == 3 and segs[0] == "make"
            and re.match(r"^\d+-", segs[1])
            and re.match(r"^\d+-", segs[2]))


def is_detail_url(url: str) -> bool:
    segs = path_segments(url)
    if len(segs) != 4 or segs[0] != "make":
        return False
    if not re.match(r"^\d+-", segs[1]) or not re.match(r"^\d+-", segs[2]):
        return False
    # FIX 2: original required segs[3] to start with a 4-digit year followed by "_".
    # The site may use formats like "2012_HONDA_CIVIC_12345678" or "lot-12345678-2012".
    # Accept any 4th segment that contains a 4-digit year anywhere in it.
    return bool(re.search(r"(19|20)\d{2}", segs[3]))


def detail_year_from_url(url: str):
    segs = path_segments(url)
    if len(segs) < 4:
        return ""
    # FIX 3: original anchored to start of segment with `^`; relax to search anywhere.
    m = re.search(r"((?:19|20)\d{2})", segs[3])
    return m.group(1) if m else ""


# ─────────────────────────────────────────────
# BROWSER
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


async def goto_resilient(page, url: str, timeout=NAV_TIMEOUT_MS):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await page.wait_for_timeout(random.randint(250, 600))
            return True
        except Exception as e:
            last_error = e
            try:
                await page.goto(url, wait_until="load", timeout=timeout)
                await page.wait_for_timeout(random.randint(300, 700))
                return True
            except Exception as e2:
                last_error = e2
                delay = RETRY_BASE_DELAY_MS * attempt + random.randint(100, 500)
                print(f"[WARN] goto retry {attempt}/{MAX_RETRIES} for {url}")
                await page.wait_for_timeout(delay)

    print(f"[ERROR] giving up on {url}: {last_error}")
    return False


async def extract_links(page, url: str):
    ok = await goto_resilient(page, url)
    if not ok:
        return []

    try:
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
    except Exception as e:
        print(f"[WARN] extract_links failed for {url}: {e}")
        return []


# ─────────────────────────────────────────────
# FIX 4: Rewritten detail extractor — multi-strategy with structured HTML first
# ─────────────────────────────────────────────
async def extract_detail_data(page, url: str):
    ok = await goto_resilient(page, url)
    if not ok:
        return None

    # Wait briefly for any JS rendering
    await page.wait_for_timeout(500)

    try:
        body_text = await page.locator("body").inner_text(timeout=10000)
    except Exception as e:
        print(f"[WARN] body read failed for {url}: {e}")
        return None

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

    # ── Title ──────────────────────────────────────────────────────
    for sel in ["h1", "h2", ".title", ".vehicle-title", ".car-title"]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                t = (await loc.inner_text()).strip()
                if t:
                    raw["title"] = normalize_whitespace(t)
                    break
        except Exception:
            pass
    if not raw["title"]:
        try:
            raw["title"] = normalize_whitespace(await page.title())
        except Exception:
            pass

    # ── Year/Make/Model from title ─────────────────────────────────
    yr, mk, mo = split_title_with_optional_year(raw["title"])
    if yr:
        raw["year"] = yr
    if mk:
        raw["make"] = mk
    if mo:
        raw["model"] = mo

    # ── Year fallback: URL or body text ───────────────────────────
    if not raw["year"]:
        raw["year"] = detail_year_from_url(url)
    if not raw["year"]:
        m = re.search(r"\b((?:19|20)\d{2})\b", body_text)
        if m:
            raw["year"] = m.group(1)

    # ── FIX 5: VIN — broaden pattern, try structured selectors first ──
    # Try targeted selectors before raw text
    for sel in ["[data-vin]", ".vin", "#vin", "td.vin", "[class*='vin']"]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                t = (await loc.inner_text()).strip()
                if re.fullmatch(r"[A-HJ-NPR-Z0-9]{17}", t.upper()):
                    raw["vin"] = t.upper()
                    break
        except Exception:
            pass

    # Try table label → next cell pattern (common in auction detail pages)
    if not raw["vin"]:
        try:
            # Look for a cell containing "VIN" and grab the adjacent cell
            cells = await page.locator("td, th, dd, li, span, div").all()
            for i, cell in enumerate(cells):
                try:
                    ct = (await cell.inner_text()).strip().upper()
                    if ct in ("VIN", "VIN NUMBER", "VIN #") and i + 1 < len(cells):
                        next_text = (await cells[i + 1].inner_text()).strip()
                        if re.fullmatch(r"[A-HJ-NPR-Z0-9]{17}", next_text.upper()):
                            raw["vin"] = next_text.upper()
                            break
                except Exception:
                    continue
        except Exception:
            pass

    # FIX 5b: Regex fallback — much broader than original (was too strict with \b boundary)
    if not raw["vin"]:
        m = re.search(
            r"(?:VIN|Vin)[^\w]*([A-HJ-NPR-Z0-9]{11,17})",
            body_text, re.IGNORECASE
        )
        if m:
            raw["vin"] = m.group(1).upper()
        else:
            # Standalone 17-char VIN anywhere (no label required)
            m = re.search(r"\b([A-HJ-NPR-Z0-9]{17})\b", body_text)
            if m:
                raw["vin"] = m.group(1).upper()

    # ── FIX 6: Lot — broaden extraction, always fallback to URL ──────
    if not raw["lot"]:
        for sel in ["[data-lot]", ".lot", "#lot", "[class*='lot']"]:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    t = (await loc.inner_text()).strip()
                    if re.fullmatch(r"[A-Z0-9\-]{4,}", t.upper()):
                        raw["lot"] = t
                        break
            except Exception:
                pass

    if not raw["lot"]:
        # Label → next cell
        try:
            cells = await page.locator("td, th, dd, li, span, div").all()
            for i, cell in enumerate(cells):
                try:
                    ct = (await cell.inner_text()).strip().upper()
                    if ct in ("LOT", "LOT #", "LOT NUMBER", "LOT NO") and i + 1 < len(cells):
                        next_text = (await cells[i + 1].inner_text()).strip()
                        if re.fullmatch(r"[A-Z0-9\-]{4,}", next_text.upper()):
                            raw["lot"] = next_text
                            break
                except Exception:
                    continue
        except Exception:
            pass

    if not raw["lot"]:
        m = re.search(r"\bLot\b[^\w]*([A-Z0-9\-]{4,})", body_text, re.IGNORECASE)
        if m:
            raw["lot"] = m.group(1)

    # FIX 6b: Always try URL as lot fallback — original only did this for numeric lots
    if not raw["lot"]:
        segs = path_segments(url)
        if len(segs) == 4:
            slug = segs[3]
            # Try all underscore/dash segments for a lot-looking token
            for token in re.split(r"[_\-]", slug):
                if re.fullmatch(r"\d{5,}", token) or re.fullmatch(r"[A-Z0-9]{6,}", token.upper()):
                    raw["lot"] = token
                    break

    # ── FIX 7: Price — comprehensive multi-strategy extraction ────────
    #
    # Strategy A: known CSS selectors
    price_found = False
    for sel in [
        ".price", ".bid-price", ".sale-price", ".sold-price", ".final-price",
        "[class*='price']", "[class*='bid']", "[class*='sold']",
        ".amount", "[class*='amount']",
    ]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                t = (await loc.inner_text()).strip()
                p = clean_price(t)
                if p > 0:
                    raw["price"] = str(p)
                    price_found = True
                    break
        except Exception:
            pass

    # Strategy B: table/dl label → adjacent value
    if not price_found:
        try:
            cells = await page.locator("td, th, dd, dt, li, span, div, p").all()
            price_labels = {
                "LAST BID", "SALE PRICE", "SOLD FOR", "FINAL BID", "WINNING BID",
                "HAMMER PRICE", "SOLD PRICE", "BID PRICE", "PRICE", "SOLD",
                "FINAL PRICE", "AMOUNT", "SELLING PRICE",
            }
            for i, cell in enumerate(cells):
                try:
                    ct = (await cell.inner_text()).strip().upper().rstrip(":")
                    if ct in price_labels and i + 1 < len(cells):
                        next_text = (await cells[i + 1].inner_text()).strip()
                        p = clean_price(next_text)
                        if p > 0:
                            raw["price"] = str(p)
                            price_found = True
                            break
                except Exception:
                    continue
        except Exception:
            pass

    # Strategy C: regex over full body text — much broader than original
    if not price_found:
        # Patterns ordered from most to least specific
        price_patterns = [
            # "Last Bid: $3,500" or "Last Bid $3,500"
            r"\bLast\s+Bid\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "Sold for $3,500" / "Sold: $3,500"
            r"\bSold\s+(?:for|price|at)?\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "Sale Price: $3,500"
            r"\bSale\s+Price\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "Winning Bid: $3,500"
            r"\bWinning\s+Bid\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "Hammer: $3,500"
            r"\bHammer\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "Final Bid: $3,500"
            r"\bFinal\s+Bid\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "Bid: $3,500"
            r"\bBid\b[^\d$]*\$?\s*([\d,]+(?:\.\d+)?)",
            # "$3,500" as a standalone dollar figure (last resort)
            r"\$([\d,]{3,}(?:\.\d+)?)",
        ]
        for pattern in price_patterns:
            m = re.search(pattern, body_text, re.IGNORECASE)
            if m:
                p = clean_price(m.group(1))
                if p > 0:
                    raw["price"] = str(p)
                    price_found = True
                    break

    # ── FIX 8: Odometer — add label → cell strategy ──────────────────
    if not raw["odometer"]:
        try:
            cells = await page.locator("td, th, dd, dt, li, span, div").all()
            odo_labels = {"ODOMETER", "MILEAGE", "MILES", "KM", "KILOMETERS"}
            for i, cell in enumerate(cells):
                try:
                    ct = (await cell.inner_text()).strip().upper().rstrip(":")
                    if ct in odo_labels and i + 1 < len(cells):
                        next_text = (await cells[i + 1].inner_text()).strip()
                        if re.search(r"\d", next_text):
                            raw["odometer"] = next_text
                            break
                except Exception:
                    continue
        except Exception:
            pass

    if not raw["odometer"]:
        m = re.search(r"\bOdometer\b[:\s]+([\d,.\s]+(?:mi|km|miles)?)", body_text, re.IGNORECASE)
        if m:
            raw["odometer"] = m.group(1).strip()

    # ── Damage ───────────────────────────────────────────────────────
    body_upper = body_text.upper()
    for dmg in DAMAGE_PATTERNS:
        if dmg in body_upper:
            raw["damage"] = dmg.title()
            break

    # ── Source ───────────────────────────────────────────────────────
    if "copart" in body_text.lower():
        raw["source"] = "Copart"
    elif "iaai" in body_text.lower() or "insurance auto" in body_text.lower():
        raw["source"] = "IAAI"

    # ── Date — also try structured selectors ─────────────────────────
    for sel in [".date", ".auction-date", ".sale-date", "[class*='date']", "time"]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                t = (await loc.inner_text()).strip()
                dm = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", t)
                if dm:
                    raw["date"] = dm.group(1)
                    break
        except Exception:
            pass
    if not raw["date"]:
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", body_text)
        if m:
            raw["date"] = m.group(1)

    # ── Location — label → cell strategy ─────────────────────────────
    for sel in [".location", ".auction-location", "[class*='location']"]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                t = (await loc.inner_text()).strip()
                if t:
                    raw["location"] = normalize_whitespace(t)
                    break
        except Exception:
            pass
    if not raw["location"]:
        m = re.search(r"\bBuyer\s+Location\b[:\s]+([^\n]+)", body_text, re.IGNORECASE)
        if m:
            raw["location"] = normalize_whitespace(m.group(1))
    if not raw["location"]:
        m = re.search(r"\bLocation\b[:\s]+([^\n]+)", body_text, re.IGNORECASE)
        if m:
            raw["location"] = normalize_whitespace(m.group(1))

    # ── Trim ─────────────────────────────────────────────────────────
    m = re.search(r"\bTrim\b[:\s]+([^\n]+)", body_text, re.IGNORECASE)
    if m:
        raw["trim"] = normalize_whitespace(m.group(1))

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
            links = await extract_links(page, url)
            page_add = 0
            for link in links:
                if is_make_url(link) and link not in make_urls:
                    make_urls.add(link)
                    page_add += 1
            print(f"[seed] make page {p}: +{page_add}, total {len(make_urls)}")
    finally:
        await page.close()
        await context.close()
    return sorted(make_urls)


async def make_worker(worker_id, browser, in_q, model_urls, model_urls_lock):
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
    detail_urls = set()
    seen_model_pages = set()
    q = asyncio.Queue()

    for u in model_urls:
        u = canonical_url(u)
        await q.put(u)
        seen_model_pages.add(u)

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

                    root = "/".join(path_segments(url)[:3])
                    root_prefix = urljoin(BASE_URL, root + "/")

                    async with model_lock:
                        for link in links:
                            if (link.startswith(root_prefix)
                                    and not is_detail_url(link)
                                    and link not in seen_model_pages):
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
                    if raw is None:
                        q.task_done()
                        continue
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
    print(f"Workers:           make={MAKE_WORKERS}, model={MODEL_WORKERS}, detail={DETAIL_WORKERS}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        make_urls = await seed_make_urls(browser)
        print(f"Discovered make URLs: {len(make_urls)}")

        make_q = asyncio.Queue()
        for u in make_urls:
            await make_q.put(u)

        model_urls = set()
        model_urls_lock = asyncio.Lock()

        make_tasks = [
            asyncio.create_task(make_worker(i + 1, browser, make_q, model_urls, model_urls_lock))
            for i in range(MAKE_WORKERS)
        ]
        await make_q.join()
        for _ in make_tasks:
            await make_q.put(None)
        await asyncio.gather(*make_tasks, return_exceptions=True)

        model_urls = sorted(model_urls)
        print(f"Discovered model URLs: {len(model_urls)}")

        if TARGET_MODELS:
            filtered = []
            for u in model_urls:
                seg = path_segments(u)[-1]
                label = seg.split("-", 1)[-1].replace("-", " ").upper()
                if any(t in label for t in TARGET_MODELS):
                    filtered.append(u)
            model_urls = filtered
            print(f"After TARGET_MODELS filter: {len(model_urls)} model URLs")

        detail_urls = await crawl_model_pages(browser, model_urls)
        print(f"Discovered detail URLs in year range: {len(detail_urls)}")

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
