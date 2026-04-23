def enrich_with_prices(df: pd.DataFrame, existing_vins: set, existing_lots: set, run_start: float) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print("  STEP 3: Price lookup by LOT first, VIN second")
    print(f"{'='*60}")

    vin_url_cache = load_json_cache(VIN_URL_CACHE_FILE)
    price_cache = load_json_cache(PRICE_CACHE_FILE)
    lot_url_cache = load_json_cache(LOT_URL_CACHE_FILE)
    lot_price_cache = load_json_cache(LOT_PRICE_CACHE_FILE)

    session = build_session()

    try:
        session.get("https://autobidcar.com/", timeout=10)
        time.sleep(0.8)
    except Exception:
        pass

    df = df.copy()
    df["final_price"] = None
    df["price_source"] = ""
    df["matched_url"] = None

    vin_col = None
    for c in ["vin", "VIN", "vehicleId"]:
        if c in df.columns:
            vin_col = c
            break

    lot_col = None
    for c in ["lotId", "lot", "lot_id"]:
        if c in df.columns:
            lot_col = c
            break

    vin_series = normalize_vin_series(df[vin_col]) if vin_col else pd.Series("", index=df.index)
    lot_series = df[lot_col].astype(str).str.strip() if lot_col else pd.Series("", index=df.index)

    existing_priced_lots = load_existing_priced_lots()
    existing_priced_vins = load_existing_priced_vins()

    lot_candidates = []
    if lot_col:
        lot_candidates = (
            lot_series[
                lot_series.notna() &
                (lot_series != "") &
                (~lot_series.isin(existing_priced_lots))
            ]
            .drop_duplicates()
            .tolist()
        )[:MAX_LOT_LOOKUPS]

    print(f"  Lots total: {lot_series.notna().sum() if lot_col else 0}")
    print(f"  Lots with price already: {len(existing_priced_lots)}")
    print(f"  Lots to look up: {len(lot_candidates):,} (capped at {MAX_LOT_LOOKUPS})")

    lot_to_url = {}
    lot_to_price = {}

    for idx, lot in enumerate(lot_candidates, start=1):
        if time.time() - run_start > MAX_RUN_SECONDS:
            print("  Runtime limit reached during lot lookup, stopping early.")
            break

        price, url = scrape_price_by_lot(lot, session, lot_url_cache, lot_price_cache)

        if url:
            lot_to_url[lot] = url
        if price:
            lot_to_price[lot] = price

        if idx % 50 == 0 or idx == len(lot_candidates):
            print(f"  Lot lookup progress: {idx}/{len(lot_candidates)} — prices found: {len(lot_to_price)}")

        maybe_sleep(LOT_DELAY)

    print(f"\n  Lot-based prices found: {len(lot_to_price)} / {len(lot_candidates)}")

    vin_candidates = []
    if vin_col:
        vin_candidates = (
            vin_series[
                vin_series.notna() &
                (vin_series != "") &
                (vin_series != "[PREMIUM]") &
                (vin_series.str.fullmatch(r"[A-HJ-NPR-Z0-9]{17}", na=False)) &
                (~vin_series.isin(existing_priced_vins))
            ]
            .drop_duplicates()
            .tolist()
        )[:MAX_VIN_LOOKUPS]

    print(f"  VINs total: {vin_series.notna().sum() if vin_col else 0}")
    print(f"  VINs with price already: {len(existing_priced_vins)}")
    print(f"  VINs to look up: {len(vin_candidates):,} (capped at {MAX_VIN_LOOKUPS})")

    vin_to_url = {}
    for idx, vin in enumerate(vin_candidates, start=1):
        if time.time() - run_start > MAX_RUN_SECONDS:
            print("  Runtime limit reached during VIN lookup, stopping early.")
            break

        url = get_autobidcar_url(vin, session, vin_url_cache)
        if url:
            vin_to_url[vin] = url

        if idx % 50 == 0 or idx == len(vin_candidates):
            print(f"  VIN lookup progress: {idx}/{len(vin_candidates)} — URLs found: {len(vin_to_url)}")

        maybe_sleep(API_DELAY)

    vin_to_price = {}
    if vin_to_url and (time.time() - run_start <= MAX_RUN_SECONDS):
        with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as ex:
            futures = [
                ex.submit(_lookup_price_for_vin, vin, url, price_cache)
                for vin, url in vin_to_url.items()
            ]
            done = 0
            for fut in as_completed(futures):
                if time.time() - run_start > MAX_RUN_SECONDS:
                    print("  Runtime limit reached during VIN detail scraping, stopping early.")
                    break

                vin, price = fut.result()
                done += 1
                if price:
                    vin_to_price[vin] = price
                if done % 50 == 0 or done == len(futures):
                    print(f"  VIN detail scrape progress: {done}/{len(futures)} — prices found: {len(vin_to_price)}")

    print(f"\n  VIN-based prices found: {len(vin_to_price)} / {len(vin_to_url)}")

    df["final_price"] = lot_series.map(lot_to_price)
    df["price_source"] = df["final_price"].notna().map(lambda x: "lot" if x else "")
    df["matched_url"] = lot_series.map(lot_to_url)

    vin_prices = vin_series.map(vin_to_price)
    vin_urls = vin_series.map(vin_to_url)
    needs_vin = df["final_price"].isna() & vin_prices.notna()

    df.loc[needs_vin, "final_price"] = vin_prices[needs_vin]
    df.loc[needs_vin, "price_source"] = "vin"
    df.loc[needs_vin, "matched_url"] = vin_urls[needs_vin]

    save_json_cache(VIN_URL_CACHE_FILE, vin_url_cache)
    save_json_cache(PRICE_CACHE_FILE, price_cache)
    save_json_cache(LOT_URL_CACHE_FILE, lot_url_cache)
    save_json_cache(LOT_PRICE_CACHE_FILE, lot_price_cache)

    return df
