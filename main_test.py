import os
import tempfile

import pandas as pd
import pytest

from main import (
    BMW_MODEL_MAP,
    build_paged_url,
    build_record,
    clean_odometer,
    clean_price,
    infer_type,
    is_excluded,
    load_existing_keys,
    normalize_make_model,
    parse_location_state,
    parse_text_block,
    split_title,
)


# ─── clean_price ──────────────────────────────────────────────────────────────

class TestCleanPrice:
    def test_plain_number(self):
        assert clean_price("5000") == 5000

    def test_dollar_sign_and_commas(self):
        assert clean_price("$5,000") == 5000

    def test_no_sale(self):
        assert clean_price("No Sale") == 0

    def test_not_sold(self):
        assert clean_price("Not Sold") == 0

    def test_na(self):
        assert clean_price("N/A") == 0

    def test_empty(self):
        assert clean_price("") == 0

    def test_decimal(self):
        assert clean_price("$1,234.99") == 1234

    def test_integer_from_float_string(self):
        assert clean_price("9999.0") == 9999


# ─── clean_odometer ───────────────────────────────────────────────────────────

class TestCleanOdometer:
    def test_plain_miles(self):
        assert clean_odometer("45000") == "45000 mi"

    def test_comma_separated(self):
        assert clean_odometer("45,000") == "45000 mi"

    def test_with_quotes(self):
        assert clean_odometer('45,000"') == "45000 mi"

    def test_no_digits(self):
        assert clean_odometer("unknown") == ""

    def test_empty(self):
        assert clean_odometer("") == ""


# ─── normalize_make_model ─────────────────────────────────────────────────────

class TestNormalizeMakeModel:
    def test_title_cases_make(self):
        make, model = normalize_make_model("TOYOTA", "Camry")
        assert make == "Toyota"

    def test_bmw_model_mapping(self):
        _, model = normalize_make_model("BMW", "3ER")
        assert model == "3 Series"

    def test_bmw_unknown_model_unchanged(self):
        _, model = normalize_make_model("BMW", "Unknown")
        assert model == "Unknown"

    def test_non_bmw_model_unchanged(self):
        _, model = normalize_make_model("HONDA", "Civic")
        assert model == "Civic"


# ─── split_title ──────────────────────────────────────────────────────────────

class TestSplitTitle:
    def test_standard_title(self):
        year, make, model = split_title("2019 Toyota Camry")
        assert year == "2019"
        assert make == "Toyota"
        assert model == "Camry"

    def test_multi_word_make(self):
        year, make, model = split_title("2015 Land Rover Discovery")
        assert year == "2015"
        assert make == "Land Rover"
        assert model == "Discovery"

    def test_alfa_romeo(self):
        year, make, model = split_title("2020 Alfa Romeo Giulia")
        assert make == "Alfa Romeo"
        assert model == "Giulia"

    def test_no_year_returns_empty(self):
        year, make, model = split_title("Toyota Camry")
        assert year == ""
        assert make == ""
        assert model == ""

    def test_empty_string(self):
        assert split_title("") == ("", "", "")

    def test_bmw_model_normalized(self):
        _, make, model = split_title("2018 BMW 3ER")
        assert make == "Bmw"
        assert model == "3 Series"


# ─── infer_type ───────────────────────────────────────────────────────────────

class TestInferType:
    def test_truck(self):
        assert infer_type("2020 Ford F-150") == "Truck"

    def test_silverado_is_truck(self):
        assert infer_type("2019 Chevy Silverado") == "Truck"

    def test_suv(self):
        assert infer_type("2021 Toyota RAV4") == "SUV"

    def test_explorer_is_suv(self):
        assert infer_type("2018 Ford Explorer") == "SUV"

    def test_coupe(self):
        assert infer_type("2022 Ford Mustang") == "Coupe"

    def test_van(self):
        assert infer_type("2017 Honda Odyssey") == "Van"

    def test_default_sedan(self):
        assert infer_type("2020 Honda Civic") == "Sedan"


# ─── parse_location_state ─────────────────────────────────────────────────────

class TestParseLocationState:
    def test_comma_separator(self):
        loc, state = parse_location_state("Los Angeles, CA")
        assert loc == "Los Angeles"
        assert state == "CA"

    def test_dash_separator(self):
        loc, state = parse_location_state("Dallas - TX")
        assert loc == "Dallas"
        assert state == "TX"

    def test_trailing_state_code(self):
        loc, state = parse_location_state("Seattle WA")
        assert loc == "Seattle"
        assert state == "WA"

    def test_no_state(self):
        loc, state = parse_location_state("SomeCity")
        assert loc == "SomeCity"
        assert state == ""

    def test_em_dash_separator(self):
        loc, state = parse_location_state("Miami \u2013 FL")
        assert loc == "Miami"
        assert state == "FL"


# ─── is_excluded ──────────────────────────────────────────────────────────────

class TestIsExcluded:
    def test_land_rover_excluded(self):
        assert is_excluded("LAND ROVER") is True

    def test_land_rover_mixed_case(self):
        assert is_excluded("Land Rover") is True

    def test_toyota_not_excluded(self):
        assert is_excluded("Toyota") is False

    def test_empty_not_excluded(self):
        assert is_excluded("") is False


# ─── build_paged_url ──────────────────────────────────────────────────────────

class TestBuildPagedUrl:
    BASE = "https://example.com/auction/"

    def test_page_one_returns_base(self):
        assert build_paged_url(self.BASE, 1) == self.BASE

    def test_page_two_adds_query(self):
        assert build_paged_url(self.BASE, 2) == self.BASE + "?page=2"

    def test_existing_query_string(self):
        url = "https://example.com/auction/?make=BMW"
        assert build_paged_url(url, 3) == url + "&page=3"

    def test_page_zero_returns_base(self):
        assert build_paged_url(self.BASE, 0) == self.BASE


# ─── load_existing_keys ───────────────────────────────────────────────────────

class TestLoadExistingKeys:
    def test_missing_file_returns_empty(self, tmp_path):
        keys = load_existing_keys(str(tmp_path / "nonexistent.csv"))
        assert keys == set()

    def test_loads_vin_lot_keys(self, tmp_path):
        csv = tmp_path / "master.csv"
        pd.DataFrame([{"vin": "ABC123", "lot": "L1"}]).to_csv(csv, index=False)
        keys = load_existing_keys(str(csv))
        assert "ABC123|L1" in keys

    def test_skips_empty_key(self, tmp_path):
        csv = tmp_path / "master.csv"
        pd.DataFrame([{"vin": "", "lot": ""}]).to_csv(csv, index=False)
        keys = load_existing_keys(str(csv))
        assert "|" not in keys

    def test_missing_columns_handled(self, tmp_path):
        csv = tmp_path / "master.csv"
        pd.DataFrame([{"price": "5000"}]).to_csv(csv, index=False)
        keys = load_existing_keys(str(csv))
        assert isinstance(keys, set)


# ─── build_record ─────────────────────────────────────────────────────────────

def _counts():
    return {"price_range": 0, "excluded_make": 0, "duplicate": 0,
            "existing": 0, "incomplete": 0, "kept": 0}

def _good_raw(**overrides):
    base = {
        "vin": "1HGCM82633A123456", "year": "2020", "make": "Honda",
        "model": "Civic", "trim": "LX", "damage": "Front end",
        "price_raw": "3500", "odometer": "45000", "lot": "LOT1",
        "date": "2024-01-01", "location": "Los Angeles, CA",
        "state": "CA", "source": "Copart", "title": "",
    }
    base.update(overrides)
    return base

class TestBuildRecord:
    def test_valid_record_returned(self):
        rec = build_record(_good_raw(), set(), set(), _counts())
        assert rec is not None
        assert rec["make"] == "Honda"
        assert rec["price"] == 3500

    def test_price_below_min_filtered(self):
        counts = _counts()
        rec = build_record(_good_raw(price_raw="100"), set(), set(), counts)
        assert rec is None
        assert counts["price_range"] == 1

    def test_price_above_max_filtered(self):
        counts = _counts()
        rec = build_record(_good_raw(price_raw="99999"), set(), set(), counts)
        assert rec is None
        assert counts["price_range"] == 1

    def test_excluded_make_filtered(self):
        counts = _counts()
        rec = build_record(_good_raw(make="LAND ROVER"), set(), set(), counts)
        assert rec is None
        assert counts["excluded_make"] == 1

    def test_duplicate_in_seen_filtered(self):
        counts = _counts()
        raw = _good_raw()
        seen = {"1HGCM82633A123456|LOT1"}
        rec = build_record(raw, set(), seen, counts)
        assert rec is None
        assert counts["duplicate"] == 1

    def test_existing_key_filtered(self):
        counts = _counts()
        raw = _good_raw()
        existing = {"1HGCM82633A123456|LOT1"}
        rec = build_record(raw, existing, set(), counts)
        assert rec is None
        assert counts["existing"] == 1

    def test_no_vin_or_lot_filtered(self):
        counts = _counts()
        rec = build_record(_good_raw(vin="", lot=""), set(), set(), counts)
        assert rec is None
        assert counts["incomplete"] == 1

    def test_missing_year_filtered(self):
        counts = _counts()
        rec = build_record(_good_raw(year=""), set(), set(), counts)
        assert rec is None
        assert counts["incomplete"] == 1

    def test_adds_key_to_seen(self):
        seen = set()
        build_record(_good_raw(), set(), seen, _counts())
        assert "1HGCM82633A123456|LOT1" in seen

    def test_type_inferred(self):
        rec = build_record(_good_raw(make="Ford", model="F-150"), set(), set(), _counts())
        assert rec["type"] == "Truck"

    def test_location_state_parsed(self):
        rec = build_record(_good_raw(location="Phoenix, AZ", state=""), set(), set(), _counts())
        assert rec["state"] == "AZ"
        assert rec["location"] == "Phoenix"


# ─── parse_text_block ─────────────────────────────────────────────────────────

class TestParseTextBlock:
    def test_extracts_price(self):
        raw = parse_text_block("2019 Honda Civic\nSold For $4,500\nLot #12345")
        assert raw is not None
        assert "4,500" in raw["price_raw"] or "4500" in raw["price_raw"]

    def test_extracts_vin(self):
        raw = parse_text_block("VIN: 1HGCM82633A123456 Lot #99 $3000")
        assert raw is not None
        assert raw["vin"] == "1HGCM82633A123456"

    def test_extracts_mileage(self):
        raw = parse_text_block("2018 Toyota Camry\n$5000\n45,000 miles")
        assert raw is not None
        assert raw["odometer"] == "45000 mi"

    def test_returns_none_when_no_identifiers(self):
        result = parse_text_block("Nothing useful here at all")
        assert result is None

    def test_extracts_location(self):
        raw = parse_text_block("2020 Ford Focus $2500 Dallas, TX")
        assert raw is not None
        assert raw["state"] == "TX"
