#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate minimal Kaspi XML from SKU_updated (PUBLIC GitHub source).

Source:
  https://raw.githubusercontent.com/einhellcentralasia/ostatki/main/data/SKU_updated/SKU_updated.csv

- Requires columns: SKU, Model, Stock, RSP (case-insensitive). Extra columns are ignored.
- Produces schema-compliant feed (no <preOrder>, integer <price>).
- Writes docs/price.xml for GitHub Pages.

Required ENV:
  COMPANY_NAME        e.g., Einhell
  MERCHANT_ID         e.g., 30245761
  KASPI_STORE_ID      e.g., PP1

Optional ENV:
  SKU_UPDATED_CSV_URL (override default URL)
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from lxml import etree

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------- helpers ----------
def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

COMPANY_NAME   = env("COMPANY_NAME")
MERCHANT_ID    = env("MERCHANT_ID")
KASPI_STORE_ID = env("KASPI_STORE_ID")

SKU_UPDATED_CSV_URL = os.getenv(
    "SKU_UPDATED_CSV_URL",
    "https://raw.githubusercontent.com/einhellcentralasia/ostatki/main/data/SKU_updated/SKU_updated.csv"
)

REQUIRED_COLS = ["SKU", "Model", "Stock", "RSP"]  # minimal set for Kaspi


def read_table_from_github_csv(url: str) -> pd.DataFrame:
    logging.info(f"Reading source CSV: {url}")

    # dtype="object" to avoid pandas guessing types too early
    df = pd.read_csv(url, dtype="object", encoding="utf-8")

    # map case-insensitively
    lower_map = {c.lower(): c for c in df.columns}
    missing = [c for c in REQUIRED_COLS if c.lower() not in lower_map]
    if missing:
        raise ValueError(
            f"Source must contain columns (any case): {REQUIRED_COLS}. Missing: {missing}. "
            f"Columns found: {list(df.columns)}"
        )

    # keep only required, normalize names
    df = df[[lower_map[c.lower()] for c in REQUIRED_COLS]].copy()
    df.columns = REQUIRED_COLS

    # clean/convert (same semantics as before)
    df["SKU"]   = df["SKU"].astype(str).str.strip()
    df["Model"] = df["Model"].astype(str).str.strip()
    df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)
    df["RSP"]   = pd.to_numeric(df["RSP"], errors="coerce").fillna(0)

    # drop empty SKU rows
    df = df[df["SKU"] != ""].reset_index(drop=True)

    return df


def build_kaspi_xml(df: pd.DataFrame) -> bytes:
    # Root with namespace & schemaLocation
    nsmap = {None: "kaspiShopping", "xsi": "http://www.w3.org/2001/XMLSchema-instance"}
    root = etree.Element(
        "kaspi_catalog",
        nsmap=nsmap,
        attrib={
            "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation":
                "kaspiShopping http://kaspi.kz/kaspishopping.xsd",
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        },
    )

    etree.SubElement(root, "company").text    = str(COMPANY_NAME)
    etree.SubElement(root, "merchantid").text = str(MERCHANT_ID)
    offers = etree.SubElement(root, "offers")

    for _, r in df.iterrows():
        sku   = r["SKU"]
        model = r["Model"]
        stock = max(int(r["Stock"]), 0)

        # Kaspi: price must be INTEGER (KZT)
        try:
            price_int = int(round(float(r["RSP"])))
        except Exception:
            price_int = 0
        price_int = max(price_int, 0)

        offer = etree.SubElement(offers, "offer", sku=str(sku))
        etree.SubElement(offer, "model").text = str(model)

        avs = etree.SubElement(offer, "availabilities")
        etree.SubElement(
            avs, "availability",
            available=("yes" if stock > 0 else "no"),
            storeId=str(KASPI_STORE_ID),
            stockCount=str(stock),
        )

        etree.SubElement(offer, "price").text = str(price_int)

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", pretty_print=True)


def write_xml(xml_bytes: bytes, out_path: str = "docs/price.xml"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(xml_bytes)


def main() -> int:
    try:
        df = read_table_from_github_csv(SKU_UPDATED_CSV_URL)
        xml_b = build_kaspi_xml(df)
        write_xml(xml_b)

        print("SUCCESS: docs/price.xml generated.")
        return 0
    except Exception as e:
        logging.exception("Run failed")
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
