#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate Kaspi autoload XML from a SINGLE Excel table via Microsoft Graph (Application permissions).
- Pulls ONLY the named table (SP_TABLE_NAME) from SP_XLSX_PATH on SharePoint.
- Hard-fails if headers ≠ ["SKU","Model","In_transit","Total_preorders","Stock","RSP"] in this exact order.
- Writes docs/price.xml for GitHub Pages.
"""

import os, sys, logging
from datetime import datetime
from typing import List, Tuple
from urllib.parse import quote

# ---- Error handling & logging ----
try:
    import requests
    from lxml import etree
    import pandas as pd
    import msal
except Exception as e:
    print(f"[FATAL] Dependency import failed: {e}", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ----------- Config helpers -----------
def env(name: str, required: bool = True, default: str = None) -> str:
    val = os.getenv(name, default)
    if val is not None:
        val = val.strip()  # trim accidental spaces/newlines
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val

TENANT_ID   = env("TENANT_ID")
CLIENT_ID   = env("CLIENT_ID")
CLIENT_SECRET = env("CLIENT_SECRET")

SP_SITE_HOSTNAME = env("SP_SITE_HOSTNAME")
SP_SITE_PATH     = env("SP_SITE_PATH")
SP_XLSX_PATH     = env("SP_XLSX_PATH")
SP_TABLE_NAME    = env("SP_TABLE_NAME")

COMPANY_NAME   = env("COMPANY_NAME")
MERCHANT_ID    = env("MERCHANT_ID")
KASPI_STORE_ID = env("KASPI_STORE_ID")

DEFAULT_PREORDER_DAYS = int(os.getenv("DEFAULT_PREORDER_DAYS", "3"))

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
EXPECTED_HEADERS = ["SKU","Model","In_transit","Total_preorders","Stock","RSP"]

# ----------- Graph auth -----------
def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MS Graph auth failed: {result.get('error_description', 'no_description')}")
    return result["access_token"]

def gget(url: str, token: str, timeout: int = 30) -> dict:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Graph GET {url} failed: {r.status_code} {r.text[:300]}")
    return r.json()

# ----------- Locate site & file -----------
def resolve_site_id(token: str) -> str:
    url = f"{GRAPH_BASE}/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}"
    data = gget(url, token)
    return data["id"]

def resolve_item_id(site_id: str, token: str) -> str:
    path_enc = quote(SP_XLSX_PATH, safe="/:+()%!$&',;=@")
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root:{path_enc}"
    data = gget(url, token)
    return data["id"]

# ----------- Read ONLY the named table -----------
def read_table_values(site_id: str, item_id: str, token: str) -> Tuple[List[str], List[List]]:
    table_seg = quote(SP_TABLE_NAME, safe="")
    base = f"{GRAPH_BASE}/sites/{site_id}/drive/items/{item_id}/workbook/tables/{table_seg}"
    hdr = gget(f"{base}/headerRowRange", token)
    headers = hdr.get("values", [[]])[0] if hdr.get("values") else []
    headers = [str(h).strip() for h in headers]
    body = gget(f"{base}/dataBodyRange", token)
    rows = body.get("values", []) or []
    return headers, rows

def validate_headers(headers: List[str]):
    if headers != EXPECTED_HEADERS:
        raise ValueError(
            "Table schema mismatch.\n"
            f"Expected: {EXPECTED_HEADERS}\n"
            f"Found   : {headers}\n"
            "Refuse to continue (safety lock)."
        )

def to_dataframe(headers: List[str], rows: List[List]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=headers)
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["Model"] = df["Model"].astype(str).str.strip()
    for col in ["In_transit","Total_preorders","Stock"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["RSP"] = pd.to_numeric(df["RSP"], errors="coerce").fillna(0).round(2)
    df = df[df["SKU"] != ""].copy()
    return df

# ----------- Build Kaspi XML -----------
def build_kaspi_xml(df: pd.DataFrame) -> bytes:
    root = etree.Element("kaspi_catalog",
                         date=datetime.now().strftime("%Y-%m-%d %H:%M"),
                         currency="KZT")
    etree.SubElement(root, "company").text = str(COMPANY_NAME)
    etree.SubElement(root, "merchantid").text = str(MERCHANT_ID)
    offers = etree.SubElement(root, "offers")
    for _, r in df.iterrows():
        sku, model = str(r["SKU"]), str(r["Model"])
        stock = int(r["Stock"])
        in_transit = int(r["In_transit"])
        preorders = int(r["Total_preorders"])
        price = float(r["RSP"])
        offer = etree.SubElement(offers, "offer", sku=sku)
        etree.SubElement(offer, "model").text = model
        etree.SubElement(offer, "price").text = f"{price:.2f}"
        avs = etree.SubElement(offer, "availabilities")
        available_flag = "yes" if stock > 0 else "no"
        etree.SubElement(
            avs, "availability",
            available=available_flag,
            storeId=str(KASPI_STORE_ID),
            stockCount=str(max(stock, 0))
        )
        if stock <= 0 and (in_transit > 0 or preorders > 0):
            etree.SubElement(offer, "preOrder").text = str(DEFAULT_PREORDER_DAYS)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", pretty_print=True)

# ----------- Write output -----------
def write_xml(xml_bytes: bytes, out_path: str = "docs/price.xml"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(xml_bytes)

# ----------- Main -----------
def main() -> int:
    try:
        token = get_token()
        logging.info("Auth OK (Graph).")
        site_id = resolve_site_id(token)
        logging.info(f"Resolved site id: {site_id}")
        item_id = resolve_item_id(site_id, token)
        logging.info(f"Resolved item id: {item_id}")
        headers, rows = read_table_values(site_id, item_id, token)
        logging.info(f"Read table '{SP_TABLE_NAME}': {len(rows)} rows")
        validate_headers(headers)
        df = to_dataframe(headers, rows)
        logging.info(f"Dataframe rows after normalization: {len(df)}")
        xml_bytes = build_kaspi_xml(df)
        write_xml(xml_bytes)
        print("=== SUCCESS: docs/price.xml generated (Kaspi autoload feed) ===")
        return 0
    except Exception as e:
        logging.exception("Run failed")
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
