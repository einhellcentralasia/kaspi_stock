#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate Kaspi autoload XML from ONE Excel table in SharePoint using Microsoft Graph (application permissions).
- Auth: Azure Entra app (CLIENT_ID / CLIENT_SECRET / TENANT_ID).
- File resolve: robust (direct path variants + search fallback).
- Strict header validation: ["SKU","Model","In_transit","Total_preorders","Stock","RSP"].
- Outputs docs/price.xml with Kaspi-required namespace & schemaLocation (NO 'currency' on root).
- Writes <price> as INTEGER (Kaspi XSD requires integer KZT), stock/preorder as integers.
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Tuple
from urllib.parse import quote, unquote
from decimal import Decimal, ROUND_HALF_UP

# ---------- Third-party ----------
# requirements.txt should include:
#   msal
#   requests
#   lxml
#   pandas
import requests
import msal
import pandas as pd
from lxml import etree

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------- Env helpers ----------
def env(name: str, required: bool = True, default: str = None) -> str:
    val = os.getenv(name, default)
    if val is not None:
        val = val.strip()
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val

TENANT_ID         = env("TENANT_ID")
CLIENT_ID         = env("CLIENT_ID")
CLIENT_SECRET     = env("CLIENT_SECRET")

SP_SITE_HOSTNAME  = env("SP_SITE_HOSTNAME")  # e.g. bavatools.sharepoint.com
SP_SITE_PATH      = env("SP_SITE_PATH")      # e.g. /sites/einhell_common
SP_XLSX_PATH      = env("SP_XLSX_PATH")      # e.g. /Shared Documents/General/_system_files/tref_file.xlsx
SP_TABLE_NAME     = env("SP_TABLE_NAME")     # e.g. tref_table

COMPANY_NAME      = env("COMPANY_NAME")      # e.g. TREF
MERCHANT_ID       = env("MERCHANT_ID")       # e.g. 30332726
KASPI_STORE_ID    = env("KASPI_STORE_ID")    # e.g. PP1

DEFAULT_PREORDER_DAYS = int(os.getenv("DEFAULT_PREORDER_DAYS", "3"))

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]

EXPECTED_HEADERS = ["SKU", "Model", "In_transit", "Total_preorders", "Stock", "RSP"]


# ---------- Graph auth / GET ----------
def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
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


def gget_raw(url: str, token: str, timeout: int = 30) -> requests.Response:
    return requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)


# ---------- Resolve site & file (robust) ----------
def resolve_site_id(token: str) -> str:
    url = f"{GRAPH_BASE}/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}"
    data = gget(url, token)
    return data["id"]


def try_item_by_path(site_id: str, path: str, token: str):
    path_enc = quote(path, safe="/:+()%!$&',;=@")
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root:{path_enc}"
    return gget_raw(url, token)


def search_item(site_id: str, filename: str, token: str):
    q = quote(filename, safe="")
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root/search(q='{q}')"
    data = gget(url, token)
    return data.get("value", [])


def _folder_variants(folder_hint: str) -> List[str]:
    v = {folder_hint}
    v.add(folder_hint.replace("/Shared Documents", "/Documents", 1))
    if folder_hint.startswith("/Shared Documents/"):
        v.add(folder_hint.replace("/Shared Documents/", "/", 1).rstrip("/"))
    if folder_hint.startswith("/Documents/"):
        v.add(folder_hint.replace("/Documents/", "/", 1).rstrip("/"))
    return list(v)


def resolve_item_id(site_id: str, token: str) -> str:
    tried = []
    candidates = [
        SP_XLSX_PATH,
        SP_XLSX_PATH.replace("/Shared Documents", "/Documents", 1),
        SP_XLSX_PATH.replace("/Documents", "/Shared Documents", 1),
    ]
    if SP_XLSX_PATH.startswith("/Shared Documents/"):
        candidates.append(SP_XLSX_PATH.replace("/Shared Documents", "", 1))
    if SP_XLSX_PATH.startswith("/Documents/"):
        candidates.append(SP_XLSX_PATH.replace("/Documents", "", 1))

    for c in candidates:
        c = c if c.startswith("/") else ("/" + c)
        r = try_item_by_path(site_id, c, token)
        tried.append((c, r.status_code))
        if r.status_code < 400:
            logging.info(f"Resolved by path: {c}")
            return r.json()["id"]

    filename = os.path.basename(unquote(SP_XLSX_PATH))
    folder_hint = os.path.dirname(unquote(SP_XLSX_PATH)).replace("\\", "/")
    variants = _folder_variants(folder_hint)
    results = search_item(site_id, filename, token)

    for it in results:
        parent_path = it.get("parentReference", {}).get("path", "")
        for v in variants:
            if parent_path.endswith(v) or ("/drive/root:" + v) in parent_path:
                logging.info(f"Resolved by search: {it.get('name')} [{it.get('id')}] @ {parent_path}")
                return it["id"]

    if results:
        best = results[0]
        logging.warning(f"Resolved by search (first match): {best.get('name')} @ {best.get('parentReference', {}).get('path','')}")
        return best["id"]

    tried_info = "; ".join([f"{p} -> {code}" for p, code in tried])
    raise RuntimeError(
        "Excel file not found via Graph.\n"
        f" Tried paths: {tried_info}\n"
        f" Search for '{filename}' returned 0 items."
    )


# ---------- Read ONLY the named table ----------
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
    for col in ["In_transit", "Total_preorders", "Stock"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    # RSP can be decimal in Excel; will be rounded to integer (KZT) later
    df["RSP"] = pd.to_numeric(df["RSP"], errors="coerce").fillna(0)
    df = df[df["SKU"] != ""].copy()
    return df


# ---------- Helpers ----------
def as_int(value) -> int:
    """Round half up to nearest integer (Kaspi XSD expects integer)."""
    return int(Decimal(str(value)).quantize(0, rounding=ROUND_HALF_UP))


# ---------- Build Kaspi XML (XSD-compliant root; integer price) ----------
def build_kaspi_xml(df: pd.DataFrame) -> bytes:
    NS_DEFAULT = "kaspiShopping"
    NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
    nsmap = {None: NS_DEFAULT, "xsi": NS_XSI}

    root = etree.Element("kaspi_catalog", nsmap=nsmap)
    root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    root.set(f"{{{NS_XSI}}}schemaLocation", f"{NS_DEFAULT} http://kaspi.kz/kaspishopping.xsd")

    # Header
    etree.SubElement(root, "company").text = str(COMPANY_NAME)
    etree.SubElement(root, "merchantid").text = str(MERCHANT_ID)

    offers = etree.SubElement(root, "offers")

    for _, r in df.iterrows():
        sku        = str(r["SKU"])
        model      = str(r["Model"])
        stock      = int(r["Stock"])
        in_transit = int(r["In_transit"])
        preorders  = int(r["Total_preorders"])
        price_val  = as_int(r["RSP"])              # << INTEGER price

        offer = etree.SubElement(offers, "offer", sku=sku)

        # Recommended order: model -> availabilities -> price
        etree.SubElement(offer, "model").text = model

        avs = etree.SubElement(offer, "availabilities")
        available_flag = "yes" if stock > 0 else "no"
        etree.SubElement(
            avs, "availability",
            available=available_flag,
            storeId=str(KASPI_STORE_ID),
            stockCount=str(max(stock, 0)),         # integer in string form
        )

        if stock <= 0 and (in_transit > 0 or preorders > 0):
            etree.SubElement(offer, "preOrder").text = str(as_int(DEFAULT_PREORDER_DAYS))

        etree.SubElement(offer, "price").text = str(price_val)   # << integer text

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", pretty_print=True)


# ---------- Write output ----------
def write_xml(xml_bytes: bytes, out_path: str = "docs/price.xml"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(xml_bytes)


# ---------- Main ----------
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
