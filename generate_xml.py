#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate minimal Kaspi XML from ONE Excel table via Microsoft Graph (application permissions).
- Requires columns: SKU, Model, Stock, RSP (case-insensitive). Extra columns are ignored.
- Produces schema-compliant feed (no <preOrder>, integer <price>).
- Writes docs/price.xml for GitHub Pages.

Required ENV (GitHub Secrets):
  TENANT_ID           (Azure AD tenant ID)
  CLIENT_ID           (App registration ID)
  CLIENT_SECRET       (Client secret)
  SP_SITE_HOSTNAME    e.g., bavatools.sharepoint.com
  SP_SITE_PATH        e.g., /sites/einhell_common
  SP_XLSX_PATH        e.g., /Shared Documents/General/_system_files/Bava_data.xlsx
  SP_TABLE_NAME       e.g., kaspi_table
  COMPANY_NAME        e.g., Einhell
  MERCHANT_ID         e.g., 30245761
  KASPI_STORE_ID      e.g., PP1
"""

import os
import sys
import logging
from datetime import datetime
from urllib.parse import quote, unquote

import pandas as pd
import requests
from lxml import etree
import msal

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------- helpers ----------
def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

TENANT_ID        = env("TENANT_ID")
CLIENT_ID        = env("CLIENT_ID")
CLIENT_SECRET    = env("CLIENT_SECRET")

SP_SITE_HOSTNAME = env("SP_SITE_HOSTNAME")
SP_SITE_PATH     = env("SP_SITE_PATH")
SP_XLSX_PATH     = env("SP_XLSX_PATH")
SP_TABLE_NAME    = env("SP_TABLE_NAME")

COMPANY_NAME     = env("COMPANY_NAME")
MERCHANT_ID      = env("MERCHANT_ID")
KASPI_STORE_ID   = env("KASPI_STORE_ID")

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]

REQUIRED_COLS = ["SKU", "Model", "Stock", "RSP"]  # minimal set for Kaspi

def get_token() -> str:
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MS Graph auth failed: {result}")
    return result["access_token"]

def gget(url: str, token: str, timeout: int = 30) -> dict:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Graph GET failed {r.status_code}: {r.text[:400]}")
    return r.json()

def gget_raw(url: str, token: str, timeout: int = 30) -> requests.Response:
    return requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)

def resolve_site_id(token: str) -> str:
    data = gget(f"{GRAPH_BASE}/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}", token)
    return data["id"]

def try_item_by_path(site_id: str, path: str, token: str):
    path = path if path.startswith("/") else "/" + path
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root:{quote(path, safe='/:+()%!$&\',;=@')}"
    return gget_raw(url, token)

def search_item(site_id: str, filename: str, token: str):
    q = quote(filename, safe="")
    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root/search(q='{q}')"
    return gget(url, token).get("value", [])

def resolve_item_id(site_id: str, token: str) -> str:
    # 1) try direct
    for candidate in {
        SP_XLSX_PATH,
        SP_XLSX_PATH.replace("/Shared Documents", "/Documents"),
        SP_XLSX_PATH.replace("/Documents", "/Shared Documents"),
        SP_XLSX_PATH.replace("/Shared Documents/", "/") if SP_XLSX_PATH.startswith("/Shared Documents/") else SP_XLSX_PATH,
        SP_XLSX_PATH.replace("/Documents/", "/")         if SP_XLSX_PATH.startswith("/Documents/") else SP_XLSX_PATH,
    }:
        r = try_item_by_path(site_id, candidate, token)
        if r.status_code < 400:
            logging.info(f"Resolved by path: {candidate}")
            return r.json()["id"]

    # 2) fallback: search by name and match folder tail
    filename = os.path.basename(unquote(SP_XLSX_PATH))
    folder   = os.path.dirname(unquote(SP_XLSX_PATH)).replace("\\", "/")
    variants = {
        folder,
        folder.replace("/Shared Documents", "/Documents"),
        folder.replace("/Documents", "/Shared Documents"),
        folder.replace("/Shared Documents/", "/") if folder.startswith("/Shared Documents/") else folder,
        folder.replace("/Documents/", "/")         if folder.startswith("/Documents/") else folder,
    }
    for it in search_item(site_id, filename, token):
        parent = it.get("parentReference", {}).get("path", "")
        if any(parent.endswith(v) or ("/drive/root:" + v) in parent for v in variants):
            logging.info(f"Resolved by search: {it.get('name')} @ {parent}")
            return it["id"]

    raise RuntimeError("Excel file not found via Graph.")

def read_table(site_id: str, item_id: str, token: str) -> pd.DataFrame:
    base = f"{GRAPH_BASE}/sites/{site_id}/drive/items/{item_id}/workbook/tables/{quote(SP_TABLE_NAME, safe='')}"
    hdr  = gget(f"{base}/headerRowRange", token).get("values", [[]])
    headers = [str(h).strip() for h in (hdr[0] if hdr else [])]

    body = gget(f"{base}/dataBodyRange", token).get("values", []) or []
    df = pd.DataFrame(body, columns=headers)

    # map case-insensitively
    lower_map = {c.lower(): c for c in df.columns}
    missing = [c for c in REQUIRED_COLS if c.lower() not in lower_map]
    if missing:
        raise ValueError(f"Table '{SP_TABLE_NAME}' must contain columns (any case): {REQUIRED_COLS}. Missing: {missing}")

    # keep only required
    df = df[[lower_map[c.lower()] for c in REQUIRED_COLS]].copy()
    df.columns = REQUIRED_COLS  # normalize names

    # clean/convert
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

    etree.SubElement(root, "company").text   = str(COMPANY_NAME)
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
        token  = get_token()
        site   = resolve_site_id(token)
        item   = resolve_item_id(site, token)
        df     = read_table(site, item, token)

        xml_b  = build_kaspi_xml(df)
        write_xml(xml_b)

        print("SUCCESS: docs/price.xml generated.")
        return 0
    except Exception as e:
        logging.exception("Run failed")
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
