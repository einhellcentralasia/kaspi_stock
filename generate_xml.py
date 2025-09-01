#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kaspi XML from ONE Excel table via Microsoft Graph (Application permissions).
- Auth with CLIENT_ID/SECRET.
- Robust file resolve: tries provided path, "Documents" variant, and search().
- Reads ONLY SP_TABLE_NAME, strict header schema.
- Writes docs/price.xml.
"""

import os, sys, logging
from datetime import datetime
from typing import List, Tuple
from urllib.parse import quote, unquote

# ---- Imports ----
try:
    import requests
    from lxml import etree
    import pandas as pd
    import msal
except Exception as e:
    print(f"[FATAL] Dependency import failed: {e}", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ----------- Env helpers -----------
def env(name: str, required: bool = True, default: str = None) -> str:
    val = os.getenv(name, default)
    if val is not None:
        val = val.strip()
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val

TENANT_ID   = env("TENANT_ID")
CLIENT_ID   = env("CLIENT_ID")
CLIENT_SECRET = env("CLIENT_SECRET")

SP_SITE_HOSTNAME = env("SP_SITE_HOSTNAME")
SP_SITE_PATH     = env("SP_SITE_PATH")         # /sites/einhell_common
SP_XLSX_PATH     = env("SP_XLSX_PATH")         # /Shared Documents/General/_system_files/tref_file.xlsx
SP_TABLE_NAME    = env("SP_TABLE_NAME")        # tref_table

COMPANY_NAME   = env("COMPANY_NAME")
MERCHANT_ID    = env("MERCHANT_ID")
KASPI_STORE_ID = env("KASPI_STORE_ID")

DEFAULT_PREORDER_DAYS = int(os.getenv("DEFAULT_PREORDER_DAYS", "3"))

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
EXPECTED_HEADERS = ["SKU","Model","In_transit","Total_preorders","Stock","RSP"]

# ----------- Graph base -----------
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

def gget_raw(url: str, token: str, timeout: int = 30) -> requests.Response:
    return requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)

# ----------- Resolve site & file (robust) -----------
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
    # Accept all three common shapes
    variants = {folder_hint}
    variants.add(folder_hint.replace("/Shared Documents", "/Documents", 1))
    # Also allow “root-relative” shape (no library segment)
    if folder_hint.startswith("/Shared Documents/"):
        variants.add(folder_hint.replace("/Shared Documents/", "/", 1).rstrip("/"))
    if folder_hint.startswith("/Documents/"):
        variants.add(folder_hint.replace("/Documents/", "/", 1).rstrip("/"))
    return list(variants)

def resolve_item_id(site_id: str, token: str) -> str:
    # 1) direct path tries
    tried = []
    for candidate in [
        SP_XLSX_PATH,                                        # as provided
        SP_XLSX_PATH.replace("/Shared Documents", "/Documents", 1),
        SP_XLSX_PATH.replace("/Documents", "/Shared Documents", 1),
        # try without library name (root of drive is "Documents")
        SP_XLSX_PATH.replace("/Shared Documents", "", 1) if SP_XLSX_PATH.startswith("/Shared Documents/") else SP_XLSX_PATH,
        SP_XLSX_PATH.replace("/Documents", "", 1) if SP_XLSX_PATH.startswith("/Documents/") else SP_XLSX_PATH,
    ]:
        candidate = candidate if candidate.startswith("/") else ("/" + candidate)
        r = try_item_by_path(site_id, candidate, token)
        tried.append((candidate, r.status_code))
        if r.status_code < 400:
            logging.info(f"Resolved by path: {candidate}")
            return r.json()["id"]

    # 2) fallback: search by name, then pick the one whose parent path matches any variant
    filename = os.path.basename(unquote(SP_XLSX_PATH))
    folder_hint = os.path.dirname(unquote(SP_XLSX_PATH)).replace("\\", "/")
    variants = _folder_variants(folder_hint)
    results = search_item(site_id, filename, token)

    # Prefer exact folder match (any variant), else first exact name
    for it in results:
        parent_path = it.get("parentReference", {}).get("path", "")
        for v in variants:
            if parent_path.endswith(v) or ("/drive/root:" + v) in parent_path:
                logging.info(f"Resolved by search: {it.get('name')} [{it.get('id')}] @ {parent_path}")
                return it["id"]

    if results:
        best = results[0]
        logging.warning(f"Resolved by search (fallback first match): {best.get('name')} @ {best.get('parentReference', {}).get('path','')}")
        return best["id"]

    tried_info = "; ".join([f"{p} -> {code}" for p, code in tried])
    raise RuntimeError(
        "Excel file not found via Graph.\n"
        f" Tried paths: {tried_info}\n"
        f" Search for '{filename}' returned 0 items."
    )

# ----------- Read ONLY the named table -----------
EXPECTED_HEADERS = ["SKU","Model","In_transit","Total_preorders","Stock","RSP"]

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
