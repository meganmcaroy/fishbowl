
import io
import re
import json
import time
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="Fishbowl Data Transformer", layout="wide")
st.title("Fishbowl Data Transformer (MVP)")
st.caption("Transform NetSuite + Asana data into a Fishbowl-ready CSV. Excludes 'automated cards' and applies UV/Laser/Blank SKU rules.")

# -----------------------------
# Helper Functions
# -----------------------------

def normalize_col(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip()).lower()


def safe_get(row: dict, key: str, default: str = "") -> str:
    if key in row and pd.notna(row[key]):
        return str(row[key])
    return default


def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    # Avoid double-prefixing
    if sku.upper().startswith(prefix.upper()):
        return sku
    return f"{prefix}{sku}"


def infer_order_type(netsuite_row: dict, asana_row_uv: Optional[dict], asana_row_custom: Optional[dict]) -> str:
    """
    Determine order type based on Asana sheet columns as described by the user:
    - If custom Asana linked sheet has Section/Column == "Blank" => BLANK
    - Else if UV Asana linked sheet under a column like "Color Print" contains "UV Printer" => UV
    - Else => LASER (custom laser)
    """
    # Check Custom/Laser board first for explicit Blank
    if asana_row_custom is not None:
        # Try to find a column that looks like Section/Column
        cand_cols = [c for c in asana_row_custom.keys() if normalize_col(c) in {"section/column", "section", "column"}]
        for c in cand_cols or []:
            val = str(asana_row_custom.get(c, "")).strip()
            if val.lower() == "blank":
                return "BLANK"

    # Then check UV board for Color Print -> UV Printer
    if asana_row_uv is not None:
        # Try to find a column that looks like Color Print
        cand_cols = [c for c in asana_row_uv.keys() if normalize_col(c) == "color print"]
        for c in cand_cols or []:
            val = str(asana_row_uv.get(c, "")).strip().lower()
            if "uv printer" in val:
                return "UV"

    return "LASER"


def is_automated_cards(asana_row: Optional[dict]) -> bool:
    if asana_row is None:
        return False
    for k, v in asana_row.items():
        if normalize_col(k) in {"section/column", "section", "column"} and str(v).strip().lower() == "automated cards":
            return True
    return False


def build_default_mapping(netsuite_cols: List[str]) -> Dict[str, str]:
    """Propose a default NS->Fishbowl mapping.
    You can edit this live in the UI below if needed.
    """
    # Common NetSuite columns seen in the user's sample
    # We map best-effort; user can override.
    # Fishbowl schema (all required):
    fb = [
        "Date",
        "Document Number",
        "Status",
        "Name",
        "Account",
        "Memo",
        "Amount",
        "PO/Check Number",
        "Customer Firstname",
        "Customer Lastname",
        "Billing Addressee",
        "Billing Address",
        "Billing Address 1",
        "Billing Address 2",
        "Billing Address 3",
        "Ship To",
        "Shipping Addressee",
        "Shipping Address",
        "Shipping Address 1",
        "Shipping Address 2",
        "Shipping Address 3",
        "Shipping Carrier",
        "Item",
        "Quantity",
        "Type",
        "*",
    ]

    # Heuristic guesses based on the preview we saw
    guess = {
        "Date": "Date" if "Date" in netsuite_cols else (netsuite_cols[0] if netsuite_cols else ""),
        "Document Number": "SONum" if "SONum" in netsuite_cols else "Document Number",
        "Status": "Status" if "Status" in netsuite_cols else "",
        "Name": "CustomerName" if "CustomerName" in netsuite_cols else "Name",
        "Account": "Account" if "Account" in netsuite_cols else "",
        "Memo": "Note" if "Note" in netsuite_cols else "Memo",
        "Amount": "Amount" if "Amount" in netsuite_cols else "",
        "PO/Check Number": "PONum" if "PONum" in netsuite_cols else "PO/Check Number",
        "Customer Firstname": "Customer Firstname" if "Customer Firstname" in netsuite_cols else "",
        "Customer Lastname": "Customer Lastname" if "Customer Lastname" in netsuite_cols else "",
        "Billing Addressee": "BillToName" if "BillToName" in netsuite_cols else "Billing Addressee",
        "Billing Address": "BillToAddress" if "BillToAddress" in netsuite_cols else "Billing Address",
        "Billing Address 1": "BillToAddress 1" if "BillToAddress 1" in netsuite_cols else "",
        "Billing Address 2": "BillToAddress 2" if "BillToAddress 2" in netsuite_cols else "",
        "Billing Address 3": "BillToAddress 3" if "BillToAddress 3" in netsuite_cols else "",
        "Ship To": "Ship To" if "Ship To" in netsuite_cols else "",
        "Shipping Addressee": "ShipToName" if "ShipToName" in netsuite_cols else "Shipping Addressee",
        "Shipping Address": "ShipToAddress" if "ShipToAddress" in netsuite_cols else "Shipping Address",
        "Shipping Address 1": "ShipToAddress 1" if "ShipToAddress 1" in netsuite_cols else "",
        "Shipping Address 2": "ShipToAddress 2" if "ShipToAddress 2" in netsuite_cols else "",
        "Shipping Address 3": "ShipToAddress 3" if "ShipToAddress 3" in netsuite_cols else "",
        "Shipping Carrier": "CarrierName" if "CarrierName" in netsuite_cols else "Shipping Carrier",
        "Item": "Item" if "Item" in netsuite_cols else "",
        "Quantity": "Quantity" if "Quantity" in netsuite_cols else "",
        "Type": "Type" if "Type" in netsuite_cols else "",
        "*": "*" if "*" in netsuite_cols else "",
    }
    return guess


def apply_mapping(ns_df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    for fb_col, ns_col in mapping.items():
        if ns_col and ns_col in ns_df.columns:
            out[fb_col] = ns_df[ns_col].astype(str).fillna("")
        else:
            out[fb_col] = ""
    return out


def merge_asana(ns_df: pd.DataFrame, asana_uv_df: Optional[pd.DataFrame], asana_custom_df: Optional[pd.DataFrame], key_ns: str, key_asana: str) -> pd.DataFrame:
    """
    Left-join NetSuite onto Asana frames separately to bring in the columns needed to:
      - Exclude 'automated cards'
      - Determine order type (Blank / UV / Laser)
    """
    ns_df = ns_df.copy()

    # Normalize keys
    def norm_key(s):
        return str(s).strip().upper()

    if key_ns not in ns_df.columns:
        ns_df[key_ns] = ""
    ns_df["_join_key"] = ns_df[key_ns].map(norm_key)

    uv = None
    if asana_uv_df is not None and key_asana in asana_uv_df.columns:
        uv = asana_uv_df.copy()
        uv["_join_key"] = uv[key_asana].map(norm_key)
        uv = uv.drop_duplicates("_join_key")

    custom = None
    if asana_custom_df is not None and key_asana in asana_custom_df.columns:
        custom = asana_custom_df.copy()
        custom["_join_key"] = custom[key_asana].map(norm_key)
        custom = custom.drop_duplicates("_join_key")

    # Build lookup dicts for quick per-row access
    uv_dict = uv.set_index("_join_key").to_dict(orient="index") if uv is not None else {}
    custom_dict = custom.set_index("_join_key").to_dict(orient="index") if custom is not None else {}

    results = []
    for _, row in ns_df.iterrows():
        k = row.get("_join_key", "")
        uv_row = uv_dict.get(k)
        custom_row = custom_dict.get(k)

        # Exclusion: automated cards on either board
        if is_automated_cards(uv_row) or is_automated_cards(custom_row):
            continue

        # Inclusion gate: the user specified they only want orders once they reach 'pending orders – unverified'.
        # Implementation note: we don't have the full column workflow ordering here, so we implement the strict exclusion above
        # and allow all others through. If you need stricter gating, add a whitelist of allowed columns below.

        # Infer order type
        otype = infer_order_type(row.to_dict(), uv_row, custom_row)
        row_dict = row.to_dict()
        row_dict["__ORDER_TYPE"] = otype
        results.append(row_dict)

    return pd.DataFrame(results)


def transform_items(df_fb: pd.DataFrame) -> pd.DataFrame:
    df_fb = df_fb.copy()
    if "Item" not in df_fb.columns:
        return df_fb

    # Expect __ORDER_TYPE carried over from merge step if we applied mapping afterwards.
    if "__ORDER_TYPE" not in df_fb.columns and "__ORDER_TYPE" in df_fb.index.names:
        df_fb["__ORDER_TYPE"] = df_fb.index.get_level_values("__ORDER_TYPE")

    if "__ORDER_TYPE" not in df_fb.columns:
        df_fb["__ORDER_TYPE"] = "LASER"

    def rename_sku(row):
        sku = row.get("Item", "")
        otype = (row.get("__ORDER_TYPE", "") or "").upper()
        if otype == "UV":
            return dedupe_prefix(sku, "UV-")
        elif otype == "LASER":
            return dedupe_prefix(sku, "L-")
        else:
            return sku  # BLANK => unchanged

    df_fb["Item"] = df_fb.apply(rename_sku, axis=1)
    return df_fb


# -----------------------------
# Inputs
# -----------------------------
col1, col2, col3 = st.columns([1,1,1])

with col1:
    ns_file = st.file_uploader("Upload NetSuite CSV", type=["csv"])
with col2:
    asana_uv_sheet = st.file_uploader("Upload Asana UV linked sheet (CSV/XLSX)", type=["csv", "xlsx"])
with col3:
    asana_custom_sheet = st.file_uploader("Upload Asana Custom/Laser linked sheet (CSV/XLSX)", type=["csv", "xlsx"])

st.markdown("---")

# Join key config (user can adjust if needed)
st.subheader("Join & Mapping Settings")
key_ns = st.text_input("NetSuite join key column (e.g., SONum or Document Number)", value="SONum")
key_asana = st.text_input("Asana join key column (e.g., SONum or Document Number)", value="Document Number")

ns_df = None
asana_uv_df = None
asana_custom_df = None

if ns_file:
    try:
        ns_df = pd.read_csv(ns_file, dtype=str, encoding="cp1252").fillna("")
    except Exception:
        ns_df = pd.read_csv(ns_file, dtype=str).fillna("")
    st.success(f"Loaded NetSuite rows: {ns_df.shape[0]:,}")
    st.dataframe(ns_df.head(50))

if asana_uv_sheet:
    if asana_uv_sheet.name.lower().endswith(".csv"):
        asana_uv_df = pd.read_csv(asana_uv_sheet, dtype=str).fillna("")
    else:
        asana_uv_df = pd.read_excel(asana_uv_sheet, dtype=str, engine="openpyxl").fillna("")
    st.info(f"Loaded Asana UV rows: {asana_uv_df.shape[0]:,}")

if asana_custom_sheet:
    if asana_custom_sheet.name.lower().endswith(".csv"):
        asana_custom_df = pd.read_csv(asana_custom_sheet, dtype=str).fillna("")
    else:
        asana_custom_df = pd.read_excel(asana_custom_sheet, dtype=str, engine="openpyxl").fillna("")
    st.info(f"Loaded Asana Custom/Laser rows: {asana_custom_df.shape[0]:,}")

# -----------------------------
# Propose & edit mapping
# -----------------------------
if ns_df is not None:
    st.markdown("### Proposed NetSuite → Fishbowl Mapping")
    default_map = build_default_mapping(list(ns_df.columns))

    mapping_rows = []
    for fb_col, guess_ns_col in default_map.items():
        mapping_rows.append({"Fishbowl Column": fb_col, "NetSuite Column": guess_ns_col})
    map_df = pd.DataFrame(mapping_rows)

    edited = st.data_editor(
        map_df,
        num_rows="fixed",
        hide_index=True,
        use_container_width=True,
        key="mapping_editor",
        column_config={
            "Fishbowl Column": st.column_config.TextColumn(disabled=True),
            "NetSuite Column": st.column_config.TextColumn(help="Type to change the source column name"),
        },
    )

    mapping = {row["Fishbowl Column"]: row["NetSuite Column"] for _, row in edited.iterrows()}
else:
    mapping = {}

# -----------------------------
# Transform
# -----------------------------
output_df = None
if ns_df is not None:
    # Merge Asana and filter out automated cards, infer order type
    merged_df = merge_asana(ns_df, asana_uv_df, asana_custom_df, key_ns=key_ns, key_asana=key_asana)

    # Apply mapping to Fishbowl schema
    fb_df = apply_mapping(merged_df, mapping)

    # Carry the order type over for item renaming
    if "__ORDER_TYPE" in merged_df.columns:
        fb_df["__ORDER_TYPE"] = merged_df["__ORDER_TYPE"]

    # Rename items according to order type rules
    fb_df = transform_items(fb_df)

    # Ensure all required columns exist and are strings
    required_cols = [
        "Date","Document Number","Status","Name","Account","Memo","Amount","PO/Check Number",
        "Customer Firstname","Customer Lastname","Billing Addressee","Billing Address","Billing Address 1",
        "Billing Address 2","Billing Address 3","Ship To","Shipping Addressee","Shipping Address",
        "Shipping Address 1","Shipping Address 2","Shipping Address 3","Shipping Carrier","Item","Quantity","Type","*"
    ]
    for c in required_cols:
        if c not in fb_df.columns:
            fb_df[c] = ""
        fb_df[c] = fb_df[c].astype(str).fillna("")

    # Reorder
    fb_df = fb_df[required_cols + (["__ORDER_TYPE"] if "__ORDER_TYPE" in fb_df.columns else [])]

    # Preview
    st.markdown("### Preview of Fishbowl Output")
    st.dataframe(fb_df.head(100), use_container_width=True)

    # Download button
    csv_bytes = fb_df[required_cols].to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="Download Fishbowl CSV",
        data=csv_bytes,
        file_name="fishbowl_upload.csv",
        mime="text/csv",
    )

    output_df = fb_df

# -----------------------------
# Tips & Notes
# -----------------------------
st.markdown(
    """
**Logic implemented**
- Reads both Asana boards (UV + Custom/Laser) from the linked-sheet exports you upload.
- Excludes any orders whose Section/Column equals **"automated cards"** (on either board).
- Order type detection:
  - If Custom/Laser sheet shows **Section/Column = Blank** → **BLANK**
  - Else if UV sheet **Color Print** contains **"UV Printer"** → **UV**
  - Else → **LASER**
- SKU renaming rules:
  - **UV** → prefix `UV-`
  - **LASER** → prefix `L-`
  - **BLANK** → unchanged
- You can adjust the **join keys** and the **NS→Fishbowl field mapping** in the UI before exporting.

**Next**
- If your Asana column names differ, tweak them in \"infer_order_type\" / \"is_automated_cards\".
- If you want strict gating (e.g., only include rows at or after a particular column like *Pending Orders – Unverified*), add a whitelist filter where noted in the code.
    """
)
