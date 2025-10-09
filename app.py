import io
import re
import pandas as pd
import streamlit as st

# -------------------------------------------------------
# App setup
# -------------------------------------------------------
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana data into a Fishbowl-ready CSV with UV/Laser/Blank SKU logic.")

# -------------------------------------------------------
# Config: required Fishbowl columns
# -------------------------------------------------------
FISHBOWL_COLUMNS = [
    'SONum','Status','CustomerName','CustomerContact','BillToName','BillToAddress','BillToCity','BillToState',
    'BillToZip','BillToCountry','ShipToName','ShipToAddress','ShipToCity','ShipToState','ShipToZip','ShipToCountry',
    'ShipToResidential','CarrierName','TaxRateName','PriorityId','PONum','VendorPONum','Date','Salesman',
    'ShippingTerms','PaymentTerms','FOB','Note','QuickBooksClassName','LocationGroupName','OrderDateScheduled',
    'URL','CarrierService','DateExpired','Phone','Email','Category','CF-Due Date','CF-Custom','SOItemTypeID',
    'ProductNumber','ProductDescription','ProductQuantity','UOM','ProductPrice','Taxable','TaxCode','ItemNote',
    'ItemQuickBooksClassName','ItemDateScheduled','ShowItem','KitItem','RevisionLevel','CustomerPartNumber'
]

# -------------------------------------------------------
# Helper functions
# -------------------------------------------------------
def normalize_col(s: str) -> str:
    return re.sub(r"\\s+", "", s.strip().lower())

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    if sku.upper().startswith(prefix.upper()):
        return sku
    return f"{prefix}{sku}"

def extract_right_sku(item_value: str) -> str:
    """Return the SKU after ':' if present"""
    if not isinstance(item_value, str):
        return ""
    if ":" in item_value:
        return item_value.split(":", 1)[1].strip()
    return item_value.strip()

def get_cus_from_name(name: str) -> str:
    """Extract CUS##### from Asana Name"""
    if not isinstance(name, str):
        return ""
    match = re.search(r"CUS\\d{3,}", name)
    return match.group(0).strip().upper() if match else ""

def is_automated_cards(asana_row: dict) -> bool:
    if asana_row is None:
        return False
    for k,v in asana_row.items():
        if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "automated cards":
            return True
    return False

def infer_order_type(asana_uv_row, asana_custom_row):
    if asana_custom_row is not None:
        for k,v in asana_custom_row.items():
            if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    if asana_uv_row is not None:
        for k,v in asana_uv_row.items():
            if normalize_col(k) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    return "LASER"

# -------------------------------------------------------
# File upload UI
# -------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with col2:
    asana_uv_file = st.file_uploader("Upload Asana UV linked sheet (.csv)", type=["csv"])
with col3:
    asana_custom_file = st.file_uploader("Upload Asana Custom/Laser linked sheet (.csv)", type=["csv"])

if not ns_file:
    st.stop()

# -------------------------------------------------------
# Read files
# -------------------------------------------------------
def read_any(file):
    if not file:
        return None
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str).fillna("")
    else:
        return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")

ns_df = read_any(ns_file)
uv_df = read_any(asana_uv_file)
custom_df = read_any(asana_custom_file)

st.success(f"Loaded NetSuite rows: {len(ns_df):,}")
if uv_df is not None:
    st.info(f"Loaded UV Asana rows: {len(uv_df):,}")
if custom_df is not None:
    st.info(f"Loaded Custom/Laser Asana rows: {len(custom_df):,}")

# -------------------------------------------------------
# Build Asana lookup dicts by extracted CUS#
# -------------------------------------------------------
def build_asana_lookup(df):
    if df is None or "Name" not in df.columns:
        return {}
    df["_CUS"] = df["Name"].map(get_cus_from_name)
    df = df[df["_CUS"] != ""]
    return df.set_index("_CUS").to_dict(orient="index")

uv_lookup = build_asana_lookup(uv_df)
custom_lookup = build_asana_lookup(custom_df)

# -------------------------------------------------------
# Merge and transform
# -------------------------------------------------------
results = []
for _, row in ns_df.iterrows():
    cus = str(row.get("PO/Check Number", "")).strip().upper()
    if not cus:
        continue

    uv_row = uv_lookup.get(cus)
    custom_row = custom_lookup.get(cus)

    # Skip if not in either Asana board
    if uv_row is None and custom_row is None:
        continue

    # Skip if automated cards
    if is_automated_cards(uv_row) or is_automated_cards(custom_row):
        continue

    # Determine order type
    otype = infer_order_type(uv_row, custom_row)

    # Handle SKU prefixing
    item_val = extract_right_sku(row.get("Item", ""))
    if otype == "UV":
        item_val = dedupe_prefix(item_val, "UV-")
    elif otype == "LASER":
        item_val = dedupe_prefix(item_val, "L-")
    # Blank = unchanged

    row_dict = row.to_dict()
    row_dict["Item"] = item_val
    row_dict["__OrderType"] = otype
    results.append(row_dict)

out_df = pd.DataFrame(results)

# -------------------------------------------------------
# Prepare Fishbowl output
# -------------------------------------------------------
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ""

# Ensure all columns exist and reorder
out_df = out_df[FISHBOWL_COLUMNS + ["__OrderType"]]

st.subheader("Preview Fishbowl Upload Data")
st.dataframe(out_df.head(100), use_container_width=True)

# -------------------------------------------------------
# Export
# -------------------------------------------------------
csv_data = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "Download Fishbowl CSV",
    data=csv_data,
    file_name="fishbowl_upload.csv",
    mime="text/csv",
)

st.markdown(
    """
### How this works
- Joins NetSuite **PO/Check Number** to the **CUS#** found in Asana “Name”.
- Skips any orders in **Automated Cards**.
- Uses Asana columns to decide type:
  - Custom/Laser sheet Section/Column = “Blank” → Blank
  - UV sheet Color Print = “UV Printer” → UV
  - Otherwise → Laser
- SKU logic:
  - Blank → unchanged
  - UV → add `UV-`
  - Laser → add `L-`
"""
)
