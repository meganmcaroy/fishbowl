import re
import pandas as pd
import streamlit as st

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transform NetSuite + Asana data into a Fishbowl-ready CSV with correct UV/Laser/Blank SKU logic and Asana filtering.")

# -----------------------------
# Required Fishbowl columns (final output order)
# -----------------------------
FISHBOWL_COLUMNS = [
    'SONum','Status','CustomerName','CustomerContact','BillToName','BillToAddress','BillToCity','BillToState','BillToZip','BillToCountry',
    'ShipToName','ShipToAddress','ShipToCity','ShipToState','ShipToZip','ShipToCountry','ShipToResidential','CarrierName','TaxRateName','PriorityId',
    'PONum','VendorPONum','Date','Salesman','ShippingTerms','PaymentTerms','FOB','Note','QuickBooksClassName','LocationGroupName','OrderDateScheduled',
    'URL','CarrierService','DateExpired','Phone','Email','Category','CF-Due Date','CF-Custom','SOItemTypeID','ProductNumber','ProductDescription',
    'ProductQuantity','UOM','ProductPrice','Taxable','TaxCode','ItemNote','ItemQuickBooksClassName','ItemDateScheduled','ShowItem','KitItem','RevisionLevel','CustomerPartNumber'
]

# -----------------------------
# Helpers
# -----------------------------
def normalize_col(s: str) -> str:
    return re.sub(r"\s+", "", str(s).strip().lower())

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    return sku if sku.upper().startswith(prefix.upper()) else f"{prefix}{sku}"

def extract_rhs_sku(item_value: str) -> str:
    """From 'PARENT : CHILD' return 'CHILD'; otherwise return trimmed value."""
    val = str(item_value or "").strip()
    if ":" in val:
        return val.split(":", 1)[1].strip()
    return val

def get_cus_from_asana_name(name: str) -> str:
    """Extract CUS##### from Asana 'Name' field."""
    m = re.search(r"CUS\d{3,}", str(name))
    return m.group(0).upper() if m else ""

def is_automated_cards(asana_row: dict | None) -> bool:
    if not asana_row:
        return False
    for k, v in asana_row.items():
        if normalize_col(k) in {"section", "section/column", "column"} and str(v).strip().lower() == "automated cards":
            return True
    return False

def infer_order_type(asana_uv_row: dict | None, asana_custom_row: dict | None) -> str:
    # Blank from Custom/Laser board
    if asana_custom_row:
        for k, v in asana_custom_row.items():
            if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    # UV from UV board
    if asana_uv_row:
        for k, v in asana_uv_row.items():
            if normalize_col(k) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    # Otherwise Laser
    return "LASER"

def read_any(file):
    if not file:
        return None
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str).fillna("")
    return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")

# -----------------------------
# UI
# -----------------------------
c1, c2, c3 = st.columns(3)
with c1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with c2:
    asana_uv_file = st.file_uploader("Upload Asana UV linked sheet (.csv)", type=["csv"])
with c3:
    asana_custom_file = st.file_uploader("Upload Asana Custom/Laser linked sheet (.csv)", type=["csv"])

if not ns_file:
    st.stop()

# -----------------------------
# Read files
# -----------------------------
ns_df = read_any(ns_file)
uv_df = read_any(asana_uv_file)
custom_df = read_any(asana_custom_file)

st.success(f"Loaded NetSuite rows: {len(ns_df):,}")
if uv_df is not None: st.info(f"Loaded UV Asana rows: {len(uv_df):,}")
if custom_df is not None: st.info(f"Loaded Custom/Laser Asana rows: {len(custom_df):,}")

# -----------------------------
# Build Asana lookups by CUS# (dedupe-safe)
# -----------------------------
def build_asana_lookup_by_cus(df: pd.DataFrame | None) -> dict[str, dict]:
    """
    Create a dict keyed by CUS#, preferring rows that are NOT in 'Automated Cards'.
    Handles duplicate CUS# by keeping the first non-automated row (or the first row if all automated).
    """
    if df is None or "Name" not in df.columns:
        return {}
    tmp = df.copy()
    tmp["_CUS"] = tmp["Name"].map(get_cus_from_asana_name)
    tmp = tmp[tmp["_CUS"] != ""]
    # Flag automated rows so we can sort them to the bottom
    def _auto_flag(row):
        return is_automated_cards(row.to_dict())
    tmp["_AUTO"] = tmp.apply(_auto_flag, axis=1)
    # Non-automated first, then drop duplicates by CUS, keeping the first
    tmp = tmp.sort_values(by=["_AUTO"]).drop_duplicates(subset="_CUS", keep="first")
    return tmp.set_index("_CUS").to_dict(orient="index")

uv_lookup = build_asana_lookup_by_cus(uv_df)
custom_lookup = build_asana_lookup_by_cus(custom_df)

# -----------------------------
# Transform rows
# -----------------------------
results = []
for _, row in ns_df.iterrows():
    cus = str(row.get("PO/Check Number", "")).strip().upper()
    if not cus:
        continue  # no CUS#, can't match

    uv_row = uv_lookup.get(cus)
    custom_row = custom_lookup.get(cus)

    # Skip orders not found in either Asana sheet
    if uv_row is None and custom_row is None:
        continue

    # Skip if in "Automated Cards" on either board
    if is_automated_cards(uv_row) or is_automated_cards(custom_row):
        continue

    # Determine order type (Blank / UV / Laser)
    otype = infer_order_type(uv_row, custom_row)

    r = row.to_dict()

    # ---- SKU handling ----
    # Source: NetSuite "Item" (take RHS after ':')
    item_value = extract_rhs_sku(r.get("Item", ""))
    if otype == "UV":
        product_number = dedupe_prefix(item_value, "UV-")
    elif otype == "LASER":
        product_number = dedupe_prefix(item_value, "L-")
    else:
        product_number = item_value  # BLANK => unchanged

    # Write to final Fishbowl field
    r["ProductNumber"] = product_number
    r["__OrderType"] = otype
    results.append(r)

out_df = pd.DataFrame(results)

# -----------------------------
# Prepare Fishbowl output
# -----------------------------
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ""

display_cols = FISHBOWL_COLUMNS + (["__OrderType"] if "__OrderType" in out_df.columns else [])
out_df = out_df.reindex(columns=display_cols)

st.subheader("Preview Fishbowl Upload Data")
if out_df.empty:
    st.warning("No rows matched after Asana filtering (not in either Asana sheet, or in 'Automated Cards').")
st.dataframe(out_df.head(100), use_container_width=True)

# -----------------------------
# Export
# -----------------------------
csv_data = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_data, file_name="fishbowl_upload.csv", mime="text/csv")

