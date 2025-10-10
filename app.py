import re
import pandas as pd
import streamlit as st

# =========================
# App setup
# =========================
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana (live Sheets) into a Fishbowl-ready CSV with UV/Laser/Blank SKU logic.")

# =========================
# LIVE Google Sheets (CSV export URLs)
# =========================
# UV Asana board
UV_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1Bgw_knhlQcdO2D2LTfJy3XB9Mn5OcDErRrhMLsvgaYM/export?format=csv&gid=790696528"
# Custom/Laser Asana board
CUSTOM_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1vJztlcMoXhdZxJdcXYkvFHYqSpYHq4PILMYT0BS_8Hk/export?format=csv&gid=1818164620"

# =========================
# Final Fishbowl output columns (exact order)
# =========================
FISHBOWL_COLUMNS = [
    'SONum','Status','CustomerName','CustomerContact','BillToName','BillToAddress','BillToCity','BillToState','BillToZip','BillToCountry',
    'ShipToName','ShipToAddress','ShipToCity','ShipToState','ShipToZip','ShipToCountry','ShipToResidential','CarrierName','TaxRateName','PriorityId',
    'PONum','VendorPONum','Date','Salesman','ShippingTerms','PaymentTerms','FOB','Note','QuickBooksClassName','LocationGroupName','OrderDateScheduled',
    'URL','CarrierService','DateExpired','Phone','Email','Category','CF-Due Date','CF-Custom','SOItemTypeID','ProductNumber','ProductDescription',
    'ProductQuantity','UOM','ProductPrice','Taxable','TaxCode','ItemNote','ItemQuickBooksClassName','ItemDateScheduled','ShowItem','KitItem','RevisionLevel','CustomerPartNumber'
]

# =========================
# Helpers
# =========================
def normalize_col(s: str) -> str:
    return re.sub(r"\s+", "", str(s).strip().lower())

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    return sku if sku.upper().startswith(prefix.upper()) else f"{prefix}{sku}"

def extract_rhs_sku(item_value: str) -> str:
    """From 'PARENT : CHILD' return 'CHILD'; otherwise trimmed value."""
    val = str(item_value or "").strip()
    if ":" in val:
        return val.split(":", 1)[1].strip()
    return val

def get_cus_from_asana_name(name: str) -> str:
    """Extract CUS##### from Asana 'Name'."""
    m = re.search(r"CUS\d{3,}", str(name))
    return m.group(0).upper() if m else ""

def is_automated_cards(asana_row: dict | None) -> bool:
    if not asana_row:
        return False
    for k, v in asana_row.items():
        if normalize_col(k) in {"section", "section/column", "column"} and str(v).strip().lower() == "automated cards":
            return True
    return False

def infer_order_type(uv_row: dict | None, custom_row: dict | None) -> str:
    """
    Priority: UV > Custom Blank > Laser
    - If present in UV board with Color Print = 'UV Printer' -> UV (priority)
    - Else if present in Custom board with Section/Column = 'Blank' -> BLANK
    - Else -> LASER
    """
    # UV priority
    if uv_row:
        for k, v in uv_row.items():
            if normalize_col(k) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    # Custom Blank
    if custom_row:
        for k, v in custom_row.items():
            if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    # Default Laser
    return "LASER"

def read_ns(file):  # .csv, .xlsx, .xls
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str).fillna("")
    if name.endswith(".xlsx"):
        return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")
    # .xls (requires xlrd)
    return pd.read_excel(file, dtype=str, engine="xlrd").fillna("")

def fetch_asana_sheet(url: str) -> pd.DataFrame:
    # Read live CSV export from Google Sheets
    df = pd.read_csv(url, dtype=str).fillna("")
    return df

def build_asana_lookup_by_cus(df: pd.DataFrame | None) -> dict[str, dict]:
    """
    Dict keyed by CUS#, preferring rows NOT in 'Automated Cards'.
    Handles duplicates by keeping the first non-automated (or first if all automated).
    """
    if df is None or "Name" not in df.columns:
        return {}
    tmp = df.copy()
    tmp["_CUS"] = tmp["Name"].map(get_cus_from_asana_name)
    tmp = tmp[tmp["_CUS"] != ""]
    # Non-automated first
    def _auto_flag(row): return is_automated_cards(row.to_dict())
    tmp["_AUTO"] = tmp.apply(_auto_flag, axis=1)
    tmp = tmp.sort_values(by=["_AUTO"]).drop_duplicates(subset="_CUS", keep="first")
    return tmp.set_index("_CUS").to_dict(orient="index")

# =========================
# UI
# =========================
col1, col2 = st.columns([1,1])
with col1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with col2:
    st.markdown("**Asana sources:** loaded live from Google Sheets (no upload needed)")

if not ns_file:
    st.stop()

# =========================
# Read data
# =========================
# NetSuite
try:
    ns_df = read_ns(ns_file)
except Exception as e:
    st.error(f"Failed to read NetSuite file: {e}")
    st.stop()

st.success(f"Loaded NetSuite rows: {len(ns_df):,}")

# Asana (live)
try:
    uv_df = fetch_asana_sheet(UV_SHEET_CSV)
    st.info(f"Loaded UV Asana rows (live): {len(uv_df):,}")
except Exception as e:
    st.error(f"Failed to fetch UV Asana sheet: {e}")
    uv_df = None
try:
    custom_df = fetch_asana_sheet(CUSTOM_SHEET_CSV)
    st.info(f"Loaded Custom/Laser Asana rows (live): {len(custom_df):,}")
except Exception as e:
    st.error(f"Failed to fetch Custom Asana sheet: {e}")
    custom_df = None

uv_lookup = build_asana_lookup_by_cus(uv_df)
custom_lookup = build_asana_lookup_by_cus(custom_df)

# =========================
# Transform
# =========================
results = []
for _, row in ns_df.iterrows():
    cus = str(row.get("PO/Check Number", "")).strip().upper()
    if not cus:
        continue  # must have CUS# to match

    uv_row = uv_lookup.get(cus)
    custom_row = custom_lookup.get(cus)

    # Skip if not present in either board
    if uv_row is None and custom_row is None:
        continue
    # Skip automated cards on either board
    if is_automated_cards(uv_row) or is_automated_cards(custom_row):
        continue

    otype = infer_order_type(uv_row, custom_row)

    r = row.to_dict()

    # SKU source: NetSuite "Item" -> take RHS after ':'
    item_rhs = extract_rhs_sku(r.get("Item", ""))
    if otype == "UV":
        product_number = dedupe_prefix(item_rhs, "UV-")
    elif otype == "LASER":
        product_number = dedupe_prefix(item_rhs, "L-")
    else:  # BLANK
        product_number = item_rhs

    # write final field
    r["ProductNumber"] = product_number
    r["__OrderType"] = otype
    results.append(r)

out_df = pd.DataFrame(results)

# Ensure all required columns exist; reorder strictly to FISHBOWL_COLUMNS (+ optional __OrderType for preview)
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ""
display_cols = FISHBOWL_COLUMNS + (["__OrderType"] if "__OrderType" in out_df.columns else [])
out_df = out_df.reindex(columns=display_cols)

st.subheader("Preview")
if out_df.empty:
    st.warning("No rows matched after Asana filtering (either not found in Asana or in 'Automated Cards').")
st.dataframe(out_df.head(100), use_container_width=True)

# =========================
# Export
# =========================
csv_bytes = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_bytes, file_name="fishbowl_upload.csv", mime="text/csv")

