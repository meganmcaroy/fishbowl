import re
import pandas as pd
import streamlit as st

# -----------------------------
# App setup
# -----------------------------
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + live Asana sheets into a Fishbowl-ready CSV with UV/Laser/Blank SKU logic.")

# -----------------------------
# Live Google Sheet Links
# -----------------------------
UV_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1Bgw_knhlQcdO2D2LTfJy3XB9Mn5OcDErRrhMLsvgaYM/export?format=csv&gid=790696528"
CUSTOM_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1vJztlcMoXhdZxJdcXYkvFHYqSpYHq4PILMYT0BS_8Hk/export?format=csv&gid=1818164620"

# -----------------------------
# Fishbowl Columns (exact order)
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
    """Take the SKU after ':' in NetSuite Item."""
    val = str(item_value or "").strip()
    if ":" in val:
        return val.split(":", 1)[1].strip()
    return val

def get_cus_from_asana_name(name: str) -> str:
    """Extract CUS##### from Asana Name column."""
    m = re.search(r"CUS\\d{3,}", str(name))
    return m.group(0).upper() if m else ""

def is_automated_cards(asana_row: dict | None) -> bool:
    if not asana_row:
        return False
    for k, v in asana_row.items():
        if normalize_col(k) in {"section", "section/column", "column"} and str(v).strip().lower() == "automated cards":
            return True
    return False

def infer_order_type(uv_row: dict | None, custom_row: dict | None) -> str:
    """UV priority > Blank > Laser"""
    if uv_row:
        for k, v in uv_row.items():
            if normalize_col(k) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    if custom_row:
        for k, v in custom_row.items():
            if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    return "LASER"

def read_ns(file):
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str).fillna("")
    if name.endswith(".xlsx"):
        return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")
    return pd.read_excel(file, dtype=str, engine="xlrd").fillna("")

def fetch_asana(url: str):
    try:
        df = pd.read_csv(url, dtype=str).fillna("")
        return df
    except Exception as e:
        st.error(f"Could not fetch Asana sheet: {e}")
        return None

def build_asana_lookup(df: pd.DataFrame | None) -> dict[str, dict]:
    if df is None or "Name" not in df.columns:
        return {}
    tmp = df.copy()
    tmp["_CUS"] = tmp["Name"].map(get_cus_from_asana_name)
    tmp = tmp[tmp["_CUS"] != ""]
    tmp["_AUTO"] = tmp.apply(lambda r: is_automated_cards(r.to_dict()), axis=1)
    tmp = tmp.sort_values(by="_AUTO").drop_duplicates(subset="_CUS", keep="first")
    return tmp.set_index("_CUS").to_dict(orient="index")

# -----------------------------
# UI
# -----------------------------
col1, col2 = st.columns([1,1])
with col1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with col2:
    st.markdown("**Asana sheets:** pulled automatically from live Google Sheets")

if not ns_file:
    st.stop()

# -----------------------------
# Load data
# -----------------------------
ns_df = read_ns(ns_file)
uv_df = fetch_asana(UV_SHEET_CSV)
custom_df = fetch_asana(CUSTOM_SHEET_CSV)

st.success(f"NetSuite rows loaded: {len(ns_df):,}")
if uv_df is not None: st.info(f"UV Asana rows loaded: {len(uv_df):,}")
if custom_df is not None: st.info(f"Custom/Laser Asana rows loaded: {len(custom_df):,}")

uv_lookup = build_asana_lookup(uv_df)
custom_lookup = build_asana_lookup(custom_df)

# -----------------------------
# Process & transform
# -----------------------------
results = []
for _, row in ns_df.iterrows():
    cus = str(row.get("PO/Check Number", "")).strip().upper()
    if not cus:
        continue

    uv_row = uv_lookup.get(cus)
    custom_row = custom_lookup.get(cus)

    if uv_row is None and custom_row is None:
        continue
    if is_automated_cards(uv_row) or is_automated_cards(custom_row):
        continue

    order_type = infer_order_type(uv_row, custom_row)
    r = row.to_dict()

    # SKU logic
    sku = extract_rhs_sku(r.get("Item", ""))
    if order_type == "UV":
        sku = dedupe_prefix(sku, "UV-")
    elif order_type == "LASER":
        sku = dedupe_prefix(sku, "L-")
    r["ProductNumber"] = sku
    r["__OrderType"] = order_type
    results.append(r)

out_df = pd.DataFrame(results)

# Preserve all NetSuite fields and reorder to Fishbowl columns
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ""
out_df = out_df.reindex(columns=FISHBOWL_COLUMNS + (["__OrderType"] if "__OrderType" in out_df.columns else []))

# -----------------------------
# Output
# -----------------------------
st.subheader("Preview (first 100 rows)")
if out_df.empty:
    st.warning("No rows matched after Asana filtering (not found or in 'Automated Cards').")
else:
    st.dataframe(out_df.head(100), use_container_width=True)

csv_bytes = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_bytes, file_name="fishbowl_upload.csv", mime="text/csv")


