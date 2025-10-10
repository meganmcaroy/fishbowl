import re
import pandas as pd
import streamlit as st
from difflib import get_close_matches

# =========================================
# App setup
# =========================================
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana data into a Fishbowl-ready CSV. Now with automatic header detection and fuzzy mapping for NetSuite columns.")

# =========================================
# Live Google Sheet Links
# =========================================
UV_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1Bgw_knhlQcdO2D2LTfJy3XB9Mn5OcDErRrhMLsvgaYM/export?format=csv&gid=790696528"
CUSTOM_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1vJztlcMoXhdZxJdcXYkvFHYqSpYHq4PILMYT0BS_8Hk/export?format=csv&gid=1818164620"

# =========================================
# Fishbowl Columns
# =========================================
FISHBOWL_COLUMNS = [
    'SONum','Status','CustomerName','CustomerContact','BillToName','BillToAddress','BillToCity','BillToState','BillToZip','BillToCountry',
    'ShipToName','ShipToAddress','ShipToCity','ShipToState','ShipToZip','ShipToCountry','ShipToResidential','CarrierName','TaxRateName','PriorityId',
    'PONum','VendorPONum','Date','Salesman','ShippingTerms','PaymentTerms','FOB','Note','QuickBooksClassName','LocationGroupName','OrderDateScheduled',
    'URL','CarrierService','DateExpired','Phone','Email','Category','CF-Due Date','CF-Custom','SOItemTypeID','ProductNumber','ProductDescription',
    'ProductQuantity','UOM','ProductPrice','Taxable','TaxCode','ItemNote','ItemQuickBooksClassName','ItemDateScheduled','ShowItem','KitItem','RevisionLevel','CustomerPartNumber'
]

# =========================================
# Helper functions
# =========================================
CUS_RE = re.compile(r"CUS\d{3,}", re.IGNORECASE)

def normalize_key(s: str) -> str:
    return str(s or "").strip().upper()

def get_cus_from_asana_name(name: str) -> str:
    m = CUS_RE.search(str(name))
    return normalize_key(m.group(0)) if m else ""

def is_automated_cards(row_dict: dict | None) -> bool:
    if not row_dict:
        return False
    for k, v in row_dict.items():
        k_norm = re.sub(r"\s+|[/_]", "", str(k).strip().lower())
        if k_norm in {"section","sectioncolumn","column"}:
            return str(v).strip().lower() == "automated cards"
    return False

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    return sku if sku.upper().startswith(prefix.upper()) else f"{prefix}{sku}"

def extract_rhs_sku(item_value: str) -> str:
    val = str(item_value or "").strip()
    return val.split(":", 1)[1].strip() if ":" in val else val

def read_ns(file):
    try:
        df = pd.read_csv(file, dtype=str).fillna("")
        if df.shape[1] == 1:
            file.seek(0)
            df = pd.read_csv(file, dtype=str, sep="\t").fillna("")
        return df
    except Exception:
        return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")

def fetch_asana(url):
    try:
        return pd.read_csv(url, dtype=str).fillna("")
    except Exception as e:
        st.error(f"Could not fetch Asana sheet: {e}")
        return pd.DataFrame()

# =========================================
# Column Mapping (flexible fuzzy version)
# =========================================
EXPECTED_KEYS = {
    "document number": "SONum",
    "status": "Status",
    "po/check number": "PONum",
    "customer": "CustomerName",
    "customer name": "CustomerName",
    "bill to": "BillToName",
    "bill to address": "BillToAddress",
    "bill to city": "BillToCity",
    "bill to state": "BillToState",
    "bill to zip": "BillToZip",
    "bill to country": "BillToCountry",
    "ship to": "ShipToName",
    "ship to address": "ShipToAddress",
    "ship to city": "ShipToCity",
    "ship to state": "ShipToState",
    "ship to zip": "ShipToZip",
    "ship to country": "ShipToCountry",
    "sales rep": "Salesman",
    "order date": "Date",
    "item": "ProductDescription",
    "quantity": "ProductQuantity",
    "price": "ProductPrice",
    "phone": "Phone",
    "email": "Email"
}

# =========================================
# Fuzzy column matcher
# =========================================
def fuzzy_map_columns(ns_cols):
    mapped = {}
    lower_ns = [c.lower().strip() for c in ns_cols]
    for expected, target in EXPECTED_KEYS.items():
        matches = get_close_matches(expected, lower_ns, n=1, cutoff=0.6)
        if matches:
            found = ns_cols[lower_ns.index(matches[0])]
            mapped[found] = target
    return mapped

# =========================================
# UI
# =========================================
col1, col2 = st.columns([1,1])
with col1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with col2:
    st.markdown("**Asana sheets** are pulled live (UV + Custom).")

if not ns_file:
    st.stop()

# =========================================
# Load data
# =========================================
ns_df = read_ns(ns_file)
ns_df.columns = [str(c).strip() for c in ns_df.columns]
uv_df = fetch_asana(UV_SHEET_CSV)
custom_df = fetch_asana(CUSTOM_SHEET_CSV)

# =========================================
# Prepare Asana data
# =========================================
def prep_asana(df, source):
    if df.empty or "Name" not in df.columns:
        return pd.DataFrame()
    df["_CUS"] = df["Name"].map(get_cus_from_asana_name)
    df = df[df["_CUS"] != ""]
    df["_AUTO"] = df.apply(lambda r: is_automated_cards(r.to_dict()), axis=1)
    df = df[~df["_AUTO"]]
    df["_SRC"] = source
    return df

uv_valid = prep_asana(uv_df, "UV")
custom_valid = prep_asana(custom_df, "CUSTOM")
asana_all = pd.concat([uv_valid, custom_valid], ignore_index=True)
asana_all = asana_all.sort_values(by=["_SRC"], key=lambda s: s.map({"UV":0,"CUSTOM":1})).drop_duplicates(subset="_CUS", keep="first")

# =========================================
# Match by PO/Check Number (NetSuite) → Asana Name
# =========================================
if "PO/Check Number" not in ns_df.columns:
    st.error("Missing 'PO/Check Number' column in NetSuite file.")
    st.stop()

ns_df["_CUS_KEY"] = ns_df["PO/Check Number"].apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", str(x)).upper())
asana_all["_CUS_KEY"] = asana_all["_CUS"].apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", str(x)).upper())
matched = ns_df.merge(asana_all[["_CUS_KEY", "_SRC"]], on="_CUS_KEY", how="inner")

if matched.empty:
    st.warning("No NetSuite orders matched Asana CUS numbers. Ensure 'PO/Check Number' matches '#CUS#####' in Asana 'Name'.")
    st.stop()

# =========================================
# Apply SKU Prefix Logic
# =========================================
def determine_sku(row):
    rhs = extract_rhs_sku(row.get("Item", ""))
    src = row.get("_SRC", "")
    if src == "UV":
        return dedupe_prefix(rhs, "UV-")
    elif src == "CUSTOM":
        return dedupe_prefix(rhs, "L-")
    return rhs

matched["ProductNumber"] = matched.apply(determine_sku, axis=1)

# =========================================
# Map columns (fuzzy-matched)
# =========================================
map_dict = fuzzy_map_columns(matched.columns)
out_df = pd.DataFrame()

for src_col, fb_col in map_dict.items():
    out_df[fb_col] = matched[src_col]

for col in FISHBOWL_COLUMNS:
    if col not in out_df.columns:
        out_df[col] = ""

out_df["ProductNumber"] = matched["ProductNumber"]
out_df = out_df[FISHBOWL_COLUMNS]

# =========================================
# Preview + Download
# =========================================
st.subheader("Preview (first 100 rows)")
st.dataframe(out_df.head(100), use_container_width=True)

csv_data = out_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_data, file_name="fishbowl_upload.csv", mime="text/csv")

# Debug view
st.markdown("### 🔍 Column mapping used:")
st.json(map_dict)

