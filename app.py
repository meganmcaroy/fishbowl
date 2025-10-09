import io
import pandas as pd
import streamlit as st

# -----------------------------
# App Config
# -----------------------------
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transform NetSuite + Asana data into a Fishbowl-ready CSV with UV/Laser/Blank SKU logic.")

# Required Fishbowl columns (user-specified)
FISHBOWL_COLUMNS = [
    'SONum','Status','CustomerName','CustomerContact','BillToName','BillToAddress','BillToCity','BillToState','BillToZip','BillToCountry',
    'ShipToName','ShipToAddress','ShipToCity','ShipToState','ShipToZip','ShipToCountry','ShipToResidential','CarrierName','TaxRateName','PriorityId',
    'PONum','VendorPONum','Date','Salesman','ShippingTerms','PaymentTerms','FOB','Note','QuickBooksClassName','LocationGroupName','OrderDateScheduled',
    'URL','CarrierService','DateExpired','Phone','Email','Category','CF-Due Date','CF-Custom','SOItemTypeID','ProductNumber','ProductDescription',
    'ProductQuantity','UOM','ProductPrice','Taxable','TaxCode','ItemNote','ItemQuickBooksClassName','ItemDateScheduled','ShowItem','KitItem','RevisionLevel','CustomerPartNumber'
]

# -----------------------------
# Helper functions
# -----------------------------
def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    if sku.upper().startswith(prefix.upper()):
        return sku
    return f"{prefix}{sku}"

def normalize_col(s: str) -> str:
    return s.strip().lower().replace(' ', '')

def is_automated_cards(asana_row):
    if asana_row is None:
        return False
    for k,v in asana_row.items():
        if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "automated cards":
            return True
    return False

def infer_order_type(asana_uv, asana_custom):
    # Check custom first
    if asana_custom is not None:
        for k,v in asana_custom.items():
            if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    if asana_uv is not None:
        for k,v in asana_uv.items():
            if normalize_col(k) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    return "LASER"

# -----------------------------
# UI Uploads
# -----------------------------
col1,col2,col3 = st.columns(3)
with col1:
    ns_file = st.file_uploader("Upload NetSuite CSV", type=['csv'])
with col2:
    uv_file = st.file_uploader("Upload Asana UV linked sheet (CSV/XLSX)", type=['csv','xlsx'])
with col3:
    custom_file = st.file_uploader("Upload Asana Custom/Laser linked sheet (CSV/XLSX)", type=['csv','xlsx'])

if not ns_file:
    st.stop()

# Read NetSuite
try:
    ns_df = pd.read_csv(ns_file, dtype=str, encoding='cp1252').fillna('')
except Exception:
    ns_df = pd.read_csv(ns_file, dtype=str).fillna('')

# Read Asana sheets if provided
def read_any(f):
    if not f:
        return None
    if f.name.lower().endswith('.csv'):
        return pd.read_csv(f, dtype=str).fillna('')
    return pd.read_excel(f, dtype=str, engine='openpyxl').fillna('')

uv_df = read_any(uv_file)
custom_df = read_any(custom_file)

# Index Asana by SONum or Document Number
def build_lookup(df):
    if df is None:
        return {}
    key_cols = [c for c in df.columns if normalize_col(c) in {"sonum","documentnumber"}]
    if not key_cols:
        return {}
    k = key_cols[0]
    return {str(r[k]).strip().upper(): r.to_dict() for _,r in df.iterrows()}

uv_lookup = build_lookup(uv_df)
custom_lookup = build_lookup(custom_df)

# Merge logic
rows = []
for _,row in ns_df.iterrows():
    key = str(row.get('SONum') or row.get('Document Number') or '').strip().upper()
    uv_row = uv_lookup.get(key)
    custom_row = custom_lookup.get(key)

    if is_automated_cards(uv_row) or is_automated_cards(custom_row):
        continue

    order_type = infer_order_type(uv_row, custom_row)
    r = row.to_dict()
    # SKU prefixing
    itemcol = None
    for c in r.keys():
        if normalize_col(c) in {"item","productnumber","sku"}:
            itemcol = c
            break
    if itemcol:
        sku = r[itemcol]
        if order_type == 'UV':
            r[itemcol] = dedupe_prefix(sku,'UV-')
        elif order_type == 'LASER':
            r[itemcol] = dedupe_prefix(sku,'L-')

    r['__OrderType'] = order_type
    rows.append(r)

out_df = pd.DataFrame(rows)

# Reorder and fill missing columns
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ''

out_df = out_df[FISHBOWL_COLUMNS + ['__OrderType']]

st.subheader("Preview Fishbowl Upload Data")
st.dataframe(out_df.head(100), use_container_width=True)

csv_data = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode('utf-8-sig')
st.download_button("Download Fishbowl CSV", data=csv_data, file_name='fishbowl_upload.csv', mime='text/csv')

