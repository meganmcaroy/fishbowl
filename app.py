import re
import pandas as pd
import streamlit as st

# -----------------------------
# App setup
# -----------------------------
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana into a Fishbowl-ready CSV. Keeps all NetSuite data and applies UV/Laser/Blank SKU logic.")

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
# Helper functions
# -----------------------------
def normalize_col(s: str) -> str:
    return re.sub(r"\s+", "", str(s).strip().lower())

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    return sku if sku.upper().startswith(prefix.upper()) else f"{prefix}{sku}"

def extract_rhs_sku(item_value: str) -> str:
    val = str(item_value or "").strip()
    if ":" in val:
        return val.split(":", 1)[1].strip()
    return val

def get_cus_from_asana_name(name: str) -> str:
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
    if uv_row:
        for k, v in uv_row.items():
            if normalize_col(k) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    if custom_row:
        for k, v in custom_row.items():
            if normalize_col(k) in {"section","section/column","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    return "LASER"

# ✅ FIXED: NetSuite file reader (auto-detects separator)
def read_ns(file):
    name = file.name.lower()
    try:
        # Try normal comma CSV first
        df = pd.read_csv(file, dtype=str).fillna("")
        # If only one column (bad parse), retry with tab delimiter
        if df.shape[1] == 1:
            file.seek(0)
            df = pd.read_csv(file, dtype=str, sep="\t").fillna("")
        return df
    except Exception:
        # Try Excel formats
        if name.endswith(".xlsx"):
            return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")
        return pd.read_excel(file, dtype=str, engine="xlrd").fillna("")

def fetch_asana(url: str):
    try:
        return pd.read_csv(url, dtype=str).fillna("")
    except Exception as e:
        st.error(f"Could not fetch Asana sheet: {e}")
        return None

# -----------------------------
# Load UI
# -----------------------------
col1, col2 = st.columns([1,1])
with col1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with col2:
    st.markdown("**Asana sheets:** automatically fetched from live Google Sheets")

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

# -----------------------------
# Prepare Asana lists
# -----------------------------
def get_valid_asana_orders(df: pd.DataFrame):
    if df is None or "Name" not in df.columns:
        return pd.DataFrame()
    tmp = df.copy()
    tmp["_CUS"] = tmp["Name"].map(get_cus_from_asana_name)
    tmp = tmp[tmp["_CUS"] != ""]
    tmp["_AUTO"] = tmp.apply(lambda r: is_automated_cards(r.to_dict()), axis=1)
    return tmp[~tmp["_AUTO"]]  # exclude automated cards

uv_valid = get_valid_asana_orders(uv_df)
custom_valid = get_valid_asana_orders(custom_df)
asana_combined = pd.concat([uv_valid.assign(_SRC="UV"), custom_valid.assign(_SRC="CUSTOM")])
asana_combined = asana_combined.drop_duplicates(subset="_CUS", keep="first")

# -----------------------------
# Merge with NetSuite
# -----------------------------
ns_df["PO/Check Number"] = ns_df["PO/Check Number"].astype(str).str.strip().str.upper()
matched_orders = ns_df[ns_df["PO/Check Number"].isin(asana_combined["_CUS"])].copy()

results = []
for _, row in matched_orders.iterrows():
    cus = row["PO/Check Number"]
    uv_row = uv_valid[uv_valid["_CUS"] == cus].to_dict("records")
    custom_row = custom_valid[custom_valid["_CUS"] == cus].to_dict("records")

    uv_row = uv_row[0] if uv_row else None
    custom_row = custom_row[0] if custom_row else None

    order_type = infer_order_type(uv_row, custom_row)
    r = row.to_dict()
    sku = extract_rhs_sku(r.get("Item", ""))
    if order_type == "UV":
        sku = dedupe_prefix(sku, "UV-")
    elif order_type == "LASER":
        sku = dedupe_prefix(sku, "L-")
    r["ProductNumber"] = sku
    r["__OrderType"] = order_type
    results.append(r)

out_df = pd.DataFrame(results)

# Preserve all NetSuite data, reorder to Fishbowl columns
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ""
out_df = out_df.reindex(columns=FISHBOWL_COLUMNS + (["__OrderType"] if "__OrderType" in out_df.columns else []))

# -----------------------------
# Output
# -----------------------------
st.subheader("Preview (first 100 rows)")
if out_df.empty:
    st.warning("No rows matched after filtering — check Asana CUS# vs NetSuite PO/Check Number.")
else:
    st.dataframe(out_df.head(100), use_container_width=True)

csv_bytes = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_bytes, file_name="fishbowl_upload.csv", mime="text/csv")



