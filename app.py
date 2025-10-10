import re
import pandas as pd
import streamlit as st

# =========================================
# App setup
# =========================================
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana into a Fishbowl-ready CSV. Uses 'Document Number' as SO number, filters Asana (excludes Automated Cards), and applies UV/Laser/Blank SKU logic.")

# =========================================
# Live Google Sheet Links (CSV export)
# =========================================
UV_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1Bgw_knhlQcdO2D2LTfJy3XB9Mn5OcDErRrhMLsvgaYM/export?format=csv&gid=790696528"
CUSTOM_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1vJztlcMoXhdZxJdcXYkvFHYqSpYHq4PILMYT0BS_8Hk/export?format=csv&gid=1818164620"

# =========================================
# Final Fishbowl columns (exact order)
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

def infer_order_type(uv_row: dict | None, custom_row: dict | None) -> str:
    if uv_row:
        for k, v in uv_row.items():
            if re.sub(r"\s+", "", str(k).strip().lower()) == "colorprint":
                if "uv printer" in str(v).strip().lower():
                    return "UV"
    if custom_row:
        for k, v in custom_row.items():
            k_norm = re.sub(r"\s+|[/_]", "", str(k).strip().lower())
            if k_norm in {"section","sectioncolumn","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    return "LASER"

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    return sku if sku.upper().startswith(prefix.upper()) else f"{prefix}{sku}"

def extract_rhs_sku(item_value: str) -> str:
    val = str(item_value or "").strip()
    return val.split(":", 1)[1].strip() if ":" in val else val

def read_ns(file) -> pd.DataFrame:
    name = file.name.lower()
    try:
        df = pd.read_csv(file, dtype=str).fillna("")
        if df.shape[1] == 1:
            file.seek(0)
            df = pd.read_csv(file, dtype=str, sep="\t").fillna("")
        if df.shape[1] == 1:
            file.seek(0)
            df = pd.read_csv(file, dtype=str, sep="\t", encoding="cp1252").fillna("")
        return df
    except Exception:
        if name.endswith(".xlsx"):
            return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")
        return pd.read_excel(file, dtype=str, engine="xlrd").fillna("")

def fetch_asana(url: str) -> pd.DataFrame | None:
    try:
        return pd.read_csv(url, dtype=str).fillna("")
    except Exception as e:
        st.error(f"Could not fetch Asana sheet: {e}")
        return None

def build_valid_asana(df: pd.DataFrame | None, source_label: str) -> pd.DataFrame:
    if df is None or "Name" not in df.columns:
        return pd.DataFrame(columns=["_CUS","_SRC"])
    tmp = df.copy()
    tmp["_CUS"] = tmp["Name"].map(get_cus_from_asana_name)
    tmp = tmp[tmp["_CUS"] != ""]
    tmp["_AUTO"] = tmp.apply(lambda r: is_automated_cards(r.to_dict()), axis=1)
    tmp = tmp[~tmp["_AUTO"]]
    tmp["_SRC"] = source_label
    return tmp

def net_suite_cus_key(row: pd.Series) -> str:
    po = normalize_key(row.get("PO/Check Number", ""))
    doc = normalize_key(row.get("Document Number", ""))
    if CUS_RE.fullmatch(po):
        return po
    if CUS_RE.fullmatch(doc):
        return doc
    return ""

# =========================================
# UI
# =========================================
c1, c2 = st.columns([1,1])
with c1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls","xlsx","csv"])
with c2:
    st.markdown("**Asana sheets** are pulled live (no upload needed).")

if not ns_file:
    st.stop()

# =========================================
# Load Data
# =========================================
ns_df = read_ns(ns_file)
ns_df.columns = [str(c).strip().replace('"', '').replace("'", "") for c in ns_df.columns]

uv_df = fetch_asana(UV_SHEET_CSV)
custom_df = fetch_asana(CUSTOM_SHEET_CSV)

st.success(f"NetSuite rows loaded: {len(ns_df):,}")
if uv_df is not None: st.info(f"UV Asana rows loaded: {len(uv_df):,}")
if custom_df is not None: st.info(f"Custom/Laser Asana rows loaded: {len(custom_df):,}")

required_cols = {"Document Number", "PO/Check Number", "Item"}
missing_cols = [c for c in required_cols if c not in ns_df.columns]
if missing_cols:
    st.error(f"Missing required NetSuite columns: {missing_cols}.")
    st.stop()

# =========================================
# Filter Asana Orders
# =========================================
uv_valid = build_valid_asana(uv_df, "UV")
custom_valid = build_valid_asana(custom_df, "CUSTOM")
asana_all = pd.concat([uv_valid, custom_valid], ignore_index=True)
asana_all = asana_all.sort_values(by=["_SRC"], key=lambda s: s.map({"UV":0,"CUSTOM":1})).drop_duplicates(subset="_CUS", keep="first")

# =========================================
# Match CUS# between NetSuite and Asana
# =========================================
ns_df["_CUS_KEY"] = ns_df.apply(net_suite_cus_key, axis=1)
matched = ns_df.merge(asana_all[["_CUS","_SRC"]], left_on="_CUS_KEY", right_on="_CUS", how="inner")

# =========================================
# Apply Order Type and SKU Prefix
# =========================================
def classify_row(row) -> str:
    if row.get("_SRC") == "UV":
        return "UV"
    cus = row.get("_CUS_KEY", "")
    if not custom_df is None:
        hit = custom_df[custom_df["Name"].str.contains(cus, na=False, case=False)]
        if not hit.empty:
            for c in hit.columns:
                if re.sub(r"\s+|[/_]", "", c.strip().lower()) in {"section","sectioncolumn","column"}:
                    if str(hit.iloc[0][c]).strip().lower() == "blank":
                        return "BLANK"
    return "LASER"

def compute_product_number(row) -> str:
    rhs = extract_rhs_sku(row.get("Item", ""))
    t = row.get("__OrderType", "LASER")
    if t == "UV":
        return dedupe_prefix(rhs, "UV-")
    if t == "LASER":
        return dedupe_prefix(rhs, "L-")
    return rhs

if matched.empty:
    st.warning("No NetSuite rows matched to Asana CUS#. Ensure 'Document Number' or 'PO/Check Number' contains CUS#####.")
    st.stop()

matched["__OrderType"] = matched.apply(classify_row, axis=1)
matched["ProductNumber"] = matched.apply(compute_product_number, axis=1)

# =========================================
# Reorder and Output
# =========================================
out_df = matched.copy()
for c in FISHBOWL_COLUMNS:
    if c not in out_df.columns:
        out_df[c] = ""
out_df = out_df.reindex(columns=FISHBOWL_COLUMNS + (["__OrderType"] if "__OrderType" in out_df.columns else []))

st.subheader("Preview (first 100 rows)")
st.dataframe(out_df.head(100), use_container_width=True)

csv_bytes = out_df[FISHBOWL_COLUMNS].to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_bytes, file_name="fishbowl_upload.csv", mime="text/csv")



