import re
import pandas as pd
import streamlit as st

# =========================================
# App setup
# =========================================
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Keeps all NetSuite fields, merges Asana matches, applies SKU logic, and outputs Fishbowl-ready CSV.")

# =========================================
# Live Google Sheet Links
# =========================================
UV_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1Bgw_knhlQcdO2D2LTfJy3XB9Mn5OcDErRrhMLsvgaYM/export?format=csv&gid=790696528"
CUSTOM_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1vJztlcMoXhdZxJdcXYkvFHYqSpYHq4PILMYT0BS_8Hk/export?format=csv&gid=1818164620"

# =========================================
# Fishbowl Column Order
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

def infer_order_type(uv_row: dict | None, custom_row: dict | None) -> str:
    if uv_row:
        for k, v in uv_row.items():
            if re.sub(r"\s+", "", str(k).strip().lower()) == "colorprint" and "uv printer" in str(v).strip().lower():
                return "UV"
    if custom_row:
        for k, v in custom_row.items():
            if re.sub(r"\s+|[/_]", "", str(k).strip().lower()) in {"section","sectioncolumn","column"} and str(v).strip().lower() == "blank":
                return "BLANK"
    return "LASER"

def read_ns(file):
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

def fetch_asana(url):
    try:
        return pd.read_csv(url, dtype=str).fillna("")
    except Exception as e:
        st.error(f"Could not fetch Asana sheet: {e}")
        return pd.DataFrame()

# =========================================
# Load UI
# =========================================
col1, col2 = st.columns([1, 1])
with col1:
    ns_file = st.file_uploader("Upload NetSuite export (.xls, .xlsx, .csv)", type=["xls", "xlsx", "csv"])
with col2:
    st.markdown("**Asana sheets** are pulled live automatically (UV + Custom).")

if not ns_file:
    st.stop()

# =========================================
# Load data
# =========================================
ns_df = read_ns(ns_file)
ns_df.columns = [str(c).strip().replace('"','').replace("'", "") for c in ns_df.columns]

uv_df = fetch_asana(UV_SHEET_CSV)
custom_df = fetch_asana(CUSTOM_SHEET_CSV)

# Confirm core fields exist
for col in ["Document Number", "Item"]:
    if col not in ns_df.columns:
        st.error(f"Missing '{col}' column in NetSuite file.")
        st.stop()

# =========================================
# Prep Asana Data
# =========================================
def prep_asana(df, source_label):
    if df.empty or "Name" not in df.columns:
        return pd.DataFrame()
    df["_CUS"] = df["Name"].map(get_cus_from_asana_name)
    df = df[df["_CUS"] != ""]
    df["_AUTO"] = df.apply(lambda r: is_automated_cards(r.to_dict()), axis=1)
    df = df[~df["_AUTO"]]
    df["_SRC"] = source_label
    return df

uv_valid = prep_asana(uv_df, "UV")
custom_valid = prep_asana(custom_df, "CUSTOM")

asana_all = pd.concat([uv_valid, custom_valid], ignore_index=True)
asana_all = asana_all.sort_values(by=["_SRC"], key=lambda s: s.map({"UV":0,"CUSTOM":1})).drop_duplicates(subset="_CUS", keep="first")

# =========================================
# Match by CUS number in Document Number
# =========================================
ns_df["_CUS_KEY"] = ns_df["Document Number"].apply(lambda x: normalize_key(x))
ns_df["_CUS_KEY"] = ns_df["_CUS_KEY"].apply(lambda x: "CUS" + re.findall(r"CUS(\d+)", x)[0] if "CUS" in x else x)

matched = ns_df.merge(asana_all[["_CUS", "_SRC"]], left_on="_CUS_KEY", right_on="_CUS", how="inner")

if matched.empty:
    st.warning("No NetSuite orders matched Asana CUS numbers. Ensure 'Document Number' contains the CUS##### value.")
    st.stop()

# =========================================
# Apply SKU Prefix Rules
# =========================================
def determine_sku(row):
    rhs = extract_rhs_sku(row.get("Item", ""))
    src = row.get("_SRC", "")
    if src == "UV":
        return dedupe_prefix(rhs, "UV-")
    elif src == "CUSTOM":
        # Check blank
        cus = row.get("_CUS_KEY", "")
        if not custom_df.empty:
            match = custom_df[custom_df["Name"].str.contains(cus, na=False, case=False)]
            if not match.empty:
                for c in match.columns:
                    if re.sub(r"\s+|[/_]", "", str(c).lower()) in {"section","sectioncolumn","column"}:
                        if str(match.iloc[0][c]).strip().lower() == "blank":
                            return rhs
        return dedupe_prefix(rhs, "L-")
    return rhs

matched["ProductNumber"] = matched.apply(determine_sku, axis=1)

# =========================================
# Fill missing Fishbowl columns & reorder
# =========================================
out_df = matched.copy()
for col in FISHBOWL_COLUMNS:
    if col not in out_df.columns:
        out_df[col] = ""

out_df = out_df.reindex(columns=FISHBOWL_COLUMNS)

# =========================================
# Preview + Download
# =========================================
st.subheader("Preview (first 100 rows)")
st.dataframe(out_df.head(100), use_container_width=True)

csv_data = out_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_data, file_name="fishbowl_upload.csv", mime="text/csv")



