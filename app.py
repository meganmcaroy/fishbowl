import re
import pandas as pd
import streamlit as st
from difflib import get_close_matches

# =========================================
# App setup
# =========================================
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana data into a Fishbowl-ready CSV with full SKU, SONum, address, and date logic.")

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
ADDRESS_RE = re.compile(r'([\w\s]+)\s+([A-Z]{2})\s+(\d{5})(?:[-\d]*)?\s*(.*)?$')

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

def extract_after_colon(text: str) -> str:
    """Get everything after ':' and uppercase it."""
    text = str(text or "").strip()
    if ":" in text:
        return text.split(":", 1)[1].strip().upper()
    return text.upper()

def format_date(date_str):
    """Convert to MM/DD/YYYY."""
    try:
        date = pd.to_datetime(date_str, errors="coerce")
        if pd.notna(date):
            return date.strftime("%m/%d/%Y")
    except Exception:
        pass
    return ""

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

def parse_address(full_address: str):
    """Try to split full address string into address, city, state, zip, country."""
    if not full_address:
        return "", "", "", "", ""
    text = " ".join(str(full_address).split())
    match = ADDRESS_RE.search(text)
    if match:
        city, state, zip_code, country = match.groups()[0], match.groups()[1], match.groups()[2], match.groups()[3] or ""
        return text, city.strip(), state.strip(), zip_code.strip(), country.strip()
    return text, "", "", "", ""

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
matched = ns_df.merge(asana_all[["_CUS_KEY", "_SRC", "_CUS"]], on="_CUS_KEY", how="inner")

if matched.empty:
    st.warning("No NetSuite orders matched Asana CUS numbers. Ensure 'PO/Check Number' matches '#CUS#####' in Asana 'Name'.")
    st.stop()

# =========================================
# ProductNumber Logic
# =========================================
def determine_product_number(row):
    desc = row.get("Item") or row.get("Product Description") or ""
    src = row.get("_SRC", "")
    sku = extract_after_colon(desc)
    if not sku:
        return ""
    if src == "UV":
        return dedupe_prefix(sku, "UV-")
    elif src == "CUSTOM":
        return dedupe_prefix(sku, "L-")
    else:
        return sku

matched["ProductNumber"] = matched.apply(determine_product_number, axis=1)

# =========================================
# Build Output Frame
# =========================================
out_df = pd.DataFrame(columns=FISHBOWL_COLUMNS)

# Product fields
out_df["ProductDescription"] = matched["Item"] if "Item" in matched.columns else matched["Product Description"]
out_df["ProductNumber"] = matched["ProductNumber"]

# Billing info
if "Billing Addressee" in matched.columns:
    out_df["CustomerName"] = matched["Billing Addressee"]
    out_df["BillToName"] = matched["Billing Addressee"]
    out_df["ShipToName"] = matched["Billing Addressee"]

if "Billing Address" in matched.columns:
    parsed = matched["Billing Address"].apply(parse_address)
    out_df["BillToAddress"] = parsed.apply(lambda x: x[0])
    out_df["BillToCity"] = parsed.apply(lambda x: x[1])
    out_df["BillToState"] = parsed.apply(lambda x: x[2])
    out_df["BillToZip"] = parsed.apply(lambda x: x[3])
    out_df["BillToCountry"] = parsed.apply(lambda x: x[4])

if "Shipping Address" in matched.columns:
    parsed_ship = matched["Shipping Address"].apply(parse_address)
    out_df["ShipToAddress"] = parsed_ship.apply(lambda x: x[0])
    out_df["ShipToCity"] = parsed_ship.apply(lambda x: x[1])
    out_df["ShipToState"] = parsed_ship.apply(lambda x: x[2])
    out_df["ShipToZip"] = parsed_ship.apply(lambda x: x[3])
    out_df["ShipToCountry"] = parsed_ship.apply(lambda x: x[4])

# =========================================
# Defaults (Status = 20)
# =========================================
out_df["Status"] = "20"
out_df["CarrierName"] = "Will Call"
out_df["LocationGroupName"] = "Farm"
out_df["Taxable"] = "FALSE"
out_df["TaxCode"] = "NON"
out_df["ShowItem"] = "TRUE"
out_df["KitItem"] = "FALSE"

# =========================================
# Date
# =========================================
date_cols = [c for c in matched.columns if c.lower() in ["order date", "date created", "transaction date", "date"]]
if date_cols:
    out_df["Date"] = matched[date_cols[0]].apply(format_date)
else:
    out_df["Date"] = ""

# =========================================
# SONum Logic
# =========================================
def make_sonum(row):
    cus = row.get("_CUS", "").strip()
    sku = row.get("ProductNumber", "").strip().upper()
    if sku.startswith("UV-"):
        return f"UV {cus}"
    elif sku.startswith("L-"):
        return f"{cus}"
    else:
        return f"Blank {cus}"

out_df["SONum"] = matched.apply(make_sonum, axis=1)

# PONum = Document Number
if "Document Number" in matched.columns:
    out_df["PONum"] = matched["Document Number"]

# =========================================
# Preview + Download
# =========================================
st.subheader("Preview (first 100 rows)")
st.dataframe(out_df.head(100), use_container_width=True)

csv_data = out_df.to_csv(index=False).encode("utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_data, file_name="fishbowl_upload.csv", mime="text/csv")


