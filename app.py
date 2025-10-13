import re
import pandas as pd
import streamlit as st

# =========================
# App setup
# =========================
st.set_page_config(page_title="Fishbowl Upload Transformer", layout="wide")
st.title("Fishbowl Upload Transformer")
st.caption("Transforms NetSuite + Asana into a Fishbowl-ready CSV. Matches a reference template’s column order and skips Automated Cards.")

# -------------------------
# Live Asana links
# -------------------------
UV_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1Bgw_knhlQcdO2D2LTfJy3XB9Mn5OcDErRrhMLsvgaYM/export?format=csv&gid=790696528"
CUSTOM_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1vJztlcMoXhdZxJdcXYkvFHYqSpYHq4PILMYT0BS_8Hk/export?format=csv&gid=1818164620"

# -------------------------
# Helpers
# -------------------------
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
        if k_norm in {"section","sectioncolumn","column","section/column"}:
            return str(v).strip().lower() == "automated cards"
    return False

def dedupe_prefix(sku: str, prefix: str) -> str:
    sku = str(sku or "").strip()
    if not sku:
        return sku
    return sku if sku.upper().startswith(prefix.upper()) else f"{prefix}{sku}"

def extract_after_colon(text: str) -> str:
    text = str(text or "").strip()
    if ":" in text:
        return text.split(":", 1)[1].strip().upper()
    return text.upper()

def parse_address(full_address: str):
    if not full_address:
        return "", "", "", "", ""
    text = " ".join(str(full_address).split())
    match = ADDRESS_RE.search(text)
    if match:
        city, state, zip_code, country = match.groups()[0], match.groups()[1], match.groups()[2], match.groups()[3] or ""
        return text, city.strip(), state.strip(), zip_code.strip(), country.strip()
    return text, "", "", "", ""

def format_date(date_str):
    try:
        date = pd.to_datetime(date_str, errors="coerce")
        if pd.notna(date):
            return date.strftime("%m/%d/%Y")
    except Exception:
        pass
    return ""

def read_any_tabular(file):
    """Read CSV / TSV / XLS / XLSX"""
    if not file:
        return pd.DataFrame()
    # Try CSV first
    try:
        df = pd.read_csv(file, dtype=str).fillna("")
        if df.shape[1] == 1:  # maybe TSV
            file.seek(0)
            df = pd.read_csv(file, dtype=str, sep="\t").fillna("")
        return df
    except Exception:
        pass
    # Try Excel
    file.seek(0)
    try:
        return pd.read_excel(file, dtype=str, engine="openpyxl").fillna("")
    except Exception:
        return pd.DataFrame()

def fetch_csv(url):
    try:
        return pd.read_csv(url, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()

# -------------------------
# UI – uploads
# -------------------------
c1, c2 = st.columns([1,1])
with c1:
    ns_file = st.file_uploader("Upload NetSuite export (CSV/XLS/XLSX)", type=["csv","xls","xlsx"])
with c2:
    ref_file = st.file_uploader("Upload reference file (10.13.25_upload.csv) to match column order", type=["csv"])

if not ns_file:
    st.stop()

# Read NetSuite & Asana
ns_df = read_any_tabular(ns_file)
ns_df.columns = [str(c).strip() for c in ns_df.columns]

uv_df = fetch_csv(UV_SHEET_CSV)
custom_df = fetch_csv(CUSTOM_SHEET_CSV)

# -------------------------
# Prep Asana data
# -------------------------
def prep_asana(df, source):
    if df.empty or "Name" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["_CUS"] = df["Name"].map(get_cus_from_asana_name)
    df = df[df["_CUS"] != ""]
    df["_AUTO"] = df.apply(lambda r: is_automated_cards(r.to_dict()), axis=1)
    df = df[~df["_AUTO"]]
    df["_SRC"] = source  # UV or CUSTOM
    return df

uv_valid = prep_asana(uv_df, "UV")
custom_valid = prep_asana(custom_df, "CUSTOM")

# Priority: UV over CUSTOM
asana_all = pd.concat([uv_valid, custom_valid], ignore_index=True)
asana_all = asana_all.sort_values(by=["_SRC"], key=lambda s: s.map({"UV":0, "CUSTOM":1}))
asana_all = asana_all.drop_duplicates(subset="_CUS", keep="first")

# -------------------------
# Match by CUS: NetSuite PO/Check Number ↔ Asana Name (#CUS…)
# -------------------------
if "PO/Check Number" not in ns_df.columns:
    st.error("NetSuite file is missing 'PO/Check Number' column.")
    st.stop()

ns_df["_CUS_KEY"] = ns_df["PO/Check Number"].apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", str(x)).upper())
asana_all["_CUS_KEY"] = asana_all["_CUS"].apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", str(x)).upper())

matched = ns_df.merge(asana_all[["_CUS_KEY", "_SRC", "_CUS"]], on="_CUS_KEY", how="inner")
if matched.empty:
    st.warning("No NetSuite orders matched Asana CUS numbers (or all matched rows were in Automated Cards).")
    st.stop()

# -------------------------
# ProductNumber logic (no colon => NO prefix)
# -------------------------
def determine_product_number(row):
    desc = row.get("Item") or row.get("Product Description") or ""
    desc = str(desc).strip()
    src = row.get("_SRC", "")
    if ":" not in desc:
        return desc.upper()  # leave as-is (uppercased) when no colon
    sku = extract_after_colon(desc)  # part after ':', uppercased
    if not sku:
        return ""
    if src == "UV":
        return dedupe_prefix(sku, "UV-")
    elif src == "CUSTOM":
        return dedupe_prefix(sku, "L-")
    else:
        return sku

matched["ProductNumber"] = matched.apply(determine_product_number, axis=1)

# -------------------------
# Build output frame (start empty; we’ll populate)
# -------------------------
out = pd.DataFrame()

# ProductDescription stays full Item text
if "Item" in matched.columns:
    out["ProductDescription"] = matched["Item"]
elif "Product Description" in matched.columns:
    out["ProductDescription"] = matched["Product Description"]
else:
    out["ProductDescription"] = ""

# ProductNumber
out["ProductNumber"] = matched["ProductNumber"]

# ProductQuantity from NetSuite
if "Quantity" in matched.columns:
    out["ProductQuantity"] = matched["Quantity"]
elif "Quanity" in matched.columns:  # if typo present
    out["ProductQuantity"] = matched["Quanity"]
else:
    out["ProductQuantity"] = ""

# Billing/Shipping Names
if "Billing Addressee" in matched.columns:
    out["CustomerName"] = matched["Billing Addressee"]
    out["BillToName"] = matched["Billing Addressee"]
    out["ShipToName"] = matched["Billing Addressee"]

# BillTo address parsing
if "Billing Address" in matched.columns:
    parsed = matched["Billing Address"].apply(parse_address)
    out["BillToAddress"]   = parsed.apply(lambda x: x[0])
    out["BillToCity"]      = parsed.apply(lambda x: x[1])
    out["BillToState"]     = parsed.apply(lambda x: x[2])
    out["BillToZip"]       = parsed.apply(lambda x: x[3])
    out["BillToCountry"]   = parsed.apply(lambda x: x[4])

# ShipTo address parsing
if "Shipping Address" in matched.columns:
    parsed_ship = matched["Shipping Address"].apply(parse_address)
    out["ShipToAddress"]   = parsed_ship.apply(lambda x: x[0])
    out["ShipToCity"]      = parsed_ship.apply(lambda x: x[1])
    out["ShipToState"]     = parsed_ship.apply(lambda x: x[2])
    out["ShipToZip"]       = parsed_ship.apply(lambda x: x[3])
    out["ShipToCountry"]   = parsed_ship.apply(lambda x: x[4])

# PONum = Document Number (SO)
if "Document Number" in matched.columns:
    out["PONum"] = matched["Document Number"]

# Date (order placed)
date_cols = [c for c in matched.columns if c.lower() in ["order date","date created","transaction date","date"]]
out["Date"] = matched[date_cols[0]].apply(format_date) if date_cols else ""

# Fixed defaults
out["Status"] = "20"
out["CarrierName"] = "Will Call"
out["LocationGroupName"] = "Farm"
out["Taxable"] = "FALSE"
out["TaxCode"] = "NON"
out["ShowItem"] = "TRUE"
out["KitItem"] = "FALSE"

# SONum logic — based on Asana source (not on SKU prefix)
def make_sonum(row):
    cus = row.get("_CUS", "").strip()
    src = row.get("_SRC", "")
    if src == "UV":
        return f"UV {cus}"
    elif src == "CUSTOM":
        return f"{cus}"          # Laser orders appear as plain CUS#### (not "Blank")
    else:
        return f"Blank {cus}"

out["SONum"] = matched.apply(make_sonum, axis=1)

# Bring through optional contact fields if present in NetSuite
for src_col, dst_col in [
    ("Phone","Phone"),
    ("Email","Email"),
    ("Shipping Terms","ShippingTerms"),
    ("Payment Terms","PaymentTerms"),
    ("Carrier Service","CarrierService"),
    ("URL","URL"),
]:
    if src_col in matched.columns:
        out[dst_col] = matched[src_col]

# -------------------------
# Match the exact column order from a reference CSV
# -------------------------
if ref_file:
    ref_df = pd.read_csv(ref_file, nrows=0)  # just headers
    ref_cols = list(ref_df.columns)
    # ensure all ref columns exist in out; add blanks if missing
    for c in ref_cols:
        if c not in out.columns:
            out[c] = ""
    # also keep any extra columns we filled that might not be in ref; append them to the end
    ordered_cols = ref_cols + [c for c in out.columns if c not in ref_cols]
    out = out[ordered_cols]
else:
    # Fallback order if no reference provided (typical Fishbowl order)
    fallback = [
        'SONum','Status','CustomerName','CustomerContact','BillToName','BillToAddress','BillToCity','BillToState','BillToZip','BillToCountry',
        'ShipToName','ShipToAddress','ShipToCity','ShipToState','ShipToZip','ShipToCountry','ShipToResidential','CarrierName','TaxRateName','PriorityId',
        'PONum','VendorPONum','Date','Salesman','ShippingTerms','PaymentTerms','FOB','Note','QuickBooksClassName','LocationGroupName','OrderDateScheduled',
        'URL','CarrierService','DateExpired','Phone','Email','Category','CF-Due Date','CF-Custom','SOItemTypeID','ProductNumber','ProductDescription',
        'ProductQuantity','UOM','ProductPrice','Taxable','TaxCode','ItemNote','ItemQuickBooksClassName','ItemDateScheduled','ShowItem','KitItem','RevisionLevel','CustomerPartNumber'
    ]
    for c in fallback:
        if c not in out.columns:
            out[c] = ""
    out = out[fallback]

# -------------------------
# Preview + Download
# -------------------------
st.subheader("Preview (first 100 rows)")
st.dataframe(out.head(100), use_container_width=True)

csv_data = out.to_csv(index=False, encoding="utf-8-sig")
st.download_button("Download Fishbowl CSV", data=csv_data, file_name="fishbowl_upload.csv", mime="text/csv")

