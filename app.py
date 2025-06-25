import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz

st.set_page_config(page_title="Lowe's Data Merge Tool", layout="wide")
st.title("Merge Lowes Data Files")
st.markdown("Upload your **Orders**, **Shipments**, and **Invoices** files to generate a merged Excel report.")

#helpers
def format_currency(x):
    return "" if pd.isna(x) else f"${x:,.2f}"

def format_date(series):
    return pd.to_datetime(series, errors="coerce").dt.strftime("%m/%d/%Y")

def dedupe_columns(cols):
    seen = {}
    new_cols = []
    for col in cols:
        if col not in seen:
            seen[col] = 0
            new_cols.append(col)
        else:
            seen[col] += 1
            new_cols.append(f"{col}.{seen[col]}")
    return new_cols

def pick_notna(series):
    return series.dropna().iloc[0] if not series.dropna().empty else ""

#file uploads
uploaded_orders = st.file_uploader("Upload Orders File (.xlsx)", type="xlsx")
uploaded_shipments = st.file_uploader("Upload Shipments File (.xlsx)", type="xlsx")
uploaded_invoices = st.file_uploader("Upload Invoices File (.xlsx)", type="xlsx")

if uploaded_orders and uploaded_shipments and uploaded_invoices:
    progress = st.progress(0, text="Reading files...")

    orders = pd.read_excel(uploaded_orders, dtype=str)
    shipments = pd.read_excel(uploaded_shipments, dtype=str)
    invoices = pd.read_excel(uploaded_invoices, dtype=str)

    orders.columns = orders.columns.str.strip()
    shipments.columns = shipments.columns.str.strip()
    invoices.columns = invoices.columns.str.strip()

    # Fill blank Record Type with Invoice Purpose if missing
    if "Record Type" in invoices.columns and "Invoice purpose" in invoices.columns:
        invoices["Record Type"] = invoices["Record Type"].fillna(invoices["Invoice purpose"])

    progress.progress(20, text="Cleaning orders and metadata...")

    #mappings
    vbu_mapping = {118871: "Fostoria", 118872: "Jackson", 503177: "Longwood", 503255: "Greenville",
                   502232: "Milazzo", 505071: "Claymont", 505496: "Gaylord", 505085: "Spring Valley Ice Melt",
                   114037: "PCI Nitrogen", 501677: "Theremorock East Inc"}

    palette_mapping = {"4983612": 50, "4983613": 50, "5113267": 50, "335456": 32, "552696": 32, "5516714": 210,
                       "5516716": 210, "5516715": 210, "71894": 12, "72931": 60, "92951": 12, "97086": 12,
                       "97809": 12, "167411": 4, "552704": 35, "91900": 12, "961539": 50, "552697": 32,
                       "71918": 12, "94833": 60, "552706": 32, "72801": 60, "101760": 50}

    vendor_item_mapping = {
    "4983612": "B8110200", "4983613": "B8110300", "5113267": "B8110100", "335456": "B1195080",
    "552696": "B1195020", "5516714": "B8100731", "5516716": "B8100732", "5516715": "B8100733",
    "71894": "B1246160", "72931": "B1224080", "92951": "B1224060", "97086": "B1260060",
    "97809": "B1201460", "167411": "B1237440", "552704": "B1195010", "91900": "B1202360",
    "961539": "B1258800", "552697": "B1195040", "71918": "B1246150", "94833": "B1260080",
    "552706": "B1195050", "72801": "B1202380", "101760": "B2063400", "120019": "B1200190",
    "1240180": "B1242620", "91299": "B1202390", "4350231": "B4973300", "876249": "B4905700",
    "758650": "B4905600", "243942": "B4904900", "335457": "B2241150", "147992": "B1288200",
    "148054": "B1298200", "1330491": "B4292000"}

    item_type_mapping = {"4983612": "Commodity", "4983613": "Commodity", "5113267": "Commodity", "335456": "Sunniland",
        "552696": "Sunniland", "5516714": "Soil", "5516716": "Soil", "5516715": "Soil",
        "71894": "Sunniland", "72931": "Sunniland", "92951": "Sunniland", "97086": "Sunniland",
        "97809": "Sunniland", "167411": "Sunniland", "552704": "Sunniland", "91900": "Sunniland",
        "961539": "Sunniland", "552697": "Sunniland", "71918": "Sunniland", "94833": "Sunniland",
        "552706": "Sunniland", "72801": "Sunniland", "101760": "Ice Melt", "1053900": "Ice Melt",
        "120019": "Sunniland", "1240180": "Sunniland", "91299": "Sunniland", "335457": "Sunniland",
        "147992": "Sunniland", "148054": "Sunniland"}

    # Normalize column names for consistency
    orders.columns = orders.columns.str.strip().str.replace("PO Line #", "PO Line#", regex=False)

    # clean
    required_order_cols = ["PO Line#", "Qty Ordered"]
    for col in required_order_cols:
        if col not in orders.columns:
            st.error(f"Missing required column in orders file: '{col}'")
            st.write("Columns found:", list(orders.columns))
            st.stop()

    orders["PO Line#"] = pd.to_numeric(orders["PO Line#"], errors="coerce")
    orders["Qty Ordered"] = pd.to_numeric(orders["Qty Ordered"], errors="coerce")

    headers = orders[
        (orders["PO Line#"].isna()) & (orders["Qty Ordered"].isna())].copy()

    details = orders[
        (orders["PO Line#"].notna()) | (orders["Qty Ordered"].notna())].copy()

    meta_cols = [
        "PO Number", "PO Date", "Vendor #",
        "Ship To Name", "Ship To State", "Requested Delivery Date"]

    headers_meta = headers[meta_cols].drop_duplicates(subset=["PO Number"])

    orders = details.merge(headers_meta, on="PO Number", how="left", suffixes=("", "_hdr"))

    for col in meta_cols[1:]:
        orders[col] = orders[col].combine_first(orders[f"{col}_hdr"])
        orders.drop(columns=[f"{col}_hdr"], inplace=True)

    #fields
    orders["Item#"] = orders["Buyers Catalog or Stock Keeping #"]
    orders["Vendor Item#"] = orders["Item#"].map(vendor_item_mapping)
    orders["Item Name"] = orders["Product/Item Description"]
    orders["VBU#"] = pd.to_numeric(orders["Vendor #"], errors="coerce")
    orders["VBU Name"] = orders["VBU#"].map(vbu_mapping)
    orders["Palettes Each"] = orders["Item#"].map(palette_mapping)
    orders["Palettes"] = (orders["Qty Ordered"] / orders["Palettes Each"]).round(1)
    orders["Item Type"] = orders["Item#"].map(item_type_mapping)
    orders["Unit Price"] = pd.to_numeric(orders["Unit Price"].replace('[\\$,]', '', regex=True), errors="coerce")
    orders["Merch Total"] = orders["Qty Ordered"] * orders["Unit Price"]

    orders["Requested Delivery Date"] = format_date(orders["Requested Delivery Date"])
    orders["PO Date"] = format_date(orders["PO Date"])
    orders["PO Date Sortable"] = pd.to_datetime(orders["PO Date"], errors="coerce")
    orders["PO Num Sort"] = pd.to_numeric(orders["PO Number"], errors="coerce")
    orders = orders.sort_values(by=["PO Date Sortable", "PO Num Sort"], ascending=[False, False])

    progress.progress(40, text="Merging Shipments...")

    shipments = shipments.rename(columns={"PO #": "PO Number", "Buyer Item #": "Item#"})
    shipment_cols = ["PO Number", "Item#", "Location #", "ASN Date", "Ship Date", "BOL", "SCAC", "ASN #"]
    shipments = shipments[[col for col in shipment_cols if col in shipments.columns]].copy()

    if "ASN Date" in shipments:
        shipments["ASN Date"] = format_date(shipments["ASN Date"])
    if "Ship Date" in shipments:
        shipments["Ship Date"] = format_date(shipments["Ship Date"])

    orders = orders.merge(shipments, how="left", on=["PO Number", "Item#"])
    orders.rename(columns={
        "BOL": "BOL#",
        "ASN #": "ASN#"
    }, inplace=True)

    progress.progress(60, text="Merging Invoices...")

    invoices = invoices.rename(columns={"Retailers PO #": "PO Number"})
    if "Invoice Total" in invoices.columns:
        invoices["Invoice Total"] = pd.to_numeric(invoices["Invoice Total"].replace('[\\$,]', '', regex=True), errors="coerce")
        invoices["Invoice Total"] = invoices["Invoice Total"].apply(format_currency)
    if "Discounted Amounted_Discount Amount" in invoices.columns:
        invoices["Discounted Amounted_Discount Amount"] = pd.to_numeric(invoices["Discounted Amounted_Discount Amount"].replace('[\\$,]', '', regex=True), errors="coerce")
        invoices["Discounted Amounted_Discount Amount"] = invoices["Discounted Amounted_Discount Amount"].apply(format_currency)
    if "Invoice Date" in invoices.columns:
        invoices["Invoice Date"] = format_date(invoices["Invoice Date"])

    invoice_grouped = (
        invoices
        .groupby("PO Number")
        .agg({
            "Invoice Number": pick_notna,
            "Invoice Date": pick_notna,
            "Invoice Total": pick_notna,
            "Discounted Amounted_Discount Amount": pick_notna}).reset_index())

    orders = orders.merge(invoice_grouped, on="PO Number", how="left")
    orders.rename(columns={"Discounted Amounted_Discount Amount": "Invoice Disc.", "Invoice Number": "Invoice#"}, inplace=True)

    progress.progress(80, text="Finalizing output...")

    orders["Fulfillment Status"] = "Not Invoiced"
    orders.loc[pd.notna(orders["Invoice#"]), "Fulfillment Status"] = "Invoiced"
    orders.loc[(pd.notna(orders["Ship Date"]) & (orders["Fulfillment Status"] == "Not Invoiced")), "Fulfillment Status"] = "Shipped Not Invoiced"
    orders["Late Ship"] = pd.to_datetime(orders["Ship Date"], errors="coerce") > pd.to_datetime(orders["Requested Delivery Date"], errors="coerce")
    orders["Late Ship"] = orders["Late Ship"].map({True: "Yes", False: "No"}).fillna("")

    orders["Month Filter"] = pd.to_datetime(orders["PO Date"], errors="coerce").dt.month
    orders["Year Filter"] = pd.to_datetime(orders["PO Date"], errors="coerce").dt.year
    orders["Quarter Filter"] = "Q" + pd.to_datetime(orders["PO Date"], errors="coerce").dt.quarter.astype(str)

    final_cols = [
        "PO Number", "PO Date", "VBU#", "VBU Name", "Item#", "Vendor Item#", "Item Name", "Item Type",
        "Qty Ordered", "Palettes", "Unit Price", "Merch Total", "PO Line#", "Ship To Name", "Ship To State",
        "Requested Delivery Date", "Fulfillment Status", "Late Ship", "ASN Date", "Ship Date", "ASN#",
        "BOL#", "SCAC", "Invoice#", "Invoice Date", "Invoice Disc.", "Invoice Total",
        "Month Filter", "Year Filter", "Quarter Filter"]

    for col in final_cols:
        if col not in orders.columns:
            orders[col] = ""

    orders = orders.reindex(columns=final_cols).fillna("")

    tz = pytz.timezone("America/New_York")
    timestamp = datetime.now(tz).strftime("%Y-%m-%d_%H%M")
    filename = f"Lowes_Merged_{timestamp}.xlsx"

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        orders.to_excel(writer, index=False, sheet_name="Orders")
        workbook = writer.book
        worksheet = writer.sheets["Orders"]
        for idx, col in enumerate(orders.columns):
            worksheet.set_column(idx, idx, 20)
            header_format = workbook.add_format({"align": "left", "bold": True})
            worksheet.write(0, idx, col, header_format)

    file_size_kb = len(output.getvalue()) / 1024
    progress.progress(100, text="Complete")
    st.success(f"✅ Your file is saved as **{filename}**")
    st.caption(f"Approx. file size: {file_size_kb:.1f} KB")
    st.info(f"Total merged rows: {len(orders):,}")
    st.download_button(
        "Download Merged Excel File",
        data=output.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
