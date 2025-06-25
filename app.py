import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz

st.set_page_config(page_title="Lowe's Merge Tool", layout="wide")

st.title("Merge Lowes Data Files")
st.markdown("Upload your **Orders**, **Shipments**, and **Invoices** files to generate a clean, merged Excel report.")

def format_currency(x): return "" if pd.isna(x) else f"${x:,.2f}"
def format_date(series): return pd.to_datetime(series, errors="coerce").dt.strftime("%-m/%-d/%Y")

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

uploaded_orders = st.file_uploader("Upload Orders File (.xlsx)", type="xlsx")
uploaded_shipments = st.file_uploader("Upload Shipments File (.xlsx)", type="xlsx")
uploaded_invoices = st.file_uploader("Upload Invoices File (.xlsx)", type="xlsx")

if uploaded_orders and uploaded_shipments and uploaded_invoices:
    progress = st.progress(0, text="Reading files...")

    orders = pd.read_excel(uploaded_orders, dtype=str)
    shipments = pd.read_excel(uploaded_shipments, dtype=str)
    invoices = pd.read_excel(uploaded_invoices, dtype=str)

    orders.columns = orders.columns.astype(str).str.strip()
    shipments.columns = shipments.columns.astype(str).str.strip()
    invoices.columns = invoices.columns.astype(str).str.strip()

    progress.progress(20, text="Cleaning Orders data...")

    vbu_mapping = {
        118871: "Fostoria", 118872: "Jackson", 503177: "Longwood", 503255: "Greenville", 502232: "Milazzo",
        505071: "Claymont", 505496: "Gaylord", 505085: "Spring Valley Ice Melt", 114037: "PCI Nitrogen",
        501677: "Theremorock East Inc"
    }

    palette_mapping = {}
    vendor_item_mapping = {}
    item_type_mapping = {}

    required_cols = ["PO Number", "PO Line#"]
    missing = [col for col in required_cols if col not in orders.columns]
    if missing:
        st.error(f"Your {missing} is either missing or in the incorrect format.")
        st.stop()

    orders["PO# Num"] = pd.to_numeric(orders["PO Number"], errors="coerce")

    headers = orders[orders["PO Line#"].isna() & orders["Qty Ordered"].isna()].copy()
    details = orders[orders["PO Line#"].notna() | orders["Qty Ordered"].notna()].copy()

    cols_to_ffill = ["PO Number", "PO Date", "Vendor #", "Ship To Location", "Requested Delivery Date"]
    details = details.sort_values(["PO# Num", "Vendor #", "PO Line#"], ascending=[True, True, True])
    details[cols_to_ffill] = details[cols_to_ffill].ffill().infer_objects(copy=False)

    orders = details.copy()
    orders = orders.drop_duplicates(subset=["PO Number", "PO Line#", "Buyers Catalog or Stock Keeping #", "Vendor #"])

    orders["Qty Ordered"] = pd.to_numeric(orders["Qty Ordered"], errors="coerce")
    orders["Unit Price"] = orders["Unit Price"].replace('[\\$,]', '', regex=True).astype(float)

    orders["Vendor # Clean"] = orders["Vendor #"].str.extract(r"(\\d+)")[0].astype(float)
    orders["VBU Name"] = orders["Vendor # Clean"].map(vbu_mapping)
    orders["VBU#"] = orders["Vendor #"]
    orders["Unit Price"] = orders["Unit Price"].apply(format_currency)

    for col in ["PO Date", "Requested Delivery Date", "Ship Dates"]:
        if col in orders.columns:
            orders[col] = format_date(orders[col])

    orders["Palettes"] = orders["Buyers Catalog or Stock Keeping #"].map(palette_mapping)
    orders["Vendor Item#"] = orders["Buyers Catalog or Stock Keeping #"].map(vendor_item_mapping)
    orders["Item Type"] = orders["Buyers Catalog or Stock Keeping #"].map(item_type_mapping)

    progress.progress(40, text="Merging Shipments...")

    shipments = shipments.rename(columns={
        "PO #": "PO Number",
        "Buyer Item #": "Buyers Catalog or Stock Keeping #",
        "Location #": "Ship To Location"
    })
    shipments.columns = dedupe_columns(shipments.columns)

    for col in ["ASN Date", "Ship Date"]:
        shipments[col] = pd.to_datetime(shipments[col], errors="coerce")

    shipment_collapsed = (
        shipments.groupby(["PO Number", "Buyers Catalog or Stock Keeping #", "Ship To Location"], as_index=False)
        .agg({
            "ASN Date": "max",
            "Ship Date": "max",
            "BOL": lambda x: "/".join(sorted(set(x.dropna().astype(str)))),
            "SCAC": lambda x: "/".join(sorted(set(x.dropna().astype(str))))
        })
    )

    shipment_collapsed["ASN Date"] = format_date(shipment_collapsed["ASN Date"])
    shipment_collapsed["Ship Date"] = format_date(shipment_collapsed["Ship Date"])

    orders = orders.merge(
        shipment_collapsed,
        on=["PO Number", "Buyers Catalog or Stock Keeping #", "Ship To Location"],
        how="left"
    )

    progress.progress(60, text="Merging Invoices...")

    invoice_collapsed = invoices.groupby("Invoice Number", as_index=False).agg({
        "Retailers PO #": "first",
        "Invoice Date": "first",
        "Discounted Amounted_Discount Amount": "first",
        "Invoice Total": "first"
    })

    invoice_collapsed["Invoice Total"] = pd.to_numeric(
        invoice_collapsed["Invoice Total"].replace('[\\$,]', '', regex=True), errors="coerce"
    ).apply(format_currency)

    invoice_collapsed["Discounted Amounted_Discount Amount"] = pd.to_numeric(
        invoice_collapsed["Discounted Amounted_Discount Amount"].replace('[\\$,]', '', regex=True), errors="coerce"
    ).apply(format_currency)

    invoice_collapsed["Invoice Date"] = format_date(invoice_collapsed["Invoice Date"])

    orders["PO_clean"] = orders["PO Number"].astype(str).str.strip().str.replace(r"\\.0$", "", regex=True)
    invoice_collapsed["PO_clean"] = invoice_collapsed["Retailers PO #"].astype(str).str.strip().str.replace(r"\\.0$", "", regex=True)

    orders = orders.merge(
        invoice_collapsed[[
            "PO_clean", "Invoice Number", "Invoice Date",
            "Discounted Amounted_Discount Amount", "Invoice Total"
        ]],
        on="PO_clean",
        how="left"
    ).drop(columns="PO_clean")

    orders = orders.rename(columns={
        "Discounted Amounted_Discount Amount": "Invoice Disc."
    })

    progress.progress(80, text="Finalizing output...")

    orders["Fulfillment Status"] = "Not Invoiced"
    orders.loc[pd.notna(orders["Invoice Number"]), "Fulfillment Status"] = "Invoiced"
    orders.loc[
        (pd.notna(orders["Ship Date"]) & (orders["Fulfillment Status"] == "Not Invoiced")),
        "Fulfillment Status"
    ] = "Shipped Not Invoiced"

    orders["Late Ship"] = pd.to_datetime(orders["Ship Date"], errors="coerce") > pd.to_datetime(orders["Requested Delivery Date"], errors="coerce")
    orders["Late Ship"] = orders["Late Ship"].map({True: "Yes", False: "No"}).fillna("")
    orders["Ship to Name"] = orders["Ship To Name"] if "Ship To Name" in orders.columns else ""

    orders["PO Date Sortable"] = pd.to_datetime(orders["PO Date"], errors="coerce")
    orders = orders.sort_values(by=["PO Date Sortable", "PO# Num"], ascending=[False, False])
    orders.drop(columns=["PO Date Sortable", "PO# Num", "Vendor # Clean"], inplace=True)

    orders["Month Filter"] = pd.to_datetime(orders["PO Date"], errors="coerce").dt.month
    orders["Year Filter"] = pd.to_datetime(orders["PO Date"], errors="coerce").dt.year
    orders["Quarter Filter"] = "Q" + pd.to_datetime(orders["PO Date"], errors="coerce").dt.quarter.astype(str)

    orders = orders.rename(columns={
        "PO Number": "PO#",
        "Buyers Catalog or Stock Keeping #": "Item#",
        "Item": "Item Name",
        "Invoice Number": "Invoice#",
        "BOL": "BOL#"
    })

    final_cols = [
        "PO#", "PO Date", "VBU#", "VBU Name", "Item#", "Vendor Item#", "Item Name", "Item Type",
        "Qty Ordered", "Palettes", "Unit Price", "PO Line#", "Ship To Location", "Ship to Name",
        "Ship To State", "Requested Delivery Date", "Fulfillment Status", "Late Ship",
        "ASN Date", "Ship Date", "BOL#", "SCAC", "Invoice#", "Invoice Date",
        "Invoice Disc.", "Invoice Total", "Month Filter", "Year Filter", "Quarter Filter"
    ]

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
    progress.progress(100, text="✅ Complete!")
    st.success("File processed")
    st.caption(f"Approx. file size: {file_size_kb:.1f} KB")
    st.info(f"Total merged rows: {len(orders):,}")
    st.download_button(
        "Download Merged Excel File",
        data=output.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
