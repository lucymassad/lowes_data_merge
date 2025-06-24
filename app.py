import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz

st.set_page_config(page_title="Lowe's Merge Tool", layout="wide")

st.title("Merge Lowes Data Files")
st.markdown("Upload your **Orders**, **Shipments**, and **Invoices** files to generate a clean, merged Excel report.")

uploaded_orders=st.file_uploader("Upload Orders File (.xlsx)", type="xlsx")
uploaded_shipments=st.file_uploader("Upload Shipments File (.xlsx)", type="xlsx")
uploaded_invoices=st.file_uploader("Upload Invoices File (.xlsx)", type="xlsx")

if uploaded_orders and uploaded_shipments and uploaded_invoices:
  progress=st.progress(0,text="Reading files...")

  orders=pd.read_excel(uploaded_orders,dtype=str)
  shipments=pd.read_excel(uploaded_shipments,dtype=str)
  invoices=pd.read_excel(uploaded_invoices,dtype=str)
  progress.progress(20,text="Cleaning Orders data...")

  vbu_mapping={
    118871:"Fostoria",118872:"Jackson",503177:"Longwood",503255:"Greenville",502232:"Milazzo",
    505071:"Claymont",505496:"Gaylord",505085:"Spring Valley Ice Melt",114037:"PCI Nitrogen",
    501677:"Theremorock East Inc"}

  def format_currency(x):return""if pd.isna(x)else f"${x:,.2f}"
  def format_date(series):return pd.to_datetime(series,errors="coerce").dt.strftime("%-m/%-d/%Y")

  orders.columns=orders.columns.astype(str).str.strip()
  required_cols=["PO Number","PO Line#"]
  missing=[col for col in required_cols if col not in orders.columns]
  if missing:
    st.error(f"Your {missing} is either missing or in the incorrect format.")
    st.stop()

  orders=orders.sort_values(["PO Number","PO Line#"]).ffill().infer_objects(copy=False)
  orders=orders[orders["PO Line#"].notna()|orders["Qty Ordered"].notna()].copy()
  orders["Qty Ordered"]=pd.to_numeric(orders["Qty Ordered"],errors="coerce")
  orders["Unit Price"]=orders["Unit Price"].replace('[\$,]','',regex=True).astype(float)
  orders["Merch Total"]=orders["Qty Ordered"]*orders["Unit Price"]
  orders["VBU Name"]=pd.to_numeric(orders["Vendor #"],errors="coerce").map(vbu_mapping)
  orders["Unit Price"]=orders["Unit Price"].apply(format_currency)
  orders["Merch Total"]=orders["Merch Total"].apply(format_currency)
  for col in ["PO Date","Requested Delivery Date","Ship Dates"]:
    orders[col]=format_date(orders[col])

  progress.progress(40,text="Merging Shipments...")
  shipments=shipments.rename(columns={"PO #":"PO Number","Buyer Item #":"Buyers Catalog or Stock Keeping #"})
  for col in ["ASN Date","Ship Date"]:
    shipments[col]=format_date(shipments[col])
  shipments=shipments[["PO Number","Buyers Catalog or Stock Keeping #","ASN Date","Ship Date","BOL","SCAC"]]
  orders=orders.merge(shipments,on=["PO Number","Buyers Catalog or Stock Keeping #"],how="left")

  progress.progress(60,text="Merging Invoices...")
  invoices.columns=invoices.columns.astype(str).str.strip()
  invoice_collapsed=(invoices.groupby("Invoice Number",as_index=False)
    .agg({"Retailers PO #":"first","Invoice Date":"first",
          "Discounted Amounted_Discount Amount":"first",
          "Invoice Total":"first"}))
  invoice_collapsed["Invoice Total"]=pd.to_numeric(invoice_collapsed["Invoice Total"].replace('[\$,]','',regex=True),errors="coerce").apply(format_currency)
  invoice_collapsed["Discounted Amounted_Discount Amount"]=pd.to_numeric(invoice_collapsed["Discounted Amounted_Discount Amount"].replace('[\$,]','',regex=True),errors="coerce").apply(format_currency)
  invoice_collapsed["Invoice Date"]=format_date(invoice_collapsed["Invoice Date"])

  orders["PO_clean"]=orders["PO Number"].astype(str).str.strip().str.replace(r"\.0$","",regex=True)
  invoice_collapsed["PO_clean"]=invoice_collapsed["Retailers PO #"].astype(str).str.strip().str.replace(r"\.0$","",regex=True)
  orders=orders.merge(invoice_collapsed[["PO_clean","Invoice Number","Invoice Date","Discounted Amounted_Discount Amount","Invoice Total"]],on="PO_clean",how="left")
  orders.drop(columns="PO_clean",inplace=True)

  progress.progress(80,text="Finalizing output...")
  orders["Fulfillment Status"]="Not Invoiced"
  orders.loc[pd.notna(orders["Invoice Number"]),"Fulfillment Status"]="Invoiced"
  orders.loc[(pd.notna(orders["Ship Date"])&(orders["Fulfillment Status"]=="Not Invoiced")),"Fulfillment Status"]="Shipped Not Invoiced"
  orders["Late Ship"]=pd.to_datetime(orders["Ship Date"],errors="coerce")>pd.to_datetime(orders["Requested Delivery Date"],errors="coerce")
  orders["Late Ship"]=orders["Late Ship"].map({True:"Yes",False:"No"}).fillna("")
  orders["Ship to Name"]=orders["Ship To Name"]

  orders=orders.rename(columns={
    "PO Number":"PO#","Vendor #":"VBU#",
    "Buyers Catalog or Stock Keeping #":"Item#",
    "Vendor Style":"Vendor Item#",
    "Item":"Item Name",
    "Invoice Number":"Invoice#",
    "Discounted Amounted_Discount Amount":"Invoice Disc.",
    "BOL":"BOL#"})

  final_cols=["PO#","PO Date","VBU#","VBU Name","Item#","Vendor Item#","Item Name","Qty Ordered",
              "Unit Price","Merch Total","Ship to Name","Ship To State","Requested Delivery Date",
              "Fulfillment Status","Late Ship","ASN Date","Ship Date","BOL#","SCAC",
              "Invoice#","Invoice Date","Invoice Disc.","Invoice Total"]
  orders=orders.reindex(columns=final_cols).fillna("")

  tz=pytz.timezone("America/New_York")
  timestamp=datetime.now(tz).strftime("%Y-%m-%d_%H%M")
  filename=f"Lowes_Merged_{timestamp}.xlsx"

  output=BytesIO()
  with pd.ExcelWriter(output,engine="xlsxwriter") as writer:
    orders.to_excel(writer,index=False,sheet_name="Orders")
    workbook=writer.book
    worksheet=writer.sheets["Orders"]
    for idx,col in enumerate(orders.columns):
      worksheet.set_column(idx,idx,20)
      header_format=workbook.add_format({"align":"left","bold":True})
      worksheet.write(0,idx,col,header_format)

  file_size_kb=len(output.getvalue())/1024
  st.success("File processed")
  st.caption(f"Approx. file size: {file_size_kb:.1f} KB")
  st.info(f"Total merged rows: {len(orders):,}")
  st.download_button("Download Merged Excel File",data=output.getvalue(),file_name=filename,mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
