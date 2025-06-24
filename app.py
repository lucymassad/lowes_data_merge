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

  vendor_item_map={
    "4983612":"B8110200","4983613":"B8110300","5113267":"B8110100","335456":"B1195080",
    "552696":"B1195020","5516714":"B8100731","5516716":"B8100732","5516715":"B8100733",
    "71894":"B1246160","72931":"B1224080","92951":"B1224060","97086":"B1260060",
    "97809":"B1201460","167411":"B1237440","552704":"B1195010","91900":"B1202360",
    "961539":"B1258800","552697":"B1195040","71918":"B1246150","94833":"B1260080",
    "552706":"B1195050","72801":"B1202380","101760":"B2063400","120019":"B1200190",
    "1240180":"B1242620","91299":"B1202390","4350231":"B4973300","876249":"B4905700",
    "758650":"B4905600","243942":"B4904900","335457":"B2241150","147992":"B1288200",
    "148054":"B1298200"}

  item_type_map={
    "4983612":"Commodity","4983613":"Commodity","5113267":"Commodity","335456":"Sunniland",
    "552696":"Sunniland","5516714":"Soil","5516716":"Soil","5516715":"Soil","71894":"Sunniland",
    "72931":"Sunniland","92951":"Sunniland","97086":"Sunniland","97809":"Sunniland",
    "167411":"Sunniland","552704":"Sunniland","91900":"Sunniland","961539":"Sunniland",
    "552697":"Sunniland","71918":"Sunniland","94833":"Sunniland","552706":"Sunniland",
    "72801":"Sunniland","101760":"Ice Melt","1053900":"Ice Melt","120019":"Sunniland",
    "1240180":"Sunniland","91299":"Sunniland","335457":"Sunniland","147992":"Sunniland",
    "148054":"Sunniland"}

  def format_currency(x):return""if pd.isna(x)else f"${x:,.2f}"
  def format_date(series):return pd.to_datetime(series,errors="coerce").dt.strftime("%-m/%-d/%Y")

  orders.columns=orders.columns.astype(str).str.strip()
  required_cols=["PO Number","PO Line#"]
  missing=[col for col in required_cols if col not in orders.columns]
  if missing:
    st.error(f"Your {missing} is either missing or in the incorrect format.")
    st.stop()

  orders=orders.sort_values("PO Date",ascending=False)
  orders=orders.sort_values(["PO Number","PO Line#"]).ffill().infer_objects(copy=False)
  orders=orders[orders["PO Line#"].notna()|orders["Qty Ordered"].notna()].copy()
  orders["Qty Ordered"]=pd.to_numeric(orders["Qty Ordered"],errors="coerce")
  orders["Unit Price"]=orders["Unit Price"].replace('[\$,]','',regex=True).astype(float)
  orders["VBU Name"]=pd.to_numeric(orders["Vendor #"],errors="coerce").map(vbu_mapping)
  orders["Vendor Item#"]=orders["Buyers Catalog or Stock Keeping #"].map(vendor_item_map)
  orders["Item Type"]=orders["Buyers Catalog or Stock Keeping #"].map(item_type_map)
  orders["Unit Price"]=orders["Unit Price"].apply(format_currency)
  for col in ["PO Date","Requested Delivery Date","Ship Dates"]:
    orders[col]=format_date(orders[col])

  orders["Ship To Code"] = orders["Ship To Location"]

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
    "Item":"Item Name",
    "Invoice Number":"Invoice#",
    "Discounted Amounted_Discount Amount":"Invoice Disc.",
    "BOL":"BOL#"})

  orders["Month Filter"] = pd.to_datetime(orders["PO Date"],errors="coerce").dt.month
  orders["Year Filter"] = pd.to_datetime(orders["PO Date"],errors="coerce").dt.year
  orders["Quarter Filter"] = pd.to_datetime(orders["PO Date"],errors="coerce").dt.quarter.map(lambda x: f"Q{x}" if pd.notna(x) else "")

  final_cols=["PO#","PO Date","VBU#","VBU Name","Item#","Vendor Item#","Item Name","Item Type","Qty Ordered",
              "PO Line#","Ship To Code","Ship to Name","Ship To State","Requested Delivery Date",
              "Fulfillment Status","Late Ship","ASN Date","Ship Date","BOL#","SCAC",
              "Invoice#","Invoice Date","Invoice Disc.","Invoice Total",
              "Month Filter","Year Filter","Quarter Filter"]
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
