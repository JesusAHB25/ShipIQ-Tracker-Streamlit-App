import streamlit as st
import pandas as pd
import numpy as np
import datetime
from pathlib import Path
import io

# 1. PAGE SETUP
st.set_page_config(page_title="ShipIQ Tracker Hub", layout="wide")

st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Processed datasets are displayed below. Update the data by pushing new CSVs to the **Downloads** folder on GitHub.")

# 2. DATA PROCESSING LOGIC (Your exact logic wrapped for Streamlit)
@st.cache_data
def get_datasets():
    # RELATIVE PATHS (Change from G:\ to local folders for GitHub)
    path = Path("Downloads") 
    files = sorted(list(path.glob("*.csv")))
    
    if not files:
        return None, None

    # --- Data Ingestion ---
    all_dfs = []
    for file_path in files:
        df = pd.read_csv(file_path)
        all_dfs.append(df)
    
    # This defines the 'paste' variable you use throughout your logic
    paste = pd.concat(all_dfs, ignore_index=True)

    # --- Data Formatting ---
    date_columns = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]
    for dc in date_columns:
        paste[dc] = pd.to_datetime(paste[dc], errors='coerce')

    # Days Past Pickup Calculation
    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    # Standardize column names
    paste = paste.rename(columns={"Purchase Order Number" : "PO #", "Vendor Name" : "Vendor"})
    paste["PO #"] = paste["PO #"].astype("Int64")

    # --- Report Controls ---
    # NOTE: Upload 'report_controls.xlsx' to the same folder as this script on GitHub
    try:
        report_controls = pd.read_excel("report_controls.xlsx", usecols=["PO # to Track", "What is the PO for?"])
    except Exception:
        st.error("Error: 'report_controls.xlsx' not found in repository root.")
        return None, None

    # Initialize summary structure
    summary = pd.DataFrame(columns = [
        "PO #", "What is the PO for?", "Vendor", "Picked Up", "Past Pickup", "Small Package", 
        "Carrier Accepted, Awaiting Pickup", "Content Review Required", "Routing In Progress", 
        "On Hold for routing", "Cancelled", "PO Status", "Earliest Pickup Date", "Latest Pickup Date", 
        "Earliest In Yard Goal Date", "Latest In Yard Goal Date", "Earliest Final Routing Date", "Latest Final Routing Date"
    ])

    summary["PO #"] = report_controls["PO # to Track"]
    summary["What is the PO for?"] = report_controls["What is the PO for?"]

    # --- Vendor Column Logic ---
    povendor = paste.merge(summary, on = "PO #", how = "left").rename(columns={"Vendor_x" : "Vendor"})
    povendor = povendor[["PO #", "Vendor"]].drop_duplicates("PO #")
    summary["Vendor"] = summary["PO #"].map(povendor.set_index("PO #")["Vendor"]).fillna("NA")

    # --- Status Reference Logic ---
    status_reference = paste.groupby(["PO #", "Status"])["Status"].count().unstack().fillna(0)
    summary = summary.set_index("PO #")
    summary.update(status_reference, join = "left", overwrite = True)
    summary = summary.rename(columns = {"Carrier Accepted, Awaiting Pickup" : "Awaiting Pickup"})
    summary = summary.fillna(0.0).reset_index()

    # --- PO Status Logic ---
    summary["PO Status"] = np.where(summary["PO Status"] != "Cancelled", "Approved", "Cancelled")

    # --- Aggregated Dates Logic ---
    date_aggs = {
        'Earliest Pickup Date': paste.groupby("PO #")["Pickup Date"].min(),
        'Latest Pickup Date': paste.groupby("PO #")["Pickup Date"].max(),
        'Earliest In Yard Goal Date': paste.groupby("PO #")["In Yard Goal Date"].min(),
        'Latest In Yard Goal Date': paste.groupby("PO #")["In Yard Goal Date"].max(),
        'Earliest Final Routing Date': paste.groupby("PO #")["Final Routing Expected By"].min(),
        'Latest Final Routing Date': paste.groupby("PO #")["Final Routing Expected By"].max()
    }

    for col, data in date_aggs.items():
        summary[col] = summary['PO #'].map(data)
        summary[col] = summary[col].apply(lambda x: "" if pd.isna(x) or x == 0 else x)

    return summary, paste

# 3. EXECUTE & RENDER
summary, all_data = get_datasets()

if summary is not None:
    tab1, tab2 = st.tabs(["📊 Summary Table", "🔍 Deeper Dive"])

    with tab1:
        st.subheader("Dataset: Summary")
        st.dataframe(summary, use_container_width=True)

    with tab2:
        st.subheader("PO Specific Details")
        
        # --- DYNAMIC SELECTOR (Option 2) ---
        po_list = summary["PO #"].unique()
        selected_po = st.selectbox("Select a PO # to inspect:", options=po_list)

        # Apply your Deeper Dive filtering logic based on the selection
        dd_filtered = all_data[all_data["PO #"] == selected_po][[
            "Department", "Address", "Shipment ID", "Destination", "Status", 
            "Pickup Date", "In Yard Goal Date", "Final Routing Expected By", 
            "Review By Date", "Last Updated By"
        ]]

        st.dataframe(dd_filtered, use_container_width=True)
        
else:
    st.info("Awaiting data. Please ensure the 'Downloads' folder contains CSVs and 'report_controls.xlsx' is in your GitHub repo.")
