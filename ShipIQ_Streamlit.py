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

# 2. DATA PROCESSING LOGIC
@st.cache_data
def get_datasets():
    # Use relative paths for GitHub/Cloud compatibility
    path = Path("Downloads") 
    files = sorted(list(path.glob("*.csv")))
    
    if not files:
        return None, None

    # --- LOAD RAW DATA ---
    df_list = [pd.read_csv(f) for f in files]
    paste = pd.concat(df_list, ignore_index=True)

    # --- Data Formatting ---
    date_columns = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]
    for dc in date_columns:
        paste[dc] = pd.to_datetime(paste[dc], errors='coerce')

    # Days Past Pickup Calculation
    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    # Standardize column names
    paste = paste.rename(columns={"Purchase Order Number" : "PO #", "Vendor Name" : "Vendor"})
    paste["PO #"] = pd.to_numeric(paste["PO #"], errors='coerce').astype("Int64")

    # --- Report Controls (External Excel on GitHub) ---
    try:
        report_controls = pd.read_excel("report_controls.xlsx", usecols=["PO # to Track", "What is the PO for?"])
    except Exception:
        st.error("Error: 'report_controls.xlsx' not found in the root folder.")
        return None, None

    # --- Build Summary Table ---
    summary = pd.DataFrame()
    summary["PO #"] = report_controls["PO # to Track"]
    summary["What is the PO for?"] = report_controls["What is the PO for?"]

    # Map Vendors
    povendor = paste[["PO #", "Vendor"]].drop_duplicates("PO #")
    summary = summary.merge(povendor, on="PO #", how="left")
    summary["Vendor"] = summary["Vendor"].fillna("NA")

    # Status Reference Logic
    status_ref = paste.groupby(["PO #", "Status"]).
