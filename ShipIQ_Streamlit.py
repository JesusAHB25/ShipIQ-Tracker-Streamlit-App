import streamlit as st
import pandas as pd
import numpy as np
import datetime
from pathlib import Path
import io

# 1. PAGE SETUP
st.set_page_config(page_title="ShipIQ Tracker Hub", layout="wide")

st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Your processed datasets are displayed below. Update the data by pushing new CSVs to GitHub.")

# 2. YOUR PANDAS LOGIC (Copied from your .ipynb)
@st.cache_data
def get_datasets():
    # Relative path for GitHub compatibility
    path = Path("Downloads") 
    files = sorted(list(path.glob("*.csv")))
    
    if not files:
        return None, None

    # --- YOUR EXACT FILE LOOP ---
    df_list = [pd.read_csv(f) for f in files]
    main_df = pd.concat(df_list, ignore_index=True)

    # --- YOUR EXACT TRANSFORMATIONS (Cels 3, 4, 5) ---
    # --- Data Formatting ---
# Define which columns need to be treated as dates
date_columns = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]

# Convert strings/objects to datetime objects for calculation
for dc in date_columns:
    paste[f"{dc}"] = pd.to_datetime(paste[f"{dc}"])

# --- Days Past Pickup Date Calculation ---
# Calculate the delta between today and the scheduled pickup
days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
# Only apply the day count if the Status is specifically "Past Pickup", otherwise set as NaN string
paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, f"{np.nan}")

# Standardize column names for easier referencing
paste = paste.rename(columns={"Purchase Order Number" : "PO #", "Vendor Name" : "Vendor"})

# Cast PO # to nullable Integer type to prevent decimal points (.0)
paste["PO #"] = paste["PO #"].astype("Int64")

# Initialize the empty summary dataframe with the required reporting structure
summary = pd.DataFrame(columns = ["PO #", "Notes", "Vendor", "Picked Up", "Past Pickup", "Small Package", "Carrier Accepted, Awaiting Pickup", "Content Review Required", "Routing In Progress", "On Hold for routing", "Cancelled", 
                        "PO Status", "Earliest Pickup Date", "Latest Pickup Date", "Earliest In Yard Goal Date", "Latest In Yard Goal Date", "Earliest Final Routing Date", "Latest Final Routing Date"])

# --- Report Controls for the Automation ---
# Load the master tracking list (external Excel) to define which POs we actually care about
report_controls = pd.read_excel("G:\\Shared drives\\VG x Forklift Shared\\VG - Reporting\\Jesus' Folder\\ShipIQ Report\\report_controls.xlsx", usecols=["PO # to Track", "What is the PO for?"])

# Map the tracking list into our summary structure
summary["PO #"] = report_controls["PO # to Track"]
summary["Notes"] = report_controls["What is the PO for?"]
summary = summary.rename(columns={"Notes" : "What is the PO for?"})

# --- Vendor Column Logic ---
# Join raw data with summary to associate Vendors with PO numbers
povendor = paste.merge(summary, on = "PO #", how = "left").rename(columns={"Vendor_x" : "Vendor"})

# Remove duplicates to ensure we have a 1:1 PO-to-Vendor mapping
povendor = povendor[["PO #", "Vendor"]].drop_duplicates("PO #")
# Fill the Vendor column in summary using the mapped index, defaulting to "NA" if not found
summary["Vendor"] = summary["PO #"].map(povendor.set_index("PO #")["Vendor"]).fillna("NA")

# --- Status Reference Logic ---
# Create a pivot-style table counting occurrences of each status per PO
status_reference = paste.groupby(["PO #", "Status"])["Status"].count().unstack().fillna(0)

# Update the summary dataframe with these counts (e.g., how many "Past Pickups" per PO)
summary = summary.set_index("PO #")
summary.update(status_reference, join = "left", overwrite = True)
summary = summary.rename(columns = {"Carrier Accepted, Awaiting Pickup" : "Awaiting Pickup"})
summary = summary.fillna(0.0)
summary = summary.reset_index()

# --- PO Status Logic ---
# Identify overall PO health; defaults to "Approved" unless explicitly "Cancelled"
poreference = paste.merge(summary, on = "PO #", how = "left")
summary["PO Status"] = np.where(summary["PO Status"] != "Cancelled", "Approved", "Cancelled")

# --- Earliest Pickup Date ---
# Aggregate raw data to find the first pickup date associated with the PO
min_values_pdate = paste.groupby("PO #")["Pickup Date"].min()
summary['Earliest Pickup Date'] = summary['PO #'].map(min_values_pdate)
# Clean up empty/zero dates for the final report display
summary['Earliest Pickup Date'] = summary['Earliest Pickup Date'].apply(lambda x: "" if x == 0 or pd.isna(x) else x)

# --- Latest Pickup Date ---
# Aggregate raw data to find the furthest pickup date associated with the PO
max_values_pdate = paste.groupby("PO #")["Pickup Date"].max()
summary["Latest Pickup Date"] = summary["PO #"].map(max_values_pdate)
summary["Latest Pickup Date"] = summary["Latest Pickup Date"].apply(lambda x : "" if x == 0 or pd.isna(x) else x)

# --- Earliest In Yard Goal Date ---
# Calculate the soonest "In Yard" goal for the PO group
min_values_ydate = paste.groupby("PO #")["In Yard Goal Date"].min()
summary['Earliest In Yard Goal Date'] = summary['PO #'].map(min_values_ydate)
summary['Earliest In Yard Goal Date'] = summary['Earliest In Yard Goal Date'].apply(lambda x: "" if x == 0 or pd.isna(x) else x)

# --- Latest In Yard Goal Date ---
# Calculate the latest "In Yard" goal for the PO group
max_values_ydate = paste.groupby("PO #")["In Yard Goal Date"].max()
summary['Latest In Yard Goal Date'] = summary['PO #'].map(max_values_ydate)
summary['Latest In Yard Goal Date'] = summary['Latest In Yard Goal Date'].apply(lambda x: "" if x == 0 or pd.isna(x) else x)

# --- Earliest Final Routing Date ---
# Logic: Find earliest routing date (Note: Currently mapping Pickup Date variable)
min_values_fdate = paste.groupby("PO #")["Final Routing Expected By"].min()
summary['Earliest Final Routing Date'] = summary['PO #'].map(min_values_fdate)
summary['Earliest Final Routing Date'] = summary['Earliest Final Routing Date'].apply(lambda x: "" if x == 0 or pd.isna(x) else x)

# --- Latest Final Routing Date ---
# Logic: Find latest routing date (Note: Currently mapping Pickup Date variable)
max_values_fdate = paste.groupby("PO #")["Final Routing Expected By"].max()
summary['Latest Final Routing Date'] = summary['PO #'].map(max_values_pdate)
summary['Latest Final Routing Date'] = summary['Latest Final Routing Date'].apply(lambda x: "" if x == 0 or pd.isna(x) else x)

dd_report_controls = pd.DataFrame(columns=["PO #", "Vendor"])

deeper_dive = pd.DataFrame(columns =
                           ["Department",	"Address", "Shipment ID",	"Destination",	"Status",	"Pickup Date",	
                            "In Yard Goal Date",	"Final Routing", "Final Routing Expected By",	"Review By Date", "Last Updated By", "Days from Pickup"]
                            )

dd_report_controls["PO #"] = [10001768577]
dd_reference = paste.merge(dd_report_controls, on="PO #", how="right").rename(columns={"Vendor_x" : "Vendor"})
dd_report_controls["Vendor"] = dd_reference["Vendor"]

dd_reference = dd_reference.drop(columns=["Vendor_y"]).set_index("PO #")

dd_reference

deeper_dive = dd_reference[["Department",	"Address", "Shipment ID",	"Destination",	"Status",	"Pickup Date",	
                            "In Yard Goal Date", "Final Routing Expected By",	"Review By Date", "Last Updated By"]]
    
    return summary, deeper_dive

# Execute logic
summary, deeper_dive = get_datasets()

# 3. PUBLISH DATASETS TO STREAMLIT
if summary is not None:
    # Navigation tabs for the team
    tab1, tab2 = st.tabs(["📊 Summary Table", "🔍 Deeper Dive"])

    with tab1:
        st.subheader("Dataset: Summary")
        # This publishes your dataframe interactively
        st.dataframe(summary, use_container_width=True)

    with tab2:
        st.subheader("Dataset: Deeper Dive")
        st.dataframe(deeper_dive, use_container_width=True)
        
else:
    st.error("No CSV files found in the 'Downloads' folder. Please upload data to GitHub.")
