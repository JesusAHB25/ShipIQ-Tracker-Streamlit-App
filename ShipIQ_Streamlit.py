import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# 1. PAGE SETUP
st.set_page_config(page_title="ShipIQ Tracker Hub", layout="wide")

st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Processed datasets are displayed below. Update the data by pushing new CSVs to the **Downloads** folder on GitHub.")

# 2. DATA PROCESSING LOGIC
@st.cache_data
def get_datasets():
    path = Path("Downloads")
    files = sorted(list(path.glob("*.csv")))

    if not files:
        return None, None

    # --- Data Ingestion ---
    all_dfs = []
    for file_path in files:
        df = pd.read_csv(file_path)
        all_dfs.append(df)

    paste = pd.concat(all_dfs, ignore_index=True)

    # --- Data Formatting ---
    date_columns = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]
    for dc in date_columns:
        paste[dc] = pd.to_datetime(paste[dc], errors='coerce')

    # Days Past Pickup Calculation
    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    # Standardize column names
    paste = paste.rename(columns={"Purchase Order Number": "PO #", "Vendor Name": "Vendor"})

    # FIX 1: Castear PO # a Int64 en paste también, no solo en summary
    paste["PO #"] = paste["PO #"].astype("Int64")

    # --- Report Controls ---
    try:
        report_controls = pd.read_excel("report_controls.xlsx", usecols=["PO # to Track", "What is the PO for?"])
    except Exception:
        st.error("Error: 'report_controls.xlsx' not found in repository root.")
        return None, None

    # FIX 2: Castear PO # en report_controls también para consistencia
    report_controls["PO # to Track"] = report_controls["PO # to Track"].astype("Int64")

    # Initialize summary structure
    summary = pd.DataFrame(columns=[
        "PO #", "What is the PO for?", "Vendor", "Picked Up", "Past Pickup", "Small Package",
        "Carrier Accepted, Awaiting Pickup", "Content Review Required", "Routing In Progress",
        "On Hold for routing", "Cancelled", "PO Status", "Earliest Pickup Date", "Latest Pickup Date",
        "Earliest In Yard Goal Date", "Latest In Yard Goal Date", "Earliest Final Routing Date", "Latest Final Routing Date"
    ])

    summary["PO #"] = report_controls["PO # to Track"]
    summary["What is the PO for?"] = report_controls["What is the PO for?"]

    # FIX 3: Asegurar tipo Int64 en summary["PO #"] explícitamente
    summary["PO #"] = summary["PO #"].astype("Int64")

    # --- Vendor Column Logic ---
    povendor = paste.merge(summary, on="PO #", how="left").rename(columns={"Vendor_x": "Vendor"})
    povendor = povendor[["PO #", "Vendor"]].drop_duplicates("PO #")
    summary["Vendor"] = summary["PO #"].map(povendor.set_index("PO #")["Vendor"]).fillna("NA")

    # --- Status Reference Logic ---
    status_reference = paste.groupby(["PO #", "Status"])["Status"].count().unstack().fillna(0)
    summary = summary.set_index("PO #")
    summary.update(status_reference, join="left", overwrite=True)
    summary = summary.rename(columns={"Carrier Accepted, Awaiting Pickup": "Awaiting Pickup"})
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

        po_list = summary["PO #"].unique()
        selected_po = st.selectbox("Select a PO # to inspect:", options=po_list)

        # Lógica original del Deeper Dive, adaptada dinámicamente al PO seleccionado
        dd_report_controls = pd.DataFrame(columns=["PO #", "Vendor"])
        dd_report_controls["PO #"] = pd.array([selected_po], dtype="Int64")

        dd_reference = all_data.merge(dd_report_controls, on="PO #", how="right").rename(columns={"Vendor_x": "Vendor"})
        dd_report_controls["Vendor"] = dd_reference["Vendor"].values

        dd_reference = dd_reference.drop(columns=["Vendor_y"]).set_index("PO #")

        deeper_dive = dd_reference[[
            "Department", "Address", "Shipment ID", "Destination", "Status",
            "Pickup Date", "In Yard Goal Date", "Final Routing Expected By",
            "Review By Date", "Last Updated By"
        ]]

        if deeper_dive.empty:
            st.warning(f"No records found for PO # {selected_po}. Verify this PO exists in the uploaded CSVs.")
        else:
            st.dataframe(deeper_dive, use_container_width=True)

else:
    st.info("Awaiting data. Please ensure the 'Downloads' folder contains CSVs and 'report_controls.xlsx' is in your GitHub repo.")
