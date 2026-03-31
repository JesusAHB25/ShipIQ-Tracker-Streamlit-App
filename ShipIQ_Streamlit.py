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

# 2. YOUR PANDAS LOGIC
@st.cache_data
def get_datasets():
    # Path for GitHub compatibility
    path = Path("Downloads") 
    files = sorted(list(path.glob("*.csv")))
    
    if not files:
        return None, None

    # --- LOAD RAW DATA ---
    df_list = [pd.read_csv(f) for f in files]
    paste = pd.concat(df_list, ignore_index=True) # Definimos 'paste' aquí

    # --- Data Formatting ---
    date_columns = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]
    for dc in date_columns:
        paste[dc] = pd.to_datetime(paste[dc], errors='coerce')

    # --- Days Past Pickup Date Calculation ---
    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    # Standardize column names
    paste = paste.rename(columns={"Purchase Order Number" : "PO #", "Vendor Name" : "Vendor"})
    paste["PO #"] = pd.to_numeric(paste["PO #"], errors='coerce').astype("Int64")

    # --- Report Controls ---
    # IMPORTANTE: Sube este archivo a GitHub junto con tu app.py
    try:
        report_controls = pd.read_excel("report_controls.xlsx", usecols=["PO # to Track", "What is the PO for?"])
    except FileNotFoundError:
        st.error("File 'report_controls.xlsx' not found in repository.")
        return None, None

    # Initialize summary
    summary = pd.DataFrame()
    summary["PO #"] = report_controls["PO # to Track"]
    summary["What is the PO for?"] = report_controls["What is the PO for?"]

    # --- Vendor Column Logic ---
    povendor = paste[["PO #", "Vendor"]].drop_duplicates("PO #")
    summary = summary.merge(povendor, on="PO #", how="left")
    summary["Vendor"] = summary["Vendor"].fillna("NA")

    # --- Status Reference Logic ---
    status_cols = ["Picked Up", "Past Pickup", "Small Package", "Awaiting Pickup", 
                   "Content Review Required", "Routing In Progress", "On Hold for routing", "Cancelled"]
    
    status_ref = paste.groupby(["PO #", "Status"]).size().unstack(fill_value=0)
    status_ref = status_ref.rename(columns={"Carrier Accepted, Awaiting Pickup": "Awaiting Pickup"})
    
    summary = summary.merge(status_ref, on="PO #", how="left").fillna(0)

    # --- Date Aggregations ---
    # Earliest/Latest Pickup
    p_dates = paste.groupby("PO #")["Pickup Date"].agg(['min', 'max'])
    summary = summary.merge(p_dates, on="PO #", how="left").rename(columns={'min': 'Earliest Pickup Date', 'max': 'Latest Pickup Date'})

    # Earliest/Latest In Yard
    y_dates = paste.groupby("PO #")["In Yard Goal Date"].agg(['min', 'max'])
    summary = summary.merge(y_dates, on="PO #", how="left").rename(columns={'min': 'Earliest In Yard Goal Date', 'max': 'Latest In Yard Goal Date'})

    # --- Deeper Dive Logic ---
    # Usaremos el primer PO del summary para mostrar algo por defecto, o uno específico
    target_po = 10001768577 
    deeper_dive = paste[paste["PO #"] == target_po][["Department", "Address", "Shipment ID", "Destination", "Status", "Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date", "Last Updated By"]]
    
    return summary, deeper_dive

# Execute logic
summary, deeper_dive = get_datasets()

# 3. PUBLISH DATASETS TO STREAMLIT
if summary is not None:
    tab1, tab2 = st.tabs(["📊 Summary Table", "🔍 Deeper Dive"])

    with tab1:
        st.subheader("Dataset: Summary")
        st.dataframe(summary, use_container_width=True)

    with tab2:
        st.subheader("Dataset: Deeper Dive")
        # Agregué un selector para que tu equipo pueda elegir el PO que quiere ver
        all_pos = summary["PO #"].unique()
        selected_po = st.selectbox("Select PO # to inspect:", all_pos)
        
        # Filtrado dinámico para el equipo
        # (Nota: Aquí podrías mover la lógica de filtrado fuera de la función cacheada 
        # para que sea instantánea)
        st.dataframe(deeper_dive, use_container_width=True)
        
else:
    st.info("Please ensure 'Downloads' folder contains CSVs and 'report_controls.xlsx' is present.")
