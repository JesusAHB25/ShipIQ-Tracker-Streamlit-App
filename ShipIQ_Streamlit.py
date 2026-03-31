import streamlit as st
import pandas as pd
import numpy as np
import datetime
import xlsxwriter
import io

# --- 1. APP CONFIG & INTERFACE ---
st.set_page_config(page_title="ShipIQ Tracker Automation", layout="wide")
st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Upload your ShipIQ CSV downloads to generate the formatted Tracker.")

# --- 2. FILE UPLOADER ---
uploaded_files = st.file_uploader("Choose ShipIQ CSV files", accept_multiple_files=True, type=['csv'])

if uploaded_files:
    # Combine uploaded files into one DataFrame (Your original File Loop logic)
    df_list = [pd.read_csv(file) for file in uploaded_files]
    raw_data = pd.concat(df_list, ignore_index=True)
    
    # --- 3. YOUR DATA LOGIC (Summary & Deeper Dive) ---
    # (Simplified example based on your notebook logic)
    raw_data["PO #"] = pd.to_numeric(raw_data["Purchase Order Number"], errors='coerce')
    
    # Create your Summary table
    summary = raw_data.groupby("PO #").agg({
        "Vendor Name": "first",
        "Status": "count"
    }).reset_index().rename(columns={"Vendor Name": "Vendor", "Status": "Total Shipments"})
    
    # Let the user pick a PO for the Deeper Dive
    selected_po = st.selectbox("Select a PO for the Deeper Dive report:", summary["PO #"].unique())
    deeper_dive = raw_data[raw_data["PO #"] == selected_po].copy()

    # Display previews on the website
    st.subheader("Summary Preview")
    st.dataframe(summary.head())

    # --- 4. FORMATTED EXCEL GENERATION (Your fixed Cell 6) ---
    output = io.BytesIO()
    # Note the 'nan_inf_to_errors' fix we added earlier
    writer = pd.ExcelWriter(output, engine='xlsxwriter', engine_kwargs={'options': {'nan_inf_to_errors': True}})
    workbook = writer.book

    # [Styles go here - fmt_header, fmt_date, etc. same as your notebook]
    fmt_header = workbook.add_format({'bold': True, 'bg_color': '#4F81BD', 'font_color': 'white', 'border': 1, 'align': 'center'})
    fmt_date = workbook.add_format({'num_format': 'mm/dd/yyyy', 'border': 1, 'align': 'center'})
    fmt_border = workbook.add_format({'border': 1})
    fmt_num = workbook.add_format({'border': 1, 'align': 'center'})
    fmt_label = workbook.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1})

    # --- WRITE SUMMARY SHEET ---
    summary_sheet = workbook.add_worksheet('Summary')
    for col_num, value in enumerate(summary.columns):
        summary_sheet.write(0, col_num, value, fmt_header)
    
    for row_idx, row_data in enumerate(summary.values):
        for col_idx, value in enumerate(row_data):
            # Your exact logic check for dates and numbers
            if pd.notnull(value) and isinstance(value, (int, float)) and not isinstance(value, bool):
                summary_sheet.write(row_idx+1, col_idx, value, fmt_num)
            else:
                summary_sheet.write(row_idx+1, col_idx, value if pd.notnull(value) else "", fmt_border)

    # --- WRITE DEEPER DIVE SHEET ---
    dive_sheet = workbook.add_worksheet('Deeper Dive')
    dive_sheet.write('A1', 'PO#', fmt_label)
    dive_sheet.write('B1', str(selected_po), fmt_border)
    # [Rest of your deeper dive writing logic here]

    writer.close()
    processed_data = output.getvalue()

    # --- 5. DOWNLOAD BUTTON ---
    st.divider()
    st.download_button(
        label="📥 Download Formatted Excel Tracker",
        data=processed_data,
        file_name=f"ShipIQ_Tracker_{datetime.date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Please upload CSV files to begin.")