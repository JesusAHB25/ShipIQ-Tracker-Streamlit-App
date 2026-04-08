import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import io

# ---------------------------------------------------------------------------
# 1. PAGE SETUP
# ---------------------------------------------------------------------------
st.set_page_config(page_title="ShipIQ Tracker Hub", layout="wide")
st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Processed datasets are displayed below. Update the data by pushing new CSVs to the **Downloads** folder on GitHub.")

# ---------------------------------------------------------------------------
# 2. REPORT CONTROLS STATE
# ---------------------------------------------------------------------------
if "report_controls" not in st.session_state:
    st.session_state.report_controls = pd.DataFrame({
        "PO # to Track": pd.array([], dtype="Int64"),
        "What is the PO for?": pd.Series([], dtype="str")
    })

# ---------------------------------------------------------------------------
# 3. DATA PROCESSING
# ---------------------------------------------------------------------------
@st.cache_data
def get_datasets(report_controls_json):

    report_controls = pd.read_json(io.StringIO(report_controls_json))
    if report_controls.empty:
        return None, None

    report_controls["PO # to Track"] = report_controls["PO # to Track"].astype("Int64")

    # --- File Discovery ---
    path = Path("Downloads")
    files = sorted(path.glob("*.csv"))
    if not files:
        return None, None

    # --- Data Ingestion & Consolidation ---
    paste = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    # --- Date Formatting ---
    date_columns = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]
    for col in date_columns:
        paste[col] = pd.to_datetime(paste[col], errors="coerce")

    # --- Days Past Pickup ---
    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    # --- Standardize Column Names ---
    paste = paste.rename(columns={"Purchase Order Number": "PO #", "Vendor Name": "Vendor"})
    paste["PO #"] = paste["PO #"].astype("Int64")

    # --- Build Summary Structure ---
    summary = pd.DataFrame({
        "PO #": report_controls["PO # to Track"].astype("Int64"),
        "What is the PO for?": report_controls["What is the PO for?"]
    })

    # --- Vendor Mapping ---
    povendor = (
        paste[["PO #", "Vendor"]]
        .drop_duplicates("PO #")
        .set_index("PO #")["Vendor"]
    )
    summary["Vendor"] = summary["PO #"].map(povendor).fillna("NA")

    # --- Status Counts (pivot-style) ---
    status_reference = (
        paste.groupby(["PO #", "Status"])["Status"]
        .count()
        .unstack(fill_value=0)
    )
    summary = summary.set_index("PO #")
    summary.update(status_reference, join="left", overwrite=True)
    summary = (
        summary
        .rename(columns={"Carrier Accepted, Awaiting Pickup": "Awaiting Pickup"})
        .fillna(0.0)
        .reset_index()
    )

    # --- PO Status ---
    cancelled_pos = paste[paste["Status"] == "Cancelled"]["PO #"].unique()
    summary["PO Status"] = np.where(summary["PO #"].isin(cancelled_pos), "Cancelled", "Approved")

    # --- Aggregated Dates ---
    date_agg_map = {
        "Earliest Pickup Date":         ("Pickup Date",               "min"),
        "Latest Pickup Date":           ("Pickup Date",               "max"),
        "Earliest In Yard Goal Date":   ("In Yard Goal Date",         "min"),
        "Latest In Yard Goal Date":     ("In Yard Goal Date",         "max"),
        "Earliest Final Routing Date":  ("Final Routing Expected By", "min"),
        "Latest Final Routing Date":    ("Final Routing Expected By", "max"),
    }

    for summary_col, (source_col, agg_fn) in date_agg_map.items():
        agg = getattr(paste.groupby("PO #")[source_col], agg_fn)()
        summary[summary_col] = summary["PO #"].map(agg)
        summary[summary_col] = summary[summary_col].apply(
            lambda x: "" if pd.isna(x) or x == 0 else x
        )

    return summary, paste


# ---------------------------------------------------------------------------
# 4. RENDER
# ---------------------------------------------------------------------------
tab0, tab1, tab2 = st.tabs(["⚙️ Report Controls", "📊 Summary Table", "🔍 Deeper Dive"])

with tab0:
    st.subheader("Report Controls")
    st.markdown("Add the PO numbers you want to track and an optional description for each.")

    edited = st.data_editor(
        st.session_state.report_controls,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "PO # to Track": st.column_config.NumberColumn(
                "PO # to Track",
                help="Enter the full PO number",
                format="%d",
                required=True
            ),
            "What is the PO for?": st.column_config.TextColumn(
                "What is the PO for?",
                help="Short description of the PO"
            )
        }
    )

    if st.button("✅ Apply & Refresh", type="primary"):
        edited["PO # to Track"] = pd.array(edited["PO # to Track"].dropna().astype(int), dtype="Int64")
        st.session_state.report_controls = edited
        st.cache_data.clear()
        st.rerun()

summary, all_data = get_datasets(st.session_state.report_controls.to_json())

if summary is not None:
    with tab1:
        st.subheader("Dataset: Summary")
        st.dataframe(summary, use_container_width=True)

    with tab2:
        st.subheader("PO Specific Details")

        selected_po = st.selectbox(
            "Select a PO # to inspect:",
            options=summary["PO #"].unique()
        )

        dd_report_controls = pd.DataFrame({
            "PO #": pd.array([selected_po], dtype="Int64"),
            "Vendor": [None]
        })

        dd_reference = (
            all_data
            .merge(dd_report_controls[["PO #"]], on="PO #", how="right")
            .rename(columns={"Vendor_x": "Vendor"})
            .drop(columns=["Vendor_y"], errors="ignore")
            .set_index("PO #")
        )

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
    with tab1:
        st.info("No data to display. Please add PO numbers in the Report Controls tab and ensure CSVs are in the Downloads folder.")
    with tab2:
        st.info("No data to display. Please add PO numbers in the Report Controls tab and ensure CSVs are in the Downloads folder.")
