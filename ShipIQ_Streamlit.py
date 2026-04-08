import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import io
import base64
import requests

# ---------------------------------------------------------------------------
# 1. PAGE SETUP
# ---------------------------------------------------------------------------
st.set_page_config(page_title="ShipIQ Tracker Hub", layout="wide")
st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Processed datasets are displayed below. Update the data by pushing new CSVs to the **Downloads** folder on GitHub.")

# ---------------------------------------------------------------------------
# 2. GITHUB CONFIG — set these in Streamlit Cloud > Secrets
# ---------------------------------------------------------------------------
GITHUB_TOKEN  = st.secrets["GITHUB_TOKEN"]
GITHUB_REPO   = st.secrets["GITHUB_REPO"]   # e.g. "username/shipiq-tracker-streamlit-app"
RC_FILE_PATH  = "report_controls_data.csv"   # path inside the repo
GITHUB_BRANCH = "main"

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{RC_FILE_PATH}"

# ---------------------------------------------------------------------------
# 3. GITHUB READ / WRITE HELPERS
# ---------------------------------------------------------------------------
def load_rc_from_github():
    response = requests.get(API_URL, headers=HEADERS, params={"ref": GITHUB_BRANCH})
    if response.status_code == 200:
        content = base64.b64decode(response.json()["content"]).decode("utf-8")
        sha = response.json()["sha"]
        try:
            df = pd.read_csv(io.StringIO(content))
            if df.empty or "PO # to Track" not in df.columns:
                raise ValueError("Empty or malformed file")
            df["PO # to Track"] = df["PO # to Track"].astype("Int64")
            return df, sha
        except Exception:
            return pd.DataFrame({
                "PO # to Track": pd.array([], dtype="Int64"),
                "What is the PO for?": pd.Series([], dtype="str")
            }), sha  # keep sha so we can overwrite the file correctly
    return pd.DataFrame({
        "PO # to Track": pd.array([], dtype="Int64"),
        "What is the PO for?": pd.Series([], dtype="str")
    }), None

def save_rc_to_github(df, sha):
    csv_content = df.to_csv(index=False)
    encoded = base64.b64encode(csv_content.encode("utf-8")).decode("utf-8")
    payload = {
        "message": "chore: update report_controls_data.csv",
        "content": encoded,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha  # required when updating an existing file
    response = requests.put(API_URL, headers=HEADERS, json=payload)
    if response.status_code not in (200, 201):
        st.error(f"Failed to save to GitHub: {response.json().get('message', 'Unknown error')}")
        return None
    return response.json()["content"]["sha"]  # return new sha

# ---------------------------------------------------------------------------
# 4. REPORT CONTROLS STATE
# ---------------------------------------------------------------------------
if "report_controls" not in st.session_state:
    df, sha = load_rc_from_github()
    st.session_state.report_controls = df
    st.session_state.rc_sha = sha

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
            lambda x: "" if pd.isna(x) or x == 0 else pd.Timestamp(x).strftime("%m/%d/%Y")
        )

    return summary, paste.assign(**{
        col: paste[col].dt.strftime("%m/%d/%Y") for col in date_columns
    })


# ---------------------------------------------------------------------------
# 4. RENDER
# ---------------------------------------------------------------------------
tab0, tab1, tab2 = st.tabs(["⚙️ Report Controls", "📊 Summary Table", "🔍 Deeper Dive"])

with tab0:
    st.subheader("Report Controls")
    st.markdown("Add the PO numbers you want to track and an optional description for each.")

    # Debug info — remove once confirmed working
    with st.expander("🔧 Debug Info"):
        st.write("SHA:", st.session_state.get("rc_sha"))
        test = requests.get(API_URL, headers=HEADERS, params={"ref": GITHUB_BRANCH})
        st.write("GitHub API status:", test.status_code)
        st.write("GitHub API response:", test.json())

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

    col_apply, col_reset = st.columns([1, 1])

    with col_apply:
        if st.button("✅ Apply & Refresh", type="primary", use_container_width=True):
            edited["PO # to Track"] = pd.array(edited["PO # to Track"].dropna().astype(int), dtype="Int64")
            st.session_state.report_controls = edited
            new_sha = save_rc_to_github(edited, st.session_state.rc_sha)
            if new_sha:
                st.session_state.rc_sha = new_sha
            st.cache_data.clear()
            st.rerun()

    with col_reset:
        if st.button("🗑️ Reset", type="secondary", use_container_width=True):
            empty = pd.DataFrame({
                "PO # to Track": pd.array([], dtype="Int64"),
                "What is the PO for?": pd.Series([], dtype="str")
            })
            st.session_state.report_controls = empty
            new_sha = save_rc_to_github(empty, st.session_state.rc_sha)
            if new_sha:
                st.session_state.rc_sha = new_sha
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
