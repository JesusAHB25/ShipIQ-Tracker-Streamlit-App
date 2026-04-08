import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import io
import base64
import requests

# =============================================================================
# CONFIGURATION
# =============================================================================
st.set_page_config(page_title="ShipIQ Tracker Hub", layout="wide")

GITHUB_TOKEN  = st.secrets["GITHUB_TOKEN"]
GITHUB_REPO   = st.secrets["GITHUB_REPO"]
RC_FILE_PATH  = "report_controls_data.csv"
GITHUB_BRANCH = "main"
API_URL       = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{RC_FILE_PATH}"
GH_HEADERS    = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

DATE_COLS     = ["Pickup Date", "In Yard Goal Date", "Final Routing Expected By", "Review By Date"]
STATUS_COLS   = ["Picked Up", "Past Pickup", "Small Package", "Awaiting Pickup",
                 "Content Review Required", "Routing In Progress", "On Hold for routing", "Cancelled"]
SUMMARY_ORDER = ["PO #", "What is the PO for?", "Expiration Date"] + STATUS_COLS + [
                 "PO Status", "Earliest Pickup Date", "Latest Pickup Date",
                 "Earliest In Yard Goal Date", "Latest In Yard Goal Date",
                 "Earliest Final Routing Date", "Latest Final Routing Date"]
DEEPDIVE_COLS = ["Department", "Address", "Shipment ID", "Destination", "Status",
                 "Pickup Date", "In Yard Goal Date", "Final Routing Expected By",
                 "Review By Date", "Last Updated By"]
EMPTY_RC      = pd.DataFrame({
    "PO # to Track":       pd.array([], dtype="Int64"),
    "What is the PO for?": pd.Series([], dtype="str"),
    "Expiration Date":     pd.Series([], dtype="object")
})

# =============================================================================
# GITHUB HELPERS
# =============================================================================
def load_rc_from_github():
    """Fetch report_controls_data.csv from GitHub. Returns (DataFrame, sha)."""
    r = requests.get(API_URL, headers=GH_HEADERS, params={"ref": GITHUB_BRANCH})
    if r.status_code != 200:
        return EMPTY_RC.copy(), None
    sha = r.json()["sha"]
    try:
        content = base64.b64decode(r.json()["content"]).decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        if df.empty or "PO # to Track" not in df.columns:
            return EMPTY_RC.copy(), sha
        df["PO # to Track"] = df["PO # to Track"].astype("Int64")
        df["What is the PO for?"] = df["What is the PO for?"].astype(str).replace("nan", "")
        if "Expiration Date" not in df.columns:
            df["Expiration Date"] = pd.NaT
        else:
            df["Expiration Date"] = pd.to_datetime(
                df["Expiration Date"], format="%m/%d/%Y", errors="coerce"
            ).dt.date
        return df, sha
    except Exception:
        return EMPTY_RC.copy(), sha


def save_rc_to_github(df, sha):
    """Push updated report_controls_data.csv to GitHub. Returns new sha or None on failure."""
    payload = {
        "message": "chore: update report_controls_data.csv",
        "content": base64.b64encode(df.to_csv(index=False).encode()).decode(),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(API_URL, headers=GH_HEADERS, json=payload)
    if r.status_code not in (200, 201):
        st.error(f"GitHub save failed: {r.json().get('message', 'Unknown error')}")
        return None
    return r.json()["content"]["sha"]


def purge_expired_pos(df, sha):
    """
    Remove rows where Expiration Date has passed today.
    If any were removed, saves the cleaned df back to GitHub.
    Returns (cleaned_df, new_sha).
    """
    today = pd.Timestamp.today().normalize()
    mask = pd.to_datetime(df["Expiration Date"], format="%m/%d/%Y", errors="coerce")
    expired = mask < today
    if expired.any():
        df = df[~expired].reset_index(drop=True)
        sha = save_rc_to_github(df, sha)
    return df, sha

# =============================================================================
# DATA PROCESSING
# =============================================================================
@st.cache_data
def get_datasets(rc_json):
    """
    Core ETL function. Takes report controls as JSON string (for cache hashing),
    reads CSVs from /Downloads, and returns (summary, paste).
    """
    # --- Report Controls ---
    rc = pd.read_json(io.StringIO(rc_json))
    if rc.empty:
        return None, None
    rc["PO # to Track"] = rc["PO # to Track"].astype("Int64")

    # --- Ingest CSVs ---
    files = sorted(Path("Downloads").glob("*.csv"))
    if not files:
        return None, None
    paste = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    # --- Parse Dates ---
    for col in DATE_COLS:
        paste[col] = pd.to_datetime(paste[col], errors="coerce")

    # --- Days Past Pickup ---
    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    # --- Rename Columns ---
    paste = paste.rename(columns={"Purchase Order Number": "PO #", "Vendor Name": "Vendor"})
    paste["PO #"] = paste["PO #"].astype("Int64")

    # --- Build Summary Skeleton ---
    summary = pd.DataFrame({
        "PO #":                rc["PO # to Track"].astype("Int64"),
        "What is the PO for?": rc["What is the PO for?"],
        "Expiration Date":     rc["Expiration Date"] if "Expiration Date" in rc.columns else ""
    })

    # --- Vendor Mapping ---
    summary["Vendor"] = summary["PO #"].map(
        paste[["PO #", "Vendor"]].drop_duplicates("PO #").set_index("PO #")["Vendor"]
    ).fillna("NA")

    # --- Status Counts ---
    status_pivot = paste.groupby(["PO #", "Status"])["Status"].count().unstack(fill_value=0)
    summary = summary.set_index("PO #")
    summary.update(status_pivot, join="left", overwrite=True)
    summary = summary.rename(columns={"Carrier Accepted, Awaiting Pickup": "Awaiting Pickup"})
    for col in STATUS_COLS:
        if col not in summary.columns:
            summary[col] = 0.0
    summary = summary.fillna(0.0).reset_index()

    # --- PO Status ---
    cancelled = paste[paste["Status"] == "Cancelled"]["PO #"].unique()
    summary["PO Status"] = np.where(summary["PO #"].isin(cancelled), "Cancelled", "Approved")

    # --- Aggregated Dates ---
    date_aggs = {
        "Earliest Pickup Date":        ("Pickup Date",               "min"),
        "Latest Pickup Date":          ("Pickup Date",               "max"),
        "Earliest In Yard Goal Date":  ("In Yard Goal Date",         "min"),
        "Latest In Yard Goal Date":    ("In Yard Goal Date",         "max"),
        "Earliest Final Routing Date": ("Final Routing Expected By", "min"),
        "Latest Final Routing Date":   ("Final Routing Expected By", "max"),
    }
    for out_col, (src_col, fn) in date_aggs.items():
        agg = getattr(paste.groupby("PO #")[src_col], fn)()
        summary[out_col] = summary["PO #"].map(agg).apply(
            lambda x: "" if pd.isna(x) or x == 0 else pd.Timestamp(x).strftime("%m/%d/%Y")
        )

    # --- Format Dates in paste for display ---
    paste_display = paste.assign(**{col: paste[col].dt.strftime("%m/%d/%Y") for col in DATE_COLS})

    return summary[SUMMARY_ORDER], paste_display

# =============================================================================
# SESSION STATE — load report controls once per session, purge expired POs
# =============================================================================
if "report_controls" not in st.session_state:
    df, sha = load_rc_from_github()
    df, sha = purge_expired_pos(df, sha)   # auto-remove expired POs on load
    st.session_state.report_controls = df
    st.session_state.rc_sha = sha

# =============================================================================
# UI
# =============================================================================
st.title("🚢 ShipIQ Tracker Automation")
st.markdown("Processed datasets are displayed below. Update the data by pushing new CSVs to the **Downloads** folder on GitHub.")

tab0, tab1, tab2 = st.tabs(["⚙️ Report Controls", "📊 Summary Table", "🔍 Deeper Dive"])

# --- Tab 0: Report Controls ---
with tab0:
    st.subheader("Report Controls")
    st.markdown("Add the PO numbers you want to track and an optional description and expiration date for each.")

    edited = st.data_editor(
        st.session_state.report_controls,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "PO # to Track": st.column_config.NumberColumn(
                "PO # to Track", format="%d", required=True
            ),
            "What is the PO for?": st.column_config.TextColumn("What is the PO for?"),
            "Expiration Date": st.column_config.DateColumn(
                "Expiration Date",
                help="When this date is reached, the PO will be automatically removed.",
                format="MM/DD/YYYY"
            )
        }
    )

    col_apply, col_reset = st.columns(2)
    with col_apply:
        if st.button("✅ Apply & Refresh", type="primary", use_container_width=True):
            edited["PO # to Track"] = pd.array(edited["PO # to Track"].dropna().astype(int), dtype="Int64")
            # Normalize Expiration Date to MM/DD/YYYY string for storage
            edited["Expiration Date"] = pd.to_datetime(
                edited["Expiration Date"], errors="coerce"
            ).dt.strftime("%m/%d/%Y").fillna("")
            st.session_state.report_controls = edited
            st.session_state.rc_sha = save_rc_to_github(edited, st.session_state.rc_sha)
            st.cache_data.clear()
            st.rerun()
    with col_reset:
        if st.button("🗑️ Reset", type="secondary", use_container_width=True):
            st.session_state.report_controls = EMPTY_RC.copy()
            st.session_state.rc_sha = save_rc_to_github(EMPTY_RC.copy(), st.session_state.rc_sha)
            st.cache_data.clear()
            st.rerun()

# --- Load Data ---
summary, all_data = get_datasets(st.session_state.report_controls.to_json())

# --- Tab 1: Summary ---
with tab1:
    if summary is not None:
        st.subheader("Dataset: Summary")
        st.dataframe(summary, use_container_width=True)
    else:
        st.info("No data to display. Add PO numbers in Report Controls and ensure CSVs are in the Downloads folder.")

# --- Tab 2: Deeper Dive ---
with tab2:
    if all_data is not None:
        st.subheader("PO Specific Details")
        selected_po = st.selectbox("Select a PO # to inspect:", options=summary["PO #"].unique())

        po_description = summary.loc[summary["PO #"] == selected_po, "What is the PO for?"].values[0]
        st.dataframe(
            pd.DataFrame({"What is this PO for?": [po_description if po_description else "No description provided"]}),
            use_container_width=True,
            hide_index=True
        )

        deeper_dive = (
            all_data
            .merge(pd.DataFrame({"PO #": pd.array([selected_po], dtype="Int64")}), on="PO #", how="right")
            .rename(columns={"Vendor_x": "Vendor"})
            .drop(columns=["Vendor_y"], errors="ignore")
            .set_index("PO #")
            [DEEPDIVE_COLS]
        )

        if deeper_dive.empty:
            st.warning(f"No records found for PO # {selected_po}.")
        else:
            st.dataframe(deeper_dive, use_container_width=True)
    else:
        st.info("No data to display. Add PO numbers in Report Controls and ensure CSVs are in the Downloads folder.")
