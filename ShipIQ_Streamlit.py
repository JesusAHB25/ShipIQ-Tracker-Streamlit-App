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
SUMMARY_ORDER = ["PO #", "Vendor", "What is the PO for?", "Exp. Date"] + STATUS_COLS + [
                 "Max Past Pickup Days", "PO Status",
                 "Earliest Pickup Date", "Latest Pickup Date",
                 "Earliest In Yard Goal Date", "Latest In Yard Goal Date",
                 "Earliest Final Routing Date", "Latest Final Routing Date"]
DEEPDIVE_COLS = ["Department", "Address", "Shipment ID", "Destination", "Status",
                 "Pickup Date", "In Yard Goal Date", "Final Routing Expected By",
                 "Review By Date", "Last Updated By"]
EMPTY_RC      = pd.DataFrame({
    "PO # to Track":       pd.array([], dtype="Int64"),
    "What is the PO for?": pd.Series([], dtype="str"),
    "Expiration Date":     pd.Series([], dtype="str")
})

# =============================================================================
# GITHUB HELPERS
# =============================================================================
def load_rc_from_github():
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
            df["Expiration Date"] = ""
        else:
            df["Expiration Date"] = pd.to_datetime(
                df["Expiration Date"], format="%m/%d/%Y", errors="coerce"
            ).dt.strftime("%m/%d/%Y").fillna("")
        return df, sha
    except Exception:
        return EMPTY_RC.copy(), sha


def save_rc_to_github(df, sha):
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
    today = pd.Timestamp.today().normalize()
    mask = pd.to_datetime(df["Expiration Date"], format="%m/%d/%Y", errors="coerce")
    expired = mask < today
    if expired.any():
        df = df[~expired].reset_index(drop=True)
        sha = save_rc_to_github(df, sha)
    return df, sha


def list_csv_files_github():
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/Downloads"
    r = requests.get(url, headers=GH_HEADERS, params={"ref": GITHUB_BRANCH})
    if r.status_code != 200:
        return []
    return [f for f in r.json() if f["name"].endswith(".csv")]


def upload_csv_to_github(filename, content_bytes):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/Downloads/{filename}"
    r = requests.get(url, headers=GH_HEADERS)
    payload = {
        "message": f"chore: upload {filename}",
        "content": base64.b64encode(content_bytes).decode(),
        "branch": GITHUB_BRANCH,
    }
    if r.status_code == 200:
        payload["sha"] = r.json()["sha"]
    r = requests.put(url, headers=GH_HEADERS, json=payload)
    return r.status_code in (200, 201)


def delete_csv_from_github(filename, sha):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/Downloads/{filename}"
    r = requests.delete(url, headers=GH_HEADERS, json={
        "message": f"chore: delete {filename}",
        "sha": sha,
        "branch": GITHUB_BRANCH,
    })
    return r.status_code == 200

# =============================================================================
# DATA PROCESSING
# =============================================================================
@st.cache_data
def get_datasets(rc_json):
    rc = pd.read_json(io.StringIO(rc_json))
    if rc.empty:
        return None, None
    rc["PO # to Track"] = rc["PO # to Track"].astype("Int64")

    files = sorted(Path("Downloads").glob("*.csv"))
    if not files:
        return None, None
    paste = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    for col in DATE_COLS:
        paste[col] = pd.to_datetime(paste[col], errors="coerce")

    days_diff = (pd.Timestamp.today() - paste["Pickup Date"]).dt.days
    paste["Days Past Pickup"] = np.where(paste["Status"] == "Past Pickup", days_diff, np.nan)

    paste = paste.rename(columns={"Purchase Order Number": "PO #", "Vendor Name": "Vendor"})
    paste["PO #"] = paste["PO #"].astype("Int64")

    # --- Build Summary Skeleton ---
    summary = pd.DataFrame({
        "PO #":                rc["PO # to Track"].astype("Int64"),
        "What is the PO for?": rc["What is the PO for?"],
        "Exp. Date":           rc["Expiration Date"] if "Expiration Date" in rc.columns else ""
    })

    # --- Vendor Mapping ---
    vendor_map = paste[["PO #", "Vendor"]].drop_duplicates("PO #").set_index("PO #")["Vendor"]
    summary["Vendor"] = summary["PO #"].map(vendor_map).fillna("NA")

    # --- Status Counts ---
    status_pivot = (
        paste.groupby(["PO #", "Status"])["Status"]
        .count()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"Carrier Accepted, Awaiting Pickup": "Awaiting Pickup"})
    )
    vendor_map_backup = summary.set_index("PO #")["Vendor"]
    summary = summary.drop(columns=["Vendor"]).merge(status_pivot, on="PO #", how="left")
    summary["Vendor"] = summary["PO #"].map(vendor_map_backup)
    for col in STATUS_COLS:
        if col not in summary.columns:
            summary[col] = 0
        else:
            summary[col] = summary[col].fillna(0).astype(int)

    # --- Max Past Pickup Days ---
    past_pickup_df = paste[paste["Status"] == "Past Pickup"]
    min_pickup = past_pickup_df.groupby("PO #")["Pickup Date"].min()
    summary["Max Past Pickup Days"] = summary["PO #"].map(min_pickup).apply(
        lambda x: max(0, (pd.Timestamp.today().normalize() - x).days) if pd.notna(x) else 0
    )

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

    paste_display = paste.assign(**{col: paste[col].dt.strftime("%m/%d/%Y") for col in DATE_COLS})

    # TEMP DEBUG — remove after confirming status values
    st.write("STATUS VALUES:", paste["Status"].unique().tolist())

    return summary[SUMMARY_ORDER], paste_display

# =============================================================================
# SESSION STATE
# =============================================================================
if "report_controls" not in st.session_state:
    df, sha = load_rc_from_github()
    df, sha = purge_expired_pos(df, sha)
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
            "Expiration Date": st.column_config.TextColumn(
                "Expiration Date",
                help="Enter date in MM/DD/YYYY format. When reached, the PO will be automatically removed."
            )
        }
    )

    col_apply, col_reset = st.columns(2)
    with col_apply:
        if st.button("✅ Apply & Refresh", type="primary", use_container_width=True):
            edited["PO # to Track"] = pd.array(edited["PO # to Track"].dropna().astype(int), dtype="Int64")
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

    st.divider()
    st.subheader("📁 CSV File Manager")
    st.markdown("Upload new ShipIQ exports or delete files you no longer need from the `Downloads/` folder.")

    uploaded_files = st.file_uploader("Upload CSV files", type="csv", accept_multiple_files=True)
    if uploaded_files:
        if st.button("⬆️ Upload to GitHub", type="primary"):
            for f in uploaded_files:
                ok = upload_csv_to_github(f.name, f.read())
                if ok:
                    st.success(f"✅ {f.name} uploaded successfully.")
                else:
                    st.error(f"❌ Failed to upload {f.name}.")
            st.cache_data.clear()
            st.rerun()

    st.markdown("**Files currently in `Downloads/`:**")
    csv_files = list_csv_files_github()
    if not csv_files:
        st.info("No CSV files found in Downloads/.")
    else:
        for f in csv_files:
            col_name, col_btn = st.columns([5, 1])
            col_name.write(f["name"])
            if col_btn.button("🗑️", key=f"del_{f['name']}"):
                ok = delete_csv_from_github(f["name"], f["sha"])
                if ok:
                    st.success(f"✅ {f['name']} deleted.")
                else:
                    st.error(f"❌ Failed to delete {f['name']}.")
                st.cache_data.clear()
                st.rerun()

# --- Load Data ---
summary, all_data = get_datasets(st.session_state.report_controls.to_json())

# --- Tab 1: Summary ---
with tab1:
    if summary is not None:
        st.subheader("Dataset: Summary")
        summary_display = summary.sort_values("Vendor", na_position="last").reset_index(drop=True)

        vendors = ["All"] + sorted(summary_display["Vendor"].dropna().unique().tolist())
        if "selected_vendor" not in st.session_state:
            st.session_state.selected_vendor = "All"

        btn_cols = st.columns(len(vendors))
        for i, vendor in enumerate(vendors):
            is_active = st.session_state.selected_vendor == vendor
            if btn_cols[i].button(
                vendor,
                key=f"vendor_{vendor}",
                type="primary" if is_active else "secondary",
                use_container_width=True
            ):
                st.session_state.selected_vendor = vendor
                st.rerun()

        if st.session_state.selected_vendor != "All":
            summary_display = summary_display[summary_display["Vendor"] == st.session_state.selected_vendor]

        def summary_to_html(df):
            col_widths = {
                "PO #": "100px",
                "Vendor": "100px",
                "What is the PO for?": "130px",
                "Exp. Date": "70px",
                "Max Past Pickup Days": "70px",
                "PO Status": "70px",
                "Earliest Pickup Date": "80px",
                "Latest Pickup Date": "80px",
                "Earliest In Yard Goal Date": "80px",
                "Latest In Yard Goal Date": "80px",
                "Earliest Final Routing Date": "80px",
                "Latest Final Routing Date": "80px",
            }
            for col in STATUS_COLS:
                col_widths[col] = "70px"

            def cell_style(col, val):
                base = "white-space:normal; word-wrap:break-word; font-size:12px; padding:6px 8px;"
                if col == "Past Pickup":
                    color = "#ff4b4b" if val > 0 else "#21c354"
                    return f'style="{base} background-color:{color}; color:white; text-align:center;"'
                if col in STATUS_COLS or col in ("Max Past Pickup Days", "PO Status"):
                    return f'style="{base} text-align:center;"'
                return f'style="{base}"'

            headers = "".join(
                f'<th style="white-space:normal; word-wrap:break-word; '
                f'width:{col_widths.get(c, "80px")}; padding:6px 8px; '
                f'font-size:12px; text-align:center; position:sticky; top:0; z-index:1;">{c}</th>'
                for c in df.columns
            )
            rows = ""
            for i, (_, row) in enumerate(df.iterrows()):
                bg = "#1a1a2e" if i % 2 == 0 else "#16213e"
                cells = "".join(
                    f'<td {cell_style(col, row[col])}>{row[col]}</td>'
                    for col in df.columns
                )
                rows += f'<tr style="background-color:{bg};" onmouseover="this.style.backgroundColor=\'#2a2a4e\'" onmouseout="this.style.backgroundColor=\'{bg}\'">{cells}</tr>'

            return f"""
            <style>
              .summary-table {{ border-collapse: collapse; width: 100%; table-layout: fixed; }}
              .summary-table thead tr {{ background-color: #1f2937; color: #e5e7eb; border-bottom: 2px solid #374151; }}
              .summary-table tbody tr {{ color: #e5e7eb; border-bottom: 1px solid #374151; transition: background-color 0.1s; }}
              .summary-table td, .summary-table th {{ border-right: 1px solid #374151; }}
              .summary-table td:last-child, .summary-table th:last-child {{ border-right: none; }}
            </style>
            <div style="overflow-x:auto; width:100%; max-height:800px; overflow-y:auto; border:1px solid #374151; border-radius:6px;">
              <table class="summary-table">
                <thead><tr>{headers}</tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </div>
            """

        st.html(summary_to_html(summary_display))
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
