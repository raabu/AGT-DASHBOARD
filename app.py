# === 1. Imports ===
import streamlit as st
import pandas as pd
import sqlite3
import scrape_notices

# === 2. Streamlit Page Config ===
st.set_page_config(layout="wide")
st.title("Algonquin Gas Pipeline Notices")

# === 3. Load & Prepare Notices Data ===
df = scrape_notices.get_notices_from_db()

column_renames = {
    "type": "Type",
    "date": "Date",
    "notice_number": "Notice Number",
    "subject": "Subject",
    "detail_link": "Detail Link",
    "full_notice": "Full Notice",
    "gas_day": "Gas Day",
    "no_notice_pct": "No-Notice %",
    "ofo_start": "OFO Start",
    "ofo_end": "OFO End",
    "ofo_lifted": "OFO Lifted",
    "ofo_lift_ref_date": "OFO Lift Ref Date"
}
df.rename(columns=column_renames, inplace=True)

required_cols = [
    "Status", "Type", "Date", "Notice Number", "Subject",
    "Gas Day", "OFO Start", "OFO End", "No-Notice %", "OFO Lift Ref Date"
]
for col in required_cols:
    if col not in df.columns:
        df[col] = None

if df.empty:
    st.warning("No data available. Please run the scraper or check the source.")
    st.stop()

# === 4. Tabs ===
tab1, tab2 = st.tabs(["📋 Main Notices", "📊 Capacity Restrictions"])

# ========== TAB 1 ==========
with tab1:
    st.subheader("📋 Main Notices")

    # --- Filter Controls ---
    notice_types = df["Type"].dropna().unique().tolist()
    notice_types.sort()
    notice_types.insert(0, "All")
    selected_type = st.selectbox("Filter by Notice Type", notice_types)

    filtered_df = df.copy()
    if selected_type != "All":
        filtered_df = df[df["Type"] == selected_type]

    # --- Dynamic Column Logic ---
    if selected_type == "Operational Flow Order":
        cols_to_show = [
            "Status", "Type", "Date", "Notice Number", "Subject",
            "Gas Day", "OFO Start", "OFO End", "OFO Lift Ref Date"
        ]
    elif selected_type == "Capacity Constraint":
        cols_to_show = [
            "Status", "Type", "Date", "Notice Number", "Subject",
            "Gas Day", "No-Notice %"
        ]
    else:
        cols_to_show = [
            "Status", "Type", "Date", "Notice Number", "Subject",
            "Gas Day", "OFO Start", "OFO End", "No-Notice %", "OFO Lift Ref Date"
        ]

    st.dataframe(filtered_df[cols_to_show], use_container_width=True)

    # --- Detailed Viewer ---
    st.subheader("Detailed Notice Viewer")
    filtered_df = filtered_df.reset_index(drop=True)
    filtered_df["Selector"] = filtered_df["Date"] + " | " + filtered_df["Subject"]

    selected_idx = st.selectbox(
        "Select a Notice",
        options=filtered_df.index,
        format_func=lambda i: filtered_df.loc[i, "Selector"]
    )

    notice = filtered_df.loc[selected_idx]
    st.markdown(f"**Type:** {notice['Type']}")
    st.markdown(f"**Date:** {notice['Date']}")
    st.markdown(f"**Notice Number:** {notice['Notice Number']}")
    st.markdown(f"**Gas Day:** {notice['Gas Day']}")
    st.markdown(f"**No-Notice Restriction:** {notice['No-Notice %']}")
    st.markdown(f"**Link:** [Open Full Notice]({notice['Detail Link']})")
    st.text_area("Full Notice Text", notice["Full Notice"], height=400)

    st.caption(f"🔢 Loaded {len(df)} notices from the database.")


# ========== TAB 2 ==========
with tab2:
    st.subheader("🧪 Capacity Restriction QA Table")

    @st.cache_data
    def load_restrictions():
        conn = sqlite3.connect("notices.db")
        df = pd.read_sql_query("SELECT * FROM restrictions", conn)
        conn.close()
        return df

    restrictions_df = load_restrictions()

    if restrictions_df.empty:
        st.info("No restriction data available.")
        st.stop()

    # === 🧼 Clean the data ===
    # Split priority_restrictions into columns (if needed)
    if "priority_restrictions" in restrictions_df.columns:
        priority_cols = ["Scheduled", "Sealed", "AO", "IT", "3B", "3A", "2C", "2B", "2A", "1"]
        split_df = restrictions_df["priority_restrictions"].str.split(",", expand=True)
        split_df.columns = priority_cols[:len(split_df.columns)]
        restrictions_df = pd.concat([restrictions_df.drop(columns=["priority_restrictions"]), split_df], axis=1)
    else:
        priority_cols = [col for col in restrictions_df.columns if col not in ["notice_number", "location"]]

    # Remove junk rows
    junk_labels = [
        "Restricted Locations", "Scheduled and Sealed", "Priority % Restricted", "Notes",
        "AO", "IT", "3B", "3A", "2C", "2B", "2A", "1", "Yes", "(1)", "", None
    ]
    restrictions_df = restrictions_df[~restrictions_df["location"].isin(junk_labels)]
    restrictions_df = restrictions_df[restrictions_df["location"].notnull()]

    # Drop rows where all priority values are empty
    restrictions_df = restrictions_df[
        restrictions_df[priority_cols].apply(
            lambda row: any(str(val).strip() not in ["", "None", "nan"] for val in row), axis=1
        )
    ]

    # === 1. Filter by Notice Number ===
    unique_notices = restrictions_df["notice_number"].dropna().unique().tolist()
    selected_notice = st.selectbox("Select Notice Number", unique_notices)

    filtered_df = restrictions_df[restrictions_df["notice_number"] == selected_notice]

    # === 2. Link to Full Notice ===
    try:
        matching_row = df[df["Notice Number"] == selected_notice].iloc[0]
        link_url = matching_row["Detail Link"]
        st.markdown(f"[🔗 View Full Notice on Enbridge]({link_url})")
    except IndexError:
        st.warning("Notice link not found in main dataset.")

    # === 3. Display Clean Table ===
    if all(col in filtered_df.columns for col in priority_cols):
        display_df = filtered_df[["location"] + priority_cols].rename(columns={"location": "Location"})
        st.markdown("### 📋 Location-Level Restrictions")
        st.dataframe(display_df, use_container_width=True)
        st.caption(f"📌 Showing {len(display_df)} restricted locations for notice {selected_notice}")
    else:
        st.warning("Missing restriction columns. You may need to re-parse capacity notices.")
