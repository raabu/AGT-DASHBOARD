# === 1. Imports ===
import streamlit as st
import pandas as pd
import scrape_notices

# === 2. Streamlit Page Config ===
st.set_page_config(layout="wide")
st.title("Algonquin Gas Pipeline Notices")

# === 3. Load & Prepare Data ===
df = scrape_notices.get_notices_from_db()

# Rename DB columns to match expected UI display
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

# Ensure required columns exist
required_cols = [
    "Status", "Type", "Date", "Notice Number", "Subject",
    "Gas Day", "OFO Start", "OFO End", "No-Notice %", "OFO Lift Ref Date"
]
for col in required_cols:
    if col not in df.columns:
        df[col] = None

# Stop if empty
if df.empty:
    st.warning("No data available. Please run the scraper or check the source.")
    st.stop()

# === 4. Filter Controls ===
notice_types = df["Type"].dropna().unique().tolist()
notice_types.sort()
notice_types.insert(0, "All")
selected_type = st.selectbox("Filter by Notice Type", notice_types)

if selected_type != "All":
    df = df[df["Type"] == selected_type]

# === 5. Determine Columns to Show Based on Type ===

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
    # Default columns for 'All' or unclassified notice types
    cols_to_show = [
        "Status", "Type", "Date", "Notice Number", "Subject",
        "Gas Day", "OFO Start", "OFO End", "No-Notice %", "OFO Lift Ref Date"
    ]
# === 6. Main Table Display ====
st.dataframe(df[cols_to_show], use_container_width=True)

# === 7. Detailed Notice Viewer ===
st.subheader("Detailed Notice Viewer")

# Unique label selection with guaranteed index match
df = df.reset_index(drop=True)
df["Selector"] = df["Date"] + " | " + df["Subject"]

selected_idx = st.selectbox(
    "Select a Notice",
    options=df.index,
    format_func=lambda i: df.loc[i, "Selector"]
)

notice = df.loc[selected_idx]

st.markdown(f"**Type:** {notice['Type']}")
st.markdown(f"**Date:** {notice['Date']}")
st.markdown(f"**Notice Number:** {notice['Notice Number']}")
st.markdown(f"**Gas Day:** {notice['Gas Day']}")
st.markdown(f"**No-Notice Restriction:** {notice['No-Notice %']}")
st.markdown(f"**Link:** [Open Full Notice]({notice['Detail Link']})")
st.text_area("Full Notice Text", notice["Full Notice"], height=400)


import sqlite3

st.markdown("---")
st.subheader("🔍 Restriction Table QA Viewer")

# Step 1: Load restriction data
@st.cache_data
def load_restrictions():
    conn = sqlite3.connect("notices.db")
    df = pd.read_sql_query("SELECT * FROM restrictions", conn)
    conn.close()
    return df

restrictions_df = load_restrictions()

# Step 2: Dropdown of unique notice numbers
unique_notices = restrictions_df["notice_number"].dropna().unique().tolist()
selected_notice = st.selectbox("Select Notice Number", unique_notices)

# Step 3: Filter table
filtered = restrictions_df[restrictions_df["notice_number"] == selected_notice]

# Step 4: Link to Enbridge full notice
link_base = "https://infopost.enbridge.com/infopost/NoticeListDetail.asp"
try:
    matching_row = df[df["Notice Number"] == selected_notice].iloc[0]
    link_url = matching_row["Detail Link"]
    st.markdown(f"[🔗 View Full Notice on Enbridge]({link_url})")
except IndexError:
    st.warning("Notice link not found in main dataset.")

# Step 5: Show restriction rows
st.dataframe(filtered[["location", "priority_restrictions"]], use_container_width=True)


# === 8. Footer ===
st.caption(f"🔢 Loaded {len(df)} notices from the database.")
st.caption("🚧 LIVE VERSION - Column logic implemented")