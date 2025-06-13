import streamlit as st
import scrape_notices
import pandas as pd

# ✅ Must be called FIRST
st.set_page_config(layout="wide")
st.title("Algonquin Gas Pipeline Notices")

# Load data
df = scrape_notices.get_notices_df()

# ✅ Safety check: prevent crash if nothing is returned
if df.empty:
    st.warning("No data available. Please try again later or check the source.")
    st.stop()

# Dropdown setup
notice_types = df["Type"].dropna().unique().tolist()
notice_types.sort()
notice_types.insert(0, "All")

selected_type = st.selectbox("Filter by Notice Type", notice_types)

# Filter DataFrame
if selected_type != "All":
    df = df[df["Type"] == selected_type]

# Badge logic
def format_status(row):
    if row["Type"] == "Operational Flow Order":
        if row["OFO Lifted"]:
            return "🟢 Lifted"
        elif pd.notna(row["OFO End"]):
            return "🔴 Active"
        else:
            return "⚪ Pending"
    return "—"

df["Status"] = df.apply(format_status, axis=1)

# Display main table
cols_to_show = [
    "Status", "Type", "Date", "Notice Number", "Subject",
    "Gas Day", "OFO Start", "OFO End", "No-Notice %",
    "OFO Lift Ref Date"
]
st.dataframe(df[cols_to_show], use_container_width=True)

# Detailed viewer
st.subheader("Detailed Notice Viewer")
selected_row = st.selectbox("Select a Notice", df["Subject"])
notice = df[df["Subject"] == selected_row].iloc[0]

st.markdown(f"**Type:** {notice['Type']}")
st.markdown(f"**Date:** {notice['Date']}")
st.markdown(f"**Notice Number:** {notice['Notice Number']}")
st.markdown(f"**Gas Day:** {notice['Gas Day']}")
st.markdown(f"**No-Notice Restriction:** {notice['No-Notice %']}")
st.markdown(f"**Link:** [Open Full Notice]({notice['Detail Link']})")
st.text_area("Full Notice Text", notice["Full Notice"], height=400)
