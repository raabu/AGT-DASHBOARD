# === 1. Imports ===
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import sqlite3
from db_utils import initialize_db, store_notice

# === 2. Constants ===
base_url = "https://infopost.enbridge.com/infopost/"
list_url = base_url + "NoticesList.asp"
params = {
    "pipe": "AG",
    "type": "CRI"
}
headers = {
    "User-Agent": "Mozilla/5.0"
}

# === 3. Utility: Classify Notice Type ===
def classify_notice_type(raw_type):
    raw = raw_type.strip().lower()
    if "capacity constraint" in raw:
        return "Capacity Constraint"
    elif "operational flow order" in raw or "ofo" in raw:
        return "Operational Flow Order"
    else:
        return "Other"

# === 4. Parsing Dispatcher ===
def extract_notice_insights(text, notice_type, notice_date=None):
    if not text:
        # Always return 7 values, even if text is missing
        return None, None, None, None, False, None, []

    notice_type = notice_type.lower()

    if "operational flow order" in notice_type or "ofo" in notice_type:
        # Return the normal 6 OFO values, plus an empty list for restrictions
        return (*parse_ofo_notice(text, notice_date), [])

    elif "capacity constraint" in notice_type:
        # This now returns all 7 values including restriction_data
        return parse_capacity_notice(text)

    else:
        # Unknown notice type — return 7 values of safe defaults
        return None, None, None, None, False, None, []

# === 5. Notice Type Parsers ===
def parse_capacity_notice(text):
    """
    Parses Capacity Constraint notices for Gas Day, No-Notice %, and Restrictions.
    """
    gas_day = None
    no_notice_pct = None

    # Extract gas day
    gas_day_match = re.search(r"For Gas Day (.+?)(?:,|\n)", text)
    if gas_day_match:
        gas_day = gas_day_match.group(1).strip()

    # Extract no-notice %
    no_notice_match = re.search(
        r"limited to approximately (\d+%) of their no-notice", text)
    if no_notice_match:
        no_notice_pct = no_notice_match.group(1)

    # NEW: Parse the restrictions table
    restriction_data = parse_restriction_table(text)

    return gas_day, no_notice_pct, None, None, False, None, restriction_data

def parse_restriction_table(text):
    """
    Parses the 'Restricted Locations' table from Capacity Constraint notices.
    Returns a list of dictionaries, each containing a location and any extracted priority restrictions.
    """
    # Start by finding the table header section
    header_pattern = r"Restricted Locations\s+Scheduled and Sealed\s+Priority % Restricted\s+Notes"
    match = re.search(header_pattern, text, re.IGNORECASE)

    if not match:
        return None  # No table found

    lines = text[match.end():].strip().splitlines()

    restrictions = []
    current_location = None
    priorities = []

    for line in lines:
        line = line.strip()

        # End of the table is usually marked by a blank line or switch in topic
        if not line or line.startswith("No-Notice Restrictions") or "FERC" in line or "Systemwide" in line:
            break

        # If line looks like a location row (starts with letters, not %s or numbers)
        if re.match(r"^[A-Za-z].*", line):
            if current_location:
                restrictions.append({
                    "location": current_location,
                    "priority_restrictions": priorities
                })
            current_location = line
            priorities = []
        else:
            # Assume it's a priority % value or a continuation
            if "%" in line:
                priorities.append(line)

    # Final push for last row
    if current_location:
        restrictions.append({
            "location": current_location,
            "priority_restrictions": priorities
        })

    return restrictions if restrictions else None


def parse_ofo_notice(text, notice_date=None):
    gas_day = None
    ofo_start = None
    ofo_end = None
    is_lifted = False
    lift_ref_date = None

    gas_day_matches = re.findall(
        r"Gas Day (?:January|February|March|April|May|June|July|August|September|October|November|December) ?\d{1,2}(?: - (?:January|February|March|April|May|June|July|August|September|October|November|December)? ?\d{1,2})?",
        text
    )
    if gas_day_matches:
        gas_day = "; ".join(gas_day_matches)
        if "until further notice" in text.lower():
            gas_day += " (Until Further Notice)"

    ofo_start_match = re.search(
        r"effective (\d{1,2}:\d{2} (?:AM|PM) CCT, .*?\d{4})", text, re.IGNORECASE)
    if ofo_start_match:
        ofo_start = ofo_start_match.group(1).strip()

    ofo_end_match = re.search(
        r"remain in effect until (\d{1,2}:\d{2} (?:AM|PM) CCT, .*?\d{4})", text, re.IGNORECASE)
    if ofo_end_match:
        ofo_end = ofo_end_match.group(1).strip()

    if "lifting the operational flow order" in text.lower():
        is_lifted = True
        lift_date_match = re.search(r"issued on (\w+ \d{1,2}, \d{4})", text)
        if lift_date_match:
            lift_ref_date = lift_date_match.group(1).strip()

        lift_eff_match = re.search(
            r"Effective (?:immediately|on )?(?:at )?(\w+ \d{1,2}, \d{4})",
            text, re.IGNORECASE
        )
        if lift_eff_match:
            ofo_end = lift_eff_match.group(1).strip()
        elif "effective immediately" in text.lower() and notice_date:
            ofo_end = notice_date

    return gas_day, None, ofo_start, ofo_end, is_lifted, lift_ref_date

# === 6. Core Scraping Logic ===
def get_notices_df(limit=None):
    # === A. Request the Notices Table ===
    response = requests.get(list_url, params=params, headers=headers)
    if not response.ok:
        print(f"❌ Request failed with status code {response.status_code}")
        return pd.DataFrame()  # Error Handling: network failure

    print("✅ Request succeeded!")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("❌ No table found on the page.")
        return pd.DataFrame()  # Error Handling: malformed HTML

    # === B. Parse the Table Rows ===
    rows = table.find_all("tr")
    notices = []

    for row in rows[1:]:  # Skip header row
        cols = row.find_all("td")
        if len(cols) < 6:
            continue  # Error Handling: skip malformed row

        # === C. Extract Basic Info from Row ===
        raw_type = cols[0].text.strip()
        notice_type = classify_notice_type(raw_type)
        date = cols[1].text.strip()
        number = cols[2].text.strip()
        subject = cols[5].text.strip()

        # === D. Get the Full Notice Text from Detail Page ===
        link_tag = cols[5].find("a")
        link = base_url + link_tag['href'] if link_tag else None

        notice_text = None
        if link:
            detail_resp = requests.get(link, headers=headers)
            if detail_resp.ok:
                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                main_div = detail_soup.find("div", {"id": "content"}) or detail_soup.find("div", {"class": "main"})
                if main_div:
                    notice_text = main_div.get_text(separator="\n", strip=True)
                else:
                    notice_text = detail_soup.get_text(separator="\n", strip=True)

        # === E. Run the Modular Parser to Extract Key Fields ===
        gas_day, no_notice_pct, ofo_start, ofo_end, is_lifted, lifted_date_ref, restrictions = extract_notice_insights(
            notice_text, notice_type, date
        )

        # === F. Optional Debug: Missing Restriction Data for Constraints ===
        if notice_type == "Capacity Constraint" and not restrictions:
            print(f"⚠️ No restrictions parsed: {subject}")

        # === G. Construct the Final Parsed Notice Dictionary ===
        notices.append({
            "Type": notice_type,
            "Date": date,
            "Notice Number": number,
            "Subject": subject,
            "Detail Link": link,
            "Full Notice": notice_text,
            "Gas Day": gas_day,
            "No-Notice %": no_notice_pct,
            "OFO Start": ofo_start,
            "OFO End": ofo_end,
            "OFO Lifted": is_lifted,
            "OFO Lift Ref Date": lifted_date_ref,
            "Restrictions": restrictions  # ✅ NEW: this is our juicy data!
        })

        # === H. Limit Control (Optional for Testing) ===
        if limit and len(notices) >= limit:
            break

    # === I. Return Parsed Notices as DataFrame ===
    return pd.DataFrame(notices)

# === 7. Read from DB ===
def get_notices_from_db(db_path='notices.db'):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM notices ORDER BY date DESC", conn)
    conn.close()
    return df

# === 8. Main Execution Block ===
if __name__ == "__main__":
    df = get_notices_df()
    pd.set_option('display.max_colwidth', None)
    print("\n\u2705 Final Parsed Output:")
    print(df.head())

    initialize_db()
    for row in df.to_dict(orient='records'):
        store_notice(row)

from db_utils import initialize_restriction_table, store_restrictions

# Initialize both tables
initialize_db()
initialize_restriction_table()

# Store notices + restrictions
for row in df.to_dict(orient='records'):
    store_notice(row)
    store_restrictions(row["Notice Number"], row.get("Restrictions"))