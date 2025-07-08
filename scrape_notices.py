# === 1. Imports ===
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import sqlite3
from db_utils import initialize_db, store_notice, store_restrictions, initialize_restriction_table

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
    
def classify_line_type(line):
        line = line.strip()
        if not line or line.lower().startswith("no-notice") or "ferc" in line.lower() or "systemwide" in line.lower():
            return "SEPARATOR"
        if re.match(r"^restricted locations\s+", line.lower()):
            return "HEADER"
        if re.match(r"^(yes|no)$", line.lower()):
            return "SCHEDULED_ROW"
        if re.match(r"^(\(?\d+%[,\s]*)+$", line):
            return "PERCENT_ROW"
        if re.match(r"^[A-Za-z].*", line):  # Starts with a word character
            return "LOCATION_ROW"
        return "UNKNOWN"

def extract_restriction_block(text):
    """
    Extracts lines starting from 'Restricted Locations' up to (but not including) 'No-Notice Restrictions'.
    """
    lines = text.splitlines()
    start_index = None
    end_index = None

    for i, line in enumerate(lines):
        lower = line.lower()
        if "restricted locations" in lower and start_index is None:
            start_index = i
        elif start_index is not None and "no-notice restrictions" in lower:
            end_index = i
            break

    if start_index is not None:
        return lines[start_index:end_index] if end_index else lines[start_index:]
    return lines

def parse_capacity_notice(text):
    """
    Parses Capacity Constraint notices for Gas Day, No-Notice %, and embedded restriction table.
    """
    gas_day = None
    no_notice_pct = None

    # Extract Gas Day
    gas_day_match = re.search(r"For Gas Day (.+?)(?:,|\n)", text)
    if gas_day_match:
        gas_day = gas_day_match.group(1).strip()

    # Extract No-Notice %
    no_notice_match = re.search(r"limited to approximately (\d+%) of their no-notice", text)
    if no_notice_match:
        no_notice_pct = no_notice_match.group(1).strip()

    # Extract the restriction table using your new modular function
    restrictions = extract_restriction_block(text)


    return gas_day, no_notice_pct, None, None, False, None, restrictions


# === 4. Parsing Dispatcher ===
from typing import Optional, List, Tuple, Dict

def extract_notice_insights(
    text: str,
    notice_type: str,
    notice_date: Optional[str] = None
) -> Tuple[
    Optional[str],  # gas_day
    Optional[str],  # no_notice_pct
    Optional[str],  # ofo_start
    Optional[str],  # ofo_end
    bool,           # is_lifted
    Optional[str],  # lift_ref_date
    List[Dict]      # restrictions
]:
    """
    Parses a pipeline notice and returns key operational fields.

    Parameters:
        text (str): The full text of the notice.
        notice_type (str): The classified type of the notice (e.g., "Capacity Constraint").
        notice_date (Optional[str]): The date associated with the notice, used for context.

    Returns:
        Tuple containing:
            - gas_day (Optional[str]): The date(s) the notice applies to.
            - no_notice_pct (Optional[str]): The percentage of no-notice capacity allowed.
            - ofo_start (Optional[str]): Start time of any Operational Flow Order.
            - ofo_end (Optional[str]): End time of any Operational Flow Order.
            - is_lifted (bool): Whether the OFO was lifted.
            - lift_ref_date (Optional[str]): Reference date the OFO was lifted.
            - restrictions (List[Dict]): Structured restrictions, if available (locations, %s).
    """

    # === A. Basic Guard Clause ===
    if not text:
        return None, None, None, None, False, None, []

    # === B. Normalize Notice Type ===
    notice_type = notice_type.lower()

    # === C. Dispatch Based on Type ===
    if "operational flow order" in notice_type or "ofo" in notice_type:
        # Parse OFO-specific details, append empty restrictions list
        return (*parse_ofo_notice(text, notice_date), [])

    elif "capacity constraint" in notice_type:
        # This function is defined separately
        return parse_capacity_notice(text)

    # === D. Unknown or Unsupported Notice Type ===
    return None, None, None, None, False, None, []

# === 5. Notice Type Parsers ===


def parse_restriction_table(lines):
    """
    Parses the 'Restricted Locations' table from Capacity Constraint notices.
    Returns a list of dictionaries with location, scheduled status, and priority restriction values.
    """
    restrictions = []

    current_location = None
    current_scheduled = None
    current_priorities = []

    for line in lines:
        line = line.strip()
        kind = classify_line_type(line)  # Use your global helper

        if kind == "SEPARATOR":
            break

        if kind == "LOCATION_ROW":
            if current_location:
                restrictions.append({
                    "location": current_location,
                    "scheduled": current_scheduled,
                    "priorities": current_priorities
                })
            current_location = line
            current_scheduled = None
            current_priorities = []

        elif kind == "SCHEDULED_ROW":
            current_scheduled = line

        elif kind == "PERCENT_ROW":
            percents = [val.strip() for val in re.split(r"[, ]+", line) if val.strip()]
            current_priorities.extend(percents)

    if current_location:
        restrictions.append({
            "location": current_location,
            "scheduled": current_scheduled,
            "priorities": current_priorities
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

if __name__ == "__main__":
    df = get_notices_df()
    pd.set_option('display.max_colwidth', None)
    print("\n✅ Final Parsed Output:")
    print(df.head())
    initialize_restriction_table()
    initialize_db()

    for row in df.to_dict(orient="records"):
        store_notice(row)