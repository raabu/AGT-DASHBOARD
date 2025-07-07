#Import Requests
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from db_utils import initialize_db, store_notice
import sqlite3
#Constants
base_url = "https://infopost.enbridge.com/infopost/"
list_url = base_url + "NoticesList.asp"
params = {
    "pipe": "AG",
    "type": "CRI"
}
headers = {
    "User-Agent": "Mozilla/5.0"
}
#Normalize capacity constraint and ofo
def classify_notice_type(raw_type):
    """
    Normalizes and categorizes the notice type.
    """
    raw = raw_type.strip().lower()

    if "capacity constraint" in raw:
        return "Capacity Constraint"
    elif "operational flow order" in raw or "ofo" in raw:
        return "Operational Flow Order"
    else:
        return "Other"


#Exctract notice insights
def extract_notice_insights(text, notice_type, notice_date=None):
    """
    Extracts:
      - Gas Day
      - No-Notice %
      - OFO Start and End if present
    """
    if not text:
        return None, None, None, None, False, None

    notice_type = notice_type.lower().strip()
    gas_day = None
    no_notice_pct = None
    ofo_start = None
    ofo_end = None
    is_lifted = False
    lifted_date_ref = None

    if "capacity constraint" in notice_type:
        gas_day_match = re.search(r"For Gas Day (.+?)(?:,|\n)", text)
        if gas_day_match:
            gas_day = gas_day_match.group(1).strip()

    elif "operational flow order" in notice_type or "ofo" in notice_type:
        # Gas Day matches
        gas_day_matches = re.findall(
            r"Gas Day (?:January|February|March|April|May|June|July|August|September|October|November|December) ?\d{1,2}(?: - (?:January|February|March|April|May|June|July|August|September|October|November|December)? ?\d{1,2})?",
            text
        )
        if gas_day_matches:
            gas_day = "; ".join(gas_day_matches)
            if "until further notice" in text.lower():
                gas_day += " (Until Further Notice)"

        # OFO start
        ofo_start_match = re.search(
            r"effective (\d{1,2}:\d{2} (?:AM|PM) CCT, .*?\d{4})", text, re.IGNORECASE)
        if ofo_start_match:
            ofo_start = ofo_start_match.group(1).strip()

        # OFO end
        ofo_end_match = re.search(
            r"remain in effect until (\d{1,2}:\d{2} (?:AM|PM) CCT, .*?\d{4})", text, re.IGNORECASE)
        if ofo_end_match:
            ofo_end = ofo_end_match.group(1).strip()

        # Check for lift
        if "lifting the operational flow order" in text.lower():
            is_lifted = True
            lifted_date_match = re.search(r"issued on (\w+ \d{1,2}, \d{4})", text)
            if lifted_date_match:
                lifted_date_ref = lifted_date_match.group(1).strip()

            # Try to match OFO end date from lift notice
            lift_date_match = re.search(
                r"Effective (?:immediately|on )?(?:at )?(\w+ \d{1,2}, \d{4})",
                text,
                re.IGNORECASE
            )
            if lift_date_match:
                ofo_end = lift_date_match.group(1).strip()
            elif "effective immediately" in text.lower() and notice_date:
                ofo_end = notice_date

    # Extract no-notice %
    no_notice_match = re.search(
        r"limited to approximately (\d+%) of their no-notice", text)
    no_notice_pct = no_notice_match.group(1) if no_notice_match else None

    return gas_day, no_notice_pct, ofo_start, ofo_end, is_lifted, lifted_date_ref

#Get-Notices Scraping Engine
def get_notices_df(limit=None):
    response = requests.get(list_url, params=params, headers=headers)
    if not response.ok:
        print(f"❌ Request failed with status code {response.status_code}")
        return pd.DataFrame()

    print("✅ Request succeeded!")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("❌ No table found on the page.")
        return pd.DataFrame()

    rows = table.find_all("tr")
    notices = []

    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        raw_type = cols[0].text.strip()
        notice_type = classify_notice_type(raw_type)
        date = cols[1].text.strip()
        number = cols[2].text.strip()
        subject = cols[5].text.strip()

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

        gas_day, no_notice_pct, ofo_start, ofo_end, is_lifted, lifted_date_ref = extract_notice_insights(notice_text, notice_type, date)

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
        })

        if limit and len(notices) >= limit:
            break

    return pd.DataFrame(notices)

if __name__ == "__main__":
    df = get_notices_df()
    pd.set_option('display.max_colwidth', None)
    print("\n✅ Final Parsed Output:")
    print(df.head())

 #NEW: Initialize database and store each notice
    initialize_db()
    for row in df.to_dict(orient='records'):
        store_notice(row)

def get_notices_from_db(db_path='notices.db'):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM notices ORDER BY date DESC", conn)
    conn.close()
    return df