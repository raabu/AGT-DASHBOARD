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
        return None, None, None, None, False, None

    notice_type = notice_type.lower()

    if "operational flow order" in notice_type or "ofo" in notice_type:
        return parse_ofo_notice(text, notice_date)

    elif "capacity constraint" in notice_type:
        return parse_capacity_notice(text)

    else:
        return None, None, None, None, False, None

# === 5. Notice Type Parsers ===
def parse_capacity_notice(text):
    gas_day = None
    no_notice_pct = None

    gas_day_match = re.search(r"For Gas Day (.+?)(?:,|\n)", text)
    if gas_day_match:
        gas_day = gas_day_match.group(1).strip()

    no_notice_match = re.search(
        r"limited to approximately (\d+%) of their no-notice", text)
    if no_notice_match:
        no_notice_pct = no_notice_match.group(1)

    return gas_day, no_notice_pct, None, None, False, None

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
    response = requests.get(list_url, params=params, headers=headers)
    if not response.ok:
        print(f"\u274c Request failed with status code {response.status_code}")
        return pd.DataFrame()

    print("\u2705 Request succeeded!")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("\u274c No table found on the page.")
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

        # Optional debug log
        if notice_type != "Other" and not any([gas_day, no_notice_pct, ofo_start, ofo_end]):
            print(f"\u26a0\ufe0f Parsing failed: {subject}")

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
