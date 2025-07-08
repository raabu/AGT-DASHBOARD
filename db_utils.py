import sqlite3
import hashlib

def hash_url(url):
    return hashlib.sha256(url.encode()).hexdigest() if url else None

def initialize_db(db_path='notices.db'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS notices (
            id INTEGER PRIMARY KEY,
            type TEXT,
            date TEXT,
            notice_number TEXT,
            subject TEXT,
            detail_link TEXT,
            full_notice TEXT,
            gas_day TEXT,
            no_notice_pct TEXT,
            ofo_start TEXT,
            ofo_end TEXT,
            ofo_lifted BOOLEAN,
            ofo_lift_ref_date TEXT,
            url_hash TEXT UNIQUE
        )
    ''')
    conn.commit()
    conn.close()

def store_notice(notice, db_path='notices.db'):
    url_hash = hash_url(notice.get('Detail Link', ''))

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    try:
        c.execute('''
            INSERT INTO notices (
                type, date, notice_number, subject,
                detail_link, full_notice, gas_day,
                no_notice_pct, ofo_start, ofo_end,
                ofo_lifted, ofo_lift_ref_date, url_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            notice.get('Type'),
            notice.get('Date'),
            notice.get('Notice Number'),
            notice.get('Subject'),
            notice.get('Detail Link'),
            notice.get('Full Notice'),
            notice.get('Gas Day'),
            notice.get('No-Notice %'),
            notice.get('OFO Start'),
            notice.get('OFO End'),
            int(notice.get('OFO Lifted', False)),
            notice.get('OFO Lift Ref Date'),
            url_hash
        ))
        conn.commit()
        print(f"✔️ Stored: {notice.get('Subject', '')}")
    except sqlite3.IntegrityError:
        print(f"⚠️ Duplicate: {notice.get('Subject', '')}")
    finally:
        conn.close()

def initialize_restriction_table(db_path='notices.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS restrictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            notice_number TEXT,
            location TEXT,
            Scheduled TEXT,
            Sealed TEXT,
            AO TEXT,
            IT TEXT,
            "3B" TEXT,
            "3A" TEXT,
            "2C" TEXT,
            "2B" TEXT,
            "2A" TEXT,
            "1" TEXT
        )
    """)
    conn.commit()
    conn.close()

def store_restrictions(notice_number, restriction_data, db_path="notices.db"):
    if not restriction_data:
        return

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    for entry in restriction_data:
        c.execute("""
            INSERT INTO restrictions (
                notice_number, location, Scheduled, Sealed, AO, IT,
                "3B", "3A", "2C", "2B", "2A", "1"
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            notice_number,
            entry.get("location", ""),
            entry.get("Scheduled", ""),
            entry.get("Sealed", ""),
            entry.get("AO", ""),
            entry.get("IT", ""),
            entry.get("3B", ""),
            entry.get("3A", ""),
            entry.get("2C", ""),
            entry.get("2B", ""),
            entry.get("2A", ""),
            entry.get("1", "")
        ))

    conn.commit()
    conn.close()
