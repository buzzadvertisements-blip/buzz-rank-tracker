import os
import sqlite3

# ── בדיקה אם יש PostgreSQL ──
DATABASE_URL = os.environ.get('DATABASE_URL', '')
USE_PG = DATABASE_URL.startswith('postgres')

if USE_PG:
    import psycopg2
    import psycopg2.extras
    # Render נותן postgresql:// אבל psycopg2 צריך postgresql://
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# Fallback ל-SQLite
DB_PATH = os.environ.get('DB_PATH', 'rank_tracker.db')

# ── סכמה ──

SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    lat REAL,
    lng REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL,
    grid_size INTEGER DEFAULT 7,
    spacing_km REAL DEFAULT 1.0,
    status TEXT DEFAULT 'pending',
    avg_rank REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scan_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER REFERENCES scans(id) ON DELETE CASCADE,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    grid_row INTEGER,
    grid_col INTEGER,
    rank INTEGER DEFAULT 20
);

CREATE TABLE IF NOT EXISTS scan_result_businesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_result_id INTEGER REFERENCES scan_results(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    name TEXT,
    address TEXT,
    rating REAL,
    reviews INTEGER,
    place_url TEXT
);
"""

SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS businesses (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS keywords (
    id SERIAL PRIMARY KEY,
    business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scans (
    id SERIAL PRIMARY KEY,
    business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    keyword TEXT NOT NULL,
    grid_size INTEGER DEFAULT 7,
    spacing_km DOUBLE PRECISION DEFAULT 1.0,
    status TEXT DEFAULT 'pending',
    avg_rank DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scan_results (
    id SERIAL PRIMARY KEY,
    scan_id INTEGER REFERENCES scans(id) ON DELETE CASCADE,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    grid_row INTEGER,
    grid_col INTEGER,
    rank INTEGER DEFAULT 20
);

CREATE TABLE IF NOT EXISTS scan_result_businesses (
    id SERIAL PRIMARY KEY,
    scan_result_id INTEGER REFERENCES scan_results(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    name TEXT,
    address TEXT,
    rating DOUBLE PRECISION,
    reviews INTEGER,
    place_url TEXT
);
"""


class DBWrapper:
    """
    עוטף חיבור SQLite או PostgreSQL עם ממשק אחיד.
    - מתרגם ? ל-%s עבור PostgreSQL
    - מחזיר שורות כ-dict בשני המקרים
    - תומך ב-lastrowid דרך cursor wrapper
    """

    def __init__(self, conn, is_pg=False):
        self._conn = conn
        self._is_pg = is_pg

    def _translate_query(self, sql):
        if self._is_pg:
            # תרגם ? ל-%s, אבל לא בתוך מחרוזות
            result = []
            in_string = False
            quote_char = None
            for ch in sql:
                if not in_string and ch in ("'", '"'):
                    in_string = True
                    quote_char = ch
                    result.append(ch)
                elif in_string and ch == quote_char:
                    in_string = False
                    result.append(ch)
                elif not in_string and ch == '?':
                    result.append('%s')
                else:
                    result.append(ch)
            translated = ''.join(result)

            # הוסף RETURNING id ל-INSERT אם אין כבר
            stripped = translated.strip().upper()
            if stripped.startswith('INSERT') and 'RETURNING' not in stripped:
                translated = translated.rstrip().rstrip(';') + ' RETURNING id'

            return translated
        return sql

    def execute(self, sql, params=None):
        sql_translated = self._translate_query(sql)
        cursor = self._conn.cursor()
        if params:
            cursor.execute(sql_translated, params)
        else:
            cursor.execute(sql_translated)
        # בדוק אם זה INSERT עם RETURNING — אם כן, קרא את ה-id
        is_insert = sql.strip().upper().startswith('INSERT')
        return CursorWrapper(cursor, self._is_pg, is_insert=is_insert)

    def executescript(self, sql):
        if self._is_pg:
            cursor = self._conn.cursor()
            cursor.execute(sql)
        else:
            self._conn.executescript(sql)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


class CursorWrapper:
    """עוטף cursor עם תמיכה ב-lastrowid ו-fetchall שמחזיר dicts"""

    def __init__(self, cursor, is_pg=False, is_insert=False):
        self._cursor = cursor
        self._is_pg = is_pg
        self._returning_id = None
        # אם זה INSERT עם RETURNING ב-PG, קרא את ה-id מיד
        if is_pg and is_insert:
            row = self._cursor.fetchone()
            if row:
                self._returning_id = row.get('id') if isinstance(row, dict) else row[0]

    @property
    def lastrowid(self):
        if self._is_pg:
            return self._returning_id
        return self._cursor.lastrowid

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not self._is_pg:
            # SQLite rows — כבר dict-like בזכות row_factory
            return rows
        return rows  # psycopg2 RealDictCursor מחזיר dicts

    def fetchone(self):
        row = self._cursor.fetchone()
        return row


def get_db():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        # RealDictCursor מחזיר שורות כ-dict
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        return DBWrapper(conn, is_pg=True)
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return DBWrapper(conn, is_pg=False)


def get_raw_connection():
    """חיבור גולמי ל-DB — לשימוש ב-scraper subprocess"""
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        return conn, True
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn, False


def init_db():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute(SCHEMA_PG)
        conn.commit()
        conn.close()
        print("✅ PostgreSQL database initialized")
    else:
        db = get_db()
        db.executescript(SCHEMA_SQLITE)
        db.commit()
        db.close()
        print("✅ SQLite database initialized")
