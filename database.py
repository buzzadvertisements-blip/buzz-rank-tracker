import sqlite3
import os

DB_PATH = os.environ.get('DB_PATH', 'rank_tracker.db')

SCHEMA = """
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

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print("✅ Database initialized")
