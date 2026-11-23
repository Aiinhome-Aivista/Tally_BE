from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

import os
import sys
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

# 1. Local SQLite Database (ONLY for storing MySQL Connection Details)
if getattr(sys, 'frozen', False):
    # If running as PyInstaller EXE, store data next to executable or user's appdata
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_DIR = os.path.join(APP_DIR, "data")
os.makedirs(DB_DIR, exist_ok=True)
LOCAL_DB_PATH = os.path.join(DB_DIR, "tally_sync_local.db").replace("\\", "/")

LOCAL_DB_URL = f"sqlite:///{LOCAL_DB_PATH}"
sqlite_engine = create_engine(LOCAL_DB_URL, connect_args={"check_same_thread": False})
LocalSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)

LocalBase = declarative_base()

# 2. Main Target Database Base (Tables will be created dynamically when engine is known)
Base = declarative_base()

def init_sqlite_migrations():
    """Ensure SQLite schema is up-to-date."""
    from sqlalchemy import text
    with sqlite_engine.connect() as conn:
        try:
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(mysql_config)")).fetchall()]
            if cols and "db_type" not in cols:
                conn.execute(text("ALTER TABLE mysql_config ADD COLUMN db_type VARCHAR(50) DEFAULT 'mysql'"))
                conn.commit()
        except Exception:
            pass

init_sqlite_migrations()

def get_local_db():
    db = LocalSessionLocal()
    try:
        yield db
    finally:
        db.close()
