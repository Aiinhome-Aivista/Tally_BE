from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

# 1. Local SQLite Database (ONLY for storing MySQL Connection Details)
DB_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
os.makedirs(DB_DIR, exist_ok=True)
LOCAL_DB_PATH = os.path.join(DB_DIR, "tally_sync_local.db")
# In Windows, SQLAlchemy needs the absolute path properly formatted or a relative path from cwd
# Using relative path for simplicity
LOCAL_DB_URL = "sqlite:///./data/tally_sync_local.db"
sqlite_engine = create_engine(LOCAL_DB_URL, connect_args={"check_same_thread": False})
LocalSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sqlite_engine)

LocalBase = declarative_base()

# 2. Main Target Database Base (Tables will be created dynamically when engine is known)
Base = declarative_base()

def get_local_db():
    db = LocalSessionLocal()
    try:
        yield db
    finally:
        db.close()
