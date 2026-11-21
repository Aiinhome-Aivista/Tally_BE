import logging
import json
import os
import sys

# Fix for PyInstaller noconsole/windowed mode where sys.stdout and sys.stderr are None
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")
if sys.stdin is None:
    sys.stdin = open(os.devnull, "r", encoding="utf-8")
import threading
import subprocess
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from database.config import Base, LocalBase, get_local_db, sqlite_engine
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import urllib.parse
from models import SyncConfig, SyncLog, MysqlConfig
from scheduler import start_scheduler, stop_scheduler, sync_job, update_scheduler_job
from services.sync_service import SyncService

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Automatically cleanup any existing stuck instances or port conflicts on Windows
if sys.platform == "win32":
    try:
        current_pid = os.getpid()
        # Find and kill other TallySyncApp processes except current
        subprocess.run(
            f'powershell -Command "Get-Process -Name TallySyncApp -ErrorAction SilentlyContinue | Where-Object {{ $_.Id -ne {current_pid} }} | Stop-Process -Force -ErrorAction SilentlyContinue"',
            shell=True,
            capture_output=True
        )
    except Exception:
        pass

# Create local tables
LocalBase.metadata.create_all(bind=sqlite_engine)

_mysql_engine = None
_mysql_db_url = None

def get_mysql_engine(local_db: Session):
    global _mysql_engine, _mysql_db_url
    mysql_config = local_db.query(MysqlConfig).first()
    if not mysql_config:
        raise Exception("MySQL Configuration not found.")
    
    encoded_password = urllib.parse.quote_plus(mysql_config.password or "")
    db_url = f"mysql+pymysql://{mysql_config.username}:{encoded_password}@{mysql_config.host}:{mysql_config.port}/{mysql_config.database_name}"
    
    # Return cached engine if the URL hasn't changed
    if _mysql_engine is not None and _mysql_db_url == db_url:
        return _mysql_engine
        
    # If it changed or doesn't exist, create a new one
    if _mysql_engine is not None:
        _mysql_engine.dispose()
        
    _mysql_engine = create_engine(db_url, pool_pre_ping=True, pool_size=10, max_overflow=20)
    _mysql_db_url = db_url
    
    # Automatically create missing tables (like sync_config, sync_logs)
    Base.metadata.create_all(bind=_mysql_engine)
    
    return _mysql_engine

def get_db(local_db: Session = Depends(get_local_db)):
    try:
        engine = get_mysql_engine(local_db)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the background scheduler
    logger.info("Starting up FastAPI application...")
    start_scheduler()
    
    # Initialize default config if not exists
    # We no longer auto-create the config so the user can delete it to stop syncing.

    yield
    # Shutdown: Stop the scheduler
    logger.info("Shutting down FastAPI application...")
    stop_scheduler()

app = FastAPI(title="Tally Sync API", lifespan=lifespan)

# Allow CORS for React UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic schemas
class ConfigSchema(BaseModel):
    connection_name: str
    tally_host: str
    tally_port: int
    company_name: Optional[str] = None
    report_name: Optional[str] = None
    scheduler_timing: Optional[str] = None
    file_format: Optional[str] = None
    request_xml: Optional[str] = None
    unique_key_field: Optional[str] = None
    dynamic_filters: Optional[dict] = None
    database_name: Optional[str] = None
    total_rows: Optional[int] = 0

class TestConfigSchema(BaseModel):
    connection_name: Optional[str] = None
    tally_host: Optional[str] = None
    tally_port: Optional[int] = None
    company_name: Optional[str] = None
    report_name: Optional[str] = None
    scheduler_timing: Optional[str] = None
    file_format: Optional[str] = None
    request_xml: Optional[str] = None
    unique_key_field: Optional[str] = None
    dynamic_filters: Optional[dict] = None
    database_name: Optional[str] = None

class MysqlConfigSchema(BaseModel):
    host: str
    port: int
    username: str
    password: Optional[str] = ""
    database_name: str

from typing import List

@app.get("/api/debug/tables")
def get_tables(db: Session = Depends(get_db)):
    """Debug endpoint to list all tables in MySQL."""
    from sqlalchemy import inspect
    engine = db.get_bind()
    
    # Drop sync_logs to recreate it with LONGTEXT
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sync_logs"))
        conn.commit()
    Base.metadata.create_all(bind=engine)
    
    inspector = inspect(engine)
    schema = {}
    for table in inspector.get_table_names():
        schema[table] = [{"name": c["name"], "type": str(c["type"])} for c in inspector.get_columns(table)]
    return {"tables": inspector.get_table_names(), "schema": schema, "url": str(engine.url).replace(engine.url.password, '***') if engine.url.password else str(engine.url)}

@app.get("/api/config", response_model=List[ConfigSchema])
def get_config(local_db: Session = Depends(get_local_db)):
    # MysqlConfig is always in SQLite
    mysql_config = local_db.query(MysqlConfig).first()
    if not mysql_config:
        return []  # MySQL not configured yet — return empty list gracefully

    # SyncConfig is in MySQL
    try:
        engine = get_mysql_engine(local_db)
        SessionMySQL = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        mysql_db = SessionMySQL()
        try:
            from sqlalchemy import inspect
            inspector = inspect(engine)
            existing_tables = set(inspector.get_table_names())

            configs = mysql_db.query(SyncConfig).all()
            result = []
            for config in configs:
                db_name = mysql_config.database_name if mysql_config else ""
                if not db_name and config.company_name:
                    import re
                    # Same logic as frontend generateDbName
                    db_name = re.sub(r'[^a-z0-9]', '_', config.company_name.lower())
                    db_name = re.sub(r'_+', '_', db_name).strip('_')

                # Calculate total rows in the target table
                table_name = ''.join(c for c in (config.report_name or "").lower() if c.isalnum() or c == '_')
                total_rows = 0
                if table_name and table_name in existing_tables:
                    try:
                        row_count = mysql_db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
                        total_rows = int(row_count) if row_count is not None else 0
                    except Exception:
                        total_rows = 0

                # Fallback to max / latest positive records_fetched from logs if table count is 0
                if total_rows == 0:
                    last_valid_log = mysql_db.query(SyncLog).filter(
                        SyncLog.connection_name == config.connection_name,
                        SyncLog.records_fetched > 0
                    ).order_by(SyncLog.timestamp.desc()).first()
                    if last_valid_log:
                        total_rows = last_valid_log.records_fetched or 0

                result.append({
                    "connection_name": config.connection_name,
                    "tally_host": config.tally_host,
                    "tally_port": config.tally_port,
                    "company_name": config.company_name or "",
                    "report_name": config.report_name or "",
                    "scheduler_timing": config.scheduler_timing or "",
                    "file_format": config.file_format or "XML",
                    "request_xml": config.request_xml or "",
                    "unique_key_field": config.unique_key_field or "",
                    "dynamic_filters": json.loads(config.dynamic_filters) if config.dynamic_filters else {},
                    "database_name": db_name,
                    "total_rows": total_rows
                })
            return result
        finally:
            mysql_db.close()
    except Exception as e:
        logger.error(f"Failed to fetch config from MySQL: {e}")
        return []


@app.post("/api/config")
def update_config(config_data: ConfigSchema, db: Session = Depends(get_db)):
    # Check if a connection with this name already exists
    existing_config = db.query(SyncConfig).filter(SyncConfig.connection_name == config_data.connection_name).first()
    if existing_config:
        raise HTTPException(status_code=400, detail=f"Connection name '{config_data.connection_name}' already exists.")
        
    config = SyncConfig()
    config.connection_name = config_data.connection_name
    config.tally_host = config_data.tally_host
    config.tally_port = config_data.tally_port
    config.company_name = config_data.company_name
    config.report_name = config_data.report_name
    config.scheduler_timing = config_data.scheduler_timing
    import json
    config.file_format = config_data.file_format
    config.request_xml = config_data.request_xml
    config.unique_key_field = config_data.unique_key_field
    if config_data.dynamic_filters:
        config.dynamic_filters = json.dumps(config_data.dynamic_filters)
    db.add(config)
    db.commit()
    update_scheduler_job()
    return {"message": "Configuration saved successfully"}

@app.put("/api/config/{connection_name}")
def modify_config(connection_name: str, config_data: ConfigSchema, db: Session = Depends(get_db)):
    config = db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
    if not config:
        raise HTTPException(status_code=404, detail="Connection not found")
        
    config.tally_host = config_data.tally_host
    config.tally_port = config_data.tally_port
    config.company_name = config_data.company_name
    
    # If the user changed the report name, reset last_alter_id to 0
    if config.report_name != config_data.report_name:
        config.last_alter_id = 0
        
    config.report_name = config_data.report_name
    import json
    config.scheduler_timing = config_data.scheduler_timing
    config.file_format = config_data.file_format
    config.request_xml = config_data.request_xml
    config.unique_key_field = config_data.unique_key_field
    if config_data.dynamic_filters is not None:
        config.dynamic_filters = json.dumps(config_data.dynamic_filters)
    db.commit()
    update_scheduler_job()
    return {"message": "Configuration updated successfully"}

class ToggleSchedulerSchema(BaseModel):
    scheduler_timing: Optional[str] = "5"

@app.post("/api/config/{connection_name}/toggle-scheduler")
def toggle_scheduler(connection_name: str, payload: ToggleSchedulerSchema, db: Session = Depends(get_db)):
    config = db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
    if not config:
        raise HTTPException(status_code=404, detail="Connection not found")
    config.scheduler_timing = payload.scheduler_timing
    db.commit()
    update_scheduler_job()
    return {"message": "Scheduler updated successfully", "scheduler_timing": config.scheduler_timing}

@app.delete("/api/config/{connection_name}")
def delete_config(connection_name: str, db: Session = Depends(get_db)):
    config = db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
    if config:
        db.delete(config)
        db.commit()
        update_scheduler_job()
        return {"message": "Connector deleted successfully"}
    return {"message": "No connector found"}

@app.get("/api/logs")
def get_logs(skip: int = 0, limit: int = 10, connection_name: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(SyncLog)
    if connection_name:
        query = query.filter(SyncLog.connection_name == connection_name)
    
    total = query.with_entities(func.count(SyncLog.id)).scalar()
    logs = query.order_by(SyncLog.timestamp.desc()).offset(skip).limit(limit).all()
    return {"total": total, "logs": logs}

@app.post("/api/sync/test")
def test_sync(test_data: Optional[TestConfigSchema] = None, local_db: Session = Depends(get_local_db)):
    """Manually trigger a sync for testing."""
    from services.tally_service import TallyService

    if test_data and test_data.tally_host and test_data.tally_port:
        host = test_data.tally_host
        port = test_data.tally_port
    else:
        raise HTTPException(status_code=400, detail="Tally host and port are required")

    # Try to get MySQL engine (optional — only needed if saving data)
    mysql_db = None
    try:
        engine = get_mysql_engine(local_db)
        SessionMySQL = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        mysql_db = SessionMySQL()
    except Exception:
        mysql_db = None  # MySQL not configured yet — okay for XML preview

    # TallyService does NOT need DB — use it directly
    tally_service = TallyService(host, port)

    try:
        if test_data and test_data.request_xml:
            # Send custom XML to Tally
            response_text = tally_service._send_request(test_data.request_xml)
            parsed_data, entity_name = tally_service.parse_xml_to_dict(response_text)
            records_count = len(parsed_data)

            if records_count > 0 and entity_name and mysql_db:
                # Save to MySQL only if configured
                service = SyncService(mysql_db, host, port)
                config = mysql_db.query(SyncConfig).first()
                report_name = config.report_name if config and config.report_name else "Manual_Test_Report"
                unique_key = test_data.unique_key_field if test_data and test_data.unique_key_field else (config.unique_key_field if config else None)
                service.db_engine.sync_data(report_name, entity_name, parsed_data, unique_key_field=unique_key)
                msg = f"Connection successful. {records_count} records saved to database table."
            elif records_count > 0:
                msg = f"Connection successful. {records_count} records found. (Configure MySQL to save data)"
            else:
                msg = "Connection successful, but no records found to save."

            # Log (only if MySQL available)
            if mysql_db:
                conn_name = test_data.connection_name if test_data else None
                if records_count == 0 and conn_name:
                    latest_prior_log = mysql_db.query(SyncLog).filter(
                        SyncLog.connection_name == conn_name
                    ).order_by(SyncLog.timestamp.desc()).first()
                    if latest_prior_log and latest_prior_log.status == "SUCCESS" and (latest_prior_log.records_fetched or 0) == 0:
                        latest_prior_log.timestamp = func.now()
                        latest_prior_log.message = msg
                        mysql_db.commit()
                        return {"status": "SUCCESS", "message": msg, "response_xml": response_text}

                log_entry = SyncLog(
                    connection_name=conn_name,
                    status="SUCCESS",
                    message=msg,
                    records_fetched=records_count,
                    request_payload=test_data.request_xml[:50000] if test_data and test_data.request_xml else None,
                    response_payload=response_text[:50000] if response_text else None
                )
                mysql_db.add(log_entry)
                mysql_db.commit()

            return {"status": "SUCCESS", "message": msg, "response_xml": response_text}
        else:
            # Just test Tally connectivity
            tally_service.get_ledgers()
            return {"status": "SUCCESS", "message": "Connection to Tally successful."}
    except Exception as e:
        if mysql_db:
            log_entry = SyncLog(
                connection_name=test_data.connection_name if test_data else None,
                status="ERROR",
                message=str(e),
                records_fetched=0,
                request_payload=(test_data.request_xml[:50000] if test_data and test_data.request_xml else None),
                response_payload=None
            )
            mysql_db.add(log_entry)
            mysql_db.commit()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if mysql_db:
            mysql_db.close()


@app.post("/api/sync/start")
def start_sync(background_tasks: BackgroundTasks, test_data: Optional[TestConfigSchema] = None, db: Session = Depends(get_db)):
    """Starts a sync in the background and returns a log_id."""
    if test_data and test_data.tally_host and test_data.tally_port:
        host = test_data.tally_host
        port = test_data.tally_port
        connection_name = test_data.connection_name
    else:
        config = db.query(SyncConfig).first()
        if not config:
            raise HTTPException(status_code=400, detail="Configuration not set")
        host = config.tally_host
        port = config.tally_port
        connection_name = config.connection_name

    # Create the initial log entry
    log_entry = SyncLog(
        connection_name=connection_name,
        status="IN_PROGRESS",
        message="Sync started in background.",
        records_fetched=0
    )
    db.add(log_entry)
    db.commit()
    db.refresh(log_entry)
    log_id = log_entry.id

    # Fire background task
    def background_sync_task(log_id, host, port, connection_name, test_data_dict):
        # We need a new DB session for the background task
        engine = get_mysql_engine(next(get_local_db()))
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        bg_db = SessionLocal()
        try:
            service = SyncService(bg_db, host, port)
            service.run_sync(connection_name=connection_name, log_id=log_id, test_data_dict=test_data_dict)
        finally:
            bg_db.close()

    test_data_dict = test_data.dict() if test_data else None
    background_tasks.add_task(background_sync_task, log_id, host, port, connection_name, test_data_dict)
    
    return {"status": "SUCCESS", "message": "Sync started.", "log_id": log_id}

@app.get("/api/sync/progress/{log_id}")
def sync_progress(log_id: int, db: Session = Depends(get_db)):
    """Poll for the current progress of a sync."""
    log_entry = db.query(SyncLog).filter(SyncLog.id == log_id).first()
    if not log_entry:
        raise HTTPException(status_code=404, detail="Log entry not found")
        
    return {
        "log_id": log_entry.id,
        "status": log_entry.status,
        "records_fetched": log_entry.records_fetched,
        "message": log_entry.message,
        "response_payload": log_entry.response_payload
    }

import urllib.parse
from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy.exc import OperationalError

@app.get("/api/mysql/config", response_model=Optional[MysqlConfigSchema])
def get_mysql_config_api(db: Session = Depends(get_local_db)):
    config = db.query(MysqlConfig).first()
    if config:
        return config
    return None

@app.post("/api/mysql/config")
def save_mysql_config_api(config_data: MysqlConfigSchema, db: Session = Depends(get_local_db)):
    config = db.query(MysqlConfig).first()
    if not config:
        config = MysqlConfig()
        db.add(config)
        
    config.host = config_data.host
    config.port = config_data.port
    config.username = config_data.username
    config.password = config_data.password
    config.database_name = config_data.database_name
    db.commit()
    
    # Try to connect and create the internal tables in the new MySQL database
    try:
        encoded_password = urllib.parse.quote_plus(config_data.password or "")
        server_url = f"mysql+pymysql://{config_data.username}:{encoded_password}@{config_data.host}:{config_data.port}/"
        server_engine = sa_create_engine(server_url)
        with server_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{config_data.database_name}`"))
            
        engine = get_mysql_engine(db)
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        logger.error(f"Failed to create tables in target MySQL database: {e}")
        raise HTTPException(status_code=500, detail=f"Configuration saved, but failed to initialize tables: {str(e)}")
        
    return {"message": "MySQL Configuration saved successfully"}

@app.post("/api/mysql/validate")
def validate_mysql_config(config_data: MysqlConfigSchema):
    try:
        # URL encode password in case it has special chars like @
        encoded_password = urllib.parse.quote_plus(config_data.password or "")
        
        # Connect to MySQL server WITHOUT specifying database name first to create it if missing
        server_url = f"mysql+pymysql://{config_data.username}:{encoded_password}@{config_data.host}:{config_data.port}/"
        server_engine = sa_create_engine(server_url)
        with server_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{config_data.database_name}`"))

        # Now test connection to the specific database
        test_url = f"mysql+pymysql://{config_data.username}:{encoded_password}@{config_data.host}:{config_data.port}/{config_data.database_name}"
        test_engine = sa_create_engine(test_url)
        with test_engine.connect() as conn:
            pass # successful connection
        return {"status": "SUCCESS", "message": "MySQL Connection Successful & DB Ready"}
    except OperationalError as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e.orig)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection error: {str(e)}")

import webbrowser
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Determine base directory for PyInstaller or normal run
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

dist_dir = os.path.join(BASE_DIR, "frontend_dist")

if os.path.exists(dist_dir):
    assets_path = os.path.join(dist_dir, "assets")
    if os.path.exists(assets_path):
        app.mount("/assets", StaticFiles(directory=assets_path), name="assets")

    @app.get("/{full_path:path}")
    async def serve_react_app(full_path: str):
        no_cache_headers = {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
        file_path = os.path.join(dist_dir, full_path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return FileResponse(file_path, headers=no_cache_headers)
        return FileResponse(os.path.join(dist_dir, "index.html"), headers=no_cache_headers)

if __name__ == "__main__":
    import time
    import socket
    import webbrowser

    def wait_for_server(host="127.0.0.1", port=5009, timeout=25):
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.create_connection((host, port), timeout=0.5):
                    return True
            except OSError:
                time.sleep(0.3)
        return False

    def start_app_window():
        # Only launch desktop window and hide console if packaged as a frozen .exe
        if not getattr(sys, 'frozen', False):
            return

        # Wait until FastAPI server is completely bound and accepting HTTP requests
        wait_for_server("127.0.0.1", 5009, timeout=25)
        time.sleep(0.4)
        url = "http://127.0.0.1:5009"
        
        # Hide the console window safely AFTER server initialization has started
        if sys.platform == "win32":
            try:
                import ctypes
                hWnd = ctypes.windll.kernel32.GetConsoleWindow()
                if hWnd != 0:
                    ctypes.windll.user32.ShowWindow(hWnd, 0)
            except Exception:
                pass

        # 1. Try launching in pure App Window Mode (Edge)
        edge_candidates = [
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe")
        ]
        for edge_exe in edge_candidates:
            if os.path.exists(edge_exe):
                try:
                    subprocess.Popen([edge_exe, f"--app={url}", "--window-size=1280,800"])
                    return
                except Exception:
                    pass

        # 2. Try Chrome App Window Mode
        chrome_candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe")
        ]
        for chrome_exe in chrome_candidates:
            if os.path.exists(chrome_exe):
                try:
                    subprocess.Popen([chrome_exe, f"--app={url}", "--window-size=1280,800"])
                    return
                except Exception:
                    pass

        # 3. Fallback to default browser
        webbrowser.open(url)

    # Launch UI Window in background thread only when running as .exe
    if getattr(sys, 'frozen', False):
        threading.Thread(target=start_app_window, daemon=True).start()
    
    # Run FastAPI server
    uvicorn.run(app, host="127.0.0.1", port=5009)
