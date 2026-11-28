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
from models import SyncConfig, SyncLog, MysqlConfig, RemoteSyncConfig
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

def _safe_create_engine(db_url, db_type=None, **kwargs):
    """Wrapper around create_engine that adds use_setinputsizes=False for SQL Server
    to fix HY104 errors with legacy ODBC drivers."""
    if db_type and db_type.lower() == "sqlserver" or "mssql" in str(db_url):
        kwargs.setdefault("use_setinputsizes", False)
    return create_engine(db_url, **kwargs)

def build_engine_url(db_type, host, port, username, password, database):
    db_type = (db_type or "mysql").lower()
    if db_type == "sqlserver":
        server_str = str(host or "localhost")
        if port and str(port).strip() not in ["1433", "0", ""] and "," not in server_str and "\\" not in server_str:
            server_str = f"{server_str},{port}"

        is_windows_auth = (not username or username.strip() == "") and (not password)
        
        modern_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 11 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server",  # Legacy fallback — works with use_setinputsizes=False
        ]
        driver = None
        try:
            import pyodbc
            available_drivers = pyodbc.drivers()
            for md in modern_drivers:
                if md in available_drivers:
                    driver = md
                    break
        except Exception:
            driver = None

        if driver:
            if is_windows_auth:
                conn_str = f"DRIVER={{{driver}}};SERVER={server_str};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;"
            else:
                conn_str = f"DRIVER={{{driver}}};SERVER={server_str};DATABASE={database};UID={username};PWD={password or ''};TrustServerCertificate=yes;Encrypt=no;"
            return f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
        else:
            # Fallback to pure built-in pymssql (requires zero external ODBC drivers)
            encoded_password = urllib.parse.quote_plus(password or "")
            user_part = f"{username}:{encoded_password}@" if username else ""
            port_part = f":{port}" if port and "\\" not in server_str else ""
            return f"mssql+pymssql://{user_part}{server_str}{port_part}/{database}?charset=utf8"
    else:
        encoded_password = urllib.parse.quote_plus(password or "")
        port_part = f":{port}" if port else ":3306"
        return f"mysql+pymysql://{username}:{encoded_password}@{host}{port_part}/{database}"

def get_mysql_engine(local_db: Session):
    global _mysql_engine, _mysql_db_url
    mysql_config = local_db.query(MysqlConfig).first()
    if not mysql_config:
        raise Exception("Database Configuration not found.")
    
    db_type = (getattr(mysql_config, 'db_type', None) or "mysql").lower()
    db_url = build_engine_url(
        db_type,
        mysql_config.host,
        mysql_config.port,
        mysql_config.username,
        mysql_config.password,
        mysql_config.database_name
    )
    
    # Return cached engine if the URL hasn't changed
    if _mysql_engine is not None and _mysql_db_url == db_url:
        return _mysql_engine
        
    # If it changed or doesn't exist, create a new one
    if _mysql_engine is not None:
        _mysql_engine.dispose()
        
    _mysql_engine = _safe_create_engine(db_url, db_type=db_type, pool_pre_ping=True, pool_size=10, max_overflow=20)
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

def ensure_database_exists(db_type, host, port, username, password, database_name):
    """Ensures target database exists on MySQL or SQL Server before connecting."""
    if not database_name:
        return
    db_type = (db_type or "mysql").lower()
    from sqlalchemy import create_engine as sa_create_engine, text
    try:
        if db_type == "sqlserver":
            master_url = build_engine_url(db_type, host, port, username, password, "master")
            master_engine = _safe_create_engine(master_url, db_type=db_type, isolation_level="AUTOCOMMIT")
            with master_engine.connect() as conn:
                conn.execute(text(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{database_name}') CREATE DATABASE [{database_name}]"))
            master_engine.dispose()
        else:
            encoded_password = urllib.parse.quote_plus(password or "")
            port_part = f":{port}" if port else ":3306"
            root_url = f"mysql+pymysql://{username}:{encoded_password}@{host}{port_part}/"
            root_engine = sa_create_engine(root_url)
            with root_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{database_name}`"))
            root_engine.dispose()
    except Exception as e:
        logger.debug(f"ensure_database_exists notice: {e}")

def get_engine_for_config(config, local_db: Session = None):
    """Returns engine for a specific SyncConfig based on its stored database credentials, or falls back to global."""
    from sqlalchemy import create_engine as sa_create_engine
    if config and config.db_host and config.database_name:
        ensure_database_exists(
            config.db_type,
            config.db_host,
            config.db_port,
            config.db_username,
            config.db_password,
            config.database_name
        )
        db_url = build_engine_url(
            config.db_type,
            config.db_host,
            config.db_port,
            config.db_username,
            config.db_password,
            config.database_name
        )
        return _safe_create_engine(db_url, db_type=config.db_type, pool_pre_ping=True)
    
    if local_db:
        return get_mysql_engine(local_db)
    
    db = next(get_local_db())
    try:
        return get_mysql_engine(db)
    finally:
        db.close()

def sync_existing_connections_to_local_sqlite(local_db: Session):
    """No-op: All connections are strictly user-managed."""
    pass

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
    id: Optional[int] = None
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
    db_type: Optional[str] = "mysql"
    db_host: Optional[str] = None
    db_port: Optional[int] = None
    db_username: Optional[str] = None
    db_password: Optional[str] = None
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
    db_type: Optional[str] = "mysql"
    host: str
    port: Optional[int] = None
    username: Optional[str] = ""
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
    # Auto-recover/mirror existing connections from MySQL and SQL Server into local SQLite
    sync_existing_connections_to_local_sqlite(local_db)
    
    configs = local_db.query(SyncConfig).order_by(SyncConfig.id.desc()).all()
    result = []
    for config in configs:
        db_name = config.database_name or ""
        total_rows = 0
        rep_name = (config.report_name or "").strip()
        table_name = ''.join(c for c in rep_name.lower() if c.isalnum() or c == '_')
        if table_name:
            try:
                target_engine = get_engine_for_config(config, local_db)
                from sqlalchemy import inspect
                inspector = inspect(target_engine)
                all_tables = inspector.get_table_names()
                all_tables_lower = {t.lower(): t for t in all_tables}
                
                matched_table = None
                if "tally_table_metadata" in all_tables_lower:
                    meta_tbl = all_tables_lower["tally_table_metadata"]
                    with target_engine.connect() as conn:
                        try:
                            meta_rows = conn.execute(
                                text(f"SELECT table_name FROM {meta_tbl} WHERE UPPER(report_name) = UPPER(:rep) ORDER BY id DESC"),
                                {"rep": rep_name}
                            ).fetchall()
                            for mr in meta_rows:
                                if mr[0] and mr[0].lower() in all_tables_lower:
                                    matched_table = all_tables_lower[mr[0].lower()]
                                    break
                        except Exception:
                            pass
                
                if not matched_table:
                    if table_name in all_tables_lower:
                        matched_table = all_tables_lower[table_name]
                    else:
                        for t_low, t_orig in all_tables_lower.items():
                            if t_low.startswith(table_name) and not t_low.endswith("_hash"):
                                matched_table = t_orig
                                break

                if matched_table:
                    is_mssql = target_engine.dialect.name == "mssql"
                    quoted_tbl = f"[{matched_table}]" if is_mssql else f"`{matched_table}`"
                    with target_engine.connect() as conn:
                        row_count = None
                        if config.company_name:
                            try:
                                count_q = text(f"SELECT COUNT(*) FROM {quoted_tbl} WHERE COMPANY_NAME = :comp")
                                row_count = conn.execute(count_q, {"comp": config.company_name}).scalar()
                            except Exception:
                                pass
                        if row_count is None:
                            count_q = text(f"SELECT COUNT(*) FROM {quoted_tbl}")
                            row_count = conn.execute(count_q).scalar()
                        total_rows = int(row_count) if row_count is not None else 0
            except Exception as e:
                logger.debug(f"Error calculating rows for {config.report_name}: {e}")
                total_rows = 0

        result.append({
            "id": config.id,
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
            "db_type": config.db_type or "mysql",
            "db_host": config.db_host,
            "db_port": config.db_port,
            "db_username": config.db_username,
            "db_password": config.db_password,
            "total_rows": total_rows
        })
    return result


@app.get("/api/sync/live-status")
def get_live_sync_status(local_db: Session = Depends(get_local_db)):
    """Lightweight non-blocking endpoint for AJAX polling: returns latest sync status & row count from SyncLog without heavy table COUNT(*) queries."""
    configs = local_db.query(SyncConfig).order_by(SyncConfig.id.desc()).all()
    statuses = {}
    for cfg in configs:
        cname = cfg.connection_name
        if not cname:
            continue
        try:
            engine = get_engine_for_config(cfg, local_db)
            SessionTarget = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            with SessionTarget() as tdb:
                latest_log = (
                    tdb.query(SyncLog)
                    .filter(SyncLog.connection_name == cname)
                    .order_by(SyncLog.id.desc())
                    .first()
                )
                if latest_log:
                    statuses[cname] = {
                        "status": latest_log.status,
                        "records_fetched": latest_log.records_fetched or 0,
                        "timestamp": latest_log.timestamp.isoformat() if latest_log.timestamp else None,
                    }
        except Exception as e:
            logger.debug(f"live-status check skipped for {cname}: {e}")
    return {"statuses": statuses}


@app.post("/api/config")
def update_config(config_data: ConfigSchema, local_db: Session = Depends(get_local_db)):
    if not config_data.connection_name or not config_data.connection_name.strip():
        raise HTTPException(status_code=400, detail="Connection Name is required and cannot be empty.")

    # Check if a connection with this name already exists in local SQLite
    existing_config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == config_data.connection_name.strip()).first()
    if existing_config:
        raise HTTPException(status_code=400, detail=f"Connection name '{config_data.connection_name.strip()}' already exists. Please choose a unique name.")
        
    config = SyncConfig()
    config.connection_name = config_data.connection_name.strip()
    config.tally_host = config_data.tally_host
    config.tally_port = config_data.tally_port
    config.company_name = config_data.company_name
    config.report_name = config_data.report_name
    config.scheduler_timing = config_data.scheduler_timing
    import json
    config.file_format = config_data.file_format
    config.request_xml = config_data.request_xml
    config.unique_key_field = config_data.unique_key_field
    if config_data.database_name:
        config.database_name = config_data.database_name
    if config_data.db_type:
        config.db_type = config_data.db_type
    if config_data.db_host:
        config.db_host = config_data.db_host
    if config_data.db_port:
        config.db_port = config_data.db_port
    if config_data.db_username is not None:
        config.db_username = config_data.db_username
    if config_data.db_password is not None:
        config.db_password = config_data.db_password
    if config_data.dynamic_filters:
        config.dynamic_filters = json.dumps(config_data.dynamic_filters)
    local_db.add(config)
    local_db.commit()

    # Mirror to remote target database's sync_config table
    try:
        target_engine = get_engine_for_config(config, local_db)
        Base.metadata.create_all(bind=target_engine)
        TargetSession = sessionmaker(autocommit=False, autoflush=False, bind=target_engine)
        with TargetSession() as target_session:
            remote_cfg = target_session.query(RemoteSyncConfig).filter(RemoteSyncConfig.connection_name == config_data.connection_name.strip()).first()
            if not remote_cfg:
                remote_cfg = RemoteSyncConfig(
                    connection_name=config_data.connection_name.strip(),
                    tally_host=config_data.tally_host,
                    tally_port=config_data.tally_port,
                    company_name=config_data.company_name,
                    report_name=config_data.report_name,
                    request_xml=config_data.request_xml,
                    scheduler_timing=config_data.scheduler_timing,
                    file_format=config_data.file_format,
                    database_name=config_data.database_name
                )
                target_session.add(remote_cfg)
            target_session.commit()
    except Exception as e:
        logger.warning(f"Could not mirror config to target database: {e}")

    update_scheduler_job()
    return {"message": "Configuration saved successfully"}

@app.put("/api/config/{connection_name}")
def modify_config(connection_name: str, config_data: ConfigSchema, local_db: Session = Depends(get_local_db)):
    config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
    if not config:
        raise HTTPException(status_code=404, detail="Connection not found")
        
    config.tally_host = config_data.tally_host
    config.tally_port = config_data.tally_port
    config.company_name = config_data.company_name
    
    # If the user changed the report name, reset last_alter_id to 0
    if config.report_name != config_data.report_name:
        config.last_alter_id = 0
        
    config.report_name = config_data.report_name
    if config_data.database_name:
        config.database_name = config_data.database_name
    if config_data.db_type:
        config.db_type = config_data.db_type
    if config_data.db_host:
        config.db_host = config_data.db_host
    if config_data.db_port:
        config.db_port = config_data.db_port
    if config_data.db_username is not None:
        config.db_username = config_data.db_username
    if config_data.db_password is not None:
        config.db_password = config_data.db_password
    import json
    config.scheduler_timing = config_data.scheduler_timing
    config.file_format = config_data.file_format
    config.request_xml = config_data.request_xml
    config.unique_key_field = config_data.unique_key_field
    if config_data.dynamic_filters is not None:
        config.dynamic_filters = json.dumps(config_data.dynamic_filters)
    local_db.commit()

    # Mirror to remote target database's sync_config table
    try:
        target_engine = get_engine_for_config(config, local_db)
        TargetSession = sessionmaker(autocommit=False, autoflush=False, bind=target_engine)
        with TargetSession() as target_session:
            remote_cfg = target_session.query(RemoteSyncConfig).filter(RemoteSyncConfig.connection_name == connection_name).first()
            if remote_cfg:
                remote_cfg.tally_host = config_data.tally_host
                remote_cfg.tally_port = config_data.tally_port
                remote_cfg.company_name = config_data.company_name
                remote_cfg.report_name = config_data.report_name
                remote_cfg.request_xml = config_data.request_xml
                remote_cfg.scheduler_timing = config_data.scheduler_timing
                remote_cfg.file_format = config_data.file_format
                remote_cfg.database_name = config_data.database_name
                target_session.commit()
    except Exception as e:
        logger.warning(f"Could not mirror update to target database: {e}")

    update_scheduler_job()
    return {"message": "Configuration updated successfully"}

class ToggleSchedulerSchema(BaseModel):
    scheduler_timing: Optional[str] = "5"

@app.post("/api/config/{connection_name}/toggle-scheduler")
def toggle_scheduler(connection_name: str, payload: ToggleSchedulerSchema, local_db: Session = Depends(get_local_db)):
    config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
    if not config:
        raise HTTPException(status_code=404, detail="Connection not found")
    config.scheduler_timing = payload.scheduler_timing
    local_db.commit()
    update_scheduler_job()
    return {"message": "Scheduler updated successfully", "scheduler_timing": config.scheduler_timing}

@app.delete("/api/config")
@app.delete("/api/config/{identifier}")
def delete_config(identifier: Optional[str] = None, local_db: Session = Depends(get_local_db)):
    config = None
    if identifier:
        # 1. Try finding by ID if numeric
        if identifier.isdigit():
            config = local_db.query(SyncConfig).filter(SyncConfig.id == int(identifier)).first()
        
        # 2. Try finding by connection_name
        if not config:
            config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == identifier).first()

        # 3. Fallback for blank/empty identifier keyword
        if not config and identifier in ("blank", "empty", "null", "none"):
            config = local_db.query(SyncConfig).filter((SyncConfig.connection_name == "") | (SyncConfig.connection_name.is_(None))).first()
    else:
        # If no identifier was passed in path (DELETE /api/config), delete the first blank connector
        config = local_db.query(SyncConfig).filter((SyncConfig.connection_name == "") | (SyncConfig.connection_name.is_(None))).first()
        
    if config:
        conn_name = config.connection_name or f"ID #{config.id}"
        # Mirror delete from remote target database and clean up data table, metadata, and logs
        try:
            target_engine = get_engine_for_config(config, local_db)
            
            # 1. Delete from remote sync_config
            try:
                TargetSession = sessionmaker(autocommit=False, autoflush=False, bind=target_engine)
                with TargetSession() as target_session:
                    remote_cfg = target_session.query(RemoteSyncConfig).filter(RemoteSyncConfig.connection_name == config.connection_name).first()
                    if remote_cfg:
                        target_session.delete(remote_cfg)
                        target_session.commit()
            except Exception as e:
                logger.warning(f"Could not mirror delete to target database: {e}")

            # 2. Delete logs for this connection
            try:
                with target_engine.connect() as conn:
                    conn.execute(text("DELETE FROM sync_logs WHERE connection_name = :cname"), {"cname": config.connection_name})
                    conn.commit()
            except Exception:
                pass

            # 3. Clean up the report's generated data table and metadata from target DB
            rep_name = (config.report_name or "").strip()
            table_name = ''.join(c for c in rep_name.lower() if c.isalnum() or c == '_')
            if table_name:
                from sqlalchemy import inspect
                inspector = inspect(target_engine)
                all_tables = inspector.get_table_names()
                all_tables_lower = {t.lower(): t for t in all_tables}
                
                matched_table = None
                if "tally_table_metadata" in all_tables_lower:
                    meta_tbl = all_tables_lower["tally_table_metadata"]
                    with target_engine.connect() as conn:
                        try:
                            meta_row = conn.execute(
                                text(f"SELECT table_name FROM {meta_tbl} WHERE UPPER(report_name) = UPPER(:rep)"),
                                {"rep": rep_name}
                            ).fetchone()
                            if meta_row and meta_row[0] in all_tables:
                                matched_table = meta_row[0]
                            # Clean up metadata row
                            conn.execute(
                                text(f"DELETE FROM {meta_tbl} WHERE UPPER(report_name) = UPPER(:rep)"),
                                {"rep": rep_name}
                            )
                            conn.commit()
                        except Exception:
                            pass
                
                if not matched_table:
                    if table_name in all_tables_lower:
                        matched_table = all_tables_lower[table_name]
                    else:
                        for t_low, t_orig in all_tables_lower.items():
                            if t_low.startswith(table_name) and not t_low.endswith("_hash"):
                                matched_table = t_orig
                                break
                
                if matched_table:
                    with target_engine.connect() as conn:
                        is_mssql = target_engine.dialect.name == "mssql"
                        quoted_tbl = f"[{matched_table}]" if is_mssql else f"`{matched_table}`"
                        conn.execute(text(f"DROP TABLE IF EXISTS {quoted_tbl}"))
                        conn.commit()
                        logger.info(f"Dropped table {matched_table} upon connection deletion.")
        except Exception as e:
            logger.warning(f"Could not complete remote clean up for deleted connection: {e}")

        local_db.delete(config)
        local_db.commit()
        update_scheduler_job()
        return {"message": f"Connector '{conn_name}' deleted successfully"}
        
    return {"message": "No connector found"}

@app.get("/api/logs")
def get_logs(skip: int = 0, limit: int = 10, connection_name: Optional[str] = None, db: Session = Depends(get_db), local_db: Session = Depends(get_local_db)):
    if connection_name:
        config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
        if config:
            try:
                target_engine = get_engine_for_config(config, local_db)
                TargetSession = sessionmaker(autocommit=False, autoflush=False, bind=target_engine)
                with TargetSession() as target_db:
                    query = target_db.query(SyncLog).filter(SyncLog.connection_name == connection_name)
                    total = query.with_entities(func.count(SyncLog.id)).scalar() or 0
                    logs = query.order_by(SyncLog.timestamp.desc()).offset(skip).limit(limit).all()
                    return {"total": total, "logs": logs}
            except Exception as e:
                logger.debug(f"Could not get logs via connection engine for '{connection_name}': {e}")

    query = db.query(SyncLog)
    if connection_name:
        query = query.filter(SyncLog.connection_name == connection_name)
    
    total = query.with_entities(func.count(SyncLog.id)).scalar() or 0
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
        host = "127.0.0.1"
        port = 9000

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
                config = (
                    local_db.query(SyncConfig).filter(SyncConfig.connection_name == test_data.connection_name).first()
                    if test_data and test_data.connection_name
                    else local_db.query(SyncConfig).first()
                )
                report_name = config.report_name if config and config.report_name else "Manual_Test_Report"
                unique_key = test_data.unique_key_field if test_data and test_data.unique_key_field else (config.unique_key_field if config else None)
                comp_name = test_data.company_name if test_data and test_data.company_name else (config.company_name if config else None)
                service.db_engine.sync_data(report_name, entity_name, parsed_data, unique_key_field=unique_key, company_name=comp_name)
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
            # Just test Tally connectivity using active company XML
            tally_service.test_connection()
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
def start_sync(background_tasks: BackgroundTasks, test_data: Optional[TestConfigSchema] = None, local_db: Session = Depends(get_local_db)):
    """Starts a sync in the background and returns a log_id."""
    if test_data and test_data.tally_host and test_data.tally_port:
        host = test_data.tally_host
        port = test_data.tally_port
        connection_name = test_data.connection_name
    else:
        config = local_db.query(SyncConfig).first()
        if not config:
            raise HTTPException(status_code=400, detail="Configuration not set")
        host = config.tally_host
        port = config.tally_port
        connection_name = config.connection_name

    target_config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
    target_engine = get_engine_for_config(target_config, local_db)
    Base.metadata.create_all(bind=target_engine)
    TargetSession = sessionmaker(autocommit=False, autoflush=False, bind=target_engine)
    target_db = TargetSession()
    try:
        # Create the initial log entry in the target database
        log_entry = SyncLog(
            connection_name=connection_name,
            status="IN_PROGRESS",
            message="Sync started in background.",
            records_fetched=0
        )
        target_db.add(log_entry)
        target_db.commit()
        target_db.refresh(log_entry)
        log_id = log_entry.id
    finally:
        target_db.close()

    # Fire background task
    def background_sync_task(log_id, host, port, connection_name, test_data_dict):
        loc_db = next(get_local_db())
        try:
            cfg = loc_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
            eng = get_engine_for_config(cfg, loc_db)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=eng)
            bg_db = SessionLocal()
            try:
                service = SyncService(bg_db, host, port)
                service.run_sync(connection_name=connection_name, log_id=log_id, test_data_dict=test_data_dict)
            finally:
                bg_db.close()
        finally:
            loc_db.close()

    test_data_dict = test_data.dict() if test_data else None
    background_tasks.add_task(background_sync_task, log_id, host, port, connection_name, test_data_dict)
    
    return {"status": "SUCCESS", "message": "Sync started.", "log_id": log_id}

@app.get("/api/sync/progress/{log_id}")
def sync_progress(log_id: int, local_db: Session = Depends(get_local_db)):
    """Poll for the current progress of a sync."""
    target_cfg = local_db.query(SyncConfig).order_by(SyncConfig.id.desc()).first()
    if target_cfg:
        try:
            eng = get_engine_for_config(target_cfg, local_db)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=eng)
            with SessionLocal() as s:
                log_entry = s.query(SyncLog).filter(SyncLog.id == log_id).first()
                if log_entry:
                    return {
                        "log_id": log_entry.id,
                        "status": log_entry.status,
                        "records_fetched": log_entry.records_fetched,
                        "message": log_entry.message,
                        "response_payload": log_entry.response_payload
                    }
        except Exception:
            pass

    try:
        eng = get_mysql_engine(local_db)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=eng)
        with SessionLocal() as s:
            log_entry = s.query(SyncLog).filter(SyncLog.id == log_id).first()
            if log_entry:
                return {
                    "log_id": log_entry.id,
                    "status": log_entry.status,
                    "records_fetched": log_entry.records_fetched,
                    "message": log_entry.message,
                    "response_payload": log_entry.response_payload
                }
    except Exception:
        pass

    raise HTTPException(status_code=404, detail="Log entry not found")

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
        
    config.db_type = (config_data.db_type or "mysql").lower()
    config.host = config_data.host
    config.port = config_data.port
    config.username = config_data.username
    config.password = config_data.password
    config.database_name = config_data.database_name
    db.commit()
    
    # Try to connect and create the database if not exists, and create tables in the database
    try:
        encoded_password = urllib.parse.quote_plus(config_data.password or "")
        db_type = (config_data.db_type or "mysql").lower()
        
        if db_type == "sqlserver":
            # In SQL Server, connect to master to check/create database
            server_url = build_engine_url(db_type, config_data.host, config_data.port, config_data.username, config_data.password, "master")
            server_engine = _safe_create_engine(server_url, db_type=db_type, isolation_level="AUTOCOMMIT")
            with server_engine.connect() as conn:
                conn.execute(text(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{config_data.database_name}') CREATE DATABASE [{config_data.database_name}]"))
        else:
            # In MySQL, connect to server without database
            server_url = f"mysql+pymysql://{config_data.username}:{encoded_password}@{config_data.host}:{config_data.port}/"
            server_engine = sa_create_engine(server_url)
            with server_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{config_data.database_name}`"))
            
        engine = get_mysql_engine(db)
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        logger.error(f"Failed to create tables in target database: {e}")
        raise HTTPException(status_code=500, detail=f"Configuration saved, but failed to initialize tables: {str(e)}")
        
    return {"message": "Database Configuration saved successfully"}

@app.post("/api/mysql/validate")
def validate_mysql_config(config_data: MysqlConfigSchema):
    try:
        encoded_password = urllib.parse.quote_plus(config_data.password or "")
        db_type = (config_data.db_type or "mysql").lower()
        
        if db_type == "sqlserver":
            # Connect to master database first to create database if not exists
            server_url = build_engine_url(db_type, config_data.host, config_data.port, config_data.username, config_data.password, "master")
            server_engine = _safe_create_engine(server_url, db_type=db_type, isolation_level="AUTOCOMMIT")
            with server_engine.connect() as conn:
                conn.execute(text(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{config_data.database_name}') CREATE DATABASE [{config_data.database_name}]"))

            # Now test connection to the specific database
            test_url = build_engine_url(db_type, config_data.host, config_data.port, config_data.username, config_data.password, config_data.database_name)
            test_engine = _safe_create_engine(test_url, db_type=db_type)
            with test_engine.connect() as conn:
                pass # successful connection
            return {"status": "SUCCESS", "message": "SQL Server Connection Successful & Database Ready"}
        else:
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
            return {"status": "SUCCESS", "message": "MySQL Connection Successful & Database Ready"}
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
