import logging
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from database.config import LocalSessionLocal
from models import SyncConfig
from services.sync_service import SyncService
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Initialize scheduler
scheduler = BackgroundScheduler()

def sync_connection_job(connection_name: str):
    """Job that runs periodically for a specific connection."""
    print(f"\n=======================================================", flush=True)
    print(f"[SCHEDULER] Auto-Sync Triggered for: '{connection_name}'", flush=True)
    print(f"=======================================================", flush=True)
    logger.info(f"Scheduler triggered sync job for connection: {connection_name}")
    local_db = LocalSessionLocal()
    db = None
    try:
        config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
        if not config:
            print(f"[SCHEDULER] [WARNING] Connection '{connection_name}' not found. Skipping.", flush=True)
            logger.warning(f"Connection '{connection_name}' not found. Skipping sync.")
            return

        # Double check if still active
        timing = (config.scheduler_timing or "").strip().lower()
        if not timing or timing in ["inactive", "off", "0", "none"]:
            print(f"[SCHEDULER] [INFO] '{connection_name}' is inactive. Skipping.", flush=True)
            logger.info(f"Scheduler for '{connection_name}' is inactive. Skipping.")
            return

        try:
            from main import get_engine_for_config
            engine = get_engine_for_config(config, local_db)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            db = SessionLocal()
        except Exception as e:
            err_msg = f"Failed to get target database connection: {e}"
            print(f"[SCHEDULER] [FAILED] '{connection_name}': {err_msg}", flush=True)
            logger.error(f"Failed to get target database connection in scheduler for {connection_name}: {e}")
            return

        # Double check if still active
        timing = (config.scheduler_timing or "").strip().lower()
        if not timing or timing in ["inactive", "off", "0", "none"]:
            print(f"[SCHEDULER] [INFO] '{connection_name}' is inactive. Skipping.", flush=True)
            logger.info(f"Scheduler for '{connection_name}' is inactive. Skipping.")
            return

        host = config.tally_host
        port = config.tally_port
        
        print(f"[SCHEDULER] [RUNNING] Syncing '{connection_name}' ({host}:{port}) ...", flush=True)
        service = SyncService(db, host, port)
        service.run_sync(connection_name=config.connection_name)
        
        # Check latest log status
        from models import SyncLog
        latest_log = db.query(SyncLog).filter(SyncLog.connection_name == connection_name).order_by(SyncLog.timestamp.desc()).first()
        if latest_log and latest_log.status == "SUCCESS":
            print(f"[SCHEDULER] [SUCCESS] Auto-Sync completed for '{connection_name}' | Records: {latest_log.records_fetched}", flush=True)
        elif latest_log and latest_log.status == "ERROR":
            print(f"[SCHEDULER] [ERROR] '{connection_name}': {latest_log.message}", flush=True)
        else:
            print(f"[SCHEDULER] [COMPLETED] Auto-Sync finished for '{connection_name}'", flush=True)
    except Exception as e:
        print(f"[SCHEDULER] [EXCEPTION] Error in sync for '{connection_name}': {e}", flush=True)
        logger.error(f"Error in sync job for connection '{connection_name}': {e}")
    finally:
        print(f"=======================================================\n", flush=True)
        if db:
            db.close()
        local_db.close()

def sync_job():
    """Fallback legacy sync job for all configs."""
    update_scheduler_job()

def update_scheduler_job():
    """Update or add sync jobs connection-wise based on database configuration."""
    local_db = LocalSessionLocal()
    try:
        configs = local_db.query(SyncConfig).all()
        active_connection_names = {c.connection_name for c in configs}

        # Remove old global legacy job if exists
        if scheduler.get_job('tally_sync_job'):
            try:
                scheduler.remove_job('tally_sync_job')
            except Exception:
                pass

        # Remove jobs for connections that no longer exist
        for job in scheduler.get_jobs():
            if job.id.startswith('sync_conn_'):
                conn_name = job.id.replace('sync_conn_', '')
                if conn_name not in active_connection_names:
                    try:
                        scheduler.remove_job(job.id)
                        logger.info(f"Removed scheduler job for removed connection: {conn_name}")
                    except Exception:
                        pass

        # Schedule or remove jobs per connection
        for config in configs:
            job_id = f"sync_conn_{config.connection_name}"
            timing = (config.scheduler_timing or "").strip().lower()
            
            # Check if active
            if not timing or timing in ["inactive", "off", "0", "none"]:
                if scheduler.get_job(job_id):
                    try:
                        scheduler.remove_job(job_id)
                        logger.info(f"Scheduler disabled for connection: {config.connection_name}")
                    except Exception:
                        pass
                continue

            # Determine trigger
            if ' ' in timing:
                trigger = CronTrigger.from_crontab(timing)
                logger.info(f"Scheduling connection '{config.connection_name}' with cron: {timing}")
            elif timing == 'daily':
                trigger = CronTrigger(hour=0, minute=0)
                logger.info(f"Scheduling connection '{config.connection_name}' daily at midnight.")
            elif timing == 'hourly':
                trigger = CronTrigger(minute=0)
                logger.info(f"Scheduling connection '{config.connection_name}' hourly.")
            elif timing.isdigit():
                minutes = int(timing)
                if minutes <= 0:
                    minutes = 5
                trigger = IntervalTrigger(minutes=minutes)
                logger.info(f"Scheduling connection '{config.connection_name}' with interval: {minutes} minutes.")
            else:
                trigger = IntervalTrigger(minutes=5)
                logger.warning(f"Unknown scheduling format '{timing}' for '{config.connection_name}', defaulting to 5 minutes.")

            scheduler.add_job(
                sync_connection_job,
                trigger=trigger,
                id=job_id,
                args=[config.connection_name],
                replace_existing=True
            )
            logger.info(f"Registered scheduler job '{job_id}' for connection: {config.connection_name}")
    except Exception as e:
        logger.error(f"Error updating connection scheduler jobs: {e}")
    finally:
        local_db.close()

def start_scheduler():
    """Start the background scheduler."""
    if not scheduler.running:
        scheduler.start()
    update_scheduler_job()
    logger.info("APScheduler started.")

def stop_scheduler():
    """Stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown()

