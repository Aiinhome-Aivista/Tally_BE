import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from database.config import LocalSessionLocal
from models import SyncConfig
from services.sync_service import SyncService
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Initialize scheduler
scheduler = BackgroundScheduler()

def sync_job():
    """Job that runs periodically to sync data."""
    logger.info("Scheduler triggered sync job.")
    local_db = LocalSessionLocal()
    db = None
    try:
        try:
            from main import get_mysql_engine
            engine = get_mysql_engine(local_db)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            db = SessionLocal()
        except Exception as e:
            logger.error(f"Failed to get MySQL connection in scheduler: {e}")
            return
            
        # Get active configs
        configs = db.query(SyncConfig).all()
        if not configs:
            logger.warning("No Tally configuration found. Skipping sync.")
            return

        for config in configs:
            try:
                logger.info(f"Running sync for connection: {config.connection_name}")
                host = config.tally_host
                port = config.tally_port
                
                service = SyncService(db, host, port)
                service.run_sync(connection_name=config.connection_name)
            except Exception as e:
                logger.error(f"Error in sync job for {config.connection_name}: {e}")
    finally:
        if db:
            db.close()
        local_db.close()

import os
from dotenv import load_dotenv

load_dotenv()

def update_scheduler_job():
    """Update or add the sync job based on database configuration and .env timing."""
    local_db = LocalSessionLocal()
    db = None
    try:
        try:
            from main import get_mysql_engine
            engine = get_mysql_engine(local_db)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            db = SessionLocal()
        except Exception as e:
            logger.error(f"Failed to connect to MySQL to check scheduler config: {e}")
            return
            
        config = db.query(SyncConfig).first()
        timing_env = os.getenv("SCHEDULER_TIMING", "")
        
        if not config or not timing_env:
            if scheduler.get_job('tally_sync_job'):
                scheduler.remove_job('tally_sync_job')
                logger.info("Removed sync job because no config or timing was found.")
            return

        timing = timing_env.strip().lower()
        
        if ' ' in timing:
            trigger = CronTrigger.from_crontab(timing)
            logger.info(f"Scheduling job with cron: {timing}")
        elif timing == 'daily':
            trigger = CronTrigger(hour=0, minute=0)
            logger.info("Scheduling job daily at midnight.")
        elif timing == 'hourly':
            trigger = CronTrigger(minute=0)
            logger.info("Scheduling job hourly.")
        elif timing.isdigit():
            trigger = IntervalTrigger(minutes=int(timing))
            logger.info(f"Scheduling job with interval: {timing} minutes.")
        else:
            trigger = IntervalTrigger(minutes=5)
            logger.warning(f"Unknown scheduling format '{timing}', defaulting to 5 minutes.")

        scheduler.add_job(sync_job, trigger=trigger, id='tally_sync_job', replace_existing=True)
    except Exception as e:
        logger.error(f"Error updating scheduler job: {e}")
    finally:
        if db:
            db.close()
        local_db.close()

def start_scheduler():
    """Start the background scheduler."""
    scheduler.start()
    update_scheduler_job()
    logger.info("APScheduler started.")

def stop_scheduler():
    """Stop the scheduler."""
    scheduler.shutdown()
