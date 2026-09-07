import logging
from sqlalchemy.orm import Session
from models import SyncLog, SyncConfig
from services.tally_service import TallyService
from services.dynamic_db_engine import DynamicDbEngine

logger = logging.getLogger(__name__)

class SyncService:
    def __init__(self, db: Session, host: str, port: int):
        self.db = db
        self.tally_service = TallyService(host, port)
        self.db_engine = DynamicDbEngine(db)

    def run_sync(self, connection_name=None):
        request_payload = None
        response_payload = None
        try:
            logger.info("Starting Tally synchronization...")
            
            # Fetch config to get request_xml and report_name
            if connection_name:
                config = self.db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
            else:
                config = self.db.query(SyncConfig).first()
                
            if not config:
                raise Exception("No sync configuration found. Please validate connection in the UI.")
                
            report_name = config.report_name or "Unknown Report"
            request_payload = config.request_xml
            
            if not request_payload:
                logger.warning("No request_xml found in config. Using fallback ledgers request.")
                response_payload = self.tally_service.get_ledgers()
                request_payload = "Fallback Ledgers Request"
            else:
                last_alter_id = config.last_alter_id or 0
                if "{LAST_ALTER_ID}" in request_payload:
                    request_payload = request_payload.replace("{LAST_ALTER_ID}", str(last_alter_id))
                    logger.info(f"Replaced {{LAST_ALTER_ID}} with {last_alter_id}")
                
                response_payload = self.tally_service._send_request(request_payload)
            
            # Parse dynamic data
            parsed_data, entity_name = self.tally_service.parse_xml_to_dict(response_payload)
            
            records_count = len(parsed_data)
            
            if records_count > 0 and entity_name:
                self.db_engine.sync_data(report_name, entity_name, parsed_data)
                
                # Extract max ALTERID and update
                max_alter_id = 0
                for record in parsed_data:
                    for key, val in record.items():
                        if key.upper() in ["ALTERID", "ATTR_ALTERID"]:
                            try:
                                aid = int(val)
                                if aid > max_alter_id:
                                    max_alter_id = aid
                            except (ValueError, TypeError):
                                pass
                
                if max_alter_id > (config.last_alter_id or 0):
                    config.last_alter_id = max_alter_id
                    logger.info(f"Updated last_alter_id to {max_alter_id}")
            elif records_count == 0:
                logger.info("No records found in the Tally response.")
            else:
                logger.warning("Could not determine entity name from Tally response.")
            
            # Log success
            log_entry = SyncLog(
                connection_name=connection_name,
                status="SUCCESS", 
                message="Sync completed successfully.", 
                records_fetched=records_count,
                request_payload=request_payload,
                response_payload=response_payload
            )
            self.db.add(log_entry)
            self.db.commit()
            
            logger.info(f"Sync completed. {records_count} records fetched and saved.")
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            # Log failure
            log_entry = SyncLog(
                connection_name=connection_name,
                status="ERROR", 
                message=str(e), 
                records_fetched=0,
                request_payload=request_payload,
                response_payload=response_payload
            )
            self.db.add(log_entry)
            self.db.commit()
