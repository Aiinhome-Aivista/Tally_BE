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

    def run_sync(self, connection_name=None, log_id=None, parameters=None):
        request_payload = None
        
        # If log_id is not provided (e.g., from scheduler), create one
        if not log_id:
            log_entry = SyncLog(
                connection_name=connection_name,
                status="IN_PROGRESS", 
                message="Sync started.", 
                records_fetched=0
            )
            self.db.add(log_entry)
            self.db.commit()
            self.db.refresh(log_entry)
            log_id = log_entry.id
            
        try:
            logger.info(f"Starting Tally synchronization for log_id: {log_id}...")
            
            # Fetch config to get request_xml and report_name
            if connection_name:
                config = self.db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
            else:
                config = self.db.query(SyncConfig).first()
                
            if not config:
                raise Exception("No sync configuration found. Please validate connection in the UI.")
                
            report_name = config.report_name or "Unknown Report"
            request_payload = config.request_xml
            
            # Update log with request payload if available
            log_entry = self.db.query(SyncLog).filter(SyncLog.id == log_id).first()
            if request_payload and log_entry:
                log_entry.request_payload = request_payload[:50000]
                self.db.commit()
            
            if not request_payload:
                logger.warning("No request_xml found in config. Cannot perform streaming sync without XML request.")
                raise Exception("No request_xml found in config.")
                
            last_alter_id = config.last_alter_id or 0
            if "{LAST_ALTER_ID}" in request_payload:
                request_payload = request_payload.replace("{LAST_ALTER_ID}", str(last_alter_id))
                logger.info(f"Replaced {{LAST_ALTER_ID}} with {last_alter_id}")
                
            dynamic_params = parameters if parameters is not None else (config.parameters or {})
            if dynamic_params:
                for k, v in dynamic_params.items():
                    placeholder = f"{{{k}}}"
                    if placeholder in request_payload:
                        request_payload = request_payload.replace(placeholder, str(v))
                        logger.info(f"Replaced {placeholder} with {v}")
                    else:
                        # Auto-inject into STATICVARIABLES if placeholder is missing from XML
                        if "</STATICVARIABLES>" in request_payload.upper():
                            import re
                            injection = f"\n                <{k}>{v}</{k}>\n            </STATICVARIABLES>"
                            request_payload = re.sub(r'</STATICVARIABLES>', injection, request_payload, flags=re.IGNORECASE)
                            logger.info(f"Auto-injected <{k}>{v}</{k}> into STATICVARIABLES")
                        
            # Remove any remaining {KEY} placeholders that were not provided in parameters
            import re
            request_payload = re.sub(r'\{[A-Za-z0-9_]+\}', '', request_payload)
            
            # Use streaming request
            response_stream = self.tally_service._send_request_stream(request_payload)
            
            # Parse and insert dynamically in chunks
            batch_size = 5000
            current_batch = []
            total_records = 0
            max_alter_id = last_alter_id
            preview_data = None
            
            for item_data, entity_name in self.tally_service.parse_xml_iteratively(response_stream):
                current_batch.append(item_data)
                
                # Update progress in DB every 100 rows for smooth UI animation
                if len(current_batch) % 100 == 0:
                    log_entry = self.db.query(SyncLog).filter(SyncLog.id == log_id).first()
                    if log_entry:
                        log_entry.records_fetched = total_records + len(current_batch)
                        self.db.commit()
                        
                # Update max alter id
                for key, val in item_data.items():
                    if key.upper() in ["ALTERID", "ATTR_ALTERID"]:
                        try:
                            aid = int(val)
                            if aid > max_alter_id:
                                max_alter_id = aid
                        except (ValueError, TypeError):
                            pass
                
                # If batch size reached, insert to DB
                if len(current_batch) >= batch_size:
                    if total_records == 0:
                        xml_parts = ["<PREVIEW>"]
                        for item in current_batch:
                            xml_parts.append("  <RECORD>")
                            for k, v in item.items():
                                safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                                xml_parts.append(f"    <{safe_k}>{v}</{safe_k}>")
                            xml_parts.append("  </RECORD>")
                        xml_parts.append("</PREVIEW>")
                        preview_data = "\n".join(xml_parts)
                        
                    self.db_engine.sync_data(report_name, entity_name, current_batch)
                    total_records += len(current_batch)
                        
                    logger.info(f"Inserted batch of {len(current_batch)} records. Total: {total_records}")
                    current_batch = [] # Reset batch
            
            # Process remaining items
            if current_batch and entity_name:
                if total_records == 0:
                    xml_parts = ["<PREVIEW>"]
                    for item in current_batch:
                        xml_parts.append("  <RECORD>")
                        for k, v in item.items():
                            safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                            xml_parts.append(f"    <{safe_k}>{v}</{safe_k}>")
                        xml_parts.append("  </RECORD>")
                    xml_parts.append("</PREVIEW>")
                    preview_data = "\n".join(xml_parts)
                self.db_engine.sync_data(report_name, entity_name, current_batch)
                total_records += len(current_batch)
                
            # Update last alter id in config
            if max_alter_id > (config.last_alter_id or 0):
                config.last_alter_id = max_alter_id
                logger.info(f"Updated last_alter_id to {max_alter_id}")
            
            # Log success
            log_entry = self.db.query(SyncLog).filter(SyncLog.id == log_id).first()
            if log_entry:
                log_entry.status = "SUCCESS"
                log_entry.message = "Sync completed successfully."
                log_entry.records_fetched = total_records
                if preview_data:
                    log_entry.response_payload = preview_data
                self.db.commit()
            
            logger.info(f"Sync completed successfully. {total_records} records fetched and saved.")
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            # Log failure
            log_entry = self.db.query(SyncLog).filter(SyncLog.id == log_id).first()
            if log_entry:
                log_entry.status = "ERROR"
                log_entry.message = str(e)
                self.db.commit()
