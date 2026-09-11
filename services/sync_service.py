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

    def run_sync(self, connection_name=None, log_id=None, test_data_dict=None):
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
            
            # Dynamically fetch max alter ID from the actual data table
            db_max_alter_id = self.db_engine.get_max_alter_id(report_name)
            if db_max_alter_id > last_alter_id:
                last_alter_id = db_max_alter_id
                logger.info(f"Using actual max alter ID {last_alter_id} from database table.")
                
            if "{LAST_ALTER_ID}" in request_payload:
                request_payload = request_payload.replace("{LAST_ALTER_ID}", str(last_alter_id))
                logger.info(f"Replaced {{LAST_ALTER_ID}} with {last_alter_id}")
                
            if "{COMPANY_NAME}" in request_payload and config.company_name:
                request_payload = request_payload.replace("{COMPANY_NAME}", config.company_name)
                logger.info(f"Replaced {{COMPANY_NAME}} with {config.company_name}")
            
            import json
            filters = {}
            if test_data_dict and test_data_dict.get('dynamic_filters'):
                filters = test_data_dict['dynamic_filters']
            elif config.dynamic_filters:
                try:
                    filters = json.loads(config.dynamic_filters)
                except Exception:
                    pass
                    
            for k, v in filters.items():
                placeholder = f"{{{k}}}"
                if placeholder in request_payload:
                    request_payload = request_payload.replace(placeholder, str(v))
                    logger.info(f"Replaced {placeholder} with {v}")
                    
            unique_key = test_data_dict.get('unique_key_field') if test_data_dict else config.unique_key_field
            
            # Use streaming request
            response_stream = self.tally_service._send_request_stream(request_payload)
            
            # Parse and insert dynamically in chunks
            batch_size = 5000
            # Key: entity_name → list of records for that entity
            entity_batches: dict = {}
            total_records = 0
            max_alter_id = last_alter_id
            preview_data = None
            first_entity = None
            
            for item_data, entity_name in self.tally_service.parse_xml_iteratively(response_stream):
                if first_entity is None:
                    first_entity = entity_name
                
                if entity_name not in entity_batches:
                    entity_batches[entity_name] = []
                entity_batches[entity_name].append(item_data)
                
                # Update progress every 100 rows
                batch_total = sum(len(b) for b in entity_batches.values())
                if batch_total % 100 == 0:
                    log_entry = self.db.query(SyncLog).filter(SyncLog.id == log_id).first()
                    if log_entry:
                        log_entry.records_fetched = total_records + batch_total
                        self.db.commit()
                        
                # Track max alter id dynamically across any column containing 'ALTERID'
                for key, val in item_data.items():
                    if "ALTERID" in key.upper():
                        try:
                            aid = int(val)
                            if aid > max_alter_id:
                                max_alter_id = aid
                        except (ValueError, TypeError):
                            pass
                
                # Flush the batch for this specific entity when it reaches batch_size
                if len(entity_batches[entity_name]) >= batch_size:
                    batch = entity_batches[entity_name]
                    
                    if total_records == 0 and entity_name == first_entity:
                        xml_parts = ["<PREVIEW>"]
                        for item in batch[:20]:
                            xml_parts.append("  <RECORD>")
                            for k, v in item.items():
                                safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                                xml_parts.append(f"    <{safe_k}>{v}</{safe_k}>")
                            xml_parts.append("  </RECORD>")
                        xml_parts.append("</PREVIEW>")
                        preview_data = "\n".join(xml_parts)
                    
                    self.db_engine.sync_data(report_name, entity_name, batch, unique_key_field=unique_key)
                    total_records += len(batch)
                    logger.info(f"[{entity_name}] Inserted batch of {len(batch)} records. Total: {total_records}")
                    entity_batches[entity_name] = []
            
            # Process remaining items in all entity batches
            for ename, batch in entity_batches.items():
                if not batch:
                    continue
                if total_records == 0 and ename == first_entity:
                    xml_parts = ["<PREVIEW>"]
                    for item in batch[:20]:
                        xml_parts.append("  <RECORD>")
                        for k, v in item.items():
                            safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                            xml_parts.append(f"    <{safe_k}>{v}</{safe_k}>")
                        xml_parts.append("  </RECORD>")
                    xml_parts.append("</PREVIEW>")
                    preview_data = "\n".join(xml_parts)
                self.db_engine.sync_data(report_name, ename, batch, unique_key_field=unique_key)
                total_records += len(batch)
                logger.info(f"[{ename}] Final batch of {len(batch)} records. Total: {total_records}")
                
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
