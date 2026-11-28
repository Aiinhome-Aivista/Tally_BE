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
            
        local_db = None
        try:
            logger.info(f"Starting Tally synchronization for log_id: {log_id}...")
            
            # Fetch config from local SQLite
            from database.config import LocalSessionLocal
            local_db = LocalSessionLocal()
            if connection_name:
                config = local_db.query(SyncConfig).filter(SyncConfig.connection_name == connection_name).first()
            else:
                config = local_db.query(SyncConfig).first()
                
            if not config:
                # Fallback: check target database's sync_config table
                try:
                    from models import RemoteSyncConfig
                    if connection_name:
                        config = self.db.query(RemoteSyncConfig).filter(RemoteSyncConfig.connection_name == connection_name).first()
                    else:
                        config = self.db.query(RemoteSyncConfig).first()
                except Exception:
                    pass

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
            
            # Dynamically fetch max alter ID from the actual data table with company_name filter
            db_max_alter_id = self.db_engine.get_max_alter_id(report_name, company_name=config.company_name)
            if db_max_alter_id > last_alter_id:
                last_alter_id = db_max_alter_id
                logger.info(f"Using actual max alter ID {last_alter_id} from database table for company {config.company_name}.")
                
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
            total_inserted = 0
            total_updated = 0
            total_duplicates = 0
            total_failed = 0
            total_skipped = 0
            sample_inserted = []
            sample_updated = []
            sample_duplicates = []
            sample_failed = []
            prev_alter_id = last_alter_id
            max_alter_id = last_alter_id
            import datetime as _dt
            sync_start_time = _dt.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
            preview_data = None
            first_entity = None
            
            for item_data, entity_name in self.tally_service.parse_xml_iteratively(response_stream):
                if first_entity is None:
                    first_entity = entity_name
                
                if entity_name not in entity_batches:
                    entity_batches[entity_name] = []
                entity_batches[entity_name].append(item_data)
                
                # Update progress every 1000 rows
                batch_total = sum(len(b) for b in entity_batches.values())
                if batch_total % 1000 == 0:
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
                    
                    sync_res = self.db_engine.sync_data(report_name, entity_name, batch, unique_key_field=unique_key, company_name=config.company_name)
                    if isinstance(sync_res, dict):
                        total_inserted += sync_res.get("inserted", 0)
                        total_updated += sync_res.get("updated", 0)
                        total_duplicates += sync_res.get("duplicates", 0)
                        total_failed += sync_res.get("failed", 0)
                        total_skipped += sync_res.get("skipped_records", 0)
                        for s in sync_res.get("sample_inserted", []):
                            if len(sample_inserted) < 2 and s not in sample_inserted:
                                sample_inserted.append(s)
                        for s in sync_res.get("sample_updated", []):
                            if len(sample_updated) < 3 and s not in sample_updated:
                                sample_updated.append(s)
                        for s in sync_res.get("sample_duplicates", []):
                            if len(sample_duplicates) < 3 and s not in sample_duplicates:
                                sample_duplicates.append(s)
                        for s in sync_res.get("sample_failed", []):
                            if len(sample_failed) < 3 and s not in sample_failed:
                                sample_failed.append(s)
                    else:
                        total_inserted += len(batch)
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
                sync_res = self.db_engine.sync_data(report_name, ename, batch, unique_key_field=unique_key, company_name=config.company_name)
                if isinstance(sync_res, dict):
                    total_inserted += sync_res.get("inserted", 0)
                    total_updated += sync_res.get("updated", 0)
                    total_duplicates += sync_res.get("duplicates", 0)
                    total_failed += sync_res.get("failed", 0)
                    total_skipped += sync_res.get("skipped_records", 0)
                    for s in sync_res.get("sample_inserted", []):
                        if len(sample_inserted) < 2 and s not in sample_inserted:
                            sample_inserted.append(s)
                    for s in sync_res.get("sample_updated", []):
                        if len(sample_updated) < 3 and s not in sample_updated:
                            sample_updated.append(s)
                    for s in sync_res.get("sample_duplicates", []):
                        if len(sample_duplicates) < 3 and s not in sample_duplicates:
                            sample_duplicates.append(s)
                    for s in sync_res.get("sample_failed", []):
                        if len(sample_failed) < 3 and s not in sample_failed:
                            sample_failed.append(s)
                else:
                    total_inserted += len(batch)
                total_records += len(batch)
                logger.info(f"[{ename}] Final batch of {len(batch)} records. Total: {total_records}")
                
            # Update last alter id in local SQLite config
            if max_alter_id > (config.last_alter_id or 0):
                config.last_alter_id = max_alter_id
                local_db.commit()
                logger.info(f"Updated last_alter_id to {max_alter_id}")
                try:
                    from models import RemoteSyncConfig
                    remote_cfg = self.db.query(RemoteSyncConfig).filter(RemoteSyncConfig.connection_name == config.connection_name).first()
                    if remote_cfg:
                        remote_cfg.last_alter_id = max_alter_id
                        self.db.commit()
                except Exception as e:
                    logger.debug(f"Could not mirror last_alter_id to remote sync_config: {e}")
            
            # Log success
            log_entry = self.db.query(SyncLog).filter(SyncLog.id == log_id).first()
            if log_entry:
                from sqlalchemy.sql import func
                xml_stats = getattr(self.tally_service, "last_xml_stats", {}) or {}
                total_xml = xml_stats.get("total_xml_records", total_records)
                fetched_xml = xml_stats.get("fetched", total_records)
                valid_xml = xml_stats.get("valid_records", total_records)
                invalid_xml = xml_stats.get("invalid_records", 0)
                empty_fields = xml_stats.get("empty_named_fields", 0)
                malformed_pairs = xml_stats.get("malformed_pairs_removed", 0)
                malformed_tokens = xml_stats.get("malformed_tokens_removed", getattr(self.tally_service, "last_blank_tags_removed", 0))

                upd_str = f" (e.g. {', '.join(sample_updated)})" if total_updated > 0 and sample_updated else ""
                dup_str = f" (e.g. {', '.join(sample_duplicates)})" if total_duplicates > 0 and sample_duplicates else ""
                fail_str = f" (e.g. {', '.join(sample_failed)})" if total_failed > 0 and sample_failed else ""

                log_entry.status = "SUCCESS"
                if total_records > 0:
                    import json as _json
                    import datetime as _dt
                    sync_end_time = _dt.datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")
                    audit_payload = {
                        "audit_version": 2,
                        "xml": {
                            "total_xml_records": total_xml,
                            "fetched": fetched_xml,
                            "valid_records": valid_xml,
                            "invalid_records": invalid_xml,
                            "empty_named_fields": empty_fields,
                            "malformed_pairs_removed": malformed_pairs,
                            "malformed_tokens_removed": malformed_tokens,
                            "parent_record_name": xml_stats.get("parent_record_name", ""),
                            "sample_records": xml_stats.get("sample_records", []),
                            "sample_invalid_records": xml_stats.get("sample_invalid_records", []),
                            "malformed_examples": xml_stats.get("malformed_examples", []),
                            "field_audit": xml_stats.get("field_audit", {}),
                        },
                        "db": {
                            "inserted": total_inserted,
                            "updated": total_updated,
                            "duplicates": total_duplicates,
                            "failed": total_failed,
                            "skipped_records": total_skipped,
                            "sample_inserted": sample_inserted,
                            "sample_updated": sample_updated,
                            "sample_duplicates": sample_duplicates,
                            "sample_failed": sample_failed,
                        },
                        "sync": {
                            "company": config.company_name or "N/A",
                            "report": report_name,
                            "start_time": sync_start_time,
                            "completion_time": sync_end_time,
                            "previous_alter_id": prev_alter_id,
                            "current_alter_id": max_alter_id,
                            "status": "SUCCESS",
                        },
                    }
                    log_entry.message = _json.dumps(audit_payload)
                else:
                    log_entry.message = (
                        f"Sync completed successfully. No new changes.\n"
                        f"Company: {config.company_name or 'N/A'} | Report: {report_name} | LAST_ALTER_ID: {prev_alter_id}"
                    )
                log_entry.records_fetched = total_records
                log_entry.timestamp = func.now()
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
        finally:
            if local_db:
                local_db.close()
