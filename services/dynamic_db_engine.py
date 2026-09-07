import hashlib
import logging
from typing import List, Dict, Tuple
from sqlalchemy import Table, Column, Integer, String, Text, MetaData, select, inspect, func, text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
import datetime
import urllib.parse
from sqlalchemy import create_engine

from models import TallyTableMetadata

logger = logging.getLogger(__name__)

class DynamicDbEngine:
    def __init__(self, db_session: Session):
        self.db = db_session
        self.metadata = MetaData()
        self.engine = self.db.get_bind()
        
    def _generate_structure_hash(self, columns: List[str]) -> str:
        """Generate a SHA256 hash based on sorted column names."""
        sorted_cols = sorted(columns)
        hash_input = "|".join(sorted_cols).encode('utf-8')
        return hashlib.sha256(hash_input).hexdigest()
        
    def _get_or_create_table(self, report_name: str, entity_name: str, columns: List[str]) -> Table:
        structure_hash = self._generate_structure_hash(columns)
        
        safe_entity = ''.join(c for c in entity_name.lower() if c.isalnum() or c == '_')
        table_name = safe_entity
        
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            logger.info(f"Reusing existing table {table_name} for report {report_name}")
            existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
            
            with self.engine.begin() as conn:
                for col_name in columns:
                    safe_col_name = ''.join(c for c in col_name if c.isalnum() or c == '_')
                    if safe_col_name not in existing_columns:
                        logger.info(f"Adding new column {safe_col_name} to {table_name}")
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {safe_col_name} LONGTEXT"))
            
            # Update metadata
            meta_entry = self.db.query(TallyTableMetadata).filter_by(table_name=table_name).first()
            if meta_entry:
                meta_entry.last_sync_at = datetime.datetime.now()
                meta_entry.structure_hash = structure_hash
                self.db.commit()
                
            return Table(table_name, self.metadata, autoload_with=self.engine)
            
        # Create new table
        logger.info(f"Creating new table {table_name} for report {report_name}")
        
        # Define SQLAlchemy columns
        db_columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
        for col_name in columns:
            safe_col_name = ''.join(c for c in col_name if c.isalnum() or c == '_')
            db_columns.append(Column(safe_col_name, Text))
            
        dynamic_table = Table(table_name, self.metadata, *db_columns)
        self.metadata.create_all(self.engine)
        
        # Save metadata
        new_meta = TallyTableMetadata(
            report_name=report_name,
            entity_name=entity_name,
            table_name=table_name,
            structure_hash=structure_hash
        )
        self.db.add(new_meta)
        self.db.commit()
        
        return dynamic_table

    def _determine_unique_key(self, columns: List[str]) -> str:
        """Find the best unique key available in the dataset."""
        candidates = ['GUID', 'ALTERID', 'VOUCHERNUMBER', 'NAME']
        for cand in candidates:
            if cand in columns:
                return cand
        return None # No clear unique key

    def sync_data(self, report_name: str, entity_name: str, data: List[Dict]):
        if not data:
            logger.warning("No data provided to sync.")
            return

        # Determine all unique columns across the dataset
        all_columns_set = set()
        for item in data:
            all_columns_set.update(item.keys())
        all_columns = list(all_columns_set)
        
        # Get or create table
        dynamic_table = self._get_or_create_table(report_name, entity_name, all_columns)
        
        unique_key = self._determine_unique_key(all_columns)
        
        with self.engine.connect() as conn:
            # Clean up dict keys to match column names (safe_col_name)
            clean_data = []
            for item in data:
                clean_item = {}
                for k, v in item.items():
                    safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                    clean_item[safe_k] = v
                clean_data.append(clean_item)

            if unique_key:
                safe_unique_key = ''.join(c for c in unique_key if c.isalnum() or c == '_')
                logger.info(f"Performing UPSERT using unique key: {unique_key}")
                
                # We use MySQL's INSERT ... ON DUPLICATE KEY UPDATE
                stmt = insert(dynamic_table).values(clean_data)
                
                update_dict = {
                    c.name: c for c in stmt.inserted if c.name not in ('id', safe_unique_key)
                }
                
                if update_dict:
                    upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
                    # For ON DUPLICATE KEY UPDATE to work, the unique key column needs a UNIQUE constraint
                    # Since we create dynamic tables without UNIQUE constraints (because we don't know types),
                    # standard UPSERT might fail if there's no unique index. 
                    # Let's do a programmatic upsert for safety.
            
            # Programmatic UPSERT fallback because we don't dynamically create UNIQUE constraints yet
            logger.info("Performing programmatic Upsert")
            for row in clean_data:
                if unique_key:
                    safe_unique_key = ''.join(c for c in unique_key if c.isalnum() or c == '_')
                    unique_val = row.get(safe_unique_key)
                    
                    if unique_val:
                        # Check if exists
                        sel = select(dynamic_table).where(getattr(dynamic_table.c, safe_unique_key) == unique_val)
                        result = conn.execute(sel).first()
                        
                        if result:
                            # Update
                            upd = dynamic_table.update().where(getattr(dynamic_table.c, safe_unique_key) == unique_val).values(**row)
                            conn.execute(upd)
                        else:
                            # Insert
                            conn.execute(dynamic_table.insert().values(**row))
                    else:
                        conn.execute(dynamic_table.insert().values(**row))
                else:
                    # No unique key, just insert
                    conn.execute(dynamic_table.insert().values(**row))
                    
            conn.commit()
            logger.info(f"Successfully synced {len(data)} records to {dynamic_table.name}")
