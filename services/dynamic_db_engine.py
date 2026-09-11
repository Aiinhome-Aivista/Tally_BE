import hashlib
import logging
from typing import List, Dict, Tuple
from sqlalchemy import Table, Column, Integer, String, Text, MetaData, select, inspect, func, text, Float, BigInteger, Date
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
import datetime
import urllib.parse
from sqlalchemy import create_engine

from models import TallyTableMetadata

logger = logging.getLogger(__name__)

def detect_column_type(values: List[str]):
    """Detect SQL data type from sample values.
    - Returns DOUBLE for decimal numbers (handles Tally's (-) negative format)
    - Returns BigInteger for whole numbers
    - Returns Date for YYYYMMDD date strings
    - Returns Text for everything else
    """
    if not values:
        return Text

    valid_values = [str(v).strip() for v in values if v is not None and str(v).strip() != ""]
    if not valid_values:
        return Text

    is_int   = True
    is_float = True
    is_date  = True

    for val in valid_values:
        val_str   = str(val).strip()
        # Tally encodes negative numbers as "(-)<number>" e.g. "(-)1234.56"
        clean_val = val_str.replace("(-)", "-")
        
        if is_int:
            try:
                int(clean_val)
            except ValueError:
                is_int = False
                
        if is_float:
            try:
                float(clean_val)
            except ValueError:
                is_float = False
                
        if is_date:
            if len(val_str) == 8 and val_str.isdigit():
                try:
                    datetime.datetime.strptime(val_str, "%Y%m%d")
                except ValueError:
                    is_date = False
            else:
                is_date = False
                
        if not is_int and not is_float and not is_date:
            return Text

    # Priority: int → float → date → text
    if is_int and not is_date:
        return BigInteger
    if is_float:
        return DOUBLE   # Use DOUBLE (not FLOAT) for full precision
    if is_date:
        return Date

    return Text

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
        
    def _get_or_create_table(self, report_name: str, entity_name: str, columns: List[str], column_types: Dict[str, type] = None) -> Table:
        if column_types is None:
            column_types = {}
            
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
                        col_type = column_types.get(col_name, Text)
                        if col_type == BigInteger:
                            sql_type = "BIGINT"
                        elif col_type == DOUBLE or col_type == Float:
                            sql_type = "DOUBLE"
                        elif col_type == Date:
                            sql_type = "DATE"
                        else:
                            sql_type = "LONGTEXT"
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {safe_col_name} {sql_type}"))
            
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
            db_columns.append(Column(safe_col_name, column_types.get(col_name, Text)))
            
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

    def _determine_unique_key(self, columns: List[str], preferred_key: str = None) -> str:
        """Find the best unique key available in the dataset."""
        if preferred_key and preferred_key in columns:
            return preferred_key
            
        candidates = ['MASTERID', 'GUID', 'ALTERID', 'VOUCHERNUMBER', 'NAME']
        for cand in candidates:
            if cand in columns:
                return cand
        return None # No clear unique key

    def sync_data(self, report_name: str, entity_name: str, data: List[Dict], unique_key_field: str = None):
        if not data:
            logger.warning("No data provided to sync.")
            return

        # Determine all unique columns across the dataset
        all_columns_set = set()
        for item in data:
            all_columns_set.update(item.keys())
        all_columns = list(all_columns_set)
        
        column_samples = {k: [] for k in all_columns}
        for item in data:
            for k, v in item.items():
                if len(column_samples[k]) < 500:
                    column_samples[k].append(v)
                    
        column_types = {}
        for k, v in column_samples.items():
            column_types[k] = detect_column_type(v)
        
        # Get or create table
        dynamic_table = self._get_or_create_table(report_name, entity_name, all_columns, column_types)
        
        unique_key = self._determine_unique_key(all_columns, preferred_key=unique_key_field)
        
        with self.engine.connect() as conn:
            # Clean up dict keys to match column names (safe_col_name)
            clean_data = []
            for item in data:
                clean_item = {}
                for k, v in item.items():
                    safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                    
                    if v is not None:
                        col_type = column_types.get(k)
                        if col_type == Date:
                            v_str = str(v).strip()
                            if len(v_str) == 8 and v_str.isdigit():
                                try:
                                    v = datetime.datetime.strptime(v_str, "%Y%m%d").date()
                                except ValueError:
                                    pass
                        elif col_type == DOUBLE or col_type == Float:
                            v_str = str(v).strip().replace("(-)", "-")
                            try:
                                v = float(v_str)
                            except ValueError:
                                v = None
                        elif col_type == BigInteger:
                            v_str = str(v).strip().replace("(-)", "-")
                            try:
                                v = int(v_str)
                            except ValueError:
                                try:
                                    v = int(float(v_str))
                                except ValueError:
                                    v = None
                                
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
            
            # Programmatic UPSERT fallback optimized for batching
            logger.info("Performing optimized programmatic Upsert")
            
            if not unique_key:
                logger.info(f"No unique key found. Clearing table {dynamic_table.name} before insert to prevent duplicates.")
                conn.execute(dynamic_table.delete())
                if clean_data:
                    conn.execute(dynamic_table.insert(), clean_data)
            else:
                safe_unique_key = ''.join(c for c in unique_key if c.isalnum() or c == '_')
                
                # Extract all unique values in this batch
                batch_unique_vals = [row[safe_unique_key] for row in clean_data if row.get(safe_unique_key)]
                
                # Fetch existing ones in ONE query
                existing_vals = set()
                if batch_unique_vals:
                    sel = select(getattr(dynamic_table.c, safe_unique_key)).where(getattr(dynamic_table.c, safe_unique_key).in_(batch_unique_vals))
                    result = conn.execute(sel).fetchall()
                    existing_vals = {r[0] for r in result}
                
                to_insert = []
                to_update = []
                
                for row in clean_data:
                    unique_val = row.get(safe_unique_key)
                    if unique_val and unique_val in existing_vals:
                        to_update.append((unique_val, row))
                    else:
                        to_insert.append(row)
                
                # Delete existing rows to be updated
                if to_update:
                    delete_vals = [val for val, _ in to_update]
                    del_stmt = dynamic_table.delete().where(getattr(dynamic_table.c, safe_unique_key).in_(delete_vals))
                    conn.execute(del_stmt)
                    
                    # Re-add them as inserts
                    for _, row in to_update:
                        to_insert.append(row)
                
                # Bulk insert all rows
                if to_insert:
                    conn.execute(dynamic_table.insert(), to_insert)
                    
            conn.commit()
            logger.info(f"Successfully synced {len(clean_data)} records to {dynamic_table.name}")
