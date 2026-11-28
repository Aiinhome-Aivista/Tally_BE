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
                # Try other common formats like 2-Apr-25, 2-Apr-2025, YYYY-MM-DD
                parsed = False
                for fmt in ("%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
                    try:
                        datetime.datetime.strptime(val_str, fmt)
                        parsed = True
                        break
                    except ValueError:
                        continue
                if not parsed:
                    is_date = False
                
        if not is_int and not is_float and not is_date:
            return Text

    # Priority: int → float → date → text
    if is_int and not is_date:
        return BigInteger
    if is_float:
        return Float().with_variant(DOUBLE(), "mysql")
    if is_date:
        return Date

    return Text

class DynamicDbEngine:
    def __init__(self, db_session: Session):
        self.db = db_session
        self.metadata = MetaData()
        self.engine = self.db.get_bind()
        
    def _quote(self, name: str) -> str:
        """Returns appropriately quoted identifier for MySQL (backticks) or SQL Server (brackets)."""
        if self.engine.dialect.name == "mssql":
            return f"[{name}]"
        return f"`{name}`"

    def _generate_structure_hash(self, columns: List[str]) -> str:
        """Generate a SHA256 hash based on sorted column names."""
        sorted_cols = sorted(columns)
        hash_input = "|".join(sorted_cols).encode('utf-8')
        return hashlib.sha256(hash_input).hexdigest()
        
    def _get_or_create_table(self, report_name: str, entity_name: str, columns: List[str], column_types: Dict[str, type] = None, company_name: str = None) -> Table:
        if column_types is None:
            column_types = {}
            
        # Ensure COMPANY_NAME column is always part of table columns
        if 'COMPANY_NAME' not in columns:
            columns.append('COMPANY_NAME')
        if 'COMPANY_NAME' not in column_types:
            column_types['COMPANY_NAME'] = String(255)

        structure_hash = self._generate_structure_hash(columns)
        
        safe_entity = ''.join(c for c in entity_name.lower() if c.isalnum() or c == '_')
        table_name = safe_entity
        
        is_mysql = self.engine.dialect.name == "mysql"
        is_mssql = self.engine.dialect.name == "mssql"
        
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            logger.info(f"Reusing existing table {table_name} for report {report_name}")
            existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
            existing_cols_upper = [c.upper() for c in existing_columns]
            
            with self.engine.begin() as conn:
                for col_name in columns:
                    safe_col_name = ''.join(c for c in col_name if c.isalnum() or c == '_')
                    # Don't duplicate if already exists in either case
                    if safe_col_name not in existing_columns and safe_col_name.upper() not in existing_cols_upper:
                        logger.info(f"Adding new column {safe_col_name} to {table_name}")
                        col_type = column_types.get(col_name, Text)
                        if col_type == BigInteger:
                            sql_type = "BIGINT"
                        elif col_type == DOUBLE or col_type == Float or isinstance(col_type, (Float, DOUBLE)):
                            sql_type = "DOUBLE" if is_mysql else "FLOAT"
                        elif col_type == Date:
                            sql_type = "DATE"
                        elif col_type == String or (isinstance(col_type, type) and issubclass(col_type, String)):
                            sql_type = "VARCHAR(255)" if is_mysql else "NVARCHAR(255)"
                        else:
                            sql_type = "LONGTEXT" if is_mysql else "NVARCHAR(MAX)"
                        
                        # Syntax ALTER TABLE table ADD col type is valid in both MySQL and SQL Server
                        q_table = self._quote(table_name)
                        q_col = self._quote(safe_col_name)
                        conn.execute(text(f"ALTER TABLE {q_table} ADD {q_col} {sql_type}"))
            
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
        
        # Save or update metadata
        existing_meta = self.db.query(TallyTableMetadata).filter_by(table_name=table_name).first()
        if existing_meta:
            existing_meta.report_name = report_name
            existing_meta.entity_name = entity_name
            existing_meta.structure_hash = structure_hash
            existing_meta.last_sync_at = datetime.datetime.now()
        else:
            new_meta = TallyTableMetadata(
                report_name=report_name,
                entity_name=entity_name,
                table_name=table_name,
                structure_hash=structure_hash
            )
            self.db.add(new_meta)
            
        self.db.commit()
        
        return dynamic_table

    def get_max_alter_id(self, report_name: str, company_name: str = None) -> int:
        """Finds the alterid column dynamically and returns its max value, filtered by company_name if provided."""
        meta = self.db.query(TallyTableMetadata).filter_by(report_name=report_name).first()
        if not meta:
            return 0
            
        table_name = meta.table_name
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            return 0
            
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        columns_lower = [c.lower() for c in columns]
        
        # Find alterid column (e.g. alterid, varalterid, attr_alterid)
        alter_col = None
        for col in columns:
            if 'alterid' in col.lower():
                alter_col = col
                break
                
        if not alter_col:
            return 0
            
        with self.engine.connect() as conn:
            try:
                # Find company column in either uppercase or lowercase
                comp_col = None
                for col in columns:
                    if col.upper() == 'COMPANY_NAME':
                        comp_col = col
                        break

                q_alter = self._quote(alter_col)
                q_table = self._quote(table_name)
                
                if company_name and comp_col:
                    q_comp = self._quote(comp_col)
                    query = text(f"SELECT MAX({q_alter}) FROM {q_table} WHERE {q_comp} = :company_name")
                    result = conn.execute(query, {"company_name": company_name}).scalar()
                else:
                    query = text(f"SELECT MAX({q_alter}) FROM {q_table}")
                    result = conn.execute(query).scalar()
                    
                if result:
                    return int(result)
            except Exception as e:
                logger.warning(f"Failed to get max alter id from {table_name}. Error: {e}")
                
        return 0

    def _determine_unique_key(self, columns: List[str], preferred_key: str = None) -> str:
        """Find the best unique key available in the dataset."""
        if preferred_key and preferred_key in columns:
            return preferred_key
            
        candidates = ['VOUCHERMASTERID', 'MASTERID', 'GUID', 'ALTERID', 'VOUCHERNUMBER', 'NAME']
        for cand in candidates:
            if cand in columns:
                return cand

        # Check for any column ending with MASTERID (e.g. VCHINVMASTERID, VCHLEDMASTERID, SJMASTERID, VCHBILLMASTERID, VCHCCMASTERID)
        for col in columns:
            if col.upper().endswith('MASTERID'):
                return col
                
        # Check for any column ending with ALTERID (e.g. VCHINVALTERID, VCHLEDALTERID, SJALTERID)
        for col in columns:
            if col.upper().endswith('ALTERID'):
                return col

        return None

    def sync_data(self, report_name: str, entity_name: str, data: List[Dict], unique_key_field: str = None, company_name: str = None):
        if not data:
            logger.warning("No data provided to sync.")
            return

        # Ensure COMPANY_NAME is present in all records (uppercase)
        if company_name:
            for item in data:
                item['COMPANY_NAME'] = company_name

        # Determine all unique columns across the dataset
        all_columns_set = set()
        for item in data:
            all_columns_set.update(item.keys())
        all_columns = list(all_columns_set)
        
        # Ensure COMPANY_NAME column is tracked
        if 'COMPANY_NAME' not in all_columns:
            all_columns.append('COMPANY_NAME')

        column_samples = {k: [] for k in all_columns}
        for item in data:
            for k, v in item.items():
                if len(column_samples[k]) < 500:
                    column_samples[k].append(v)
                    
        column_types = {}
        for k, v in column_samples.items():
            if k.upper() == 'COMPANY_NAME':
                column_types[k] = String(255)
            else:
                column_types[k] = detect_column_type(v)
        
        # Get or create table with COMPANY_NAME column
        dynamic_table = self._get_or_create_table(report_name, entity_name, all_columns, column_types, company_name=company_name)
        
        # Query actual column types from dynamic_table to know real DB types
        db_column_types = {}
        for c in dynamic_table.columns:
            db_column_types[c.name] = type(c.type)

        unique_key = self._determine_unique_key(all_columns, preferred_key=unique_key_field)

        with self.engine.connect() as conn:
            # Clean up dict keys to match column names (safe_col_name)
            clean_data = []
            for item in data:
                clean_item = {}
                for k, v in item.items():
                    safe_k = ''.join(c for c in k if c.isalnum() or c == '_')
                    
                    if v is not None:
                        # Check detected type or existing DB column type
                        col_type = column_types.get(k)
                        actual_db_type = db_column_types.get(safe_k)
                        
                        v_str = str(v).strip()
                        if col_type == Date or actual_db_type == Date:
                            if len(v_str) == 8 and v_str.isdigit():
                                try:
                                    v = datetime.datetime.strptime(v_str, "%Y%m%d").date().isoformat()
                                except ValueError:
                                    v = None
                            else:
                                parsed = False
                                for fmt in ("%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
                                    try:
                                        v = datetime.datetime.strptime(v_str, fmt).date().isoformat()
                                        parsed = True
                                        break
                                    except ValueError:
                                        continue
                                if not parsed:
                                    v = None
                        elif col_type in (DOUBLE, Float) or isinstance(col_type, (Float, DOUBLE)) or actual_db_type in (DOUBLE, Float) or (isinstance(actual_db_type, type) and issubclass(actual_db_type, (Float, DOUBLE))):
                            v_clean = v_str.replace("(-)", "-")
                            if v_clean == "":
                                v = None
                            else:
                                try:
                                    v = float(v_clean)
                                except ValueError:
                                    v = None
                        elif col_type == BigInteger or actual_db_type in (BigInteger, Integer):
                            v_clean = v_str.replace("(-)", "-")
                            if v_clean == "":
                                v = None
                            else:
                                try:
                                    v = int(v_clean)
                                except ValueError:
                                    try:
                                        v = int(float(v_clean))
                                    except ValueError:
                                        v = None
                    else:
                        v = None
                                
                    clean_item[safe_k] = v
                clean_data.append(clean_item)

            # Programmatic UPSERT optimized for batching, scoped by COMPANY_NAME
            logger.info("Performing optimized programmatic Upsert")
            
            comp_col_obj = dynamic_table.c.get('COMPANY_NAME')
            if comp_col_obj is None:
                comp_col_obj = dynamic_table.c.get('company_name')
            has_company_col = comp_col_obj is not None
            
            CHUNK_SIZE = 500

            if not unique_key:
                if clean_data:
                    for i in range(0, len(clean_data), CHUNK_SIZE):
                        conn.execute(dynamic_table.insert(), clean_data[i:i + CHUNK_SIZE])
            else:
                safe_unique_key = ''.join(c for c in unique_key if c.isalnum() or c == '_')
                
                # Extract all unique values in this batch
                batch_unique_vals = list({row[safe_unique_key] for row in clean_data if row.get(safe_unique_key)})
                
                # Fetch existing ones in chunks of 500 (SQL Server parameter limit is 2100)
                existing_vals = set()
                if batch_unique_vals:
                    for i in range(0, len(batch_unique_vals), CHUNK_SIZE):
                        chunk = batch_unique_vals[i:i + CHUNK_SIZE]
                        where_clauses = [getattr(dynamic_table.c, safe_unique_key).in_(chunk)]
                        if company_name and has_company_col:
                            where_clauses.append(comp_col_obj == company_name)
                        sel = select(getattr(dynamic_table.c, safe_unique_key)).where(*where_clauses)
                        result = conn.execute(sel).fetchall()
                        existing_vals.update(r[0] for r in result)
                
                to_insert = []
                to_update = []
                
                for row in clean_data:
                    unique_val = row.get(safe_unique_key)
                    if unique_val and unique_val in existing_vals:
                        to_update.append((unique_val, row))
                    else:
                        to_insert.append(row)
                
                # Delete existing rows to be updated (scoped by COMPANY_NAME) in chunks of 500
                if to_update:
                    delete_vals = list({val for val, _ in to_update})
                    for i in range(0, len(delete_vals), CHUNK_SIZE):
                        chunk = delete_vals[i:i + CHUNK_SIZE]
                        del_where = [getattr(dynamic_table.c, safe_unique_key).in_(chunk)]
                        if company_name and has_company_col:
                            del_where.append(comp_col_obj == company_name)
                        del_stmt = dynamic_table.delete().where(*del_where)
                        conn.execute(del_stmt)
                    
                    # Re-add them as inserts
                    for _, row in to_update:
                        to_insert.append(row)
                
                # Bulk insert all rows in chunks of 500
                if to_insert:
                    for i in range(0, len(to_insert), CHUNK_SIZE):
                        conn.execute(dynamic_table.insert(), to_insert[i:i + CHUNK_SIZE])
                    
            conn.commit()
            logger.info(f"Successfully synced {len(clean_data)} records to {dynamic_table.name}")
