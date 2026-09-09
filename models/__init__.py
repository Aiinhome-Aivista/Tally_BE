from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.sql import func
from database.config import Base, LocalBase

class SyncConfig(Base):
    __tablename__ = "sync_config"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String(255), unique=True, index=True, nullable=False)
    tally_host = Column(String(255), nullable=False)
    tally_port = Column(Integer, nullable=False)
    company_name = Column(String(255), nullable=True)
    report_name = Column(String(255), nullable=True)
    request_xml = Column(Text, nullable=True)
    scheduler_timing = Column(String(50), nullable=True)
    file_format = Column(String(10), default="XML")
    last_sync_time = Column(DateTime(timezone=True), default=None)
    status = Column(String(50), default="IDLE") # IDLE, SYNCING, ERROR
    last_alter_id = Column(Integer, default=0)
    parameters = Column(JSON, nullable=True)

class SyncLog(Base):
    __tablename__ = "sync_logs"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String(255), index=True, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String(50)) # SUCCESS, ERROR
    message = Column(Text)
    records_fetched = Column(Integer, default=0)
    request_payload = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=True)
    response_payload = Column(Text().with_variant(LONGTEXT(), "mysql"), nullable=True)

class TallyTableMetadata(Base):
    __tablename__ = "tally_table_metadata"

    id = Column(Integer, primary_key=True, index=True)
    report_name = Column(String(255), index=True)
    entity_name = Column(String(100), index=True)
    table_name = Column(String(100), unique=True, index=True)
    structure_hash = Column(String(255), unique=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_sync_at = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())

class MysqlConfig(LocalBase):
    __tablename__ = "mysql_config"

    id = Column(Integer, primary_key=True, index=True)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String(255), nullable=False)
    password = Column(String(255), nullable=True)
    database_name = Column(String(255), nullable=False)
