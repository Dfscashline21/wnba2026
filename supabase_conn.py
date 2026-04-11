# -*- coding: utf-8 -*-
"""
Supabase connection module for WNBA data pipeline
Replaces db_conn.py with Supabase integration (Engine only)

@author: trent
"""

import os
from typing import Optional
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseConnection:
    """Supabase connection manager for WNBA data pipeline (Engine only)"""
    
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.password = os.getenv("SUPABASE_PASSWORD")  # Database password for pooler
        self.host = os.getenv("SUPABASE_HOST")
        self.user = os.getenv("SUPABASE_USER", "postgres")
        self.dbname = os.getenv("SUPABASE_DBNAME", "postgres")
        self.port = os.getenv("SUPABASE_PORT", "5432")
        self.schema = os.getenv("SUPABASE_SCHEMA", "wnba")
        
        if not all([self.url, self.service_key]):
            raise ValueError("Missing required Supabase environment variables: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        
        self._engine = None
        logger.info("✅ Supabase connection initialized (engine only)")
    
    def get_client(self) -> None:
        """Get Supabase client instance (not available in engine-only mode)"""
        logger.warning("⚠️ Supabase client is not available in engine-only mode")
        return None
    
    def get_engine(self):
        """Get SQLAlchemy engine for pandas operations"""
        if self._engine is None:
            try:
                # Extract project ID
                if self.url.startswith('db.'):
                    project_id = self.url.replace('db.', '').replace('.supabase.co', '')
                    pooler_host = self.url
                else:
                    project_id = self.url.replace('.supabase.co', '')
                    pooler_host = f"db.{project_id}.supabase.co"
                
                # Try connection pooler first (port 6543) - works when direct connections are blocked
                # Pooler uses database password, not service role key
                if self.password:
                    logger.info(f"🔗 Attempting connection via Supabase pooler (port 6543)...")
                    connection_string = f"postgresql://{self.user}:{self.password}@{pooler_host}:6543/{self.dbname}"
                    
                    try:
                        self._engine = create_engine(
                            connection_string,
                            connect_args={
                                "sslmode": "require",
                                "connect_timeout": 60,
                                "application_name": "wnba_pipeline"
                            },
                            pool_pre_ping=True,
                            pool_recycle=300,
                            pool_size=3,
                            max_overflow=5,
                            pool_timeout=30
                        )
                        
                        # Test the connection
                        with self._engine.connect() as conn:
                            result = conn.execute(text("SELECT version();"))
                            version = result.fetchone()
                            logger.info(f"✅ Supabase pooler connection successful: {version[0]}")
                            return self._engine
                            
                    except Exception as pooler_error:
                        logger.warning(f"⚠️ Pooler connection failed: {pooler_error}")
                        logger.info("🔄 Trying direct connection (port 5432)...")
                else:
                    logger.info("⚠️ SUPABASE_PASSWORD not set, skipping pooler...")
                    
                    # Fallback to direct connection (port 5432)
                    if self.url.startswith('db.'):
                        direct_host = f"{project_id}.supabase.co"
                    else:
                        direct_host = self.url
                    
                    connection_string = f"postgresql://{self.user}:{self.service_key}@{direct_host}:5432/{self.dbname}"
                    
                    self._engine = create_engine(
                        connection_string,
                        connect_args={
                            "sslmode": "require",
                            "connect_timeout": 120,
                            "application_name": "wnba_pipeline",
                            "options": "-c statement_timeout=300000"
                        },
                        pool_pre_ping=True,
                        pool_recycle=300,
                        pool_size=5,
                        max_overflow=10,
                        pool_timeout=60
                    )
                    
                    # Test direct connection
                    with self._engine.connect() as conn:
                        result = conn.execute(text("SELECT version();"))
                        version = result.fetchone()
                        logger.info(f"✅ Supabase direct connection successful: {version[0]}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to create Supabase engine: {e}")
                raise
                
        return self._engine
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "wnba", 
                        if_exists: str = "replace", index: bool = False, chunk_size: int = 1000) -> bool:
        """
        Upload DataFrame to Supabase table with timeout handling and chunking
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            schema: Database schema (default: wnba)
            if_exists: How to behave if table exists ('replace', 'append', 'fail')
            index: Whether to include DataFrame index
            chunk_size: Number of rows to upload at once (default: 1000)
            
        Returns:
            bool: Success status
        """
        try:
            engine = self.get_engine()
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # If DataFrame is small, upload directly
            if len(df) <= chunk_size:
                logger.info(f"📤 Uploading {len(df)} rows to {full_table_name}...")
                df.to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists=if_exists,
                    index=index,
                    method='multi'
                )
                logger.info(f"✅ Successfully uploaded {len(df)} rows to {full_table_name}")
                return True
            
            # For large DataFrames, use chunking
            logger.info(f"📤 Uploading {len(df)} rows to {full_table_name} in chunks of {chunk_size}...")
            
            # Create table with first chunk if replacing
            if if_exists == 'replace':
                first_chunk = df.head(chunk_size)
                first_chunk.to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists='replace',
                    index=index,
                    method='multi'
                )
                logger.info(f"✅ Created table with first {len(first_chunk)} rows")
                
                # Upload remaining chunks
                remaining_df = df.iloc[chunk_size:]
                if not remaining_df.empty:
                    for i in range(0, len(remaining_df), chunk_size):
                        chunk = remaining_df.iloc[i:i + chunk_size]
                        chunk.to_sql(
                            name=table_name,
                            schema=schema,
                            con=engine,
                            if_exists='append',
                            index=index,
                            method='multi'
                        )
                        logger.info(f"✅ Uploaded chunk {i//chunk_size + 2}: {len(chunk)} rows")
            else:
                # For append mode, upload all chunks
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i + chunk_size]
                    chunk.to_sql(
                        name=table_name,
                        schema=schema,
                        con=engine,
                        if_exists=if_exists if i == 0 else 'append',
                        index=index,
                        method='multi'
                    )
                    logger.info(f"✅ Uploaded chunk {i//chunk_size + 1}: {len(chunk)} rows")
            
            logger.info(f"✅ Successfully uploaded all {len(df)} rows to {full_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading to {table_name}: {e}")
            return False
    
    def query_dataframe(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute SQL query and return DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame or None if error
        """
        try:
            engine = self.get_engine()
            df = pd.read_sql(query, engine)
            logger.info(f"✅ Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error executing query: {e}")
            return None
    
    def insert_data(self, table_name: str, data: dict, schema: str = "wnba") -> bool:
        """
        Insert single record into Supabase table using SQL
        
        Args:
            table_name: Target table name
            data: Dictionary of data to insert
            schema: Database schema
            
        Returns:
            bool: Success status
        """
        try:
            engine = self.get_engine()
            
            # Build insert query
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ', '.join(['%s'] * len(values))
            
            query = f"INSERT INTO {schema}.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            with engine.connect() as conn:
                conn.execute(query, values)
                logger.info(f"✅ Successfully inserted data into {schema}.{table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error inserting into {table_name}: {e}")
            return False
    
    def batch_insert(self, table_name: str, data_list: list, schema: str = "wnba") -> bool:
        """
        Insert multiple records into Supabase table using SQL
        
        Args:
            table_name: Target table name
            data_list: List of dictionaries to insert
            schema: Database schema
            
        Returns:
            bool: Success status
        """
        try:
            engine = self.get_engine()
            
            if not data_list:
                logger.warning("⚠️ No data to insert")
                return False
            
            # Get columns from first record
            columns = list(data_list[0].keys())
            
            # Build insert query
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {schema}.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Prepare values
            values_list = []
            for data in data_list:
                values = [data.get(col) for col in columns]
                values_list.append(values)
            
            with engine.connect() as conn:
                conn.executemany(query, values_list)
                logger.info(f"✅ Successfully inserted {len(data_list)} records into {schema}.{table_name}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error batch inserting into {table_name}: {e}")
            return False
    
    def get_table_data(self, table_name: str, schema: str = "wnba", 
                      limit: int = None, filters: dict = None) -> Optional[pd.DataFrame]:
        """
        Get data from Supabase table using SQL
        
        Args:
            table_name: Table name
            schema: Database schema
            limit: Maximum number of rows to return
            filters: Dictionary of column filters
            
        Returns:
            DataFrame or None if error
        """
        try:
            engine = self.get_engine()
            
            # Build query
            query = f"SELECT * FROM {schema}.{table_name}"
            
            # Apply filters
            if filters:
                filter_conditions = []
                for column, value in filters.items():
                    if isinstance(value, str):
                        filter_conditions.append(f"{column} = '{value}'")
                    else:
                        filter_conditions.append(f"{column} = {value}")
                if filter_conditions:
                    query += " WHERE " + " AND ".join(filter_conditions)
            
            # Apply limit
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, engine)
            
            if not df.empty:
                logger.info(f"✅ Retrieved {len(df)} rows from {table_name}")
                return df
            else:
                logger.warning(f"⚠️ No data found in {table_name}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error retrieving from {table_name}: {e}")
            return None
    
    def delete_table_data(self, table_name: str, filters: dict = None) -> bool:
        """
        Delete data from Supabase table using SQL
        
        Args:
            table_name: Table name
            filters: Dictionary of column filters for deletion
            
        Returns:
            bool: Success status
        """
        try:
            engine = self.get_engine()
            
            # Build delete query
            query = f"DELETE FROM {self.schema}.{table_name}"
            
            # Apply filters
            if filters:
                filter_conditions = []
                for column, value in filters.items():
                    if isinstance(value, str):
                        filter_conditions.append(f"{column} = '{value}'")
                    else:
                        filter_conditions.append(f"{column} = {value}")
                if filter_conditions:
                    query += " WHERE " + " AND ".join(filter_conditions)
            
            with engine.connect() as conn:
                result = conn.execute(query)
                logger.info(f"✅ Successfully deleted data from {table_name}")
                return True
            
        except Exception as e:
            logger.error(f"❌ Error deleting from {table_name}: {e}")
            return False

# Global Supabase instance
supabase_conn = SupabaseConnection()

def get_supabase_client() -> None:
    """Get Supabase client (not available in engine-only mode)"""
    logger.warning("⚠️ get_supabase_client() is not available in engine-only mode")
    return None

def get_supabase_engine():
    """Get SQLAlchemy engine (replaces get_db_engine)"""
    return supabase_conn.get_engine()

# Backward compatibility functions
def get_db_connection():
    """Legacy function - now returns None (client not available)"""
    logger.warning("⚠️ get_db_connection() is deprecated. Client not available in engine-only mode.")
    return None

def get_db_engine():
    """Legacy function - now returns Supabase engine"""
    logger.warning("⚠️ get_db_engine() is deprecated. Use get_supabase_engine() instead.")
    return get_supabase_engine()
