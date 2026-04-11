# -*- coding: utf-8 -*-
"""
Supabase connection module for WNBA data pipeline
Database-only version without Supabase client

@author: trent
"""

import os
from typing import Optional
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseConnection:
    """Supabase connection manager for WNBA data pipeline"""
    
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.host = os.getenv("SUPABASE_HOST")
        self.user = os.getenv("SUPABASE_USER", "postgres")
        self.dbname = os.getenv("SUPABASE_DBNAME", "postgres")
        self.port = os.getenv("SUPABASE_PORT", "5432")
        self.schema = os.getenv("SUPABASE_SCHEMA", "wnba")
        
        if not all([self.url, self.service_key]):
            raise ValueError("Missing required Supabase environment variables: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        
        self._engine = None
    
    def get_engine(self):
        """Get SQLAlchemy engine for pandas operations"""
        if self._engine is None:
            try:
                # Try direct PostgreSQL connection first (more reliable)
                if self.url.startswith('db.'):
                    # Convert pooler URL to direct PostgreSQL URL
                    project_id = self.url.replace('db.', '').replace('.supabase.co', '')
                    direct_url = f"{project_id}.supabase.co"
                    connection_string = f"postgresql://{self.user}:{self.service_key}@{direct_url}:5432/{self.dbname}"
                else:
                    # Already using direct URL
                    connection_string = f"postgresql://{self.user}:{self.service_key}@{self.url}:5432/{self.dbname}"
                
                logger.info(f"🔗 Creating Supabase connection: {connection_string.split('@')[1]}")
                
                # Add SSL and other connection parameters
                self._engine = create_engine(
                    connection_string,
                    connect_args={
                        "sslmode": "require",
                        "connect_timeout": 30,
                        "application_name": "wnba_migration"
                    },
                    pool_pre_ping=True,
                    pool_recycle=300
                )
                
                # Test the connection
                with self._engine.connect() as conn:
                    result = conn.execute("SELECT version();")
                    version = result.fetchone()
                    logger.info(f"✅ Supabase connection successful: {version[0]}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to create Supabase engine: {e}")
                raise
                
        return self._engine
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "wnba", 
                        if_exists: str = "replace", index: bool = False) -> bool:
        """
        Upload DataFrame to Supabase table
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            schema: Database schema (default: wnba)
            if_exists: How to behave if table exists ('replace', 'append', 'fail')
            index: Whether to include DataFrame index
            
        Returns:
            bool: Success status
        """
        try:
            engine = self.get_engine()
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            logger.info(f"📤 Uploading {len(df)} rows to {full_table_name}...")
            
            df.to_sql(
                name=table_name,
                schema=schema,
                con=engine,
                if_exists=if_exists,
                index=index,
                method='multi',  # Faster for large datasets
                chunksize=1000   # Process in chunks
            )
            
            logger.info(f"✅ Successfully uploaded {len(df)} rows to {full_table_name}")
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
            logger.info(f"🔍 Executing query: {query[:50]}...")
            
            df = pd.read_sql(query, engine)
            logger.info(f"✅ Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error executing query: {e}")
            return None
    
    def get_table_data(self, table_name: str, schema: str = "wnba", 
                      limit: int = None, filters: dict = None) -> Optional[pd.DataFrame]:
        """
        Get data from Supabase table
        
        Args:
            table_name: Table name
            schema: Database schema
            limit: Maximum number of rows to return
            filters: Dictionary of column filters
            
        Returns:
            DataFrame or None if error
        """
        try:
            # Build query
            query = f"SELECT * FROM {schema}.{table_name}"
            
            # Add filters
            if filters:
                filter_conditions = []
                for column, value in filters.items():
                    if isinstance(value, str):
                        filter_conditions.append(f"{column} = '{value}'")
                    else:
                        filter_conditions.append(f"{column} = {value}")
                query += " WHERE " + " AND ".join(filter_conditions)
            
            # Add limit
            if limit:
                query += f" LIMIT {limit}"
            
            return self.query_dataframe(query)
                
        except Exception as e:
            logger.error(f"❌ Error retrieving from {table_name}: {e}")
            return None
    
    def delete_table_data(self, table_name: str, filters: dict = None) -> bool:
        """
        Delete data from Supabase table
        
        Args:
            table_name: Table name
            filters: Dictionary of column filters for deletion
            
        Returns:
            bool: Success status
        """
        try:
            engine = self.get_engine()
            
            # Build query
            query = f"DELETE FROM {self.schema}.{table_name}"
            
            # Add filters
            if filters:
                filter_conditions = []
                for column, value in filters.items():
                    if isinstance(value, str):
                        filter_conditions.append(f"{column} = '{value}'")
                    else:
                        filter_conditions.append(f"{column} = {value}")
                query += " WHERE " + " AND ".join(filter_conditions)
            
            with engine.connect() as conn:
                conn.execute(query)
                conn.commit()
            
            logger.info(f"✅ Successfully deleted data from {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error deleting from {table_name}: {e}")
            return False

# Global Supabase instance
try:
    supabase_conn = SupabaseConnection()
    logger.info("✅ Supabase connection initialized successfully")
except Exception as e:
    logger.error(f"❌ Failed to initialize Supabase connection: {e}")
    supabase_conn = None

def get_supabase_engine():
    """Get SQLAlchemy engine (replaces get_db_engine)"""
    if supabase_conn:
        return supabase_conn.get_engine()
    else:
        raise RuntimeError("Supabase connection not available")

# Backward compatibility functions
def get_db_engine():
    """Legacy function - now returns Supabase engine"""
    logger.warning("⚠️ get_db_engine() is deprecated. Use get_supabase_engine() instead.")
    return get_supabase_engine()
