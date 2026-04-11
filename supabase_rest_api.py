# -*- coding: utf-8 -*-
"""
Supabase REST API connection module for WNBA data pipeline
Uses REST API instead of direct database connections

@author: trent
"""

import os
import requests
import json
import pandas as pd
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseRestAPI:
    """Supabase REST API connection manager for WNBA data pipeline"""
    
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.anon_key = os.getenv("SUPABASE_ANON_KEY")
        self.service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.schema = os.getenv("SUPABASE_SCHEMA", "wnba")
        
        if not all([self.url, self.service_key]):
            raise ValueError("Missing required Supabase environment variables: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        
        # Convert connection pooler URL to REST API URL
        if self.url.startswith('db.'):
            project_id = self.url.replace('db.', '').replace('.supabase.co', '')
            self.rest_url = f"https://{project_id}.supabase.co"
        else:
            self.rest_url = f"https://{self.url}"
        
        # Headers for REST API calls
        self.headers = {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        
        logger.info(f"🔗 Initialized Supabase REST API: {self.rest_url}")
    
    def _make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict:
        """Make HTTP request to Supabase REST API"""
        url = f"{self.rest_url}/rest/v1/{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers, params=params)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method.upper() == "PUT":
                response = requests.put(url, headers=self.headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=self.headers, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            if response.content:
                return response.json()
            else:
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ REST API request failed: {e}")
            raise
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "wnba", 
                        if_exists: str = "replace", index: bool = False) -> bool:
        """
        Upload DataFrame to Supabase table using REST API
        
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
            # Convert DataFrame to list of dictionaries
            if index:
                df = df.reset_index()
            
            data_list = df.to_dict('records')
            
            logger.info(f"📤 Uploading {len(data_list)} rows to {schema}.{table_name} via REST API...")
            
            # Handle if_exists behavior
            if if_exists == "replace":
                # Delete existing data first
                self._make_request("DELETE", f"{table_name}?select=*")
                logger.info(f"🗑️ Deleted existing data from {table_name}")
            
            # Upload data in chunks (REST API has limits)
            chunk_size = 1000
            for i in range(0, len(data_list), chunk_size):
                chunk = data_list[i:i + chunk_size]
                result = self._make_request("POST", table_name, data=chunk)
                logger.info(f"✅ Uploaded chunk {i//chunk_size + 1}: {len(chunk)} rows")
            
            logger.info(f"✅ Successfully uploaded {len(data_list)} rows to {schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading to {table_name}: {e}")
            return False
    
    def query_dataframe(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute SQL query using REST API (limited to SELECT queries)
        
        Args:
            query: SQL SELECT query string
            
        Returns:
            DataFrame or None if error
        """
        try:
            # REST API only supports SELECT queries
            if not query.strip().upper().startswith('SELECT'):
                raise ValueError("REST API only supports SELECT queries")
            
            logger.info(f"🔍 Executing query via REST API: {query[:50]}...")
            
            # Use the SQL endpoint for complex queries
            url = f"{self.rest_url}/rest/v1/rpc/exec_sql"
            data = {"query": query}
            
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            
            result = response.json()
            df = pd.DataFrame(result)
            
            logger.info(f"✅ Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error executing query: {e}")
            return None
    
    def get_table_data(self, table_name: str, schema: str = "wnba", 
                      limit: int = None, filters: Dict = None) -> Optional[pd.DataFrame]:
        """
        Get data from Supabase table using REST API
        
        Args:
            table_name: Table name
            schema: Database schema
            limit: Maximum number of rows to return
            filters: Dictionary of column filters
            
        Returns:
            DataFrame or None if error
        """
        try:
            endpoint = table_name
            params = {}
            
            # Add limit
            if limit:
                params["limit"] = limit
            
            # Add filters
            if filters:
                for column, value in filters.items():
                    params[f"{column}"] = f"eq.{value}"
            
            logger.info(f"🔍 Getting data from {schema}.{table_name} via REST API...")
            
            result = self._make_request("GET", endpoint, params=params)
            df = pd.DataFrame(result)
            
            logger.info(f"✅ Retrieved {len(df)} rows from {table_name}")
            return df
                
        except Exception as e:
            logger.error(f"❌ Error retrieving from {table_name}: {e}")
            return None
    
    def delete_table_data(self, table_name: str, filters: Dict = None) -> bool:
        """
        Delete data from Supabase table using REST API
        
        Args:
            table_name: Table name
            filters: Dictionary of column filters for deletion
            
        Returns:
            bool: Success status
        """
        try:
            endpoint = table_name
            params = {}
            
            # Add filters
            if filters:
                for column, value in filters.items():
                    params[f"{column}"] = f"eq.{value}"
            
            logger.info(f"🗑️ Deleting data from {table_name} via REST API...")
            
            self._make_request("DELETE", endpoint, params=params)
            
            logger.info(f"✅ Successfully deleted data from {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error deleting from {table_name}: {e}")
            return False
    
    def create_table(self, table_name: str, columns: Dict[str, str], schema: str = "wnba") -> bool:
        """
        Create table using REST API (requires RPC function)
        
        Args:
            table_name: Table name
            columns: Dictionary of column_name: column_type
            schema: Database schema
            
        Returns:
            bool: Success status
        """
        try:
            # Build CREATE TABLE SQL
            column_defs = []
            for col_name, col_type in columns.items():
                column_defs.append(f"{col_name} {col_type}")
            
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {', '.join(column_defs)}
            );
            """
            
            logger.info(f"🏗️ Creating table {schema}.{table_name} via REST API...")
            
            # Use RPC endpoint to execute SQL
            url = f"{self.rest_url}/rest/v1/rpc/exec_sql"
            data = {"query": create_sql}
            
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            
            logger.info(f"✅ Successfully created table {schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating table {table_name}: {e}")
            return False

# Global Supabase REST API instance
try:
    supabase_rest = SupabaseRestAPI()
    logger.info("✅ Supabase REST API initialized successfully")
except Exception as e:
    logger.error(f"❌ Failed to initialize Supabase REST API: {e}")
    supabase_rest = None

def get_supabase_rest():
    """Get Supabase REST API instance"""
    if supabase_rest:
        return supabase_rest
    else:
        raise RuntimeError("Supabase REST API not available")

# Backward compatibility functions (REST API versions)
def get_db_engine():
    """Legacy function - now returns REST API instance"""
    logger.warning("⚠️ get_db_engine() is deprecated. Use get_supabase_rest() instead.")
    return get_supabase_rest()
