# -*- coding: utf-8 -*-
"""
Supabase Storage integration for WNBA data pipeline
Replaces S3 file storage with Supabase Storage

@author: trent
"""

import os
from typing import Optional, Dict, Any
from supabase import create_client, Client
import pandas as pd
import json
from datetime import datetime
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SupabaseStorage:
    """Supabase Storage manager for WNBA data pipeline"""
    
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_ANON_KEY")
        
        # Convert connection pooler URL to REST API URL for Supabase client
        if self.url.startswith('db.'):
            project_id = self.url.replace('db.', '').replace('.supabase.co', '')
            supabase_url = f"https://{project_id}.supabase.co"
        else:
            supabase_url = f"https://{self.url}"
        
        self.client: Client = create_client(supabase_url, self.key)
        
    def upload_csv(self, df: pd.DataFrame, bucket_name: str, file_name: str, 
                   public: bool = False) -> bool:
        """
        Upload DataFrame as CSV to Supabase Storage
        
        Args:
            df: DataFrame to upload
            bucket_name: Storage bucket name
            file_name: File name in storage
            public: Whether file should be public
            
        Returns:
            bool: Success status
        """
        try:
            # Convert DataFrame to CSV string
            csv_data = df.to_csv(index=False)
            
            # Upload to Supabase Storage
            result = self.client.storage.from_(bucket_name).upload(
                path=file_name,
                file=csv_data.encode('utf-8'),
                file_options={"content-type": "text/csv"}
            )
            
            # Make public if requested
            if public:
                self.client.storage.from_(bucket_name).update_public_url(file_name)
            
            logger.info(f"✅ Successfully uploaded {file_name} to {bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading {file_name}: {e}")
            return False
    
    def download_csv(self, bucket_name: str, file_name: str) -> Optional[pd.DataFrame]:
        """
        Download CSV from Supabase Storage as DataFrame
        
        Args:
            bucket_name: Storage bucket name
            file_name: File name in storage
            
        Returns:
            DataFrame or None if error
        """
        try:
            # Download file from storage
            result = self.client.storage.from_(bucket_name).download(file_name)
            
            # Convert to DataFrame
            df = pd.read_csv(result)
            
            logger.info(f"✅ Successfully downloaded {file_name} from {bucket_name}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error downloading {file_name}: {e}")
            return None
    
    def list_files(self, bucket_name: str, folder: str = "") -> list:
        """
        List files in Supabase Storage bucket
        
        Args:
            bucket_name: Storage bucket name
            folder: Folder path (optional)
            
        Returns:
            List of file names
        """
        try:
            result = self.client.storage.from_(bucket_name).list(folder)
            files = [item['name'] for item in result]
            
            logger.info(f"✅ Found {len(files)} files in {bucket_name}/{folder}")
            return files
            
        except Exception as e:
            logger.error(f"❌ Error listing files in {bucket_name}: {e}")
            return []
    
    def delete_file(self, bucket_name: str, file_name: str) -> bool:
        """
        Delete file from Supabase Storage
        
        Args:
            bucket_name: Storage bucket name
            file_name: File name to delete
            
        Returns:
            bool: Success status
        """
        try:
            self.client.storage.from_(bucket_name).remove([file_name])
            
            logger.info(f"✅ Successfully deleted {file_name} from {bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error deleting {file_name}: {e}")
            return False
    
    def get_public_url(self, bucket_name: str, file_name: str) -> Optional[str]:
        """
        Get public URL for file in Supabase Storage
        
        Args:
            bucket_name: Storage bucket name
            file_name: File name
            
        Returns:
            Public URL or None if error
        """
        try:
            result = self.client.storage.from_(bucket_name).get_public_url(file_name)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error getting public URL for {file_name}: {e}")
            return None
    
    def upload_json(self, data: Dict[str, Any], bucket_name: str, file_name: str, 
                   public: bool = False) -> bool:
        """
        Upload JSON data to Supabase Storage
        
        Args:
            data: Dictionary to upload as JSON
            bucket_name: Storage bucket name
            file_name: File name in storage
            public: Whether file should be public
            
        Returns:
            bool: Success status
        """
        try:
            # Convert to JSON string
            json_data = json.dumps(data, indent=2)
            
            # Upload to Supabase Storage
            result = self.client.storage.from_(bucket_name).upload(
                path=file_name,
                file=json_data.encode('utf-8'),
                file_options={"content-type": "application/json"}
            )
            
            # Make public if requested
            if public:
                self.client.storage.from_(bucket_name).update_public_url(file_name)
            
            logger.info(f"✅ Successfully uploaded {file_name} to {bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading {file_name}: {e}")
            return False
    
    def download_json(self, bucket_name: str, file_name: str) -> Optional[Dict[str, Any]]:
        """
        Download JSON from Supabase Storage
        
        Args:
            bucket_name: Storage bucket name
            file_name: File name in storage
            
        Returns:
            Dictionary or None if error
        """
        try:
            # Download file from storage
            result = self.client.storage.from_(bucket_name).download(file_name)
            
            # Convert to dictionary
            data = json.loads(result.decode('utf-8'))
            
            logger.info(f"✅ Successfully downloaded {file_name} from {bucket_name}")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error downloading {file_name}: {e}")
            return None

# Global Supabase Storage instance
supabase_storage = SupabaseStorage()

# Migration helper functions
def migrate_s3_to_supabase():
    """
    Migrate data from S3 to Supabase Storage
    This function helps migrate existing S3 data
    """
    try:
        import boto3
        
        # S3 client (from existing code)
        s3_client = boto3.client("s3",
                                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
        
        bucket_name = 'cbbdata2023'  # Your S3 bucket
        
        # List all files in S3 bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        for obj in response.get('Contents', []):
            file_name = obj['Key']
            
            # Download from S3
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            df = pd.read_csv(s3_response['Body'])
            
            # Upload to Supabase Storage
            supabase_storage.upload_csv(df, 'wnba-data', file_name, public=True)
            
            logger.info(f"✅ Migrated {file_name} from S3 to Supabase")
            
    except Exception as e:
        logger.error(f"❌ Error during S3 migration: {e}")

# Example usage functions
def save_daily_data(df: pd.DataFrame, data_type: str):
    """
    Save daily data to Supabase Storage
    
    Args:
        df: DataFrame to save
        data_type: Type of data (e.g., 'projections', 'props', 'game_logs')
    """
    today = datetime.now().strftime('%Y-%m-%d')
    file_name = f"{data_type}_{today}.csv"
    
    return supabase_storage.upload_csv(df, 'wnba-daily', file_name, public=False)

def load_daily_data(data_type: str, date: str = None):
    """
    Load daily data from Supabase Storage
    
    Args:
        data_type: Type of data to load
        date: Specific date (defaults to today)
    """
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    file_name = f"{data_type}_{date}.csv"
    
    return supabase_storage.download_csv('wnba-daily', file_name)
