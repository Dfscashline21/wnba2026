# -*- coding: utf-8 -*-
"""
Script to check what tables exist in the AWS RDS database
"""

import pandas as pd
from db_conn import get_db_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_existing_tables():
    """Check what tables exist in the wnba schema"""
    try:
        # Get database connection
        engine = get_db_engine()
        if engine is None:
            logger.error("❌ Could not connect to database")
            return
        
        # Query to get all tables in the wnba schema
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'wnba' 
        ORDER BY table_name;
        """
        
        df = pd.read_sql(query, engine)
        
        logger.info(f"📊 Found {len(df)} tables in wnba schema:")
        for table in df['table_name']:
            logger.info(f"   • {table}")
        
        return df['table_name'].tolist()
        
    except Exception as e:
        logger.error(f"❌ Error checking tables: {e}")
        return []

if __name__ == "__main__":
    tables = check_existing_tables()
    print(f"\nTotal tables found: {len(tables)}")
