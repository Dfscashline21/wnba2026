# -*- coding: utf-8 -*-
"""
Script to generate SQL for creating tables in Supabase based on AWS RDS structure
"""

import pandas as pd
from db_conn import get_db_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_structure(table_name):
    """Get the structure of a table from AWS RDS"""
    try:
        engine = get_db_engine()
        if engine is None:
            return None
        
        # Query to get column information
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = 'wnba' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        logger.error(f"❌ Error getting structure for {table_name}: {e}")
        return None

def generate_create_table_sql(table_name, columns_df):
    """Generate CREATE TABLE SQL from column information"""
    if columns_df is None or columns_df.empty:
        return None
    
    sql_lines = [f"CREATE TABLE IF NOT EXISTS wnba.{table_name} ("]
    
    column_definitions = []
    for _, row in columns_df.iterrows():
        col_name = row['column_name']
        data_type = row['data_type'].upper()
        is_nullable = row['is_nullable']
        column_default = row['column_default']
        
        # Map PostgreSQL types to Supabase types
        if 'character varying' in data_type.lower():
            data_type = 'TEXT'
        elif 'character' in data_type.lower():
            data_type = 'TEXT'
        elif 'timestamp' in data_type.lower():
            data_type = 'TIMESTAMP'
        elif 'date' in data_type.lower():
            data_type = 'DATE'
        elif 'numeric' in data_type.lower():
            data_type = 'NUMERIC'
        elif 'integer' in data_type.lower():
            data_type = 'INTEGER'
        elif 'bigint' in data_type.lower():
            data_type = 'BIGINT'
        elif 'double precision' in data_type.lower():
            data_type = 'NUMERIC'
        elif 'boolean' in data_type.lower():
            data_type = 'BOOLEAN'
        
        # Build column definition
        col_def = f"    {col_name} {data_type}"
        
        # Add NOT NULL if needed
        if is_nullable == 'NO':
            col_def += " NOT NULL"
        
        # Add default if exists
        if column_default and column_default != 'NULL':
            col_def += f" DEFAULT {column_default}"
        
        column_definitions.append(col_def)
    
    sql_lines.append(',\n'.join(column_definitions))
    sql_lines.append(");")
    
    return '\n'.join(sql_lines)

def generate_all_table_sql():
    """Generate SQL for all tables"""
    tables_to_migrate = [
        'PLAYER_GAME_LOGS',
        'injuries', 
        'TEAMS',
        'PLAYERS',
        'Games',
        'wowy',
        'pace',
        'underdog',
        'draftkings',
        'prizepicks',
        'betmgm',
        'caesars',
        'projmins',
        'todaysmins'
    ]
    
    all_sql = []
    all_sql.append("-- SQL script to create tables in Supabase")
    all_sql.append("-- Generated from AWS RDS structure")
    all_sql.append("-- Run this in your Supabase SQL editor")
    all_sql.append("")
    all_sql.append("-- Create the wnba schema if it doesn't exist")
    all_sql.append("CREATE SCHEMA IF NOT EXISTS wnba;")
    all_sql.append("")
    
    for table in tables_to_migrate:
        logger.info(f"📊 Getting structure for table: {table}")
        columns_df = get_table_structure(table)
        
        if columns_df is not None and not columns_df.empty:
            sql = generate_create_table_sql(table, columns_df)
            if sql:
                all_sql.append(f"-- Create {table} table")
                all_sql.append(sql)
                all_sql.append("")
                logger.info(f"✅ Generated SQL for {table}")
        else:
            logger.warning(f"⚠️ Could not get structure for {table}")
    
    return '\n'.join(all_sql)

if __name__ == "__main__":
    sql = generate_all_table_sql()
    
    # Save to file
    with open('create_supabase_tables_dynamic.sql', 'w') as f:
        f.write(sql)
    
    logger.info("✅ Generated SQL file: create_supabase_tables_dynamic.sql")
    print("\nGenerated SQL:")
    print("="*50)
    print(sql)
