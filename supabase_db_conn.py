# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 15:46:19 2025

@author: trent
"""


import psycopg2
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()  # Load .env variables into environment

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST"),
            database=os.getenv("SUPABASE_DBNAME"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD"),
            port=os.getenv("SUPABASE_PORT", 5432)  # default to 5432
        )
        return conn
    except Exception as e:
        print("Database connection error:", e)
        return None


def get_db_engine():
    try:
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT", 5432)
        db = os.getenv("SUPABASE_DBNAME")

        # engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
        # engine = create_engine(f"postgresql://postgres:{password}@db.eblwllnfjdsmxesrlfja.supabase.co:5432/postgres")
        
        engine = create_engine(f"postgresql://postgres.eblwllnfjdsmxesrlfja:{password}@aws-1-us-west-1.pooler.supabase.com:5432/postgres")
        return engine
    except Exception as e:
        print("❌ Could not create engine:", e)
        return None