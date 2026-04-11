# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:38:05 2025

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
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", 5432)  # default to 5432
        )
        return conn
    except Exception as e:
        print("Database connection error:", e)
        return None


def get_db_engine():
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", 5432)
        db = os.getenv("DB_NAME")

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
        return engine
    except Exception as e:
        print("❌ Could not create engine:", e)
        return None