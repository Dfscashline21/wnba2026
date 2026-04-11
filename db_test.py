# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:39:10 2025

@author: trent
"""

from db_conn import get_db_connection

conn = get_db_connection()

if conn:
    print("✅ Connected to the database!")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            result = cur.fetchone()
            print("PostgreSQL version:", result)
    finally:
        conn.close()
        print("🔌 Connection closed.")
else:
    print("❌ Could not connect.")