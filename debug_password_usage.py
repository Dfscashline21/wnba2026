# -*- coding: utf-8 -*-
"""
Debug script to show what password is being used
"""

import os
from dotenv import load_dotenv

load_dotenv()

print("🔍 Debugging Supabase password usage...")
print("=" * 50)

# Show all relevant environment variables
print("📋 Environment Variables:")
print(f"SUPABASE_URL: {os.getenv('SUPABASE_URL')}")
print(f"SUPABASE_ANON_KEY: {os.getenv('SUPABASE_ANON_KEY')[:20]}...")
print(f"SUPABASE_SERVICE_ROLE_KEY: {os.getenv('SUPABASE_SERVICE_ROLE_KEY')[:20]}...")
print(f"SUPABASE_HOST: {os.getenv('SUPABASE_HOST')}")
print(f"SUPABASE_USER: {os.getenv('SUPABASE_USER')}")
print(f"SUPABASE_PASSWORD: {os.getenv('SUPABASE_PASSWORD')[:20]}...")
print(f"SUPABASE_PORT: {os.getenv('SUPABASE_PORT')}")
print(f"SUPABASE_DBNAME: {os.getenv('SUPABASE_DBNAME')}")

print("\n" + "=" * 50)
print("🔧 Current Connection Logic:")

# Simulate what the current code is doing
url = os.getenv("SUPABASE_URL")
service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
user = os.getenv("SUPABASE_USER", "postgres")
dbname = os.getenv("SUPABASE_DBNAME", "postgres")

if url.startswith('db.'):
    connection_string = f"postgresql://{user}:{service_key}@{url}:6543/{dbname}"
else:
    host = os.getenv("SUPABASE_HOST")
    project_id = host.replace('.supabase.co', '')
    pooler_url = f"db.{project_id}.supabase.co"
    connection_string = f"postgresql://{user}:{service_key}@{pooler_url}:6543/{dbname}"

print(f"🔗 Connection String: {connection_string}")
print(f"👤 User: {user}")
print(f"🔑 Password being used: {service_key[:20]}... (SUPABASE_SERVICE_ROLE_KEY)")
print(f"🌐 Host: {url if url.startswith('db.') else pooler_url}")
print(f"🔌 Port: 6543 (connection pooler)")

print("\n" + "=" * 50)
print("🎯 Analysis:")
print("✅ The code is using SUPABASE_SERVICE_ROLE_KEY as the password")
print("✅ This is correct for Supabase database connections")
print("❌ The SASL authentication error suggests the connection pooler")
print("   might require different authentication parameters")
print("\n💡 Next steps:")
print("   1. Check if database access is enabled in Supabase dashboard")
print("   2. Verify the connection string format in Supabase settings")
print("   3. Try direct PostgreSQL connection (port 5432) instead of pooler")
