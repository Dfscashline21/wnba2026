# -*- coding: utf-8 -*-
"""
Test script to check environment variables and Supabase connection
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("🔍 Checking environment variables...")
print("=" * 50)

# Check for Supabase environment variables
supabase_vars = [
    'SUPABASE_URL',
    'SUPABASE_ANON_KEY', 
    'SUPABASE_SERVICE_ROLE_KEY',
    'SUPABASE_HOST',
    'SUPABASE_USER',
    'SUPABASE_PASSWORD',
    'SUPABASE_PORT',
    'SUPABASE_DBNAME',
    'SUPABASE_SCHEMA'
]

print("📋 Supabase Environment Variables:")
for var in supabase_vars:
    value = os.getenv(var)
    if value:
        # Mask sensitive values
        if 'KEY' in var or 'PASSWORD' in var:
            masked_value = value[:10] + "..." if len(value) > 10 else "***"
            print(f"   ✅ {var}: {masked_value}")
        else:
            print(f"   ✅ {var}: {value}")
    else:
        print(f"   ❌ {var}: Not set")

print("\n📋 Other Environment Variables:")
other_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_PORT']
for var in other_vars:
    value = os.getenv(var)
    if value:
        if 'PASSWORD' in var:
            masked_value = value[:10] + "..." if len(value) > 10 else "***"
            print(f"   ✅ {var}: {masked_value}")
        else:
            print(f"   ✅ {var}: {value}")
    else:
        print(f"   ❌ {var}: Not set")

print("\n" + "=" * 50)
print("🎯 Next Steps:")
if os.getenv('SUPABASE_URL') and os.getenv('SUPABASE_SERVICE_ROLE_KEY'):
    print("✅ Supabase environment variables are set!")
    print("   We can proceed with fixing the connection.")
else:
    print("❌ Missing Supabase environment variables.")
    print("   Please check your .env file or set the required variables.")
