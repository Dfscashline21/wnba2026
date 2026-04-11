# -*- coding: utf-8 -*-
"""
Test the fixed Supabase connection with connection pooling
"""

import sys
import traceback

print("🧪 Testing fixed Supabase connection (connection pooling)...")
print("=" * 50)

try:
    # Test importing the fixed module
    print("📋 Testing module import...")
    from supabase_conn_fixed import supabase_conn, get_supabase_engine
    
    if supabase_conn is None:
        print("❌ Supabase connection failed to initialize")
        sys.exit(1)
    
    print("✅ Module imported successfully")
    
    # Test getting the engine
    print("\n📋 Testing engine creation...")
    engine = get_supabase_engine()
    print("✅ Engine created successfully")
    
    # Test a simple query
    print("\n📋 Testing simple query...")
    with engine.connect() as conn:
        result = conn.execute("SELECT current_database(), current_user, version();")
        db_info = result.fetchone()
        print(f"✅ Query successful:")
        print(f"   Database: {db_info[0]}")
        print(f"   User: {db_info[1]}")
        print(f"   Version: {db_info[2]}")
    
    # Test schema access
    print("\n📋 Testing schema access...")
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'wnba';
        """)
        schema_exists = result.fetchone()
        if schema_exists:
            print("✅ wnba schema exists")
        else:
            print("⚠️ wnba schema does not exist")
    
    print("\n" + "=" * 50)
    print("🎉 SUPABASE CONNECTION TEST SUCCESSFUL!")
    print("=" * 50)
    print("✅ Module import: PASSED")
    print("✅ Engine creation: PASSED")
    print("✅ Database query: PASSED")
    print("✅ Schema access: PASSED")
    print("\n🎯 You can now run the migration script!")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    print("\n📋 Full error details:")
    traceback.print_exc()
    sys.exit(1)
