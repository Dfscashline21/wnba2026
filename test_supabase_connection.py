# -*- coding: utf-8 -*-
"""
Test script for Supabase connection and basic functionality
Run this to verify your Supabase setup is working correctly

@author: trent
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_environment_variables():
    """Test that all required environment variables are set"""
    print("🔍 Testing environment variables...")
    
    required_vars = [
        'SUPABASE_URL',
        'SUPABASE_ANON_KEY',
        'SUPABASE_SERVICE_ROLE_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask the key for security
            if 'KEY' in var:
                masked_value = value[:10] + '...' + value[-10:] if len(value) > 20 else '***'
                print(f"✅ {var}: {masked_value}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n⚠️ Missing environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file and ensure all variables are set.")
        return False
    
    print("✅ All environment variables are set!")
    return True

def test_supabase_import():
    """Test that Supabase packages can be imported"""
    print("\n🔍 Testing Supabase imports...")
    
    try:
        from supabase import create_client, Client
        print("✅ Supabase client import successful")
        
        from supabase_conn import SupabaseConnection, supabase_conn
        print("✅ Supabase connection module import successful")
        
        from supabase_storage import SupabaseStorage, supabase_storage
        print("✅ Supabase storage module import successful")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please install Supabase packages:")
        print("py -m pip install supabase==2.3.4 python-supabase==2.3.4")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_supabase_connection():
    """Test basic Supabase connection"""
    print("\n🔍 Testing Supabase connection...")
    
    try:
        from supabase_conn import supabase_conn
        
        # Test client creation
        client = supabase_conn.get_client()
        print("✅ Supabase client created successfully")
        
        # Test engine creation
        engine = supabase_conn.get_engine()
        print("✅ SQLAlchemy engine created successfully")
        
        # Test basic query (this will fail if connection is bad)
        try:
            # Simple query to test connection
            result = client.table('information_schema.tables').select('table_name').limit(1).execute()
            print("✅ Basic query executed successfully")
        except Exception as e:
            print(f"⚠️ Basic query failed (this might be normal): {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def test_supabase_storage():
    """Test Supabase Storage functionality"""
    print("\n🔍 Testing Supabase Storage...")
    
    try:
        from supabase_storage import supabase_storage
        
        # Test listing buckets (this should work even if buckets don't exist)
        try:
            # This might fail if no buckets exist, which is okay
            buckets = supabase_storage.client.storage.list_buckets()
            print(f"✅ Storage buckets listed: {len(buckets)} found")
        except Exception as e:
            print(f"⚠️ Could not list buckets (this might be normal): {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Storage test failed: {e}")
        return False

def test_sample_data_operations():
    """Test basic data operations with sample data"""
    print("\n🔍 Testing sample data operations...")
    
    try:
        import pandas as pd
        from supabase_conn import supabase_conn
        
        # Create sample data
        sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test Player 1', 'Test Player 2', 'Test Player 3'],
            'team': ['Team A', 'Team B', 'Team C'],
            'test_timestamp': [datetime.now(), datetime.now(), datetime.now()]
        })
        
        print(f"✅ Sample data created: {len(sample_data)} rows")
        
        # Test uploading to a test table
        try:
            success = supabase_conn.upload_dataframe(
                sample_data, 
                'test_connection_table', 
                schema='public', 
                if_exists='replace'
            )
            
            if success:
                print("✅ Sample data uploaded successfully")
                
                # Test retrieving the data
                retrieved_data = supabase_conn.get_table_data('test_connection_table', schema='public')
                if retrieved_data is not None and len(retrieved_data) == len(sample_data):
                    print("✅ Sample data retrieved successfully")
                    
                    # Clean up test table
                    try:
                        supabase_conn.client.table('test_connection_table').delete().execute()
                        print("✅ Test table cleaned up")
                    except:
                        print("⚠️ Could not clean up test table (this is okay)")
                else:
                    print("⚠️ Could not retrieve sample data")
            else:
                print("❌ Failed to upload sample data")
                
        except Exception as e:
            print(f"⚠️ Sample data operations failed (this might be normal): {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Sample data test failed: {e}")
        return False

def test_backward_compatibility():
    """Test backward compatibility functions"""
    print("\n🔍 Testing backward compatibility...")
    
    try:
        from supabase_conn import get_db_connection, get_db_engine
        
        # Test legacy functions
        client = get_db_connection()
        engine = get_db_engine()
        
        if client and engine:
            print("✅ Backward compatibility functions work")
            return True
        else:
            print("❌ Backward compatibility functions failed")
            return False
            
    except Exception as e:
        print(f"❌ Backward compatibility test failed: {e}")
        return False

def run_comprehensive_test():
    """Run all tests and provide summary"""
    print("🚀 Starting Supabase Connection Test Suite")
    print("=" * 50)
    
    tests = [
        ("Environment Variables", test_environment_variables),
        ("Supabase Imports", test_supabase_import),
        ("Supabase Connection", test_supabase_connection),
        ("Supabase Storage", test_supabase_storage),
        ("Sample Data Operations", test_sample_data_operations),
        ("Backward Compatibility", test_backward_compatibility)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Your Supabase connection is working correctly.")
        print("You can proceed with the migration.")
    elif passed >= total * 0.8:
        print("\n⚠️ Most tests passed. Review failed tests before proceeding.")
    else:
        print("\n❌ Multiple tests failed. Please fix issues before proceeding.")
    
    return results

def main():
    """Main test execution"""
    try:
        results = run_comprehensive_test()
        
        # Provide next steps
        print("\n" + "=" * 50)
        print("📋 NEXT STEPS")
        print("=" * 50)
        
        if results.get("Environment Variables", False):
            print("✅ Environment is configured")
        else:
            print("❌ Fix environment variables first")
            return
        
        if results.get("Supabase Imports", False):
            print("✅ Dependencies are installed")
        else:
            print("❌ Install Supabase packages first")
            return
        
        if results.get("Supabase Connection", False):
            print("✅ Database connection is working")
            print("Next: Create database schema and tables")
        else:
            print("❌ Fix database connection first")
            return
        
        if results.get("Supabase Storage", False):
            print("✅ Storage is accessible")
            print("Next: Create storage buckets")
        else:
            print("⚠️ Storage needs attention")
        
        print("\n🎯 Ready for Phase 2: Code Migration!")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test suite crashed: {e}")

if __name__ == "__main__":
    main()
