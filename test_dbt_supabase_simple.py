# -*- coding: utf-8 -*-
"""
Simple dbt Supabase Test (Bypass Supabase Client Issues)
Test dbt with Supabase using direct database connection

@author: trent
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
import tempfile
import shutil

load_dotenv()

def create_test_profiles():
    """Create a temporary profiles file for testing"""
    print("🔧 Creating temporary dbt profiles for testing...")
    
    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    test_profiles_file = temp_dir / "profiles.yml"
    
    # Read the profiles template
    current_dir = Path.cwd()
    profiles_template = current_dir / "profiles_supabase.yml"
    
    if not profiles_template.exists():
        print("❌ profiles_supabase.yml not found!")
        return None, None
    
    # Copy profiles file to temp location
    shutil.copy(profiles_template, test_profiles_file)
    print(f"✅ Temporary dbt profiles created at: {test_profiles_file}")
    
    return temp_dir, test_profiles_file

def update_environment_variables():
    """Update environment variables for dbt Supabase connection"""
    print("🔧 Setting up environment variables for dbt testing...")
    
    # Extract Supabase connection info from URL
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        print("❌ SUPABASE_URL not set")
        return False
    
    # Parse Supabase URL to get host
    # URL format: https://project-id.supabase.co
    host = supabase_url.replace("https://", "").replace(".supabase.co", "")
    host = f"{host}.supabase.co"
    
    # Set environment variables for dbt
    os.environ["SUPABASE_HOST"] = host
    os.environ["SUPABASE_USER"] = "postgres"
    os.environ["SUPABASE_PASSWORD"] = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    os.environ["SUPABASE_PORT"] = "5432"
    os.environ["SUPABASE_DBNAME"] = "postgres"
    os.environ["SUPABASE_SCHEMA"] = "wnba_test"  # Use test schema
    
    print("✅ Environment variables updated")
    return True

def create_test_schema_direct():
    """Create test schema using direct SQLAlchemy connection"""
    print("🏗️ Creating test schema in Supabase...")
    
    try:
        import psycopg2
        from sqlalchemy import create_engine
        
        # Get connection details
        host = os.environ["SUPABASE_HOST"]
        user = os.environ["SUPABASE_USER"]
        password = os.environ["SUPABASE_PASSWORD"]
        port = os.environ["SUPABASE_PORT"]
        dbname = os.environ["SUPABASE_DBNAME"]
        
        # Create connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # Create engine
        engine = create_engine(
            connection_string,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 10
            }
        )
        
        # Create schema
        with engine.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS wnba_test;")
            conn.commit()
        
        print("✅ Test schema 'wnba_test' created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error creating test schema: {e}")
        return False

def test_dbt_connection(temp_dir):
    """Test dbt connection to Supabase using temporary profiles"""
    print("🔍 Testing dbt connection to Supabase...")
    
    try:
        # Change to dbt directory
        original_dir = os.getcwd()
        os.chdir("wnba_dbt")
        
        # Set DBT_PROFILES_DIR to use our temporary profiles
        os.environ["DBT_PROFILES_DIR"] = str(temp_dir)
        
        # Run dbt debug
        result = subprocess.run(
            ["dbt", "debug"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        print("✅ dbt debug successful")
        print("=" * 40)
        print("DBT DEBUG OUTPUT:")
        print("=" * 40)
        print(result.stdout)
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt debug failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Error testing dbt connection: {e}")
        return False
    finally:
        # Change back to original directory
        os.chdir(original_dir)

def test_simple_model(temp_dir):
    """Test a simple dbt model"""
    print("🚀 Testing simple dbt model...")
    
    try:
        # Change to dbt directory
        original_dir = os.getcwd()
        os.chdir("wnba_dbt")
        
        # Set DBT_PROFILES_DIR to use our temporary profiles
        os.environ["DBT_PROFILES_DIR"] = str(temp_dir)
        
        # Create a simple test model
        test_model_content = """
-- Simple test model
SELECT 
    1 as id,
    'test' as name,
    CURRENT_TIMESTAMP as created_at
"""
        
        # Write test model to a temporary file
        test_model_file = Path("models") / "test_simple_model.sql"
        test_model_file.write_text(test_model_content)
        
        print("📝 Created test model: test_simple_model.sql")
        
        # Run the test model
        result = subprocess.run(
            ["dbt", "run", "--select", "test_simple_model"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        print("✅ Test model executed successfully")
        print(result.stdout)
        
        # Clean up test model
        test_model_file.unlink()
        print("🧹 Test model cleaned up")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt model test failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Error testing dbt model: {e}")
        return False
    finally:
        # Change back to original directory
        os.chdir(original_dir)

def verify_test_results():
    """Verify that test tables were created in Supabase"""
    print("🔍 Verifying test results in Supabase...")
    
    try:
        import psycopg2
        from sqlalchemy import create_engine
        import pandas as pd
        
        # Get connection details
        host = os.environ["SUPABASE_HOST"]
        user = os.environ["SUPABASE_USER"]
        password = os.environ["SUPABASE_PASSWORD"]
        port = os.environ["SUPABASE_PORT"]
        dbname = os.environ["SUPABASE_DBNAME"]
        
        # Create connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # Create engine
        engine = create_engine(
            connection_string,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 10
            }
        )
        
        # Check what tables exist in the test schema
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'wnba_test'
        ORDER BY table_name;
        """
        
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            print("✅ Test tables found in Supabase:")
            for _, row in df.iterrows():
                print(f"   📊 {row['table_name']}")
        else:
            print("⚠️ No test tables found in Supabase")
            
        return True
        
    except Exception as e:
        print(f"❌ Error verifying test results: {e}")
        return False

def cleanup_test_schema():
    """Clean up test schema (optional)"""
    print("🧹 Cleaning up test schema...")
    
    try:
        import psycopg2
        from sqlalchemy import create_engine
        
        # Get connection details
        host = os.environ["SUPABASE_HOST"]
        user = os.environ["SUPABASE_USER"]
        password = os.environ["SUPABASE_PASSWORD"]
        port = os.environ["SUPABASE_PORT"]
        dbname = os.environ["SUPABASE_DBNAME"]
        
        # Create connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # Create engine
        engine = create_engine(
            connection_string,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 10
            }
        )
        
        # Drop test schema
        with engine.connect() as conn:
            conn.execute("DROP SCHEMA IF EXISTS wnba_test CASCADE;")
            conn.commit()
        
        print("✅ Test schema cleaned up")
        return True
        
    except Exception as e:
        print(f"⚠️ Error cleaning up test schema: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 Testing dbt Supabase Setup (Direct Connection)")
    print("=" * 60)
    print("This test will:")
    print("✅ Use temporary dbt profiles")
    print("✅ Create test schema (wnba_test)")
    print("✅ Test connection without Supabase client")
    print("✅ Test simple dbt model")
    print("=" * 60)
    
    temp_dir = None
    
    try:
        # Step 1: Update environment variables
        if not update_environment_variables():
            print("❌ Failed to update environment variables")
            return
        
        # Step 2: Create temporary profiles
        temp_dir, profiles_file = create_test_profiles()
        if not temp_dir:
            print("❌ Failed to create temporary profiles")
            return
        
        # Step 3: Create test schema
        if not create_test_schema_direct():
            print("❌ Failed to create test schema")
            return
        
        # Step 4: Test dbt connection
        if not test_dbt_connection(temp_dir):
            print("❌ Failed to test dbt connection")
            return
        
        # Step 5: Test simple model
        test_model = input("\nDo you want to test a simple dbt model? (y/n): ").lower().strip()
        if test_model == 'y':
            if not test_simple_model(temp_dir):
                print("❌ Failed to test dbt model")
                return
            
            # Step 6: Verify results
            if not verify_test_results():
                print("❌ Failed to verify test results")
                return
        
        # Step 7: Cleanup (optional)
        cleanup = input("\nDo you want to clean up the test schema? (y/n): ").lower().strip()
        if cleanup == 'y':
            cleanup_test_schema()
        
        # Success summary
        print("\n" + "=" * 60)
        print("🎉 SUPABASE DBT TEST COMPLETE")
        print("=" * 60)
        print("✅ Connection test: PASSED")
        print("✅ Schema creation: PASSED")
        if test_model == 'y':
            print("✅ Model testing: PASSED")
        print("✅ Your existing dbt setup is unchanged")
        print("\n🎯 You can now proceed with the full migration!")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
    finally:
        # Cleanup temporary directory
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                print(f"🧹 Temporary files cleaned up")
            except:
                print(f"⚠️ Could not clean up temporary files: {temp_dir}")

if __name__ == "__main__":
    main()
