# -*- coding: utf-8 -*-
"""
dbt Supabase Setup Script
Creates the wnba schema and configures dbt for Supabase

@author: trent
"""

import os
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def setup_dbt_profiles():
    """Setup dbt profiles for Supabase"""
    print("🔧 Setting up dbt profiles for Supabase...")
    
    # Get user home directory
    home_dir = Path.home()
    dbt_dir = home_dir / ".dbt"
    profiles_file = dbt_dir / "profiles.yml"
    
    # Create .dbt directory if it doesn't exist
    dbt_dir.mkdir(exist_ok=True)
    
    # Read the profiles template
    current_dir = Path.cwd()
    profiles_template = current_dir / "profiles_supabase.yml"
    
    if not profiles_template.exists():
        print("❌ profiles_supabase.yml not found!")
        return False
    
    # Copy profiles file
    import shutil
    shutil.copy(profiles_template, profiles_file)
    print(f"✅ dbt profiles created at: {profiles_file}")
    
    return True

def create_supabase_schema():
    """Create the wnba schema in Supabase"""
    print("🏗️ Creating wnba schema in Supabase...")
    
    try:
        from supabase_conn import supabase_conn
        
        # Create schema using SQL
        schema_query = """
        CREATE SCHEMA IF NOT EXISTS wnba;
        """
        
        engine = supabase_conn.get_engine()
        with engine.connect() as conn:
            conn.execute(schema_query)
            conn.commit()
        
        print("✅ wnba schema created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error creating schema: {e}")
        return False

def test_dbt_connection():
    """Test dbt connection to Supabase"""
    print("🔍 Testing dbt connection to Supabase...")
    
    try:
        # Change to dbt directory
        os.chdir("wnba_dbt")
        
        # Run dbt debug
        result = subprocess.run(
            ["dbt", "debug"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        print("✅ dbt debug successful")
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
        os.chdir("..")

def run_dbt_models():
    """Run dbt models to create tables in Supabase"""
    print("🚀 Running dbt models in Supabase...")
    
    try:
        # Change to dbt directory
        os.chdir("wnba_dbt")
        
        # Run dbt models
        result = subprocess.run(
            ["dbt", "run"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        print("✅ dbt models run successful")
        print(result.stdout)
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt run failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Error running dbt models: {e}")
        return False
    finally:
        # Change back to original directory
        os.chdir("..")

def update_environment_variables():
    """Update environment variables for dbt Supabase connection"""
    print("🔧 Updating environment variables for dbt...")
    
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
    os.environ["SUPABASE_SCHEMA"] = "wnba"
    
    print("✅ Environment variables updated")
    return True

def main():
    """Main setup function"""
    print("🚀 Setting up dbt for Supabase...")
    print("=" * 50)
    
    # Step 1: Update environment variables
    if not update_environment_variables():
        print("❌ Failed to update environment variables")
        return
    
    # Step 2: Setup dbt profiles
    if not setup_dbt_profiles():
        print("❌ Failed to setup dbt profiles")
        return
    
    # Step 3: Create Supabase schema
    if not create_supabase_schema():
        print("❌ Failed to create Supabase schema")
        return
    
    # Step 4: Test dbt connection
    if not test_dbt_connection():
        print("❌ Failed to test dbt connection")
        return
    
    # Step 5: Run dbt models (optional - ask user)
    print("\n" + "=" * 50)
    print("📋 SETUP COMPLETE")
    print("=" * 50)
    print("✅ dbt is now configured for Supabase")
    print("✅ wnba schema created")
    print("✅ Connection tested successfully")
    
    response = input("\nDo you want to run dbt models now? (y/n): ")
    if response.lower() == 'y':
        if run_dbt_models():
            print("🎉 dbt models successfully created in Supabase!")
        else:
            print("⚠️ dbt models failed to run. Check the error messages above.")
    else:
        print("📝 You can run dbt models later with:")
        print("   cd wnba_dbt")
        print("   dbt run")

if __name__ == "__main__":
    main()
