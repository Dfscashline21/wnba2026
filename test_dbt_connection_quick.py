# -*- coding: utf-8 -*-
"""
Quick dbt Supabase Connection Test
Test dbt connection to Supabase without schema creation

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
    os.environ["SUPABASE_SCHEMA"] = "public"  # Use public schema for testing
    
    print("✅ Environment variables updated")
    print(f"   Host: {host}")
    print(f"   Database: postgres")
    print(f"   Schema: public")
    return True

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
        print("Running dbt debug...")
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

def test_dbt_list(temp_dir):
    """Test dbt list command"""
    print("📋 Testing dbt list command...")
    
    try:
        # Change to dbt directory
        original_dir = os.getcwd()
        os.chdir("wnba_dbt")
        
        # Set DBT_PROFILES_DIR to use our temporary profiles
        os.environ["DBT_PROFILES_DIR"] = str(temp_dir)
        
        # Run dbt list
        print("Running dbt list...")
        result = subprocess.run(
            ["dbt", "ls"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        print("✅ dbt list successful")
        print("=" * 40)
        print("DBT LIST OUTPUT:")
        print("=" * 40)
        print(result.stdout)
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt list failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Error testing dbt list: {e}")
        return False
    finally:
        # Change back to original directory
        os.chdir(original_dir)

def main():
    """Main test function"""
    print("🧪 Quick dbt Supabase Connection Test")
    print("=" * 50)
    print("This test will:")
    print("✅ Use temporary dbt profiles")
    print("✅ Test dbt connection to Supabase")
    print("✅ Test dbt list command")
    print("✅ Use public schema (no schema creation needed)")
    print("=" * 50)
    
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
        
        # Step 3: Test dbt connection
        if not test_dbt_connection(temp_dir):
            print("❌ Failed to test dbt connection")
            return
        
        # Step 4: Test dbt list
        if not test_dbt_list(temp_dir):
            print("❌ Failed to test dbt list")
            return
        
        # Success summary
        print("\n" + "=" * 50)
        print("🎉 DBT SUPABASE CONNECTION TEST COMPLETE")
        print("=" * 50)
        print("✅ Connection test: PASSED")
        print("✅ List command: PASSED")
        print("✅ Your existing dbt setup is unchanged")
        print("\n🎯 dbt can connect to Supabase!")
        print("Next: Test with actual models")
        
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
