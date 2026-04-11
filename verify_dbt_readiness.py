# -*- coding: utf-8 -*-
"""
Quick dbt Setup Verification
Check your current dbt configuration and Supabase readiness

@author: trent
"""

import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def check_current_dbt_setup():
    """Check current dbt setup"""
    print("🔍 Checking current dbt setup...")
    
    # Check if dbt is installed
    try:
        result = subprocess.run(["dbt", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ dbt is installed")
            print(f"   Version: {result.stdout.strip()}")
        else:
            print("❌ dbt is not installed or not in PATH")
            return False
    except FileNotFoundError:
        print("❌ dbt command not found")
        return False
    
    # Check current profiles
    home_dir = Path.home()
    profiles_file = home_dir / ".dbt" / "profiles.yml"
    
    if profiles_file.exists():
        print(f"✅ Current dbt profiles found: {profiles_file}")
        print("   (This will be preserved during testing)")
    else:
        print("⚠️ No current dbt profiles found")
    
    return True

def check_supabase_environment():
    """Check Supabase environment variables"""
    print("\n🔍 Checking Supabase environment...")
    
    required_vars = [
        'SUPABASE_URL',
        'SUPABASE_ANON_KEY', 
        'SUPABASE_SERVICE_ROLE_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
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
        return False
    
    print("✅ All Supabase environment variables are set")
    return True

def check_dbt_project():
    """Check dbt project structure"""
    print("\n🔍 Checking dbt project...")
    
    dbt_dir = Path("wnba_dbt")
    if not dbt_dir.exists():
        print("❌ wnba_dbt directory not found")
        return False
    
    print(f"✅ dbt project found: {dbt_dir}")
    
    # Check for dbt_project.yml
    project_file = dbt_dir / "dbt_project.yml"
    if project_file.exists():
        print("✅ dbt_project.yml found")
    else:
        print("❌ dbt_project.yml not found")
        return False
    
    # Check for models directory
    models_dir = dbt_dir / "models"
    if models_dir.exists():
        print("✅ models directory found")
        
        # Count model files
        model_files = list(models_dir.rglob("*.sql"))
        print(f"   Found {len(model_files)} model files")
    else:
        print("❌ models directory not found")
        return False
    
    return True

def check_supabase_connection():
    """Test Supabase connection"""
    print("\n🔍 Testing Supabase connection...")
    
    try:
        from supabase_conn import supabase_conn
        
        # Test basic connection
        client = supabase_conn.get_client()
        print("✅ Supabase client created successfully")
        
        # Test engine
        engine = supabase_conn.get_engine()
        print("✅ SQLAlchemy engine created successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return False

def main():
    """Main verification function"""
    print("🔍 DBT SUPABASE READINESS CHECK")
    print("=" * 50)
    
    checks = [
        ("Current dbt setup", check_current_dbt_setup),
        ("Supabase environment", check_supabase_environment),
        ("dbt project structure", check_dbt_project),
        ("Supabase connection", check_supabase_connection)
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"❌ {check_name} check failed: {e}")
            results[check_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 READINESS SUMMARY")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for check_name, result in results.items():
        status = "✅ READY" if result else "❌ NOT READY"
        print(f"{check_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n🎉 All checks passed! You're ready to test dbt with Supabase.")
        print("Run: py test_dbt_supabase_safe.py")
    elif passed >= total * 0.75:
        print("\n⚠️ Most checks passed. Review failed checks before proceeding.")
    else:
        print("\n❌ Multiple checks failed. Fix issues before proceeding.")
    
    # Next steps
    print("\n📋 NEXT STEPS:")
    if results.get("Current dbt setup", False) and results.get("Supabase environment", False):
        print("1. Run safe test: py test_dbt_supabase_safe.py")
    else:
        print("1. Fix environment issues above")
        print("2. Install dbt if needed: py -m pip install dbt-postgres")
        print("3. Set up Supabase environment variables")
    
    print("2. If test passes, proceed with full migration")
    print("3. If test fails, review error messages")

if __name__ == "__main__":
    main()
