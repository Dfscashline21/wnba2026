# -*- coding: utf-8 -*-
"""
Test script for Caesars WNBA player props pull

@author: trent
"""
import sys
import os

# Add current directory to path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from caesars_pull import pull_caesars, main as caesars_main
    print("✅ Successfully imported caesars_pull module")
except ImportError as e:
    print(f"❌ Failed to import caesars_pull: {e}")
    print("💡 Make sure caesars_pull.py is in the same directory")

try:
    from sportbooks_pull import pull_caesars as sb_pull_caesars
    print("✅ Successfully imported sportbooks_pull module")
except ImportError as e:
    print(f"❌ Failed to import sportbooks_pull: {e}")

def test_caesars_pull():
    """
    Test the Caesars pull functionality
    """
    print("\n🚀 Testing Caesars pull functionality...")
    
    # Test the standalone Caesars pull
    print("\n📊 Testing standalone Caesars pull...")
    try:
        caesars_data = pull_caesars()
        if caesars_data is not None and not caesars_data.empty:
            print(f"✅ Standalone pull successful: {len(caesars_data)} records")
            print("\n📋 Sample data:")
            print(caesars_data.head())
        else:
            print("⚠️ Standalone pull returned empty data")
    except Exception as e:
        print(f"❌ Standalone pull failed: {e}")
    
    # Test the sportbooks_pull version
    print("\n📊 Testing sportbooks_pull version...")
    try:
        sb_caesars_data = sb_pull_caesars()
        if sb_caesars_data is not None and not sb_caesars_data.empty:
            print(f"✅ Sportbooks pull successful: {len(sb_caesars_data)} records")
            print("\n📋 Sample data:")
            print(sb_caesars_data.head())
        else:
            print("⚠️ Sportbooks pull returned empty data")
    except Exception as e:
        print(f"❌ Sportbooks pull failed: {e}")

def test_main_function():
    """
    Test the main function from caesars_pull
    """
    print("\n🚀 Testing main function...")
    try:
        result = caesars_main()
        if result is not None and not result.empty:
            print(f"✅ Main function successful: {len(result)} records")
        else:
            print("⚠️ Main function returned empty data")
    except Exception as e:
        print(f"❌ Main function failed: {e}")

if __name__ == "__main__":
    print("🧪 Caesars WNBA Player Props Test Script")
    print("=" * 50)
    
    # Test individual functions
    test_caesars_pull()
    
    # Test main function
    test_main_function()
    
    print("\n�� Test completed!")
