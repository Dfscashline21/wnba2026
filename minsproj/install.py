"""
WNBA Minutes Prediction Model - Installation Helper
Installs required dependencies and verifies system setup
"""

import subprocess
import sys
import importlib
from pathlib import Path

def install_requirements():
    """Install required packages"""
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("❌ requirements.txt not found!")
        return False
    
    print("📦 Installing required packages...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ])
        print("✅ All packages installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Installation failed: {e}")
        return False

def verify_installation():
    """Verify that all required modules can be imported"""
    required_modules = [
        'pandas', 'numpy', 'sklearn', 'xgboost', 'lightgbm',
        'requests', 'bs4', 'schedule', 'aiohttp', 'matplotlib',
        'seaborn', 'websocket', 'scipy'
    ]
    
    print("\n🔍 Verifying installation...")
    
    all_good = True
    for module in required_modules:
        try:
            if module == 'bs4':
                importlib.import_module('bs4')
            elif module == 'websocket':
                importlib.import_module('websocket')
            elif module == 'sklearn':
                importlib.import_module('sklearn')
            else:
                importlib.import_module(module)
            print(f"✅ {module}")
        except ImportError as e:
            print(f"❌ {module} - {e}")
            all_good = False
    
    return all_good

def check_optional_dependencies():
    """Check optional dependencies"""
    print("\n🔧 Checking optional dependencies...")
    
    optional_modules = {
        'tensorflow': 'Deep learning models (LSTM)',
        'pymc': 'Bayesian modeling'
    }
    
    for module, description in optional_modules.items():
        try:
            importlib.import_module(module)
            print(f"✅ {module} - {description}")
        except ImportError:
            print(f"⚠️  {module} - {description} (optional, install if needed)")

def main():
    """Main installation process"""
    print("=" * 60)
    print("WNBA MINUTES PREDICTION MODEL - INSTALLATION")
    print("=" * 60)
    
    # Install requirements
    if not install_requirements():
        print("\n❌ Installation failed. Please check error messages above.")
        return False
    
    # Verify installation
    if not verify_installation():
        print("\n❌ Some modules failed to import. Please check installation.")
        return False
    
    # Check optional dependencies
    check_optional_dependencies()
    
    print("\n" + "=" * 60)
    print("🎉 INSTALLATION COMPLETE!")
    print("=" * 60)
    print("\n✅ System is ready to use!")
    print("\nNext steps:")
    print("1. Run 'python main.py' for complete system demonstration")
    print("2. See README.md for usage examples")
    print("3. Check main.py for detailed implementation examples")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)