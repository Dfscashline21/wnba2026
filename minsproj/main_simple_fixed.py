"""
WNBA Minutes Prediction Model - Simplified Demo (Unicode-safe)
Runs without optional dependencies and handles import errors gracefully
"""

import sys
import traceback
from datetime import datetime

def check_required_imports():
    """Check and import required modules"""
    missing_modules = []
    
    try:
        import pandas as pd
        import numpy as np
        print("[OK] Core data processing libraries loaded")
    except ImportError as e:
        missing_modules.append(f"pandas/numpy: {e}")
    
    try:
        from sklearn.ensemble import RandomForestRegressor
        print("[OK] Machine learning libraries loaded")
    except ImportError as e:
        missing_modules.append(f"sklearn: {e}")
    
    try:
        import requests
        print("[OK] Network libraries loaded")
    except ImportError as e:
        missing_modules.append(f"requests: {e}")
    
    if missing_modules:
        print("\n[ERROR] Missing required modules:")
        for module in missing_modules:
            print(f"   - {module}")
        print("\n[TIP] Run 'python install.py' to install dependencies")
        return False
    
    return True

def run_simple_demo():
    """Run simplified demonstration without external dependencies"""
    
    print("=" * 60)
    print("WNBA DAILY MINUTES PROJECTION MODEL - SIMPLE DEMO")
    print("=" * 60)
    
    # Import our modules
    try:
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        
        print("\n[INIT] Initializing prediction system (demo mode)...")
        
        # Create demo data
        n_players = 24  # 2 teams x 12 players
        demo_data = pd.DataFrame({
            'player_id': range(1, n_players + 1),
            'player_name': [f'Player_{i}' for i in range(1, n_players + 1)],
            'team': ['LAS'] * 12 + ['SEA'] * 12,
            'position': np.random.choice(['PG', 'SG', 'SF', 'PF', 'C'], n_players),
            'is_starter': ([True] * 5 + [False] * 7) * 2,
            'avg_minutes_season': np.random.normal(18, 8, n_players),
            'games_played': np.random.randint(15, 25, n_players),
            'age': np.random.randint(20, 35, n_players),
            'injury_status': np.random.choice(['available', 'questionable', 'probable'], n_players, p=[0.8, 0.15, 0.05])
        })
        
        # Ensure realistic minute ranges
        demo_data['avg_minutes_season'] = np.clip(demo_data['avg_minutes_season'], 0, 40)
        
        print("[OK] Demo data created")
        
        # Simple prediction logic (without ML models)
        print("\n[PRED] Generating predictions...")
        
        predictions = []
        
        for _, player in demo_data.iterrows():
            # Base prediction from season average
            base_minutes = player['avg_minutes_season']
            
            # Simple adjustments
            final_minutes = base_minutes
            
            # Starter boost
            if player['is_starter']:
                final_minutes += 3
            
            # Age adjustment
            if player['age'] > 32:
                final_minutes -= 1
                
            # Injury adjustment
            if player['injury_status'] == 'questionable':
                final_minutes *= 0.7
            elif player['injury_status'] == 'probable':
                final_minutes *= 0.9
                
            # Home court boost (assume LAS is home)
            if player['team'] == 'LAS':
                final_minutes += 1
                
            # Ensure realistic range
            final_minutes = max(0, min(final_minutes, 42))
            
            predictions.append({
                'player_name': player['player_name'],
                'team': player['team'],
                'position': player['position'],
                'predicted_minutes': round(final_minutes, 1),
                'confidence': 0.75 if player['injury_status'] == 'available' else 0.6,
                'key_factors': []
            })
            
            # Add key factors
            factors = []
            if player['is_starter']:
                factors.append('Starter (+3.0 min)')
            if player['age'] > 32:
                factors.append('Veteran rest (-1.0 min)')
            if player['injury_status'] != 'available':
                factors.append(f"Injury status: {player['injury_status']}")
            if player['team'] == 'LAS':
                factors.append('Home court (+1.0 min)')
                
            predictions[-1]['key_factors'] = factors
        
        # Display results
        print("[OK] Predictions generated")
        
        print("\n" + "="*50)
        print("GAME PREDICTION RESULTS")
        print("="*50)
        print(f"\n[GAME] Seattle Storm @ Las Vegas Aces")
        print(f"   Date: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"   Venue: Michelob ULTRA Arena")
        
        # Las Vegas Aces (Home)
        las_players = [p for p in predictions if p['team'] == 'LAS']
        las_players.sort(key=lambda x: x['predicted_minutes'], reverse=True)
        
        print(f"\n   Las Vegas Aces (Home) - Rotation:")
        for i, player in enumerate(las_players[:8], 1):
            starter_flag = "[S]" if i <= 5 else "   "
            confidence_icon = "[HIGH]" if player['confidence'] > 0.7 else "[MED] "
            
            print(f"   {starter_flag} {i:2d}. {player['player_name']:<15} "
                 f"{player['predicted_minutes']:5.1f} min {confidence_icon}")
            
            if player['key_factors']:
                for factor in player['key_factors'][:2]:
                    print(f"      + {factor}")
        
        # Seattle Storm (Away)
        sea_players = [p for p in predictions if p['team'] == 'SEA']
        sea_players.sort(key=lambda x: x['predicted_minutes'], reverse=True)
        
        print(f"\n   Seattle Storm (Away) - Rotation:")
        for i, player in enumerate(sea_players[:8], 1):
            starter_flag = "[S]" if i <= 5 else "   "
            confidence_icon = "[HIGH]" if player['confidence'] > 0.7 else "[MED] "
            
            print(f"   {starter_flag} {i:2d}. {player['player_name']:<15} "
                 f"{player['predicted_minutes']:5.1f} min {confidence_icon}")
            
            if player['key_factors']:
                for factor in player['key_factors'][:2]:
                    print(f"      + {factor}")
        
        # Summary statistics
        total_las_minutes = sum(p['predicted_minutes'] for p in las_players)
        total_sea_minutes = sum(p['predicted_minutes'] for p in sea_players)
        avg_confidence = np.mean([p['confidence'] for p in predictions])
        
        print(f"\n   [SUMMARY] Game Analysis:")
        print(f"      Total LAS minutes: {total_las_minutes:.0f}")
        print(f"      Total SEA minutes: {total_sea_minutes:.0f}")
        print(f"      Average confidence: {avg_confidence:.1f}%")
        print(f"      Rotation depth: {len([p for p in las_players if p['predicted_minutes'] > 10])} + {len([p for p in sea_players if p['predicted_minutes'] > 10])} players")
        
        # Save simple output
        print(f"\n[SAVE] Saving results...")
        
        # Create simple CSV output
        output_data = []
        for pred in predictions:
            output_data.append({
                'Player': pred['player_name'],
                'Team': pred['team'],
                'Position': pred['position'],
                'Predicted_Minutes': pred['predicted_minutes'],
                'Confidence': pred['confidence'],
                'Key_Factors': '; '.join(pred['key_factors'])
            })
        
        output_df = pd.DataFrame(output_data)
        csv_filename = f"simple_predictions_{datetime.now().strftime('%Y%m%d')}.csv"
        output_df.to_csv(csv_filename, index=False)
        print(f"[OK] Results saved to {csv_filename}")
        
        print("\n" + "="*60)
        print("[SUCCESS] SIMPLE DEMO COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\n[OK] This demonstrates core prediction logic")
        print("[TIP] For full system with ML models, run: python install.py")
        print("[TIP] Then run: python main.py")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Error during demo: {e}")
        traceback.print_exc()
        return False

def main():
    """Main entry point"""
    
    # Check required imports
    if not check_required_imports():
        print("\n[TIP] Try running: pip install pandas numpy scikit-learn requests")
        return False
    
    # Run simple demo
    return run_simple_demo()

if __name__ == "__main__":
    success = main()
    
    if not success:
        print("\n[HELP] Troubleshooting:")
        print("1. Install dependencies: python install.py")
        print("2. Or manually: pip install pandas numpy scikit-learn requests")
        print("3. For help, check README.md")
    
    sys.exit(0 if success else 1)