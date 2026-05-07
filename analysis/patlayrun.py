from .parlay import WNBAParlayAnalyzer

# Method 1: Load from CSV file
analyzer = WNBAParlayAnalyzer(r'dk_caesars_comparison_20250826_152704.csv')

# Find optimal parlays
parlays = analyzer.find_optimal_parlays(
    min_legs=2,
    max_legs=6,
    min_ev=0,        # Minimum $50 expected value
    min_roi=5,       # Minimum 50% ROI
    max_risk=.80,    # Max 20% chance of losing
    min_value_category='Medium Value'
)

# Generate and print report
report = analyzer.generate_parlay_report(parlays, top_n=15)
print(report)

# Export results
if parlays:
    analyzer.export_results(parlays, 'best_wnba_parlays.csv')
    print(f"\nExported {len(parlays)} profitable parlays to 'best_wnba_parlays.csv'")