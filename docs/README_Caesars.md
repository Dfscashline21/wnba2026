# Caesars WNBA Player Props Pull Script

This script pulls WNBA player prop over/under lines and odds from Caesars sportbook.

## Features

- **Multiple Data Sources**: Tries direct Caesars API, Oddsshopper API, and website scraping
- **Comprehensive Coverage**: Pulls all available WNBA player props
- **Data Cleaning**: Automatically processes and structures the data
- **Error Handling**: Robust error handling with fallback methods
- **Rate Limiting**: Respectful API usage with built-in delays

## Files

- `caesars_pull.py` - Standalone Caesars pull script
- `sportbooks_pull.py` - Updated with improved Caesars function
- `test_caesars.py` - Test script to verify functionality

## Installation

Make sure you have the required dependencies:

```bash
py -m pip install -r requirements.txt
```

## Usage

### Method 1: Standalone Script

```python
from caesars_pull import pull_caesars

# Pull Caesars data
caesars_data = pull_caesars()

# Check if data was retrieved
if not caesars_data.empty:
    print(f"Retrieved {len(caesars_data)} records")
    print(caesars_data.head())
```

### Method 2: From sportbooks_pull Module

```python
from sportbooks_pull import pull_caesars

# Pull Caesars data
caesars_data = pull_caesars()

# Process the data
if not caesars_data.empty:
    # Data is already in pivot table format
    print(caesars_data.head())
```

### Method 3: Command Line

```bash
py caesars_pull.py
```

### Method 4: Test Script

```bash
py test_caesars.py
```

## Data Structure

The script returns a pandas DataFrame with the following columns:

- `player` - Player name
- `date` - Game date
- `offerName` - Type of prop (e.g., "Points", "Rebounds", "Assists")
- `over/under` - Whether it's an over or under bet
- `line` - The over/under line value
- `americanOdds` - American odds format
- `sportsbook` - Always "Caesars"
- `game_time` - Full game timestamp

## Output Formats

### Raw Data
Returns all props with individual over/under lines and odds.

### Pivot Table (Default)
Creates a pivot table with:
- Index: `player` and `date`
- Columns: Different prop types
- Values: Over lines (excluding under lines)

## Configuration

### Date Range
The script automatically sets a date range:
- Start: 7 days ago
- End: 7 days from now

You can modify this in the `pull_caesars_oddsshopper()` function:

```python
start_date = current_date - timedelta(days=7)  # Change 7 to desired days
end_date = current_date + timedelta(days=7)    # Change 7 to desired days
```

### State/Location
Currently set to New Jersey (NJ) based on the Caesars URL structure. The script automatically tries multiple locations:

```python
# Supported locations (automatically tried)
locations = ['nj', 'ny', 'pa', 'mi', 'in', 'il', 'co', 'nv', 'az']

# Change default state in Oddsshopper API
params = {
    'state': 'NJ',  # Change to your preferred state
    # ... other params
}
```

## Error Handling

The script includes multiple fallback methods:

1. **Direct Caesars API** - Tries multiple common endpoints with enhanced patterns
2. **Caesars Sportsbook Pages** - Direct access to sportsbook pages to extract data
3. **Oddsshopper API** - Uses aggregated odds data as fallback
4. **Website Scraping** - Advanced scraping with Selenium/Playwright (future implementation)

## Troubleshooting

### Common Issues

1. **No Data Returned**
   - Check if WNBA games are available
   - Verify API endpoints are accessible
   - Check network connectivity

2. **API Rate Limiting**
   - The script includes 0.5-second delays between requests
   - Increase delay if needed: `time.sleep(1.0)`

3. **Import Errors**
   - Ensure all dependencies are installed
   - Check file paths and module structure

### Debug Mode

Enable debug output by modifying the print statements or adding logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Integration

### With Main Pipeline

Add to your main WNBA data pipeline:

```python
import sportbooks_pull as sb

# Pull Caesars data
caesars_props = sb.pull_caesars()

# Process with other data sources
if not caesars_props.empty:
    # Combine with DraftKings, Underdog, etc.
    all_props = pd.concat([dk_props, ud_props, caesars_props])
```

### Data Export

The script automatically saves data to CSV:

```python
from caesars_pull import save_caesars_data

# Save with custom filename
save_caesars_data(caesars_data, 'my_caesars_data.csv')
```

## Future Enhancements

- [ ] Direct Caesars API integration
- [ ] Website scraping with Selenium/Playwright
- [ ] Real-time odds updates
- [ ] Historical data retrieval
- [ ] Multiple state support
- [ ] Database integration

## Support

For issues or questions:
1. Check the error messages in the console output
2. Verify your network can access the APIs
3. Ensure all dependencies are properly installed
4. Check if the APIs have changed their structure

## License

This script is part of the WNBA data pipeline project.
