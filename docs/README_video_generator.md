# WNBA Social Media Video Generator

This tool creates engaging social media videos from your dbt model data, featuring WNBA players and their PrizePicks projections.

## Features

- **Local Player Images**: Uses player images from the `pics/` folder with naming convention `playerid_playername.png`
- **PrizePicks Integration**: Pulls data from your `prizepicksrec` dbt model
- **Social Media Optimized**: Creates videos in Instagram Story format (1080x1920)
- **Professional Design**: Includes player images, stats, projections, and game information
- **Automated Generation**: Creates complete videos with intro, player slides, and outro

## Requirements

- Python 3.7+
- Required packages (see `requirements.txt`)
- `players.csv` file with player data
- `pics/` folder with player images
- Access to your dbt database

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Ensure you have:
   - `players.csv` file in the root directory
   - `pics/` folder with player images named as `playerid_playername.png`
   - Database connection configured in `db_conn.py`

## Usage

### Quick Start

Run the video generator:
```bash
python run_video_generator.py
```

This will:
1. Query your `prizepicksrec` dbt model for the top 3 picks
2. Load player images from the `pics/` folder
3. Create a professional social media video
4. Save it as `wnba_prizepicks_YYYYMMDD_HHMMSS.mp4`

### Testing

Test the components before generating a full video:
```bash
python test_video_generator.py
```

This will verify:
- Player image loading
- Database connectivity
- Data retrieval

### Custom Usage

You can also use the classes directly in your own scripts:

```python
from wnba_social_video_generator import SocialMediaVideoGenerator

# Create generator
generator = SocialMediaVideoGenerator()

# Generate video with custom filename
output_path = generator.generate_video("my_custom_video.mp4")
```

## File Structure

```
├── wnba_social_video_generator.py  # Main video generator
├── run_video_generator.py          # Script to run the generator
├── test_video_generator.py         # Test script
├── players.csv                     # Player database
├── pics/                           # Player images folder
│   ├── 1627668_Breanna Stewart.png
│   ├── 1628932_A'ja Wilson.png
│   └── ...
└── requirements.txt                # Python dependencies
```

## Video Format

The generated video includes:

1. **Intro Slide** (2 seconds)
   - "WNBA PrizePicks" title
   - "Top Picks Today" subtitle

2. **Player Slides** (4 seconds each)
   - Circular player image
   - Player name
   - Stat type and line score
   - Projection
   - Team vs opponent

3. **Outro Slide** (2 seconds)
   - Call to action
   - Branding

## Player Image Requirements

Player images should be:
- PNG format
- Named as `playerid_playername.png`
- Stored in the `pics/` folder
- Ideally square aspect ratio (will be cropped to circle)

Example filenames:
- `1627668_Breanna Stewart.png`
- `1628932_A'ja Wilson.png`
- `1642288_Caitlin Clark.png`

## Database Schema

The tool expects your `prizepicksrec` dbt model to have these columns:
- `player_name`: Player's full name
- `team`: Player's team
- `stat_type`: Type of stat (e.g., "Points", "Rebounds")
- `line_score`: The line/over-under
- `projection`: Projected score
- `confidence`: Confidence rating
- `game_time`: Game time
- `opponent`: Opposing team

## Troubleshooting

### Common Issues

1. **Player images not found**
   - Check that images are named correctly: `playerid_playername.png`
   - Verify player names match exactly in `players.csv`

2. **Database connection errors**
   - Ensure `db_conn.py` is configured correctly
   - Check that your dbt model `prizepicksrec` exists

3. **Missing dependencies**
   - Run `pip install -r requirements.txt`
   - Ensure you have ffmpeg installed for video processing

### Logs

The tool provides detailed logging. Check the console output for:
- Player image loading status
- Database query results
- Video generation progress

## Customization

You can customize the video by modifying:

- **Colors**: Edit the RGB values in the slide creation methods
- **Fonts**: Change font files and sizes
- **Duration**: Adjust slide timing
- **Layout**: Modify image positioning and text placement
- **Content**: Add or remove information displayed

## Output

The generated video will be:
- Format: MP4
- Resolution: 1080x1920 (Instagram Story)
- FPS: 24
- Codec: H.264
- Ready for social media upload

## Support

For issues or questions:
1. Check the troubleshooting section
2. Run the test script to identify problems
3. Review the logs for error messages 