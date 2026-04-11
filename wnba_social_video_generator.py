#!/usr/bin/env python3
"""
WNBA Social Media Video Generator
Creates engaging social media videos from dbt model data with local player images and background music
"""

import requests
import pandas as pd
import os
import time
import json
import logging
from PIL import Image, ImageDraw, ImageFont
import moviepy
import moviepy.audio
import numpy as np
from bs4 import BeautifulSoup
import re
from urllib.parse import quote
import textwrap
from supabase_db_conn import get_db_engine
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlayerImageManager:
    """Player image manager using local images from pics folder"""
    
    def __init__(self):
        self.pics_dir = "pics"
        self.players_df = None
        self.load_players_data()
    
    def load_players_data(self):
        """Load player data from players.csv"""
        try:
            self.players_df = pd.read_csv("players.csv")
            logger.info(f"Loaded {len(self.players_df)} players from players.csv")
        except Exception as e:
            logger.error(f"Error loading players.csv: {e}")
            self.players_df = pd.DataFrame()
    
    def get_player_image_path(self, player_name):
        """Get the local image path for a player"""
        if self.players_df.empty:
            return None
        
        # Find player in the dataframe
        player_row = self.players_df[self.players_df['Name'].str.contains(player_name, case=False, na=False)]
        
        if player_row.empty:
            # Try with ShortName
            player_row = self.players_df[self.players_df['ShortName'].str.contains(player_name, case=False, na=False)]
        
        if player_row.empty:
            logger.warning(f"Player not found in database: {player_name}")
            return None
        
        # Get player ID and name
        player_id = player_row.iloc[0]['EntityId']
        player_name_clean = player_row.iloc[0]['Name']
        
        # Create the expected filename
        filename = f"{player_id}_{player_name_clean}.png"
        image_path = os.path.join(self.pics_dir, filename)
        
        if os.path.exists(image_path):
            logger.info(f"Found image for {player_name}: {image_path}")
            return image_path
        else:
            logger.warning(f"Image not found: {image_path}")
            return None
    
    def get_player_image(self, player_name, size=(400, 400)):
        """Get player image as PIL Image object"""
        image_path = self.get_player_image_path(player_name)
        
        if image_path:
            try:
                img = Image.open(image_path)
                img = img.convert('RGBA')
                img = img.resize(size, Image.Resampling.LANCZOS)
                return img
            except Exception as e:
                logger.error(f"Error loading image {image_path}: {e}")
        
        # Return a placeholder if image not found
        return self.create_placeholder_image(player_name, size)
    
    def create_placeholder_image(self, player_name, size=(400, 400)):
        """Create a placeholder image when player image is not found"""
        img = Image.new('RGBA', size, (50, 50, 50, 255))
        draw = ImageDraw.Draw(img)
        
        # Try to load a font, fall back to default if not available
        try:
            font = ImageFont.truetype("arial.ttf", 24)
        except:
            font = ImageFont.load_default()
        
        # Draw player name
        text = player_name[:20] if len(player_name) > 20 else player_name
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        x = (size[0] - text_width) // 2
        y = (size[1] - text_height) // 2
        
        draw.text((x, y), text, fill=(255, 255, 255, 255), font=font)
        
        return img

class SocialMediaVideoGenerator:
    """Generate social media videos from dbt model data"""
    
    def __init__(self):
        self.image_manager = PlayerImageManager()
        self.db_engine = get_db_engine()
    
    def get_prizepicks_data(self, limit=3):
        """Get PrizePicks data from dbt model"""
        query = """
        SELECT 
            player_name,
            odds_type,
            stat,
            line,
            over_value,
            bettype,
            over_value5,
            over_value10
        FROM wnba.prizepicksrec 
        ORDER BY over_value DESC 
        LIMIT %s
        """
        
        try:
            df = pd.read_sql(query, self.db_engine, params=(limit,))
            logger.info(f"Retrieved {len(df)} PrizePicks records")
            return df
        except Exception as e:
            logger.error(f"Error querying PrizePicks data: {e}")
            return pd.DataFrame()
    
    def get_unique_players(self, num_players=3):
        """Get unique players from PrizePicks data"""
        try:
            # Get more data than needed to ensure we have enough unique players
            data = self.get_prizepicks_data(limit=num_players * 2)
            
            if data.empty:
                logger.warning("No PrizePicks data available")
                return []
            
            # Get unique player names
            unique_players = data['player_name'].unique()
            
            # Return the requested number of players
            selected_players = unique_players[:num_players].tolist()
            
            logger.info(f"Selected {len(selected_players)} unique players: {selected_players}")
            return selected_players
            
        except Exception as e:
            logger.error(f"Error getting unique players: {e}")
            return []
    
    def load_background_music(self):
        """Load background music from music.mp3 file"""
        music_path = "music.mp3"
        
        if not os.path.exists(music_path):
            logger.warning(f"Background music file not found: {music_path}")
            return None
        
        try:
            music_clip = moviepy.AudioFileClip(music_path)
            logger.info(f"Loaded background music: {music_path} (duration: {music_clip.duration:.2f}s)")
            return music_clip
        except Exception as e:
            logger.error(f"Error loading background music: {e}")
            return None
    
    def create_player_slide(self, player_data, duration=4):
        """Create a video slide for a single player"""
        # Get player image
        player_img = self.image_manager.get_player_image(player_data['player_name'])
        
        # Create background
        width, height = 1080, 1920  # Instagram Story dimensions
        bg = Image.new('RGBA', (width, height), (20, 20, 40, 255))
        
        # Add gradient overlay
        gradient = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(gradient)
        for y in range(height):
            alpha = int(100 * (1 - y / height))
            draw.line([(0, y), (width, y)], fill=(0, 0, 0, alpha))
        
        bg = Image.alpha_composite(bg, gradient)
        
        # Add player image (centered, top half)
        img_size = (400, 400)
        player_img_resized = player_img.resize(img_size, Image.Resampling.LANCZOS)
        
        # Create circular mask for player image
        mask = Image.new('L', img_size, 0)
        mask_draw = ImageDraw.Draw(mask)
        mask_draw.ellipse((0, 0, img_size[0], img_size[1]), fill=255)
        
        # Apply mask to player image
        output = Image.new('RGBA', img_size, (0, 0, 0, 0))
        output.paste(player_img_resized, (0, 0))
        output.putalpha(mask)
        
        # Position player image
        img_x = (width - img_size[0]) // 2
        img_y = 200
        bg.paste(output, (img_x, img_y), output)
        
        # Add text
        draw = ImageDraw.Draw(bg)
        
        # Try to load fonts
        try:
            title_font = ImageFont.truetype("arial.ttf", 48)
            subtitle_font = ImageFont.truetype("arial.ttf", 32)
            body_font = ImageFont.truetype("arial.ttf", 24)
        except:
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
            body_font = ImageFont.load_default()
        
        # Player name
        player_name = player_data['player_name']
        bbox = draw.textbbox((0, 0), player_name, font=title_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y = img_y + img_size[1] + 50
        draw.text((x, y), player_name, fill=(255, 255, 255, 255), font=title_font)
        
        # Stat line
        stat_text = f"{player_data['stat']}: {player_data['line']}"
        bbox = draw.textbbox((0, 0), stat_text, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 80
        draw.text((x, y), stat_text, fill=(255, 215, 0, 255), font=subtitle_font)
        
        # Last 5 games
        last5_text = f"Last 5 Games: {player_data['over_value5'] * 100:.0f}%"
        bbox = draw.textbbox((0, 0), last5_text, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 60
        draw.text((x, y), last5_text, fill=(0, 255, 0, 255), font=subtitle_font)
        
        # Last 10 games
        last10_text = f"Last 10 Games: {player_data['over_value10'] * 100:.0f}%"
        bbox = draw.textbbox((0, 0), last10_text, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 60
        draw.text((x, y), last10_text, fill=(0, 255, 0, 255), font=subtitle_font)
        
        # Bet type
        bet_text = f"Bet Type: {player_data['bettype']}"
        bbox = draw.textbbox((0, 0), bet_text, font=body_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 80
        draw.text((x, y), bet_text, fill=(200, 200, 200, 255), font=body_font)
        
        # Convert PIL image to numpy array for moviepy
        img_array = np.array(bg)
        
        # Create video clip
        clip = moviepy.ImageClip(img_array, duration=duration)
        
        return clip
    
    def create_intro_slide(self, duration=2):
        """Create intro slide with WNBA logo, FastBreak Fantasy Hoops logo, and date"""
        width, height = 1080, 1920
        
        # Create background
        bg = Image.new('RGBA', (width, height), (20, 20, 40, 255))
        draw = ImageDraw.Draw(bg)
        
        # Try to load WNBA logo
        try:
            wnba_logo_path = os.path.join("pics", "wnba.webp")
            if os.path.exists(wnba_logo_path):
                wnba_logo = Image.open(wnba_logo_path)
                wnba_logo = wnba_logo.convert('RGBA')
                
                # Resize WNBA logo to appropriate size
                wnba_logo_size = (250, 150)  # Smaller size for top
                wnba_logo = wnba_logo.resize(wnba_logo_size, Image.Resampling.LANCZOS)
                
                # Position WNBA logo at top left
                wnba_logo_x = 50
                wnba_logo_y = 100
                bg.paste(wnba_logo, (wnba_logo_x, wnba_logo_y), wnba_logo)
                
                logger.info("WNBA logo added to intro slide")
            else:
                logger.warning("WNBA logo not found at pics/wnba.webp")
        except Exception as e:
            logger.error(f"Error loading WNBA logo: {e}")
        
        # Try to load FastBreak Fantasy Hoops logo
        try:
            fbfh_logo_path = os.path.join("pics", "Fast Break Fantasy Hoops.jpg")
            if os.path.exists(fbfh_logo_path):
                fbfh_logo = Image.open(fbfh_logo_path)
                fbfh_logo = fbfh_logo.convert('RGBA')
                
                # Resize FastBreak Fantasy Hoops logo
                fbfh_logo_size = (300, 200)  # Larger size for main branding
                fbfh_logo = fbfh_logo.resize(fbfh_logo_size, Image.Resampling.LANCZOS)
                
                # Position FastBreak Fantasy Hoops logo at top right
                fbfh_logo_x = width - fbfh_logo_size[0] - 50
                fbfh_logo_y = 100
                bg.paste(fbfh_logo, (fbfh_logo_x, fbfh_logo_y), fbfh_logo)
                
                logger.info("FastBreak Fantasy Hoops logo added to intro slide")
            else:
                logger.warning("FastBreak Fantasy Hoops logo not found at pics/Fast Break Fantasy Hoops.jpg")
        except Exception as e:
            logger.error(f"Error loading FastBreak Fantasy Hoops logo: {e}")
        
        # Try to load font
        try:
            title_font = ImageFont.truetype("arial.ttf", 72)
            subtitle_font = ImageFont.truetype("arial.ttf", 36)
            date_font = ImageFont.truetype("arial.ttf", 28)
        except:
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
            date_font = ImageFont.load_default()
        
        # Get current date
        current_date = datetime.now().strftime("%B %d, %Y")
        
        # Title
        title = "PrizePicks"
        bbox = draw.textbbox((0, 0), title, font=title_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y = height // 2 - 50
        draw.text((x, y), title, fill=(255, 255, 255, 255), font=title_font)
        
        # Subtitle
        subtitle = "Top Picks Today"
        bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 100
        draw.text((x, y), subtitle, fill=(255, 215, 0, 255), font=subtitle_font)
        
        # Date
        bbox = draw.textbbox((0, 0), current_date, font=date_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 80
        draw.text((x, y), current_date, fill=(200, 200, 200, 255), font=date_font)
        
        img_array = np.array(bg)
        clip = moviepy.ImageClip(img_array, duration=duration)
        
        return clip
    
    def create_outro_slide(self, duration=2):
        """Create outro slide with FastBreak Fantasy Hoops branding"""
        width, height = 1080, 1920
        
        bg = Image.new('RGBA', (width, height), (20, 20, 40, 255))
        draw = ImageDraw.Draw(bg)
        
        # Try to load FastBreak Fantasy Hoops logo
        try:
            fbfh_logo_path = os.path.join("pics", "Fast Break Fantasy Hoops.jpg")
            if os.path.exists(fbfh_logo_path):
                fbfh_logo = Image.open(fbfh_logo_path)
                fbfh_logo = fbfh_logo.convert('RGBA')
                
                # Resize FastBreak Fantasy Hoops logo for center placement
                fbfh_logo_size = (400, 250)
                fbfh_logo = fbfh_logo.resize(fbfh_logo_size, Image.Resampling.LANCZOS)
                
                # Position FastBreak Fantasy Hoops logo at center
                fbfh_logo_x = (width - fbfh_logo_size[0]) // 2
                fbfh_logo_y = height // 2 - 200
                bg.paste(fbfh_logo, (fbfh_logo_x, fbfh_logo_y), fbfh_logo)
                
                logger.info("FastBreak Fantasy Hoops logo added to outro slide")
            else:
                logger.warning("FastBreak Fantasy Hoops logo not found at pics/Fast Break Fantasy Hoops.jpg")
        except Exception as e:
            logger.error(f"Error loading FastBreak Fantasy Hoops logo: {e}")
        
        try:
            title_font = ImageFont.truetype("arial.ttf", 48)
            subtitle_font = ImageFont.truetype("arial.ttf", 24)
        except:
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
        
        # Title
        title = "Follow for More Picks!"
        bbox = draw.textbbox((0, 0), title, font=title_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y = height // 2 + 100
        draw.text((x, y), title, fill=(255, 255, 255, 255), font=title_font)
        
        # Subtitle
        subtitle = "Daily WNBA PrizePicks Analysis"
        bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 80
        draw.text((x, y), subtitle, fill=(200, 200, 200, 255), font=subtitle_font)
        
        # FastBreak Fantasy Hoops branding
        branding = "@FastBreakFantasyHoops"
        bbox = draw.textbbox((0, 0), branding, font=subtitle_font)
        text_width = bbox[2] - bbox[0]
        x = (width - text_width) // 2
        y += 60
        draw.text((x, y), branding, fill=(255, 215, 0, 255), font=subtitle_font)
        
        img_array = np.array(bg)
        clip = moviepy.ImageClip(img_array, duration=duration)
        
        return clip
    
    def generate_video(self, output_path=None):
        """Generate the complete social media video with background music"""
        logger.info("Starting video generation...")
        
        # Generate filename with date if not provided
        if output_path is None:
            current_date = datetime.now().strftime("%Y-%m-%d")
            output_path = f"wnba_prizepicks_{current_date}.mp4"
        
        # Get PrizePicks data
        picks_data = self.get_prizepicks_data(3)
        
        if picks_data.empty:
            logger.error("No PrizePicks data available")
            return None
        
        # Create video clips
        clips = []
        
        # Intro slide
        intro_clip = self.create_intro_slide(2)
        clips.append(intro_clip)
        
        # Player slides
        for idx, (_, player_data) in enumerate(picks_data.iterrows(), 1):
            player_clip = self.create_player_slide(player_data, 4)
            clips.append(player_clip)
        
        # Outro slide
        outro_clip = self.create_outro_slide(2)
        clips.append(outro_clip)
        
        # Concatenate all clips
        final_video = moviepy.concatenate_videoclips(clips, method="compose")
        
        # Add background music
        background_music = self.load_background_music()
        if background_music:
            # Loop music if it's shorter than the video
            video_duration = final_video.duration
            music_duration = background_music.duration
            
            if music_duration < video_duration:
                # Calculate how many times to loop the music
                loops_needed = int(video_duration / music_duration) + 1
                music_clips = [background_music] * loops_needed
                looped_music = moviepy.concatenate_audioclips(music_clips)
                # Trim to exact video duration
                background_music = looped_music.subclipped(0, video_duration)
            else:
                # Trim music to video duration
                background_music = background_music.subclipped(0, video_duration)
            
            # Set the background music volume (adjust as needed)
            # background_music = background_music.volume(0.3)  # 30% volume - commented out due to AttributeError
            
            # Add background music to video
            final_video = final_video.with_audio(background_music)
            logger.info("Background music added to video")
        else:
            logger.warning("No background music available")
        
        # Write video file
        logger.info(f"Writing video to {output_path}")
        final_video.write_videofile(
            output_path,
            fps=24,
            codec='libx264',
            audio_codec='aac',
            temp_audiofile='temp-audio.m4a',
            remove_temp=True
        )
        
        logger.info("Video generation complete!")
        return output_path

def main():
    """Main function to generate the video"""
    generator = SocialMediaVideoGenerator()
    output_file = generator.generate_video()  # Will use date-based filename
    
    if output_file:
        print(f"Video created successfully: {output_file}")
    else:
        print("Failed to create video")

if __name__ == "__main__":
    main() 