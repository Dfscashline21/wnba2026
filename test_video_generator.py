#!/usr/bin/env python3
"""
Test script for WNBA Social Media Video Generator
"""

import os
import sys
from wnba_social_video_generator import PlayerImageManager, SocialMediaVideoGenerator

def test_player_image_manager():
    """Test the PlayerImageManager class with actual PrizePicks data"""
    print("Testing PlayerImageManager with PrizePicks data...")
    
    manager = PlayerImageManager()
    
    # Get actual players from PrizePicks data
    try:
        generator = SocialMediaVideoGenerator()
        prizepicks_data = generator.get_prizepicks_data(limit=10)  # Get 10 players for testing
        
        if prizepicks_data.empty:
            print("No PrizePicks data found!")
            return
        
        print(f"Found {len(prizepicks_data)} players in PrizePicks data")
        print("\nTesting player images:")
        
        for idx, row in prizepicks_data.iterrows():
            player_name = row['player_name']
            print(f"\n{idx + 1}. Testing player: {player_name}")
            
            # Test image path
            image_path = manager.get_player_image_path(player_name)
            if image_path:
                print(f"   ✓ Found image: {image_path}")
            else:
                print(f"   ✗ No image found for {player_name}")
            
            # Test image loading
            try:
                img = manager.get_player_image(player_name)
                if img:
                    print(f"   ✓ Successfully loaded image ({img.size[0]}x{img.size[1]})")
                else:
                    print(f"   ✗ Failed to load image")
            except Exception as e:
                print(f"   ✗ Error loading image: {e}")
            
            # Only test first 5 players to avoid too much output
            if idx >= 4:
                break
                
    except Exception as e:
        print(f"Error getting PrizePicks data: {e}")
        return

def test_video_generator():
    """Test the video generator with actual data"""
    print("\n" + "="*50)
    print("Testing Video Generator with PrizePicks data...")
    
    try:
        generator = SocialMediaVideoGenerator()
        
        # Test getting PrizePicks data
        print("1. Testing PrizePicks data retrieval...")
        data = generator.get_prizepicks_data(limit=5)
        if not data.empty:
            print(f"   ✓ Retrieved {len(data)} players from PrizePicks data")
            print("   Sample data:")
            for idx, row in data.head(3).iterrows():
                print(f"      {row['player_name']} - {row['stat']} {row['line']}")
        else:
            print("   ✗ No PrizePicks data found")
            return
        
        # Test getting unique players
        print("\n2. Testing unique player selection...")
        unique_players = generator.get_unique_players(3)
        if unique_players:
            print(f"   ✓ Selected {len(unique_players)} unique players:")
            for player in unique_players:
                print(f"      - {player}")
        else:
            print("   ✗ No unique players found")
            return
        
        # Test image loading for selected players
        print("\n3. Testing image loading for selected players...")
        for player in unique_players:
            image_path = generator.image_manager.get_player_image_path(player)
            if image_path:
                print(f"   ✓ {player}: {image_path}")
            else:
                print(f"   ✗ {player}: No image found")
        
        print("\n4. Testing video generation...")
        # Generate a test video
        output_filename = "test_video.mp4"
        output_path = generator.generate_video(output_filename)
        
        if output_path and os.path.exists(output_path):
            print(f"   ✓ Video generated successfully: {output_path}")
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            print(f"   File size: {file_size:.2f} MB")
        else:
            print("   ✗ Video generation failed")
            
    except Exception as e:
        print(f"Error in video generator test: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main test function"""
    print("WNBA Social Media Video Generator - Test Suite")
    print("=" * 60)
    
    # Test player image manager
    test_player_image_manager()
    
    # Test video generator
    test_video_generator()
    
    print("\n" + "="*60)
    print("Test completed!")

if __name__ == "__main__":
    main() 