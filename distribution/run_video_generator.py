#!/usr/bin/env python3
"""
Run the WNBA Social Media Video Generator
"""

import os
import sys
from datetime import datetime
from .wnba_social_video_generator import SocialMediaVideoGenerator

def main():
    """Main function to run the video generator"""
    print("WNBA Social Media Video Generator")
    print("=" * 40)
    
    # Create timestamp for unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"wnba_prizepicks_{timestamp}.mp4"
    
    try:
        # Initialize the video generator
        print("Initializing video generator...")
        generator = SocialMediaVideoGenerator()
        
        # Generate the video
        print("Generating video...")
        output_path = generator.generate_video(output_filename)
        
        if output_path and os.path.exists(output_path):
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # Convert to MB
            print(f"✓ Video created successfully!")
            print(f"  File: {output_path}")
            print(f"  Size: {file_size:.2f} MB")
            print(f"  Ready for social media upload!")
        else:
            print("✗ Failed to create video")
            return False
            
    except Exception as e:
        print(f"✗ Error generating video: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Video generation completed successfully!")
    else:
        print("\n❌ Video generation failed. Check the logs above for details.")
        sys.exit(1) 