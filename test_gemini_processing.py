#!/usr/bin/env python3
"""
Test script for the Gemini video processing function.

This script demonstrates how to call the process_video_with_gemini function directly.

Usage:
    modal run test_gemini_processing.py --video-url "https://example.com/video.mp4"
"""

import modal
from modal_video_scenes import app, process_video_with_gemini


@app.local_entrypoint()
def main(video_url: str = "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4"):
    """
    Test the Gemini video processing function with a sample video.
    
    Args:
        video_url: URL of the video to process (default: Big Buck Bunny sample)
    """
    print(f"Testing Gemini video processing with URL: {video_url}")
    print("-" * 80)
    
    # Call the function - this will run on Modal infrastructure
    result = process_video_with_gemini.remote(video_url)
    
    print("\n" + "=" * 80)
    print("FINAL RESULT:")
    print("=" * 80)
    
    import json
    print(json.dumps(result, indent=2))
    
    print("\n" + "=" * 80)
    print(f"Successfully processed video! Found {len(result)} scenes.")
    print("=" * 80)

