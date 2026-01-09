import json
import os
import subprocess
import io
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List

import modal

# Modal app
app = modal.App("clip-video")

# Image with FFmpeg and required libraries
image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("ffmpeg")
    .pip_install(
        "fastapi",
        "google-genai", # Updated to the latest unified SDK
        "requests",
    )
)

# In-memory job storage for placeholder (not production-ready)
job_store: Dict[str, Dict[str, Any]] = {}

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("kaiber-secrets")],
    timeout=3600,
)
def process_video_with_gemini(url: str) -> List[Dict[str, Any]]:
    from google import genai
    from google.genai import types

    # Configure Gemini Client
    api_key = os.environ.get("GOOGLE_GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_GEMINI_API_KEY not found in environment")
    
    client = genai.Client(api_key=api_key)
    
    print(f"Processing video URL: {url}")
    
    # FIX 1 & 2: Fragmented MP4 + 1 FPS Downsampling
    # This creates a "streamable" file that is tiny but retains all visual info for AI
    ffmpeg_cmd = [
        "ffmpeg",
        "-i", url,
        "-vf", "scale=1280:-2,fps=1", # 720p at 1 frame per second
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "30",
        "-an", # No audio needed for scene detection (saves space)
        "-f", "mp4",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof", # Fragmented MP4 for piping
        "pipe:1",
    ]
    
    try:
        print("Streaming video through FFmpeg pipe...")
        process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # FIX 3: Direct Memory Pipe
        # We read the stream into a buffer. Because of 1 FPS, a 30m video is only ~20-40MB.
        stdout_data, stderr_data = process.communicate()
        
        if process.returncode != 0:
            print(f"FFmpeg Error: {stderr_data.decode()}")
            raise RuntimeError("FFmpeg pipe failed")

        print(f"Transcode complete. Buffer size: {len(stdout_data)} bytes")

        # Upload to Gemini File API using the bytes buffer
        video_file = client.files.upload(
            file=io.BytesIO(stdout_data),
            config=types.UploadFileConfig(
                mime_type="video/mp4",
                display_name="video_analysis_stream.mp4"
            )
        )
        
        # Step 3: Poll until file is ACTIVE
        print(f"Polling file {video_file.name}...")
        while True:
            file_info = client.files.get(name=video_file.name)
            if file_info.state.name == "ACTIVE":
                break
            if file_info.state.name == "FAILED":
                raise RuntimeError("Gemini file processing failed")
            time.sleep(2)
        
        # FIX 4: Use Gemini 2.5 Flash-Lite
        print("Analyzing with gemini-2.5-flash-lite...")
        prompt = "Identify distinct scenes. Return ONLY a JSON array of objects: [{'start_time': sec, 'end_time': sec, 'title': str}]"
        
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=[file_info, prompt]
        )
        
        # Cleanup: Remove markdown backticks if Gemini adds them
        clean_json = response.text.replace("```json", "").replace("```", "").strip()
        timestamps = json.loads(clean_json)
        
        print(f"Successfully identified {len(timestamps)} scenes.")
        
        # Cleanup Gemini Storage
        client.files.delete(name=video_file.name)
        
        return timestamps

    except Exception as e:
        print(f"Error in Track A: {e}")
        raise

# FastAPI endpoints remain similar, but call .spawn() on the new optimized function
@app.function(image=image)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI
    from pydantic import BaseModel
    web_app = FastAPI()

    class StartRequest(BaseModel):
        url: str

    @web_app.post("/start")
    async def start_job(data: StartRequest):
        job_id = str(uuid.uuid4())
        # Track A: Kick off AI analysis
        process_video_with_gemini.spawn(data.url)
        # Note: You should also kick off Track B (High-res download) here in parallel
        return {"job_id": job_id, "status": "started"}

    return web_app