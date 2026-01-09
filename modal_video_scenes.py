import json
import os
import subprocess
import io
import time
import uuid
import asyncio
from datetime import datetime
from typing import Dict, Any, List

import modal

# Import environment variables
try:
    from env_vars import (
        R2_ACCESS_KEY_ID,
        R2_SECRET_ACCESS_KEY,
        R2_ENDPOINT_URL,
        R2_BUCKET_NAME,
        GOOGLE_GEMINI_API_KEY,
    )
except ImportError:
    # Fallback for local execution or if env_vars is not yet available
    R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
    R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL", "")
    R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "")
    GOOGLE_GEMINI_API_KEY = os.environ.get("GOOGLE_GEMINI_API_KEY", "")

# 1. SHARED STORAGE & APP CONFIG
# Volume for shared high-res videos between coordinator and parallel workers
video_storage = modal.Volume.from_name("high-res-videos", create_if_missing=True)
app = modal.App("clip-video")

# Image containing all required binaries and libraries
image = (
    modal.Image.debian_slim(python_version="3.10")
    .apt_install("ffmpeg", "git", "nodejs", "npm")  # nodejs/npm required for dotenv-vault CLI
    .pip_install(
        "fastapi", 
        "google-genai", 
        "requests", 
        "yt-dlp", 
        "boto3",
        "python-dotenv",
    )
    .run_commands("pip uninstall -y bson || true")
    .add_local_file("env_vars.py", "/root/env_vars.py", copy=True)
    .add_local_file(".env.vault", "/root/.env.vault", copy=True)
)

# Install kaiber-utils from repo
image = image.run_commands(
    "pip install git+https://$GITHUB_TOKEN@github.com/KaiberAI/kaiber-utils.git",
    secrets=[modal.Secret.from_name("kaiber-secrets")],
)

# 2. FAN-OUT WORKER: Cut and Upload to R2
@app.function(
    image=image, 
    volumes={"/videos": video_storage},
    secrets=[modal.Secret.from_name("kaiber-secrets")], # Contains Gemini & R2 keys
    cpu=1.0 
)
def cut_and_upload_clip(scene: Dict[str, Any], input_path: str) -> Dict[str, Any]:
    """Runs in parallel for every scene: Clips via FFmpeg and uploads to R2."""
    import subprocess
    import boto3
    from botocore.config import Config

    clip_id = f"{uuid.uuid4().hex[:10]}"
    clip_filename = f"clip_{clip_id}.mp4"
    local_output_path = f"/videos/{clip_filename}"
    
    # Fast Seek (-ss before -i) + Stream Copy (-c copy)
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(scene["start_time"]), 
        "-i", input_path,
        "-to", str(scene["end_time"]),
        "-c", "copy",
        "-map_metadata", "0",
        "-movflags", "+faststart",
        local_output_path
    ]
    
    subprocess.run(cmd, check=True)

    # Initialize R2 Client (S3 Compatible)
    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto" 
    )

    # Upload to R2
    s3.upload_file(local_output_path, R2_BUCKET_NAME, clip_filename)

    # Generate Presigned URL (Valid for 24 hours)
    presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": R2_BUCKET_NAME, "Key": clip_filename},
        ExpiresIn=86400 
    )

    # Cleanup Volume storage
    if os.path.exists(local_output_path):
        os.remove(local_output_path)

    return {
        **scene,
        "clip_url": presigned_url,
        "filename": clip_filename
    }

# 3. TRACK B HELPER: High-Res Download
async def download_high_res_video(url: str, output_path: str):
    import yt_dlp
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': output_path,
        'quiet': True,
    }
    def download():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        video_storage.commit()
    
    await asyncio.to_thread(download)

# 4. COORDINATOR: Track A (Gemini) + Parallel Fan-out Orchestration
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("kaiber-secrets")],
    volumes={"/videos": video_storage},
    timeout=3600,
)
async def process_video_with_gemini(url: str) -> List[Dict[str, Any]]:
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=GOOGLE_GEMINI_API_KEY)
    video_id = str(uuid.uuid4())
    high_res_path = f"/videos/{video_id}.mp4"

    async def perform_gemini_analysis():
        from google.genai import types
        
        # 1. FFmpeg 1 FPS Stream logic
        ffmpeg_cmd = [
            "ffmpeg", "-i", url,
            "-vf", "scale=1280:-2,fps=1", 
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
            "-an", "-f", "mp4",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "pipe:1"
        ]
        
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout_data, _ = await process.communicate()
        
        video_file = client.files.upload(
            file=io.BytesIO(stdout_data),
            config=types.UploadFileConfig(mime_type="video/mp4")
        )
        
        while True:
            file_info = client.files.get(name=video_file.name)
            if file_info.state.name == "ACTIVE": break
            await asyncio.sleep(2)
        
        # 2. Define the Schema for Structured Output
        # This mathematically guarantees valid JSON and correct key names
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "scenes": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "start_time": {"type": "NUMBER"},
                            "end_time": {"type": "NUMBER"},
                            "title": {"type": "STRING"},
                        },
                        "required": ["start_time", "end_time", "title"]
                    }
                }
            },
            "required": ["scenes"]
        }

        prompt = "Analyze the video and identify distinct scenes with timestamps."
        
        # 3. Requesting the content with Structured Config
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=[file_info, prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_schema,
                temperature=0.1, # Lower temperature = higher consistency
                safety_settings=[
                    types.SafetySetting(
                        category="HARM_CATEGORY_HARASSMENT",
                        threshold="BLOCK_NONE",
                    ),
                    types.SafetySetting(
                        category="HARM_CATEGORY_HATE_SPEECH",
                        threshold="BLOCK_NONE",
                    ),
                    types.SafetySetting(
                        category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                        threshold="BLOCK_NONE",
                    ),
                    types.SafetySetting(
                        category="HARM_CATEGORY_DANGEROUS_CONTENT",
                        threshold="BLOCK_NONE",
                    ),  
                ]
            )
        )
        
        # 4. Cleanup and Parsing
        try:
            # Check for parsed object first
            if hasattr(response, 'parsed') and response.parsed is not None:
                # If we wrapped it in 'scenes', we extract it here
                raw_data = response.parsed
                # Handle both dict and object-style access
                timestamps = raw_data.get("scenes", []) if isinstance(raw_data, dict) else getattr(raw_data, "scenes", [])
            else:
                # Fallback to text with a default empty object
                data = json.loads(response.text or '{"scenes": []}')
                timestamps = data.get("scenes", [])
        except Exception as e:
            print(f"JSON Bug caught: {e}. Raw: {response.text}")
            timestamps = []
        finally:
            client.files.delete(name=video_file.name)
            
        return timestamps

    try:
        # EXECUTE TRACKS A & B IN PARALLEL
        # This is where we save the 60-90 seconds of download latency
        _, timestamps = await asyncio.gather(
            download_high_res_video(url, high_res_path),
            perform_gemini_analysis()
        )
        
        # 5. TRIGGER FAN-OUT
        print(f"Analysis complete. Clipping {len(timestamps)} scenes in parallel...")
        # map() handles massive scale automatically
        final_clips = []
        async for clip in cut_and_upload_clip.map.aio(timestamps, kwargs={"input_path": high_res_path}):
            final_clips.append(clip)
        
        # Cleanup original high-res video from Volume
        if os.path.exists(high_res_path):
            os.remove(high_res_path)
            video_storage.commit()

        return final_clips

    except Exception as e:
        print(f"Pipeline Error: {e}")
        raise

# 6. WEB INTERFACE
@app.function(image=image)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI
    from pydantic import BaseModel
    web_app = FastAPI(title="AI Video Clipping API")

    class StartRequest(BaseModel):
        url: str

    @web_app.post("/start")
    async def start_job(data: StartRequest):
        # We use .spawn() to trigger the job and return immediately
        job = process_video_with_gemini.spawn(data.url)
        return {"job_id": job.object_id, "status": "processing"}

    return web_app
    