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

# Volume for shared high-res videos between coordinator and parallel workers
video_storage = modal.Volume.from_name("high-res-videos", create_if_missing=True)

# Persistent dictionary for tracking cross-container job status
progress_tracker = modal.Dict.from_name("clipping-progress", create_if_missing=True)

app = modal.App("clip-video")

# Image containing all required binaries and libraries
image = (
    modal.Image.debian_slim(python_version="3.10")
    .apt_install("ffmpeg", "git", "nodejs", "npm")
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

# Install kaiber-utils from github repo
image = image.run_commands(
    "pip install git+https://$GITHUB_TOKEN@github.com/KaiberAI/kaiber-utils.git",
    secrets=[modal.Secret.from_name("kaiber-secrets")],
)

def get_video_duration(path: str) -> float:
    import subprocess
    try:
        cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except Exception as e:
        print(f"Error probing video duration: {e}")
        return 0.0

# FAN-OUT WORKER: Cut and Upload to R2
@app.function(
    image=image, 
    volumes={"/videos": video_storage},
    secrets=[modal.Secret.from_name("kaiber-secrets")], # Contains Gemini & R2 keys
    cpu=1.0,
    retries=2,
    timeout=300
)
def cut_and_upload_clip(scene: Dict[str, Any], input_path: str) -> Dict[str, Any]:
    """Runs in parallel for every scene: Clips via FFmpeg and uploads to R2."""
    import subprocess
    import boto3
    import hashlib
    from botocore.config import Config
    from env_vars import config
    R2_ENDPOINT_URL = config.R2_ENDPOINT_URL
    R2_ACCESS_KEY_ID = config.R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY = config.R2_SECRET_ACCESS_KEY
    R2_BUCKET_NAME = config.R2_BUCKET_NAME

    width = scene.get("width")
    height = scene.get("height")

    # Generate unique hash for filename
    file_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    clip_filename = f"{file_hash}.mp4"
    local_output_path = f"/videos/{clip_filename}"
    key = f"user-videos/temp/{clip_filename}"
    
    # Fast Seek (-ss before -i) + Stream Copy (-c copy)
    duration = scene["end_time"] - scene["start_time"]

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(scene["start_time"]), 
        "-i", input_path,
        "-t", str(duration),
        "-c:v", "libx264",        # Use H.264 video codec
        "-preset", "ultrafast",   # Keep it fast
        "-crf", "23",             # Good balance of quality/size
        "-c:a", "aac",            # Ensure audio is compatible
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
    s3.upload_file(local_output_path, R2_BUCKET_NAME, key)

    # Generate Presigned URL (Valid for 24 hours) - UNUSED
    presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": R2_BUCKET_NAME, "Key": key},
        ExpiresIn=86400 
    )

    # Cleanup Volume storage
    if os.path.exists(local_output_path):
        os.remove(local_output_path)

    return {
        **scene,
        "url": presigned_url,
        "filename": clip_filename,
        "key": key,
        "width": width,
        "height": height,
        "length": round(duration, 2),
    }

# TRACK B HELPER: High-Res Download
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

# COORDINATOR: Track A (Gemini) + Parallel Fan-out Orchestration
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("kaiber-secrets")],
    volumes={"/videos": video_storage},
    timeout=3600,
)
async def process_video_with_gemini(url: str, width: int, height: int, target_scene_count: int) -> List[Dict[str, Any]]:
    job_id = modal.current_function_call_id()
    progress_tracker[job_id] = 0.1
    
    from google import genai
    from google.genai import types
    from env_vars import config

    client = genai.Client(api_key=config.GOOGLE_GEMINI_API_KEY)
    video_id = str(uuid.uuid4())
    high_res_path = f"/videos/{video_id}.mp4"

    async def perform_gemini_analysis(target_scene_count: int):
        from google.genai import types

        progress_tracker[job_id] = 0.2
        
        # FFmpeg 1 FPS Stream logic
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

        progress_tracker[job_id] = 0.3
        
        video_file = client.files.upload(
            file=io.BytesIO(stdout_data),
            config=types.UploadFileConfig(mime_type="video/mp4")
        )

        progress_tracker[job_id] = 0.35
        
        while True:
            file_info = client.files.get(name=video_file.name)
            if file_info.state.name == "ACTIVE": break
            await asyncio.sleep(2)

        progress_tracker[job_id] = 0.45
        
        # Define the response schema for Gemini
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "scenes": {
                    "type": "ARRAY",
                    "maxItems": 50,  # Enforce a maximum number of scenes
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "start_time": {
                                "type": "NUMBER", 
                                "description": "Start time in CUMULATIVE SECONDS (e.g., 90.0 for 1:30). Do NOT use decimal minutes."
                            },
                            "end_time": {
                                "type": "NUMBER", 
                                "description": "End time in CUMULATIVE SECONDS. Must be greater than start_time."
                            },
                        },
                        "required": ["start_time", "end_time"]
                    }
                }
            },
            "required": ["scenes"]
        }

        prompt = f"""
        ACT AS: A Technical Video Editor.
        TASK: Shot-Based Scene Detection.

        CORE OBJECTIVE:
        Partition this video into distinct visual units. Do NOT aim for a specific number of scenes or a specific duration. Let the visual changes of the video dictate the segments.

        SCENE DETECTION LOGIC (Trigger a new scene ONLY when):
        1. "Physical Cut": An instantaneous jump from one camera angle to another.
        2. "Environment Swap": A complete change in the background or setting.
        3. "B-Roll Insertion": A cut away from the main subject to supporting footage.
        4. "Visual Reset": A transition to a full-screen graphic, title card, or black frame.

        HANDLING VARIABLE LENGTHS:
        - If the camera stays on one subject for 5 minutes without cutting, return ONE 5-minute scene.
        - If there are 10 rapid cuts in 10 seconds, return 10 short scenes.
        - Respect the editor's original intent: every "hard cut" is a new boundary.

        CONSTRAINTS:
        - MINIMUM DURATION: Every scene MUST be at least 2.0 seconds long.
        - TIMESTAMP FORMAT: Total cumulative seconds (e.g., 125.5). NO decimal minutes.
        """
        
        # Gemini inference
        response = client.models.generate_content(
            model="gemini-3-pro-preview",
            contents=[file_info, prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_schema,
                temperature=0.0, # Lower temperature = higher consistency
                thinking_config=types.ThinkingConfig(thinking_level="low"),
                max_output_tokens=8192, # Maximum possible to prevent truncation
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

        progress_tracker[job_id] = 0.5
        
        # Cleanup and Parsing
        try:
            # Check for parsed object first
            if hasattr(response, 'parsed') and response.parsed is not None:
                print(f"Gemini response: {response.parsed}")
                raw_data = response.parsed
                timestamps = raw_data.get("scenes", []) if isinstance(raw_data, dict) else getattr(raw_data, "scenes", [])
            else:
                # Fallback to text parsing with better error reporting
                clean_text = response.text.strip() if response.text else '{"scenes": []}'
                data = json.loads(clean_text)
                timestamps = data.get("scenes", [])
        except Exception as e:
            print(f"JSON Parsing Error: {e}")
            print(f"Raw response text (truncated): {response.text[:1000] if response.text else 'Empty'}")
            timestamps = []
        finally:
            client.files.delete(name=video_file.name)
            
        return timestamps

    try:
        # EXECUTE TRACKS A & B IN PARALLEL
        _, timestamps = await asyncio.gather(
            download_high_res_video(url, high_res_path),
            perform_gemini_analysis(target_scene_count)
        )

        actual_duration = get_video_duration(high_res_path)

        # Filter/validate timestamps
        valid_timestamps = []
        for ts in timestamps:
            start = float(ts.get("start_time", 0))
            end = float(ts.get("end_time", 0))
            
            # Scene starts after the video actually ends (FAIL)
            if start >= actual_duration:
                print(f"Skipping ghost scene: {start} is beyond duration {actual_duration}")
                continue
                
            # Scene ends after the video ends (CLAMP)
            if end > actual_duration:
                ts["end_time"] = actual_duration
            
            # Filter clips shorter than 2 seconds
            if (float(ts["end_time"]) - float(ts["start_time"])) >= 2.0:
                valid_timestamps.append(ts)

            ts["width"] = width
            ts["height"] = height

        if not valid_timestamps:
            raise ValueError(f"No valid scenes found.")
    
        # Trigger Fan-out
        total_scenes = len(valid_timestamps)
        if total_scenes == 0:
            progress_tracker[job_id] = 1.0
            return []

        print(f"Analysis complete. Clipping {total_scenes} scenes in parallel...")
        # map() handles massive scale automatically
        final_clips = []
        completed_count = 0

        async for clip in cut_and_upload_clip.map.aio(
            valid_timestamps, 
            kwargs={"input_path": high_res_path},
            return_exceptions=True,            # Don't crash the whole job
            wrap_returned_exceptions=False     # Return the raw error
        ):
            if isinstance(clip, Exception):
                print(f"⚠️ Scene failed: {type(clip).__name__} - {clip}")
                continue

            completed_count += 1
            # Calculate granular progress for the 0.5 to 1.0 range
            percent_of_fanout = completed_count / total_scenes
            current_val = round(0.5 + (percent_of_fanout * 0.5), 2)
            progress_tracker[job_id] = current_val

            final_clips.append(clip)
        
        # Cleanup original high-res video from Volume
        if os.path.exists(high_res_path):
            os.remove(high_res_path)
            video_storage.commit()

        # Sort clips by start_time (map.aio returns in completion order)
        final_clips.sort(key=lambda x: float(x.get("start_time", 0)))

        return final_clips

    except Exception as e:
        print(f"Pipeline Error: {e}")
        raise

    finally:
        if job_id in progress_tracker:
            del progress_tracker[job_id]

# WEB INTERFACE
@app.function(image=image)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI
    from pydantic import BaseModel
    import modal.functions
    import json
    import asyncio
    
    web_app = FastAPI(title="Beat Sync Video Clipping Worker")

    class StartRequest(BaseModel):
        url: str
        width: int
        height: int
        target_scene_count: int

    @web_app.post("/start")
    async def start_job(data: StartRequest):
        # Spawn the job and get the call_id
        job = process_video_with_gemini.spawn(data.url, data.width, data.height, data.target_scene_count)
        return {"job_id": job.object_id}

    @web_app.get("/status/{job_id}")
    async def get_status(job_id: str):
        print(f"--- Polling status for Job ID: {job_id} ---")
        try:
            call = modal.functions.FunctionCall.from_id(job_id)
            
            try:
                # Poll result (timeout=0 does not block)
                result = call.get(timeout=0)
                
                response_data = {
                    "status": "completed",
                    "scenes": [
                        {
                            "key": s.get("key"),
                            "length": s.get("length"),
                            "width": s.get("width"),
                            "height": s.get("height"),
                            "url": s.get("url")
                        } for i, s in enumerate(result)
                    ],
                    "progress": 1.0
                }
                
                print(f"SUCCESS: Job {job_id} finished. Result count: {len(result) if result else 0}")
                print(f"response_data: {response_data}")
                return response_data

            except TimeoutError:
                print(f"STILL PROCESSING: Job {job_id} is not yet finished.")
                progress = progress_tracker.get(job_id, 0.1)
                return {"status": "processing", "progress": progress}
                
        except Exception as e:
            print(f"ERROR: Job {job_id} failed with exception: {str(e)}")
            print.error(f"Stack trace for {job_id}:", exc_info=True)
            return {
                "status": "failed", 
                "error": str(e)
            }

    return web_app