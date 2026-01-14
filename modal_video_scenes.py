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

# 1. SHARED STORAGE & APP CONFIG
# Volume for shared high-res videos between coordinator and parallel workers
video_storage = modal.Volume.from_name("high-res-videos", create_if_missing=True)

# A persistent dictionary for tracking cross-container job status
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
    cpu=1.0,
    retries=2,
    timeout=300
)
def cut_and_upload_clip(scene: Dict[str, Any], input_path: str, width: int, height: int) -> Dict[str, Any]:
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
    s3.upload_file(local_output_path, R2_BUCKET_NAME, key)

    # Generate Presigned URL (Valid for 24 hours) - DEBUG only
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
        "height": height
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

# 5. COORDINATOR: Track A (Gemini) + Parallel Fan-out Orchestration
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
    GOOGLE_GEMINI_API_KEY = config.GOOGLE_GEMINI_API_KEY

    client = genai.Client(api_key=GOOGLE_GEMINI_API_KEY)
    video_id = str(uuid.uuid4())
    high_res_path = f"/videos/{video_id}.mp4"

    async def perform_gemini_analysis(target_scene_count: int):
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

        progress_tracker[job_id] = 0.25
        
        # 2. Define the Schema for Structured Output
        # This mathematically guarantees valid JSON and correct key names
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "scenes": {
                    "type": "ARRAY",
                    "maxItems": target_scene_count,  # Enforce a maximum number of scenes
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
        ACT AS: A Lead Film Editor.
        TASK: Perform semantic scene segmentation to identify the primary narrative "chapters" of this video.

        CORE OBJECTIVE:
        Identify the most important semantic units, aiming for a maximum of {target_scene_count} scenes. 
        If the video has fewer than {target_scene_count} meaningful narrative shifts, return only the scenes that are truly distinct. Quality and semantic unity are more important than hitting the target count.

        SCENE DEFINITION (The "Unit"):
        A scene is a continuous story beat or conceptual chapter. Do NOT split a scene just because of:
        - Internal edits: Jump cuts or camera angle changes within the same location/topic.
        - B-roll: Cutaway footage that supports the current speaker or primary action.
        - Minor motion: The subject or camera moving within the same environment.

        ONLY mark a new scene boundary when there is a:
        1. "Hard Transition": A change in physical location or a significant jump in time.
        2. "Topic Shift": A clear transition to a new subject, talking point, or narrative beat.
        3. "Visual Reset": A radical change in environment, a title card, or a fade-to-black.

        CONSTRAINTS:
        - BUDGET: Up to {target_scene_count} scenes. Fewer is perfectly acceptable if the video is simple or short.
        - MINIMUM DURATION: Every scene MUST be at least 2.0 seconds long.
        - TIMESTAMP FORMAT: Total cumulative seconds (e.g., 125.5). NO decimal minutes.
        """
        
        # 3. Requesting the content with Structured Config
        response = client.models.generate_content(
            model="gemini-3-pro-preview",
            contents=[file_info, prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_schema,
                temperature=0.0, # Lower temperature = higher consistency
                # thinking_level="low",
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
        
        # 4. Cleanup and Parsing
        try:
            # Check for parsed object first
            if hasattr(response, 'parsed') and response.parsed is not None:
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
        # This is where we save the 60-90 seconds of download latency
        _, timestamps = await asyncio.gather(
            download_high_res_video(url, high_res_path),
            perform_gemini_analysis(target_scene_count)
        )

        # Filter out clips shorter than 2 seconds (and and invalid timestamps)
        timestamps = [
            ts for ts in timestamps 
            if (float(ts.get("end_time", 0)) - float(ts.get("start_time", 0))) >= 2.0
        ]
    
        # 5. TRIGGER FAN-OUT
        total_scenes = len(timestamps)
        if total_scenes == 0:
            progress_tracker[job_id] = 1.0
            return []

        print(f"Analysis complete. Clipping {total_scenes} scenes in parallel...")
        # map() handles massive scale automatically
        final_clips = []
        completed_count = 0

        async for clip in cut_and_upload_clip.map.aio(
            timestamps, 
            kwargs={"input_path": high_res_path, "width": width, "height": height},
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

        return final_clips

    except Exception as e:
        print(f"Pipeline Error: {e}")
        raise

    finally:
        if job_id in progress_tracker:
            del progress_tracker[job_id]

# 7. WEB INTERFACE
@app.function(image=image)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI
    from pydantic import BaseModel
    import modal.functions
    import logging
    import json
    import asyncio

    # Set up logging to show in Modal console
    logger = logging.getLogger("status-logger")
    logger.setLevel(logging.INFO)
    
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
        return {"job_id": job.object_id, "status": "processing"}

    @web_app.get("/status/{job_id}")
    async def get_status(job_id: str):
        print(f"--- Polling status for Job ID: {job_id} ---") # Standard stdout log
        try:
            call = modal.functions.FunctionCall.from_id(job_id)
            
            try:
                # Poll result (timeout=0 does not block)
                result = call.get(timeout=0)
                
                # LOG THE RAW RESULT
                # This helps you see if Gemini/Workers returned what you expected
                print(f"SUCCESS: Job {job_id} finished. Result count: {len(result) if result else 0}")
                logger.info(f"Raw Modal Result: {json.dumps(result, indent=2)}")

                response_data = {
                    "status": "completed",
                    "scenes": [
                        {
                            "key": s.get("key"),
                            "length": round(float(s.get("end_time", 0)) - float(s.get("start_time", 0)), 2),
                            "width": s.get("width"),
                            "height": s.get("height"),
                            "url": s.get("url")
                        } for i, s in enumerate(result)
                    ],
                    "progress": 1.0
                }
                
                # LOG THE FINAL MAPPED RESPONSE
                print(f"Final Response sent to client: {len(response_data['scenes'])} scenes")
                return response_data

            except TimeoutError:
                print(f"STILL PROCESSING: Job {job_id} is not yet finished.")
                progress = progress_tracker.get(job_id, 0.1)
                return {"status": "processing", "progress": progress}
                
        except Exception as e:
            # LOG THE ERROR
            print(f"ERROR: Job {job_id} failed with exception: {str(e)}")
            logger.error(f"Stack trace for {job_id}:", exc_info=True)
            return {
                "status": "failed", 
                "error": str(e)
            }

    return web_app