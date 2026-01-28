import json
import os
import subprocess
import io
import time
import uuid
import asyncio
from typing import Dict, Any, List

import modal

try:
    import boto3
    import hashlib
    import yt_dlp
    import numpy as np
    from scenedetect import SceneManager, StatsManager, ContentDetector, AdaptiveDetector, HashDetector, open_video, FrameTimecode
    from google import genai
    from google.genai import types
except ImportError:
    pass

# Volume for shared high-res videos between coordinator and parallel workers
video_storage = modal.Volume.from_name("high-res-videos", create_if_missing=True)

# Persistent dictionary for tracking cross-container job status
progress_tracker = modal.Dict.from_name("clipping-progress", create_if_missing=True)

app = modal.App("clip-video")

# Image containing all required binaries and libraries
image = (
    modal.Image.debian_slim(python_version="3.10")
    .apt_install(
        "ffmpeg", 
        "git", 
        "nodejs", 
        "npm", 
        "libgl1-mesa-glx", 
        "libglib2.0-0", 
    )
    .pip_install(
        "fastapi", 
        "google-genai", 
        "requests", 
        "yt-dlp", 
        "boto3",
        "python-dotenv",
        "scenedetect[opencv-headless]",
        "numpy"
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

def find_precise_boundary(fuzzy_time: float, video_path: str) -> float:
    try:
        print(f"Finding precise boundary. Gemini time: {fuzzy_time} video path: {video_path}")
        
        video = open_video(video_path)
        stats_manager = StatsManager()
        scene_manager = SceneManager(stats_manager)
        scene_manager.add_detector(ContentDetector(threshold=27.0))

        # Search window: +/- 1.5 second
        SEARCH_RADIUS = 1.5
        start_search = max(0, fuzzy_time - SEARCH_RADIUS)
        end_search = fuzzy_time + SEARCH_RADIUS
        
        video.seek(start_search)
        scene_manager.detect_scenes(video, end_time=end_search)
        scene_list = scene_manager.get_scene_list()
    
        if not scene_list:
            return fuzzy_time

        # scene_list contains (start_time, end_time) tuples
        # Cuts are the start times of scenes after the first one
        cuts = [scene[0] for scene in scene_list[1:]]
    
        if not cuts:
            return fuzzy_time

        def get_fitness_score(timecode):
            m = stats_manager.get_metrics(timecode.get_frames(), ['content_val'])
            cut_strength = m[0] if (m and m[0] is not None) else 0
            
            # Higher distance = lower multiplier
            distance = abs(timecode.get_seconds() - fuzzy_time)
            proximity_multiplier = max(0, 1.0 - (distance / SEARCH_RADIUS))
            
            # Combine scores. We square the proximity to bias heavily toward the fuzzy_time.
            return cut_strength * (proximity_multiplier ** 2)

        best_cut_timecode = max(cuts, key=get_fitness_score)
        actual_cut = best_cut_timecode.get_seconds()
        print(f"Actual cut: {actual_cut}")
        return actual_cut
        
    except Exception as e:
        print(f"Precise detection failed for {fuzzy_time}: {e}")
        return fuzzy_time  # Fallback to Gemini's original timestamp

def detect_high_confidence_subscenes(start_time: float, end_time: float, video_path: str) -> List[tuple]:
    ADAPTIVE_THRESHOLD = 7.0
    SCREAMER_ADAPTIVE_THRESHOLD = 13.0
    HASH_THRESHOLD = 0.4
    
    try:
        video = open_video(video_path)
        fps = video.frame_rate
        video.seek(start_time)

        stats_manager = StatsManager()
        scene_manager = SceneManager(stats_manager)
        scene_manager.add_detector(AdaptiveDetector(adaptive_threshold=ADAPTIVE_THRESHOLD))
        scene_manager.add_detector(HashDetector(threshold=HASH_THRESHOLD))
        scene_manager.detect_scenes(video, end_time=end_time)

        raw_scenes = scene_manager.get_scene_list()
        if len(raw_scenes) <= 1:
            return []

        final_scenes = []
        current_scene_start = raw_scenes[0][0]

        for i in range(len(raw_scenes) - 1):
            cut_timecode = raw_scenes[i][1] # The 'cut' is the end of the current raw scene
            cut_frame = cut_timecode.get_frames()

            # --- CONSENSUS CHECK ---
            adaptive_metric = 'adaptive_ratio (w=2)'
            hash_metric = 'hash_dist [size=16 lowpass=2]'

            if stats_manager.metrics_exist(cut_frame, [adaptive_metric, hash_metric]):
                metrics = stats_manager.get_metrics(cut_frame, [adaptive_metric, hash_metric])
                
                adaptive_val = metrics[0] if metrics[0] is not None else 0
                hash_val = metrics[1] if metrics[1] is not None else 0

                # DUAL CONSENSUS: Both must cross the threshold
                # OR one must be an extreme 'screamer'
                is_consensus = (adaptive_val >= ADAPTIVE_THRESHOLD and hash_val >= HASH_THRESHOLD)
                is_screamer = (adaptive_val >= SCREAMER_ADAPTIVE_THRESHOLD)

                if is_consensus or is_screamer:
                    label = "CONSENSUS" if is_consensus else "SCREAMER"
                    print(f"CUT ACCEPTED [{label}] at frame {cut_frame}: A={adaptive_val:.2f}, H={hash_val:.2f}")
                    final_scenes.append((current_scene_start, cut_timecode))
                    current_scene_start = cut_timecode
                else:
                    print(f"CUT REJECTED at frame {cut_frame}: A={adaptive_val:.2f}, H={hash_val:.2f} (Weak Signal)")
            else:
                print(f"CUT REJECTED at frame {cut_frame}: NO METRICS FOUND in StatsManager")

        if not final_scenes:
            return []

        # Add the final segment (from the last valid cut to the end of the search window)
        final_end_time = FrameTimecode(end_time, fps=fps)
        final_scenes.append((current_scene_start, final_end_time))

        return final_scenes

    except Exception as e:
        print(f"High confidence subscenes detection failed for: {e}")
        return []

def cut_and_upload_clip(start_time: float, end_time: float, input_path: str, scene_metadata: Dict[str, Any]) -> Dict[str, Any]:
    from botocore.config import Config
    from env_vars import config

    width = scene_metadata.get("width")
    height = scene_metadata.get("height")
    duration = (end_time - start_time) - 0.06 # 0.06 to prevent frame bleed
    file_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    clip_filename = f"{file_hash}.mp4"
    local_output_path = f"/tmp/{clip_filename}"
    key = f"user-videos/temp/{clip_filename}"

    # FFmpeg command
    cmd = [
        "ffmpeg", "-y",
        "-ss", f"{start_time:.3f}",         # Seek before input
        "-i", input_path,
        "-to", f"{duration:.3f}",           # Use duration 
        "-c:v", "libx264",                  # Use H.264 video codec
        "-preset", "ultrafast",             # Keep it fast
        "-crf", "23",                       # Good balance of quality/size
        "-fps_mode", "cfr",                 # Force constant frame rate logic
        "-c:a", "aac",                      # Ensure audio is compatible
        "-avoid_negative_ts", "make_zero",  # Resets start timestamp to 0.0
        "-map_metadata", "-1",              # Strips old sync data
        "-movflags", "+faststart",
        local_output_path
    ]
    subprocess.run(cmd, check=True)

    # Upload to R2 bucket
    s3 = boto3.client(
        "s3",
        endpoint_url=config.R2_ENDPOINT_URL,
        aws_access_key_id=config.R2_ACCESS_KEY_ID,
        aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto" 
    )
    s3.upload_file(local_output_path, config.R2_BUCKET_NAME, key)

    # Generate presigned URL (valid for 24 hours) - CURRENTLY UNUSED
    presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.R2_BUCKET_NAME, "Key": key},
        ExpiresIn=86400 
    )

    if os.path.exists(local_output_path):
        os.remove(local_output_path)

    return {
        **scene_metadata,
        "url": presigned_url,
        "filename": clip_filename,
        "key": key,
        "width": width,
        "height": height,
        "length": round(duration, 2),
    }

# FAN-OUT WORKER: Cut and Upload to R2
@app.function(
    image=image, 
    volumes={"/videos": video_storage},
    secrets=[modal.Secret.from_name("kaiber-secrets")], # Contains Gemini & R2 keys
    cpu=1.0,
    retries=2,
    timeout=300
)
def create_clip(scene: Dict[str, Any], input_path: str) -> Dict[str, Any]:
    """Runs in parallel for every scene: Clips via FFmpeg and uploads to R2."""

    start_time = find_precise_boundary(scene["start_time"], input_path)
    end_time = find_precise_boundary(scene["end_time"], input_path)

    # Perform sub-scene analysis
    sub_scenes = detect_high_confidence_subscenes(start_time, end_time, input_path)
    final_clips = []

    if not sub_scenes:
        print("No obvious cuts. Process as one single clip.")
        final_clips.append(cut_and_upload_clip(start_time, end_time, input_path, scene))
    else:
        print("Potentially obvious cuts missed by Gemini. Process sub-clips.")
        for sub in sub_scenes:
            abs_start = sub[0].get_seconds()
            abs_end = sub[1].get_seconds()
            if abs_end - abs_start >= 0.8:
                final_clips.append(cut_and_upload_clip(abs_start, abs_end, input_path, scene))

    return final_clips

# TRACK B HELPER: High-Res Download
async def download_high_res_video(url: str, output_path: str):
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'outtmpl': output_path,
        'quiet': True,
        'socket_timeout': 30,
        'retries': 5,
        'fragment_retries': 10,
        'retry_sleep': lambda n: 2 * n,
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
async def process_video_with_gemini(url: str, width: int, height: int) -> List[Dict[str, Any]]:
    job_id = modal.current_function_call_id()
    progress_tracker[job_id] = 0.1
    
    from env_vars import config

    client = genai.Client(api_key=config.GOOGLE_GEMINI_API_KEY)
    video_id = str(uuid.uuid4())
    high_res_path = f"/videos/{video_id}.mp4"

    async def perform_gemini_analysis():
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
                    "minItems": 1,
                    "maxItems": 50,
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
                            "description": {
                                "type": "STRING", 
                                "description": "Briefly explain WHY this is a new scene (e.g., 'Cut to close-up', 'Location change')."
                            }
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

        HOW TO DETECT SCENES:
        Divide the video based on Visual and Narrative Discontinuity.
        1. High-Density Rule: If there are rapid-fire cuts, capture each unique camera angle as a scene.
        2. Low-Density Rule: If the camera is static or following a single subject without cutting, do NOT split it, regardless of how long the video is.
        3. The 'Significant Change' Test: A new scene is defined by a change in Subject, Location, or Camera Perspective. If none of these three change, it is the same scene.

        CONSTRAINTS:
        - TIMESTAMP FORMAT: Total cumulative seconds (e.g., 125.5). NO decimal minutes.
        """
        
        # Gemini inference
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=[file_info, prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_schema,
                temperature=0.0, # Lower temperature = higher consistency
                thinking_config=types.ThinkingConfig(thinking_level="high"),
                max_output_tokens=65536, # Maximum possible to prevent truncation
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
            perform_gemini_analysis()
        )

        actual_duration = get_video_duration(high_res_path)

        # Filter/validate timestamps
        valid_timestamps = []
        MIN_RESULT_DURATION = 2.0

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

            duration = end - start
            
            # Filter clips shorter than 2 seconds
            if duration >= MIN_RESULT_DURATION:
                ts.update({
                    "start_time": start,
                    "end_time": end,
                    "width": width,
                    "height": height
                })
                valid_timestamps.append(ts)

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

        async for clip in create_clip.map.aio(
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

            final_clips.extend(clip)
        
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
    
    web_app = FastAPI(title="Beat Sync Video Clipping Worker")

    class StartRequest(BaseModel):
        url: str
        width: int
        height: int

    @web_app.post("/start")
    async def start_job(data: StartRequest):
        # Spawn the job and get the call_id
        job = process_video_with_gemini.spawn(data.url, data.width, data.height)
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