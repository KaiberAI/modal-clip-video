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

# 1. Shared Storage
video_storage = modal.Volume.from_name("high-res-videos", create_if_missing=True)
app = modal.App("clip-video")

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("ffmpeg")
    .pip_install("fastapi", "google-genai", "requests", "yt-dlp")
)

# 2. FAN-OUT WORKER: This cuts the actual clips
@app.function(image=image, volumes={"/videos": video_storage})
def cut_clip(scene: Dict[str, Any], input_path: str):
    """Runs in parallel for every scene identified by Gemini."""
    import subprocess
    clip_id = str(uuid.uuid4())[:8]
    output_path = f"/videos/clip_{clip_id}.mp4"
    
    # Fast Seek (-ss BEFORE -i)
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(scene["start_time"]),
        "-i", input_path,
        "-to", str(scene["end_time"]),
        "-c", "copy", # No re-encoding = instant speed
        output_path
    ]
    subprocess.run(cmd, check=True)
    return output_path

# 3. HELPER: Track B (Download)
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
    await asyncio.to_thread(download)
    video_storage.commit() # Important: Flush to Volume

# 4. COORDINATOR: Track A (Gemini) + Fan-out Trigger
@app.function(
    image=image,
    secrets=[modal.Secret.from_name("kaiber-secrets")],
    volumes={"/videos": video_storage},
    timeout=3600,
)
async def process_video_with_gemini(url: str) -> List[Dict[str, Any]]:
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GOOGLE_GEMINI_API_KEY"])
    
    # FIX: Ensure path is on the Volume, not /tmp
    video_id = str(uuid.uuid4())
    high_res_path = f"/videos/{video_id}.mp4"

    async def perform_gemini_analysis():
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
        
        prompt = "Identify scenes. Return JSON: [{'start_time': sec, 'end_time': sec, 'title': str}]"
        response = client.models.generate_content(model="gemini-2.5-flash-lite", contents=[file_info, prompt])
        
        timestamps = json.loads(response.text.replace("```json", "").replace("```", "").strip())
        client.files.delete(name=video_file.name)
        return timestamps

    try:
        # Run Tracks A & B in parallel
        _, timestamps = await asyncio.gather(
            download_high_res_video(url, high_res_path),
            perform_gemini_analysis()
        )
        
        # FAN-OUT: Trigger parallel clipping workers
        print(f"Starting fan-out for {len(timestamps)} clips...")
        # map() handles the distribution automatically
        clip_paths = list(cut_clip.map(timestamps, kwargs={"input_path": high_res_path}))
        
        for i, ts in enumerate(timestamps):
            ts["clip_path"] = clip_paths[i]

        return timestamps

    except Exception as e:
        print(f"Error: {e}")
        raise