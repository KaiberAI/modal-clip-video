"""
Modal app for video scene detection and splitting.

Provides endpoints for:
- Splitting videos into scene-based clips using PySceneDetect

Based on the mirror-api project, adapted for Modal deployment.
"""

import os
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, Literal
from datetime import datetime

import modal

# Modal app
app = modal.App("clip-video")

# Work directory
WORKDIR = "/root/.cache/video_scenes"

# Python packages
PYTHON_PACKAGES = [
    "scenedetect==0.6.1",
    "opencv-python-headless==4.10.0.84",
    "requests",
    "fastapi",
    "boto3",
]

# Modal Image with dependencies
image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("ffmpeg")  # ffprobe is included with ffmpeg
    .pip_install(*PYTHON_PACKAGES)
    .add_local_file("services/scenes.py", f"{WORKDIR}/scenes.py", copy=True)
    .add_local_file("services/ffmpeg_utils.py", f"{WORKDIR}/ffmpeg_utils.py", copy=True)
    .add_local_file("env_vars.py", "/root/env_vars.py", copy=True)
)


def generate_unique_id() -> str:
    """Generates a unique identifier combining UUID and timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_uuid = str(uuid.uuid4())[:8]
    return f"{timestamp}_{unique_uuid}"


def get_filename_from_url(url: str) -> str:
    """Extract filename from URL"""
    from urllib.parse import urlparse
    basename = os.path.basename(urlparse(url).path)
    return basename.replace(" ", "_") or "video.mp4"


def create_retry_session(
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
):
    """Create a requests session with retry logic"""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def download_file(
    url: str,
    local_path: str,
    *,
    session,
    connect_timeout: float = 10.0,
    read_timeout: float = 300.0,
    max_attempts: int = 5,
    backoff_base_seconds: float = 0.5,
    max_bytes: int = 524288000,  # 500MB default
) -> str:
    """Download file with retry logic and resume support"""
    import requests

    safe_url = requests.utils.requote_uri(url)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    temp_path = f"{local_path}.part"

    # Best-effort expected size
    expected_size = None
    try:
        head = session.head(safe_url, timeout=(connect_timeout, read_timeout), allow_redirects=True)
        if head.ok and head.headers.get('Content-Length'):
            content_length = int(head.headers['Content-Length'])
            if content_length > max_bytes:
                raise RuntimeError(f"File size {content_length} exceeds limit {max_bytes}")
            expected_size = content_length
    except Exception:
        pass

    downloaded_bytes = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0

    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        headers = {}
        mode = 'ab'
        if downloaded_bytes > 0:
            headers['Range'] = f"bytes={downloaded_bytes}-"
        else:
            mode = 'wb'

        try:
            with session.get(
                safe_url,
                stream=True,
                timeout=(connect_timeout, read_timeout),
                headers=headers,
            ) as response:
                if response.status_code == 200 and downloaded_bytes > 0:
                    downloaded_bytes = 0
                    mode = 'wb'
                response.raise_for_status()
                with open(temp_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        downloaded_bytes += len(chunk)
                        if downloaded_bytes > max_bytes:
                            raise RuntimeError(f"Download aborted: exceeded maximum size {max_bytes}")
                        f.write(chunk)

            if expected_size is not None and downloaded_bytes < expected_size:
                raise RuntimeError(
                    f"incomplete download: got {downloaded_bytes} of {expected_size} bytes"
                )
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            os.replace(temp_path, local_path)
            return local_path

        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError):
            import time as _time
            wait = backoff_base_seconds * (2 ** (attempt - 1))
            _time.sleep(wait)
            continue
        except Exception as e:
            if attempt < max_attempts:
                import time as _time
                wait = backoff_base_seconds * (2 ** (attempt - 1))
                _time.sleep(wait)
                continue
            try:
                if os.path.exists(temp_path) and os.path.getsize(temp_path) == 0:
                    os.remove(temp_path)
            except Exception:
                pass
            raise RuntimeError(f"Failed to download {safe_url} after {max_attempts} attempts: {e}")

    raise RuntimeError(f"Failed to download {safe_url} after {max_attempts} attempts")


def sanitize_filename(name: str) -> str:
    """Sanitize filename for safe storage"""
    import re
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return sanitized if sanitized else "file"


def _initialize_r2_connection():
    """
    Initialize R2 connection using environment variables.
    Re-used from modal-gen-media-thumbnails pattern.
    """
    import boto3
    from botocore.config import Config
    from env_vars import (
        R2_ACCESS_KEY_ID,
        R2_SECRET_ACCESS_KEY,
        R2_ENDPOINT_URL,
        R2_BUCKET_NAME,
    )
    
    # Validate required environment variables
    if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
        raise ValueError(
            "R2 credentials are not set. Ensure R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY "
            "are in Modal secrets (kaiber-secrets) or environment variables."
        )
    
    # Use boto3 directly for R2 operations (same pattern as modal-gen-media-thumbnails)
    session = boto3.Session(
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    )
    config = Config(s3={"use_accelerate_endpoint": False}, signature_version="s3v4")
    s3_resource = session.resource(
        "s3",
        config=config,
        endpoint_url=R2_ENDPOINT_URL,
    )
    
    return s3_resource, R2_BUCKET_NAME


def store_clip_to_r2(clip_path: Path, r2_key: str) -> Dict[str, Any]:
    """
    Store clip to Cloudflare R2 and return metadata.
    Re-uses pattern from modal-gen-media-thumbnails.
    """
    from env_vars import R2_PUBLIC_CDN_URL
    
    s3_resource, bucket_name = _initialize_r2_connection()
    
    # Upload to R2 (same pattern as modal-gen-media-thumbnails)
    s3_resource.Bucket(bucket_name).upload_file(
        str(clip_path), r2_key
    )
    
    # Generate public URL
    public_url = f"{R2_PUBLIC_CDN_URL}/{r2_key}"
    
    return {
        "fileName": os.path.basename(r2_key),
        "sizeBytes": clip_path.stat().st_size,
        "storageProvider": "r2",
        "gsUri": f"r2://{bucket_name}/{r2_key}",
        "objectName": r2_key,
        "publicUrl": public_url,
        "mimeType": "video/mp4",
    }


# Modal class for video processing
@app.cls(
    image=image,
    cpu=8,  # Multi-core for scene detection and encoding
    timeout=3600,  # 1 hour
    secrets=[modal.Secret.from_name("kaiber-secrets")],  # R2 credentials from Modal secrets
    scaledown_window=300,  # 5 minutes
)
class VideoScenes:
    @modal.enter()
    def setup(self):
        """Setup method - creates working directories"""
        import sys

        os.makedirs(f"{WORKDIR}/input_videos", exist_ok=True)
        os.makedirs(f"{WORKDIR}/output_clips", exist_ok=True)

        # Add modules to path
        sys.path.insert(0, WORKDIR)
        
        self.consecutive_failures = 0

    @modal.method()
    def split_video_on_scenes(
        self,
        video_url: str,
        job_id: str,
        threshold: Optional[str] = None,
        min_scene_ms: int = 3000,
        include_audio: bool = True,
        mode: Literal["fast", "precision"] = "fast",
    ) -> Dict[str, Any]:
        """
        Split video into scene-based clips.

        Args:
            video_url: Direct HTTP(S) URL to video file
            threshold: "auto" or numeric threshold for scene detection
            min_scene_ms: Minimum scene duration in milliseconds
            include_audio: Include audio in exported clips
            mode: "fast" (keyframe-aligned) or "precision" (re-encode)

        Returns:
            Dictionary with scene detection results and clips
        """
        if not isinstance(video_url, str) or not video_url.strip():
            raise ValueError("video_url must be a non-empty string")

        safe_url = video_url.strip()
        
        # Import adapted modules
        import sys
        sys.path.insert(0, WORKDIR)
        from scenes import (
            auto_choose_threshold_for_video,
            detect_scenes_seconds,
            is_video_open_failure,
        )
        from ffmpeg_utils import (
            resolve_ffmpeg_path,
            resolve_ffprobe_path,
            get_keyframe_times,
        )
        
        try:
            # Download video
            session = create_retry_session()
            work_dir = Path(f"{WORKDIR}/input_videos")
            work_dir.mkdir(parents=True, exist_ok=True)
            
            # Use a simpler download approach
            input_name = get_filename_from_url(safe_url)
            input_path = work_dir / input_name
            downloaded = download_file(
                safe_url,
                str(input_path),
                session=session,
                connect_timeout=10.0,
                read_timeout=300.0,
                max_bytes=524288000,  # 500MB
            )
            print(f"Downloaded video to: {downloaded}")

            if not input_path.exists() or input_path.stat().st_size == 0:
                raise RuntimeError("Downloaded file is empty or missing")

            ffmpeg_path = resolve_ffmpeg_path()
            ffprobe_path = resolve_ffprobe_path()
            min_scene_seconds = max(0.0, float(min_scene_ms) / 1000.0)
            use_precision = (mode == "precision")

            # Pre-compute keyframes for fast mode
            keyframe_times: List[float] = []
            if not use_precision:
                keyframe_times = get_keyframe_times(input_path, ffprobe_path) or []

            # Determine threshold - matches original logic
            used_threshold: float
            use_auto = False
            if threshold is not None:
                text = str(threshold).strip().lower()
                if text == "" or text == "auto":
                    use_auto = True
                else:
                    try:
                        used_threshold = float(text)
                        print(f"Using provided threshold: {used_threshold}")
                    except Exception:
                        raise ValueError(f"Invalid threshold value: {threshold}")
            if threshold is None or use_auto:
                try:
                    used_threshold = auto_choose_threshold_for_video(input_path, min_scene_seconds)
                    print(f"Auto-selected threshold: {used_threshold}")
                except Exception as e:
                    print(f"Auto threshold selection failed: {e}, using default 27.0")
                    used_threshold = 27.0

            # Detect scenes
            print("Detecting scenes...")
            try:
                scenes = detect_scenes_seconds(
                    input_path,
                    threshold=used_threshold,
                    min_scene_seconds=min_scene_seconds,
                )
            except Exception as e:
                if is_video_open_failure(e):
                    raise RuntimeError(f"Unable to open video: {e}")
                raise RuntimeError(f"Scene detection failed: {e}")

            print(f"Found {len(scenes)} scenes")

            # Export segments
            base_no_ext = os.path.splitext(input_name)[0]
            clip_results = self._export_segments(
                input_path=input_path,
                segments=scenes,
                mode=mode,
                include_audio=include_audio,
                work_dir=Path(f"{WORKDIR}/output_clips"),
                base_no_ext=base_no_ext,
                label="scene",
                ffmpeg_path=ffmpeg_path,
                ffprobe_path=ffprobe_path,
                keyframe_times=keyframe_times if not use_precision else None,
                job_id=job_id,
                fallback_probe_start=True,  # Match original split behavior
                fallback_probe_end=True,
            )

            self.consecutive_failures = 0
            return {
                "job_id": job_id,
                "usedThreshold": used_threshold,
                "clips": clip_results,
            }

        except Exception as e:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 10:
                raise Exception(f"Too many consecutive failures ({self.consecutive_failures})")
            print(f"Error: {e}")
            raise

    def _align_segment_fast(
        self,
        input_path: Path,
        start_s: float,
        end_s: float,
        *,
        keyframe_times: Optional[List[float]],
        ffprobe_path: str,
        fallback_probe_start: bool,
        fallback_probe_end: bool,
    ) -> Tuple[float, float]:
        """
        Align segment boundaries for stream-copy:
        - If keyframe_times available: start -> next keyframe >= start, end -> prev keyframe <= end.
        - Otherwise: optionally probe next keyframe for start; leave end unchanged.
        """
        from bisect import bisect_left, bisect_right
        import sys
        sys.path.insert(0, WORKDIR)
        from ffmpeg_utils import get_next_keyframe_time, get_previous_keyframe_time
        
        if keyframe_times:
            si = bisect_left(keyframe_times, start_s)
            aligned_start = keyframe_times[si] if si < len(keyframe_times) else start_s
            ei = bisect_right(keyframe_times, end_s) - 1
            aligned_end = keyframe_times[ei] if ei >= 0 else end_s
            return aligned_start, aligned_end
        aligned_start = start_s
        aligned_end = end_s
        if fallback_probe_start:
            probed_start = get_next_keyframe_time(input_path, start_s, ffprobe_path)
            aligned_start = probed_start if probed_start is not None else start_s
        if fallback_probe_end:
            probed_end = get_previous_keyframe_time(input_path, end_s, ffprobe_path)
            aligned_end = probed_end if probed_end is not None else end_s
        return aligned_start, aligned_end

    def _export_segments(
        self,
        *,
        input_path: Path,
        segments: List[Tuple[float, float]],
        mode: Literal["fast", "precision"],
        include_audio: bool,
        work_dir: Path,
        base_no_ext: str,
        label: str,
        ffmpeg_path: str,
        ffprobe_path: str,
        keyframe_times: Optional[List[float]] = None,
        job_id: str,
        fallback_probe_start: bool = False,
        fallback_probe_end: bool = False,
    ) -> List[Dict[str, Any]]:
        """Export segments to MP4 clips - matches original export_segments behavior"""
        import sys
        sys.path.insert(0, WORKDIR)
        from ffmpeg_utils import (
            ffmpeg_export_clip,
            ffmpeg_export_clip_precise,
        )
        
        results: List[Dict[str, Any]] = []
        index = 0
        
        for start_s, end_s in segments:
            if mode == "precision":
                export_start = start_s
                export_end = end_s
            else:
                export_start, export_end = self._align_segment_fast(
                    input_path,
                    start_s,
                    end_s,
                    keyframe_times=keyframe_times,
                    ffprobe_path=ffprobe_path,
                    fallback_probe_start=fallback_probe_start,
                    fallback_probe_end=fallback_probe_end,
                )
            # Skip zero/negative or extremely short segments
            # Note: index is incremented even when skipping to match original behavior
            if export_end <= export_start or (export_end - export_start) < 0.05:
                index += 1
                continue

            clip_base = sanitize_filename(f"{base_no_ext}-{label}-{index:03d}.mp4")
            clip_path = work_dir / clip_base

            try:
                if mode == "precision":
                    ffmpeg_export_clip_precise(
                        input_path,
                        export_start,
                        export_end,
                        clip_path,
                        ffmpeg_path,
                        include_audio=include_audio,
                    )
                else:
                    ffmpeg_export_clip(
                        input_path,
                        export_start,
                        export_end,
                        clip_path,
                        ffmpeg_path,
                        include_audio=include_audio,
                    )
            except Exception as e:
                raise RuntimeError(f"Failed to export clip {index}: {e}")

            # Upload to R2 (same pattern as modal-gen-media-thumbnails)
            r2_key = f"video-clips/{job_id}/{clip_base}"
            clip_metadata = store_clip_to_r2(clip_path, r2_key)
            
            duration_ms = int(round((export_end - export_start) * 1000.0))
            results.append({
                "startMs": int(round(export_start * 1000.0)),
                "endMs": int(round(export_end * 1000.0)),
                "durationMs": duration_ms,
                **clip_metadata,
            })
            index += 1
        
        return results


# --- Web endpoints ---

@app.function(
    image=image,
    min_containers=1,
    secrets=[modal.Secret.from_name("kaiber-secrets")],  # R2 credentials from Modal secrets
)
@modal.asgi_app()
def fastapi_app():
    from fastapi import FastAPI
    from modal.functions import FunctionCall
    from pydantic import BaseModel
    from typing import Optional, List, Literal

    web_app = FastAPI(title="Video Scenes API")

    class SplitRequest(BaseModel):
        url: str
        threshold: Optional[str] = None
        min_scene_ms: int = 3000
        include_audio: bool = True
        mode: Literal["fast", "precision"] = "fast"

    @web_app.post("/video/split")
    async def start_split_job(data: SplitRequest):
        """Start a video scene splitting job asynchronously."""
        model = VideoScenes()
        job_id = generate_unique_id()
        
        call = model.split_video_on_scenes.spawn(
            video_url=data.url,
            job_id=job_id,
            threshold=data.threshold,
            min_scene_ms=data.min_scene_ms,
            include_audio=data.include_audio,
            mode=data.mode,
        )
        return {
            "workflow_name": "clip-video-split",
            "job_id": job_id,
            "call_id": call.object_id,
            "start_time": datetime.now().isoformat(),
        }

    @web_app.get("/status/{call_id}")
    async def check_status(call_id: str):
        """Check the status of a video processing job using the call_id from the start response."""
        try:
            function_call = FunctionCall.from_id(call_id)

            try:
                result = function_call.get(timeout=0)
                return {"status": "done", "result": result}
            except TimeoutError:
                return {"status": "pending", "result": None}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    return web_app


# --- CLI for local testing ---

@app.local_entrypoint()
def main(
    video_url: str = "https://example.com/video.mp4",
    threshold: Optional[str] = None,
    min_scene_ms: int = 3000,
):
    """
    Local entrypoint for testing video scene detection.

    Usage:
        modal run modal_video_scenes.py --video-url <URL> --mode split --threshold auto
    """
    model = VideoScenes()
    
    job_id = generate_unique_id()
    result = model.split_video_on_scenes.remote(
        video_url=video_url,
        job_id=job_id,
        threshold=threshold,
        min_scene_ms=min_scene_ms,
        include_audio=True,
        mode="fast",
    )
    print(f"Detected {len(result['clips'])} scenes")
    print(f"Used threshold: {result['usedThreshold']}")
