from pathlib import Path
from typing import List, Optional
import subprocess

try:
    from imageio_ffmpeg import get_ffmpeg_exe
except Exception:
    get_ffmpeg_exe = None  # type: ignore


def resolve_ffmpeg_path() -> str:
    """Return path to ffmpeg binary, preferring imageio-ffmpeg if available."""
    if get_ffmpeg_exe is not None:
        try:
            return str(get_ffmpeg_exe())
        except Exception:
            pass
    return "ffmpeg"


def resolve_ffprobe_path() -> str:
    # imageio-ffmpeg does not provide ffprobe; rely on system ffprobe.
    return "ffprobe"


def ffmpeg_copy_without_audio(input_path: Path, output_path: Path, ffmpeg_path: str) -> None:
    cmd = [
        ffmpeg_path,
        "-y",
        "-i",
        str(input_path),
        "-c",
        "copy",
        "-an",
        str(output_path),
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError(f"ffmpeg failed to strip audio: {result.stderr.strip()[:500]}")


def probe_video_duration_seconds(input_path: Path, ffprobe_path: str) -> Optional[float]:
    """
    Return the total duration of the media (in seconds) using ffprobe.
    """
    cmd = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(input_path),
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            return None
        text = result.stdout.strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def get_next_keyframe_time(input_path: Path, start_seconds: float, ffprobe_path: str) -> Optional[float]:
    try:
        # List keyframe timestamps only.
        cmd = [
            ffprobe_path,
            "-loglevel",
            "error",
            "-skip_frame",
            "nokey",
            "-select_streams",
            "v:0",
            "-show_frames",
            "-show_entries",
            "frame=pkt_pts_time",
            "-of",
            "csv=p=0",
            str(input_path),
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            return None
        times: List[float] = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                times.append(float(line))
            except Exception:
                continue
        if not times:
            return None
        for ts in times:
            if ts >= start_seconds:
                return ts
        return None
    except Exception:
        return None


def get_keyframe_times(input_path: Path, ffprobe_path: str) -> Optional[List[float]]:
    """
    Return a sorted list of keyframe timestamps (in seconds) for the first video stream.
    Using one probe per input allows callers to align multiple segments without repeated probes.
    """
    try:
        cmd = [
            ffprobe_path,
            "-loglevel",
            "error",
            "-skip_frame",
            "nokey",
            "-select_streams",
            "v:0",
            "-show_frames",
            "-show_entries",
            "frame=pkt_pts_time",
            "-of",
            "csv=p=0",
            str(input_path),
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            return None
        times: List[float] = []
        for line in result.stdout.splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                times.append(float(text))
            except Exception:
                continue
        if not times:
            return None
        times.sort()
        return times
    except Exception:
        return None


def get_previous_keyframe_time(input_path: Path, end_seconds: float, ffprobe_path: str) -> Optional[float]:
    """
    Return the greatest keyframe timestamp <= end_seconds.
    """
    try:
        cmd = [
            ffprobe_path,
            "-loglevel",
            "error",
            "-skip_frame",
            "nokey",
            "-select_streams",
            "v:0",
            "-show_frames",
            "-show_entries",
            "frame=pkt_pts_time",
            "-of",
            "csv=p=0",
            str(input_path),
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            return None
        last: Optional[float] = None
        for line in result.stdout.splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                t = float(text)
            except Exception:
                continue
            if t <= end_seconds:
                last = t
            else:
                break
        return last
    except Exception:
        return None

def ffmpeg_export_clip(input_path: Path, start_seconds: float, end_seconds: float, output_path: Path, ffmpeg_path: str, *, include_audio: bool) -> None:
    # Keyframe-aligned fast seek compatible with ffmpeg 4.2.x:
    # - Place -ss before -i and use -t for duration with stream copy.
    # - Avoid flags not present in 4.2.x (e.g., -copyinkf, -reset_timestamps).
    duration = max(0.0, end_seconds - start_seconds)
    cmd = [
        ffmpeg_path,
        "-y",
        "-ss",
        f"{start_seconds:.6f}",
        "-i",
        str(input_path),
        "-t",
        f"{duration:.6f}",
        "-c",
        "copy",
    ]
    if not include_audio:
        cmd += ["-an"]
    cmd += [
        "-avoid_negative_ts",
        "make_zero",
        str(output_path),
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr.strip()[:500]}")


def ffmpeg_export_clip_precise(input_path: Path, start_seconds: float, end_seconds: float, output_path: Path, ffmpeg_path: str, *, include_audio: bool, video_crf: int = 23, video_preset: str = "veryfast") -> None:
    """
    Re-encode segment with accurate trimming. Places -ss/-to after -i and re-encodes.
    Defaults aim for speed with acceptable quality; caller can adjust CRF/preset if needed.
    """
    if end_seconds <= start_seconds:
        raise ValueError("end_seconds must be greater than start_seconds")
    cmd = [
        ffmpeg_path,
        "-y",
        "-i",
        str(input_path),
        "-ss",
        f"{start_seconds:.6f}",
        "-to",
        f"{end_seconds:.6f}",
        "-c:v",
        "libx264",
        "-preset",
        str(video_preset),
        "-crf",
        str(video_crf),
    ]
    if include_audio:
        cmd += ["-c:a", "aac", "-b:a", "192k"]
    else:
        cmd += ["-an"]
    cmd += [
        "-movflags",
        "+faststart",
        "-avoid_negative_ts",
        "make_zero",
        str(output_path),
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError(f"ffmpeg precise export failed: {result.stderr.strip()[:500]}")

