import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    # Lazy imports used by the video splitting endpoint
    from scenedetect import open_video, SceneManager, StatsManager  # type: ignore
    from scenedetect.detectors import ContentDetector  # type: ignore
    try:
        from scenedetect.video_stream import VideoOpenFailure  # type: ignore
    except Exception:  # pragma: no cover
        VideoOpenFailure = None  # type: ignore
except Exception:
    open_video = None  # type: ignore
    SceneManager = None  # type: ignore
    ContentDetector = None  # type: ignore
    StatsManager = None  # type: ignore
    VideoOpenFailure = None  # type: ignore


def is_video_open_failure(exc: Exception) -> bool:
    """
    Return True when the exception corresponds to PySceneDetect's VideoOpenFailure.
    Returns False if VideoOpenFailure type is not available in the environment.
    """
    if VideoOpenFailure is None:
        return False
    return isinstance(exc, VideoOpenFailure)  # type: ignore[arg-type]


def _safe_get_video_fps(video_obj) -> Optional[float]:
    try:
        # PySceneDetect provides base_timecode with framerate
        base = getattr(video_obj, "base_timecode", None)
        if base is not None:
            fps_val = getattr(base, "framerate", None)
            if fps_val:
                return float(fps_val)
    except Exception:
        pass
    try:
        fps_val = getattr(video_obj, "frame_rate", None)
        if fps_val:
            return float(fps_val)
    except Exception:
        pass
    return None


def _percentile_of_sorted(sorted_values: List[float], percent: float) -> float:
    # percent in [0, 100]
    n = len(sorted_values)
    if n == 0:
        raise ValueError("Empty values")
    if n == 1:
        return sorted_values[0]
    if percent <= 0:
        return sorted_values[0]
    if percent >= 100:
        return sorted_values[-1]
    rank = (percent / 100.0) * (n - 1)
    low = int(rank)
    high = min(low + 1, n - 1)
    fraction = rank - low
    return sorted_values[low] * (1.0 - fraction) + sorted_values[high] * fraction


def _find_local_maxima(values: List[float], min_separation: int) -> List[int]:
    peaks: List[int] = []
    last_idx = -min_separation
    # Require strict greater than previous and >= next to be a peak
    for i in range(1, len(values) - 1):
        if i - last_idx < min_separation:
            continue
        if values[i] > values[i - 1] and values[i] >= values[i + 1]:
            peaks.append(i)
            last_idx = i
    return peaks


def _choose_threshold_from_content_vals(content_vals: List[float], fps: float, min_scene_seconds: float) -> float:
    if not content_vals:
        # Fallback; caller should log
        return 27.0
    min_sep = max(1, int(min_scene_seconds * fps))
    sorted_vals = sorted(content_vals)
    # Candidate thresholds from upper percentiles
    percentiles = [80.0, 85.0, 90.0, 92.5, 95.0, 97.0, 98.0, 98.5, 99.0, 99.2, 99.4, 99.6, 99.8, 99.9]
    candidates = sorted({_percentile_of_sorted(sorted_vals, p) for p in percentiles})

    best_threshold: Optional[float] = None
    best_score: Optional[tuple] = None  # (num_scenes, -threshold)

    for t in candidates:
        peaks = _find_local_maxima(content_vals, min_sep)
        peaks = [idx for idx in peaks if content_vals[idx] >= t]
        if not peaks:
            continue
        # Build scene lengths in frames by diff of cut indices
        cut_idxs = [0] + peaks + [len(content_vals) - 1]
        lengths = [cut_idxs[i + 1] - cut_idxs[i] for i in range(len(cut_idxs) - 1)]
        if any(length < min_sep for length in lengths):
            # Reject thresholds that produce sub-min scenes
            continue
        score = (len(lengths), -t)
        if best_score is None or score > best_score:
            best_score = score
            best_threshold = t

    if best_threshold is not None:
        return float(best_threshold)
    # Fallback to high percentile if no candidate satisfied constraints
    try:
        return float(_percentile_of_sorted(sorted_vals, 98.0))
    except Exception:
        return 27.0


def _extract_content_vals_from_stats_csv(csv_path: Path) -> List[float]:
    import csv as _csv
    vals: List[float] = []
    with csv_path.open(newline="") as f:
        reader = _csv.DictReader(f)
        col = "content_val" if "content_val" in (reader.fieldnames or []) else None
        if not col:
            if reader.fieldnames:
                for c in reader.fieldnames:
                    if "content" in c and "val" in c:
                        col = c
                        break
        if not col:
            raise RuntimeError("Stats CSV missing 'content_val' column")
        for row in reader:
            text = row.get(col)
            if not text:
                continue
            try:
                vals.append(float(text))
            except Exception:
                continue
    if not vals:
        raise RuntimeError("No content values found in stats CSV")
    return vals


def auto_choose_threshold_for_video(video_path: Path, min_scene_seconds: float) -> float:
    if open_video is None or SceneManager is None or ContentDetector is None or StatsManager is None:
        raise RuntimeError("PySceneDetect is not installed. Please add 'scenedetect' to requirements.")

    video = open_video(str(video_path))
    fps = _safe_get_video_fps(video)
    if fps is None or fps <= 0:
        # As a very last resort, assume 30 fps to keep service functional.
        fps = 30.0

    # Single default pass (no explicit downscale kwarg to maximize compatibility).
    stats_manager = StatsManager()
    scene_manager = SceneManager(stats_manager=stats_manager)
    scene_manager.add_detector(ContentDetector(threshold=27.0))
    scene_manager.detect_scenes(video=video)

    temp_dir = Path(tempfile.mkdtemp(prefix="tt2mp3-stats-"))
    try:
        stats_csv = temp_dir / "stats.csv"
        stats_manager.save_to_csv(str(stats_csv))
        content_vals = _extract_content_vals_from_stats_csv(stats_csv)
    finally:
        try:
            for p in temp_dir.glob("*"):
                try:
                    p.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass
            temp_dir.rmdir()
        except Exception:
            pass

    chosen_threshold = _choose_threshold_from_content_vals(content_vals, fps=float(fps), min_scene_seconds=min_scene_seconds)
    logger.info("auto-scene selected threshold=%.2f", chosen_threshold)
    return float(chosen_threshold)


def detect_scenes_seconds(video_path: Path, *, threshold: float, min_scene_seconds: float) -> List[Tuple[float, float]]:
    if open_video is None or SceneManager is None or ContentDetector is None:
        raise RuntimeError("PySceneDetect is not installed. Please add 'scenedetect' to requirements.")

    video = open_video(str(video_path))
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector(threshold=threshold))

    # Use default detection settings.
    scene_manager.detect_scenes(video=video)
    scene_list = scene_manager.get_scene_list()
    if not scene_list:
        try:
            duration = float(video.duration.get_seconds())  # type: ignore[attr-defined]
        except Exception:
            # Fallback if duration not available
            duration = 0.0
        return [(0.0, duration)]

    ranges: List[Tuple[float, float]] = []
    for start, end in scene_list:
        start_s = float(start.get_seconds())
        end_s = float(end.get_seconds())
        if (end_s - start_s) >= max(0.0, min_scene_seconds):
            ranges.append((start_s, end_s))
    if not ranges:
        try:
            duration = float(video.duration.get_seconds())  # type: ignore[attr-defined]
        except Exception:
            duration = 0.0
        return [(0.0, duration)]
    return ranges


