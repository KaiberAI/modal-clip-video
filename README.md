# Video Scenes Modal Project

A Modal AI deployment for video scene detection and splitting, extracted from the mirror-api project.

## Overview

This Modal project provides endpoints for:
- **Scene-based splitting**: Automatically detect and split videos into scenes using PySceneDetect

Both operations support:
- **Fast mode**: Keyframe-aligned stream copy (very fast, approximate cuts)
- **Precision mode**: Re-encode for exact frame-accurate cuts (slower, higher CPU)

## Features

- **High-performance scene detection**: Uses PySceneDetect's ContentDetector with automatic threshold selection
- **Flexible export modes**: Choose between speed (fast) or accuracy (precision)
- **Cloudflare R2 storage**: Clips are stored in R2 with CDN access (same pattern as modal-gen-media-thumbnails)
- **Asynchronous processing**: Submit jobs and poll for completion
- **RESTful API**: Clean FastAPI endpoints for integration

## Project Structure

```
.
├── modal_video_scenes.py    # Main Modal app with endpoints
├── env_vars.py              # Environment variable handling for R2
├── requirements.txt          # Python dependencies
├── README.md                # This file
└── services/                # Core processing modules
    ├── __init__.py
    ├── scenes.py            # Scene detection logic
    └── ffmpeg_utils.py      # FFmpeg utilities
```

## Deployment

### Prerequisites

1. Install Modal:
   ```bash
   pip install modal
   ```

2. Authenticate with Modal:
   ```bash
   modal token new
   ```

3. Set up R2 credentials in Modal secrets:
   
   The project uses the `kaiber-secrets` Modal secret (same as other projects).
   Ensure it contains:
   - `R2_ACCESS_KEY_ID`: Your Cloudflare R2 access key ID
   - `R2_SECRET_ACCESS_KEY`: Your Cloudflare R2 secret access key
   - `R2_ENDPOINT_URL`: R2 endpoint URL (optional, has default)
   - `R2_BUCKET_NAME`: R2 bucket name (optional, has default)
   - `R2_PUBLIC_CDN_URL`: Public CDN URL for serving files (optional, has default)

### Deploy

```bash
modal deploy modal_video_scenes.py
```

After deployment, you'll get a URL like:
```
https://your-username--clip-video-fastapi-app.modal.run
```

### Test Locally

```bash
# Test scene splitting
modal run modal_video_scenes.py --video-url "https://example.com/video.mp4" --threshold auto
```

## API Endpoints

### POST `/start`

Split a video into scene-based clips.

**Request:**
```json
{
  "url": "https://example.com/video.mp4",
  "threshold": "auto",
  "min_scene_ms": 3000,
  "include_audio": true,
  "mode": "fast"
}
```

**Response:**
```json
{
  "workflow_name": "clip-video-split",
  "job_id": "call-xxxxx",
  "start_time": "2025-01-17T12:34:56.789123"
}
```

**Note:** The `job_id` returned is the Modal call_id, which should be used for status checking.

**Parameters:**
- `url` (string, required): Direct HTTP(S) URL to video file
- `threshold` (string, optional): Scene detection threshold - `"auto"` or a numeric value (default: auto-select)
- `min_scene_ms` (int, optional): Minimum scene duration in milliseconds (default: 3000)
- `include_audio` (bool, optional): Include audio in clips (default: true)
- `mode` (string, optional): `"fast"` or `"precision"` (default: `"fast"`)

### GET `/status/{job_id}`

Check the status of a processing job using the `job_id` from the start response.

**Response (Success):**
```json
{
  "status": "done",
  "result": {
    "job_id": "20250117_abc12345",
    "usedThreshold": 27.5,
    "clips": [
      {
        "startMs": 0,
        "endMs": 3000,
        "durationMs": 3000,
        "fileName": "video-scene-000.mp4",
        "sizeBytes": 1234567,
        "storageProvider": "r2",
        "gsUri": "r2://bucket-name/video-clips/20250117_abc12345/video-scene-000.mp4",
        "objectName": "video-clips/20250117_abc12345/video-scene-000.mp4",
        "publicUrl": "https://media.kybercorp.org/video-clips/20250117_abc12345/video-scene-000.mp4",
        "mimeType": "video/mp4",
        "downloadUrl": "/clips/20250117_abc12345/video-scene-000.mp4"
      }
    ]
  }
}
```

**Note:** Clips are served via the public CDN URL returned in the `publicUrl` field. No separate download endpoint is needed - use the `publicUrl` directly.

## Usage Examples

### JavaScript/TypeScript

```javascript
// Start a scene splitting job
async function splitVideo(videoUrl) {
  const response = await fetch(`${MODAL_API_URL}/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      url: videoUrl,
      threshold: "auto",
      min_scene_ms: 3000,
      include_audio: true,
      mode: "fast"
    })
  });
  
  const { job_id } = await response.json();
  
  // Poll for completion
  let status = "pending";
  while (status === "pending") {
    await new Promise(resolve => setTimeout(resolve, 3000));
    const statusResponse = await fetch(`${MODAL_API_URL}/status/${job_id}`);
    const data = await statusResponse.json();
    status = data.status;
    
    if (status === "done") {
      console.log(`Found ${data.result.clips.length} scenes`);
      // Access clips via public CDN URLs
      for (const clip of data.result.clips) {
        console.log(`Clip URL: ${clip.publicUrl}`);
      }
    }
  }
}
```

### Python

```python
import modal
import requests
import time

# Get the Modal function
VideoScenes = modal.Cls.lookup("clip-video", "VideoScenes")

# Create instance and process
detector = VideoScenes()
job_id = "test_123"  # Generate your own job_id
result = detector.split_video_on_scenes.remote(
    video_url="https://example.com/video.mp4",
    job_id=job_id,
    threshold="auto",
    min_scene_ms=3000,
    include_audio=True,
    mode="fast"
)

print(f"Detected {len(result['clips'])} scenes")
print(f"Used threshold: {result['usedThreshold']}")
```

### curl

```bash
# Start split job
curl -X POST "${MODAL_API_URL}/start" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/video.mp4",
    "threshold": "auto",
    "min_scene_ms": 3000,
    "include_audio": true,
    "mode": "fast"
  }'

# Check status (use job_id from response)
curl "${MODAL_API_URL}/status/JOB_ID"

# Download clip (use publicUrl from status result)
curl "https://media.kybercorp.org/video-clips/JOB_ID/video-scene-000.mp4" -o clip.mp4
```

## Configuration

### Machine Resources

The Modal class is configured with:
- **CPU**: 8 cores (good for scene detection and encoding)
- **Timeout**: 1 hour per job
- **Scaledown**: 5 minutes of idle time before container shutdown

### Storage

Clips are stored in Cloudflare R2 (same pattern as modal-gen-media-thumbnails):
- Clips are uploaded to R2 at `video-clips/{job_id}/{filename}.mp4`
- Public URLs are generated using the CDN: `{R2_PUBLIC_CDN_URL}/video-clips/{job_id}/{filename}.mp4`
- Files are accessible via the `publicUrl` field in the response

### Video Download Limits

- **Max file size**: 500MB (524288000 bytes)
- **Download timeout**: 300 seconds
- **Retry attempts**: 5 with exponential backoff

## Export Modes

### Fast Mode (default)

- Uses keyframe-aligned stream copy
- Very fast processing
- Cuts may be slightly before/after requested times (aligned to nearest keyframe)
- No re-encoding, preserves original quality
- Best for: Quick previews, rough cuts, minimizing processing time

### Precision Mode

- Re-encodes segments for exact frame-accurate cuts
- Slower processing (CPU-intensive)
- Exact start/end times as specified
- Slight quality loss from re-encoding
- Best for: Final exports, exact timing requirements

## Scene Detection

The scene detection uses PySceneDetect's ContentDetector:

1. **Auto threshold selection** (default):
   - Analyzes video content statistics
   - Selects optimal threshold from upper percentiles
   - Ensures minimum scene duration is respected
   - Recommended for most videos

2. **Manual threshold**:
   - Provide a numeric value (e.g., `"27.5"`)
   - Lower values = more sensitive (more scenes)
   - Higher values = less sensitive (fewer scenes)
   - Typical range: 20-35

## Performance

Processing time depends on:
- Video length and resolution
- Number of scenes/clips
- Export mode (fast vs precision)
- Video codec complexity

**Typical performance** (8-core CPU):
- 1-minute video, fast mode: ~5-10 seconds
- 5-minute video, fast mode: ~20-40 seconds
- 1-minute video, precision mode: ~30-60 seconds
- 5-minute video, precision mode: ~3-5 minutes

## Troubleshooting

### Common Issues

1. **Download failures**:
   - Verify video URL is accessible
   - Check file size is under 500MB
   - Ensure URL returns video content (not HTML)

2. **No scenes detected**:
   - Try a lower threshold value
   - Reduce `min_scene_ms` if scenes are too short
   - Verify video has clear scene changes

3. **Slow processing**:
   - Normal for long videos or precision mode
   - Consider splitting long videos into chunks
   - Use fast mode if exact timing isn't critical

4. **Import errors**:
   - Ensure all dependencies are installed in Modal image
   - Check that module files are correctly copied
   - Verify Python version compatibility (3.12)

## License

This project is extracted from the mirror-api codebase.

## Support

For issues or questions, refer to Modal's documentation at https://modal.com/docs.
