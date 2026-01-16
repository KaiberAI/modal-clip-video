# Modal Clip Video API Interface

This document defines the interface requirements for the Modal service that handles video clipping functionality.

## Base URL

```
https://kaiber-ai--clip-video-fastapi-app.modal.run
```

---

## Endpoints

### 1. Start Clip Video Job

Initiates a new video clipping job.

**Endpoint:** `POST /start`

**Request Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
  "url": "https://example.com/video.mp4",
  "width": 1280,
  "height": 720
}
```

**Request Schema:**
| Field   | Type   | Required | Description                          |
|---------|--------|----------|--------------------------------------|
| `url`   | string | Yes      | The URL of the video to clip         |
| `width` | number | Yes      | Video width in pixels                |
| `height`| number | Yes      | Video height in pixels               |

**Response (Success - 200 OK):**
```json
{
  "job_id": "uuid-string"
}
```

**Response Schema:**
| Field    | Type   | Required | Description                        |
|----------|--------|----------|------------------------------------|
| `job_id` | string | Yes      | Unique identifier for the clipping job |

**Error Response:**
- Returns non-200 status code on failure
- Error details should be included in response body

---

### 2. Get Clip Video Job Status

Retrieves the current status of a clipping job.

**Endpoint:** `GET /status/{job_id}`

**Path Parameters:**
| Parameter | Type   | Required | Description                          |
|-----------|--------|----------|--------------------------------------|
| `job_id`  | string | Yes      | The job ID returned from `/start`    |

**Request Headers:**
```
Content-Type: application/json
```

**Response (Processing/Queued):**
```json
{
  "status": "processing",
  "progress": 0.45
}
```

**Response Schema (Processing):**
| Field      | Type   | Required | Description                                    |
|------------|--------|----------|------------------------------------------------|
| `status`   | string | Yes      | Must be "processing" or "queued"               |
| `progress` | number | No       | Progress value (0.0-1.0), defaults to 0.1      |

---

**Response (Completed):**
```json
{
  "status": "completed",
  "scenes": [
    {
      "key": "user-videos/temp/hash1.mp4",
      "length": 5.5,
      "width": 1280,
      "height": 720,
      "url": "https://..."
    },
    {
      "key": "user-videos/temp/hash2.mp4",
      "length": 3.2,
      "width": 1280,
      "height": 720,
      "url": "https://..."
    }
  ],
  "progress": 1.0
}
```

**Response Schema (Completed):**
| Field      | Type   | Required | Description                                  |
|------------|--------|----------|----------------------------------------------|
| `status`   | string | Yes      | Must be "completed"                          |
| `scenes`   | array  | Yes      | Array of clipped video scenes (can be empty) |
| `progress` | number | No       | Progress value (1.0 for completed)           |

**Scene Object Schema:**
| Field      | Type   | Required | Description                                      |
|------------|--------|----------|--------------------------------------------------|
| `key`      | string | No       | R2 storage key for the clip                      |
| `length`   | number | No       | Duration of clip in seconds                      |
| `width`    | number | No       | Video width in pixels                            |
| `height`   | number | No       | Video height in pixels                           |
| `url`      | string | No       | Presigned R2 URL (valid for 24 hours)            |

---

**Response (Failed):**
```json
{
  "status": "failed",
  "error": "Error message describing what went wrong"
}
```

**Response Schema (Failed):**
| Field    | Type   | Required | Description                                |
|----------|--------|----------|--------------------------------------------|
| `status` | string | Yes      | Must be "failed"                           |
| `error`  | string | No       | Error message, defaults to "Unknown error" |

---

## Status Flow

1. **Job Created** → Status: `"processing"` with `progress: 0.1`
2. **Job In Progress** → Status: `"processing"` with `progress` (0.0-1.0, e.g., 0.45)
3. **Job Complete** → Status: `"completed"` with `scenes` array and `progress: 1.0`
4. **Job Failed** → Status: `"failed"` with `error` message

---

## Integration Notes

### Server-Side Behavior (ClipVideoService)

The Kaiber server (`ClipVideoService.ts`) performs the following transformations:

1. **On Start:**
   - Sends `{ url: source, width: ..., height: ... }` to Modal `/start`
   - Receives `{ job_id }` from Modal
   - Returns `{ jobId, createdAt }` to client

2. **On Status Check:**
   - Sends GET to Modal `/status/{jobId}`
   - Transforms Modal response to internal schema
   - **On Completed:** Creates MongoDB Media documents for each scene
   - Returns standardized response to client

3. **Media Creation:**
   - Each scene becomes a `Media` document with:
     - New `mediaId` (UUID v4)
     - `type: MediaType.Video`
     - `path.key: scene.url` or `scene.video_url`
     - Thumbnail generation via `genThumbnailForMedia`

### Error Handling

- Non-200 responses throw `InternalServerErrorException`
- Missing scenes array defaults to empty array `[]`
- Missing optional fields use sensible defaults:
  - `progress`: defaults to `0`
  - `error`: defaults to `"Unknown error"`

---

## Gemini Video Processing (NEW)

### Overview

The Modal app now includes a `process_video_with_gemini` function that:
1. Takes a video URL, width, and height
2. Streams the video at 1 FPS with scale 1280 using FFmpeg directly to Google's Gemini File API
3. Downloads the high-resolution video in parallel for clipping
4. Polls until the Gemini file is ACTIVE
5. Uses `gemini-3-flash-preview` to analyze the video and extract scene timestamps
6. Validates and filters scenes (minimum 2 seconds, trims 0.2s from start and 0.5s from end)
7. Clips scenes in parallel using fan-out workers that upload to R2
8. Returns structured JSON with scene boundaries, descriptions, and R2 URLs

### Direct Function Call

You can call the Gemini processing function directly using Modal CLI (note: requires url, width, and height parameters):

### Using with /start Endpoint

```bash
curl -X POST https://kaiber-ai--clip-video-fastapi-app.modal.run/start \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/video.mp4", "width": 1280, "height": 720}'
```

### Output Format

The Gemini processing function returns JSON with scene data:

```json
[
  {
    "start_time": 0.0,
    "end_time": 5.5,
    "url": "https://...",
    "key": "user-videos/temp/hash.mp4",
    "filename": "hash.mp4",
    "width": 1280,
    "height": 720,
    "length": 5.5,
    "description": "Brief scene description"
  }
]
```

### Requirements

1. **FFmpeg**: Automatically installed in the Modal container image

2. **Google Generative AI SDK**: Automatically installed via requirements.txt

### Logs

All processing steps are logged to Modal logs:
- FFmpeg stream progress (1 FPS with scale 1280)
- High-resolution video download progress
- Gemini File API upload status
- Polling for ACTIVE state
- Scene validation and filtering
- Parallel clip processing and R2 uploads
- Final scene data (printed to logs)

Access logs via Modal dashboard or CLI:
```bash
modal logs clip-video
```

---

## Testing

### Example: Start a Job

```bash
curl -X POST https://kaiber-ai--clip-video-fastapi-app.modal.run/start \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/video.mp4", "width": 1280, "height": 720}'
```

### Example: Check Status

```bash
curl -X GET https://kaiber-ai--clip-video-fastapi-app.modal.run/status/{job_id} \
  -H "Content-Type: application/json"
```
