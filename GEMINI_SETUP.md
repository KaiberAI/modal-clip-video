# Gemini Video Processing Setup Guide

This guide will help you set up and test the new Gemini video processing function.

## Prerequisites

1. **Modal Account**: Sign up at https://modal.com
2. **Gemini API Key**: Get your API key from https://aistudio.google.com/app/apikey

## Setup Steps

### 1. Install Modal CLI

```bash
pip install modal
```

### 2. Authenticate with Modal

```bash
modal token new
```

Follow the prompts to authenticate.

### 3. Create Modal Secret

Create a secret named `gemini-secret` with your Gemini API key:

```bash
modal secret create gemini-secret GOOGLE_GEMINI_API_KEY=your-api-key-here
```

Replace `your-api-key-here` with your actual Gemini API key.

### 4. Deploy the Modal App

```bash
modal deploy modal_video_scenes.py
```

This will build the container image with FFmpeg and deploy your app.

## Testing

### Option 1: Direct Function Call (Recommended for Testing)

Test the Gemini processing function directly with a sample video:

```bash
modal run test_gemini_processing.py
```

This uses the Big Buck Bunny sample video by default.

To test with your own video:

```bash
modal run test_gemini_processing.py --video-url "https://example.com/your-video.mp4"
```

### Option 2: Via HTTP Endpoint

First, get your Modal app URL:

```bash
modal app list
```

Then make a POST request:

```bash
curl -X POST https://kaiber-ai--clip-video-fastapi-app.modal.run/start \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/video.mp4"}'
```

## Viewing Logs

To see the processing logs in real-time:

```bash
modal logs clip-video
```

The logs will show:
- FFmpeg transcode progress
- Video upload to Gemini File API
- Polling status until ACTIVE
- Gemini analysis response
- **Final JSON timestamps** (clearly marked in logs)

## Expected Output

The function returns JSON with scene timestamps:

```json
[
  {
    "start_time": 0.0,
    "end_time": 5.5,
    "title": "Opening Scene",
    "description": "Introduction with title card"
  },
  {
    "start_time": 5.5,
    "end_time": 12.3,
    "title": "Main Content",
    "description": "Person speaking to camera"
  }
]
```

## Troubleshooting

### "GOOGLE_GEMINI_API_KEY not found in environment"

Make sure you created the Modal secret correctly:

```bash
modal secret list
```

You should see `gemini-secret` in the list. If not, create it:

```bash
modal secret create gemini-secret GOOGLE_GEMINI_API_KEY=your-key
```

### FFmpeg Error

The function will print FFmpeg errors to logs. Common issues:
- Invalid video URL (not accessible)
- Unsupported video format (rare, most formats work)
- Network timeout (try a smaller video)

### Gemini File API Timeout

If the file doesn't become ACTIVE within 10 minutes, the function will timeout. This can happen with very large videos. Consider:
- Using shorter videos for testing
- Increasing the `max_wait_time` in the code

### Rate Limits

Gemini API has rate limits. If you hit them:
- Wait a few minutes
- Check your quota at https://aistudio.google.com
- Consider upgrading your Gemini API tier

## What's Happening Under the Hood

1. **FFmpeg Transcoding**: The video is downloaded and transcoded to 720p H.264 with AAC audio
2. **Streaming to Gemini**: The transcoded video is streamed directly to Gemini File API (no local storage)
3. **Polling**: The function polls every 5 seconds until Gemini finishes processing the video
4. **AI Analysis**: Gemini's `gemini-2.5-flash-lite` model analyzes the video and identifies scene boundaries
5. **JSON Extraction**: The response is parsed and cleaned to extract pure JSON
6. **Cleanup**: The uploaded file is deleted from Gemini to save quota

## Performance Notes

- Transcoding time depends on video length and resolution (typically 0.5-2x realtime)
- Gemini processing typically takes 1-5 minutes for videos under 10 minutes
- Total processing time: usually 2-10 minutes for typical videos
- The function has a 1-hour timeout to handle long videos

## Cost Considerations

- **Modal**: Charged for compute time (FFmpeg transcoding + waiting)
- **Gemini**: Charged per video minute processed
- Both are typically very affordable for testing (<$0.10 per video)

