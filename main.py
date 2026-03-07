"""
Video Downloader API — main.py
FastAPI + yt-dlp backend for downloading videos/audio from social media platforms.
"""

import asyncio
import logging
import os
import re
import subprocess
import sys
import tempfile
from typing import Optional
import mimetypes
import json
import httpx
from urllib.parse import quote, urlparse, parse_qs

import yt_dlp
import uuid
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("viddl")

# ─── Downloader Binaries ───────────────────────────────────────────────────
FFMPEG_EXE: Optional[str] = None
IS_KOYEB: bool = os.path.exists("/app") or "KOYEB" in os.environ
VIDSSAVE_AUTH: str = "20250901majwlqo"
VIDSSAVE_API: str = "https://api.vidssave.com/api/contentsite_api/media/parse"


def _init_binaries() -> bool:
    """Detect ffmpeg binaries and set their paths."""
    global FFMPEG_EXE

    # 1. FFmpeg detection
    found_ffmpeg = False
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=3, check=True)
        FFMPEG_EXE = "ffmpeg"
        logger.info("Using system ffmpeg")
        found_ffmpeg = True
    except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired):
        # 2. Try imageio-ffmpeg bundled binary
        try:
            import imageio_ffmpeg
            exe = imageio_ffmpeg.get_ffmpeg_exe()
            if os.path.exists(exe):
                FFMPEG_EXE = exe
                logger.info("Found bundled ffmpeg: %s", exe)
                ffmpeg_dir = os.path.dirname(exe)
                os.environ["PATH"] = ffmpeg_dir + os.pathsep + os.environ.get("PATH", "")
                found_ffmpeg = True
        except (ImportError, Exception):
            pass
    
    logger.info(f"Binaries initialized: FFmpeg={FFMPEG_EXE}")
    return found_ffmpeg

FFMPEG_AVAILABLE: bool = _init_binaries()


# ─── App Setup ────────────────────────────────────────────────────────────────
app = FastAPI(title="VidDL", version="1.0.0", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the static frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


# ─── Request / Response Models ────────────────────────────────────────────────
class FetchRequest(BaseModel):
    url: str

class DownloadJobRequest(BaseModel):
    url: str
    format_id: str
    ext: str
    title: Optional[str] = None
    download_url: Optional[str] = None
    resource_content: Optional[str] = None

# ─── Job Registry ─────────────────────────────────────────────────────────────
# Stores job state: { job_id: { status, progress, title, ext, path, error } }
JOBS: dict[str, dict] = {}


# FFmpeg detection already performed above
logger.info("FFmpeg available: %s", FFMPEG_AVAILABLE)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_youtube_id(url: str) -> Optional[str]:
    """Extract video ID from a YouTube URL."""
    parsed = urlparse(url)
    if parsed.hostname in ('youtu.be', 'www.youtu.be'):
        return parsed.path[1:]
    if parsed.hostname in ('youtube.com', 'www.youtube.com'):
        if parsed.path == '/watch':
            return parse_qs(parsed.query).get('v', [None])[0]
        if parsed.path.startswith(('/embed/', '/v/', '/shorts/')):
            return parsed.path.split('/')[2]
    return None

def _sanitize_filename(name: str) -> str:
    """Remove characters that are illegal in filenames."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def _use_vidssave(url: str) -> bool:
    """Return True if the URL should be handled by Vidssave API."""
    u = url.lower()
    vid_sources = [
        "youtube.com", "youtu.be",
        "instagram.com", "instagr.am", "ig.me",
        "facebook.com", "fb.com", "fb.watch",
        "tiktok.com", "x.com", "twitter.com"
    ]
    return any(x in u for x in vid_sources)

async def _resolve_vidssave_stream_url(resource_content: str) -> Optional[str]:
    """Perform the 2-step media/download -> SSE query for direct URL generation."""
    headers = {
        "Referer": "https://vidssave.com/",
        "Origin": "https://vidssave.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Step 1: Request task_id
        dl_api = "https://api.vidssave.com/api/contentsite_api/media/download"
        data = {
            "auth": VIDSSAVE_AUTH,
            "domain": "api-ak.vidssave.com",
            "request": resource_content,
            "no_encrypt": "1"
        }
        try:
            resp_dl = await client.post(dl_api, data=data, headers=headers)
            if resp_dl.status_code != 200:
                logger.error(f"Vidssave media/download error: {resp_dl.status_code}")
                return None
                
            task_id = resp_dl.json().get("data", {}).get("task_id")
            if not task_id:
                logger.error("Vidssave media/download returned no task_id")
                return None
                
            # Step 2: Poll via SSE
            sse_url = f"https://api.vidssave.com/sse/contentsite_api/media/download_query"
            sse_params = {"auth": VIDSSAVE_AUTH, "task_id": task_id}
            
            logger.info(f"Polling Vidssave SSE for task: {task_id[:10]}...")
            async with client.stream("GET", sse_url, params=sse_params, headers=headers, timeout=60.0) as sse_resp:
                async for line in sse_resp.aiter_lines():
                    if "data:" in line:
                        data_str = line.replace("data:", "").strip()
                        if not data_str: continue
                        try:
                            # Vidssave sometimes returns raw JSON or prefixed data
                            evt_data = json.loads(data_str)
                            # Some responses might have a 'data' wrap
                            inner = evt_data.get("data") if isinstance(evt_data.get("data"), dict) else evt_data
                            
                            # 'status': 'success' is the event we want
                            if evt_data.get("status") == "success" or evt_data.get("event") == "success":
                                final_url = inner.get("url") or inner.get("link")
                                if final_url:
                                    logger.info(f"Vidssave SSE SUCCESS. URL generated.")
                                    return final_url
                            
                            if evt_data.get("status") == "failed":
                                logger.error(f"Vidssave SSE reported failure: {evt_data.get('message')}")
                                return None
                        except Exception as parse_err:
                            # If it's not JSON, might be status text
                            if "success" in data_str.lower() and "http" in data_str:
                                # regex fallback for raw string
                                match = re.search(r'(https?://[^\s",}]+)', data_str)
                                if match: return match.group(1)
            
            return None
        except Exception as e:
            logger.error(f"Vidssave URL resolution workflow failed: {e}")
            return None

def _get_vidssave_platform(url: str) -> str:
    u = url.lower()
    if "youtube.com" in u or "youtu.be" in u: return "youtube"
    if any(x in u for x in ["instagram.com", "instagr.am", "ig.me"]): return "instagram"
    if any(x in u for x in ["facebook.com", "fb.com", "fb.watch"]): return "facebook"
    if "tiktok.com" in u: return "tiktok"
    if "x.com" in u or "twitter.com" in u: return "x"
    return "social"


async def _fetch_vidssave_metadata(url: str) -> Optional[dict]:
    """Fetch metadata from vidssave.com API with retry for source origin."""
    headers = {
        "Referer": "https://vidssave.com/",
        "Origin": "https://vidssave.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    
    async with httpx.AsyncClient(timeout=25.0) as client:
        # Try 'cache' first, then 'source' if no resources found
        for origin in ["cache", "source"]:
            try:
                data = {
                    "auth": VIDSSAVE_AUTH,
                    "domain": "api-ak.vidssave.com",
                    "origin": origin,
                    "link": url
                }
                logger.info(f"Vidssave fetch attempt with origin: {origin}")
                resp = await client.post(VIDSSAVE_API, data=data, headers=headers)
                
                if resp.status_code != 200:
                    continue
                    
                res_json = resp.json()
                if res_json.get("status") != 1:
                    continue
                    
                d = res_json.get("data", {})
                resources = d.get("resources", [])
                
                if not resources and origin == "cache":
                    logger.info("Vidssave cache miss, retrying with source...")
                    continue
                
                if not resources:
                    return None

                formats = []
                for res in resources:
                    # Check multiple possible keys for the direct URL
                    durl = res.get("download_url") or res.get("url") or res.get("link")
                    if not durl:
                        continue
                        
                    q = res.get("quality", "Unknown")
                    f = res.get("format", "mp4").lower()
                    size = res.get("size")
                    
                    formats.append({
                        "format_id": f"vidssave_{res.get('resource_id')}",
                        "label": f"{q} ({f.upper()}) [Source 2]",
                        "ext": f,
                        "type": res.get("type", "video"),
                        "download_url": durl,
                        "resource_content": res.get("resource_content"),
                        "filesize_approx": size
                    })
                
                if not formats and origin == "cache":
                    logger.info("Vidssave cache has resources but no valid download URLs, retrying with source...")
                    continue
                
                if not formats:
                    return None
                
                platform = _get_vidssave_platform(url)
                return {
                    "type": "video",
                    "title": d.get("title", f"{platform.capitalize()} video"),
                    "thumbnail": d.get("thumbnail"),
                    "duration": d.get("duration"),
                    "channel": platform.capitalize(),
                    "platform": f"{platform} (vidssave)",
                    "webpage_url": url,
                    "formats": formats
                }
            except Exception as e:
                logger.error(f"Vidssave attempt ({origin}) failed: {e}")
                if origin == "source":
                    return None
    return None




def _build_ydl_opts(extra: dict | None = None) -> dict:
    """Base yt-dlp options shared across all extractions."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": False,
        "extractor_args": {
            # TikTok: fetch from the CDN that serves watermark-free streams
            "tiktok": {
                "api_hostname": ["api22-normal-c-useast2a.tiktokv.com"],
                "app_version": ["20.9.3"],
            },
            # YouTube: Bypass bot detection using PO Token and Visitor Data if provided
            "youtube": {
                "player_client": ["android", "ios", "web", "mweb"],
                "po_token": [f"web+{os.environ.get('YOUTUBE_PO_TOKEN', '')}"] if os.environ.get('YOUTUBE_PO_TOKEN') else [],
                "visitor_data": [os.environ.get('YOUTUBE_VISITOR_DATA', '')] if os.environ.get('YOUTUBE_VISITOR_DATA') else [],
            }
        },
        # Prefer HTTP(S) sources, skip DRM-protected formats
        "format_sort": ["res", "ext:mp4:m4a", "br", "asr"],
        "nocheckcertificate": True,
        "merge_output_format": "mp4",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    
    # YouTube bypass using environment variables
    po_token = os.environ.get('YOUTUBE_PO_TOKEN')
    visitor_data = os.environ.get('YOUTUBE_VISITOR_DATA')
    
    if po_token and visitor_data:
        # If user provided a raw token, we ensure the web+ prefix is present for the web client
        if not po_token.startswith("web+"):
            po_token = f"web+{po_token}"
            
        opts["extractor_args"]["youtube"].update({
            "po_token": [po_token],
            "visitor_data": [visitor_data],
        })
        logger.info("Using PO Token and Visitor Data for YouTube bypass")

    # Use cookies.txt if provided in root
    if os.path.exists("cookies.txt"):
        opts["cookiefile"] = "cookies.txt"
        logger.info("Using cookies.txt for authentication")
    
    if extra:
        opts.update(extra)
    return opts


def _pick_formats(info: dict) -> list[dict]:
    """
    Build a list of download options from yt-dlp format info.
    Strategy:
      - If ffmpeg IS available: show all resolution tiers (merging v+a is fine).
      - If ffmpeg is NOT available: only show pre-combined formats (vcodec+acodec present).
      - Always add a best-quality fallback.
      - Always add an Audio option (mp3 if ffmpeg available, else best native audio).
    """
    formats = info.get("formats", [])

    target_heights = [2160, 1440, 1080, 720, 480, 360, 240]
    label_map = {
        2160: "4K (2160p)",
        1440: "2K (1440p)",
        1080: "1080p HD",
        720: "720p HD",
        480: "480p",
        360: "360p",
        240: "240p",
    }

    seen_heights: set[int] = set()
    result: list[dict] = []

    # All video formats sorted by height desc, then bitrate desc
    video_formats = [
        f for f in formats
        if f.get("vcodec", "none") != "none" and f.get("height") is not None
    ]
    video_formats.sort(key=lambda f: (f.get("height", 0), f.get("tbr", 0)), reverse=True)

    # Combined formats = already have both video + audio (no ffmpeg needed to merge)
    combined_formats = [
        f for f in video_formats
        if f.get("acodec", "none") != "none"
    ]

    for h in target_heights:
        # When ffmpeg is available, consider all video formats (can merge)
        # When not available, only consider pre-combined formats
        pool = video_formats if FFMPEG_AVAILABLE else combined_formats

        candidates = [f for f in pool if h <= f.get("height", 0) < h * 1.6]
        if not candidates:
            candidates = [f for f in pool if abs(f.get("height", 0) - h) <= h * 0.25]
        if not candidates:
            continue

        best = candidates[0]
        actual_height = best.get("height", h)
        if actual_height in seen_heights:
            continue
        seen_heights.add(actual_height)

        has_audio = best.get("acodec", "none") != "none"
        if has_audio:
            # Pre-combined: use directly
            fmt_selector = best["format_id"]
        else:
            # Video-only: merge with best audio (requires ffmpeg)
            fmt_selector = f"{best['format_id']}+bestaudio[ext=m4a]/bestaudio"

        label = label_map.get(h) or f"{actual_height}p"

        result.append({
            "format_id": fmt_selector,
            "label": label,
            "ext": "mp4",
            "height": actual_height,
            "filesize_approx": best.get("filesize") or best.get("filesize_approx"),
            "vcodec": best.get("vcodec", ""),
            "acodec": best.get("acodec", ""),
            "type": "video",
            "needs_merge": not has_audio,
        })

    # Fallback: if nothing was found, add a generic best option
    if not result:
        result.append({
            "format_id": "best[ext=mp4]/best",
            "label": "Best Quality",
            "ext": "mp4",
            "height": None,
            "filesize_approx": None,
            "vcodec": "",
            "acodec": "",
            "type": "video",
            "needs_merge": False,
        })

    # Audio option
    if FFMPEG_AVAILABLE:
        # Full MP3 conversion
        result.append({
            "format_id": "bestaudio/best",
            "label": "Audio Only (MP3)",
            "ext": "mp3",
            "height": None,
            "filesize_approx": None,
            "vcodec": "none",
            "acodec": "mp3",
            "type": "audio",
        })
    else:
        # No ffmpeg: download best native audio (m4a/webm) as-is
        result.append({
            "format_id": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio",
            "label": "Audio Only (M4A)",
            "ext": "m4a",
            "height": None,
            "filesize_approx": None,
            "vcodec": "none",
            "acodec": "aac",
            "type": "audio",
        })

    return result


# ─── Routes ───────────────────────────────────────────────────────────────────


@app.get("/health", tags=["System"])
async def health_check():
    """Koyeb / load-balancer health check endpoint."""
    return {"status": "ok"}


@app.get("/", tags=["Frontend"], include_in_schema=False)
async def serve_index():
    """Redirect bare root to the static index page."""
    from fastapi.responses import FileResponse
    return FileResponse("static/index.html")


@app.post("/api/fetch", tags=["Downloader"])
async def fetch_metadata(body: FetchRequest):
    """
    Fetch video metadata (title, thumbnail, available formats) for a given URL.
    Supports YouTube, TikTok, Instagram, Facebook, Reddit, X/Twitter, and more.
    """
    url = body.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URL is required.")

    logger.info("Fetching metadata for: %s", url)
    loop = asyncio.get_event_loop()

    # ── Vidssave Exclusive Flow (YouTube, Instagram, Facebook) ────────────────
    if _use_vidssave(url):
        platform_name = _get_vidssave_platform(url).capitalize()
        logger.info("Using Vidssave API for %s metadata: %s", platform_name, url)
        try:
            vids_data = await _fetch_vidssave_metadata(url)
            if vids_data:
                return JSONResponse(vids_data)
            else:
                raise HTTPException(status_code=422, detail=f"Vidssave could not find any formats for this {platform_name} video.")
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Vidssave fetch failed: %s", e)
            raise HTTPException(status_code=422, detail=f"{platform_name} Metadata Error (Vidssave): {str(e)}")



    # ── Standard Flow (Other Platforms) ──────────────────────────────────────
    ydl_opts = _build_ydl_opts(
        {
            "skip_download": True,
            "noplaylist": False,
            "extract_flat": "in_playlist",
        }
    )
    async def _extract_with_retry():
        return await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(url, download=False))

    
    try:
        info = await _extract_with_retry()
    except Exception as e:
        logger.error("yt-dlp error: %s", e)
        raise HTTPException(status_code=422, detail=f"Could not process URL: {str(e)}")


    # ── Playlist ──────────────────────────────────────────────────────────────
    if info.get("_type") == "playlist":
        entries = info.get("entries", [])
        playlist_items = []
        for entry in entries[:50]:  # Limit to 50 items per fetch
            if not entry:
                continue
            
            # Robust thumbnail selection for playlist items
            item_thumb = entry.get("thumbnail")
            if not item_thumb:
                entry_thumbs = entry.get("thumbnails")
                if entry_thumbs:
                    # Filter for those with URLs and pick the last one (usually highest res)
                    valid_thumbs = [t for t in entry_thumbs if t.get("url")]
                    if valid_thumbs:
                        item_thumb = valid_thumbs[-1]["url"]

            playlist_items.append(
                {
                    "id": entry.get("id", ""),
                    "title": entry.get("title", "Untitled"),
                    "url": entry.get("url") or entry.get("webpage_url") or entry.get("id"),
                    "thumbnail": item_thumb,
                    "duration": entry.get("duration"),
                }
            )

        return JSONResponse(
            {
                "type": "playlist",
                "title": info.get("title", "Playlist"),
                "thumbnail": info.get("thumbnail"),
                "channel": info.get("uploader") or info.get("channel"),
                "item_count": len(entries),
                "items": playlist_items,
            }
        )

    # ── Single video ──────────────────────────────────────────────────────────
    # Re-extract with full format info if we only got a flat entry
    if "formats" not in info:
        full_opts = _build_ydl_opts({"skip_download": True})
        actual_url = info.get("webpage_url") or url
        try:
            info = await loop.run_in_executor(
                None, lambda: yt_dlp.YoutubeDL(full_opts).extract_info(actual_url, download=False)
            )
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Could not retrieve format list: {str(e)}")

    formats = _pick_formats(info)

        

    # Pick best thumbnail
    thumbnails = info.get("thumbnails") or []
    thumbnail = info.get("thumbnail")
    if thumbnails:
        # Prefer the largest thumbnail
        sorted_thumbs = sorted(
            [t for t in thumbnails if t.get("url")],
            key=lambda t: (t.get("width", 0) or 0),
            reverse=True,
        )
        if sorted_thumbs:
            thumbnail = sorted_thumbs[0]["url"]

    return JSONResponse(
        {
            "type": "video",
            "title": info.get("title", "Unknown Title"),
            "thumbnail": thumbnail,
            "duration": info.get("duration"),
            "channel": info.get("uploader") or info.get("channel") or info.get("creator"),
            "platform": info.get("extractor_key", "").lower(),
            "webpage_url": info.get("webpage_url", url),
            "formats": formats,
        }
    )


@app.post("/api/download/request", tags=["Downloader"])
async def request_download(body: DownloadJobRequest, background_tasks: BackgroundTasks):
    """
    Create a background download job and return a Job ID.
    Follows the downbot.app architecture for better UX.
    """
    job_id = str(uuid.uuid4())
    safe_title = _sanitize_filename(body.title or "download")
    
    JOBS[job_id] = {
        "status": "queued",
        "progress": 0,
        "title": safe_title,
        "ext": body.ext,
        "path": None,
        "error": None,
        "created_at": asyncio.get_event_loop().time()
    }
    
    background_tasks.add_task(_run_download_job, job_id, body)
    return {"jobId": job_id}


@app.get("/api/download/status/{job_id}", tags=["Downloader"])
async def get_job_status(job_id: str):
    """Check the status and progress of a background job."""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "jobId": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "error": job["error"],
        "metadata": {
            "title": job["title"],
            "ext": job["ext"]
        }
    }


@app.get("/api/download/file/{job_id}", tags=["Downloader"])
async def get_job_file(job_id: str):
    """Download the completed file for a given job ID."""
    job = JOBS.get(job_id)
    if not job or job["status"] != "completed":
        raise HTTPException(status_code=404, detail="File not ready or job not found")
    
    path = job["path"]
    if not path or not os.path.exists(path):
         raise HTTPException(status_code=404, detail="File lost or deleted")

    # Use a generic but safe filename for the response. 
    # FastAPI's FileResponse internally handles UTF-8 filenames correctly 
    # when the 'filename' parameter is used.
    ext = job.get("ext") or "mp4"
    filename = f"{job['title']}.{ext}"
    
    # We MUST ensure the media_type is exactly what matches the extension
    mime_type, _ = mimetypes.guess_type(path)
    if not mime_type:
        mime_type = "video/mp4" if ext == "mp4" else "application/octet-stream"

    return FileResponse(
        path, 
        media_type=mime_type,
        filename=filename  # Let FastAPI handle Content-Disposition automatically
    )


async def _run_download_job(job_id: str, body: DownloadJobRequest):
    """Internal worker to execute yt-dlp in the background with progress tracking."""
    job = JOBS[job_id]
    job["status"] = "processing"
    
    suffix = f".{body.ext}"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp_path = tmp.name
    tmp.close()
    
    if body.format_id.startswith("vidssave_"):
        d_url = body.download_url
        
        # If no direct URL, try resolving from resource_content (Facebook/IG flow)
        if not d_url and body.resource_content:
            logger.info(f"Job {job_id} requires SSE resolution. Resolving...")
            job["status"] = "generating direct link"
            d_url = await _resolve_vidssave_stream_url(body.resource_content)
        
        if d_url:
            await _run_direct_download(job_id, d_url, tmp_path)
            return
        else:
            logger.error(f"Vidssave job {job_id} missing final download link. Body: %s", body.model_dump())
            job["status"] = "failed"
            job["error"] = "Vidssave source URL generation failed. Please try again."
            return




    def _progress_hook(d):
        if d['status'] == 'downloading':
            p = d.get('_percent_str', '0%').replace('%','')
            try:
                job["progress"] = float(p)
                job["status"] = "downloading"
            except:
                pass
        elif d['status'] == 'finished':
            job["progress"] = 100
            job["status"] = "merging"

    ydl_opts = _build_ydl_opts({
        "format": body.format_id,
        "outtmpl": tmp_path,
        "progress_hooks": [_progress_hook],
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
    })
    
    if FFMPEG_EXE:
        ydl_opts["ffmpeg_location"] = FFMPEG_EXE
    
    if body.ext == "mp3":
        ydl_opts.update({
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }]
        })

    loop = asyncio.get_event_loop()
    try:
        def _exec():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([body.url])
        
        await loop.run_in_executor(None, _exec)
        
        
        actual_path = _resolve_output_path(tmp_path, suffix)
        if os.path.exists(actual_path):
            # If requested MP4 but got something else, FORCE conversion
            if body.ext == "mp4" and not actual_path.endswith(".mp4") and FFMPEG_EXE:
                job["status"] = "converting to mp4"
                mp4_path = actual_path.rsplit('.', 1)[0] + ".mp4_final"
                cmd = [FFMPEG_EXE, "-i", actual_path, "-c:v", "copy", "-c:a", "aac", "-y", mp4_path]
                subprocess.run(cmd, capture_output=True)
                if os.path.exists(mp4_path):
                    _cleanup(actual_path)
                    actual_path = mp4_path.replace(".mp4_final", ".mp4")
                    os.replace(mp4_path, actual_path)

            job["path"] = actual_path
            job["status"] = "completed"
            job["progress"] = 100
            
            # Ensure extension logic is robust
            detected_ext = os.path.splitext(actual_path)[1].lstrip('.')
            job["ext"] = detected_ext if detected_ext else body.ext
            
            logger.info(f"Job {job_id} completed: {actual_path}")
        else:
            job["status"] = "failed"
            job["error"] = "Output file resolution failed"
            
    except Exception as e:
        logger.exception(f"Job {job_id} failed")
        job["status"] = "failed"
        job["error"] = str(e)
        _cleanup(tmp_path)





# ─── Legacy Route (Optional) ──────────────────────────────────────────────────
@app.get("/api/download", tags=["Downloader"], include_in_schema=False)
async def download_video_legacy(
    url: str = Query(..., description="Video page URL"),
    format_id: str = Query(..., description="yt-dlp format selector"),
    ext: str = Query("mp4", description="Output container extension"),
    title: Optional[str] = Query(None, description="Suggested filename"),
):
    """
    Download video/audio to a temp file via yt-dlp (blocking subprocess in thread pool),
    then stream the completed file back to the browser and delete it.
    This approach works on all platforms including Windows where asyncio subprocesses
    are not fully supported.
    """
    if not url:
        raise HTTPException(status_code=400, detail="url is required.")

    safe_title = _sanitize_filename(title or "download")
    is_audio = ext in ("mp3", "m4a")
    suffix = f".{ext}"
    filename = f"{safe_title}{suffix}"

    loop = asyncio.get_event_loop()

    # ── Build command ──────────────────────────────────────────────────────────
    # We always write to a temp file — avoids all pipe/seeking issues on any OS.
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp_path = tmp.name
    tmp.close()

    if ext == "mp3":
        # MP3 requires ffmpeg (guaranteed available when ext=mp3 is returned by fetch)
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--format", format_id,
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            "--extract-audio",
            "--audio-format", "mp3",
            "--audio-quality", "0",
            "--extractor-args",
            "tiktok:api_hostname=api22-normal-c-useast2a.tiktokv.com,app_version=20.9.3",
            "-o", tmp_path,
            url,
        ]
        if FFMPEG_EXE:
            cmd.extend(["--ffmpeg-location", FFMPEG_EXE])
        media_type = "audio/mpeg"
    elif ext == "m4a":
        # M4A: download best audio natively, no conversion needed
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--format", format_id,
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            "--extractor-args",
            "tiktok:api_hostname=api22-normal-c-useast2a.tiktokv.com,app_version=20.9.3",
            "-o", tmp_path,
            url,
        ]
        if FFMPEG_EXE:
            cmd.extend(["--ffmpeg-location", FFMPEG_EXE])
        media_type = "audio/mp4"
    else:
        # Video: download and merge into mp4 (ffmpeg merges if format has +)
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--format", format_id,
            "--no-playlist",
            "--quiet",
            "--no-warnings",
            "--merge-output-format", "mp4",
            "--extractor-args",
            "tiktok:api_hostname=api22-normal-c-useast2a.tiktokv.com,app_version=20.9.3",
            "-o", tmp_path,
            url,
        ]
        if FFMPEG_EXE:
            cmd.extend(["--ffmpeg-location", FFMPEG_EXE])
        media_type = "video/mp4"

    logger.info("Starting download to temp file: %s", tmp_path)

    # ── Run yt-dlp in a thread (blocking) ─────────────────────────────────────
    def _run_blocking():
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=False,
        )
        return result

    try:
        result = await loop.run_in_executor(None, _run_blocking)
    except Exception as exc:
        _cleanup(tmp_path)
        logger.exception("Subprocess error during download")
        raise HTTPException(status_code=500, detail=f"Download failed: {exc}")

    if result.returncode != 0:
        stderr_msg = result.stderr.decode(errors="replace")
        logger.error("yt-dlp exited %s: %s", result.returncode, stderr_msg)
        _cleanup(tmp_path)
        raise HTTPException(status_code=500, detail=f"Download failed: {stderr_msg[:400]}")

    # Resolve actual path (yt-dlp might have changed the extension)
    actual_path = _resolve_output_path(tmp_path, suffix)
    if not os.path.exists(actual_path):
        _cleanup(tmp_path)
        raise HTTPException(status_code=500, detail="Output file not found after download.")

    file_size = os.path.getsize(actual_path)
    logger.info("Download complete: %s (%d bytes)", actual_path, file_size)

    # ─── Stream the file to the browser then delete ─────────────────────────────
    # Resolve correct extension and media type from what was actually downloaded
    actual_extension = os.path.splitext(actual_path)[1] or suffix
    download_filename = f"{safe_title}{actual_extension}"

    import mimetypes
    mime_type, _ = mimetypes.guess_type(actual_path)
    if not mime_type:
        mime_type = media_type # fallback

    async def file_streamer():
        try:
            with open(actual_path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk :
                        break
                    yield chunk
        finally:
            _cleanup(actual_path)
            # Also cleanup original tmp_path if it was different
            if actual_path != tmp_path:
                _cleanup(tmp_path)

    encoded_filename = quote(download_filename)
    headers = {
        "Content-Disposition": f'attachment; filename="{download_filename}"; filename*=UTF-8\'\'{encoded_filename}',
        "Content-Length": str(file_size),
        "X-Content-Type-Options": "nosniff",
    }
    return StreamingResponse(file_streamer(), media_type=mime_type, headers=headers)


# ─── Download Utilities ───────────────────────────────────────────────────────

def _cleanup(path: str) -> None:
    """Silently delete a file if it exists."""
    try:
        if path and os.path.exists(path):
            os.unlink(path)
    except OSError:
        pass


def _resolve_output_path(tmp_path: str, suffix: str) -> str:
    """
    yt-dlp sometimes writes the file with a different name if it appends another extension
    (e.g. .mp4 -> .mp4.mkv) or if the merge fails.
    """
    if os.path.exists(tmp_path):
        return tmp_path
    
    # Check if yt-dlp appended an extension to our path
    # e.g. we asked for tmp123.mp4 but it wrote tmp123.mp4.webm
    dir_name = os.path.dirname(tmp_path)
    base_name = os.path.basename(tmp_path)
    
    if os.path.exists(dir_name):
        for f in os.listdir(dir_name):
            if f.startswith(base_name) and len(f) > len(base_name):
                return os.path.join(dir_name, f)
                
    # Fallback to suffix variations
    for candidate in [
        tmp_path + suffix,
        tmp_path.rsplit('.', 1)[0] + suffix,
    ]:
        if os.path.exists(candidate):
            return candidate
            
    return tmp_path




async def _run_direct_download(job_id: str, url: str, tmp_path: str):
    """Download directly from a URL with progress tracking."""
    job = JOBS[job_id]
    try:
        job["status"] = "downloading from source"
        async with httpx.AsyncClient(timeout=300.0) as client:
            async with client.stream("GET", url, follow_redirects=True) as response:
                total = int(response.headers.get("Content-Length", 0))
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            job["progress"] = int((downloaded / total) * 100)
        
        job["path"] = tmp_path
        job["status"] = "completed"
        job["progress"] = 100
        logger.info(f"Job {job_id} completed via direct download: {tmp_path}")
    except Exception as e:
        logger.error(f"Direct download failed: {e}")
        job["status"] = "failed"
        job["error"] = str(e)
        _cleanup(tmp_path)
