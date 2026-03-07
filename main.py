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


def _is_youtube(url: str) -> bool:
    """Return True if the URL is from YouTube."""
    return "youtube.com" in url or "youtu.be" in url


async def _fetch_vidssave_metadata(url: str) -> Optional[dict]:
    """Fetch metadata from vidssave.com API."""
    try:
        data = {
            "auth": VIDSSAVE_AUTH,
            "domain": "api-ak.vidssave.com",
            "origin": "cache",
            "link": url
        }
        headers = {
            "Referer": "https://vidssave.com/",
            "Origin": "https://vidssave.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(VIDSSAVE_API, data=data, headers=headers)
            if resp.status_code != 200:
                return None
            
            res_json = resp.json()
            if res_json.get("status") != 1:
                return None
            
            d = res_json.get("data", {})
            formats = []
            for res in d.get("resources", []):
                q = res.get("quality", "Unknown")
                f = res.get("format", "mp4").lower()
                size = res.get("size")
                size_str = f" (~{size // 1048576}MB)" if size else ""
                
                formats.append({
                    "format_id": f"vidssave_{res.get('resource_id')}",
                    "label": f"{q} ({f.upper()}) [Source 2]",
                    "ext": f,
                    "type": res.get("type", "video"),
                    "download_url": res.get("download_url"),
                    "filesize_approx": size
                })
            
            if not formats:
                return None

            return {
                "type": "video",
                "title": d.get("title", "YouTube video"),
                "thumbnail": d.get("thumbnail"),
                "duration": d.get("duration"),
                "channel": "YouTube",
                "platform": "youtube (vidssave)",
                "webpage_url": url,
                "formats": formats
            }

    except Exception as e:
        logger.error(f"Vidssave metadata fetch failed: {e}")
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

    # ── YouTube pytubefix Flow ────────────────────────────────────────────────
    if _is_youtube(url):
        logger.info("Using pytubefix for YouTube metadata: %s", url)
        try:
            from pytubefix import YouTube
            def _get_yt():
                # Use a common user agent to avoid some blocks
                return YouTube(url, use_oauth=False, allow_oauth_cache=True)
            
            yt = await loop.run_in_executor(None, _get_yt)
            
            formats = []
            # High quality video filters
            formats.append({"format_id": "pytubefix_1080p", "label": "1080p HD (Pytube)", "ext": "mp4", "type": "video", "needs_merge": True})
            formats.append({"format_id": "pytubefix_720p", "label": "720p HD (Pytube)", "ext": "mp4", "type": "video", "needs_merge": False})
            formats.append({"format_id": "pytubefix_360p", "label": "360p (Pytube)", "ext": "mp4", "type": "video", "needs_merge": False})
            formats.append({"format_id": "pytubefix_audio", "label": "Audio Only (M4A)", "ext": "m4a", "type": "audio"})
            formats.append({"format_id": "pytubefix_mp3", "label": "Audio Only (MP3)", "ext": "mp3", "type": "audio"})

            return JSONResponse({
                "type": "video",
                "title": yt.title,
                "thumbnail": yt.thumbnail_url,
                "duration": yt.length,
                "channel": yt.author,
                "platform": "youtube",
                "webpage_url": url,
                "formats": formats
            })
        except Exception as e:
            error_msg = str(e)
            logger.error("pytubefix fetch failed: %s", error_msg)
            
            # Try VidsSave as a primary fallback
            try:
                logger.info("Attempting VidsSave fallback for: %s", url)
                vids_data = await _fetch_vidssave_metadata(url)
                if vids_data:
                    return JSONResponse(vids_data)
            except Exception as ve:
                logger.error("VidsSave fallback failed: %s", ve)

            # Final fall-through: If it's YouTube but pytube/vidssave failed, 
            # we MUST let it fall through to the yt-dlp block below.
            logger.info("pytubefix and vidsave failed. Falling back to final yt-dlp flow...")
            # We don't raise here; we let it fall through to the Standard Flow.



    # ── Standard Flow (Other Platforms) ──────────────────────────────────────
    ydl_opts = _build_ydl_opts(
        {
            "skip_download": True,
            "noplaylist": False,
            "extract_flat": "in_playlist",
        }
    )
    async def _extract_with_retry():
        try:
            return await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(url, download=False))
        except Exception as e:
            err_msg = str(e).lower()
            if ("index out of range" in err_msg or "unavailable" in err_msg) and _is_youtube(url):
                logger.info("yt-dlp failed (likely signature/parsing issue). Retrying with safe options...")
                safe_opts = _build_ydl_opts({
                    "skip_download": True,
                    "extractor_args": {"youtube": {"player_client": ["android", "ios"]}}
                })
                try:
                    return await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(safe_opts).extract_info(url, download=False))
                except:
                    pass
            raise e
    
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
    if info.get("is_pytubefix"):
        # Custom format list for pytubefix
        formats = [
            {"format_id": "pytubefix_720p", "label": "720p (Pytube)", "ext": "mp4", "type": "video", "needs_merge": False},
            {"format_id": "pytubefix_360p", "label": "360p (Pytube)", "ext": "mp4", "type": "video", "needs_merge": False},
            {"format_id": "pytubefix_audio", "label": "Audio (Pytube)", "ext": "m4a", "type": "audio"},
        ]
    else:
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
    
    if body.format_id.startswith("vidssave_") and body.download_url:
        await _run_direct_download(job_id, body.download_url, tmp_path)
        return

    if _is_youtube(body.url):
        await _run_pytubefix_download(job_id, body, tmp_path)
        return

    # If format_id explicitly requested pytubefix
    if body.format_id.startswith("pytubefix_"):
        await _run_pytubefix_download(job_id, body, tmp_path)
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


async def _run_pytubefix_download(job_id: str, body: DownloadJobRequest, tmp_path: str):
    """Download using pytubefix."""
    job = JOBS[job_id]
    from pytubefix import YouTube
    from pytubefix.cli import on_progress
    
    try:
        job["status"] = "connecting to youtube"
        def _get_yt():
            return YouTube(
                body.url, 
                on_progress_callback=lambda stream, chunk, bytes_remaining: _pytube_progress(job, stream, chunk, bytes_remaining),
                use_oauth=False,
                allow_oauth_cache=True
            )
        
        loop = asyncio.get_event_loop()
        yt = await loop.run_in_executor(None, _get_yt)
        
        job["status"] = "selecting stream"
        stream = None
        
        if body.format_id == "pytubefix_1080p":
            # 1080p is usually adaptive (video only), needs merge
            stream = yt.streams.filter(res="1080p", file_extension="mp4", adaptive=True).first()
            if not stream:
                stream = yt.streams.filter(res="1080p", adaptive=True).first()
        elif body.format_id == "pytubefix_720p":
            stream = yt.streams.filter(res="720p", file_extension="mp4", progressive=True).first()
            if not stream:
                stream = yt.streams.filter(res="720p", progressive=True).first()
        elif body.format_id == "pytubefix_360p":
            stream = yt.streams.filter(res="360p", file_extension="mp4", progressive=True).first()
        elif body.format_id in ("pytubefix_audio", "pytubefix_mp3"):
            stream = yt.streams.filter(only_audio=True).first()

        if not stream:
            # Fallback to best progressive mp4
            stream = yt.streams.get_highest_resolution()

        job["status"] = "downloading"
        
        # Pytube download is blocking
        def _do_download():
            return stream.download(output_path=os.path.dirname(tmp_path), filename=os.path.basename(tmp_path))
        
        downloaded_path = await loop.run_in_executor(None, _do_download)
        
        # If it was adaptive 1080p, we might need to merge audio
        if body.format_id == "pytubefix_1080p" and stream.is_adaptive and FFMPEG_EXE:
            job["status"] = "fetching audio for merge"
            audio_stream = yt.streams.filter(only_audio=True).first()
            audio_tmp = downloaded_path + ".audio"
            await loop.run_in_executor(None, lambda: audio_stream.download(output_path=os.path.dirname(tmp_path), filename=os.path.basename(audio_tmp)))
            
            job["status"] = "merging video and audio"
            merged_path = downloaded_path + ".merged.mp4"
            cmd = [FFMPEG_EXE, "-i", downloaded_path, "-i", audio_tmp, "-c:v", "copy", "-c:a", "aac", "-y", merged_path]
            subprocess.run(cmd, capture_output=True)
            
            _cleanup(downloaded_path)
            _cleanup(audio_tmp)
            if os.path.exists(merged_path):
                os.replace(merged_path, downloaded_path)

        # Handle MP3 conversion if requested
        if body.format_id == "pytubefix_mp3" and FFMPEG_EXE:
            job["status"] = "converting to mp3"
            mp3_path = downloaded_path.rsplit('.', 1)[0] + ".mp3"
            cmd = [FFMPEG_EXE, "-i", downloaded_path, "-vn", "-ab", "192k", "-y", mp3_path]
            subprocess.run(cmd, capture_output=True)
            _cleanup(downloaded_path)
            downloaded_path = mp3_path

        job["path"] = downloaded_path
        job["status"] = "completed"
        job["progress"] = 100
        job["ext"] = os.path.splitext(downloaded_path)[1].lstrip('.')
        logger.info(f"Job {job_id} completed via pytubefix: {downloaded_path}")

    except Exception as e:
        logger.exception(f"pytubefix job {job_id} failed")
        job["status"] = "failed"
        job["error"] = str(e)
        if 'downloaded_path' in locals() and os.path.exists(downloaded_path):
            _cleanup(downloaded_path)

def _pytube_progress(job, stream, chunk, bytes_remaining):
    """Callback for pytubefix progress."""
    total_size = stream.filesize
    bytes_downloaded = total_size - bytes_remaining
    percentage = (bytes_downloaded / total_size) * 100
    job["progress"] = int(percentage)
    job["status"] = "downloading"

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
