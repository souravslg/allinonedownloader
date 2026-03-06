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
from urllib.parse import quote

import yt_dlp
from pytubefix import YouTube
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

# ─── Environment ─────────────────────────────────────────────────────────────
FFMPEG_EXE: Optional[str] = None
IS_KOYEB: bool = os.path.exists("/app") or "KOYEB" in os.environ

def _init_ffmpeg() -> bool:
    """Detect ffmpeg and return True if found, False otherwise."""
    global FFMPEG_EXE
    # 1. Try system PATH (standard 'ffmpeg')
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=3)
        FFMPEG_EXE = "ffmpeg"
        logger.info("Using system ffmpeg")
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # 2. Try imageio-ffmpeg bundled binary
    try:
        import imageio_ffmpeg
        exe = imageio_ffmpeg.get_ffmpeg_exe()
        if os.path.exists(exe):
            FFMPEG_EXE = exe
            logger.info("Found bundled ffmpeg: %s", exe)
            # Prepend to PATH so yt-dlp internals and other calls can find it
            ffmpeg_dir = os.path.dirname(exe)
            os.environ["PATH"] = ffmpeg_dir + os.pathsep + os.environ.get("PATH", "")
            return True
    except (ImportError, Exception) as e:
        logger.debug("imageio-ffmpeg check failed: %s", e)

    logger.warning("FFmpeg NOT found. High-quality merges and MP3 conversion will be disabled.")
    return False

FFMPEG_AVAILABLE: bool = _init_ffmpeg()


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

# ─── Job Registry ─────────────────────────────────────────────────────────────
# Stores job state: { job_id: { status, progress, title, ext, path, error } }
JOBS: dict[str, dict] = {}


# FFmpeg detection already performed above
logger.info("FFmpeg available: %s", FFMPEG_AVAILABLE)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _sanitize_filename(name: str) -> str:
    """Remove characters that are illegal in filenames."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def _is_youtube(url: str) -> bool:
    """Return True if the URL is from YouTube."""
    return "youtube.com" in url or "youtu.be" in url


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
        "merge_output_format": "mp4"
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

    # ── Koyeb Force YouTube Fix ──────────────────────────────────────────────
    # Data center IPs are often blocked by yt-dlp. Force pytubefix primary on Koyeb.
    if IS_KOYEB and _is_youtube(url):
        logger.info("Running on Koyeb: Forcing pytubefix for YouTube metadata")
        try:
            def _pytube_extract():
                # MWEB or WEB_EMBED usually work better on server IPs
                yt = YouTube(url, client='MWEB')
                return {
                    "title": yt.title,
                    "thumbnail": yt.thumbnail_url,
                    "duration": yt.length,
                    "uploader": yt.author,
                    "extractor_key": "youtube",
                    "webpage_url": url,
                    "formats": [], 
                    "is_pytubefix": True
                }
            info = await loop.run_in_executor(None, _pytube_extract)
            # Skip to specialized format picker
            formats = [
                {"format_id": "pytubefix_720p", "label": "720p (Pytube)", "ext": "mp4", "type": "video", "needs_merge": False},
                {"format_id": "pytubefix_360p", "label": "360p (Pytube)", "ext": "mp4", "type": "video", "needs_merge": False},
                {"format_id": "pytubefix_audio", "label": "Audio (Pytube)", "ext": "m4a", "type": "audio"},
            ]
            return JSONResponse({
                "type": "video",
                "title": info["title"],
                "thumbnail": info["thumbnail"],
                "channel": info["uploader"],
                "duration": info["duration"],
                "platform": "youtube",
                "webpage_url": info["webpage_url"],
                "formats": formats,
            })
        except Exception as pe:
             logger.error("Koyeb Force Pytube failed: %s", pe)
             # Fall back to yt-dlp just in case, or let error handle
    
    # ── Standard Flow (yt-dlp first) ────────────────────────────────────────
    ydl_opts = _build_ydl_opts(
        {
            "skip_download": True,
            "noplaylist": False,
            "extract_flat": "in_playlist",  # Fast: list playlist items without fetching each
        }
    )

    loop = asyncio.get_event_loop()

    def _extract():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await loop.run_in_executor(None, _extract)
    except Exception as e:
        # If YouTube, try pytubefix as fallback
        if _is_youtube(url):
            logger.info("yt-dlp failed for YouTube, trying pytubefix fallback...")
            try:
                def _pytube_extract():
                    # For data centers, use 'WEB_EMBED' or similar
                    yt = YouTube(url, client='WEB_EMBED')
                    return {
                        "title": yt.title,
                        "thumbnail": yt.thumbnail_url,
                        "duration": yt.length,
                        "uploader": yt.author,
                        "extractor_key": "youtube",
                        "webpage_url": url,
                        "formats": [], 
                        "is_pytubefix": True
                    }
                info = await loop.run_in_executor(None, _pytube_extract)
            except Exception as pe:
                logger.error("pytubefix also failed: %s", pe)
                # Provide a more helpful message for Koyeb users
                error_msg = str(e) if "Sign in to confirm" in str(e) else str(pe)
                raise HTTPException(
                    status_code=422, 
                    detail=f"YouTube is blocking this server IP. Try adding YOUTUBE_PO_TOKEN and YOUTUBE_VISITOR_DATA to environment variables. (Error: {error_msg})"
                )
        else:
            logger.error("yt-dlp error: %s", e)
            raise HTTPException(status_code=422, detail=f"Could not process URL: {str(e)}")

    # ── Playlist ──────────────────────────────────────────────────────────────
    if info.get("_type") == "playlist":
        entries = info.get("entries", [])
        playlist_items = []
        for entry in entries[:50]:  # Limit to 50 items per fetch
            if not entry:
                continue
            playlist_items.append(
                {
                    "id": entry.get("id", ""),
                    "title": entry.get("title", "Untitled"),
                    "url": entry.get("url") or entry.get("webpage_url") or entry.get("id"),
                    "thumbnail": entry.get("thumbnail") or entry.get("thumbnails", [{}])[-1].get("url"),
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
    
    # If format_id starts with pytubefix_, use pytubefix worker
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


async def _run_pytubefix_download(job_id: str, body: DownloadJobRequest, tmp_path: str):
    """Fallback YouTube downloader using pytubefix."""
    job = JOBS[job_id]
    job["status"] = "downloading (pytubefix)"
    
    loop = asyncio.get_event_loop()
    try:
        def _exec():
            yt = YouTube(body.url, client='WEB_EMBED')
            if body.format_id == "pytubefix_720p":
                stream = yt.streams.filter(res="720p", file_extension="mp4").first() or yt.streams.get_highest_resolution()
            elif body.format_id == "pytubefix_360p":
                stream = yt.streams.filter(res="360p", file_extension="mp4").first()
            else:
                stream = yt.streams.get_audio_only()
            
            # Pytube downloads to a filename
            out_file = stream.download(output_path=os.path.dirname(tmp_path))
            
            # If we need MP3 we MUST convert with FFmpeg
            if body.ext == "mp3" and FFMPEG_EXE:
                job["status"] = "converting to mp3"
                final_path = tmp_path.rsplit('.', 1)[0] + ".mp3"
                cmd = [FFMPEG_EXE, "-i", out_file, "-vn", "-ab", "192k", "-ar", "44100", "-y", final_path]
                subprocess.run(cmd, capture_output=True)
                _cleanup(out_file)
                job["path"] = final_path
                job["ext"] = "mp3"
            elif body.ext == "mp4" and not out_file.endswith(".mp4") and FFMPEG_EXE:
                job["status"] = "converting to mp4"
                final_path = tmp_path.rsplit('.', 1)[0] + ".mp4"
                cmd = [FFMPEG_EXE, "-i", out_file, "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-y", final_path]
                subprocess.run(cmd, capture_output=True)
                _cleanup(out_file)
                job["path"] = final_path
                job["ext"] = "mp4"
            else:
                # Just rename to ensure it has an extension
                actual_ext = os.path.splitext(out_file)[1] or f".{body.ext}"
                final_path = tmp_path.rsplit('.', 1)[0] + actual_ext
                if out_file != final_path:
                    os.replace(out_file, final_path)
                job["path"] = final_path
                job["ext"] = actual_ext.lstrip('.')

        await loop.run_in_executor(None, _exec)
        
        if job.get("path") and os.path.exists(job["path"]):
            job["status"] = "completed"
            job["progress"] = 100
            logger.info(f"Job {job_id} completed via pytubefix: {job['path']}")
        else:
            job["status"] = "failed"
            job["error"] = "File not found after pytube download"
        
    except Exception as e:
        logger.exception(f"Pytubefix job {job_id} failed")
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
