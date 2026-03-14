#
# This module handles the /ytdl command for downloading and sending
# files from various supported websites using yt-dlp.
#

import os
import uuid
import logging
import asyncio
import time
import re
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, RPCError

from .utils import data_paths, ensure_dirs, humanbytes, DownloadCancelled, safe_edit_text
from .file_splitter import split_file
import yt_dlp
from yt_dlp.utils import DownloadError

log = logging.getLogger("ytdl")
ACTIVE_TASKS = {} # This is now for ytdl tasks

# ---------------- Telegram-safe split size ----------------
MAX_SIZE = 1900 * 1024 * 1024 # 1900 MiB ≈ 1.86 GiB

def cancel_btn(tid):
    return InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Cancel", callback_data=f"cancel_ytdl:{tid}")]])

def sanitize_filename(name):
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return name.strip()

def clean_ansi_codes(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def register_ytdl_handlers(app: Client):
    @app.on_message(filters.command("ytdl") & (filters.private | filters.group))
    async def cmd_ytdl(_, m: Message):
        args = m.text.split(maxsplit=1)
        if len(args) < 2:
            return await m.reply("Usage: `/ytdl <video URL>`")

        url = args[1].strip()
        user_id = m.from_user.id
        paths = data_paths(user_id)
        ensure_dirs()

        msg = await m.reply("🔍 Fetching best formats…")

        try:
            # Fetch exactly 1 format per resolution + sizes
            fmts = await asyncio.to_thread(list_formats, url, paths["cookies"])
        except Exception as e:
            return await msg.edit(f"❌ Error fetching formats:\n`{e}`")

        if not fmts:
            return await msg.edit("❌ No formats found.")

        tid = str(uuid.uuid4())[:8]
        # Store formats in the task memory to avoid Telegram's 64-byte callback limit
        ACTIVE_TASKS[tid] = {"user_id": user_id, "url": url, "msg_id": msg.id, "cancel": False, "formats": fmts}

        kb = []
        # Create perfectly clean buttons: 1 per resolution, descending
        for i, f in enumerate(fmts):
            size_text = humanbytes(f.get("size", 0)) if f.get("size", 0) > 0 else "Unknown Size"
            if f.get('res') == 0:
                label = f"🎵 Audio Only • {size_text}"
            else:
                label = f"🎬 {f.get('res')}p • {size_text}"
            
            # Pass the list index (i) instead of the long format string
            kb.append([InlineKeyboardButton(label, callback_data=f"choose_ytdl:{tid}:{i}")])

        await msg.edit("🎞 **Choose Quality:**\n_(Formats are automatically merged with best audio)_", reply_markup=InlineKeyboardMarkup(kb))

    @app.on_callback_query(filters.regex(r"^cancel_ytdl:(.+)$"))
    async def cancel_ytdl_cb(_, q):
        tid = q.data.split(":")[1]
        if tid in ACTIVE_TASKS:
            ACTIVE_TASKS[tid]["cancel"] = True
            await q.answer("⛔ Task cancelled.", show_alert=True)
            await safe_edit_text(q.message, "⛔ **Cancellation requested...**", reply_markup=None)
        else:
            await q.answer("❌ Task not found or already finished.", show_alert=True)

    @app.on_callback_query(filters.regex(r"^choose_ytdl:(.+?):(\d+)$"))
    async def cb_ytdl(_, q):
        tid, fmt_idx = q.data.split(":")[1:]
        task_info = ACTIVE_TASKS.get(tid)
        
        if not task_info:
            return await q.answer("❌ Task not found or expired.", show_alert=True)

        # Retrieve the exact format string from memory using the index
        fmt = task_info["formats"][int(fmt_idx)]["id"]
        url = task_info["url"]
        user_id = task_info["user_id"]
        paths = data_paths(user_id)

        st = await q.message.edit("⏳ Preparing download…", reply_markup=cancel_btn(tid))

        class ProgressUpdater:
            def __init__(self, msg, url):
                self.msg = msg
                self.url = url
                self.queue = asyncio.Queue()
                self.last_update = 0
                self.last_uploaded_bytes = 0
                self.last_downloaded_bytes = 0
                self.start_time = time.time()
                self.task = None

            def start(self):
                self.task = asyncio.create_task(self.updater_task())

            def stop(self):
                if self.task and not self.task.done():
                    self.task.cancel()

            async def updater_task(self):
                try:
                    while True:
                        text = await self.queue.get()
                        await safe_edit_text(self.msg, f"{text}\n\n`{self.url}`", reply_markup=cancel_btn(tid))
                        self.queue.task_done()
                except asyncio.CancelledError:
                    pass

            def progress_hook(self, d):
                if ACTIVE_TASKS.get(tid, {}).get("cancel"):
                    raise DownloadCancelled() 

                if d["status"] == "downloading":
                    now = time.time()
                    if now - self.last_update < 3:
                        return

                    pct_str = d.get("_percent_str", "").strip()
                    if not pct_str: return

                    pct = float(clean_ansi_codes(pct_str).replace('%', ''))
                    downloaded = d.get("downloaded_bytes", 0)
                    total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0

                    download_speed = clean_ansi_codes(d.get("_speed_str", "N/A")).strip()
                    eta = clean_ansi_codes(d.get("_eta_str", "N/A")).strip()

                    progress_text = f"**Downloading**:\n"
                    progress_text += f"**File:** `{clean_ansi_codes(d.get('filename', 'Unknown File'))}`\n"
                    progress_text += f"{get_progress_bar(pct)} **{pct:.1f}%**\n"
                    progress_text += f"**Size:** {humanbytes(downloaded)} / {humanbytes(total)}\n"
                    progress_text += f"**Speed:** {download_speed} • **ETA:** {eta}"

                    self.queue.put_nowait(progress_text)
                    self.last_update = now

        async def runner():
            fpaths = []
            full_path = ""
            updater = ProgressUpdater(st, url)
            updater.start()

            try:
                await st.edit("✅ Download starting...", reply_markup=cancel_btn(tid))
                
                # Download media using the generated optimal format string
                full_path, fname = await asyncio.to_thread(
                    download_media, url, paths["downloads"], paths["cookies"], updater.progress_hook, fmt
                )
                
                thumb_path = os.path.splitext(full_path)[0] + ".jpg"
                filesize = os.path.getsize(full_path)

                if filesize <= MAX_SIZE:
                    fpaths = [full_path]
                else:
                    await st.edit(f"✅ Download complete. Splitting file into parts…")
                    fpaths = await asyncio.to_thread(split_file, full_path, MAX_SIZE)
                    os.remove(full_path) 

                total_parts = len(fpaths)
                for idx, fpath in enumerate(fpaths, 1):
                    if ACTIVE_TASKS.get(tid, {}).get("cancel"):
                        await safe_edit_text(st, "❌ Upload cancelled by user.")
                        return

                    if not os.path.exists(fpath):
                        continue

                    retries = 3
                    while retries > 0:
                        try:
                            file_ext = os.path.splitext(fpath)[1].lower()
                            is_video = file_ext in ['.mp4', '.mkv', '.avi', '.mov', '.webm']
                            is_audio = file_ext in ['.m4a', '.mp3', '.wav', '.ogg']

                            if total_parts == 1 and is_video:
                                await app.send_video(
                                    q.message.chat.id,
                                    fpath,
                                    caption=f"✅ Uploaded: `{fname}`",
                                    supports_streaming=True, # MAKES VIDEO STREAMABLE
                                    thumb=thumb_path if os.path.exists(thumb_path) else None,
                                    progress=lambda cur, tot: upload_progress(cur, tot, updater, tid, "video", fname, 1, 1)
                                )
                            elif total_parts == 1 and is_audio:
                                await app.send_audio(
                                    q.message.chat.id,
                                    fpath,
                                    caption=f"✅ Uploaded: `{fname}`",
                                    progress=lambda cur, tot: upload_progress(cur, tot, updater, tid, "audio", fname, 1, 1)
                                )
                            else:
                                part_name = sanitize_filename(os.path.basename(fpath))
                                if len(part_name) > 150: part_name = part_name[:150] + os.path.splitext(part_name)[1]

                                await app.send_document(
                                    q.message.chat.id,
                                    fpath,
                                    caption=f"✅ Uploaded part {idx}/{total_parts}: `{part_name}`",
                                    thumb=thumb_path if os.path.exists(thumb_path) else None,
                                    progress=lambda cur, tot: upload_progress(cur, tot, updater, tid, "document", part_name, idx, total_parts)
                                )
                            break
                        except FloodWait as e:
                            await asyncio.sleep(e.value)
                        except RPCError as e:
                            retries -= 1
                            if retries > 0: await asyncio.sleep(5)
                            else: raise e 

                await safe_edit_text(st, "✅ All parts uploaded successfully!")

            except DownloadCancelled:
                await safe_edit_text(st, "❌ Download/Upload cancelled.")
            except Exception as e:
                if ACTIVE_TASKS.get(tid, {}).get("cancel") or "DownloadCancelled" in str(e):
                    await safe_edit_text(st, "❌ Download/Upload cancelled.")
                else:
                    await safe_edit_text(st, f"❌ Error: {e}")
            finally:
                updater.stop()
                ACTIVE_TASKS.pop(tid, None)
                for fpath in fpaths:
                    if os.path.exists(fpath): os.remove(fpath)
                if full_path:
                    thumb_path = os.path.splitext(full_path)[0] + ".jpg"
                    if os.path.exists(thumb_path): os.remove(thumb_path)

        asyncio.create_task(runner())

    def upload_progress(cur, tot, updater, tid, file_type, name, part, total_parts):
        if ACTIVE_TASKS.get(tid, {}).get("cancel"): raise DownloadCancelled()

        now = time.time()
        if now - updater.last_update < 2: return

        speed = (cur - updater.last_uploaded_bytes) / (now - updater.last_update) if now > updater.last_update else 0
        eta = (tot - cur) / speed if speed > 0 else "N/A"

        updater.last_uploaded_bytes = cur
        updater.last_update = now
        frac = cur / tot * 100 if tot else 0
        bar = get_progress_bar(frac)

        progress_text = f"**Uploading{' part ' + str(part) + '/' + str(total_parts) if total_parts > 1 else ''}**:\n`{name}`\n"
        progress_text += f"{bar} **{frac:.1f}%**\n"
        progress_text += f"**Size:** {humanbytes(cur)} / {humanbytes(tot)}\n"
        progress_text += f"**Speed:** {humanbytes(speed)}/s • **ETA:** {int(eta)}s"
        updater.queue.put_nowait(progress_text)


def list_formats(url, cookies=None):
    cookie_path = cookies if (cookies and os.path.exists(cookies)) else None

    opts = {
        "quiet": True,
        "skip_download": True,
        "cookiefile": cookie_path,
        "noplaylist": True,
        "extractor_args": {"generic": ["impersonate"]}, # STRONG CLOUDFLARE BYPASS
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as e:
            raise Exception(str(e))

        formats = info.get("formats", [])
        
        # 1. First, locate the absolute best Audio-only stream
        best_audio = None
        for f in formats:
            if f.get("acodec") != "none" and f.get("vcodec") == "none":
                if not best_audio or (f.get("filesize") or f.get("filesize_approx") or 0) > (best_audio.get("filesize") or best_audio.get("filesize_approx") or 0):
                    best_audio = f
                    
        best_audio_size = best_audio.get("filesize") or best_audio.get("filesize_approx") or 0 if best_audio else 0
        best_audio_id = best_audio.get("format_id") if best_audio else "bestaudio"

        unique_res = {}
        
        # 2. Iterate through video streams, grouping them exclusively by Height/Resolution
        for f in formats:
            height = f.get("height")
            if not height: 
                continue
            
            v_id = f.get("format_id")
            has_audio = f.get("acodec") != "none"
            size = f.get("filesize") or f.get("filesize_approx") or 0
            
            # Estimate combined size. If video lacks audio, add the best_audio_size.
            total_size = size if has_audio else (size + best_audio_size)
            
            # Combine the yt-dlp download string (e.g. "137+140" if they are separated)
            dl_format = f"{v_id}" if has_audio else f"{v_id}+{best_audio_id}"
            
            # Only keep the largest/highest quality version of THIS specific resolution
            if height not in unique_res or total_size > unique_res[height]["size"]:
                unique_res[height] = {
                    "id": dl_format,
                    "res": height,
                    "size": total_size
                }

        # 3. Sort strictly by resolution descending (1080p -> 720p -> 480p)
        sorted_list = sorted(unique_res.values(), key=lambda x: x["res"], reverse=True)
        
        # 4. Append the audio-only option at the very bottom
        if best_audio:
            sorted_list.append({
                "id": best_audio_id,
                "res": 0,
                "size": best_audio_size
            })

        return sorted_list


def download_media(url, path, cookies, progress_hook, fmt_id):
    cookie_path = cookies if (cookies and os.path.exists(cookies)) else None

    opts = {
        "format": fmt_id,
        "outtmpl": os.path.join(path, "%(title)s.%(ext)s"),
        "cookiefile": cookie_path,
        "progress_hooks": [progress_hook],
        "extractor_args": {"generic": ["impersonate"]},
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        },
        "writethumbnail": True, 
        "merge_output_format": "mp4", # Forces merge of video+audio into standard MP4 format
        "postprocessors": [
            {
                'key': 'FFmpegThumbnailsConvertor',
                'format': 'jpg',
            }
        ]
    }

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        full_path = ydl.prepare_filename(info)
        
        # When yt-dlp merges files, it might change the extension safely behind the scenes.
        # This checks the disk to guarantee we return the EXACT correct file path to Telegram.
        base, ext = os.path.splitext(full_path)
        if not os.path.exists(full_path):
            for e in [".mp4", ".mkv", ".m4a", ".mp3", ".webm"]:
                if os.path.exists(base + e):
                    full_path = base + e
                    break
                    
        return full_path, info.get("title")


def get_progress_bar(percentage):
    filled_length = int(percentage // 5)
    bar = "█" * filled_length + "░" * (20 - filled_length)
    return f"`[{bar}]`"
