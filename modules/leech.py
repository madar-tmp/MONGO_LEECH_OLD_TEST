#
# This module handles the /leech command for downloading and sending
# files from a direct URL.
#

import os
import uuid
import logging
import asyncio
import time
import requests
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message

from .utils import data_paths, ensure_dirs, humanbytes, DownloadCancelled, safe_edit_text

log = logging.getLogger("leech")
ACTIVE_TASKS = {}

def cancel_btn(tid):
    """
    Creates an inline keyboard with a single "Cancel" button.
    """
    return InlineKeyboardMarkup([[InlineKeyboardButton("⛔ Cancel", callback_data=f"cancel:{tid}")]])

def sanitize_filename(name):
    """
    Removes characters Telegram cannot handle from a filename.
    """
    return name.strip()

def register_leech_handlers(app: Client):
    """
    Registers the command and callback query handlers for the leech module.
    """
    @app.on_message(filters.command("leech") & (filters.private | filters.group))
    async def cmd_leech(_, m: Message):
        args = m.text.split(maxsplit=1)
        if len(args) < 2:
            return await m.reply("Usage: `/leech <direct file URL>`")

        url = args[1].strip()
        user_id = m.from_user.id
        paths = data_paths(user_id)
        ensure_dirs()
        tid = str(uuid.uuid4())[:8]
        
        ACTIVE_TASKS[tid] = {"user_id": user_id, "url": url, "msg_id": None, "cancel": False}

        msg = await m.reply("⏳ Starting direct file download...", reply_markup=cancel_btn(tid))
        ACTIVE_TASKS[tid]["msg_id"] = msg.id

        async def runner():
            try:
                loop = asyncio.get_event_loop()
                await asyncio.to_thread(download_file, loop, url, paths["downloads"], tid, msg)

                filename = os.path.basename(url)
                download_path = os.path.join(paths["downloads"], filename)

                if not os.path.exists(download_path):
                    await safe_edit_text(msg, "❌ Download failed. File not found.")
                    return

                await safe_edit_text(msg, f"✅ Download complete. Uploading `{filename}`...")
                
                last_upload_update_time = time.time()
                
                async def upload_progress(cur, tot):
                    nonlocal last_upload_update_time
                    
                    now = time.time()
                    if (now - last_upload_update_time) < 3:
                        return
                    
                    last_upload_update_time = now
                    
                    frac = cur / tot * 100 if tot else 0
                    bar = "█" * int(frac // 5) + "░" * (20 - int(frac // 5))
                    await safe_edit_text(
                        msg, 
                        f"**Uploading...**\n`{filename}`\n{bar} **{frac:.1f}%**\n⬆ {humanbytes(cur)}/{humanbytes(tot)}", 
                        reply_markup=cancel_btn(tid)
                    )
                    
                    if ACTIVE_TASKS.get(tid, {}).get("cancel"):
                        raise DownloadCancelled()

                await app.send_document(m.chat.id, download_path, progress=upload_progress)
                await safe_edit_text(msg, f"✅ Uploaded `{filename}` successfully!")
                os.remove(download_path) 

            except DownloadCancelled:
                await safe_edit_text(msg, "❌ Download/Upload cancelled.")
                filename = os.path.basename(url)
                download_path = os.path.join(paths["downloads"], filename)
                if os.path.exists(download_path):
                    os.remove(download_path)
            except Exception as e:
                # Use safe_edit_text to handle errors and avoid crashing
                if ACTIVE_TASKS.get(tid, {}).get("cancel"):
                     await safe_edit_text(msg, "❌ Download/Upload cancelled.")
                else:
                     await safe_edit_text(msg, f"❌ Error: {e}")
            finally:
                if tid in ACTIVE_TASKS:
                    ACTIVE_TASKS.pop(tid)
        
        asyncio.create_task(runner())

    @app.on_callback_query(filters.regex(r"^cancel:(.+)$"))
    async def cancel_leech_cb(_, q):
        """
        Handles the "Cancel" button click.
        """
        tid = q.data.split(":")[1]
        if tid in ACTIVE_TASKS:
            ACTIVE_TASKS[tid]["cancel"] = True
            await q.answer("⛔ Task cancelled.", show_alert=True)
            await safe_edit_text(q.message, "⛔ **Cancellation requested...**", reply_markup=None)
        else:
            await q.answer("❌ Task not found.", show_alert=True)

def download_file(loop, url, path, tid, msg):
    try:
        filename = os.path.basename(url)
        filepath = os.path.join(path, filename)
        
        last_download_update_time = time.time()

        if not url.startswith(("http://", "https://")):
            raise ValueError("URL is not valid")

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            downloaded = 0
            
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if ACTIVE_TASKS.get(tid, {}).get("cancel"):
                        raise DownloadCancelled()
                    
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    now = time.time()
                    if (now - last_download_update_time) > 3:
                        last_download_update_time = now
                        pct = (downloaded / total_size) * 100 if total_size > 0 else 0
                        bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
                        loop.call_soon_threadsafe(
                            asyncio.create_task,
                            safe_edit_text(
                                msg, 
                                f"**Downloading...**\n`{filename}`\n{bar} **{pct:.1f}%**\n⬇ {humanbytes(downloaded)}/{humanbytes(total_size)}", 
                                reply_markup=cancel_btn(tid)
                            )
                        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download file: {e}")
