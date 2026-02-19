import os, re, math, asyncio, time, random
from collections import deque
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.functions.messages import SendMediaRequest
from telethon.tl.types import InputFileBig, InputMediaUploadedDocument, DocumentAttributeFilename
from telethon.errors import FloodWaitError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Config ────────────────────────────────────────────────────────────────

API_ID         = int(os.environ.get("TG_API_ID", 0))
API_HASH       = os.environ.get("TG_API_HASH", "")
BOT_TOKEN      = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
CHANNEL_ID     = -1003818449922

WORK_DIR       = "downloads"
CHUNK_SIZE     = 512 * 1024          # 512KB - Telegram upload chunk
PART_SIZE      = 1900 * 1024 * 1024  # 1.9GB - Telegram max file size
DL_CHUNK       = 8 * 1024 * 1024     # 8MB - download chunk (Oracle network optimal)
UPLOAD_SEM     = 16                  # parallel upload chunks (free tier safe)
UA             = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
PROXIES        = {"http": "socks5h://127.0.0.1:40000", "https": "socks5h://127.0.0.1:40000"}

os.makedirs(WORK_DIR, exist_ok=True)

user_client = TelegramClient(
    StringSession(STRING_SESSION), API_ID, API_HASH,
    connection_retries=10,
    retry_delay=2,
    flood_sleep_threshold=60,   # auto sleep on flood wait up to 60s
)
bot_client = TelegramClient(
    "bot_session", API_ID, API_HASH,
    flood_sleep_threshold=60,
)

job_queue      = deque()
worker_running = False

# ── HTTP Session (connection pooling for speed) ───────────────────────────

def make_session():
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": UA})
    return s

# ── GoFile ────────────────────────────────────────────────────────────────

def get_website_token(sess):
    try:
        r = sess.get("https://gofile.io/dist/js/config.js", proxies=PROXIES, timeout=15)
        if 'appdata.wt = "' in r.text:
            return r.text.split('appdata.wt = "')[1].split('"')[0]
        m = re.search(r'websiteToken["\']?\s*[=:]\s*["\']([^"\']{4,})["\']', r.text)
        return m.group(1) if m else "none"
    except:
        return "none"

def resolve_gofile(page_url):
    cid  = page_url.rstrip("/").split("/d/")[-1]
    sess = make_session()
    hdrs = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://gofile.io/",
        "Origin": "https://gofile.io"
    }

    acc_token = None
    for attempt in range(5):
        try:
            r = sess.post("https://api.gofile.io/accounts", headers=hdrs, proxies=PROXIES, timeout=20).json()
            if r.get("status") == "ok":
                acc_token = r["data"]["token"]
                break
        except Exception as e:
            print(f"⚠️ Account create attempt {attempt}: {e}")
            time.sleep(3)

    if not acc_token:
        raise Exception("GoFile guest account creation failed.")

    wt       = get_website_token(sess)
    api_hdrs = {**hdrs, "Authorization": f"Bearer {acc_token}", "X-Website-Token": wt}

    resp = sess.get(f"https://api.gofile.io/contents/{cid}?cache=true", headers=api_hdrs, proxies=PROXIES, timeout=25).json()
    if resp.get("status") != "ok":
        raise Exception(f"GoFile API Error: {resp.get('status')}")

    data     = resp.get("data", {})
    children = data.get("children", {})

    item = None
    if isinstance(children, dict):
        item = next((v for v in children.values() if v.get("type") == "file"), None)
    elif isinstance(children, list):
        item = next((v for v in children if v.get("type") == "file"), None)

    if not item:
        if data.get("type") == "file":
            item = data
        else:
            raise Exception("No downloadable file found.")

    return item["link"], item["name"], item.get("size", 0), api_hdrs, sess

# ── Safe Edit (flood wait handle) ────────────────────────────────────────

async def safe_edit(msg, text):
    try:
        await msg.edit(text)
    except FloodWaitError as e:
        print(f"⏳ FloodWait: {e.seconds}s")
        await asyncio.sleep(e.seconds + 2)
        try:
            await msg.edit(text)
        except:
            pass
    except Exception:
        pass

# ── Download to Disk (streaming, no RAM buffer) ───────────────────────────

def download_to_disk(dl_url, hdrs, sess, fname, status_cb):
    """
    Streams file directly to part files on disk.
    No large RAM buffer - writes chunks directly.
    Returns list of part file paths.
    """
    parts      = []
    part_num   = 1
    downloaded = 0
    start_time = time.time()
    last_print = time.time()

    pname      = os.path.join(WORK_DIR, f"{fname}.part{part_num}")
    part_file  = open(pname, "wb")
    part_bytes = 0
    parts.append(pname)

    with sess.get(dl_url, headers=hdrs, stream=True, proxies=PROXIES, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))

        for chunk in r.iter_content(chunk_size=DL_CHUNK):
            if not chunk:
                continue

            # ── write chunk, split if needed ──
            while chunk:
                space      = PART_SIZE - part_bytes
                to_write   = chunk[:space]
                part_file.write(to_write)
                part_bytes += len(to_write)
                chunk       = chunk[space:]

                if part_bytes >= PART_SIZE:
                    part_file.close()
                    print(f"✂️ Part {part_num} complete: {pname} ({part_bytes//(1024**2)} MB)")
                    part_num  += 1
                    part_bytes = 0
                    pname      = os.path.join(WORK_DIR, f"{fname}.part{part_num}")
                    part_file  = open(pname, "wb")
                    parts.append(pname)

            downloaded += DL_CHUNK if len(chunk) == 0 else 0

            now = time.time()
            if now - last_print > 5:
                # recalculate properly
                elapsed    = now - start_time
                dl_so_far  = sum(os.path.getsize(p) for p in parts if os.path.exists(p)) + part_bytes
                spd        = dl_so_far / elapsed / (1024 * 1024) if elapsed > 0 else 0
                pct        = int(dl_so_far / total * 100) if total else 0
                status_cb(pct, dl_so_far, total, spd)
                last_print = now

        part_file.close()
        if part_bytes == 0 and os.path.exists(pname):
            # empty last part - remove
            os.remove(pname)
            parts.pop()
        else:
            print(f"✂️ Final part {part_num}: {pname} ({part_bytes//(1024**2)} MB)")

    return parts

# ── Upload ────────────────────────────────────────────────────────────────

async def fast_upload(file_path, status_msg, fname, part_label):
    file_size   = os.path.getsize(file_path)
    total_parts = math.ceil(file_size / CHUNK_SIZE)
    file_id     = random.randint(0, 2**63)
    sem         = asyncio.Semaphore(UPLOAD_SEM)
    lock        = asyncio.Lock()
    state       = {"done": 0, "last_edit": 0, "failed": []}

    async def upload_one(idx):
        with open(file_path, "rb") as f:
            f.seek(idx * CHUNK_SIZE)
            data = f.read(CHUNK_SIZE)

        if not data:
            print(f"⚠️ Chunk {idx}: empty, skipping")
            return

        success = False
        async with sem:
            for attempt in range(15):
                try:
                    await user_client(SaveBigFilePartRequest(
                        file_id=file_id,
                        file_part=idx,
                        file_total_parts=total_parts,
                        bytes=data
                    ))
                    success = True
                    break
                except FloodWaitError as e:
                    print(f"⏳ FloodWait {e.seconds}s (chunk {idx})")
                    await asyncio.sleep(e.seconds + 2)
                except Exception as e:
                    wait = 2 ** min(attempt, 6)
                    print(f"⚠️ Chunk {idx} attempt {attempt}: {e} (retry {wait}s)")
                    await asyncio.sleep(wait)

        if not success:
            async with lock:
                state["failed"].append(idx)
            print(f"❌ Chunk {idx} failed permanently!")
            return

        # progress update
        async with lock:
            state["done"] += 1
            now = time.time()
            should_edit = (now - state["last_edit"]) > 5
            if should_edit:
                state["last_edit"] = now
                pct      = int(state["done"] / total_parts * 100)
                done_mb  = min(state["done"] * CHUNK_SIZE, file_size) // (1024 ** 2)
                total_mb = file_size // (1024 ** 2)

        if should_edit:
            await safe_edit(
                status_msg,
                f"⬆️ **Uploading {part_label}:** `{fname}`\n\n"
                f"[{'█'*int(pct/10)}{'░'*(10-int(pct/10))}] {pct}%\n"
                f"📤 {done_mb} MB / {total_mb} MB"
            )

    # upload in small batches
    BATCH = 32
    for start in range(0, total_parts, BATCH):
        batch = [upload_one(i) for i in range(start, min(start + BATCH, total_parts))]
        await asyncio.gather(*batch)

    # verify
    if state["failed"]:
        raise Exception(f"❌ {len(state['failed'])} chunks failed: {state['failed'][:5]}")
    if state["done"] != total_parts:
        raise Exception(f"❌ Upload incomplete: {state['done']}/{total_parts} chunks")

    print(f"✅ All {total_parts} chunks uploaded: {fname}")
    return InputFileBig(id=file_id, parts=total_parts, name=fname)

# ── Worker ────────────────────────────────────────────────────────────────

async def process_job(url, status_msg):
    parts = []
    try:
        await safe_edit(status_msg, "🔍 **Analysing link...**")
        dl_url, fname, size, hdrs, sess = resolve_gofile(url)

        await safe_edit(status_msg, f"⬇️ **Downloading:** `{fname}`\n\n📦 Starting...")

        loop = asyncio.get_event_loop()

        def status_cb(pct, dl, total, spd):
            text = (
                f"⬇️ **Downloading:** `{fname}`\n\n"
                f"[{'█'*int(pct/10)}{'░'*(10-int(pct/10))}] {pct}%\n"
                f"📦 {dl//(1024**2)} MB / {total//(1024**2)} MB\n"
                f"⚡ {spd:.1f} MB/s"
            )
            asyncio.run_coroutine_threadsafe(safe_edit(status_msg, text), loop)

        # ── Download in thread (blocking IO, don't block event loop) ──
        parts = await loop.run_in_executor(
            None,
            lambda: download_to_disk(dl_url, hdrs, sess, fname, status_cb)
        )

        total_count = len(parts)
        if total_count == 0:
            raise Exception("Download failed - no parts saved.")

        total_mb = sum(os.path.getsize(p) for p in parts if os.path.exists(p)) // (1024 ** 2)
        print(f"📦 Download complete: {total_mb} MB → {total_count} part(s)")

        # ── Upload each part ──
        for i, p in enumerate(parts, 1):
            if not os.path.exists(p):
                raise Exception(f"Part file missing: {p}")

            # ✅ Single part = original filename, multi part = .part1/.part2 etc
            if total_count == 1:
                display_name = fname
            else:
                display_name = f"{fname}.part{i}"

            psize_mb = os.path.getsize(p) // (1024 ** 2)
            label    = f"Part {i}/{total_count}"

            # ✅ Rename file on disk to match display name
            new_path = os.path.join(WORK_DIR, display_name)
            if p != new_path:
                os.rename(p, new_path)
                parts[i - 1] = new_path
                p = new_path

            print(f"⬆️ Uploading {label}: {display_name} ({psize_mb} MB)")
            input_file = await fast_upload(p, status_msg, display_name, label)

            await user_client(SendMediaRequest(
                peer=CHANNEL_ID,
                media=InputMediaUploadedDocument(
                    file=input_file,
                    mime_type="application/octet-stream",
                    attributes=[DocumentAttributeFilename(display_name)]
                ),
                message=(
                    f"✅ **{fname}**\n"
                    f"🗂 {label}\n"
                    f"📦 {psize_mb} MB"
                ),
                random_id=random.randint(0, 2**63),
            ))

            os.remove(p)
            print(f"✅ {label} uploaded & deleted.")

        await safe_edit(
            status_msg,
            f"✅ **Done!** `{fname}`\n"
            f"📦 {total_mb} MB → {total_count} part{'s' if total_count > 1 else ''} uploaded!"
        )

    except Exception as e:
        print(f"❌ Error: {e}")
        await safe_edit(status_msg, f"❌ **Error:** `{e}`")

    finally:
        for p in parts:
            if os.path.exists(p):
                try:
                    os.remove(p)
                    print(f"🗑️ Cleaned: {p}")
                except:
                    pass

async def worker():
    global worker_running
    worker_running = True
    while job_queue:
        url, msg = job_queue.popleft()
        await process_job(url, msg)
        await asyncio.sleep(2)
    worker_running = False

# ── Handlers ──────────────────────────────────────────────────────────────

@bot_client.on(events.NewMessage(pattern="/start"))
async def start(event):
    await event.respond(
        "⚡ **GoFile Bot Active!**\n"
        "Send a `gofile.io/d/...` link to start.\n\n"
        "📌 Files >1.9GB will be auto-split."
    )

@bot_client.on(events.NewMessage(pattern=r"https://gofile\.io/d/\S+"))
async def link_handler(event):
    global worker_running
    url = event.pattern_match.group(0).strip()
    pos = len(job_queue) + (1 if worker_running else 0)
    msg = await event.respond(
        f"⏳ **Added to queue** (position: {pos})\n`{url}`"
    )
    job_queue.append((url, msg))
    if not worker_running:
        asyncio.create_task(worker())

@bot_client.on(events.NewMessage(pattern="/queue"))
async def queue_status(event):
    if not job_queue and not worker_running:
        await event.respond("✅ Queue empty.")
    else:
        await event.respond(
            f"📋 **Queue:** {len(job_queue)} pending\n"
            f"⚙️ Worker: {'running' if worker_running else 'idle'}"
        )

# ── Main ──────────────────────────────────────────────────────────────────

async def main():
    print("🚀 Starting bot...")
    await user_client.start()
    me = await user_client.get_me()
    print(f"✅ User: {me.first_name} (ID: {me.id})")

    await bot_client.start(bot_token=BOT_TOKEN)
    me = await bot_client.get_me()
    print(f"✅ Bot: @{me.username}")
    print("✅ Ready!")

    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())