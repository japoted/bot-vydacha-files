import os
import asyncio
import sqlite3
import logging
import hashlib
import time
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, LabeledPrice,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters, ChatMemberHandler, ChatJoinRequestHandler,
    PreCheckoutQueryHandler, JobQueue
)

load_dotenv()
ADMIN_ID = int(os.getenv("ADMIN_ID"))
BOT_TOKEN = os.getenv("BOT_TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "games (3).db")

class FloodControl:
    def __init__(self):
        self._user_ts = {}
        self._user_cb_ts = {}
        self._start_flood = {}
    def check(self, uid, cooldown=1.0):
        now = time.time()
        last = self._user_ts.get(uid, 0)
        if now - last < cooldown:
            return False
        self._user_ts[uid] = now
        return True
    def check_start(self, uid, limit=5, window=60):
        now = time.time()
        times = self._start_flood.setdefault(uid, [])
        times[:] = [t for t in times if now - t < window]
        if len(times) >= limit:
            return False
        times.append(now)
        return True
    def check_cb(self, uid, cooldown=0.5):
        now = time.time()
        last = self._user_cb_ts.get(uid, 0)
        if now - last < cooldown:
            return False
        self._user_cb_ts[uid] = now
        return True
    def cleanup(self):
        now = time.time()
        for d in (self._user_ts, self._user_cb_ts, self._start_flood):
            stale = [k for k, v in d.items() if now - v > 300]
            for k in stale: del d[k]

FLOOD = FloodControl()

S_IDLE="idle"; S_NAME="name"; S_DESC="desc"; S_CAT="cat"; S_FILE="file"
S_BCAST="bcast"; S_ADMIN="admin"; S_CATNEW="catnew"
S_ADDCH="addch"; S_LAZY="lazy"; S_ADDLINK="addlink"; S_CHNAME="chname"
S_PREMIUM="premium"; S_BCAST_P="bcast_p"; S_ADMIN_LVL="admin_lvl"
S_BCAST_BTN="bcast_btn"; S_BCAST_BTN_ADD="bcast_btn_add"

def init_db():
    c = _get_conn().cursor()
    c.execute("CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
    c.execute("""CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, category TEXT, description TEXT DEFAULT '',
        chat_id INTEGER, message_id TEXT, file_name TEXT, file_size INTEGER DEFAULT 0,
        channel TEXT DEFAULT '', downloads INTEGER DEFAULT 0, deep_link TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    try: c.execute("ALTER TABLE games ADD COLUMN deep_link TEXT DEFAULT ''")
    except: pass
    try: c.execute("ALTER TABLE games ADD COLUMN link_opens INTEGER DEFAULT 0")
    except: pass
    for cat in ["Игры","Приложения","Моды","Другое","Порноигры"]:
        c.execute("INSERT OR IGNORE INTO categories VALUES (NULL,?)", (cat,))
    c.execute("""CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER DEFAULT 0,
        username TEXT DEFAULT '',
        title TEXT DEFAULT '',
        link TEXT DEFAULT '',
        mode TEXT DEFAULT 'check',
        joins INTEGER DEFAULT 0,
        leaves INTEGER DEFAULT 0,
        subs_required INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS channel_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id INTEGER, user_id INTEGER, action TEXT,
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS channel_members (
        channel_id INTEGER, user_id INTEGER, status TEXT DEFAULT 'active',
        PRIMARY KEY (channel_id, user_id))""")
    c.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT '', premium INTEGER DEFAULT 0)")
    try: c.execute("ALTER TABLE users ADD COLUMN premium INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT '2025-01-01 00:00:00'")
    except: pass
    try: c.execute("ALTER TABLE channels ADD COLUMN subs_required INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE channels ADD COLUMN custom_joins INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE channels ADD COLUMN custom_leaves INTEGER DEFAULT 0")
    except: pass
    c.execute("""CREATE TABLE IF NOT EXISTS user_downloads (
        user_id INTEGER, date TEXT, count INTEGER DEFAULT 0, game_id INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, date, game_id))""")
    try: c.execute("ALTER TABLE user_downloads ADD COLUMN game_id INTEGER DEFAULT 0")
    except: pass
    c.execute("CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY, level INTEGER DEFAULT 1)")
    c.execute("""CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, charge_id TEXT, payload TEXT,
        amount INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS fraud_scores (
        user_id INTEGER PRIMARY KEY, score INTEGER DEFAULT 0,
        signals TEXT DEFAULT '', last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS shown_free (
        user_id INTEGER, channel_id INTEGER,
        PRIMARY KEY (user_id, channel_id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS game_likes (
        user_id INTEGER, game_id INTEGER, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, game_id))""")
    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_games_deep_link ON games(deep_link)",
        "CREATE INDEX IF NOT EXISTS idx_games_category ON games(category)",
        "CREATE INDEX IF NOT EXISTS idx_channels_mode ON channels(mode)",
        "CREATE INDEX IF NOT EXISTS idx_channel_members_status ON channel_members(channel_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_channel_members_user ON channel_members(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_channel_stats_action ON channel_stats(channel_id, action)",
        "CREATE INDEX IF NOT EXISTS idx_channel_stats_user ON channel_stats(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_user_downloads_date ON user_downloads(user_id, date)",
        "CREATE INDEX IF NOT EXISTS idx_users_premium ON users(premium)",
        "CREATE INDEX IF NOT EXISTS idx_fraud_scores_score ON fraud_scores(score)",
    ]:
        c.execute(idx)
    c.connection.commit()

_conn = None

def _get_conn():
    global _conn
    if _conn is None:
        try:
            _conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
            _conn.execute("PRAGMA journal_mode=WAL")
            _conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.DatabaseError:
            logging.error("DB corrupted, attempting recovery...")
            try:
                import shutil
                bak = DB_PATH + ".bak"
                shutil.copy2(DB_PATH, bak)
                os.system(f'sqlite3 "{DB_PATH}" ".dump" > "{DB_PATH}.dump"')
                os.system(f'sqlite3 "{DB_PATH}.new" < "{DB_PATH}.dump"')
                shutil.move(DB_PATH + ".new", DB_PATH)
                os.remove(DB_PATH + ".dump")
            except: pass
            _conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
            _conn.execute("PRAGMA journal_mode=WAL")
            _conn.execute("PRAGMA busy_timeout=5000")
    return _conn

def db(q, p=()):
    c = _get_conn()
    cur = c.cursor(); cur.execute(q, p); r = cur.fetchall(); c.commit(); return r

def st(ctx, v=None):
    if v is not None: ctx.user_data["s"] = v
    return ctx.user_data.get("s", S_IDLE)

_admin_cache = {}

def is_admin(uid, min_level=1):
    if uid == ADMIN_ID: return True
    now = time.time()
    cached = _admin_cache.get(uid)
    if cached and now - cached[1] < 60:
        return cached[0] >= min_level
    r = db("SELECT level FROM admins WHERE user_id=?", (uid,))
    level = r[0][0] if r else 0
    _admin_cache[uid] = (level, now)
    return level >= min_level

def get_admin_level(uid):
    if uid == ADMIN_ID: return 3
    now = time.time()
    cached = _admin_cache.get(uid)
    if cached and now - cached[1] < 60:
        return cached[0]
    r = db("SELECT level FROM admins WHERE user_id=?", (uid,))
    level = r[0][0] if r else 0
    _admin_cache[uid] = (level, now)
    return level

def track(uid, un, fn):
    db("INSERT OR IGNORE INTO users (user_id,username,first_name,premium) VALUES (?,?,?,0)", (uid, un or "", fn or ""))

def get_users(): return db("SELECT user_id FROM users")

_premium_cache = {}

def is_premium(uid):
    now = time.time()
    cached = _premium_cache.get(uid)
    if cached is not None and now - cached[1] < 120:
        return cached[0]
    r = db("SELECT premium FROM users WHERE user_id=?", (uid,))
    val = r and r[0][0] == 1
    _premium_cache[uid] = (val, now)
    return val

def track_download(uid, gid=0):
    today = time.strftime("%Y-%m-%d")
    db("INSERT INTO user_downloads (user_id,date,count,game_id) VALUES (?,?,1,?) "
       "ON CONFLICT(user_id,date,game_id) DO UPDATE SET count=count+1", (uid, today, gid))

async def check_fraud(uid, ctx=None):
    score = 0
    signals = []
    today = time.strftime("%Y-%m-%d")

    dl_today = db("SELECT SUM(count) FROM user_downloads WHERE user_id=? AND date=?", (uid, today))
    dl_count = dl_today[0][0] if dl_today and dl_today[0][0] else 0

    if dl_count > 20:
        score += 30
        signals.append(f"downloads_today={dl_count}")
    elif dl_count > 10:
        score += 15
        signals.append(f"downloads_today={dl_count}")

    if ctx:
        try:
            u = await ctx.bot.get_chat(uid)
            if not u.photo:
                score += 10
                signals.append("no_avatar")
            if not u.username:
                score += 5
                signals.append("no_username")
        except:
            pass

    first = db("SELECT created_at FROM users WHERE user_id=?", (uid,))
    if first:
        try:
            created = time.mktime(time.strptime(first[0][0], "%Y-%m-%d %H:%M:%S"))
            age_days = (time.time() - created) / 86400
            if age_days < 1:
                score += 25
                signals.append(f"account_age={age_days:.1f}d")
            elif age_days < 7:
                score += 10
                signals.append(f"account_age={age_days:.1f}d")
        except:
            pass

    last_dl = db("SELECT ts FROM channel_stats WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,))
    if last_dl and last_dl[0]:
        try:
            ts_str = last_dl[0][0] if isinstance(last_dl[0], (tuple, list)) else last_dl[0]
            last = time.mktime(time.strptime(ts_str, "%Y-%m-%d %H:%M:%S"))
            diff = time.time() - last
            if diff < 3:
                score += 20
                signals.append(f"interval={diff:.1f}s")
        except:
            pass

    if dl_count == 0:
        score += 5
        signals.append("no_history")

    sig_str = ",".join(signals) if signals else "clean"
    db("INSERT OR REPLACE INTO fraud_scores (user_id,score,signals,last_check) VALUES (?,?,?,?)",
       (uid, score, sig_str, time.strftime("%Y-%m-%d %H:%M:%S")))
    return score, signals

def get_premium_users(): return db("SELECT user_id FROM users WHERE premium=1")

def like_count(gid):
    r = db("SELECT COUNT(*) FROM game_likes WHERE game_id=?", (gid,))
    return r[0][0] if r else 0

def user_liked(uid, gid):
    r = db("SELECT 1 FROM game_likes WHERE user_id=? AND game_id=?", (uid, gid))
    return bool(r)

def toggle_like(uid, gid):
    if user_liked(uid, gid):
        db("DELETE FROM game_likes WHERE user_id=? AND game_id=?", (uid, gid))
        return False
    else:
        db("INSERT OR IGNORE INTO game_likes (user_id, game_id) VALUES (?,?)", (uid, gid))
        return True

def fmt(s):
    for u in ["B","KB","MB","GB"]:
        if s<1024: return f"{s:.1f} {u}"
        s/=1024
    return f"{s:.2f} TB"

def gen_code():
    h = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
    while db("SELECT 1 FROM games WHERE deep_link=?", (h,)):
        h = hashlib.md5(str(time.time()+hash(h)).encode()).hexdigest()[:8]
    return h

def admin_kb():
    return ReplyKeyboardMarkup([
        ["➕ Файл","🚀 Ленивая загрузка"],
        ["📋 Файлы","📢 Каналы"],
        ["📊 Стат","📊 Моя стат","📊 За период"],
        ["📢 Рассылка","📢 Рассылка ПМ"],
        ["💎 Премиум","👤 Админы","🛡 Фрод"],
        ["📦 Экспорт БД"],
    ], resize_keyboard=True)

def cancel_kb():
    return ReplyKeyboardMarkup([["❌ Отмена"]], resize_keyboard=True)

def user_menu_kb(uid):
    kb = [[InlineKeyboardButton("📂 Каталог", callback_data="catalog")]]
    if not is_premium(uid):
        kb.append([InlineKeyboardButton("💎 Премиум", callback_data="premium")])
    kb.extend([
        [InlineKeyboardButton("💎 Премиум игры", url="https://t.me/PrivateSided/3")],
        [InlineKeyboardButton("📢 Наш канал", url="https://t.me/apksided")],
    ])
    return InlineKeyboardMarkup(kb)

def broadcast_actions_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить", callback_data="bc_send")],
        [InlineKeyboardButton("👀 Предпросмотр", callback_data="bc_preview")],
        [InlineKeyboardButton("🔗 Добавить кнопку", callback_data="bc_add_btn")],
        [InlineKeyboardButton("❌ Отмена", callback_data="bc_cancel")],
    ])

def extract_broadcast_payload(message):
    if message.text:
        return {
            "kind": "text",
            "text": message.text,
            "entities": message.entities or [],
        }
    if message.photo:
        return {
            "kind": "photo",
            "file_id": message.photo[-1].file_id,
            "caption": message.caption or "",
            "caption_entities": message.caption_entities or [],
        }
    if message.video:
        return {
            "kind": "video",
            "file_id": message.video.file_id,
            "caption": message.caption or "",
            "caption_entities": message.caption_entities or [],
        }
    if message.animation:
        return {
            "kind": "animation",
            "file_id": message.animation.file_id,
            "caption": message.caption or "",
            "caption_entities": message.caption_entities or [],
        }
    if message.audio:
        return {
            "kind": "audio",
            "file_id": message.audio.file_id,
            "caption": message.caption or "",
            "caption_entities": message.caption_entities or [],
        }
    if message.document:
        return {
            "kind": "document",
            "file_id": message.document.file_id,
            "caption": message.caption or "",
            "caption_entities": message.caption_entities or [],
        }
    return None

def extract_album_item(message):
    if message.photo:
        return {"type": "photo", "file_id": message.photo[-1].file_id}
    if message.video:
        return {"type": "video", "file_id": message.video.file_id}
    if message.document:
        return {"type": "document", "file_id": message.document.file_id}
    if message.audio:
        return {"type": "audio", "file_id": message.audio.file_id}
    return None

def store_broadcast_payload(message, ctx, premium_only=False):
    payload = extract_broadcast_payload(message)
    if not payload:
        return None
    ctx.user_data["bcast_payload"] = payload
    ctx.user_data["bcast_buttons"] = []
    ctx.user_data["bcast_target"] = "premium" if premium_only else "all"
    return payload

def broadcast_preview(payload):
    kind = payload.get("kind", "text")
    label = {
        "text": "📝 Текст",
        "photo": "🖼 Фото",
        "video": "🎬 Видео",
        "animation": "🎞 GIF/анимация",
        "audio": "🎵 Аудио",
        "document": "📎 Файл",
        "album": f"🖼 Альбом ({len(payload.get('items', []))})",
    }.get(kind, "📨 Сообщение")
    text = payload.get("text") or payload.get("caption") or ""
    if len(text) > 400:
        text = text[:400] + "..."
    return f"{label}\n\n{text}".strip()

async def preview_broadcast_payload(ctx, chat_id, payload, markup=None):
    await send_broadcast_payload(ctx, chat_id, payload, markup)
    await ctx.bot.send_message(chat_id, "👆 Это предпросмотр рассылки")

# ─── Быстрая рассылка ──────────────────────────────────────

SEMAPHORE = None

async def send_broadcast_payload(ctx, uid, payload, markup=None):
    kind = payload.get("kind", "text")
    if kind == "text":
        await ctx.bot.send_message(
            uid,
            payload.get("text", ""),
            entities=payload.get("entities"),
            reply_markup=markup,
        )
    elif kind == "photo":
        await ctx.bot.send_photo(
            uid,
            payload["file_id"],
            caption=payload.get("caption"),
            caption_entities=payload.get("caption_entities"),
            reply_markup=markup,
        )
    elif kind == "video":
        await ctx.bot.send_video(
            uid,
            payload["file_id"],
            caption=payload.get("caption"),
            caption_entities=payload.get("caption_entities"),
            reply_markup=markup,
        )
    elif kind == "animation":
        await ctx.bot.send_animation(
            uid,
            payload["file_id"],
            caption=payload.get("caption"),
            caption_entities=payload.get("caption_entities"),
            reply_markup=markup,
        )
    elif kind == "audio":
        await ctx.bot.send_audio(
            uid,
            payload["file_id"],
            caption=payload.get("caption"),
            caption_entities=payload.get("caption_entities"),
            reply_markup=markup,
        )
    elif kind == "document":
        await ctx.bot.send_document(
            uid,
            payload["file_id"],
            caption=payload.get("caption"),
            caption_entities=payload.get("caption_entities"),
            reply_markup=markup,
        )
    elif kind == "album":
        media = []
        caption = payload.get("caption") or ""
        caption_entities = payload.get("caption_entities")
        for i, item in enumerate(payload.get("items", [])):
            kwargs = {}
            if i == 0 and caption:
                kwargs["caption"] = caption
                kwargs["caption_entities"] = caption_entities
            if item["type"] == "photo":
                media.append(InputMediaPhoto(item["file_id"], **kwargs))
            elif item["type"] == "video":
                media.append(InputMediaVideo(item["file_id"], **kwargs))
            elif item["type"] == "document":
                media.append(InputMediaDocument(item["file_id"], **kwargs))
            elif item["type"] == "audio":
                media.append(InputMediaAudio(item["file_id"], **kwargs))
        if not media:
            raise ValueError("Album payload is empty")
        await ctx.bot.send_media_group(uid, media=media)
        if markup:
            await ctx.bot.send_message(uid, "⬇️ Кнопки:", reply_markup=markup)
    else:
        raise ValueError(f"Unsupported broadcast payload kind: {kind}")

async def finalize_broadcast_album(ctx, admin_uid, media_group_id):
    await asyncio.sleep(1.2)
    cache = ctx.application.bot_data.setdefault("broadcast_media_groups", {})
    key = (admin_uid, media_group_id)
    group = cache.pop(key, None)
    if not group:
        return
    payload = {
        "kind": "album",
        "items": group["items"],
        "caption": group.get("caption", ""),
        "caption_entities": group.get("caption_entities", []),
    }
    user_data = ctx.user_data
    user_data["bcast_payload"] = payload
    user_data["bcast_buttons"] = []
    user_data["bcast_target"] = group["target"]
    user_data["s"] = S_BCAST_BTN
    await ctx.bot.send_message(
        group["chat_id"],
        f"{broadcast_preview(payload)}\n\nАльбом собран. Отправить как есть, посмотреть предпросмотр или добавить кнопки?",
        reply_markup=broadcast_actions_kb(),
    )

async def fast_broadcast(ctx, users, payload, markup=None, skip_premium=False, progress_chat_id=None):
    global SEMAPHORE
    if SEMAPHORE is None:
        SEMAPHORE = asyncio.Semaphore(50)
    targets = [u[0] for u in users if not (skip_premium and is_premium(u[0]))]
    total = len(targets)
    ok = 0
    dead = 0
    last_report = 0
    REPORT_EVERY = 2000

    async def send_one(uid):
        nonlocal ok, dead, last_report
        async with SEMAPHORE:
            try:
                await send_broadcast_payload(ctx, uid, payload, markup)
                ok += 1
            except Exception as e:
                dead += 1
                logging.warning(f"Broadcast failed uid={uid}: {e}")
            await asyncio.sleep(0.01)

            if (ok + dead) % REPORT_EVERY == 0 and (ok + dead) > last_report and progress_chat_id:
                last_report = ok + dead
                try:
                    await ctx.bot.send_message(
                        progress_chat_id,
                        f"⏳ Прогресс: {ok + dead}/{total}\n"
                        f"✅ Успешно: {ok}\n"
                        f"❌ Ошибки: {dead}"
                    )
                except: pass

    await asyncio.gather(*[send_one(uid) for uid in targets], return_exceptions=True)

    if progress_chat_id:
        try:
            await ctx.bot.send_message(
                progress_chat_id,
                f"✅ Рассылка завершена!\n\n"
                f"📨 Отправлено: {ok}\n"
                f"💀 Ошибки: {dead}\n"
                f"👥 Всего: {total}"
            )
        except: pass

    return ok, dead

# ─── Проверка подписки ────────────────────────────────────

async def is_subscribed(uid, chat_id, ctx):
    try:
        m = await ctx.bot.get_chat_member(chat_id, uid)
        return m.status in ["member","administrator","creator","restricted"]
    except Exception as e:
        logging.warning(f"is_subscribed check failed uid={uid} chat={chat_id}: {e}")
        return False

async def check_all_subs(uid, ctx):
    """Возвращает список каналов (id,chat_id,link,mode,title) на которые НЕ подписан.
    Проверяются все каналы кроме 'free'."""
    missing = []
    chs = db("SELECT id,chat_id,link,mode,title FROM channels WHERE mode != 'free'")
    for ch_id, chat_id, link, mode, title in chs:
        if chat_id:
            ok = await is_subscribed(uid, chat_id, ctx)
            if not ok:
                missing.append((ch_id, chat_id, link, mode, title))
    return missing

def channels_to_show(uid, ctx=None):
    """Все каналы для показа пользователю (с ссылкой)."""
    return db("SELECT id,chat_id,link,mode,title FROM channels WHERE link != ''")

def sub_kb(show_list, dl_game_id=None):
    kb = []
    for ch_id, chat_id, link, mode, title in show_list:
        kb.append([InlineKeyboardButton(f"📢 {title}", url=link)])
    free = db("SELECT id,link,title FROM channels WHERE mode='free' AND link != ''")
    for cid, link, title in free:
        kb.append([InlineKeyboardButton(f"📢 {title}", url=link)])
    cb = f"chk_all_dl_{dl_game_id}" if dl_game_id else "chk_all"
    kb.append([InlineKeyboardButton("✅ Проверить подписку", callback_data=cb)])
    kb.append([InlineKeyboardButton("💎 Премиум", callback_data="premium")])
    return InlineKeyboardMarkup(kb)

def free_channels_kb(uid):
    free = db("SELECT id,link,title FROM channels WHERE mode='free' AND link != ''")
    if not free: return None, []
    shown = db("SELECT channel_id FROM shown_free WHERE user_id=?", (uid,))
    shown_ids = {r[0] for r in shown}
    unseen = [(cid, link, title) for cid, link, title in free if cid not in shown_ids]
    if not unseen: return None, []
    kb = []
    for cid, link, title in unseen:
        kb.append([InlineKeyboardButton(f"📢 {title}", url=link)])
    kb.append([InlineKeyboardButton("✅ Далее", callback_data="free_done")])
    return InlineKeyboardMarkup(kb), [cid for cid, _, _ in unseen]

# ─── Старт + проверка каналов ─────────────────────────────

async def start(update, ctx):
    u = update.effective_user
    if not FLOOD.check_start(u.id):
        try: await update.message.reply_text("⚠️ Слишком часто! Подожди немного.")
        except: pass
        return
    track(u.id, u.username, u.first_name)

    args = ctx.args if hasattr(ctx, 'args') else []
    dl_game_id = None
    if args:
        code = args[0]
        g = db("SELECT id,name,category,description,chat_id,message_id,file_name,file_size,downloads FROM games WHERE deep_link=?", (code,))
        if g:
            dl_game_id = g[0][0]
            if not is_premium(u.id):
                missing = await check_all_subs(u.id, ctx)
                if missing:
                    await update.message.reply_text(
                        "📌 Подпишись на каналы и нажми «Проверить подписку» чтобы скачать файл:",
                        reply_markup=sub_kb(missing, dl_game_id))
                    return
            gid, name, cat, desc, chat_id, file_id, fname, fsize, dls = g[0]
            db("UPDATE games SET downloads=downloads+1, link_opens=link_opens+1 WHERE id=?", (gid,))
            track_download(u.id, gid)
            await check_fraud(u.id, ctx)
            try: await ctx.bot.send_document(update.message.chat_id, document=file_id, caption=f"by @apksided")
            except Exception as e:
                logging.error(f"Failed to send {name}: {e}")
                await update.message.reply_text(f"⚠️ Ошибка отправки: {e}")
            return
        else:
            await update.message.reply_text("❌ Файл не найден")
            return

    if is_premium(u.id):
        kb = [
            [InlineKeyboardButton("📂 Каталог", callback_data="catalog")],
            [InlineKeyboardButton("📢 Наш канал", url="https://t.me/apksided")],
        ]
        await update.message.reply_text(
            "👋 Привет дорогой!\n\n💎 Премиум активен\nВыбери 👇",
            reply_markup=InlineKeyboardMarkup(kb))
        return

    missing = await check_all_subs(u.id, ctx)
    if missing:
        await update.message.reply_text(
            "📌 Подпишись на каналы и нажми «Проверить подписку» чтобы продолжить:",
            reply_markup=sub_kb(missing))
        return

    free_kb, free_ids = free_channels_kb(u.id)
    if free_kb:
        await update.message.reply_text(
            "👋 Привет дорогой!\n\n📢 Подпишись на каналы:",
            reply_markup=free_kb)
        for cid in free_ids:
            db("INSERT OR IGNORE INTO shown_free (user_id, channel_id) VALUES (?,?)", (u.id, cid))
        return

    await update.message.reply_text(
        "👋 Привет дорогой!\n\nПодпишись на @apksided\nВыбери 👇",
        reply_markup=user_menu_kb(u.id))

async def chk_cb(update, ctx):
    q = update.callback_query
    uid = q.from_user.id
    if not FLOOD.check_cb(uid, 1.0):
        return await q.answer("⚠️ Подожди", show_alert=False)
    data = q.data

    # Новый формат: chk_all / chk_all_dl_{game_id}
    if data.startswith("chk_all"):
        parts = data.split("_")
        dl_game_id = None
        if len(parts) > 3 and parts[2] == "dl":
            dl_game_id = int(parts[3])
        missing = await check_all_subs(q.from_user.id, ctx)
        logging.info(f"chk_all uid={q.from_user.id} missing={[m[4] for m in missing]}")
        if not missing:
            uid = q.from_user.id
            existing = {r[0] for r in db("SELECT channel_id FROM channel_members WHERE user_id=? AND status='active'", (uid,))}
            checked = db("SELECT id,chat_id FROM channels WHERE mode != 'free'")
            for cid, chat_id in checked:
                if not chat_id: continue
                if cid not in existing:
                    db("INSERT OR REPLACE INTO channel_members (channel_id,user_id,status) VALUES (?,?,'active')", (cid, uid))
                    db("INSERT INTO channel_stats (channel_id,user_id,action) VALUES (?,?,?)", (cid, uid, "join"))
        if missing:
            await q.answer("❌ Ещё не на всех каналах!", show_alert=True)
            await q.edit_message_text("📌 Подпишись на все каналы:", reply_markup=sub_kb(missing, dl_game_id))
            return
        # Все подписаны
        if dl_game_id:
            g = db("SELECT name,chat_id,message_id FROM games WHERE id=?", (dl_game_id,))
            if g and g[0][2]:
                db("UPDATE games SET downloads=downloads+1, link_opens=link_opens+1 WHERE id=?", (dl_game_id,))
                track_download(q.from_user.id, dl_game_id)
                try: await ctx.bot.send_document(q.message.chat_id, document=g[0][2], caption=f"by @apksided")
                except Exception as e: await q.message.reply_text(f"❌ {e}")
                await q.message.reply_text("Выбери 👇", reply_markup=user_menu_kb(q.from_user.id))
                return
        try:
            await q.edit_message_text("👋 Добро пожаловать!\nВыбери 👇", reply_markup=user_menu_kb(q.from_user.id))
        except:
            await q.message.reply_text("👋 Добро пожаловать!\nВыбери 👇", reply_markup=user_menu_kb(q.from_user.id))
        return

    # Старый формат: chk_{ch_id} / chk_{ch_id}_dl_{game_id}
    parts = data.split("_")
    ch_id = int(parts[1])
    dl_game_id = int(parts[3]) if len(parts) > 3 and parts[2] == "dl" else None
    row = db("SELECT chat_id,link,mode FROM channels WHERE id=?", (ch_id,))
    if not row: await q.answer("❌"); return
    chat_id, link, mode = row[0]

    if chat_id:
        ok = await is_subscribed(q.from_user.id, chat_id, ctx)
        if not ok:
            await q.answer("❌ Ещё не подписан!", show_alert=True); return
        db("INSERT OR REPLACE INTO channel_members (channel_id,user_id,status) VALUES (?,?,'active')", (ch_id, q.from_user.id))

    if dl_game_id:
        g = db("SELECT name,chat_id,message_id FROM games WHERE id=?", (dl_game_id,))
        if g and g[0][2]:
            db("UPDATE games SET downloads=downloads+1, link_opens=link_opens+1 WHERE id=?", (dl_game_id,))
            track_download(q.from_user.id, dl_game_id)
            try: await ctx.bot.send_document(q.message.chat_id, document=g[0][2], caption=f"by @apksided")
            except Exception as e: await q.message.reply_text(f"❌ {e}")
            await q.message.reply_text("Выбери 👇", reply_markup=user_menu_kb(q.from_user.id))
            return

    try:
        await q.edit_message_text("👋 Добро пожаловать!\nВыбери 👇", reply_markup=user_menu_kb(q.from_user.id))
    except:
        await q.message.reply_text("👋 Добро пожаловать!\nВыбери 👇", reply_markup=user_menu_kb(q.from_user.id))

# ─── Админ меню ───────────────────────────────────────────

async def admin_menu(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    r = db("SELECT (SELECT COUNT(*) FROM games), (SELECT COUNT(*) FROM channels), (SELECT COUNT(*) FROM users)")
    n, ch, u = r[0]
    await update.message.reply_text(
        f"⚙️ Админ\n\n📦 {n} файлов\n📢 {ch} каналов | 👥 {u} юзеров",
        reply_markup=admin_kb())

# ─── Текст роутер ─────────────────────────────────────────

async def on_text(update, ctx):
    if not is_admin(update.effective_user.id, 2):
        return await search_text(update, ctx)
    s = st(ctx)
    txt = update.message.text
    if txt == "❌ Отмена":
        st(ctx, S_IDLE); return await update.message.reply_text("Отменено", reply_markup=admin_kb())
    if txt.lower().startswith("пдп"):
        parts = txt.split()
        if len(parts) != 3:
            return await update.message.reply_text("Формат: пдп [ID канала] [число]\n\nID каналов:")
        try:
            ch_id = int(parts[1])
            n = int(parts[2])
        except: return await update.message.reply_text("❌ Числа:")
        row = db("SELECT id,title FROM channels WHERE id=?", (ch_id,))
        if not row: return await update.message.reply_text("❌ Канал не найден")
        db("UPDATE channels SET subs_required=? WHERE id=?", (n, ch_id))
        return await update.message.reply_text(f"✅ {row[0][1]}: нужно {n} подписчиков", reply_markup=admin_kb())
    if txt.lower().startswith("стат+"):
        parts = txt.split()
        if len(parts) != 3:
            return await update.message.reply_text("Формат: стат+ [ID канала] [число]\n\nДобавляет к своей статистике")
        try:
            ch_id = int(parts[1])
            n = int(parts[2])
        except: return await update.message.reply_text("❌ Числа:")
        row = db("SELECT id,title,custom_joins FROM channels WHERE id=?", (ch_id,))
        if not row: return await update.message.reply_text("❌ Канал не найден")
        cur = row[0][2] or 0
        db("UPDATE channels SET custom_joins=? WHERE id=?", (cur + n, ch_id))
        return await update.message.reply_text(f"✅ {row[0][1]}: своя статистика +{n} (итого {cur + n})", reply_markup=admin_kb())
    if txt.lower().startswith("стат-"):
        parts = txt.split()
        if len(parts) != 3:
            return await update.message.reply_text("Формат: стат- [ID канала] [число]\n\nУбавляет из своей статистики")
        try:
            ch_id = int(parts[1])
            n = int(parts[2])
        except: return await update.message.reply_text("❌ Числа:")
        row = db("SELECT id,title,custom_joins FROM channels WHERE id=?", (ch_id,))
        if not row: return await update.message.reply_text("❌ Канал не найден")
        cur = row[0][2] or 0
        db("UPDATE channels SET custom_joins=? WHERE id=?", (max(0, cur - n), ch_id))
        return await update.message.reply_text(f"✅ {row[0][1]}: своя статистика -{n} (итого {max(0, cur - n)})", reply_markup=admin_kb())
    if s == S_NAME:
        ctx.user_data["gn"] = txt; st(ctx, S_DESC)
        return await update.message.reply_text("✏️ Описание (или /skip):")
    if s == S_DESC:
        ctx.user_data["gd"] = txt; return await ask_game_cat(update, ctx)
    if s == S_CATNEW:
        try:
            db("INSERT INTO categories VALUES (NULL,?)", (txt,))
            await update.message.reply_text(f"✅ '{txt}'", reply_markup=admin_kb())
        except: await update.message.reply_text("❌ Уже есть", reply_markup=admin_kb())
        st(ctx, S_IDLE); return
    if s == S_BCAST:
        payload = store_broadcast_payload(update.message, ctx, premium_only=False)
        st(ctx, S_BCAST_BTN)
        return await update.message.reply_text(
            f"{broadcast_preview(payload)}\n\nОтправить как есть, посмотреть предпросмотр или добавить кнопки?",
            reply_markup=broadcast_actions_kb())
    if s == S_BCAST_BTN_ADD:
        buttons = ctx.user_data.get("bcast_buttons", [])
        lines = txt.strip().split("\n")
        if len(lines) >= 2:
            names = [n.strip() for n in lines[0].split("|")]
            urls = [u.strip() for u in lines[1].split("|")]
            for i in range(min(len(names), len(urls))):
                buttons.append([InlineKeyboardButton(names[i], url=urls[i])])
        elif len(lines) == 1 and "|" in lines[0]:
            parts = [p.strip() for p in lines[0].split("|")]
            if len(parts) >= 2:
                buttons.append([InlineKeyboardButton(parts[0], url=parts[1])])
        else:
            return await update.message.reply_text("❌ Формат:\n<code>Текст кнопки</code>\n<code>https://ссылка</code>",
                parse_mode="HTML")
        ctx.user_data["bcast_buttons"] = buttons
        return await update.message.reply_text(
            f"✅ Кнопок: {len(buttons)}\nМожно отправлять, смотреть предпросмотр или добавить ещё.",
            reply_markup=broadcast_actions_kb())
    if s == S_BCAST_P:
        payload = store_broadcast_payload(update.message, ctx, premium_only=True)
        st(ctx, S_BCAST_BTN)
        return await update.message.reply_text(
            f"{broadcast_preview(payload)}\n\nОтправить как есть, посмотреть предпросмотр или добавить кнопки?",
            reply_markup=broadcast_actions_kb())
    if s == S_PREMIUM:
        try: uid = int(txt)
        except: return await update.message.reply_text("❌ Число:")
        db("UPDATE users SET premium=1 WHERE user_id=?", (uid,))
        st(ctx, S_IDLE)
        return await update.message.reply_text(f"✅ Премиум выдан: {uid}", reply_markup=admin_kb())
    if s == S_ADMIN:
        try: uid = int(txt)
        except: return await update.message.reply_text("❌ Число:")
        if len(db("SELECT 1 FROM admins WHERE user_id=?", (uid,))):
            return await update.message.reply_text("⚠️ Уже админ")
        ctx.user_data["new_admin"] = uid
        st(ctx, S_ADMIN_LVL)
        kb = [
            [InlineKeyboardButton("👶 Младший (только файлы)", callback_data="alvl_1")],
            [InlineKeyboardButton("🧑 Средний (всё кроме админов)", callback_data="alvl_2")],
            [InlineKeyboardButton("👨 Старший (всё как владелец)", callback_data="alvl_3")],
        ]
        return await update.message.reply_text(f"🔐 Уровень для {uid}:", reply_markup=InlineKeyboardMarkup(kb))
    if s == S_ADDCH:
        ch_input = txt.strip()
        chat_id = 0
        username = ""
        title = ""

        if ch_input.startswith("-"):
            try: chat_id = int(ch_input)
            except: pass
        else:
            if ch_input.startswith("@"): ch_input = ch_input[1:]
            username = ch_input

        if not (chat_id or username):
            return await update.message.reply_text("❌ Неверный формат. Используй ID (−100...) или @username")

        ctx.user_data["ch_id"] = chat_id
        ctx.user_data["ch_title"] = username or str(chat_id)
        ctx.user_data["ch_username"] = username
        kb = [
            [InlineKeyboardButton("📋 По заявкам (автоприём)", callback_data="cm_approve")],
            [InlineKeyboardButton("🔒 С проверкой подписки", callback_data="cm_check")],
            [InlineKeyboardButton("🔓 Без проверки", callback_data="cm_free")],
        ]
        st(ctx, S_IDLE)
        return await update.message.reply_text(
            f"Выбери режим:",
            reply_markup=InlineKeyboardMarkup(kb))
    if s == S_ADDLINK:
        link = txt.strip()
        if not link:
            return await update.message.reply_text("❌ Ссылка не может быть пустой")
        mode = ctx.user_data.get("ch_mode", "free")
        if mode == "free":
            if not link.startswith("http"):
                link = "https://" + link
        else:
            if not link.startswith("https://t.me/") and not link.startswith("t.me/"):
                return await update.message.reply_text("❌ Неверная ссылка. Формат: https://t.me/... или https://t.me/+...")
            if link.startswith("t.me/"): link = "https://" + link
        ctx.user_data["ch_link"] = link
        title = ctx.user_data.get("ch_title","")
        username = ctx.user_data.get("ch_username","")
        st(ctx, S_CHNAME)
        return await update.message.reply_text(
            f"🔗 {link}\n\n"
            f"📝 Введи имя канала (как показывать пользователям).\n"
            f"Или отправь «-» чтобы оставить: {title or username or 'канал'}")
    if s == S_CHNAME:
        name = txt.strip()
        chat_id = ctx.user_data.get("ch_id",0)
        title = ctx.user_data.get("ch_title","")
        username = ctx.user_data.get("ch_username","")
        link = ctx.user_data.get("ch_link","")
        mode = ctx.user_data.get("ch_mode","free")
        if name and name != "-":
            title = name
        elif not title:
            title = username or "канал"

        # Проверяем, сможет ли бот чекать подписку (нужен chat_id + бот-админ)
        check_ok = False
        warn = ""
        if mode in ("check","approve"):
            if chat_id:
                try:
                    me = await ctx.bot.get_me()
                    m = await ctx.bot.get_chat_member(chat_id, me.id)
                    if m.status in ("administrator","creator"):
                        check_ok = True
                    else:
                        warn = "\n\n⚠️ Бот НЕ админ канала — проверка подписки не будет работать! Добавь бота админом."
                except Exception as e:
                    warn = f"\n\n⚠️ Бот не может проверить канал ({e}). Проверка подписки не будет работать. Добавь бота админом."
            else:
                warn = "\n\n⚠️ Нет chat_id канала — проверка подписки не будет работать. Добавь канал через ID/@username где бот админ."

        db("INSERT INTO channels (chat_id,username,title,link,mode) VALUES (?,?,?,?,?)",
           (chat_id, username, title, link, mode))
        st(ctx, S_IDLE)
        icons = {"approve":"📋 По заявкам","check":"🔒 С проверкой","free":"🔓 Без проверки"}
        status = "✅ Проверка подписки работает" if check_ok else ("🔓 Без проверки" if mode=="free" else "")
        return await update.message.reply_text(
            f"✅ Канал добавлен!\n\n{icons.get(mode,mode)}\n📢 {title}\n🔗 {link}\n{status}{warn}",
            reply_markup=admin_kb())
    return await update.message.reply_text("🤔", reply_markup=admin_kb())

async def skip(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    s = st(ctx)
    if s == S_DESC:
        ctx.user_data["gd"] = ""; return await ask_game_cat(update, ctx)

# ─── Ленивая загрузка ──────────────────────────────────────

async def lazy_file(update, ctx):
    if not is_admin(update.effective_user.id, 1): return
    st(ctx, S_LAZY)
    await update.message.reply_text("📎 Скинь файл:", reply_markup=cancel_kb())

# ─── Добавить файл ────────────────────────────────────────

async def add_file(update, ctx):
    if not is_admin(update.effective_user.id, 1): return
    st(ctx, S_NAME)
    await update.message.reply_text("📝 Название:", reply_markup=cancel_kb())

async def ask_game_cat(update, ctx):
    cats = db("SELECT name FROM categories")
    if not cats:
        await update.message.reply_text("Нет категорий", reply_markup=admin_kb()); st(ctx, S_IDLE); return
    kb = [[InlineKeyboardButton(c[0], callback_data=f"gc_{c[0]}")] for c in cats]
    st(ctx, S_CAT)
    await update.message.reply_text("📂 Категория:", reply_markup=InlineKeyboardMarkup(kb))

async def gc_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    ctx.user_data["gc"] = q.data[3:]
    st(ctx, S_FILE)
    await q.edit_message_text(f"✅ {ctx.user_data['gn']} | {ctx.user_data['gc']}\n\n📎 Файл:")

async def file_rcv(update, ctx):
    if not is_admin(update.effective_user.id, 1): return
    if st(ctx) != S_FILE and st(ctx) != S_LAZY: return
    doc = update.message.document
    if not doc: return await update.message.reply_text("❌ Файл:")
    code = gen_code()
    if st(ctx) == S_LAZY:
        name = doc.file_name or "file"
        cat = "Любая"
        desc = "Подпишись на канал @apksided"
    else:
        name = ctx.user_data.get("gn","?")
        cat = ctx.user_data.get("gc","?")
        desc = ctx.user_data.get("gd","")
    db("INSERT INTO games (name,category,description,chat_id,message_id,file_name,file_size,deep_link) VALUES (?,?,?,?,?,?,?,?)",
       (name, cat, desc, update.message.chat_id, doc.file_id, doc.file_name, doc.file_size, code))
    st(ctx, S_IDLE)
    bot = ctx.bot.username
    await update.message.reply_text(
        f"✅ Добавлено!\n\n🎮 {name}\n📎 {doc.file_name} ({fmt(doc.file_size)})\n\n"
        f"🔗 Ссылка на файл:\nt.me/{bot}?start={code}",
        reply_markup=admin_kb())

# ─── Файлы ────────────────────────────────────────────────

async def list_files(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    games = db("SELECT id,name,category,downloads,link_opens FROM games ORDER BY id DESC LIMIT 20")
    if not games: return await update.message.reply_text("📭 Пусто", reply_markup=admin_kb())
    t = "📋 Файлы:\n\n"
    for g in games: t += f"#{g[0]} {g[1]} [{g[2]}] ⬇{g[3]} 🔗{g[4]}\n"
    await update.message.reply_text(t, reply_markup=admin_kb())

async def del_file_start(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    games = db("SELECT id,name FROM games ORDER BY id DESC LIMIT 20")
    if not games: return await update.message.reply_text("📭", reply_markup=admin_kb())
    kb = [[InlineKeyboardButton(f"#{g[0]} {g[1]}", callback_data=f"df_{g[0]}")] for g in games]
    await update.message.reply_text("🗑:", reply_markup=InlineKeyboardMarkup(kb))

async def df_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    gid = int(q.data[3:])
    g = db("SELECT file_name FROM games WHERE id=?", (gid,))
    if g:
        db("DELETE FROM games WHERE id=?", (gid,))
        await q.edit_message_text(f"✅ {g[0][0]}")
    else: await q.edit_message_text("❌")

# ─── Категории ────────────────────────────────────────────

async def cats_menu(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    cats = db("""SELECT c.name, COUNT(g.id) FROM categories c
        LEFT JOIN games g ON g.category=c.name GROUP BY c.name""")
    t = "📁 Категории:\n\n"
    for name, n in cats:
        t += f"• {name} ({n})\n"
    await update.message.reply_text(t, reply_markup=ReplyKeyboardMarkup([["➕ Категорию"],["⚙️ Меню админа"]], resize_keyboard=True))

async def add_cat(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_CATNEW)
    await update.message.reply_text("📝 Название:", reply_markup=cancel_kb())

# ─── Статистика ───────────────────────────────────────────

def _channel_stats(cid):
    r = db("""SELECT
        (SELECT COUNT(*) FROM channel_members WHERE channel_id=? AND status='active'),
        (SELECT COUNT(*) FROM channel_stats WHERE channel_id=? AND action='join(link)'),
        (SELECT COUNT(*) FROM channel_members WHERE channel_id=? AND status='active'
         AND user_id IN (SELECT user_id FROM channel_stats WHERE channel_id=? AND action='join(link)'))
    """, (cid, cid, cid, cid))
    return r[0] if r else (0, 0, 0)

async def my_stats(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    chs = db("SELECT id,title,mode,custom_joins,custom_leaves FROM channels WHERE mode IN ('check','approve')")
    if not chs:
        return await update.message.reply_text("📭 Каналов нет", reply_markup=admin_kb())
    t = "📊 Своя статистика:\n\n"
    for cid, title, mode, cj, cl in chs:
        icon = "🔒" if mode == "check" else "📋"
        active, total_link, cur_link = _channel_stats(cid)
        now_total = max(0, active + (cj or 0) - (cl or 0))
        t += (
            f"{icon} {title}\n"
            f"Сейчас на подписке: {now_total}\n"
            f"Сейчас по ссылке: {cur_link}\n"
            f"Всего входов по ссылке: {total_link}\n\n"
        )
    t += "Команды:\nстат+ [ID] [число] — добавить\nстат- [ID] [число] — убавить"
    await update.message.reply_text(t, reply_markup=admin_kb())

async def stats(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    r = db("SELECT (SELECT COUNT(*) FROM games), (SELECT COALESCE(SUM(downloads),0) FROM games), (SELECT COUNT(*) FROM users)")
    n, dl, u = r[0]
    chs = db("SELECT id,title,mode,custom_joins,custom_leaves FROM channels")
    t = f"📊\n📦 {n} файлов\n⬇️ {dl} скачиваний | 👥 {u} юзеров\n\n📢 Каналы:\n"
    for cid, title, mode, cj, cl in chs:
        active, total_link, cur_link = _channel_stats(cid)
        now_total = max(0, active + (cj or 0) - (cl or 0))
        t += (
            f"• {title} [{mode}]\n"
            f"   Сейчас на подписке: {now_total}\n"
            f"   Сейчас по ссылке: {cur_link}\n"
            f"   Всего входов по ссылке: {total_link}\n"
        )
    await update.message.reply_text(t, reply_markup=admin_kb())

async def export_db(update, ctx):
    if update.effective_user.id != ADMIN_ID: return
    users = db("SELECT user_id, username, first_name FROM users ORDER BY user_id")
    if not users:
        return await update.message.reply_text("📭 Пусто", reply_markup=admin_kb())
    lines = []
    for uid, un, fn in users:
        name = f"@{un}" if un else ""
        if fn: name += f" ({fn})"
        lines.append(f"{uid} | {name.strip() if name else 'нет данных'}")
    txt = "\n".join(lines)
    path = os.path.join(os.path.dirname(__file__), "users_export.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(txt)
    await ctx.bot.send_document(update.message.chat_id, document=open(path, "rb"),
        caption=f"📦 Экспорт: {len(users)} юзеров")
    os.remove(path)

# ─── Фрод ─────────────────────────────────────────────────

FRAUD_THRESHOLD = 50

async def fraud_list(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    bots = db("SELECT user_id, score, signals FROM fraud_scores WHERE score>=? ORDER BY score DESC", (FRAUD_THRESHOLD,))
    if not bots:
        return await update.message.reply_text("✅ Подозрительных юзеров нет", reply_markup=admin_kb())
    today = time.strftime("%Y-%m-%d")
    uids = [b[0] for b in bots[:30]]
    ph = ",".join("?" for _ in uids)
    names = {r[0]: r[1:] for r in db(f"SELECT user_id, username, first_name FROM users WHERE user_id IN ({ph})", uids)}
    dls = {r[0]: r[1] for r in db(f"SELECT user_id, COALESCE(SUM(count),0) FROM user_downloads WHERE user_id IN ({ph}) AND date=? GROUP BY user_id", (*uids, today))}
    t = f"🛡 Подозрительные ({len(bots)}):\n\n"
    for uid, score, sig in bots[:30]:
        u = names.get(uid)
        name = f"@{u[0]}" if u and u[0] else str(uid)
        if u and u[1]: name += f" ({u[1]})"
        dlc = dls.get(uid, 0)
        t += f"🔴 {name} | score:{score} | сегодня:{dlc}\n  {sig}\n"
    kb = [[InlineKeyboardButton(f"🚫 Забанить все ({len(bots)})", callback_data="fraud_ban")]]
    await update.message.reply_text(t, reply_markup=InlineKeyboardMarkup(kb))

async def fraud_ban(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    bots = db("SELECT user_id FROM fraud_scores WHERE score>=?", (FRAUD_THRESHOLD,))
    if not bots:
        return await update.message.reply_text("✅ Нет кого банить", reply_markup=admin_kb())
    count = 0
    for (uid,) in bots:
        try:
            await ctx.bot.ban_chat_member(chat_id=update.message.chat_id, user_id=uid)
            count += 1
        except: pass
    await update.message.reply_text(f"✅ Забанено: {count} из {len(bots)}", reply_markup=admin_kb())

async def fraud_ban_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    bots = db("SELECT user_id FROM fraud_scores WHERE score>=?", (FRAUD_THRESHOLD,))
    if not bots:
        return await q.edit_message_text("✅ Нет кого банить", reply_markup=admin_kb())
    count = 0
    for (uid,) in bots:
        try:
            await ctx.bot.ban_chat_member(chat_id=q.message.chat_id, user_id=uid)
            count += 1
        except: pass
    await q.edit_message_text(f"✅ Забанено: {count} из {len(bots)}", reply_markup=admin_kb())

# ─── Рассылка (inline кнопки) ─────────────────────────────

async def bc_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    data = q.data[3:]
    if data == "preview":
        payload = ctx.user_data.get("bcast_payload")
        if not payload:
            return await q.message.reply_text("❌ Сначала отправь текст или медиа для рассылки", reply_markup=admin_kb())
        buttons = ctx.user_data.get("bcast_buttons", [])
        markup = InlineKeyboardMarkup(buttons) if buttons else None
        await preview_broadcast_payload(ctx, q.message.chat_id, payload, markup)
    elif data in ("send", "done"):
        payload = ctx.user_data.get("bcast_payload")
        if not payload:
            st(ctx, S_IDLE)
            return await q.message.reply_text("❌ Сначала отправь текст или медиа для рассылки", reply_markup=admin_kb())
        buttons = ctx.user_data.get("bcast_buttons", [])
        markup = InlineKeyboardMarkup(buttons) if buttons else None
        target = ctx.user_data.get("bcast_target", "all")
        users = get_premium_users() if target == "premium" else get_users()
        skip_premium = target == "all"
        start_text = "📢 Рассылка премиум началась..." if target == "premium" else "📢 Рассылка началась..."
        st(ctx, S_IDLE)
        ctx.user_data.pop("bcast_payload", None)
        ctx.user_data.pop("bcast_buttons", None)
        ctx.user_data.pop("bcast_target", None)
        await q.edit_message_text(f"{start_text}\n\n⏳ Запущена фоновая задача...")

        async def bg_broadcast():
            ok, dead = await fast_broadcast(ctx, users, payload, markup, skip_premium=skip_premium, progress_chat_id=q.message.chat_id)
            try:
                await ctx.bot.send_message(q.message.chat_id, reply_markup=admin_kb())
            except: pass

        asyncio.create_task(bg_broadcast())
    elif data == "add_btn":
        st(ctx, S_BCAST_BTN_ADD)
        await q.edit_message_text(
            "🔗 Отправь кнопку в формате:\n\n"
            "<b>Текст кнопки</b>\n<code>https://ссылка</code>\n\n"
            "Или несколько кнопок (каждая с новой строки):\n"
            "<code>Кнопка 1 | Кнопка 2</code>\n<code>https://a.com | https://b.com</code>",
            parse_mode="HTML")
    elif data == "cancel":
        st(ctx, S_IDLE)
        ctx.user_data.pop("bcast_payload", None)
        ctx.user_data.pop("bcast_buttons", None)
        ctx.user_data.pop("bcast_target", None)
        await q.edit_message_text("❌ Рассылка отменена")
        await q.message.reply_text("Возврат в меню админа", reply_markup=admin_kb())

# ─── Уровень админа ────────────────────────────────────────

async def alvl_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 3): return
    level = int(q.data[5:])
    uid = ctx.user_data.get("new_admin")
    if not uid: return await q.edit_message_text("❌ Повтори")
    levels = {1: "👶 Младший", 2: "🧑 Средний", 3: "👨 Старший"}
    db("INSERT OR REPLACE INTO admins (user_id,level) VALUES (?,?)", (uid, level))
    st(ctx, S_IDLE)
    await q.edit_message_text(f"✅ {uid} — {levels.get(level, '?')}")

# ─── Рассылка ─────────────────────────────────────────────

async def bcast_start(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_BCAST)
    await update.message.reply_text(
        f"📢 Рассылка ({len(get_users())} юзеров)\n\n"
        "Отправь текст, одно медиа или альбом.\n"
        "Форматирование и премиум-эмодзи будут сохранены.",
        reply_markup=cancel_kb())

# ─── Админы ───────────────────────────────────────────────

async def admins_start(update, ctx):
    if not is_admin(update.effective_user.id, 3): return
    st(ctx, S_ADMIN)
    adm = db("SELECT user_id, level FROM admins")
    levels = {1: "👶 Младший", 2: "🧑 Средний", 3: "👨 Старший"}
    t = "👤 Админы:\n"
    if adm:
        for a in adm: t += f"• {a[0]} [{levels.get(a[1], '?')}]\n"
    else:
        t += "нет"
    t += "\n📝 ID:"
    await update.message.reply_text(t, reply_markup=cancel_kb())

# ─── Каналы (ОП) ─────────────────────────────────────────

async def ch_menu(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    chs = db("""SELECT c.id,c.title,c.mode,c.link,
        (SELECT COUNT(*) FROM channel_members WHERE channel_id=c.id AND status='active')
        FROM channels c""")
    if not chs:
        await update.message.reply_text("📢 Каналы\n\nПусто",
            reply_markup=ReplyKeyboardMarkup([["➕ Канал"],["⚙️ Меню админа"]], resize_keyboard=True))
        return
    t = "📢 Каналы:\n\n"
    for c in chs:
        active = c[4]
        if c[2]=="approve": icon="📋"
        elif c[2]=="check": icon="🔒"
        else: icon="🔓"
        t += f"{icon} {c[1]} | 👤 {active}\n🔗 {c[3]}\n\n"
    await update.message.reply_text(t, reply_markup=ReplyKeyboardMarkup(
        [["➕ Канал","📊 Детали","🗑 Канал"],["🔗 Пересоздать ссылки"],["⚙️ Меню админа"]], resize_keyboard=True))

async def regen_links(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    chs = db("SELECT id,title,mode,chat_id FROM channels WHERE chat_id != 0")
    if not chs:
        return await update.message.reply_text("📭 Каналов нет", reply_markup=admin_kb())
    await update.message.reply_text("🔗 Создаю ссылки для точного подсчёта переходов...")
    res = []
    for cid, title, mode, chat_id in chs:
        try:
            if mode == "approve":
                obj = await ctx.bot.create_chat_invite_link(chat_id, name="bot", creates_join_request=True)
            else:
                obj = await ctx.bot.create_chat_invite_link(chat_id, name="bot")
            db("UPDATE channels SET link=? WHERE id=?", (obj.invite_link, cid))
            res.append(f"✅ {title}\n{obj.invite_link}")
        except Exception as e:
            res.append(f"❌ {title}: не удалось ({e})")
            logging.error(f"regen_links fail chat={chat_id}: {e}")
    await update.message.reply_text(
        "🔗 Готово:\n\n" + "\n\n".join(res) +
        "\n\nТеперь бот использует отдельные invite-link, чтобы точнее считать входы именно по своей ссылке.",
        reply_markup=admin_kb())

async def add_ch(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_ADDCH)
    await update.message.reply_text(
        "📝 Отправь ID канала или @username:\n\n"
        "• −1001234567890\n"
        "• @mychannel",
        reply_markup=cancel_kb())

async def ch_mode_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    mode = q.data[3:]
    chat_id = ctx.user_data.get("ch_id",0)
    username = ctx.user_data.get("ch_username","")
    title = ctx.user_data.get("ch_title","")
    ctx.user_data["ch_mode"] = mode

    if mode == "free":
        st(ctx, S_ADDLINK)
        await q.edit_message_text(
            f"🔓 Без проверки\n\n📝 Отправь ссылку на канал:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="ch_cancel")]]))
        return

    if not chat_id:
        await q.edit_message_text("❌ Канал не найден. Заново: ➕ Канал")
        return

    try:
        target = chat_id if chat_id else f"@{username}"
        chat = await ctx.bot.get_chat(target)
        chat_id = chat.id
        title = chat.title or ""
        username = chat.username or ""
        ctx.user_data["ch_id"] = chat_id
        ctx.user_data["ch_title"] = title
        ctx.user_data["ch_username"] = username
    except Exception as e:
        logging.warning(f"get_chat failed for {chat_id or username}: {e}")
        await q.edit_message_text(
            f"❌ Бот не может найти канал. Убедись что бот — админ канала.\n\nОшибка: {e}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="ch_cancel")]]))
        return

    icons = {"approve":"📋 По заявкам","check":"🔒 С проверкой"}
    kb = [
        [InlineKeyboardButton("🤖 Бот создаст ссылку", callback_data="clsrc_auto")],
        [InlineKeyboardButton("✍️ Я дам свою ссылку", callback_data="clsrc_manual")],
    ]
    await q.edit_message_text(
        f"{icons.get(mode,mode)}\n📢 {title}\n\nОткуда взять ссылку?",
        reply_markup=InlineKeyboardMarkup(kb))

async def ch_cancel_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    st(ctx, S_IDLE)
    await q.edit_message_text("❌ Отменено")
    await q.message.reply_text("Возврат в меню", reply_markup=admin_kb())

async def free_done_cb(update, ctx):
    q = update.callback_query; await q.answer()
    uid = q.from_user.id
    await q.edit_message_text("👋 Добро пожаловать!\nВыбери 👇", reply_markup=user_menu_kb(uid))

async def ch_linksrc_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    src = q.data[6:]
    chat_id = ctx.user_data.get("ch_id",0)
    title = ctx.user_data.get("ch_title","")
    username = ctx.user_data.get("ch_username","")
    mode = ctx.user_data.get("ch_mode","free")
    if not chat_id:
        await q.edit_message_text("❌ Канал не найден. Заново: ➕ Канал")
        return

    # Ручной ввод ссылки — для любого режима
    if src == "manual":
        st(ctx, S_ADDLINK)
        await q.edit_message_text(
            f"📢 {title}\n\n"
            f"📝 Отправь свою ссылку (https://t.me/... или https://t.me/+...):")
        return

    # Автосоздание ссылки
    ch_link = ""
    try:
        if mode == "approve":
            obj = await ctx.bot.create_chat_invite_link(chat_id, name="bot", creates_join_request=True)
            ch_link = obj.invite_link
        else:
            obj = await ctx.bot.create_chat_invite_link(chat_id, name="bot")
            ch_link = obj.invite_link
    except Exception as e:
        logging.error(f"Link create error: {e}")
        ch_link = ""

    if not ch_link and username:
        ch_link = f"https://t.me/{username}"

    if not ch_link:
        st(ctx, S_ADDLINK)
        await q.edit_message_text(
            f"⚠️ Бот не смог создать ссылку (не админ / приватный канал).\n\n"
            f"📝 Отправь ссылку вручную (https://t.me/... или https://t.me/+...):")
        return

    ctx.user_data["ch_link"] = ch_link
    st(ctx, S_CHNAME)
    await q.edit_message_text(
        f"🔗 {ch_link}\n\n"
        f"📝 Введи имя канала (как показывать пользователям).\n"
        f"Или отправь «-» чтобы оставить: {title or username or 'канал'}")

async def del_ch_start(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    chs = db("SELECT id,title FROM channels")
    if not chs: return await update.message.reply_text("📭", reply_markup=admin_kb())
    kb = [[InlineKeyboardButton(f"{c[1]}", callback_data=f"dch_{c[0]}")] for c in chs]
    await update.message.reply_text("🗑:", reply_markup=InlineKeyboardMarkup(kb))

async def dch_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    st(ctx, S_IDLE)
    cid = int(q.data[4:])
    c = db("SELECT title FROM channels WHERE id=?", (cid,))
    if c: db("DELETE FROM channels WHERE id=?", (cid,)); await q.edit_message_text(f"✅ {c[0][0]}")
    else: await q.edit_message_text("❌")

async def ch_details(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    chs = db("SELECT id,title,mode FROM channels")
    if not chs: return await update.message.reply_text("📭", reply_markup=admin_kb())
    kb = [[InlineKeyboardButton(f"{c[1]} [{c[2]}]", callback_data=f"chdet_{c[0]}")] for c in chs]
    await update.message.reply_text("📊:", reply_markup=InlineKeyboardMarkup(kb))

async def chdet_cb(update, ctx):
    q = update.callback_query; await q.answer()
    cid = int(q.data[6:])
    c = db("SELECT title,mode,link,subs_required,custom_joins,custom_leaves FROM channels WHERE id=?", (cid,))
    if not c: return await q.message.reply_text("❌")
    ch = c[0]
    cj = ch[4] or 0
    cl = ch[5] or 0
    active, total_link, cur_link = _channel_stats(cid)
    now_total = max(0, active + cj - cl)
    recent = db("SELECT user_id,action,ts FROM channel_stats WHERE channel_id=? ORDER BY ts DESC LIMIT 10", (cid,))
    if ch[1]=="approve": mode="Заявки"
    elif ch[1]=="check": mode="Подписка"
    else: mode="Открытый"
    t = (
        f"Статистика: {ch[0]}\n\n"
        f"Режим: {mode}\n"
        f"Сейчас на подписке: {now_total}\n"
        f"Сейчас по ссылке: {cur_link}\n"
        f"Всего входов по ссылке: {total_link}"
    )
    if ch[1]=="check" and ch[3]: t += f"\nНужно подписчиков: {ch[3]}"
    t += f"\nСсылка: {ch[2]}\n\nПоследние действия:\n"
    for r in recent:
        act = "по ссылке" if r[1]=="join(link)" else ("присоединился" if r[1] in ("join","join(auto)") else "вышел")
        t += f"  {r[0]} — {act} — {r[2]}\n"
    if not recent: t += "  пусто\n"
    kb = [[InlineKeyboardButton("📋 Копировать ссылку", callback_data=f"clink_{cid}")]]
    try:
        await q.edit_message_text(t, reply_markup=InlineKeyboardMarkup(kb))
    except:
        await q.message.reply_text(t, reply_markup=InlineKeyboardMarkup(kb))

async def clink_cb(update, ctx):
    q = update.callback_query
    cid = int(q.data[6:])
    row = db("SELECT link FROM channels WHERE id=?", (cid,))
    if row: await q.answer(f"📋 {row[0][0]}", show_alert=True)
    else: await q.answer("❌", show_alert=True)

# ─── Автоприём заявок ─────────────────────────────────────

async def auto_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    ch_id = req.chat.id
    uid = req.from_user.id
    row = db("SELECT id,mode,link FROM channels WHERE chat_id=?", (ch_id,))
    used_link = req.invite_link.invite_link if req.invite_link else None
    logging.info(f"Join request: user={uid} chat={ch_id} used_link={used_link} db_match={row}")
    if not row:
        return
    bot_link = row[0][2]
    via_bot_link = bool(used_link and bot_link and used_link == bot_link)
    try:
        await ctx.bot.approve_chat_join_request(ch_id, uid)
        db("INSERT OR REPLACE INTO channel_members (channel_id,user_id,status) VALUES (?,?,'active')", (row[0][0], uid))
        action = "join(link)" if via_bot_link else "join(auto)"
        db("INSERT INTO channel_stats (channel_id,user_id,action) VALUES (?,?,?)", (row[0][0], uid, action))
        logging.info(f"Approved: user={uid} chat={ch_id} via_bot_link={via_bot_link}")
    except Exception as e:
        logging.error(f"Auto-approve failed: user={uid} chat={ch_id}: {e}")

# ─── Трекинг ──────────────────────────────────────────────

async def track_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.chat_member: return
    mc = update.chat_member
    ch_id = mc.chat.id
    old = mc.old_chat_member.status
    new = mc.new_chat_member.status
    uid = mc.new_chat_member.user.id
    ch_row = db("SELECT id,link FROM channels WHERE chat_id=?", (ch_id,))
    if not ch_row: return
    cid = ch_row[0][0]
    bot_link = ch_row[0][1]
    if old in ["left","kicked"] and new in ["member","administrator","creator"]:
        used_link = mc.invite_link.invite_link if mc.invite_link else None
        via_bot_link = bool(used_link and bot_link and used_link == bot_link)
        db("INSERT OR REPLACE INTO channel_members (channel_id,user_id,status) VALUES (?,?,'active')", (cid, uid))
        action = "join(link)" if via_bot_link else "join"
        db("INSERT INTO channel_stats (channel_id,user_id,action) VALUES (?,?,?)", (cid, uid, action))
        logging.info(f"track_member join uid={uid} chat={ch_id} used_link={used_link} via_bot_link={via_bot_link}")
    elif old in ["member","administrator","creator"] and new in ["left","kicked"]:
        db("UPDATE channel_members SET status='left' WHERE channel_id=? AND user_id=?", (cid, uid))
        db("INSERT INTO channel_stats (channel_id,user_id,action) VALUES (?,?,?)", (cid, uid, "leave"))

# ─── Периодическая проверка подписчиков ────────────────────

async def check_members(ctx):
    channels = db("SELECT id, chat_id FROM channels WHERE chat_id != 0")
    for cid, chat_id in channels:
        members = db("SELECT user_id FROM channel_members WHERE channel_id=? AND status='active'", (cid,))
        for (uid,) in members:
            try:
                m = await ctx.bot.get_chat_member(chat_id, uid)
                if m.status in ["left","kicked"]:
                    db("UPDATE channel_members SET status='left' WHERE channel_id=? AND user_id=?", (cid, uid))
                    db("INSERT INTO channel_stats (channel_id,user_id,action) VALUES (?,?,?)", (cid, uid, "leave"))
            except Exception as e:
                logging.warning(f"check_members skip uid={uid} chat={chat_id}: {e}")
                continue

# ─── Премиум ─────────────────────────────────────────────

PREMIUM_PRICE = 150  # Stars

async def premium_cb(update, ctx):
    q = update.callback_query; await q.answer()
    uid = q.from_user.id
    if is_premium(uid):
        await q.edit_message_text("💎 У тебя уже отключена реклама!\n\nВсе каналы и рассылки отключены.")
        return
    await ctx.bot.send_invoice(
        chat_id=q.message.chat_id,
        title="🚫 Отключить рекламу",
        description="Без проверки каналов и без рассылки — навсегда",
        payload=f"premium_{uid}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice("Отключить рекламу", PREMIUM_PRICE)],
        start_parameter="disable-ads"
    )

async def pre_checkout_cb(update, ctx):
    q = update.pre_checkout_query
    if q.invoice_payload.startswith("premium_"):
        await q.answer(ok=True)
    else:
        await q.answer(ok=False, error_message="Ошибка оплаты")

async def successful_payment_cb(update, ctx):
    p = update.message.successful_payment
    uid = update.effective_user.id
    charge_id = p.telegram_payment_charge_id
    payload = p.invoice_payload
    db("INSERT INTO payments (user_id,charge_id,payload,amount) VALUES (?,?,?,?)",
       (uid, charge_id, payload, p.total_amount))
    if payload.startswith("premium_"):
        db("UPDATE users SET premium=1 WHERE user_id=?", (uid,))
    await update.message.reply_text(
        f"✅ Оплата прошла!\n\n🚫 Реклама отключена навсегда.\n"
        f"Теперь нет проверки каналов и ты не в рассылке.")

# ─── Админ: Премиум ──────────────────────────────────────

async def prem_start(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    prem = get_premium_users()
    t = "💎 Премиум:\n" + ("\n".join(f"• {a[0]}" for a in prem) if prem else "нет")
    st(ctx, S_PREMIUM)
    await update.message.reply_text(t + "\n\n📝 ID юзера:", reply_markup=cancel_kb())

async def bcast_prem_start(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    prem = get_premium_users()
    st(ctx, S_BCAST_P)
    await update.message.reply_text(
        f"📢 Рассылка премиум ({len(prem)} юзеров)\n\n"
        "Отправь текст, одно медиа или альбом.\n"
        "Форматирование и премиум-эмодзи будут сохранены.",
        reply_markup=cancel_kb())

# ─── Отмена ───────────────────────────────────────────────

async def cancel(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    st(ctx, S_IDLE)
    await update.message.reply_text("Отменено", reply_markup=admin_kb())

# ─── Юзер: Каталог ────────────────────────────────────────

async def catalog_cb(update, ctx):
    q = update.callback_query; await q.answer()
    cats = db("SELECT DISTINCT category FROM games")
    if not cats: return await q.edit_message_text("📭", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️", callback_data="back")]]))
    kb = [[InlineKeyboardButton(c[0], callback_data=f"uc_{c[0]}")] for c in cats]
    kb.append([InlineKeyboardButton("🔍 Поиск", callback_data="search")])
    kb.append([InlineKeyboardButton("◀️", callback_data="back")])
    await q.edit_message_text("📂:", reply_markup=InlineKeyboardMarkup(kb))

async def ucat_cb(update, ctx):
    q = update.callback_query; await q.answer()
    cat = q.data[3:]
    games = db("SELECT id,name,downloads,file_size FROM games WHERE category=? ORDER BY id DESC", (cat,))
    if not games: return await q.edit_message_text(f"📭 {cat}")
    kb = [[InlineKeyboardButton(f"⬇️ {g[1]} ({fmt(g[3])})", callback_data=f"dl_{g[0]}")] for g in games]
    kb.append([InlineKeyboardButton("◀️", callback_data="catalog")])
    await q.edit_message_text(f"📂 {cat}", reply_markup=InlineKeyboardMarkup(kb))

async def dl_cb(update, ctx):
    q = update.callback_query; await q.answer()
    uid = q.from_user.id
    if not FLOOD.check_cb(uid): return
    gid = int(q.data[3:])
    g = db("SELECT name,chat_id,message_id,file_name,file_size,downloads FROM games WHERE id=?", (gid,))
    if not g: return await q.edit_message_text("❌")
    db("UPDATE games SET downloads=downloads+1 WHERE id=?", (gid,))
    track_download(uid, gid)
    await check_fraud(uid, ctx)
    likes = like_count(gid)
    liked = "❤️" if user_liked(uid, gid) else "🤍"
    kb = [[InlineKeyboardButton(f"{liked} {likes}", callback_data=f"like_{gid}")]]
    try: await ctx.bot.send_document(q.message.chat_id, document=g[0][2], caption=f"by @apksided", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e: await q.message.reply_text(f"❌ {e}")

# ─── Назад / Поиск ────────────────────────────────────────

async def back_cb(update, ctx):
    q = update.callback_query; await q.answer()
    await q.edit_message_text("👋 Выбирай 👇", reply_markup=user_menu_kb(q.from_user.id))

async def broadcast_media_rcv(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    s = st(ctx)
    if s not in (S_BCAST, S_BCAST_P):
        return await file_rcv(update, ctx)
    premium_only = (s == S_BCAST_P)
    if update.message.media_group_id:
        item = extract_album_item(update.message)
        if not item:
            return await update.message.reply_text(
                "❌ Для альбома поддерживаются фото, видео, документы и аудио",
                reply_markup=cancel_kb())
        cache = ctx.application.bot_data.setdefault("broadcast_media_groups", {})
        key = (update.effective_user.id, update.message.media_group_id)
        group = cache.get(key)
        if not group:
            group = {
                "items": [],
                "caption": "",
                "caption_entities": [],
                "chat_id": update.message.chat_id,
                "target": "premium" if premium_only else "all",
            }
            cache[key] = group
            ctx.application.create_task(finalize_broadcast_album(
                ctx,
                update.effective_user.id,
                update.message.media_group_id,
            ))
            await update.message.reply_text("📥 Собираю альбом...")
        group["items"].append(item)
        if update.message.caption and not group.get("caption"):
            group["caption"] = update.message.caption
            group["caption_entities"] = update.message.caption_entities or []
        return
    payload = store_broadcast_payload(update.message, ctx, premium_only=(s == S_BCAST_P))
    if not payload:
        return await update.message.reply_text(
            "❌ Поддерживаются текст, фото, видео, GIF, аудио и документы",
            reply_markup=cancel_kb())
    st(ctx, S_BCAST_BTN)
    await update.message.reply_text(
        f"{broadcast_preview(payload)}\n\nОтправить как есть, посмотреть предпросмотр или добавить кнопки?",
        reply_markup=broadcast_actions_kb())

async def search_cb(update, ctx):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🔍 Название:")

async def search_text(update, ctx):
    t = update.message.text
    games = db("SELECT id,name,category,file_size FROM games WHERE name LIKE ? LIMIT 10", (f"%{t}%",))
    if not games: return await update.message.reply_text("🔍 Ничего")
    kb = [[InlineKeyboardButton(f"⬇️ {g[1]} [{g[2]}]", callback_data=f"dl_{g[0]}")] for g in games]
    kb.append([InlineKeyboardButton("◀️", callback_data="catalog")])
    await update.message.reply_text(f"🔍 '{t}':", reply_markup=InlineKeyboardMarkup(kb))

# ─── Лайки ────────────────────────────────────────────────

async def like_cb(update, ctx):
    q = update.callback_query
    uid = q.from_user.id
    if not FLOOD.check_cb(uid, 1.0): return await q.answer("⚠️ Подожди", show_alert=False)
    gid = int(q.data[5:])
    liked = toggle_like(uid, gid)
    cnt = like_count(gid)
    heart = "❤️" if liked else "🤍"
    await q.answer(f"{heart} {cnt}")
    try:
        await q.edit_message_reply_markup(InlineKeyboardMarkup([[InlineKeyboardButton(f"{heart} {cnt}", callback_data=f"like_{gid}")]]))
    except: pass

# ─── История скачиваний ──────────────────────────────────

async def history_cmd(update, ctx):
    uid = update.effective_user.id
    if not FLOOD.check(uid, 2):
        return await update.message.reply_text("⚠️ Подожди немного")
    rows = db("""
        SELECT g.id, g.name, g.category, g.file_size, ud.date, ud.count
        FROM user_downloads ud
        JOIN games g ON g.id = ud.game_id
        WHERE ud.user_id=? AND ud.game_id > 0
        ORDER BY ud.date DESC
        LIMIT 20
    """, (uid,))
    if not rows:
        total = db("SELECT COALESCE(SUM(count),0) FROM user_downloads WHERE user_id=?", (uid,))[0][0]
        return await update.message.reply_text(f"📭 Пока нет истории файлов.\nВсего скачиваний: {total}")
    t = "📥 Твоя история:\n\n"
    for gid, name, cat, fsize, date, cnt in rows:
        t += f"• {name} [{cat}] — {fmt(fsize)} — {date} (×{cnt})\n"
    await update.message.reply_text(t)

# ─── Статистика за период ─────────────────────────────────

async def stats_period(update, ctx):
    if not is_admin(update.effective_user.id, 2): return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Сегодня", callback_data="speriod_day")],
        [InlineKeyboardButton("📅 Неделя", callback_data="speriod_week")],
        [InlineKeyboardButton("📅 Месяц", callback_data="speriod_month")],
        [InlineKeyboardButton("📅 Всё время", callback_data="speriod_all")],
    ])
    await update.message.reply_text("📊 Статистика за период:", reply_markup=kb)

async def speriod_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id, 2): return
    period = q.data[8:]
    now = time.time()
    if period == "day":
        since = time.strftime("%Y-%m-%d", time.localtime(now))
        label = "Сегодня"
    elif period == "week":
        since = time.strftime("%Y-%m-%d", time.localtime(now - 7*86400))
        label = "За неделю"
    elif period == "month":
        since = time.strftime("%Y-%m-%d", time.localtime(now - 30*86400))
        label = "За месяц"
    else:
        since = "2000-01-01"
        label = "Всё время"

    new_users = db("SELECT COUNT(*) FROM users WHERE created_at >= ?", (since,))[0][0]
    r = db("SELECT COALESCE(SUM(count),0), COUNT(DISTINCT user_id) FROM user_downloads WHERE date >= ?", (since,))
    downloads, active_users = r[0]
    top_files = db("""
        SELECT g.name, SUM(ud.count) as total
        FROM user_downloads ud
        JOIN games g ON g.id = ud.game_id
        WHERE ud.date >= ? AND ud.game_id > 0
        GROUP BY ud.game_id
        ORDER BY total DESC
        LIMIT 5
    """, (since,))

    t = (
        f"📊 {label}\n\n"
        f"👥 Новых юзеров: {new_users}\n"
        f"📥 Скачиваний: {downloads}\n"
        f"👤 Активных: {active_users}\n"
    )
    if top_files:
        t += "\n🏆 Топ файлов:\n"
        for name, total in top_files:
            t += f"  • {name} — {total}\n"
    await q.edit_message_text(t)

# ─── Main ─────────────────────────────────────────────────

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CommandHandler("skip", skip))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("history", history_cmd))

    app.add_handler(PreCheckoutQueryHandler(pre_checkout_cb))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_cb))

    for txt, fn in [
        ("⚙️ Меню админа", admin_menu), ("➕ Файл", add_file), ("🚀 Ленивая загрузка", lazy_file),
        ("📋 Файлы", list_files), ("🗑 Файлы", del_file_start),
        ("📢 Каналы", ch_menu), ("➕ Канал", add_ch), ("🗑 Канал", del_ch_start), ("🔗 Пересоздать ссылки", regen_links),
        ("📊 Стат", stats), ("📊 Моя стат", my_stats), ("📊 Детали", ch_details),
        ("📊 За период", stats_period),
        ("📢 Рассылка", bcast_start), ("👤 Админы", admins_start), ("🛡 Фрод", fraud_list), ("📦 Экспорт БД", export_db),
        ("💎 Премиум", prem_start), ("📢 Рассылка ПМ", bcast_prem_start),
        ("➕ Категорию", add_cat), ("📁 Категории", cats_menu),
        ("❌ Отмена", cancel),
    ]:
        app.add_handler(MessageHandler(filters.Text([txt]), fn))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.Document.ALL, file_rcv))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.AUDIO, broadcast_media_rcv))

    for pat, fn in [
        ("chk_", chk_cb), ("catalog$", catalog_cb), ("back$", back_cb),
        ("uc_", ucat_cb), ("dl_", dl_cb),
        ("gc_", gc_cb), ("df_", df_cb), ("dch_", dch_cb), ("chdet_", chdet_cb), ("clink_", clink_cb),
        ("search$", search_cb), ("cm_", ch_mode_cb), ("clsrc_", ch_linksrc_cb),
        ("ch_cancel$", ch_cancel_cb),
        ("free_done$", free_done_cb),
        ("premium$", premium_cb),
        ("fraud_ban$", fraud_ban_cb),
        ("alvl_", alvl_cb),
        ("bc_", bc_cb),
        ("like_", like_cb),
        ("speriod_", speriod_cb),
    ]:
        app.add_handler(CallbackQueryHandler(fn, pattern=f"^{pat}"))

    app.add_handler(ChatMemberHandler(track_member, ChatMemberHandler.CHAT_MEMBER), group=1)
    app.add_handler(ChatJoinRequestHandler(auto_approve), group=1)

    app.job_queue.run_repeating(check_members, interval=60, first=10)
    app.job_queue.run_repeating(lambda ctx: FLOOD.cleanup(), interval=300, first=60)

    print("Bot started!")
    app.run_polling()

if __name__ == "__main__":
    main()
