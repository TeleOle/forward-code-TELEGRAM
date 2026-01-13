# 🤖 Multi-User Telegram Auto-Forward Bot

A powerful Telegram bot + user-account hybrid that automatically forwards messages
between channels and groups with **advanced filters, caption cleaning, watermarking,
and multi-account support**.

---

## 🚀 Features

✅ Multiple source → destination rules  
✅ Telethon (MTProto) for **2GB+ file forwarding**  
✅ Copy mode & Forward mode  
✅ Caption cleaning (hashtags, links, emojis, mentions, phones, emails)  
✅ Album (grouped media) handling  
✅ Text & Logo watermark (Image + Video via FFmpeg)  
✅ Per-user rules stored in SQLite  
✅ Duplicate file protection  
✅ Delay, header/footer, word replace  
✅ Telegram Bot UI (inline buttons)

---

## 🧱 Tech Stack

- **Python 3.10+**
- `python-telegram-bot`
- `Telethon`
- `SQLite`
- `FFmpeg`
- `Pillow`

---

## ⚙️ Environment Variables (Railway)

Set these in **Railway → Variables**:

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_BOT_TOKEN=1234567890:ABCDEF...
ADMIN_USER_ID=123456789   # optional
SESSION_DIR=user_sessions
DATABASE_FILE=autoforward.db
