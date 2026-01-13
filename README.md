# Telegram Auto-Forward Bot

A powerful multi-user Telegram auto-forward bot with:

- Multiple source → destination rules
- Telethon + Bot API
- Media filters & caption cleaning
- Watermark support (image/video)
- SQLite database
- Railway-ready Docker deployment

---

## 🚀 Deploy on Railway

### 1. Fork or Upload to GitHub
Push this project to a GitHub repository.

### 2. Create Railway Project
- Go to https://railway.app
- New Project → Deploy from GitHub
- Select your repository

### 3. Set Environment Variables

In **Railway → Variables**:

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_BOT_TOKEN=your_bot_token
ADMIN_USER_ID=123456789
