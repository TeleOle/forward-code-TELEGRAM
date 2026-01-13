
import asyncio
import logging
import os
import sqlite3
import json
import re
import time
import signal
import tempfile
from datetime import datetime
from typing import Dict, Set, Optional, List
from asyncio import Lock
from contextlib import contextmanager, asynccontextmanager
from pathlib import Path
from collections import OrderedDict
from queue import Queue
from threading import Lock as ThreadLock

def escape_markdown(text: str) -> str:
    """Escape special Markdown characters to prevent parsing errors."""
    if not text:
        return ""
    # Escape special characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

def safe_text(text: str) -> str:
    """Make text safe for Markdown by escaping special characters."""
    if not text:
        return ""
    # Only escape the most problematic characters for Telegram Markdown
    return text.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')

def parse_markdown_to_entities(text: str):
    """
    Parse markdown-formatted text into Telegram entities.

    Supports:
    - **bold**
    - __underline__
    - *italic* or _italic_
    - ~~strikethrough~~
    - ||spoiler||
    - `code`
    - ```code block```
    - [text](url) for links
    - {newline} for line breaks

    Returns tuple: (plain_text, entities_list)
    """
    if not text:
        return "", []

    # Replace {newline} placeholder with actual newlines
    text = text.replace('{newline}', '\n')

    # Check if telethon is available
    if not TELETHON_AVAILABLE:
        return text, []

    entities = []
    plain_text = []
    offset = 0
    i = 0

    while i < len(text):
        # Bold: **text**
        if text[i:i+2] == '**':
            end = text.find('**', i+2)
            if end != -1:
                content = text[i+2:end]
                entities.append(MessageEntityBold(offset, len(content)))
                plain_text.append(content)
                offset += len(content)
                i = end + 2
                continue

        # Underline: __text__
        if text[i:i+2] == '__':
            end = text.find('__', i+2)
            if end != -1:
                content = text[i+2:end]
                entities.append(MessageEntityUnderline(offset, len(content)))
                plain_text.append(content)
                offset += len(content)
                i = end + 2
                continue

        # Strikethrough: ~~text~~
        if text[i:i+2] == '~~':
            end = text.find('~~', i+2)
            if end != -1:
                content = text[i+2:end]
                entities.append(MessageEntityStrike(offset, len(content)))
                plain_text.append(content)
                offset += len(content)
                i = end + 2
                continue

        # Spoiler: ||text||
        if text[i:i+2] == '||':
            end = text.find('||', i+2)
            if end != -1:
                content = text[i+2:end]
                entities.append(MessageEntitySpoiler(offset, len(content)))
                plain_text.append(content)
                offset += len(content)
                i = end + 2
                continue

        # Italic: *text* or _text_ (single char)
        if text[i] in ['*', '_'] and text[i:i+2] not in ['**', '__', '~~', '||']:
            marker = text[i]
            end = text.find(marker, i+1)
            if end != -1 and end > i+1:
                content = text[i+1:end]
                entities.append(MessageEntityItalic(offset, len(content)))
                plain_text.append(content)
                offset += len(content)
                i = end + 1
                continue

        # Code block: ```text```
        if text[i:i+3] == '```':
            end = text.find('```', i+3)
            if end != -1:
                content = text[i+3:end]
                entities.append(MessageEntityPre(offset, len(content), ''))
                plain_text.append(content)
                offset += len(content)
                i = end + 3
                continue

        # Inline code: `text`
        if text[i] == '`':
            end = text.find('`', i+1)
            if end != -1:
                content = text[i+1:end]
                entities.append(MessageEntityCode(offset, len(content)))
                plain_text.append(content)
                offset += len(content)
                i = end + 1
                continue

        # Link: [text](url)
        if text[i] == '[':
            text_end = text.find(']', i+1)
            if text_end != -1 and text_end+1 < len(text) and text[text_end+1] == '(':
                url_end = text.find(')', text_end+2)
                if url_end != -1:
                    link_text = text[i+1:text_end]
                    url = text[text_end+2:url_end]
                    entities.append(MessageEntityTextUrl(offset, len(link_text), url))
                    plain_text.append(link_text)
                    offset += len(link_text)
                    i = url_end + 1
                    continue

        # Regular character
        plain_text.append(text[i])
        offset += 1
        i += 1

    return ''.join(plain_text), entities

def serialize_entities(entities):
    """Convert Telegram entities to JSON-serializable format."""
    if not entities:
        return []

    serialized = []
    for entity in entities:
        entity_dict = {
            '_': entity.__class__.__name__,
            'offset': entity.offset,
            'length': entity.length
        }
        # Add URL for TextUrl entities
        if hasattr(entity, 'url'):
            entity_dict['url'] = entity.url
        # Add language for Pre entities
        if hasattr(entity, 'language'):
            entity_dict['language'] = entity.language
        serialized.append(entity_dict)
    return serialized

def deserialize_entities(entities_data):
    """Convert serialized entities back to Telegram entity objects."""
    if not entities_data:
        return []

    # Check if telethon is available
    if not TELETHON_AVAILABLE:
        return []

    entities = []
    entity_map = {
        'MessageEntityBold': MessageEntityBold,
        'MessageEntityItalic': MessageEntityItalic,
        'MessageEntityCode': MessageEntityCode,
        'MessageEntityPre': MessageEntityPre,
        'MessageEntityTextUrl': MessageEntityTextUrl,
        'MessageEntityStrike': MessageEntityStrike,
        'MessageEntityUnderline': MessageEntityUnderline,
        'MessageEntitySpoiler': MessageEntitySpoiler
    }

    for data in entities_data:
        if not isinstance(data, dict):
            continue

        entity_type = data.get('_')
        offset = data.get('offset', 0)
        length = data.get('length', 0)

        if entity_type in entity_map:
            if entity_type == 'MessageEntityTextUrl':
                url = data.get('url', '')
                entities.append(entity_map[entity_type](offset, length, url))
            elif entity_type == 'MessageEntityPre':
                language = data.get('language', '')
                entities.append(entity_map[entity_type](offset, length, language))
            else:
                entities.append(entity_map[entity_type](offset, length))

    return entities

def extract_media_attributes(msg):
    """
    Extract all media attributes from original message to preserve format.

    Returns tuple: (media_type, attributes_list, mime_type, thumb)
    """
    if not TELETHON_AVAILABLE or not msg:
        return None, [], None, None

    attributes = []
    mime_type = None
    thumb = None
    media_type = None

    try:
        # PHOTO
        if msg.photo:
            media_type = 'photo'
            # Photos don't need attributes, Telethon handles them
            return media_type, attributes, mime_type, thumb

        # VIDEO NOTE
        if msg.video_note:
            media_type = 'video_note'
            if msg.document:
                for attr in getattr(msg.document, 'attributes', []):
                    if hasattr(attr, 'duration'):
                        attributes.append(types.DocumentAttributeVideo(
                            duration=getattr(attr, 'duration', 0),
                            w=getattr(attr, 'w', 384),
                            h=getattr(attr, 'h', 384),
                            round_message=True
                        ))
                mime_type = getattr(msg.document, 'mime_type', 'video/mp4')
            return media_type, attributes, mime_type, thumb

        # VOICE
        if msg.voice:
            media_type = 'voice'
            if msg.document:
                for attr in getattr(msg.document, 'attributes', []):
                    if hasattr(attr, 'duration'):
                        attributes.append(types.DocumentAttributeAudio(
                            duration=getattr(attr, 'duration', 0),
                            voice=True
                        ))
                        break
                mime_type = getattr(msg.document, 'mime_type', 'audio/ogg')
            return media_type, attributes, mime_type, thumb

        # VIDEO
        if msg.video:
            media_type = 'video'
            if msg.document:
                for attr in getattr(msg.document, 'attributes', []):
                    if hasattr(attr, 'duration') and hasattr(attr, 'w'):
                        attributes.append(types.DocumentAttributeVideo(
                            duration=getattr(attr, 'duration', 0),
                            w=getattr(attr, 'w', 1280),
                            h=getattr(attr, 'h', 720),
                            supports_streaming=True
                        ))
                        break
                    # Also check for filename
                    if hasattr(attr, 'file_name') and attr.file_name:
                        attributes.append(types.DocumentAttributeFilename(attr.file_name))
                mime_type = getattr(msg.document, 'mime_type', 'video/mp4')
                thumb = getattr(msg.document, 'thumbs', None)
                if thumb and len(thumb) > 0:
                    thumb = thumb[-1]  # Get largest thumbnail
            return media_type, attributes, mime_type, thumb

        # GIF/ANIMATION
        if msg.gif:
            media_type = 'gif'
            if msg.document:
                for attr in getattr(msg.document, 'attributes', []):
                    # Preserve animation attribute
                    if hasattr(attr, 'w') and hasattr(attr, 'h'):
                        attributes.append(types.DocumentAttributeVideo(
                            duration=0,
                            w=getattr(attr, 'w', 320),
                            h=getattr(attr, 'h', 320),
                            supports_streaming=False
                        ))
                    if hasattr(attr, 'file_name') and attr.file_name:
                        attributes.append(types.DocumentAttributeFilename(attr.file_name))
                mime_type = getattr(msg.document, 'mime_type', 'video/mp4')
            return media_type, attributes, mime_type, thumb

        # STICKER
        if msg.sticker:
            media_type = 'sticker'
            if msg.document:
                # Preserve ALL sticker attributes (important for animated stickers)
                for attr in getattr(msg.document, 'attributes', []):
                    attributes.append(attr)
                mime_type = getattr(msg.document, 'mime_type', 'image/webp')
            return media_type, attributes, mime_type, thumb

        # AUDIO
        if msg.audio:
            media_type = 'audio'
            if msg.document:
                duration = 0
                title = None
                performer = None
                filename = None

                for attr in getattr(msg.document, 'attributes', []):
                    if hasattr(attr, 'duration'):
                        duration = getattr(attr, 'duration', 0)
                    if hasattr(attr, 'title'):
                        title = getattr(attr, 'title', None)
                    if hasattr(attr, 'performer'):
                        performer = getattr(attr, 'performer', None)
                    if hasattr(attr, 'file_name'):
                        filename = getattr(attr, 'file_name', None)

                # Add audio attribute
                if duration or title or performer:
                    attributes.append(types.DocumentAttributeAudio(
                        duration=duration,
                        title=title,
                        performer=performer,
                        voice=False
                    ))
                # Add filename
                if filename:
                    attributes.append(types.DocumentAttributeFilename(filename))

                mime_type = getattr(msg.document, 'mime_type', 'audio/mpeg')
            return media_type, attributes, mime_type, thumb

        # DOCUMENT
        if msg.document:
            media_type = 'document'
            # Extract ALL document attributes to preserve format
            for attr in getattr(msg.document, 'attributes', []):
                attributes.append(attr)
            mime_type = getattr(msg.document, 'mime_type', None)
            thumb = getattr(msg.document, 'thumbs', None)
            if thumb and len(thumb) > 0:
                thumb = thumb[-1]
            return media_type, attributes, mime_type, thumb

    except Exception as e:
        import traceback
        log.error(f"Error extracting media attributes: {e}")
        log.error(traceback.format_exc())

    return media_type, attributes, mime_type, thumb

# Default filters configuration (all OFF = keep everything)
DEFAULT_FILTERS = {
    # Media type filters (ignore/skip these)
    'document': False,      # Ignore document messages (PDF, DOCX, etc.)
    'video': False,         # Ignore video messages
    'audio': False,         # Ignore audio messages
    'sticker': False,       # Ignore sticker messages
    'text': False,          # Ignore text-only messages
    'photo': False,         # Ignore photo messages
    'photo_only': False,    # Ignore photos WITHOUT caption
    'photo_with_text': False,  # Ignore photos WITH caption
    'album': False,         # Ignore album/grouped media
    'poll': False,          # Ignore poll messages
    'voice': False,         # Ignore voice messages
    'video_note': False,    # Ignore round video notes
    'gif': False,           # Ignore animated GIFs
    'emoji': False,         # Ignore animated emoji
    'forward': False,       # Ignore forwarded messages
    'reply': False,         # Ignore reply messages
    'button': False,        # Ignore messages with buttons
    
    # Cleaner options (remove from caption)
    'clean_caption': False,   # Remove all captions - forward media without any caption text
    'clean_hashtag': False,   # Remove #hashtags from caption
    'clean_mention': False,   # Remove @mentions from caption
    'clean_link': False,      # Remove links from caption
    'clean_emoji': False,     # Remove emojis from caption
    'clean_phone': False,     # Remove phone numbers from caption
    'clean_email': False,     # Remove email addresses from caption
}

# Default modify content configuration
DEFAULT_MODIFY = {
    # Filename rename
    'rename_enabled': False,
    'rename_pattern': '{original}',  # Patterns: {original}, {date}, {time}, {random}, {counter}

    # Block/Whitelist words
    'block_words_enabled': False,
    'block_words': [],  # List of words to block (message skipped if contains)
    'whitelist_enabled': False,
    'whitelist_words': [],  # List of words required (message skipped if NOT contains)

    # Word replacement
    'replace_enabled': False,
    'replace_pairs': [],  # List of {'from': 'old', 'to': 'new', 'regex': False}

    # Caption editing
    'header_enabled': False,
    'header_text': '',  # Text to add at the beginning
    'footer_enabled': False,
    'footer_text': '',  # Text to add at the end

    # Link buttons
    'buttons_enabled': False,
    'buttons': [],  # List of [{'text': 'Button', 'url': 'https://...'}] per row

    # Delay
    'delay_enabled': False,
    'delay_seconds': 0,  # Delay in seconds before forwarding

    # History
    'history_enabled': False,
    'history_count': 0,  # Number of past messages to forward when rule created

    # Watermark
    'watermark_enabled': False,
    'watermark_type': 'text',  # 'text' or 'logo'

    # Text watermark settings
    'watermark_text': '',
    'watermark_text_color': 'white',  # white, black, blue, red, or custom hex
    'watermark_text_custom_color': '#FFFFFF',

    # Logo watermark settings
    'watermark_logo_file_id': None,  # Telegram file_id of the logo image
    'watermark_logo_path': None,  # Local path to logo (temporary storage)

    # Common watermark settings (both text and logo)
    'watermark_position': 'bottom-right',  # top-left, top, top-right, left, center, right, bottom-left, bottom, bottom-right
    'watermark_opacity': 50,  # 10-100%
    'watermark_rotation': 0,  # 0-359 degrees
    'watermark_size': 10,  # 10-100% (for text: font size, for logo: scale)

    # Caption replacement
    'caption_enabled': False,
    'caption_text': '',  # Replace caption with custom text
    'caption_entities': [],  # Message entities for formatting

    # Spoiler effect
    'apply_spoiler': False,  # Apply spoiler effect to photos and videos (blur/shimmer effect)
}

# Optional dependency handling
TELETHON_AVAILABLE = True
try:
    from telethon import TelegramClient, events, errors
    from telethon.tl.types import (
        User, Channel, Chat, PeerChannel,
        MessageEntityBold, MessageEntityItalic, MessageEntityCode,
        MessageEntityPre, MessageEntityTextUrl, MessageEntityStrike,
        MessageEntityUnderline, MessageEntitySpoiler
    )
    from telethon.tl import types
except ImportError:
    TELETHON_AVAILABLE = False
    class _PlaceholderErrors:
        class SessionPasswordNeededError(Exception): pass
        class PhoneCodeInvalidError(Exception): pass
        class PhoneCodeExpiredError(Exception): pass
        class FloodWaitError(Exception):
            def __init__(self, seconds=0): self.seconds = seconds
        class PhoneNumberBannedError(Exception): pass
        class ChannelPrivateError(Exception): pass
        class ChatWriteForbiddenError(Exception): pass
    errors = _PlaceholderErrors
    TelegramClient = None
    events = None
    PeerChannel = None
    types = None

TELEGRAM_AVAILABLE = True
try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
    from telegram.ext import (
        Application, ApplicationBuilder, ContextTypes, CommandHandler,
        CallbackQueryHandler, MessageHandler, filters
    )
    from telegram.error import BadRequest
except ImportError:
    TELEGRAM_AVAILABLE = False
    Update = None
    ContextTypes = None
    BadRequest = Exception

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# ==================== COMPILED REGEX PATTERNS ====================
# Pre-compile all regex patterns for performance (avoid re-compilation on every message)
# This improves CPU usage by ~20% for high-volume message processing

# Caption cleaning patterns
REGEX_HASHTAG = re.compile(r'#\w+')
REGEX_MENTION = re.compile(r'@\w+')
REGEX_URL_HTTPS = re.compile(r'https?://\S+')
REGEX_URL_HTTP = re.compile(r'http?://\S+')
REGEX_URL_WWW = re.compile(r'www\.\S+')
REGEX_URL_TG_ME = re.compile(r't\.me/\S+')
REGEX_URL_TG_SCHEME = re.compile(r'tg://\S+')
REGEX_URL_LEGACY = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

# Phone number patterns (various formats)
REGEX_PHONE_FORMATTED = re.compile(r'\+?\d{1,4}[-.\s]?\(?\d{1,3}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}')
REGEX_PHONE_SIMPLE = re.compile(r'\+?\d{10,15}')
REGEX_PHONE_BASIC = re.compile(r'\+?\d[\d\s\-\(\)]{7,}\d')

# Email pattern
REGEX_EMAIL = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
REGEX_EMAIL_STRICT = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

# Whitespace cleanup patterns
REGEX_WHITESPACE_MULTI = re.compile(r'[ \t]+')
REGEX_LINE_TRIM = re.compile(r'^ +| +$', flags=re.MULTILINE)
REGEX_NEWLINE_MULTI = re.compile(r'\n{3,}')

# Filename cleanup
REGEX_FILENAME_SPACES = re.compile(r'[_\s]+')

# Comprehensive emoji pattern (all Unicode emoji ranges)
REGEX_EMOJI = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons 😀-🙏
    "\U0001F300-\U0001F5FF"  # symbols & pictographs 🌀-🗿
    "\U0001F680-\U0001F6FF"  # transport 🚀-🛿
    "\U0001F700-\U0001F77F"  # alchemical
    "\U0001F780-\U0001F7FF"  # geometric
    "\U0001F800-\U0001F8FF"  # arrows
    "\U0001F900-\U0001F9FF"  # supplemental 🤀-🧿
    "\U0001FA00-\U0001FA6F"  # chess
    "\U0001FA70-\U0001FAFF"  # extended-a 🩰-🫿
    "\U00002702-\U000027B0"  # dingbats ✂-➰
    "\U0001F1E0-\U0001F1FF"  # flags 🇦-🇿
    "\U00002600-\U000026FF"  # misc ☀-⛿
    "\U00002700-\U000027BF"  # dingbats ✀-➿
    "\U0001F000-\U0001F02F"  # mahjong
    "\U0001F0A0-\U0001F0FF"  # cards
    "\U0000FE00-\U0000FE0F"  # variation selectors
    "\U0000FE0E-\U0000FE0F"  # text/emoji variation
    "\U0000200D"             # zero width joiner
    "\U00002640-\U00002642"  # gender ♀♂
    "\U000023E9-\U000023F3"  # media ⏩-⏳
    "\U000023F8-\U000023FA"  # media ⏸-⏺
    "\U00002B50"             # star ⭐
    "\U00002B55"             # circle ⭕
    "\U00002934-\U00002935"  # arrows
    "\U00002B05-\U00002B07"  # arrows
    "\U00002B1B-\U00002B1C"  # squares
    "\U00003030"             # wavy dash
    "\U0000303D"             # part mark
    "\U00003297"             # circled
    "\U00003299"             # circled
    "\U000024C2-\U0001F251"  # enclosed
    "\U00002500-\U00002BEF"  # various
    "\U0000231A-\U0000231B"  # watch ⌚⌛
    "\U000025AA-\U000025AB"  # squares
    "\U000025B6"             # play ▶
    "\U000025C0"             # reverse ◀
    "\U000025FB-\U000025FE"  # squares
    "\U00002764"             # heart ❤
    "\U00002763"             # heart exclamation ❣
    "\U00002665"             # heart suit ♥
    "\U0001F493-\U0001F49F"  # hearts 💓-💟
    "\U00002714"             # check ✔
    "\U00002716"             # x ✖
    "\U0000270A-\U0000270D"  # hands ✊-✍
    "\U000023CF"             # eject ⏏
    "\U000023ED-\U000023EF"  # media ⏭-⏯
    "\U000023F1-\U000023F2"  # timer ⏱⏲
    "\U0000200B-\U0000200F"  # zero width chars
    "\U00002028-\U0000202F"  # separators
    "\U0000205F-\U0000206F"  # format chars
    "]+",
    flags=re.UNICODE
)

log.info("✅ Compiled regex patterns loaded for optimized performance")

# ==================== CONFIGURATION ====================
class Config:
    """
    Configuration loaded from environment variables.
    Set these environment variables before running:
    - TELEGRAM_API_ID      : Your Telegram API ID from my.telegram.org
    - TELEGRAM_API_HASH    : Your Telegram API Hash from my.telegram.org
    - TELEGRAM_BOT_TOKEN   : Bot token from @BotFather
    - ADMIN_USER_ID        : (Optional) Admin user ID for special permissions
    - SESSION_DIR          : (Optional) Directory for session files
    - DATABASE_FILE        : (Optional) SQLite database file path
    """
    # Required settings
    API_ID: int = int(os.getenv('TELEGRAM_API_ID', '0'))
    API_HASH: str = os.getenv('TELEGRAM_API_HASH', '')
    BOT_TOKEN: str = os.getenv('TELEGRAM_BOT_TOKEN', os.getenv('MANAGER_BOT_TOKEN', ''))
    
    # Optional settings
    ADMIN_USER_ID: int = int(os.getenv('ADMIN_USER_ID', '0'))
    SESSION_DIR: str = os.getenv('SESSION_DIR', 'user_sessions')
    DATABASE_FILE: str = os.getenv('DATABASE_FILE', 'autoforward.db')
    
    # Bot settings
    MAX_RULES_PER_USER: int = int(os.getenv('MAX_RULES_PER_USER', '50'))
    MAX_ACCOUNTS_PER_USER: int = int(os.getenv('MAX_ACCOUNTS_PER_USER', '10'))
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that all required configuration is present."""
        errors = []
        
        if not cls.API_ID or cls.API_ID == 0:
            errors.append("TELEGRAM_API_ID is required")
        
        if not cls.API_HASH:
            errors.append("TELEGRAM_API_HASH is required")
        
        if not cls.BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN is required")
        
        if errors:
            log.error("❌ Configuration errors:")
            for err in errors:
                log.error(f"   - {err}")
            log.error("")
            log.error("Please set environment variables or create a .env file:")
            log.error("   export TELEGRAM_API_ID=your_api_id")
            log.error("   export TELEGRAM_API_HASH=your_api_hash")
            log.error("   export TELEGRAM_BOT_TOKEN=your_bot_token")
            return False
        
        log.info("✅ Configuration loaded successfully")
        log.info(f"   - API_ID: {cls.API_ID}")
        log.info(f"   - API_HASH: {cls.API_HASH[:8]}...")
        log.info(f"   - BOT_TOKEN: {cls.BOT_TOKEN[:20]}...")
        log.info(f"   - SESSION_DIR: {cls.SESSION_DIR}")
        log.info(f"   - DATABASE_FILE: {cls.DATABASE_FILE}")
        
        if cls.ADMIN_USER_ID:
            log.info(f"   - ADMIN_USER_ID: {cls.ADMIN_USER_ID}")
        
        return True
    
    @classmethod
    def load_dotenv(cls):
        """Load configuration from .env file if python-dotenv is available."""
        try:
            from dotenv import load_dotenv
            load_dotenv()
            # Reload values after loading .env
            cls.API_ID = int(os.getenv('TELEGRAM_API_ID', '0'))
            cls.API_HASH = os.getenv('TELEGRAM_API_HASH', '')
            cls.BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', os.getenv('MANAGER_BOT_TOKEN', ''))
            cls.ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '0'))
            cls.SESSION_DIR = os.getenv('SESSION_DIR', 'user_sessions')
            cls.DATABASE_FILE = os.getenv('DATABASE_FILE', 'autoforward.db')
            cls.MAX_RULES_PER_USER = int(os.getenv('MAX_RULES_PER_USER', '50'))
            cls.MAX_ACCOUNTS_PER_USER = int(os.getenv('MAX_ACCOUNTS_PER_USER', '10'))
            log.info("✅ Loaded .env file")
        except ImportError:
            log.debug("python-dotenv not installed, skipping .env file")
        except Exception as e:
            log.warning(f"Failed to load .env file: {e}")

# Load .env file if available
Config.load_dotenv()

# Create session directory
os.makedirs(Config.SESSION_DIR, exist_ok=True)

# Backward compatibility aliases
API_ID = Config.API_ID
API_HASH = Config.API_HASH
BOT_TOKEN = Config.BOT_TOKEN
SESSION_DIR = Config.SESSION_DIR
DATABASE_FILE = Config.DATABASE_FILE

# ==================== CRITICAL FIX CLASSES ====================

class TimeoutError(Exception):
    """Raised when an operation times out."""
    pass


def safe_path_join(base_dir: str, *paths) -> str:
    """
    Safely join paths preventing directory traversal.

    Args:
        base_dir: Base directory (must be absolute)
        *paths: Path components to join

    Returns:
        Safe absolute path within base_dir

    Raises:
        ValueError: If resulting path is outside base_dir
    """
    base_dir = os.path.realpath(base_dir)
    target_path = os.path.realpath(os.path.join(base_dir, *paths))

    if not target_path.startswith(base_dir):
        raise ValueError(f"Path traversal detected: {target_path} is outside {base_dir}")

    return target_path


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename removing dangerous characters.

    Args:
        filename: Original filename

    Returns:
        Safe filename
    """
    filename = os.path.basename(filename)
    invalid_chars = '<>:"/\\|?*\n\r\t\x00'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    filename = filename.strip('. ')
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255-len(ext)] + ext
    if not filename:
        filename = 'unnamed_file'
    return filename


def safe_regex_replace(pattern: str, replacement: str, text: str, timeout_seconds: int = 1) -> str:
    """
    Safely execute regex replacement with timeout protection.

    Args:
        pattern: Regex pattern
        replacement: Replacement string
        text: Text to process
        timeout_seconds: Max execution time

    Returns:
        Processed text

    Raises:
        ValueError: If pattern is invalid or too complex
    """
    if len(pattern) > 1000:
        raise ValueError("Regex pattern too long (max 1000 chars)")

    dangerous_patterns = [r'(\w+)*', r'(a+)+', r'(a*)*', r'(a|a)*', r'(a|ab)*']
    for dangerous in dangerous_patterns:
        if dangerous in pattern:
            raise ValueError(f"Potentially dangerous regex pattern detected")

    try:
        compiled = re.compile(pattern)
        result = compiled.sub(replacement, text)
        return result
    except re.error as e:
        raise ValueError(f"Invalid regex pattern: {e}")


@asynccontextmanager
async def temporary_file(suffix='', prefix='tmp', dir=None):
    """
    Async context manager for temporary files with guaranteed cleanup.
    """
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        os.close(fd)
        yield path
    finally:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            log.warning(f"Failed to cleanup temp file {path}: {e}")


class AlbumCacheManager:
    """Thread-safe album cache with automatic TTL cleanup."""

    def __init__(self, ttl_seconds=5, cleanup_interval=10):
        self.cache = {}
        self.lock = asyncio.Lock()
        self.ttl_seconds = ttl_seconds
        self.cleanup_interval = cleanup_interval
        self._cleanup_task = None

    async def start(self):
        """Start background cleanup task."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self):
        """Stop background cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self):
        """Background task to clean expired entries."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Album cache cleanup error: {e}")

    async def cleanup_expired(self):
        """Remove expired entries."""
        async with self.lock:
            now = time.time()
            expired = [
                gid for gid, entry in self.cache.items()
                if now - entry['timestamp'] > self.ttl_seconds
            ]
            for gid in expired:
                del self.cache[gid]
            if expired:
                log.debug(f"Cleaned {len(expired)} expired album cache entries")

    async def get(self, grouped_id):
        """Get cache entry."""
        async with self.lock:
            entry = self.cache.get(grouped_id)
            if entry:
                return entry.get('data')
            return None

    async def set(self, grouped_id, data):
        """Set cache entry with timestamp."""
        async with self.lock:
            self.cache[grouped_id] = {
                'data': data,
                'timestamp': time.time()
            }

    async def pop(self, grouped_id):
        """Remove and return cache entry."""
        async with self.lock:
            entry = self.cache.pop(grouped_id, None)
            if entry:
                return entry.get('data')
            return None


class LRUCache:
    """Thread-safe LRU cache with TTL."""

    def __init__(self, max_size=1000, ttl_seconds=3600):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.lock = asyncio.Lock()

    async def get(self, key):
        """Get value from cache."""
        async with self.lock:
            if key not in self.cache:
                return None

            value, timestamp = self.cache[key]

            if time.time() - timestamp > self.ttl_seconds:
                del self.cache[key]
                return None

            self.cache.move_to_end(key)
            return value

    async def set(self, key, value):
        """Set value in cache."""
        async with self.lock:
            if len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)

            self.cache[key] = (value, time.time())

    async def clear(self):
        """Clear all cache entries."""
        async with self.lock:
            self.cache.clear()


class AsyncLockWithTimeout:
    """Async lock wrapper with timeout support."""

    def __init__(self, lock: asyncio.Lock, timeout: float = 30.0):
        self._lock = lock
        self._timeout = timeout

    async def __aenter__(self):
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=self._timeout)
            return self
        except asyncio.TimeoutError:
            raise TimeoutError(f"Failed to acquire lock within {self._timeout}s")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()
        return False


class TokenBucketRateLimiter:
    """Token bucket rate limiter for API protection."""

    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens."""
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update

            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate
            )
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


class UserRateLimiter:
    """Per-user rate limiting."""

    def __init__(self, requests_per_minute: int = 60):
        self.limiters = {}
        self.rpm = requests_per_minute
        self.lock = asyncio.Lock()

    async def check_rate_limit(self, user_id: int) -> bool:
        """Check if user is within rate limit."""
        async with self.lock:
            if user_id not in self.limiters:
                self.limiters[user_id] = TokenBucketRateLimiter(
                    rate=self.rpm / 60.0,
                    capacity=self.rpm
                )

        return await self.limiters[user_id].acquire()


class DatabaseConnectionPool:
    """Connection pool for SQLite to prevent connection overhead."""

    def __init__(self, db_path: str, pool_size: int = 20):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool = Queue(maxsize=pool_size)
        self._lock = ThreadLock()
        self._init_pool()

    def _init_pool(self):
        """Initialize connection pool."""
        for _ in range(self.pool_size):
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            self._pool.put(conn)

    @asynccontextmanager
    async def get_connection(self):
        """Get connection from pool."""
        loop = asyncio.get_running_loop()
        conn = await loop.run_in_executor(
            None, self._pool.get, True, 5.0
        )
        try:
            yield conn
        finally:
            await loop.run_in_executor(
                None, self._pool.put, conn
            )

    def close_all(self):
        """Close all connections."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except:
                pass


# Global rate limiter for user requests
user_rate_limiter = UserRateLimiter(requests_per_minute=60)

# ==================== HELPER FUNCTIONS ====================
def parse_multi_ids(text: str) -> List[str]:
    """Parse comma/space separated IDs into a list."""
    # Replace common separators with comma
    text = text.replace('\n', ',').replace(';', ',').replace(' ', ',')
    # Split and clean
    parts = [p.strip() for p in text.split(',')]
    # Filter empty and duplicates while preserving order
    seen = set()
    result = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            result.append(p)
    return result

def format_id_list(ids: List[str], max_show: int = 3) -> str:
    """Format ID list for display."""
    if len(ids) <= max_show:
        return ', '.join(f'`{i}`' for i in ids)
    return ', '.join(f'`{i}`' for i in ids[:max_show]) + f' +{len(ids)-max_show} more'

# ==================== WATERMARK PROCESSING ====================
def apply_watermark_with_ffmpeg(input_path: str, output_path: str, watermark_config: dict, is_video: bool = False) -> bool:
    """
    Apply watermark to image or video using FFmpeg (preferred method).

    Args:
        input_path: Path to input file (image or video)
        output_path: Path to save watermarked file
        watermark_config: Dictionary with watermark settings
        is_video: True if input is video, False if image

    Returns:
        True if successful, False otherwise
    """
    try:
        import subprocess
        import os

        media_type = "video" if is_video else "image"
        log.info(f"🎬 FFmpeg Watermark: Processing {media_type}: {input_path}")

        watermark_type = watermark_config.get('watermark_type', 'text')
        position = watermark_config.get('watermark_position', 'bottom-right')
        opacity = watermark_config.get('watermark_opacity', 50) / 100.0
        size_percent = watermark_config.get('watermark_size', 10)

        log.info(f"🎬 FFmpeg Watermark: type={watermark_type}, position={position}, opacity={opacity}, size={size_percent}%")

        # Position mapping for FFmpeg
        position_map = {
            'top-left': 'x=10:y=10',
            'top': 'x=(W-w)/2:y=10',
            'top-right': 'x=W-w-10:y=10',
            'left': 'x=10:y=(H-h)/2',
            'center': 'x=(W-w)/2:y=(H-h)/2',
            'right': 'x=W-w-10:y=(H-h)/2',
            'bottom-left': 'x=10:y=H-h-10',
            'bottom': 'x=(W-w)/2:y=H-h-10',
            'bottom-right': 'x=W-w-10:y=H-h-10',
        }
        pos_string = position_map.get(position, 'x=W-w-10:y=H-h-10')

        if watermark_type == 'text':
            text = watermark_config.get('watermark_text', '')
            if not text:
                log.error("🎬 FFmpeg Watermark FAILED: No watermark text provided")
                return False

            log.info(f"🎬 FFmpeg Watermark: Adding text '{text[:30]}...'")

            color_name = watermark_config.get('watermark_text_color', 'white')
            color_map = {
                'white': 'white',
                'black': 'black',
                'blue': 'blue',
                'red': 'red'
            }
            color = color_map.get(color_name, 'white')

            # Escape special characters for FFmpeg
            text = text.replace("'", "\\'").replace(":", "\\:")

            # Build FFmpeg filter for text watermark
            font_size = f"fontsize=h*{size_percent}/100"
            filter_str = f"drawtext=text='{text}':{pos_string}:fontcolor={color}@{opacity}:{font_size}"

            # For images, we need to specify output format
            if is_video:
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-vf', filter_str,
                    '-codec:a', 'copy',
                    '-y',  # Overwrite output file
                    output_path
                ]
            else:
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-vf', filter_str,
                    '-frames:v', '1',  # Write only one frame
                    '-update', '1',    # Allow overwriting single image
                    '-y',  # Overwrite output file
                    output_path
                ]

        else:  # logo watermark
            logo_path = watermark_config.get('watermark_logo_path')
            if not logo_path or not os.path.exists(logo_path):
                log.error(f"🎬 FFmpeg Watermark FAILED: Logo not found at {logo_path}")
                return False

            log.info(f"🎬 FFmpeg Watermark: Adding logo from {logo_path}")

            # Build FFmpeg filter for logo overlay - extract first frame from logo if it's a video
            scale = f"scale=iw*{size_percent}/100:-1"
            filter_str = f"[1:v]{scale},format=rgba,colorchannelmixer=aa={opacity}[wm];[0:v][wm]overlay={pos_string}"

            # For images, we need to specify output format
            if is_video:
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-i', logo_path,
                    '-filter_complex', filter_str,
                    '-codec:a', 'copy',
                    '-y',  # Overwrite output file
                    output_path
                ]
            else:
                cmd = [
                    'ffmpeg', '-i', input_path,
                    '-i', logo_path,
                    '-filter_complex', filter_str,
                    '-frames:v', '1',  # Write only one frame
                    '-update', '1',    # Allow overwriting single image
                    '-y',  # Overwrite output file
                    output_path
                ]

        # Run FFmpeg
        log.info(f"🎬 FFmpeg Watermark: Running command")
        log.info(f"🎬 FFmpeg Watermark: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            log.info(f"🎬 FFmpeg Watermark: ✅ {media_type.capitalize()} watermark completed successfully")
            return True
        else:
            log.error(f"❌ FFmpeg error (returncode={result.returncode}): {result.stderr}")
            return False

    except FileNotFoundError:
        log.warning("⚠️ FFmpeg not installed. Will try Pillow for images.")
        return False
    except subprocess.TimeoutExpired:
        log.error("❌ FFmpeg timeout (60s exceeded)")
        return False
    except Exception as e:
        log.error(f"❌ Failed to apply FFmpeg watermark: {e}")
        import traceback
        log.error(f"❌ FFmpeg watermark traceback: {traceback.format_exc()}")
        return False


def apply_watermark_to_image(input_path: str, output_path: str, watermark_config: dict) -> bool:
    """
    Apply watermark to an image using Pillow (PIL).

    Args:
        input_path: Path to input image
        output_path: Path to save watermarked image
        watermark_config: Dictionary with watermark settings

    Returns:
        True if successful, False otherwise
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import os

        log.info(f"📸 Watermark: Loading image from {input_path}")

        # Load the main image
        img = Image.open(input_path).convert('RGBA')
        width, height = img.size
        log.info(f"📸 Watermark: Image size {width}x{height}")

        # Create watermark layer
        watermark_layer = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(watermark_layer)

        watermark_type = watermark_config.get('watermark_type', 'text')
        position = watermark_config.get('watermark_position', 'bottom-right')
        opacity = int(watermark_config.get('watermark_opacity', 50) * 2.55)  # Convert 0-100 to 0-255
        rotation = watermark_config.get('watermark_rotation', 0)
        size_percent = watermark_config.get('watermark_size', 10)

        log.info(f"📸 Watermark: type={watermark_type}, position={position}, opacity={opacity}/255, rotation={rotation}°, size={size_percent}%")

        if watermark_type == 'text':
            # Text watermark
            text = watermark_config.get('watermark_text', '')
            if not text:
                log.error("📸 Watermark FAILED: No watermark text provided")
                return False

            log.info(f"📸 Watermark: Adding text '{text[:30]}...'")


            color_name = watermark_config.get('watermark_text_color', 'white')
            color_map = {
                'white': (255, 255, 255, opacity),
                'black': (0, 0, 0, opacity),
                'blue': (0, 0, 255, opacity),
                'red': (255, 0, 0, opacity)
            }
            color = color_map.get(color_name, (255, 255, 255, opacity))

            # Calculate font size based on image size and size_percent
            font_size = int(min(width, height) * size_percent / 100)
            if font_size < 10:  # Minimum font size
                font_size = 10

            try:
                # Try common font paths
                font = None
                font_paths = [
                    "arial.ttf",
                    "C:\\Windows\\Fonts\\arial.ttf",
                    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                    "/System/Library/Fonts/Helvetica.ttc",  # macOS
                    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
                ]

                for font_path in font_paths:
                    try:
                        font = ImageFont.truetype(font_path, font_size)
                        break
                    except (OSError, IOError) as e:
                        # Font file not found or cannot be read
                        continue

                # If no TrueType font found, use default (but it won't resize well)
                if font is None:
                    log.warning("No TrueType font found, using default font (watermark may be small)")
                    font = ImageFont.load_default()
            except Exception as e:
                log.error(f"Font loading error: {e}")
                font = ImageFont.load_default()

            # Get text bounding box
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            # Calculate position
            if position == 'top-left':
                x, y = 10, 10
            elif position == 'top':
                x, y = (width - text_width) // 2, 10
            elif position == 'top-right':
                x, y = width - text_width - 10, 10
            elif position == 'left':
                x, y = 10, (height - text_height) // 2
            elif position == 'center':
                x, y = (width - text_width) // 2, (height - text_height) // 2
            elif position == 'right':
                x, y = width - text_width - 10, (height - text_height) // 2
            elif position == 'bottom-left':
                x, y = 10, height - text_height - 10
            elif position == 'bottom':
                x, y = (width - text_width) // 2, height - text_height - 10
            else:  # bottom-right
                x, y = width - text_width - 10, height - text_height - 10

            # Draw text
            draw.text((x, y), text, font=font, fill=color)

        else:  # logo watermark
            logo_path = watermark_config.get('watermark_logo_path')
            if not logo_path:
                log.error("📸 Watermark FAILED: No logo path provided")
                return False
            if not os.path.exists(logo_path):
                log.error(f"📸 Watermark FAILED: Logo file not found at {logo_path}")
                return False

            log.info(f"📸 Watermark: Adding logo from {logo_path}")

            # Load logo
            logo = Image.open(logo_path).convert('RGBA')
            log.info(f"📸 Watermark: Logo loaded, size: {logo.size[0]}x{logo.size[1]}")

            # Resize logo based on size_percent
            logo_width = int(width * size_percent / 100)
            logo_height = int(logo.size[1] * logo_width / logo.size[0])
            logo = logo.resize((logo_width, logo_height), Image.Resampling.LANCZOS)

            # Apply opacity
            if opacity < 255:
                alpha = logo.split()[3]
                alpha = alpha.point(lambda p: int(p * opacity / 255))
                logo.putalpha(alpha)

            # Calculate position
            if position == 'top-left':
                x, y = 10, 10
            elif position == 'top':
                x, y = (width - logo_width) // 2, 10
            elif position == 'top-right':
                x, y = width - logo_width - 10, 10
            elif position == 'left':
                x, y = 10, (height - logo_height) // 2
            elif position == 'center':
                x, y = (width - logo_width) // 2, (height - logo_height) // 2
            elif position == 'right':
                x, y = width - logo_width - 10, (height - logo_height) // 2
            elif position == 'bottom-left':
                x, y = 10, height - logo_height - 10
            elif position == 'bottom':
                x, y = (width - logo_width) // 2, height - logo_height - 10
            else:  # bottom-right
                x, y = width - logo_width - 10, height - logo_height - 10

            # Paste logo onto watermark layer
            watermark_layer.paste(logo, (x, y), logo)

        # Rotate if needed (note: rotation is applied to the entire watermark layer)
        # Since we need same size for alpha_composite, we don't use expand=True
        if rotation != 0:
            watermark_layer = watermark_layer.rotate(-rotation, fillcolor=(0, 0, 0, 0))

        # Composite the watermark onto the original image
        log.info(f"📸 Watermark: Compositing watermark onto image")
        img = Image.alpha_composite(img, watermark_layer)

        # Convert back to RGB if saving as JPEG
        if output_path.lower().endswith('.jpg') or output_path.lower().endswith('.jpeg'):
            log.info(f"📸 Watermark: Converting to RGB for JPEG")
            img = img.convert('RGB')

        # Save the result
        log.info(f"📸 Watermark: Saving watermarked image to {output_path}")
        img.save(output_path, quality=95)
        log.info(f"📸 Watermark: ✅ Image watermark completed successfully")
        return True

    except ImportError as e:
        log.error(f"❌ Pillow (PIL) not installed. Install with: pip install Pillow")
        log.error(f"❌ Import error details: {e}")
        return False
    except Exception as e:
        log.error(f"❌ Failed to apply image watermark: {e}")
        import traceback
        log.error(f"❌ Watermark traceback: {traceback.format_exc()}")
        return False


# ==================== DATABASE MANAGER ====================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = Lock()
        self._pool = DatabaseConnectionPool(db_path, pool_size=20)
        self._initialized = False

    async def ensure_initialized(self):
        """Ensure database is initialized (lazy init)."""
        if not self._initialized:
            await self._init_db()
            self._initialized = True

    async def _init_db(self):
        async with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS connected_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    phone TEXT NOT NULL,
                    display_name TEXT,
                    connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    UNIQUE(user_id, phone)
                )
            ''')
            
            # Updated schema: sources and destinations are comma-separated lists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forward_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    phone TEXT NOT NULL,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    is_enabled INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    forward_count INTEGER DEFAULT 0
                )
            ''')
            
            # Migration columns - Whitelist validation for security
            allowed_columns = ['source_id', 'dest_id', 'sources', 'destinations', 'forward_mode', 'filters', 'modify']
            for col in allowed_columns:
                # Validate column name to prevent SQL injection (even though source is trusted)
                if not col.replace('_', '').isalnum():
                    log.error(f"Invalid column name: {col}")
                    continue
                try:
                    cursor.execute(f'ALTER TABLE forward_rules ADD COLUMN {col} TEXT')
                    log.info(f"Added {col} column")
                except sqlite3.OperationalError:
                    pass  # Column already exists

            # File deduplication tracking table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    rule_id INTEGER NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    dest_chat_id INTEGER NOT NULL,
                    file_size INTEGER,
                    file_name TEXT,
                    file_hash TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(file_unique_id, rule_id, dest_chat_id)
                )
            ''')

            # Create indexes for faster lookups
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_file_cache_lookup
                ON file_cache(file_unique_id, rule_id, dest_chat_id)
            ''')

            # ========== PERFORMANCE INDEXES ==========
            # Index for get_user_rules query (Line 1104)
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_rules_user_id_enabled
                ON forward_rules(user_id, is_enabled)
            ''')

            # Index for get_rules_by_phone query (Line 1140)
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_rules_phone_enabled
                ON forward_rules(phone, is_enabled)
            ''')

            # Index for get_all_active_phones query (Line 1256)
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_accounts_phone_active
                ON connected_accounts(phone, is_active)
            ''')

            # Index for file cache cleanup (Line 1301)
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_file_cache_processed_at
                ON file_cache(processed_at)
            ''')

            # Index for rule status queries
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_rules_id_enabled
                ON forward_rules(id, is_enabled)
            ''')

            # Index for user account queries
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_accounts_user_active
                ON connected_accounts(user_id, is_active)
            ''')

            log.info("✅ Database indexes created successfully")
            conn.commit()

    async def ensure_user(self, user_id: int, username: str = None, first_name: str = None):
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO users (user_id, username, first_name) VALUES (?, ?, ?)',
                             (user_id, username, first_name))
                conn.commit()
    
    async def add_connected_account(self, user_id: int, phone: str, display_name: str = None):
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO connected_accounts (user_id, phone, display_name, is_active) VALUES (?, ?, ?, 1)',
                             (user_id, phone, display_name))
                conn.commit()
    
    async def get_user_accounts(self, user_id: int) -> List[dict]:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT phone, display_name, connected_at FROM connected_accounts WHERE user_id = ? AND is_active = 1', (user_id,))
                return [dict(row) for row in cursor.fetchall()]
    
    async def remove_account(self, user_id: int, phone: str):
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE connected_accounts SET is_active = 0 WHERE user_id = ? AND phone = ?', (user_id, phone))
                cursor.execute('UPDATE forward_rules SET is_enabled = 0 WHERE user_id = ? AND phone = ?', (user_id, phone))
                conn.commit()
    
    async def add_forward_rule(self, user_id: int, phone: str, sources: List[str], destinations: List[str], forward_mode: str = "forward", filters: dict = None, modify: dict = None) -> int:
        """Add rule with multiple sources and destinations."""
        await self.ensure_initialized()
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                # Store as comma-separated for backward compatibility
                source_str = ','.join(sources)
                dest_str = ','.join(destinations)
                filters_str = json.dumps(filters) if filters else None
                modify_str = json.dumps(modify) if modify else None
                cursor.execute('''
                    INSERT INTO forward_rules (user_id, phone, source, destination, sources, destinations, forward_mode, filters, modify)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, phone, source_str, dest_str, source_str, dest_str, forward_mode, filters_str, modify_str))
                conn.commit()
                return cursor.lastrowid
    
    async def get_user_rules(self, user_id: int) -> List[dict]:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM forward_rules WHERE user_id = ? ORDER BY id DESC', (user_id,))
                rows = cursor.fetchall()
                result = []
                for row in rows:
                    d = dict(row)
                    # Parse sources and destinations
                    d['sources'] = d.get('sources') or d.get('source', '')
                    d['destinations'] = d.get('destinations') or d.get('destination', '')
                    d['source_list'] = [s.strip() for s in d['sources'].split(',') if s.strip()]
                    d['dest_list'] = [s.strip() for s in d['destinations'].split(',') if s.strip()]
                    d['forward_mode'] = d.get('forward_mode') or 'forward'
                    # Parse filters
                    filters_str = d.get('filters')
                    if filters_str:
                        try:
                            d['filters'] = json.loads(filters_str)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            d['filters'] = DEFAULT_FILTERS.copy()
                    else:
                        d['filters'] = DEFAULT_FILTERS.copy()
                    # Parse modify settings
                    modify_str = d.get('modify')
                    if modify_str:
                        try:
                            d['modify'] = json.loads(modify_str)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            d['modify'] = DEFAULT_MODIFY.copy()
                    else:
                        d['modify'] = DEFAULT_MODIFY.copy()
                    result.append(d)
                return result
    
    async def get_rules_by_phone(self, phone: str) -> List[dict]:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM forward_rules WHERE phone = ? AND is_enabled = 1', (phone,))
                rows = cursor.fetchall()
                result = []
                for row in rows:
                    d = dict(row)
                    d['sources'] = d.get('sources') or d.get('source', '')
                    d['destinations'] = d.get('destinations') or d.get('destination', '')
                    d['source_list'] = [s.strip() for s in d['sources'].split(',') if s.strip()]
                    d['dest_list'] = [s.strip() for s in d['destinations'].split(',') if s.strip()]
                    d['forward_mode'] = d.get('forward_mode') or 'forward'
                    # Parse filters
                    filters_str = d.get('filters')
                    if filters_str:
                        try:
                            d['filters'] = json.loads(filters_str)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            d['filters'] = DEFAULT_FILTERS.copy()
                    else:
                        d['filters'] = DEFAULT_FILTERS.copy()
                    # Parse modify settings
                    modify_str = d.get('modify')
                    if modify_str:
                        try:
                            d['modify'] = json.loads(modify_str)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            d['modify'] = DEFAULT_MODIFY.copy()
                    else:
                        d['modify'] = DEFAULT_MODIFY.copy()
                    result.append(d)
                return result
    
    async def delete_rule(self, user_id: int, rule_id: int) -> bool:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM forward_rules WHERE id = ? AND user_id = ?', (rule_id, user_id))
                conn.commit()
                return cursor.rowcount > 0
    
    async def toggle_rule(self, user_id: int, rule_id: int) -> Optional[bool]:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE forward_rules SET is_enabled = 1 - is_enabled WHERE id = ? AND user_id = ?', (rule_id, user_id))
                conn.commit()
                if cursor.rowcount > 0:
                    cursor.execute('SELECT is_enabled FROM forward_rules WHERE id = ?', (rule_id,))
                    row = cursor.fetchone()
                    return bool(row['is_enabled']) if row else None
                return None
    
    async def update_rule_mode(self, user_id: int, rule_id: int, mode: str) -> bool:
        """Update rule forward mode."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE forward_rules SET forward_mode = ? WHERE id = ? AND user_id = ?', 
                             (mode, rule_id, user_id))
                conn.commit()
                return cursor.rowcount > 0
    
    async def update_rule_sources(self, user_id: int, rule_id: int, sources: List[str]) -> bool:
        """Update rule sources."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                source_str = ','.join(sources)
                cursor.execute('UPDATE forward_rules SET source = ?, sources = ? WHERE id = ? AND user_id = ?', 
                             (source_str, source_str, rule_id, user_id))
                conn.commit()
                return cursor.rowcount > 0
    
    async def update_rule_destinations(self, user_id: int, rule_id: int, destinations: List[str]) -> bool:
        """Update rule destinations."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                dest_str = ','.join(destinations)
                cursor.execute('UPDATE forward_rules SET destination = ?, destinations = ? WHERE id = ? AND user_id = ?', 
                             (dest_str, dest_str, rule_id, user_id))
                conn.commit()
                return cursor.rowcount > 0
    
    async def update_rule_filters(self, user_id: int, rule_id: int, filters: dict) -> bool:
        """Update rule filters."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                filters_str = json.dumps(filters)
                cursor.execute('UPDATE forward_rules SET filters = ? WHERE id = ? AND user_id = ?', 
                             (filters_str, rule_id, user_id))
                conn.commit()
                return cursor.rowcount > 0
    
    async def update_rule_modify(self, user_id: int, rule_id: int, modify: dict) -> bool:
        """Update rule modify settings."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                modify_str = json.dumps(modify)
                cursor.execute('UPDATE forward_rules SET modify = ? WHERE id = ? AND user_id = ?', 
                             (modify_str, rule_id, user_id))
                conn.commit()
                return cursor.rowcount > 0
    
    async def increment_forward_count(self, rule_id: int):
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE forward_rules SET forward_count = forward_count + 1 WHERE id = ?', (rule_id,))
                conn.commit()
    
    async def get_all_active_phones(self) -> List[str]:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT DISTINCT phone FROM connected_accounts WHERE is_active = 1')
                return [row['phone'] for row in cursor.fetchall()]
    
    async def get_phone_user_id(self, phone: str) -> Optional[int]:
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT user_id FROM connected_accounts WHERE phone = ? AND is_active = 1', (phone,))
                row = cursor.fetchone()
                return row['user_id'] if row else None

    async def is_file_processed(self, file_unique_id: str, rule_id: int, dest_chat_id: int) -> bool:
        """Check if file was already processed for this rule and destination."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id FROM file_cache
                    WHERE file_unique_id = ? AND rule_id = ? AND dest_chat_id = ?
                ''', (file_unique_id, rule_id, dest_chat_id))
                return cursor.fetchone() is not None

    async def mark_file_processed(self, file_id: str, file_unique_id: str, rule_id: int,
                                  source_chat_id: int, dest_chat_id: int,
                                  file_size: int = 0, file_name: str = None):
        """Mark file as processed to prevent duplicate forwarding."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO file_cache
                        (file_id, file_unique_id, rule_id, source_chat_id, dest_chat_id, file_size, file_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (file_id, file_unique_id, rule_id, source_chat_id, dest_chat_id, file_size, file_name))
                    conn.commit()
                except Exception as e:
                    log.error(f"Failed to mark file as processed: {e}")

    async def clear_old_file_cache(self, days: int = 30):
        """Clear file cache older than specified days."""
        async with self._lock:
            async with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM file_cache
                    WHERE processed_at < datetime('now', '-' || ? || ' days')
                ''', (days,))
                deleted = cursor.rowcount
                conn.commit()
                if deleted > 0:
                    log.info(f"🧹 Cleared {deleted} old file cache entries")
                return deleted

# ==================== SESSION MANAGER ====================
class UserSessionManager:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.clients: Dict[str, TelegramClient] = {}
        self.handlers_attached: Set[str] = set()
        self._locks: Dict[str, Lock] = {}
        # Use managed album cache with automatic cleanup
        self.album_cache_manager = AlbumCacheManager(ttl_seconds=5, cleanup_interval=10)
        # Entity resolution cache for performance
        self.entity_cache = LRUCache(max_size=5000, ttl_seconds=3600)
        self._album_cache_started = False
    
    def _get_session_path(self, user_id: int, phone: str) -> str:
        safe_phone = phone.replace("+", "").replace(" ", "")
        return os.path.join(SESSION_DIR, f"user_{user_id}_{safe_phone}")
    
    async def get_or_create_client(self, user_id: int, phone: str) -> TelegramClient:
        if not TELETHON_AVAILABLE:
            raise RuntimeError("Telethon not installed")
        
        if phone in self.clients:
            client = self.clients[phone]
            if not client.is_connected():
                await client.connect()
            return client
        
        session_path = self._get_session_path(user_id, phone)
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()
        self.clients[phone] = client
        return client
    
    async def disconnect_client(self, phone: str):
        if phone in self.clients:
            try:
                client = self.clients[phone]
                if client.is_connected():
                    await client.disconnect()
            except Exception as e:
                log.error(f"Error disconnecting {phone}: {e}")
            finally:
                self.clients.pop(phone, None)
                self.handlers_attached.discard(phone)
    
    async def resolve_entity(self, phone: str, identifier: str) -> tuple:
        """Returns (success, entity, error_msg)"""
        client = self.clients.get(phone)
        if not client:
            return False, None, "Client not connected"

        # Check cache first
        cache_key = f"{phone}:{identifier}"
        cached_entity = await self.entity_cache.get(cache_key)
        if cached_entity:
            return True, cached_entity, None

        try:
            if identifier.startswith('@'):
                entity = await client.get_entity(identifier)
                await self.entity_cache.set(cache_key, entity)
                return True, entity, None

            try:
                num_id = int(identifier)
            except ValueError:
                return False, None, "Invalid ID format"

            # Try direct resolution
            try:
                entity = await client.get_entity(num_id)
                await self.entity_cache.set(cache_key, entity)
                return True, entity, None
            except Exception:
                pass
            
            # For -100 format channel IDs
            if str(num_id).startswith('-100'):
                real_id = int(str(num_id)[4:])
                try:
                    entity = await client.get_entity(PeerChannel(real_id))
                    return True, entity, None
                except Exception:
                    pass
            
            # Search through dialogs
            async for dialog in client.iter_dialogs():
                dialog_id = dialog.id
                if dialog_id == num_id or abs(dialog_id) == abs(num_id):
                    return True, dialog.entity, None
                if str(num_id).startswith('-100'):
                    real_id = int(str(num_id)[4:])
                    if dialog_id == real_id or abs(dialog_id) == real_id:
                        return True, dialog.entity, None
            
            return False, None, "Entity not found. Make sure account is a member."
            
        except Exception as e:
            return False, None, str(e)
    
    async def load_existing_sessions(self):
        if not TELETHON_AVAILABLE:
            return

        # Start album cache cleanup task
        if not self._album_cache_started:
            await self.album_cache_manager.start()
            self._album_cache_started = True
            log.info("✅ Album cache manager started")

        phones = await self.db.get_all_active_phones()
        log.info(f"Loading {len(phones)} sessions...")
        
        for phone in phones:
            try:
                user_id = await self.db.get_phone_user_id(phone)
                if not user_id:
                    continue
                
                session_path = self._get_session_path(user_id, phone)
                if not os.path.exists(session_path + '.session'):
                    continue
                
                client = TelegramClient(session_path, API_ID, API_HASH)
                await client.connect()
                
                if await client.is_user_authorized():
                    self.clients[phone] = client
                    await self.attach_forward_handler(phone)
                    log.info(f"✅ Loaded: {phone}")
                else:
                    await client.disconnect()
            except Exception as e:
                log.error(f"Failed loading {phone}: {e}")
    
    async def attach_forward_handler(self, phone: str):
        if phone in self.handlers_attached:
            return
        
        client = self.clients.get(phone)
        if not client:
            return
        
        db_ref = self.db
        phone_ref = phone
        entity_cache = {}
        entity_cache_lock = asyncio.Lock()  # Protect concurrent access to entity_cache

        async def retry_on_timeout(func, *args, max_retries=3, **kwargs):
            """Retry function on timeout/connection errors."""
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (OSError, TimeoutError, ConnectionError) as e:
                    error_msg = str(e)
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2  # 2s, 4s, 6s
                        log.warning(f"⚠️ [{phone_ref}] Timeout/connection error (attempt {attempt + 1}/{max_retries}): {error_msg}. Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        # Reconnect client if needed
                        if not client.is_connected():
                            log.info(f"🔄 [{phone_ref}] Reconnecting client...")
                            await client.connect()
                    else:
                        log.error(f"❌ [{phone_ref}] Failed after {max_retries} attempts: {error_msg}")
                        raise
                except errors.FloodWaitError as e:
                    if attempt < max_retries - 1:
                        wait_time = e.seconds
                        log.warning(f"⚠️ [{phone_ref}] FloodWait: sleeping {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                    else:
                        raise
                except Exception as e:
                    # Don't retry on other errors
                    raise

        async def resolve_dest(dest_str: str):
            """Resolve destination with caching (thread-safe)."""
            # Check cache with lock (fast path)
            async with entity_cache_lock:
                if dest_str in entity_cache:
                    return entity_cache[dest_str]

            # Cache miss - resolve entity (slow I/O, lock not held)
            entity = None
            try:
                if dest_str.startswith('@'):
                    entity = await client.get_entity(dest_str)
                else:
                    dest_id = int(dest_str)

                    try:
                        entity = await client.get_entity(dest_id)
                    except Exception:
                        pass

                    if not entity and str(dest_id).startswith('-100'):
                        real_id = int(str(dest_id)[4:])
                        try:
                            entity = await client.get_entity(PeerChannel(real_id))
                        except Exception:
                            pass

                    # Fallback: scan dialogs (limit to 500 to prevent infinite loop)
                    if not entity:
                        count = 0
                        async for dialog in client.iter_dialogs():
                            count += 1
                            if count > 500:
                                break

                            did = dialog.id
                            if did == dest_id or abs(did) == abs(dest_id):
                                entity = dialog.entity
                                break
                            if str(dest_id).startswith('-100'):
                                real_id = int(str(dest_id)[4:])
                                if did == real_id or abs(did) == real_id:
                                    entity = dialog.entity
                                    break

            except Exception as e:
                log.error(f"Failed to resolve {dest_str}: {e}")
                return None

            # Update cache with lock
            if entity:
                async with entity_cache_lock:
                    entity_cache[dest_str] = entity

            return entity
        
        def check_source_match(chat_id: int, chat_username: str, source: str) -> bool:
            """Check if chat matches a source."""
            # Username match
            if source.startswith('@'):
                src_username = source[1:].lower()
                return chat_username and chat_username.lower() == src_username
            
            # Numeric ID match
            try:
                src_num = int(source)
                if chat_id == src_num or abs(chat_id) == abs(src_num):
                    return True
                # Handle -100 prefix
                if str(src_num).startswith('-100'):
                    src_real = int(str(src_num)[4:])
                    if str(chat_id).startswith('-100'):
                        chat_real = int(str(chat_id)[4:])
                        if chat_real == src_real:
                            return True
                    if abs(chat_id) == src_real:
                        return True
                if str(chat_id).startswith('-100'):
                    chat_real = int(str(chat_id)[4:])
                    if chat_real == abs(src_num):
                        return True
            except ValueError:
                pass
            
            return False
        
        # Album handling - use manager for automatic cleanup
        album_cache_manager_ref = self.album_cache_manager

        async def send_album_group(grouped_id: int):
            """Send collected album messages as a group."""
            album_data = await album_cache_manager_ref.pop(grouped_id)
            if not album_data:
                return

            messages = album_data['messages']
            dest_list = album_data['dest_list']
            rule = album_data['rule']
            original_caption = album_data.get('caption_text', '')
            filters = album_data.get('filters', {})
            modify = album_data.get('modify', {})
            forward_mode = rule.get('forward_mode', 'forward')

            if not messages:
                return

            log.info(f"📚 [{phone_ref}] Sending album with {len(messages)} items")

            # Apply caption cleaning to album caption
            caption_text = original_caption

            # Apply caption cleaning filters
            if filters.get('clean_caption', False):
                caption_text = ""
            elif caption_text:
                # Apply specific cleaners (using pre-compiled regex patterns)
                if filters.get('clean_hashtag', False):
                    caption_text = REGEX_HASHTAG.sub('', caption_text)
                if filters.get('clean_mention', False):
                    caption_text = REGEX_MENTION.sub('', caption_text)
                if filters.get('clean_link', False):
                    caption_text = REGEX_URL_LEGACY.sub('', caption_text)
                if filters.get('clean_emoji', False):
                    caption_text = REGEX_EMOJI.sub('', caption_text)
                if filters.get('clean_phone', False):
                    caption_text = REGEX_PHONE_BASIC.sub('', caption_text)
                if filters.get('clean_email', False):
                    caption_text = REGEX_EMAIL_STRICT.sub('', caption_text)

                # Clean up whitespace (using pre-compiled patterns)
                caption_text = REGEX_WHITESPACE_MULTI.sub(' ', caption_text)
                caption_text = REGEX_LINE_TRIM.sub('', caption_text)
                caption_text = REGEX_NEWLINE_MULTI.sub('\n\n', caption_text)
                caption_text = caption_text.strip()

            # Apply modify features
            # Track if we should use entities
            caption_entities = None

            # Apply custom caption if enabled (replaces original caption)
            if modify.get('caption_enabled', False):
                custom_caption = modify.get('caption_text', '')
                if custom_caption:
                    caption_text = custom_caption
                    # Get pre-parsed entities for custom caption (deserialize from storage)
                    caption_entities = deserialize_entities(modify.get('caption_entities', []))

            if modify.get('header_enabled', False):
                header = modify.get('header_text', '')
                if header:
                    caption_text = f"{header}\n{caption_text}" if caption_text else header
                    # Clear entities if modifying after custom caption
                    caption_entities = None

            if modify.get('footer_enabled', False):
                footer = modify.get('footer_text', '')
                if footer:
                    caption_text = f"{caption_text}\n{footer}" if caption_text else footer
                    # Clear entities if modifying after custom caption
                    caption_entities = None

            # Check if caption cleaning or modification is active
            caption_cleaning_active = any([
                filters.get('clean_caption', False),
                filters.get('clean_hashtag', False),
                filters.get('clean_mention', False),
                filters.get('clean_link', False),
                filters.get('clean_emoji', False),
                filters.get('clean_phone', False),
                filters.get('clean_email', False)
            ])
            modify_caption_active = any([
                modify.get('header_enabled', False),
                modify.get('footer_enabled', False),
                modify.get('caption_enabled', False),
                modify.get('replace_enabled', False),
                modify.get('watermark_enabled', False),  # Watermark requires copy mode
                modify.get('apply_spoiler', False)  # Spoiler effect requires copy mode
            ])

            # Use copy mode if explicitly selected OR if caption/content modification is needed
            use_copy_mode = (forward_mode == "copy") or caption_cleaning_active or modify_caption_active

            for dest in dest_list:
                try:
                    dest_entity = await resolve_dest(dest)
                    if dest_entity is None:
                        log.error(f"❌ [{phone_ref}] Could not resolve: {dest}")
                        continue

                    if use_copy_mode:
                        # COPY MODE: Download and re-upload as album
                        import tempfile
                        import os as temp_os

                        # Download all media files
                        files = []
                        for msg in messages:
                            temp_file = await retry_on_timeout(
                                client.download_media,
                                msg,
                                file=tempfile.gettempdir()
                            )
                            if temp_file:
                                # Apply watermark if enabled
                                if modify.get('watermark_enabled', False):
                                    try:
                                        is_image = msg.photo or (hasattr(msg, 'document') and msg.document and hasattr(msg.document, 'mime_type') and msg.document.mime_type and msg.document.mime_type.startswith('image/'))
                                        is_video = msg.video or (hasattr(msg, 'document') and msg.document and hasattr(msg.document, 'mime_type') and msg.document.mime_type and msg.document.mime_type.startswith('video/'))

                                        if is_image or is_video:
                                            original_file = temp_file
                                            # Sanitize basename to prevent path traversal
                                            safe_basename = temp_os.path.basename(temp_file).replace('..', '')
                                            watermarked_file = temp_os.path.join(
                                                temp_os.path.dirname(temp_file),
                                                'watermarked_' + safe_basename
                                            )

                                            success = False
                                            media_type = "video" if is_video else "image"
                                            log.info(f"🎨 [{phone_ref}] Album: Applying {media_type} watermark...")

                                            # Use FFmpeg for watermarking (works for both images and videos)
                                            success = apply_watermark_with_ffmpeg(temp_file, watermarked_file, modify, is_video=is_video)

                                            if success and temp_os.path.exists(watermarked_file):
                                                try:
                                                    temp_os.remove(original_file)
                                                except (OSError, IOError) as e:
                                                    log.warning(f"Failed to cleanup original file {original_file}: {e}")
                                                temp_file = watermarked_file
                                    except Exception as e:
                                        log.error(f"[{phone_ref}] Album watermark error: {e}")

                                files.append(temp_file)

                        if files:
                            try:
                                # Send as album (first file gets caption with entities)
                                send_kwargs = {}
                                if caption_text:
                                    send_kwargs['caption'] = caption_text
                                    if caption_entities:
                                        send_kwargs['formatting_entities'] = caption_entities

                                # Apply spoiler effect if enabled (pass as list for albums)
                                if modify.get('apply_spoiler', False):
                                    send_kwargs['spoiler'] = [True] * len(files)

                                await retry_on_timeout(
                                    client.send_file,
                                    dest_entity,
                                    files,
                                    **send_kwargs
                                )
                                log.info(f"📚 [{phone_ref}] ALBUM ({len(files)} files) -> {dest}")
                            finally:
                                # Clean up temp files
                                for f in files:
                                    try:
                                        if f and temp_os.path.exists(f):
                                            temp_os.remove(f)
                                    except Exception:
                                        pass
                    else:
                        # FORWARD MODE: Forward all messages together
                        await client.forward_messages(entity=dest_entity, messages=messages)
                        log.info(f"✅ [{phone_ref}] ALBUM forwarded ({len(messages)} items) -> {dest}")

                except Exception as e:
                    log.error(f"❌ [{phone_ref}] Album send failed: {e}")
        
        @client.on(events.NewMessage(incoming=True))
        async def forward_handler(event):
            try:
                chat_id = event.chat_id
                chat = await event.get_chat()
                chat_username = getattr(chat, 'username', None)
                
                rules = await db_ref.get_rules_by_phone(phone_ref)
                if not rules:
                    return
                
                for rule in rules:
                    source_list = rule.get('source_list', [])
                    dest_list = rule.get('dest_list', [])
                    rule_id = rule['id']
                    forward_mode = rule.get('forward_mode', 'forward')
                    filters = rule.get('filters', {})
                    modify = rule.get('modify', {})

                    # Check if message matches ANY source
                    is_match = False
                    matched_source = None
                    
                    for src in source_list:
                        if check_source_match(chat_id, chat_username, src):
                            is_match = True
                            matched_source = src
                            break
                    
                    if not is_match:
                        continue
                    
                    # ========== FILTER CHECK ==========
                    msg = event.message
                    
                    # Determine message type for filtering
                    msg_type = None
                    has_caption = bool(msg.message or msg.text)
                    
                    if msg.photo:
                        msg_type = 'photo'
                        # Check photo_only (photo WITHOUT text)
                        if filters.get('photo_only', False) and not has_caption:
                            log.info(f"[{phone_ref}] SKIPPED: photo_only (no caption)")
                            continue
                        # Check photo_with_text (photo WITH text)
                        if filters.get('photo_with_text', False) and has_caption:
                            log.info(f"[{phone_ref}] SKIPPED: photo_with_text (has caption)")
                            continue
                    elif msg.video_note:
                        msg_type = 'video_note'
                    elif msg.video:
                        msg_type = 'video'
                    elif msg.voice:
                        msg_type = 'voice'
                    elif msg.audio:
                        msg_type = 'audio'
                    elif msg.sticker:
                        msg_type = 'sticker'
                    elif msg.gif:
                        msg_type = 'gif'
                    elif msg.document:
                        msg_type = 'document'
                    elif msg.poll:
                        msg_type = 'poll'
                    elif msg.text or msg.message:
                        msg_type = 'text'
                    
                    # Check POLL filter
                    if msg.poll and filters.get('poll', False):
                        log.info(f"[{phone_ref}] SKIPPED: poll")
                        continue
                    
                    # Check ALBUM filter (grouped media) - if filter ON, skip albums
                    if msg.grouped_id and filters.get('album', False):
                        log.info(f"[{phone_ref}] SKIPPED: album (grouped_id={msg.grouped_id})")
                        continue
                    
                    # Handle ALBUM (grouped media) - collect and send as group
                    if msg.grouped_id and not filters.get('album', False):
                        grouped_id = msg.grouped_id

                        # Check if this is first message of album
                        existing = await album_cache_manager_ref.get(grouped_id)
                        if not existing:
                            # First message of album - start collecting
                            await album_cache_manager_ref.set(grouped_id, {
                                'messages': [msg],
                                'dest_list': dest_list,
                                'rule': rule,
                                'caption_text': msg.message or msg.text or "",
                                'filters': filters,
                                'modify': modify,
                            })
                            # Schedule sending after 1.5 seconds
                            loop = asyncio.get_running_loop()
                            loop.call_later(
                                1.5,
                                lambda gid=grouped_id: asyncio.create_task(send_album_group(gid))
                            )
                            log.info(f"📚 [{phone_ref}] Album started: {grouped_id}")
                        else:
                            # Additional message in album
                            existing['messages'].append(msg)
                            if msg.message or msg.text:
                                existing['caption_text'] = msg.message or msg.text
                            await album_cache_manager_ref.set(grouped_id, existing)
                            log.info(f"📚 [{phone_ref}] Album item added: {grouped_id} (total: {len(existing['messages'])})")

                        # Skip normal processing - album will be sent by timer
                        continue
                    
                    # Check FORWARD filter (forwarded messages)
                    if msg.forward and filters.get('forward', False):
                        log.info(f"[{phone_ref}] SKIPPED: forwarded message")
                        continue
                    
                    # Check REPLY filter
                    if msg.reply_to and filters.get('reply', False):
                        log.info(f"[{phone_ref}] SKIPPED: reply message")
                        continue

                    # Check BUTTON filter (inline keyboards)
                    if msg.reply_markup and filters.get('button', False):
                        log.info(f"[{phone_ref}] SKIPPED: has buttons")
                        continue
                    
                    # Check animated EMOJI filter
                    if filters.get('emoji', False):
                        # Check for custom emoji entities
                        if msg.entities:
                            has_custom_emoji = False
                            for ent in msg.entities:
                                if hasattr(ent, '__class__') and ent.__class__.__name__ == 'MessageEntityCustomEmoji':
                                    has_custom_emoji = True
                                    break
                            if has_custom_emoji:
                                log.info(f"[{phone_ref}] SKIPPED: has custom/animated emoji")
                                continue
                    
                    # Check if this message type should be SKIPPED
                    # filter value True = SKIP this type (checked button)
                    # filter value False = KEEP this type (unchecked button)
                    skip_filter = filters.get(msg_type, False) if msg_type else False
                    if skip_filter:
                        log.info(f"[{phone_ref}] SKIPPED: {msg_type} (filter ON)")
                        continue
                    
                    # ========== CAPTION CONTENT CLEANING ==========
                    # Get original text/caption
                    original_text = msg.message or msg.text or ""
                    original_entities = msg.entities  # Capture original formatting entities
                    filtered_text = original_text
                    removed_items = []
                    
                    # 0. CAPTION REMOVE - Remove entire caption
                    if filters.get('clean_caption', False):
                        if original_text:
                            filtered_text = ""
                            removed_items.append('entire caption')
                    
                    # 1. HASHTAG CLEANER - Remove all #hashtags (pre-compiled pattern)
                    if filters.get('clean_hashtag', False) and filtered_text:
                        before = filtered_text
                        filtered_text = REGEX_HASHTAG.sub('', filtered_text)
                        if before != filtered_text:
                            removed_items.append('#hashtags')

                    # 2. MENTION CLEANER - Remove all @mentions (pre-compiled pattern)
                    if filters.get('clean_mention', False):
                        before = filtered_text
                        filtered_text = REGEX_MENTION.sub('', filtered_text)
                        if before != filtered_text:
                            removed_items.append('@mentions')

                    # 3. LINK CLEANER - Remove URLs from text (pre-compiled patterns)
                    if filters.get('clean_link', False):
                        before = filtered_text
                        # Remove various URL formats using pre-compiled patterns
                        filtered_text = REGEX_URL_HTTPS.sub('', filtered_text)
                        filtered_text = REGEX_URL_HTTP.sub('', filtered_text)
                        filtered_text = REGEX_URL_WWW.sub('', filtered_text)
                        filtered_text = REGEX_URL_TG_ME.sub('', filtered_text)
                        filtered_text = REGEX_URL_TG_SCHEME.sub('', filtered_text)
                        if before != filtered_text:
                            removed_items.append('links')
                    
                    # 4. EMOJI CLEANER - Remove all emojis from text (pre-compiled pattern)
                    if filters.get('clean_emoji', False):
                        before = filtered_text
                        filtered_text = REGEX_EMOJI.sub('', filtered_text)
                        if before != filtered_text:
                            removed_items.append('emojis')

                    # 5. PHONE CLEANER - Remove phone numbers (pre-compiled patterns)
                    if filters.get('clean_phone', False):
                        before = filtered_text
                        # Match various phone formats using pre-compiled patterns
                        filtered_text = REGEX_PHONE_FORMATTED.sub('', filtered_text)
                        filtered_text = REGEX_PHONE_SIMPLE.sub('', filtered_text)
                        if before != filtered_text:
                            removed_items.append('phones')

                    # 6. EMAIL CLEANER - Remove email addresses (pre-compiled pattern)
                    if filters.get('clean_email', False):
                        before = filtered_text
                        filtered_text = REGEX_EMAIL.sub('', filtered_text)
                        if before != filtered_text:
                            removed_items.append('emails')

                    # Clean up the filtered text (pre-compiled patterns)
                    filtered_text = REGEX_WHITESPACE_MULTI.sub(' ', filtered_text)  # multiple spaces
                    filtered_text = REGEX_LINE_TRIM.sub('', filtered_text)  # line trim
                    filtered_text = REGEX_NEWLINE_MULTI.sub('\n\n', filtered_text)  # multiple newlines
                    filtered_text = filtered_text.strip()
                    
                    # Store for copy mode
                    caption_text = filtered_text
                    caption_entities = None  # Entities invalid after text modification
                    
                    # Link preview removal
                    remove_link_preview = filters.get('clean_link', False)
                    
                    log.info(f"[{phone_ref}] MATCH rule {rule_id}: {msg_type}, mode={forward_mode}")
                    if removed_items:
                        log.info(f"[{phone_ref}] Cleaned: {', '.join(removed_items)}")
                        log.info(f"[{phone_ref}] Before: {original_text[:100]}")
                        log.info(f"[{phone_ref}] After: {filtered_text[:100]}")

                    # ========== MODIFY CONTENT FEATURES ==========

                    # 1. BLOCK WORDS - Skip message if contains blocked words
                    if modify.get('block_words_enabled', False):
                        block_words = modify.get('block_words', [])
                        if block_words and caption_text:
                            text_lower = caption_text.lower()
                            should_skip = False
                            for word in block_words:
                                if word.lower() in text_lower:
                                    log.info(f"[{phone_ref}] SKIPPED: Contains blocked word '{word}'")
                                    should_skip = True
                                    break  # Stop checking more words
                            if should_skip:
                                continue  # Skip to next rule

                    # 2. WHITELIST WORDS - Skip message if NOT contains whitelist words
                    if modify.get('whitelist_enabled', False):
                        whitelist_words = modify.get('whitelist_words', [])
                        if whitelist_words and caption_text:
                            text_lower = caption_text.lower()
                            has_whitelist = any(word.lower() in text_lower for word in whitelist_words)
                            if not has_whitelist:
                                log.info(f"[{phone_ref}] SKIPPED: Does not contain any whitelist word")
                                continue  # Skip to next rule

                    # 3. WORD REPLACEMENT - Replace words/phrases in text
                    # Note: User-defined regex patterns are cached for performance
                    if modify.get('replace_enabled', False):
                        replace_pairs = modify.get('replace_pairs', [])
                        if replace_pairs and caption_text:
                            # Cache for user-defined regex patterns (prevents recompilation)
                            if not hasattr(forward_handler, '_user_regex_cache'):
                                forward_handler._user_regex_cache = {}

                            for pair in replace_pairs:
                                old = pair.get('from', '')
                                new = pair.get('to', '')
                                use_regex = pair.get('regex', False)
                                if old:
                                    if use_regex:
                                        try:
                                            # Use cached compiled pattern if available
                                            if old not in forward_handler._user_regex_cache:
                                                forward_handler._user_regex_cache[old] = re.compile(old)
                                            pattern = forward_handler._user_regex_cache[old]
                                            caption_text = pattern.sub(new, caption_text)
                                        except Exception as e:
                                            log.error(f"[{phone_ref}] Regex replace error: {e}")
                                    else:
                                        caption_text = caption_text.replace(old, new)

                    # 4. CUSTOM CAPTION - Replace original caption with custom caption
                    # Store caption entities for formatted captions
                    custom_caption_entities = None
                    if modify.get('caption_enabled', False):
                        custom_caption = modify.get('caption_text', '')
                        if custom_caption:
                            caption_text = custom_caption
                            # Get pre-parsed entities for custom caption (deserialize from storage)
                            custom_caption_entities = deserialize_entities(modify.get('caption_entities', []))
                            # Clear original entities since we're replacing caption
                            caption_entities = custom_caption_entities

                    # 5. HEADER TEXT - Add text to beginning
                    if modify.get('header_enabled', False):
                        header = modify.get('header_text', '')
                        if header:
                            caption_text = f"{header}\n{caption_text}" if caption_text else header
                            # Clear entities if modifying after custom caption
                            if custom_caption_entities:
                                caption_entities = None

                    # 6. FOOTER TEXT - Add text to end
                    if modify.get('footer_enabled', False):
                        footer = modify.get('footer_text', '')
                        if footer:
                            caption_text = f"{caption_text}\n{footer}" if caption_text else footer
                            # Clear entities if modifying after custom caption
                            if custom_caption_entities:
                                caption_entities = None

                    # 7. DELAY - Wait before forwarding
                    if modify.get('delay_enabled', False):
                        delay_seconds = modify.get('delay_seconds', 0)
                        if delay_seconds > 0:
                            log.info(f"[{phone_ref}] Delaying {delay_seconds}s before forwarding...")
                            await asyncio.sleep(delay_seconds)

                    # 7. LINK BUTTONS - Prepare custom button markup
                    button_markup = None
                    if modify.get('buttons_enabled', False) and types:
                        buttons_data = modify.get('buttons', [])
                        if buttons_data:
                            try:
                                # Build button rows (Telethon requires KeyboardButtonRow objects)
                                button_rows = []
                                for row in buttons_data:
                                    if isinstance(row, list):
                                        btn_row = []
                                        for btn in row:
                                            if isinstance(btn, dict) and 'text' in btn and 'url' in btn:
                                                btn_row.append(types.KeyboardButtonUrl(btn['text'], btn['url']))
                                        if btn_row:
                                            # Wrap each row in KeyboardButtonRow
                                            button_rows.append(types.KeyboardButtonRow(btn_row))
                                if button_rows:
                                    button_markup = types.ReplyInlineMarkup(button_rows)
                                    log.info(f"[{phone_ref}] Added {len(button_rows)} button row(s)")
                            except Exception as e:
                                log.error(f"[{phone_ref}] Button creation error: {e}")

                    # Forward/Copy to ALL destinations
                    success_count = 0
                    for dest in dest_list:
                        try:
                            dest_entity = await resolve_dest(dest)
                            if dest_entity is None:
                                log.error(f"❌ [{phone_ref}] Could not resolve: {dest}")
                                continue

                            # Get destination chat ID for deduplication
                            dest_chat_id = dest_entity.id if hasattr(dest_entity, 'id') else 0

                            # === DEDUPLICATION CHECK ===
                            # Get file unique ID for duplicate detection
                            file_unique_id = None
                            file_id_for_cache = None
                            file_name_for_cache = None

                            if msg.media:
                                # Extract file unique ID from media
                                if hasattr(msg, 'document') and msg.document:
                                    file_unique_id = getattr(msg.document, 'id', None)
                                    file_id_for_cache = str(file_unique_id) if file_unique_id else None
                                    # Get filename from attributes
                                    for attr in getattr(msg.document, 'attributes', []):
                                        if hasattr(attr, 'file_name'):
                                            file_name_for_cache = attr.file_name
                                            break
                                elif hasattr(msg, 'photo') and msg.photo:
                                    # For photos, use the largest size ID
                                    if hasattr(msg.photo, 'id'):
                                        file_unique_id = msg.photo.id
                                        file_id_for_cache = str(file_unique_id)

                            # Check if file was already processed for this rule and destination
                            if file_unique_id:
                                is_duplicate = await db_ref.is_file_processed(
                                    str(file_unique_id),
                                    rule_id,
                                    dest_chat_id
                                )
                                if is_duplicate:
                                    log.info(f"⏭️ [{phone_ref}] SKIPPED DUPLICATE: {file_name_for_cache or 'file'} already sent to {dest}")
                                    continue  # Skip this destination

                            # Check if any caption cleaning is enabled
                            # If caption cleaning is active, force copy mode to apply the changes
                            caption_cleaning_active = any([
                                filters.get('clean_caption', False),
                                filters.get('clean_hashtag', False),
                                filters.get('clean_mention', False),
                                filters.get('clean_link', False),
                                filters.get('clean_emoji', False),
                                filters.get('clean_phone', False),
                                filters.get('clean_email', False)
                            ])

                            # Also check if modify features that affect caption or media are enabled
                            modify_caption_active = any([
                                modify.get('header_enabled', False),
                                modify.get('footer_enabled', False),
                                modify.get('caption_enabled', False),
                                modify.get('replace_enabled', False),
                                modify.get('watermark_enabled', False),  # Watermark requires copy mode
                                modify.get('apply_spoiler', False)  # Spoiler effect requires copy mode
                            ])

                            # Use copy mode if explicitly selected OR if caption/content modification is needed
                            use_copy_mode = (forward_mode == "copy") or caption_cleaning_active or modify_caption_active

                            if use_copy_mode:
                                # COPY MODE: Download & re-upload with filtered caption
                                # (Auto-enabled when caption cleaning or modification is active)
                                
                                try:
                                    # Use filtered text (hashtags, mentions, links, emojis removed if filter ON)
                                    # caption_text and caption_entities are set above in filter section
                                    
                                    # Check if message has web preview (link preview card)
                                    has_web_preview = msg.web_preview is not None and not remove_link_preview
                                    
                                    if msg.media:
                                        # Check if media is ONLY a web preview (no photo/video/doc)
                                        is_only_web_preview = (
                                            msg.web_preview is not None and 
                                            not msg.photo and 
                                            not msg.video and 
                                            not msg.document and 
                                            not msg.audio and 
                                            not msg.voice and 
                                            not msg.sticker and 
                                            not msg.gif and
                                            not msg.video_note
                                        )
                                        
                                        if is_only_web_preview:
                                            # TEXT with hidden link + preview card
                                            await client.send_message(
                                                dest_entity,
                                                caption_text,
                                                formatting_entities=caption_entities if caption_entities else None,
                                                link_preview=not remove_link_preview,  # Remove preview if link filter ON
                                                buttons=button_markup
                                            )
                                            success_count += 1
                                            log.info(f"🔗 [{phone_ref}] TEXT+PREVIEW -> {dest}")
                                        else:
                                            # HAS REAL MEDIA - Download and re-send with filtered caption
                                            import tempfile
                                            import os as temp_os

                                            # Get file size for progress tracking
                                            file_size = 0
                                            if hasattr(msg, 'document') and msg.document:
                                                file_size = msg.document.size
                                            elif hasattr(msg, 'photo') and msg.photo:
                                                file_size = max([size.size for size in msg.photo.sizes if hasattr(size, 'size')], default=0)
                                            elif hasattr(msg, 'video') and msg.video:
                                                file_size = msg.video.size if hasattr(msg.video, 'size') else 0

                                            # Format file size for logging
                                            def format_bytes(bytes_num):
                                                for unit in ['B', 'KB', 'MB', 'GB']:
                                                    if bytes_num < 1024.0:
                                                        return f"{bytes_num:.2f} {unit}"
                                                    bytes_num /= 1024.0
                                                return f"{bytes_num:.2f} TB"

                                            # Check file size limits (Telegram limits: 2GB for bots, 4GB for premium)
                                            MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB limit for bots
                                            if file_size > MAX_FILE_SIZE:
                                                log.warning(f"⚠️ [{phone_ref}] Large file detected: {format_bytes(file_size)} (may fail for non-premium)")
                                            elif file_size > 100 * 1024 * 1024:  # 100MB
                                                log.info(f"📦 [{phone_ref}] Large file: {format_bytes(file_size)} - using optimized MTProto transfer")

                                            # Progress callback for downloads
                                            last_percentage = [0]  # Use list to allow modification in nested function
                                            def download_progress(current, total):
                                                if total > 0:
                                                    percentage = int((current / total) * 100)
                                                    # Log every 10% progress for large files (>10MB)
                                                    if total > 10 * 1024 * 1024 and percentage >= last_percentage[0] + 10:
                                                        last_percentage[0] = percentage
                                                        log.info(f"📥 [{phone_ref}] Downloading: {percentage}% ({format_bytes(current)}/{format_bytes(total)})")

                                            # Download to temp file with progress tracking
                                            if file_size > 10 * 1024 * 1024:  # Log for files > 10MB
                                                log.info(f"📥 [{phone_ref}] Starting download: {format_bytes(file_size)}")

                                            temp_file = await retry_on_timeout(
                                                client.download_media,
                                                msg,
                                                file=tempfile.gettempdir(),
                                                progress_callback=download_progress if file_size > 10 * 1024 * 1024 else None
                                            )

                                            # 8. FILENAME RENAME - Rename file if enabled
                                            if temp_file and modify.get('rename_enabled', False):
                                                rename_pattern = modify.get('rename_pattern', '{original}')
                                                if rename_pattern and rename_pattern != '{original}':
                                                    try:
                                                        import random
                                                        from datetime import datetime

                                                        # Get original filename
                                                        original_name = temp_os.path.basename(temp_file)
                                                        name_parts = temp_os.path.splitext(original_name)
                                                        original_base = name_parts[0]
                                                        extension = name_parts[1]

                                                        # Apply pattern replacements
                                                        new_name = rename_pattern
                                                        new_name = new_name.replace('{original}', original_base)
                                                        new_name = new_name.replace('{date}', datetime.now().strftime('%Y%m%d'))
                                                        new_name = new_name.replace('{time}', datetime.now().strftime('%H%M%S'))
                                                        new_name = new_name.replace('{random}', str(random.randint(1000, 9999)))
                                                        # Note: {counter} would need persistent storage, using random for now
                                                        new_name = new_name.replace('{counter}', str(random.randint(1, 999)))

                                                        # Add extension
                                                        new_filename = new_name + extension

                                                        # Sanitize filename - remove invalid characters for Windows/Unix
                                                        invalid_chars = '<>:"/\\|?*\n\r\t'
                                                        for char in invalid_chars:
                                                            new_filename = new_filename.replace(char, '_')
                                                        # Remove multiple underscores and spaces (pre-compiled pattern)
                                                        new_filename = REGEX_FILENAME_SPACES.sub('_', new_filename)
                                                        new_filename = new_filename.strip('_')

                                                        new_path = temp_os.path.join(temp_os.path.dirname(temp_file), new_filename)

                                                        # Rename file
                                                        temp_os.rename(temp_file, new_path)
                                                        temp_file = new_path
                                                        log.info(f"[{phone_ref}] Renamed: {original_name} -> {new_filename}")
                                                    except Exception as e:
                                                        log.error(f"[{phone_ref}] Rename error: {e}")

                                            # 9. WATERMARK - Apply watermark if enabled
                                            if temp_file and modify.get('watermark_enabled', False):
                                                try:
                                                    # Check if it's an image or video
                                                    is_image = msg.photo or (hasattr(msg, 'document') and msg.document and hasattr(msg.document, 'mime_type') and msg.document.mime_type and msg.document.mime_type.startswith('image/'))
                                                    is_video = msg.video or (hasattr(msg, 'document') and msg.document and hasattr(msg.document, 'mime_type') and msg.document.mime_type and msg.document.mime_type.startswith('video/'))

                                                    if is_image or is_video:
                                                        import os as wm_os
                                                        original_file = temp_file
                                                        # Sanitize basename to prevent path traversal
                                                        safe_basename = temp_os.path.basename(temp_file).replace('..', '')
                                                        watermarked_file = temp_os.path.join(
                                                            temp_os.path.dirname(temp_file),
                                                            'watermarked_' + safe_basename
                                                        )

                                                        # Apply appropriate watermark
                                                        success = False
                                                        media_type = "video" if is_video else "image"

                                                        log.info(f"🎨 [{phone_ref}] Applying {media_type} watermark to: {temp_file}")
                                                        log.info(f"🎨 [{phone_ref}] Watermark config: type={modify.get('watermark_type')}, text={modify.get('watermark_text')[:20] if modify.get('watermark_text') else 'None'}...")

                                                        # Use FFmpeg for watermarking (works for both images and videos)
                                                        log.info(f"🎨 [{phone_ref}] Applying FFmpeg watermark...")
                                                        success = apply_watermark_with_ffmpeg(temp_file, watermarked_file, modify, is_video=is_video)

                                                        log.info(f"🎨 [{phone_ref}] {media_type.capitalize()} watermark result: {'SUCCESS' if success else 'FAILED'}")

                                                        if success and wm_os.path.exists(watermarked_file):
                                                            # Delete original and use watermarked version
                                                            try:
                                                                wm_os.remove(original_file)
                                                            except (OSError, IOError) as e:
                                                                log.warning(f"Failed to cleanup original file {original_file}: {e}")
                                                            temp_file = watermarked_file
                                                            log.info(f"✅ [{phone_ref}] Watermark applied successfully")
                                                        else:
                                                            if not success:
                                                                log.warning(f"⚠️ [{phone_ref}] Watermark failed: watermark function returned False")
                                                            elif not wm_os.path.exists(watermarked_file):
                                                                log.warning(f"⚠️ [{phone_ref}] Watermark failed: output file not created at {watermarked_file}")
                                                            else:
                                                                log.warning(f"⚠️ [{phone_ref}] Watermark failed, sending original")
                                                            # Clean up failed watermarked file if it exists
                                                            try:
                                                                if wm_os.path.exists(watermarked_file):
                                                                    wm_os.remove(watermarked_file)
                                                            except (OSError, IOError) as e:
                                                                log.warning(f"Failed to cleanup watermarked file {watermarked_file}: {e}")

                                                except Exception as e:
                                                    log.error(f"[{phone_ref}] Watermark error: {e}")
                                                    import traceback
                                                    log.error(f"[{phone_ref}] Watermark traceback: {traceback.format_exc()}")
                                                    # Continue with original file if watermark fails

                                            if temp_file is None:
                                                log.error(f"❌ [{phone_ref}] Download failed")
                                                if caption_text:
                                                    await client.send_message(
                                                        dest_entity,
                                                        caption_text,
                                                        formatting_entities=caption_entities if caption_entities else None,
                                                        link_preview=has_web_preview,
                                                        buttons=button_markup
                                                    )
                                                continue
                                            
                                            try:
                                                # Progress callback for uploads
                                                upload_last_percentage = [0]
                                                def upload_progress(current, total):
                                                    if total > 0:
                                                        percentage = int((current / total) * 100)
                                                        # Log every 10% progress for large files (>10MB)
                                                        if total > 10 * 1024 * 1024 and percentage >= upload_last_percentage[0] + 10:
                                                            upload_last_percentage[0] = percentage
                                                            log.info(f"📤 [{phone_ref}] Uploading: {percentage}% ({format_bytes(current)}/{format_bytes(total)})")

                                                # Get file size for upload progress
                                                upload_file_size = 0
                                                if temp_file and temp_os.path.exists(temp_file):
                                                    upload_file_size = temp_os.path.getsize(temp_file)
                                                    if upload_file_size > 10 * 1024 * 1024:  # Log for files > 10MB
                                                        log.info(f"📤 [{phone_ref}] Starting upload: {format_bytes(upload_file_size)}")

                                                # Common send parameters for filtered caption
                                                caption_kwargs = {}
                                                if caption_text:
                                                    caption_kwargs['caption'] = caption_text
                                                    if caption_entities:
                                                        caption_kwargs['formatting_entities'] = caption_entities
                                                # Add buttons if enabled
                                                if button_markup:
                                                    caption_kwargs['buttons'] = button_markup
                                                # Add progress callback for large files
                                                if upload_file_size > 10 * 1024 * 1024:
                                                    caption_kwargs['progress_callback'] = upload_progress
                                                # Apply spoiler effect if enabled
                                                if modify.get('apply_spoiler', False):
                                                    caption_kwargs['spoiler'] = True

                                                # Extract media attributes for format preservation
                                                media_type, media_attrs, media_mime, media_thumb = extract_media_attributes(msg)

                                                # PHOTO with caption
                                                if msg.photo:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        force_document=False,  # Keep as photo
                                                        attributes=media_attrs if media_attrs else None,
                                                        thumb=media_thumb,
                                                        **caption_kwargs
                                                    )
                                                    log.info(f"📷 [{phone_ref}] PHOTO -> {dest}")
                                                
                                                # VIDEO NOTE (round video - no caption)
                                                elif msg.video_note:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        video_note=True,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime
                                                    )
                                                    log.info(f"⭕ [{phone_ref}] VIDEO_NOTE -> {dest}")
                                                
                                                # VOICE MESSAGE
                                                elif msg.voice:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        voice_note=True,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime
                                                    )
                                                    # Voice doesn't support caption, send separately
                                                    if original_text:
                                                        await client.send_message(
                                                            dest_entity,
                                                            original_text,
                                                            formatting_entities=original_entities,
                                                            link_preview=has_web_preview,
                                                            buttons=button_markup
                                                        )
                                                    log.info(f"🎤 [{phone_ref}] VOICE -> {dest}")
                                                
                                                # VIDEO with caption
                                                elif msg.video:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        supports_streaming=True,
                                                        force_document=False,  # Keep as video, not document
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime,
                                                        thumb=media_thumb,
                                                        **caption_kwargs
                                                    )
                                                    log.info(f"🎥 [{phone_ref}] VIDEO -> {dest}")

                                                # GIF / Animation with caption
                                                elif msg.gif:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        force_document=False,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime,
                                                        thumb=media_thumb,
                                                        **caption_kwargs
                                                    )
                                                    log.info(f"🎞️ [{phone_ref}] GIF -> {dest}")

                                                # STICKER (no caption)
                                                elif msg.sticker:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        force_document=False,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime
                                                    )
                                                    log.info(f"🎨 [{phone_ref}] STICKER -> {dest}")
                                                
                                                # AUDIO (music) with caption
                                                elif msg.audio:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        force_document=False,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime,
                                                        thumb=media_thumb,
                                                        **caption_kwargs
                                                    )
                                                    log.info(f"🎵 [{phone_ref}] AUDIO -> {dest}")

                                                # DOCUMENT (file) with caption
                                                elif msg.document:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        force_document=True,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime,
                                                        thumb=media_thumb,
                                                        **caption_kwargs
                                                    )
                                                    log.info(f"📄 [{phone_ref}] DOCUMENT -> {dest}")

                                                # OTHER MEDIA with caption
                                                else:
                                                    await retry_on_timeout(
                                                        client.send_file,
                                                        dest_entity,
                                                        temp_file,
                                                        attributes=media_attrs if media_attrs else None,
                                                        mime_type=media_mime,
                                                        thumb=media_thumb,
                                                        **caption_kwargs
                                                    )
                                                    log.info(f"📎 [{phone_ref}] MEDIA -> {dest}")

                                                success_count += 1

                                                # Mark file as processed to prevent duplicates
                                                if file_unique_id and file_id_for_cache:
                                                    await db_ref.mark_file_processed(
                                                        file_id_for_cache,
                                                        str(file_unique_id),
                                                        rule_id,
                                                        chat_id,
                                                        dest_chat_id,
                                                        upload_file_size,
                                                        file_name_for_cache
                                                    )

                                            finally:
                                                # Clean up temp file
                                                try:
                                                    if temp_file and temp_os.path.exists(temp_file):
                                                        temp_os.remove(temp_file)
                                                except Exception:
                                                    pass
                                    
                                    else:
                                        # TEXT ONLY MESSAGE (no media, may have link preview)
                                        if caption_text:
                                            await client.send_message(
                                                dest_entity,
                                                caption_text,
                                                formatting_entities=caption_entities if caption_entities else None,
                                                link_preview=not remove_link_preview,  # Remove preview if link filter ON
                                                buttons=button_markup
                                            )
                                            success_count += 1
                                            log.info(f"💬 [{phone_ref}] TEXT -> {dest}")
                                
                                except Exception as copy_err:
                                    log.error(f"❌ [{phone_ref}] Copy failed: {copy_err}")
                                    import traceback
                                    traceback.print_exc()
                                
                            else:
                                # FORWARD MODE: Keep original sender + forward header
                                await client.forward_messages(entity=dest_entity, messages=event.message)
                                success_count += 1
                                log.info(f"✅ [{phone_ref}] Forwarded -> {dest}")

                                # Mark file as processed to prevent duplicates
                                if file_unique_id and file_id_for_cache:
                                    await db_ref.mark_file_processed(
                                        file_id_for_cache,
                                        str(file_unique_id),
                                        rule_id,
                                        chat_id,
                                        dest_chat_id,
                                        file_size,
                                        file_name_for_cache
                                    )

                        except Exception as e:
                            log.error(f"❌ [{phone_ref}] {forward_mode} to {dest} failed: {e}")
                    
                    if success_count > 0:
                        await db_ref.increment_forward_count(rule_id)
                        
            except Exception as e:
                log.exception(f"Handler error: {e}")
        
        self.handlers_attached.add(phone)
        log.info(f"✅ Handler attached for {phone}")
    
    async def cleanup(self):
        # Stop album cache cleanup task
        if self._album_cache_started:
            await self.album_cache_manager.stop()
            self._album_cache_started = False
        # Disconnect all clients
        for phone in list(self.clients.keys()):
            await self.disconnect_client(phone)

# ==================== CONNECT STATE ====================
class ConnectState:
    IDLE = "idle"
    WAITING_PHONE = "waiting_phone"
    WAITING_CODE = "waiting_code"
    WAITING_PASSWORD = "waiting_password"
    ADD_RULE_SOURCE = "add_rule_source"
    ADD_RULE_DEST = "add_rule_dest"
    ADD_RULE_MODE = "add_rule_mode"  # Step 3: Forward mode
    ADD_RULE_FILTERS = "add_rule_filters"  # Step 4: Media filters
    ADD_RULE_CLEANER = "add_rule_cleaner"  # Step 5: Caption cleaner
    ADD_RULE_MODIFY = "add_rule_modify"  # Step 6: Modify content
    # Sub-states for modify content input
    MODIFY_RENAME = "modify_rename"
    MODIFY_BLOCK_WORDS = "modify_block_words"
    MODIFY_WHITELIST = "modify_whitelist"
    MODIFY_REPLACE = "modify_replace"
    MODIFY_HEADER = "modify_header"
    MODIFY_FOOTER = "modify_footer"
    MODIFY_BUTTONS = "modify_buttons"
    MODIFY_DELAY = "modify_delay"
    MODIFY_HISTORY = "modify_history"
    MODIFY_WATERMARK = "modify_watermark"
    MODIFY_WATERMARK_TEXT = "modify_watermark_text"
    MODIFY_WATERMARK_LOGO = "modify_watermark_logo"
    MODIFY_WATERMARK_POSITION = "modify_watermark_position"
    MODIFY_WATERMARK_COLOR = "modify_watermark_color"
    MODIFY_WATERMARK_OPACITY = "modify_watermark_opacity"
    MODIFY_WATERMARK_ROTATION = "modify_watermark_rotation"
    MODIFY_WATERMARK_SIZE = "modify_watermark_size"
    MODIFY_CAPTION = "modify_caption"
    # Edit rule states
    EDIT_RULE_SOURCE = "edit_rule_source"
    EDIT_RULE_DEST = "edit_rule_dest"
    
    def __init__(self):
        self.step = self.IDLE
        self.phone: Optional[str] = None
        self.phone_code_hash: Optional[str] = None
        self.sources: List[str] = []
        self.destinations: List[str] = []
        self.forward_mode: str = "forward"  # "forward" or "copy"
        self.filters: Dict[str, bool] = DEFAULT_FILTERS.copy()
        self.modify: Dict = DEFAULT_MODIFY.copy()  # Step 6 settings
        self.edit_rule_id: Optional[int] = None  # For editing existing rule

# ==================== GLOBALS ====================
db: DatabaseManager = None
session_manager: UserSessionManager = None
health_server_instance = None  # Health check HTTP server
connect_states: Dict[int, ConnectState] = {}
connect_locks: Dict[int, Lock] = {}

async def get_connect_lock(user_id: int) -> Lock:
    if user_id not in connect_locks:
        connect_locks[user_id] = Lock()
    return connect_locks[user_id]

# ==================== KEYBOARDS ====================
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Connect Account", callback_data="connect")],
        [InlineKeyboardButton("📋 My Rules", callback_data="rules")],
        [InlineKeyboardButton("📱 My Accounts", callback_data="accounts")],
        [InlineKeyboardButton("➕ Add Rule", callback_data="add_rule")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
    ])

def back_kb(callback: str = "main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=callback)]])

def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])

# ==================== HANDLERS ====================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db.ensure_user(user.id, user.username, user.first_name)
    
    text = (
        f"👋 Welcome, {user.first_name}!\n\n"
        "🤖 *Auto-Forward Bot*\n\n"
        "• Connect your Telegram accounts\n"
        "• Create forwarding rules\n"
        "• Support multiple sources & destinations\n"
        "• Messages forward automatically 24/7\n\n"
        "Choose an option:"
    )
    
    if not TELETHON_AVAILABLE:
        text += "\n\n⚠️ _Telethon not installed_"
    
    await update.message.reply_text(text, reply_markup=main_menu_kb())

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    accounts = await db.get_user_accounts(user.id)
    
    if not accounts:
        await update.message.reply_text("📊 No accounts connected.")
        return
    
    text = "📊 *Status:*\n\n"
    for acc in accounts:
        phone = acc['phone']
        client = session_manager.clients.get(phone)
        status = "🟢" if client and client.is_connected() else "🔴"
        rules = await db.get_user_rules(user.id)
        count = len([r for r in rules if r['phone'] == phone and r['is_enabled']])
        text += f"{status} {phone} ({count} rules)\n"
    
    await update.message.reply_text(text)

async def cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    rules = await db.get_user_rules(user.id)
    
    if not rules:
        await update.message.reply_text("📋 No rules configured.")
        return
    
    text = "📋 *Your Rules:*\n\n"
    for i, r in enumerate(rules, 1):
        status = "✅" if r['is_enabled'] else "⏸️"
        src_count = len(r.get('source_list', []))
        dst_count = len(r.get('dest_list', []))
        text += f"{i}. {status} {src_count} sources → {dst_count} destinations\n"
        text += f"   📱 {r['phone']} | 📨 {r['forward_count']}\n\n"
    
    await update.message.reply_text(text)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """
ℹ️ *Help*

*Commands:*
/start - Menu
/status - Account status  
/rules - Your rules
/help - This help

*How to use:*
1. Connect your Telegram account
2. Add forwarding rules
3. Done! Messages forward automatically

*Multiple Sources/Destinations:*
You can add multiple IDs separated by commas:

Sources: `-1001234567890, -1009876543210, @channel1`
Destinations: `@dest1, -1001111111111, 123456789`

*Format:*
• `@channelname` - public channel
• `-1001234567890` - channel ID

*Get Channel ID:*
Forward message to @userinfobot
    """
    await update.message.reply_text(text)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    data = query.data
    
    log.info(f"Callback from {user.id}: {data}")
    
    try:
        await query.answer()
    except Exception:
        pass
    
    await db.ensure_user(user.id, user.username, user.first_name)
    
    try:
        if data == "main":
            await show_main_menu(query)
        elif data == "cancel":
            connect_states.pop(user.id, None)
            await query.edit_message_text("❌ Cancelled.", reply_markup=main_menu_kb())
        elif data == "connect":
            await start_connect_flow(query, user)
        elif data == "accounts":
            await show_accounts(query, user)
        elif data == "rules":
            await show_rules(query, user)
        elif data == "add_rule":
            await start_add_rule(query, user)
        elif data == "help":
            await show_help(query)
        elif data.startswith("acc_"):
            await handle_account_callback(query, user, data)
        elif data.startswith("rule_"):
            await handle_rule_callback(query, user, data)
        elif data.startswith("selphone_"):
            phone = data.replace("selphone_", "")
            await start_add_rule_for_phone(query, user, phone)
        elif data.startswith("mode_"):
            await handle_mode_selection(query, user, data)
        elif data.startswith("filter_"):
            await handle_filter_toggle(query, user, data)
        elif data == "filters_done":
            await handle_filters_done(query, user)
        elif data == "filters_all_on":
            await handle_filters_all(query, user, True)
        elif data == "filters_all_off":
            await handle_filters_all(query, user, False)
        elif data == "filters_back":
            await handle_filters_back(query, user)
        elif data == "goto_cleaner":
            await handle_goto_cleaner(query, user)
        elif data == "cleaner_done":
            await handle_cleaner_done(query, user)
        elif data == "cleaner_all_on":
            await handle_cleaner_all(query, user, True)
        elif data == "cleaner_all_off":
            await handle_cleaner_all(query, user, False)
        elif data == "cleaner_back":
            await handle_cleaner_back(query, user)
        # Step 6: Modify content callbacks
        elif data == "goto_modify":
            await handle_goto_modify(query, user)
        elif data == "modify_done":
            await handle_modify_done(query, user)
        elif data == "modify_back":
            await handle_modify_back(query, user)
        elif data == "modify_back_to_main":
            await handle_modify_back_to_main(query, user)
        elif data == "modify_rename":
            await handle_modify_rename(query, user)
        elif data == "modify_block":
            await handle_modify_block(query, user)
        elif data == "modify_whitelist":
            await handle_modify_whitelist(query, user)
        elif data == "modify_replace":
            await handle_modify_replace(query, user)
        elif data == "modify_header":
            await handle_modify_header(query, user)
        elif data == "modify_footer":
            await handle_modify_footer(query, user)
        elif data == "modify_caption":
            await handle_modify_caption(query, user)
        elif data == "modify_spoiler":
            await handle_modify_spoiler(query, user)
        elif data == "modify_buttons":
            await handle_modify_buttons(query, user)
        elif data == "modify_delay":
            await handle_modify_delay(query, user)
        elif data == "modify_history":
            await handle_modify_history(query, user)
        elif data == "modify_watermark":
            await handle_modify_watermark(query, user)
        elif data.startswith("watermark_type_"):
            await handle_watermark_type(query, user, data)
        elif data.startswith("watermark_pos_"):
            await handle_watermark_position(query, user, data)
        elif data.startswith("watermark_color_"):
            await handle_watermark_color(query, user, data)
        elif data.startswith("watermark_opacity_"):
            await handle_watermark_opacity(query, user, data)
        elif data.startswith("watermark_rotation_"):
            await handle_watermark_rotation(query, user, data)
        elif data.startswith("watermark_size_"):
            await handle_watermark_size(query, user, data)
        elif data == "watermark_preview":
            await handle_watermark_preview(query, user)
        elif data == "watermark_text_input":
            await handle_watermark_text_input(query, user)
        elif data == "watermark_logo_input":
            await handle_watermark_logo_input(query, user)
        elif data.startswith("toggle_"):
            await handle_modify_toggle(query, user, data)
        elif data.startswith("delay_"):
            await handle_delay_set(query, user, data)
        elif data.startswith("history_"):
            await handle_history_set(query, user, data)
        elif data.startswith("clear_"):
            await handle_clear_option(query, user, data)
        elif data == "noop":
            # Do nothing - just a label button
            await query.answer()
        else:
            log.warning(f"Unknown callback: {data}")
            
    except BadRequest as e:
        log.error(f"BadRequest: {e}")
    except Exception as e:
        log.exception(f"Callback error: {e}")
        try:
            await query.edit_message_text(f"❌ Error: {e}", reply_markup=main_menu_kb())
        except Exception:
            pass

async def handle_mode_selection(query, user, data: str):
    """Handle forward mode selection (Step 3) -> Go to Step 4 (Filters)."""
    # Format: mode_forward or mode_copy
    mode = data.replace("mode_", "")
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODE:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.forward_mode = mode
        state.step = ConnectState.ADD_RULE_FILTERS
        
        # Show filters selection (Step 4)
        await show_filters_keyboard(query, user, state)

def build_filters_keyboard(filters: dict) -> InlineKeyboardMarkup:
    """Build inline keyboard for filter toggles (Step 4)."""
    
    # All ignore filters with their descriptions
    ignore_filters = [
        ('document', '📄', 'Document'),
        ('video', '🎥', 'Video'),
        ('audio', '🎵', 'Audio'),
        ('sticker', '🎨', 'Sticker'),
        ('text', '💬', 'Text'),
        ('photo', '📷', 'Photo'),
        ('photo_only', '🖼️', 'Photo Only'),
        ('photo_with_text', '📝', 'Photo+Text'),
        ('album', '📚', 'Album'),
        ('poll', '📊', 'Poll'),
        ('voice', '🎤', 'Voice'),
        ('video_note', '⭕', 'Video Note'),
        ('gif', '🎞️', 'GIF'),
        ('emoji', '😀', 'Emoji'),
        ('forward', '↩️', 'Forwards'),
        ('reply', '💬', 'Reply'),
        ('button', '🔘', 'Button'),
    ]
    
    buttons = []
    
    # Header
    buttons.append([InlineKeyboardButton("━━━ 🚫 IGNORE FILTERS ━━━", callback_data="noop")])
    
    # Filter buttons (3 per row)
    row = []
    for key, icon, label in ignore_filters:
        is_on = filters.get(key, False)
        status = "✅" if is_on else "⬜"
        btn_text = f"{status} {icon}"
        row.append(InlineKeyboardButton(btn_text, callback_data=f"filter_{key}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    # Legend
    buttons.append([InlineKeyboardButton("✅ = Ignore | ⬜ = Keep", callback_data="noop")])
    
    # Control buttons
    buttons.append([
        InlineKeyboardButton("✅ All ON", callback_data="filters_all_on"),
        InlineKeyboardButton("⬜ All OFF", callback_data="filters_all_off")
    ])
    
    # Navigation
    buttons.append([
        InlineKeyboardButton("✂️ Cleaner", callback_data="goto_cleaner"),
        InlineKeyboardButton("✅ Done", callback_data="filters_done")
    ])
    buttons.append([
        InlineKeyboardButton("🔙 Back", callback_data="filters_back"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel")
    ])
    
    return InlineKeyboardMarkup(buttons)


def build_cleaner_keyboard(filters: dict) -> InlineKeyboardMarkup:
    """Build inline keyboard for caption cleaner options (Step 5)."""
    
    buttons = []
    
    # Header
    buttons.append([InlineKeyboardButton("━━━ ✂️ CAPTION CLEANER ━━━", callback_data="noop")])
    
    # Remove all captions - useful for forwarding competitor content without their branding
    caption_status = "✅" if filters.get('clean_caption', False) else "⬜"
    buttons.append([InlineKeyboardButton(f"{caption_status} 🚫 Remove All Captions", callback_data="filter_clean_caption")])

    # Explanation of how Remove All Captions works
    buttons.append([InlineKeyboardButton("💡 Auto uses Copy Mode to remove captions", callback_data="noop")])
    buttons.append([InlineKeyboardButton("Works for: 📷 Photos 🎥 Videos 📄 Docs", callback_data="noop")])

    buttons.append([InlineKeyboardButton("─ Or remove specific items: ─", callback_data="noop")])
    
    cleaner_options = [
        ('clean_hashtag', '#️⃣', 'Hashtags'),
        ('clean_mention', '@', 'Mentions'),
        ('clean_link', '🔗', 'Links'),
        ('clean_emoji', '😀', 'Emojis'),
        ('clean_phone', '📞', 'Phones'),
        ('clean_email', '📧', 'Emails'),
    ]
    
    # Cleaner buttons (2 per row)
    row = []
    for key, icon, label in cleaner_options:
        is_on = filters.get(key, False)
        status = "✅" if is_on else "⬜"
        btn_text = f"{status} {icon} {label}"
        row.append(InlineKeyboardButton(btn_text, callback_data=f"filter_{key}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    # Legend
    buttons.append([InlineKeyboardButton("✅ = Remove | ⬜ = Keep", callback_data="noop")])
    
    # Control buttons
    buttons.append([
        InlineKeyboardButton("✅ All ON", callback_data="cleaner_all_on"),
        InlineKeyboardButton("⬜ All OFF", callback_data="cleaner_all_off")
    ])
    
    # Navigation - Go to Step 6 (Modify)
    buttons.append([
        InlineKeyboardButton("🔙 Back", callback_data="cleaner_back"),
        InlineKeyboardButton("✏️ Modify", callback_data="goto_modify")
    ])
    buttons.append([
        InlineKeyboardButton("✅ Done", callback_data="cleaner_done"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel")
    ])
    
    return InlineKeyboardMarkup(buttons)


def build_modify_keyboard(modify: dict) -> InlineKeyboardMarkup:
    """Build inline keyboard for modify content options (Step 6)."""
    buttons = []
    
    # Header
    buttons.append([InlineKeyboardButton("━━━ ✏️ MODIFY CONTENT ━━━", callback_data="noop")])
    
    # Rename
    rename_status = "✅" if modify.get('rename_enabled') else "⬜"
    rename_pattern = modify.get('rename_pattern', '{original}')
    buttons.append([InlineKeyboardButton(f"{rename_status} 📝 Rename Files: {rename_pattern[:15]}", callback_data="modify_rename")])
    
    # Block Words
    block_status = "✅" if modify.get('block_words_enabled') else "⬜"
    block_count = len(modify.get('block_words', []))
    buttons.append([InlineKeyboardButton(f"{block_status} 🚫 Block Words ({block_count})", callback_data="modify_block")])
    
    # Whitelist
    whitelist_status = "✅" if modify.get('whitelist_enabled') else "⬜"
    whitelist_count = len(modify.get('whitelist_words', []))
    buttons.append([InlineKeyboardButton(f"{whitelist_status} ✅ Whitelist ({whitelist_count})", callback_data="modify_whitelist")])
    
    # Replace Words
    replace_status = "✅" if modify.get('replace_enabled') else "⬜"
    replace_count = len(modify.get('replace_pairs', []))
    buttons.append([InlineKeyboardButton(f"{replace_status} 🔄 Replace Words ({replace_count})", callback_data="modify_replace")])
    
    # Header/Footer
    header_status = "✅" if modify.get('header_enabled') else "⬜"
    footer_status = "✅" if modify.get('footer_enabled') else "⬜"
    buttons.append([
        InlineKeyboardButton(f"{header_status} 📌 Header", callback_data="modify_header"),
        InlineKeyboardButton(f"{footer_status} 📎 Footer", callback_data="modify_footer")
    ])
    
    # Link Buttons
    buttons_status = "✅" if modify.get('buttons_enabled') else "⬜"
    buttons_count = len(modify.get('buttons', []))
    buttons.append([InlineKeyboardButton(f"{buttons_status} 🔘 Link Buttons ({buttons_count})", callback_data="modify_buttons")])
    
    # Delay
    delay_status = "✅" if modify.get('delay_enabled') else "⬜"
    delay_sec = modify.get('delay_seconds', 0)
    buttons.append([InlineKeyboardButton(f"{delay_status} ⏱️ Delay ({delay_sec}s)", callback_data="modify_delay")])
    
    # History
    history_status = "✅" if modify.get('history_enabled') else "⬜"
    history_count = modify.get('history_count', 0)
    buttons.append([InlineKeyboardButton(f"{history_status} 📜 History ({history_count} msgs)", callback_data="modify_history")])

    # Watermark
    watermark_status = "✅" if modify.get('watermark_enabled') else "⬜"
    watermark_type = modify.get('watermark_type', 'text')
    watermark_icon = "📝" if watermark_type == 'text' else "🖼"
    buttons.append([InlineKeyboardButton(f"{watermark_status} {watermark_icon} Watermark", callback_data="modify_watermark")])

    # Caption
    caption_status = "✅" if modify.get('caption_enabled') else "⬜"
    caption_text = modify.get('caption_text', '')
    caption_preview = caption_text[:20] + '...' if len(caption_text) > 20 else caption_text
    caption_display = caption_preview if caption_text else 'Not set'
    buttons.append([InlineKeyboardButton(f"{caption_status} 💬 Caption: {caption_display}", callback_data="modify_caption")])

    # Spoiler
    spoiler_status = "✅" if modify.get('apply_spoiler') else "⬜"
    buttons.append([InlineKeyboardButton(f"{spoiler_status} 🙈 Spoiler Effect", callback_data="modify_spoiler")])

    # Navigation
    buttons.append([
        InlineKeyboardButton("🔙 Back", callback_data="modify_back"),
        InlineKeyboardButton("✅ Done", callback_data="modify_done")
    ])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])

    return InlineKeyboardMarkup(buttons)


async def show_modify_keyboard(query, user, state):
    """Show the modify content keyboard (Step 6)."""
    mode_text = "📤 Forward" if state.forward_mode == "forward" else "📋 Copy"
    
    # Count active modifications
    active_mods = sum([
        state.modify.get('rename_enabled', False),
        state.modify.get('block_words_enabled', False),
        state.modify.get('whitelist_enabled', False),
        state.modify.get('replace_enabled', False),
        state.modify.get('header_enabled', False),
        state.modify.get('footer_enabled', False),
        state.modify.get('buttons_enabled', False),
        state.modify.get('delay_enabled', False),
        state.modify.get('history_enabled', False),
        state.modify.get('watermark_enabled', False),
        state.modify.get('caption_enabled', False),
        state.modify.get('apply_spoiler', False),
    ])
    
    text = (
        f"*Step 6: Modify Content*\n\n"
        f"📱 Phone: `{state.phone}`\n"
        f"⚙️ Mode: {mode_text}\n"
        f"✏️ Modifications: {active_mods} active\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Configure content modifications:\n\n"
        f"Tap an option to configure:"
    )
    
    await query.edit_message_text(
        text,
        reply_markup=build_modify_keyboard(state.modify),
        parse_mode='Markdown'
    )


async def show_filters_keyboard(query, user, state):
    """Show the filters selection keyboard (Step 4)."""
    mode_text = "📤 Forward" if state.forward_mode == "forward" else "📋 Copy"
    
    # Count active filters
    ignore_keys = ['document', 'video', 'audio', 'sticker', 'text', 'photo',
                   'photo_only', 'photo_with_text', 'album', 'poll', 'voice',
                   'video_note', 'gif', 'emoji', 'forward', 'reply', 'button']
    active_ignores = sum(1 for k in ignore_keys if state.filters.get(k, False))
    
    text = (
        f"*Step 4: Ignore Filters*\n\n"
        f"📱 Phone: `{state.phone}`\n"
        f"📥 Sources: {len(state.sources)}\n"
        f"📤 Destinations: {len(state.destinations)}\n"
        f"⚙️ Mode: {mode_text}\n"
        f"🚫 Ignoring: {active_ignores} types\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Select message types to *IGNORE*:\n\n"
        f"✅ = Will be IGNORED (not forwarded)\n"
        f"⬜ = Will be FORWARDED\n\n"
        f"Tap to toggle:"
    )
    
    await query.edit_message_text(
        text,
        reply_markup=build_filters_keyboard(state.filters),
        parse_mode='Markdown'
    )


async def show_cleaner_keyboard(query, user, state):
    """Show the caption cleaner keyboard (Step 5)."""
    mode_text = "📤 Forward" if state.forward_mode == "forward" else "📋 Copy"
    
    # Count active cleaners
    cleaner_keys = ['clean_hashtag', 'clean_mention', 'clean_link', 
                    'clean_emoji', 'clean_phone', 'clean_email']
    active_cleaners = sum(1 for k in cleaner_keys if state.filters.get(k, False))
    
    text = (
        f"*Step 5: Caption Cleaner*\n\n"
        f"📱 Phone: `{state.phone}`\n"
        f"⚙️ Mode: {mode_text}\n"
        f"✂️ Cleaning: {active_cleaners} types\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Select what to *REMOVE* from captions:\n\n"
        f"✅ = Will be REMOVED from text\n"
        f"⬜ = Will be KEPT in text\n\n"
        f"Tap to toggle:"
    )
    
    await query.edit_message_text(
        text,
        reply_markup=build_cleaner_keyboard(state.filters)
    )

async def handle_filter_toggle(query, user, data: str):
    """Toggle a single filter on/off."""
    # Format: filter_text, filter_photo, filter_clean_hashtag, etc.
    filter_key = data.replace("filter_", "")
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step not in [ConnectState.ADD_RULE_FILTERS, ConnectState.ADD_RULE_CLEANER]:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        # Toggle the filter
        current = state.filters.get(filter_key, False)
        state.filters[filter_key] = not current
        
        # Update appropriate keyboard
        if state.step == ConnectState.ADD_RULE_CLEANER:
            await show_cleaner_keyboard(query, user, state)
        else:
            await show_filters_keyboard(query, user, state)

async def handle_filters_all(query, user, turn_on: bool):
    """Turn all ignore filters on or off."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_FILTERS:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        # Only set ignore filters (not cleaner filters)
        ignore_keys = ['document', 'video', 'audio', 'sticker', 'text', 'photo', 
                       'photo_only', 'photo_with_text', 'album', 'poll', 'voice', 
                       'video_note', 'gif', 'emoji', 'forward', 'reply', 'link', 'button']
        for key in ignore_keys:
            state.filters[key] = turn_on
        
        # Update keyboard
        await show_filters_keyboard(query, user, state)

async def handle_cleaner_all(query, user, turn_on: bool):
    """Turn all cleaner filters on or off."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_CLEANER:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        # Only set cleaner filters (excluding clean_caption which should be toggled separately)
        cleaner_keys = ['clean_hashtag', 'clean_mention', 'clean_link', 
                        'clean_emoji', 'clean_phone', 'clean_email']
        for key in cleaner_keys:
            state.filters[key] = turn_on
        
        # Update keyboard
        await show_cleaner_keyboard(query, user, state)

async def handle_goto_cleaner(query, user):
    """Go to cleaner step (Step 5)."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_FILTERS:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.ADD_RULE_CLEANER
        await show_cleaner_keyboard(query, user, state)

async def handle_cleaner_back(query, user):
    """Go back from cleaner to filters (Step 4)."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_CLEANER:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.ADD_RULE_FILTERS
        await show_filters_keyboard(query, user, state)

# ==================== STEP 6: MODIFY CONTENT HANDLERS ====================

async def handle_goto_modify(query, user):
    """Go to modify content step (Step 6)."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step not in [ConnectState.ADD_RULE_CLEANER, ConnectState.ADD_RULE_FILTERS]:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.ADD_RULE_MODIFY
        await show_modify_keyboard(query, user, state)

async def handle_modify_back(query, user):
    """Go back from modify to cleaner (Step 5)."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step not in [ConnectState.ADD_RULE_MODIFY, ConnectState.MODIFY_RENAME,
                                           ConnectState.MODIFY_BLOCK_WORDS, ConnectState.MODIFY_WHITELIST,
                                           ConnectState.MODIFY_REPLACE, ConnectState.MODIFY_HEADER,
                                           ConnectState.MODIFY_FOOTER, ConnectState.MODIFY_CAPTION,
                                           ConnectState.MODIFY_BUTTONS,
                                           ConnectState.MODIFY_DELAY, ConnectState.MODIFY_HISTORY,
                                           ConnectState.MODIFY_WATERMARK, ConnectState.MODIFY_WATERMARK_TEXT,
                                           ConnectState.MODIFY_WATERMARK_LOGO]:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.step = ConnectState.ADD_RULE_CLEANER
        await show_cleaner_keyboard(query, user, state)

async def handle_modify_done(query, user):
    """Finish from modify and create the rule."""
    await finalize_rule_creation(query, user)

async def handle_modify_rename(query, user):
    """Configure file rename pattern."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_RENAME
        current = state.modify.get('rename_pattern', '{original}')
        enabled = state.modify.get('rename_enabled', False)
        
        await query.edit_message_text(
            f"*📝 Rename Files*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Pattern: `{current}`\n\n"
            f"*Available patterns:*\n"
            f"• `{{original}}` - Original filename\n"
            f"• `{{date}}` - Current date (YYYY-MM-DD)\n"
            f"• `{{time}}` - Current time (HH-MM-SS)\n"
            f"• `{{random}}` - Random 6 characters\n"
            f"• `{{counter}}` - Incrementing number\n\n"
            f"*Example:*\n"
            f"`{{date}}_{{original}}` → `2024-01-15_photo.jpg`\n\n"
            f"Send new pattern or tap toggle:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_rename")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_block(query, user):
    """Configure block words."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_BLOCK_WORDS
        words = state.modify.get('block_words', [])
        enabled = state.modify.get('block_words_enabled', False)
        words_str = ', '.join(words[:10]) if words else 'None'
        
        await query.edit_message_text(
            f"*🚫 Block Words*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Words: {len(words)}\n"
            f"Current: `{words_str}`\n\n"
            f"Messages containing these words will be *SKIPPED*.\n\n"
            f"*Send words to block:*\n"
            f"One per line or comma-separated:\n"
            f"`spam, advertisement, promo`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_block")],
                [InlineKeyboardButton("🗑️ Clear All", callback_data="clear_block")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_whitelist(query, user):
    """Configure whitelist keywords."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_WHITELIST
        words = state.modify.get('whitelist_words', [])
        enabled = state.modify.get('whitelist_enabled', False)
        words_str = ', '.join(words[:10]) if words else 'None'
        
        await query.edit_message_text(
            f"*✅ Whitelist Keywords*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Words: {len(words)}\n"
            f"Current: `{words_str}`\n\n"
            f"Only messages containing these words will be forwarded.\n"
            f"Messages WITHOUT these words will be *SKIPPED*.\n\n"
            f"*Send keywords:*\n"
            f"One per line or comma-separated:\n"
            f"`crypto, bitcoin, trading`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_whitelist")],
                [InlineKeyboardButton("🗑️ Clear All", callback_data="clear_whitelist")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_replace(query, user):
    """Configure word replacement."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_REPLACE
        pairs = state.modify.get('replace_pairs', [])
        enabled = state.modify.get('replace_enabled', False)
        pairs_str = '\n'.join([f"• `{p['from']}` → `{p['to']}`" for p in pairs[:5]]) if pairs else 'None'
        
        await query.edit_message_text(
            f"*🔄 Replace Words*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Pairs: {len(pairs)}\n\n"
            f"Current:\n{pairs_str}\n\n"
            f"*Format:*\n"
            f"`old_word -> new_word`\n"
            f"or `old_word => new_word`\n\n"
            f"*Example:*\n"
            f"`@oldchannel -> @newchannel`\n"
            f"`http://old.com -> http://new.com`\n\n"
            f"Send replacement pairs (one per line):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_replace")],
                [InlineKeyboardButton("🗑️ Clear All", callback_data="clear_replace")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_header(query, user):
    """Configure header text."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_HEADER
        header = state.modify.get('header_text', '')
        enabled = state.modify.get('header_enabled', False)
        
        await query.edit_message_text(
            f"*📌 Add Header*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Current: `{header[:50] if header else 'None'}`\n\n"
            f"This text will be added at the *BEGINNING* of every message.\n\n"
            f"*Supports formatting:*\n"
            f"• `**bold**` for bold\n"
            f"• `__italic__` for italic\n"
            f"• `{{newline}}` for line break\n\n"
            f"*Example:*\n"
            f"`📢 **ANNOUNCEMENT** {{newline}}`\n\n"
            f"Send header text:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_header")],
                [InlineKeyboardButton("🗑️ Clear", callback_data="clear_header")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_footer(query, user):
    """Configure footer text."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_FOOTER
        footer = state.modify.get('footer_text', '')
        enabled = state.modify.get('footer_enabled', False)
        
        await query.edit_message_text(
            f"*📎 Add Footer*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Current: `{footer[:50] if footer else 'None'}`\n\n"
            f"This text will be added at the *END* of every message.\n\n"
            f"*Supports formatting:*\n"
            f"• `**bold**` for bold\n"
            f"• `__italic__` for italic\n"
            f"• `{{newline}}` for line break\n\n"
            f"*Example:*\n"
            f"`{{newline}}━━━━━━{{newline}}📢 @YourChannel`\n\n"
            f"Send footer text:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_footer")],
                [InlineKeyboardButton("🗑️ Clear", callback_data="clear_footer")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_caption(query, user):
    """Configure caption for media."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.step = ConnectState.MODIFY_CAPTION
        caption = state.modify.get('caption_text', '')
        enabled = state.modify.get('caption_enabled', False)

        await query.edit_message_text(
            f"*💬 Custom Caption*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Current: `{caption[:100] if caption else 'None'}`\n\n"
            f"This caption will *REPLACE* the original caption when forwarding media\\.\n\n"
            f"*All Telegram formatting supported:*\n"
            f"• `**bold**` → **bold**\n"
            f"• `__underline__` → underline\n"
            f"• `*italic*` or `_italic_` → _italic_\n"
            f"• `~~strike~~` → strikethrough\n"
            f"• `||spoiler||` → hidden/spoiler\n"
            f"• `` `code` `` → `monospace`\n"
            f"• `` ```code block``` `` → multi\\-line code\n"
            f"• `[text](url)` → clickable link\n"
            f"• `{{newline}}` → line break\n\n"
            f"*Example:*\n"
            f"```\n"
            f"**New Post** {{newline}}\n"
            f"Check _latest_ update with __important__ info\\!\n"
            f"~~Old price~~ ||New price|| available\\.\n"
            f"Use `code` or visit [Site](https://example\\.com)\n"
            f"```\n\n"
            f"Send your formatted caption:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_caption")],
                [InlineKeyboardButton("🗑️ Clear", callback_data="clear_caption")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='MarkdownV2'
        )

async def handle_modify_spoiler(query, user):
    """Configure spoiler effect for photos and videos."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        enabled = state.modify.get('apply_spoiler', False)

        await query.edit_message_text(
            f"*🙈 Spoiler Effect*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n\n"
            f"When enabled, applies a blur/shimmer effect to photos and videos\\.\n\n"
            f"⚠️ *Important:*\n"
            f"• Works with photos and videos only\n"
            f"• Recipient sees media with blur effect\n"
            f"• Tap to reveal the media\n"
            f"• Only works in Copy mode\n\n"
            f"Toggle the spoiler effect:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_spoiler")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='MarkdownV2'
        )

async def handle_modify_buttons(query, user):
    """Configure link buttons."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_BUTTONS
        buttons_list = state.modify.get('buttons', [])
        enabled = state.modify.get('buttons_enabled', False)
        
        buttons_str = ''
        for row in buttons_list[:3]:
            row_str = ' && '.join([f"{b['text']} - {b['url']}" for b in row])
            buttons_str += f"• {row_str}\n"
        if not buttons_str:
            buttons_str = 'None'
        
        await query.edit_message_text(
            f"*🔘 Link Buttons*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Rows: {len(buttons_list)}\n\n"
            f"Current:\n{buttons_str}\n\n"
            f"*Format:*\n"
            f"• Single button:\n"
            f"  `Button Text - https://link.com`\n\n"
            f"• Multiple buttons in one row:\n"
            f"  `Btn1 - url1 && Btn2 - url2`\n\n"
            f"• Multiple rows:\n"
            f"  `Row1 Btn - url1`\n"
            f"  `Row2 Btn - url2`\n\n"
            f"*Example:*\n"
            f"`📢 Join - https://t.me/channel && 💬 Chat - https://t.me/chat`\n\n"
            f"Send button configuration:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_buttons")],
                [InlineKeyboardButton("🗑️ Clear All", callback_data="clear_buttons")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_delay(query, user):
    """Configure delay."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_DELAY
        delay = state.modify.get('delay_seconds', 0)
        enabled = state.modify.get('delay_enabled', False)
        
        await query.edit_message_text(
            f"*⏱️ Delay*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Current: `{delay} seconds`\n\n"
            f"Messages will be delayed before forwarding.\n\n"
            f"*Quick options:*",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("5s", callback_data="delay_5"),
                    InlineKeyboardButton("30s", callback_data="delay_30"),
                    InlineKeyboardButton("1m", callback_data="delay_60"),
                    InlineKeyboardButton("5m", callback_data="delay_300")
                ],
                [
                    InlineKeyboardButton("10m", callback_data="delay_600"),
                    InlineKeyboardButton("30m", callback_data="delay_1800"),
                    InlineKeyboardButton("1h", callback_data="delay_3600"),
                    InlineKeyboardButton("Off", callback_data="delay_0")
                ],
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_delay")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_history(query, user):
    """Configure history forwarding."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_MODIFY:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.MODIFY_HISTORY
        count = state.modify.get('history_count', 0)
        enabled = state.modify.get('history_enabled', False)
        
        await query.edit_message_text(
            f"*📜 History*\n\n"
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n"
            f"Messages: `{count}`\n\n"
            f"Forward past messages when rule is created.\n"
            f"This will send the last N messages from source.\n\n"
            f"*Quick options:*",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("10", callback_data="history_10"),
                    InlineKeyboardButton("50", callback_data="history_50"),
                    InlineKeyboardButton("100", callback_data="history_100"),
                    InlineKeyboardButton("500", callback_data="history_500")
                ],
                [
                    InlineKeyboardButton("1000", callback_data="history_1000"),
                    InlineKeyboardButton("Off", callback_data="history_0")
                ],
                [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_history")],
                [InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]
            ]),
            parse_mode='Markdown'
        )

async def handle_modify_watermark(query, user):
    """Configure watermark settings - main menu."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step not in [ConnectState.ADD_RULE_MODIFY, ConnectState.MODIFY_WATERMARK,
                                            ConnectState.MODIFY_WATERMARK_TEXT, ConnectState.MODIFY_WATERMARK_LOGO]:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.step = ConnectState.MODIFY_WATERMARK
        enabled = state.modify.get('watermark_enabled', False)
        watermark_type = state.modify.get('watermark_type', 'text')
        watermark_text = state.modify.get('watermark_text', '')
        position = state.modify.get('watermark_position', 'bottom-right')
        opacity = state.modify.get('watermark_opacity', 50)
        rotation = state.modify.get('watermark_rotation', 0)
        size = state.modify.get('watermark_size', 10)

        # Build status text
        type_icon = "📝" if watermark_type == 'text' else "🖼"
        status_lines = [
            f"*{type_icon} Watermark Configuration*\n",
            f"Status: {'✅ Enabled' if enabled else '⬜ Disabled'}\n",
            f"Type: {type_icon} {'Text' if watermark_type == 'text' else 'Logo'}\n",
        ]

        if watermark_type == 'text' and watermark_text:
            text_preview = watermark_text[:30] + "..." if len(watermark_text) > 30 else watermark_text
            color = state.modify.get('watermark_text_color', 'white')
            status_lines.append(f"Text: `{text_preview}`\n")
            status_lines.append(f"Color: {color}\n")
        elif watermark_type == 'logo':
            has_logo = state.modify.get('watermark_logo_file_id') or state.modify.get('watermark_logo_path')
            status_lines.append(f"Logo: {'✅ Uploaded' if has_logo else '⬜ Not set'}\n")

        status_lines.extend([
            f"Position: {position}\n",
            f"Opacity: {opacity}%\n",
            f"Rotation: {rotation}°\n",
            f"Size: {size}%\n",
        ])

        buttons = []

        # Type selection
        buttons.append([InlineKeyboardButton("━━━ TYPE ━━━", callback_data="noop")])
        buttons.append([
            InlineKeyboardButton(f"{'✅' if watermark_type == 'text' else '⬜'} 📝 Text", callback_data="watermark_type_text"),
            InlineKeyboardButton(f"{'✅' if watermark_type == 'logo' else '⬜'} 🖼 Logo", callback_data="watermark_type_logo")
        ])

        # Content configuration
        if watermark_type == 'text':
            buttons.append([InlineKeyboardButton(f"📝 Set Text: {watermark_text[:10] if watermark_text else 'None'}...", callback_data="watermark_text_input")])
            # Color selection
            color = state.modify.get('watermark_text_color', 'white')
            buttons.append([InlineKeyboardButton("━━━ COLOR ━━━", callback_data="noop")])
            buttons.append([
                InlineKeyboardButton(f"{'✅' if color == 'white' else '⬜'} ⚪", callback_data="watermark_color_white"),
                InlineKeyboardButton(f"{'✅' if color == 'black' else '⬜'} ⚫", callback_data="watermark_color_black"),
                InlineKeyboardButton(f"{'✅' if color == 'blue' else '⬜'} 🔵", callback_data="watermark_color_blue"),
                InlineKeyboardButton(f"{'✅' if color == 'red' else '⬜'} 🔴", callback_data="watermark_color_red")
            ])
        else:
            buttons.append([InlineKeyboardButton("🖼 Upload Logo", callback_data="watermark_logo_input")])

        # Position selection
        buttons.append([InlineKeyboardButton("━━━ POSITION ━━━", callback_data="noop")])
        buttons.append([
            InlineKeyboardButton("↖" if position == "top-left" else "⬜", callback_data="watermark_pos_top-left"),
            InlineKeyboardButton("⬆" if position == "top" else "⬜", callback_data="watermark_pos_top"),
            InlineKeyboardButton("↗" if position == "top-right" else "⬜", callback_data="watermark_pos_top-right")
        ])
        buttons.append([
            InlineKeyboardButton("⬅" if position == "left" else "⬜", callback_data="watermark_pos_left"),
            InlineKeyboardButton("🎯" if position == "center" else "⬜", callback_data="watermark_pos_center"),
            InlineKeyboardButton("➡" if position == "right" else "⬜", callback_data="watermark_pos_right")
        ])
        buttons.append([
            InlineKeyboardButton("↙" if position == "bottom-left" else "⬜", callback_data="watermark_pos_bottom-left"),
            InlineKeyboardButton("⬇" if position == "bottom" else "⬜", callback_data="watermark_pos_bottom"),
            InlineKeyboardButton("↘" if position == "bottom-right" else "⬜", callback_data="watermark_pos_bottom-right")
        ])

        # Opacity selection
        buttons.append([InlineKeyboardButton(f"━━━ OPACITY: {opacity}% ━━━", callback_data="noop")])
        buttons.append([
            InlineKeyboardButton("10%", callback_data="watermark_opacity_10"),
            InlineKeyboardButton("25%", callback_data="watermark_opacity_25"),
            InlineKeyboardButton("50%", callback_data="watermark_opacity_50"),
            InlineKeyboardButton("75%", callback_data="watermark_opacity_75"),
            InlineKeyboardButton("100%", callback_data="watermark_opacity_100")
        ])

        # Rotation selection
        buttons.append([InlineKeyboardButton(f"━━━ ROTATION: {rotation}° ━━━", callback_data="noop")])
        buttons.append([
            InlineKeyboardButton("0°", callback_data="watermark_rotation_0"),
            InlineKeyboardButton("45°", callback_data="watermark_rotation_45"),
            InlineKeyboardButton("90°", callback_data="watermark_rotation_90"),
            InlineKeyboardButton("180°", callback_data="watermark_rotation_180")
        ])

        # Size selection
        buttons.append([InlineKeyboardButton(f"━━━ SIZE: {size}% ━━━", callback_data="noop")])
        buttons.append([
            InlineKeyboardButton("10%", callback_data="watermark_size_10"),
            InlineKeyboardButton("20%", callback_data="watermark_size_20"),
            InlineKeyboardButton("30%", callback_data="watermark_size_30"),
            InlineKeyboardButton("50%", callback_data="watermark_size_50")
        ])

        # Preview and control buttons
        buttons.append([InlineKeyboardButton("👁 Preview", callback_data="watermark_preview")])
        buttons.append([InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data="toggle_watermark")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")])

        await query.edit_message_text(
            ''.join(status_lines),
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode='Markdown'
        )

async def handle_watermark_type(query, user, data: str):
    """Handle watermark type selection."""
    watermark_type = data.replace("watermark_type_", "")
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.modify['watermark_type'] = watermark_type

    # Call outside lock to avoid double-locking
    await handle_modify_watermark(query, user)

async def handle_watermark_position(query, user, data: str):
    """Handle watermark position selection."""
    position = data.replace("watermark_pos_", "")
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.modify['watermark_position'] = position

    # Call outside lock to avoid double-locking
    await handle_modify_watermark(query, user)

async def handle_watermark_color(query, user, data: str):
    """Handle watermark text color selection."""
    color = data.replace("watermark_color_", "")
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.modify['watermark_text_color'] = color

    # Call outside lock to avoid double-locking
    await handle_modify_watermark(query, user)

async def handle_watermark_opacity(query, user, data: str):
    """Handle watermark opacity selection."""
    opacity = int(data.replace("watermark_opacity_", ""))
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.modify['watermark_opacity'] = opacity

    # Call outside lock to avoid double-locking
    await handle_modify_watermark(query, user)

async def handle_watermark_rotation(query, user, data: str):
    """Handle watermark rotation selection."""
    rotation = int(data.replace("watermark_rotation_", ""))
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.modify['watermark_rotation'] = rotation

    # Call outside lock to avoid double-locking
    await handle_modify_watermark(query, user)

async def handle_watermark_size(query, user, data: str):
    """Handle watermark size selection."""
    size = int(data.replace("watermark_size_", ""))
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.modify['watermark_size'] = size

    # Call outside lock to avoid double-locking
    await handle_modify_watermark(query, user)

async def handle_watermark_preview(query, user):
    """Show watermark preview."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        watermark_type = state.modify.get('watermark_type', 'text')
        if watermark_type == 'text':
            text = state.modify.get('watermark_text', '')
            if not text:
                await query.answer("⚠️ Please set watermark text first!", show_alert=True)
                return
        else:
            has_logo = state.modify.get('watermark_logo_file_id') or state.modify.get('watermark_logo_path')
            if not has_logo:
                await query.answer("⚠️ Please upload logo image first!", show_alert=True)
                return

        await query.answer("👁 Preview will be shown when processing media", show_alert=True)

async def handle_watermark_text_input(query, user):
    """Prompt user to enter watermark text."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.step = ConnectState.MODIFY_WATERMARK_TEXT
        current_text = state.modify.get('watermark_text', '')

        await query.edit_message_text(
            f"*📝 Watermark Text*\n\n"
            f"Current: `{current_text if current_text else 'Not set'}`\n\n"
            f"Send me the text you want to use as watermark.\n"
            f"Example: © MyChannel",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]]),
            parse_mode='Markdown'
        )

async def handle_watermark_logo_input(query, user):
    """Prompt user to upload logo image."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return

        state.step = ConnectState.MODIFY_WATERMARK_LOGO

        await query.edit_message_text(
            "*🖼 Watermark Logo*\n\n"
            "Send me an image (PNG with transparency recommended) to use as watermark logo.\n\n"
            "The image will be resized according to your size settings.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="modify_back_to_main")]]),
            parse_mode='Markdown'
        )

async def handle_modify_toggle(query, user, data: str):
    """Handle toggle buttons for modify options."""
    toggle_type = data.replace("toggle_", "")
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        # Map toggle type to modify key
        toggle_map = {
            'rename': 'rename_enabled',
            'block': 'block_words_enabled',
            'whitelist': 'whitelist_enabled',
            'replace': 'replace_enabled',
            'header': 'header_enabled',
            'footer': 'footer_enabled',
            'buttons': 'buttons_enabled',
            'delay': 'delay_enabled',
            'history': 'history_enabled',
            'watermark': 'watermark_enabled',
            'caption': 'caption_enabled',
            'spoiler': 'apply_spoiler',
        }
        
        key = toggle_map.get(toggle_type)
        if key:
            state.modify[key] = not state.modify.get(key, False)
        
        # Go back to modify main screen
        state.step = ConnectState.ADD_RULE_MODIFY
        await show_modify_keyboard(query, user, state)

async def handle_modify_back_to_main(query, user):
    """Go back from sub-option to modify main screen."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.step = ConnectState.ADD_RULE_MODIFY
        await show_modify_keyboard(query, user, state)

async def handle_delay_set(query, user, data: str):
    """Set delay value."""
    delay_sec = int(data.replace("delay_", ""))
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.modify['delay_seconds'] = delay_sec
        state.modify['delay_enabled'] = delay_sec > 0
        
        # Go back to modify main
        state.step = ConnectState.ADD_RULE_MODIFY
        await show_modify_keyboard(query, user, state)

async def handle_history_set(query, user, data: str):
    """Set history count."""
    count = int(data.replace("history_", ""))
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        state.modify['history_count'] = count
        state.modify['history_enabled'] = count > 0
        
        # Go back to modify main
        state.step = ConnectState.ADD_RULE_MODIFY
        await show_modify_keyboard(query, user, state)

async def handle_clear_option(query, user, data: str):
    """Clear specific modify option data."""
    option = data.replace("clear_", "")
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        clear_map = {
            'block': ('block_words', [], 'block_words_enabled'),
            'whitelist': ('whitelist_words', [], 'whitelist_enabled'),
            'replace': ('replace_pairs', [], 'replace_enabled'),
            'header': ('header_text', '', 'header_enabled'),
            'footer': ('footer_text', '', 'footer_enabled'),
            'buttons': ('buttons', [], 'buttons_enabled'),
            'caption': ('caption_text', '', 'caption_enabled'),
        }
        
        if option in clear_map:
            key, default, enabled_key = clear_map[option]
            state.modify[key] = default
            state.modify[enabled_key] = False
            # Also clear caption entities when clearing caption
            if option == 'caption':
                state.modify['caption_entities'] = []

        # Go back to modify main
        state.step = ConnectState.ADD_RULE_MODIFY
        await show_modify_keyboard(query, user, state)

async def handle_filters_back(query, user):
    """Go back to mode selection (Step 3)."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step != ConnectState.ADD_RULE_FILTERS:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        # Go back to mode selection
        state.step = ConnectState.ADD_RULE_MODE
        
        await query.edit_message_text(
            f"Step 4: Forward Mode\n\n"
            f"Sources: {len(state.sources)}\n"
            f"Destinations: {len(state.destinations)}\n\n"
            f"📤 Forward: Keep 'Forwarded from' header\n"
            f"📋 Copy: No forward header (re-upload media)\n\n"
            f"Choose mode:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📤 Forward", callback_data="mode_forward")],
                [InlineKeyboardButton("📋 Copy", callback_data="mode_copy")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
            ])
        )

async def handle_filters_done(query, user):
    """Finish from filters (skip cleaner) and create the rule."""
    await finalize_rule_creation(query, user)

async def handle_cleaner_done(query, user):
    """Finish from cleaner and create the rule."""
    await finalize_rule_creation(query, user)

async def finalize_rule_creation(query, user):
    """Create or update the rule with all settings."""
    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state or state.step not in [ConnectState.ADD_RULE_FILTERS, ConnectState.ADD_RULE_CLEANER, ConnectState.ADD_RULE_MODIFY]:
            await query.edit_message_text("❌ Session expired.", reply_markup=main_menu_kb())
            return
        
        phone = state.phone
        sources = state.sources
        destinations = state.destinations
        mode = state.forward_mode
        filters_dict = state.filters
        modify_dict = state.modify
        edit_rule_id = state.edit_rule_id
        
        if edit_rule_id:
            # UPDATE existing rule
            await db.update_rule_filters(user.id, edit_rule_id, filters_dict)
            await db.update_rule_modify(user.id, edit_rule_id, modify_dict)
            rule_id = edit_rule_id
            action_text = "Updated"
        else:
            # CREATE new rule
            rule_id = await db.add_forward_rule(user.id, phone, sources, destinations, mode, filters_dict, modify_dict)
            action_text = "Created"

            # 9. HISTORY - Forward past messages if enabled
            if modify_dict.get('history_enabled', False):
                history_count = modify_dict.get('history_count', 0)
                if history_count > 0:
                    try:
                        client = session_manager.clients.get(phone)
                        if client and client.is_connected():
                            log.info(f"[{phone}] Forwarding {history_count} past messages from history...")
                            for source in sources:
                                try:
                                    # Resolve source entity
                                    if source.startswith('@'):
                                        source_entity = await client.get_entity(source)
                                    else:
                                        source_id = int(source)
                                        source_entity = await client.get_entity(source_id)

                                    # Get past messages
                                    messages = await client.get_messages(source_entity, limit=history_count)
                                    log.info(f"[{phone}] Found {len(messages)} messages in {source}")

                                    # Forward to all destinations
                                    for dest in destinations:
                                        try:
                                            if dest.startswith('@'):
                                                dest_entity = await client.get_entity(dest)
                                            else:
                                                dest_id = int(dest)
                                                dest_entity = await client.get_entity(dest_id)

                                            # Forward messages (reversed to maintain chronological order)
                                            if messages:
                                                await client.forward_messages(dest_entity, messages[::-1])
                                                log.info(f"✅ [{phone}] Forwarded {len(messages)} history messages: {source} -> {dest}")
                                        except Exception as e:
                                            log.error(f"❌ [{phone}] History forward failed to {dest}: {e}")
                                except Exception as e:
                                    log.error(f"❌ [{phone}] History fetch failed from {source}: {e}")
                    except Exception as e:
                        log.error(f"❌ [{phone}] History feature error: {e}")

        # Ensure handler is attached/refreshed
        await session_manager.attach_forward_handler(phone)
        
        connect_states.pop(user.id, None)

        # Count enabled ignore filters
        ignore_keys = ['document', 'video', 'audio', 'sticker', 'text', 'photo',
                       'photo_only', 'photo_with_text', 'album', 'poll', 'voice',
                       'video_note', 'gif', 'emoji', 'forward', 'reply', 'link', 'button']
        active_ignores = sum(1 for k in ignore_keys if filters_dict.get(k, False))

        # Count enabled cleaner filters
        cleaner_keys = ['clean_caption', 'clean_hashtag', 'clean_mention', 'clean_link',
                        'clean_emoji', 'clean_phone', 'clean_email']
        active_cleaners = sum(1 for k in cleaner_keys if filters_dict.get(k, False))

        # Check if caption cleaning or modification forces copy mode
        caption_cleaning_active = any(filters_dict.get(k, False) for k in cleaner_keys)
        modify_caption_active = any([
            modify_dict.get('header_enabled', False),
            modify_dict.get('footer_enabled', False),
            modify_dict.get('replace_enabled', False),
            modify_dict.get('watermark_enabled', False),
            modify_dict.get('apply_spoiler', False)
        ])

        # Determine actual mode being used
        if mode == "forward" and (caption_cleaning_active or modify_caption_active):
            mode_text = "📤 Forward → 📋 Copy (auto)"
        elif mode == "forward":
            mode_text = "📤 Forward"
        else:
            mode_text = "📋 Copy"
        
        # Count enabled modify options
        modify_keys = ['rename_enabled', 'block_words_enabled', 'whitelist_enabled',
                       'replace_enabled', 'header_enabled', 'footer_enabled',
                       'buttons_enabled', 'delay_enabled', 'history_enabled', 'watermark_enabled']
        active_mods = sum(1 for k in modify_keys if modify_dict.get(k, False))
        
        await query.edit_message_text(
            f"✅ *Rule {action_text}!*\n\n"
            f"📱 Phone: `{phone}`\n"
            f"📥 Sources: {len(sources)}\n"
            f"📤 Destinations: {len(destinations)}\n"
            f"⚙️ Mode: {mode_text}\n"
            f"🚫 Ignoring: {active_ignores} types\n"
            f"✂️ Cleaning: {active_cleaners} types\n"
            f"✏️ Modifying: {active_mods} options\n\n"
            "🚀 Active now!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 View Rules", callback_data="rules")],
                [InlineKeyboardButton("➕ Add Another", callback_data="add_rule")],
                [InlineKeyboardButton("🏠 Menu", callback_data="main")]
            ]),
            parse_mode='Markdown'
        )

async def show_main_menu(query):
    await query.edit_message_text(
        "🤖 Main Menu\n\nChoose an option:",
        reply_markup=main_menu_kb()
    )

async def show_help(query):
    text = """
ℹ️ *Help*

*Multiple Sources/Destinations:*
Enter IDs separated by commas:
`-1001234567890, @channel, -1009876543210`

*Format:*
• `@channelname` - public
• `-1001234567890` - ID

Get ID: forward msg to @userinfobot
    """
    await query.edit_message_text(text, reply_markup=back_kb())

async def start_connect_flow(query, user):
    if not TELETHON_AVAILABLE:
        await query.edit_message_text("❌ Telethon not installed.", reply_markup=main_menu_kb())
        return
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = ConnectState()
        state.step = ConnectState.WAITING_PHONE
        connect_states[user.id] = state
    
    await query.edit_message_text(
        "📱 *Connect Account*\n\n"
        "Send your phone number:\n"
        "Example: `+919876543210`",
        reply_markup=cancel_kb()
    )

async def show_accounts(query, user):
    accounts = await db.get_user_accounts(user.id)
    
    if not accounts:
        await query.edit_message_text(
            "📱 My Accounts\n\nNo accounts connected.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Connect", callback_data="connect")],
                [InlineKeyboardButton("🔙 Back", callback_data="main")]
            ])
        )
        return
    
    buttons = []
    for acc in accounts:
        phone = acc['phone']
        name = acc.get('display_name') or phone
        buttons.append([InlineKeyboardButton(f"📱 {name}", callback_data=f"acc_view_{phone}")])
    
    buttons.append([InlineKeyboardButton("🔗 Connect New", callback_data="connect")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main")])
    
    await query.edit_message_text(
        f"📱 My Accounts ({len(accounts)})\n\nSelect to manage:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_account_callback(query, user, data: str):
    parts = data.split("_", 2)
    if len(parts) < 3:
        return
    
    action = parts[1]
    phone = parts[2]
    
    if action == "view":
        client = session_manager.clients.get(phone)
        status = "🟢 Connected" if client and client.is_connected() else "🔴 Disconnected"
        
        rules = await db.get_user_rules(user.id)
        rule_count = len([r for r in rules if r['phone'] == phone])
        
        await query.edit_message_text(
            f"📱 Account\n\n"
            f"Phone: {phone}\n"
            f"Status: {status}\n"
            f"Rules: {rule_count}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Rules", callback_data=f"acc_rules_{phone}")],
                [InlineKeyboardButton("🔌 Disconnect", callback_data=f"acc_disc_{phone}")],
                [InlineKeyboardButton("🔙 Back", callback_data="accounts")]
            ])
        )
    
    elif action == "rules":
        rules = await db.get_user_rules(user.id)
        phone_rules = [r for r in rules if r['phone'] == phone]
        
        if not phone_rules:
            await query.edit_message_text(
                f"📋 Rules for {phone}\n\nNo rules.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Rule", callback_data=f"selphone_{phone}")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"acc_view_{phone}")]
                ])
            )
            return
        
        buttons = []
        for r in phone_rules[:8]:
            status = "✅" if r['is_enabled'] else "⏸️"
            src_count = len(r.get('source_list', []))
            dst_count = len(r.get('dest_list', []))
            buttons.append([InlineKeyboardButton(
                f"{status} {src_count}src→{dst_count}dst",
                callback_data=f"rule_view_{r['id']}"
            )])
        
        buttons.append([InlineKeyboardButton("➕ Add", callback_data=f"selphone_{phone}")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"acc_view_{phone}")])
        
        await query.edit_message_text(
            f"📋 Rules for {phone}",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    
    elif action == "disc":
        await session_manager.disconnect_client(phone)
        await db.remove_account(user.id, phone)
        
        await query.edit_message_text(
            f"✅ {phone} disconnected.",
            reply_markup=back_kb("accounts")
        )

async def show_rules(query, user):
    rules = await db.get_user_rules(user.id)
    
    if not rules:
        await query.edit_message_text(
            "📋 My Rules\n\nNo rules configured.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Rule", callback_data="add_rule")],
                [InlineKeyboardButton("🔙 Back", callback_data="main")]
            ])
        )
        return
    
    buttons = []
    for idx, r in enumerate(rules[:10], 1):
        status = "✅" if r['is_enabled'] else "⏸️"
        src_count = len(r.get('source_list', []))
        dst_count = len(r.get('dest_list', []))
        mode_icon = "📤" if r.get('forward_mode', 'forward') == 'forward' else "📋"
        buttons.append([InlineKeyboardButton(
            f"{status} #{idx} | {src_count}→{dst_count} | {mode_icon}",
            callback_data=f"rule_view_{r['id']}"
        )])
    
    buttons.append([InlineKeyboardButton("➕ Add Rule", callback_data="add_rule")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main")])
    
    await query.edit_message_text(
        f"📋 My Rules ({len(rules)})\n\n"
        f"✅ = Active | ⏸️ = Paused\n"
        f"📤 = Forward | 📋 = Copy",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_rule_callback(query, user, data: str):
    parts = data.split("_", 2)
    if len(parts) < 3:
        return
    
    action = parts[1]
    
    try:
        rule_id = int(parts[2])
    except ValueError:
        return
    
    if action == "view":
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        
        if not rule:
            await query.edit_message_text("❌ Rule not found.", reply_markup=main_menu_kb())
            return
        
        status = "✅ Enabled" if rule['is_enabled'] else "⏸️ Paused"
        source_list = rule.get('source_list', [])
        dest_list = rule.get('dest_list', [])
        forward_mode = rule.get('forward_mode', 'forward')
        filters = rule.get('filters', {})
        modify = rule.get('modify', {})
        phone = rule['phone']

        # Check if caption cleaning or modification forces copy mode
        caption_cleaning_active = any([
            filters.get('clean_caption', False),
            filters.get('clean_hashtag', False),
            filters.get('clean_mention', False),
            filters.get('clean_link', False),
            filters.get('clean_emoji', False),
            filters.get('clean_phone', False),
            filters.get('clean_email', False)
        ])
        modify_caption_active = any([
            modify.get('header_enabled', False),
            modify.get('footer_enabled', False),
            modify.get('replace_enabled', False),
            modify.get('watermark_enabled', False),
            modify.get('apply_spoiler', False)
        ])

        # Determine actual mode being used
        if forward_mode == "forward" and (caption_cleaning_active or modify_caption_active):
            mode_text = "📤 Forward → 📋 Copy (auto)"
        elif forward_mode == "forward":
            mode_text = "📤 Forward"
        else:
            mode_text = "📋 Copy"
        
        # Try to get entity names
        async def get_entity_display(identifier: str) -> str:
            """Get display name for entity (name + id)."""
            try:
                client = session_manager.clients.get(phone)
                if client and client.is_connected():
                    if identifier.startswith('@'):
                        entity = await client.get_entity(identifier)
                        name = getattr(entity, 'title', None) or getattr(entity, 'first_name', '') or identifier
                        # Escape special characters in name
                        safe_name = safe_text(name)
                        return f"{safe_name} ({identifier})"
                    else:
                        try:
                            ent_id = int(identifier)
                            entity = await client.get_entity(ent_id)
                            name = getattr(entity, 'title', None) or getattr(entity, 'first_name', '') or 'Unknown'
                            # Escape special characters in name
                            safe_name = safe_text(name)
                            return f"{safe_name} ({identifier})"
                        except Exception:
                            return identifier
            except Exception:
                pass
            return identifier
        
        # Build source list with names
        source_displays = []
        for s in source_list[:5]:
            display = await get_entity_display(s)
            source_displays.append(display)
        
        # Build dest list with names
        dest_displays = []
        for d in dest_list[:5]:
            display = await get_entity_display(d)
            dest_displays.append(display)
        
        # Build filter summary
        filter_icons = {
            'text': '💬', 'photo': '📷', 'video': '🎥', 'document': '📄',
            'audio': '🎵', 'voice': '🎤', 'sticker': '🎨', 'gif': '🎞️',
            'video_note': '⭕', 'link': '🔗', 'hashtag': '#️⃣', 'mention': '@', 'emoji': '😀'
        }
        enabled = [filter_icons.get(k, k) for k, v in filters.items() if v]
        disabled = [filter_icons.get(k, k) for k, v in filters.items() if not v]
        
        text = f"📋 Rule #{rule_id}\n\n"
        text += f"📱 Phone: {phone}\n"
        text += f"📊 Status: {status}\n"
        text += f"⚙️ Mode: {mode_text}\n"
        text += f"📈 Forwards: {rule['forward_count']}\n\n"
        text += f"Sources ({len(source_list)}):\n"
        for s in source_displays:
            text += f"  • {s}\n"
        if len(source_list) > 5:
            text += f"  ...and {len(source_list)-5} more\n"
        text += f"\nDestinations ({len(dest_list)}):\n"
        for d in dest_displays:
            text += f"  • {d}\n"
        if len(dest_list) > 5:
            text += f"  ...and {len(dest_list)-5} more\n"
        
        # Show filters
        if enabled:
            text += f"\n🟢 Ignoring: {' '.join(enabled[:8])}"
            if len(enabled) > 8:
                text += f" +{len(enabled)-8}"
            text += "\n"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔧 Change", callback_data=f"rule_change_{rule_id}"),
                    InlineKeyboardButton("⏯️ Toggle", callback_data=f"rule_toggle_{rule_id}"),
                ],
                [
                    InlineKeyboardButton("🗑️ Delete", callback_data=f"rule_del_{rule_id}"),
                    InlineKeyboardButton("🔙 Back", callback_data="rules")
                ]
            ])
        )
    
    elif action == "toggle":
        new_state = await db.toggle_rule(user.id, rule_id)
        if new_state is not None:
            status = "enabled ✅" if new_state else "paused ⏸️"
            await query.answer(f"Rule {status}")
            await handle_rule_callback(query, user, f"rule_view_{rule_id}")
        else:
            await query.answer("Rule not found")
    
    elif action == "del":
        success = await db.delete_rule(user.id, rule_id)
        if success:
            await query.edit_message_text("✅ Rule deleted.", reply_markup=back_kb("rules"))
        else:
            await query.answer("Failed to delete")
    
    elif action == "change":
        # Show change menu for this rule
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        
        if not rule:
            await query.edit_message_text("❌ Rule not found.", reply_markup=main_menu_kb())
            return
        
        forward_mode = rule.get('forward_mode', 'forward')
        filters = rule.get('filters', {})
        modify = rule.get('modify', {})
        source_list = rule.get('source_list', [])
        dest_list = rule.get('dest_list', [])

        # Check if caption cleaning or modification forces copy mode
        caption_cleaning_active = any([
            filters.get('clean_caption', False),
            filters.get('clean_hashtag', False),
            filters.get('clean_mention', False),
            filters.get('clean_link', False),
            filters.get('clean_emoji', False),
            filters.get('clean_phone', False),
            filters.get('clean_email', False)
        ])
        modify_caption_active = any([
            modify.get('header_enabled', False),
            modify.get('footer_enabled', False),
            modify.get('replace_enabled', False),
            modify.get('watermark_enabled', False),
            modify.get('apply_spoiler', False)
        ])

        # Determine actual mode being used
        if forward_mode == "forward" and (caption_cleaning_active or modify_caption_active):
            mode_text = "📤 Forward → 📋 Copy (auto)"
            auto_copy_hint = "\n💡 Using Copy Mode (caption cleaning/modification enabled)"
        elif forward_mode == "forward":
            mode_text = "📤 Forward"
            auto_copy_hint = ""
        else:
            mode_text = "📋 Copy"
            auto_copy_hint = ""

        # Build mode button with current mode marked as 🟢
        if forward_mode == "forward":
            if caption_cleaning_active or modify_caption_active:
                mode_btn = InlineKeyboardButton("🟢 📤 Forward → 📋 Copy (auto)", callback_data=f"rule_chmode_copy_{rule_id}")
            else:
                mode_btn = InlineKeyboardButton("🟢 📤 Forward | 📋 Copy", callback_data=f"rule_chmode_copy_{rule_id}")
        else:
            mode_btn = InlineKeyboardButton("📤 Forward | 🟢 📋 Copy", callback_data=f"rule_chmode_forward_{rule_id}")
        
        await query.edit_message_text(
            f"🔧 Change Rule #{rule_id}\n\n"
            f"📱 Phone: {rule['phone']}\n"
            f"📥 Sources: {len(source_list)}\n"
            f"📤 Destinations: {len(dest_list)}\n"
            f"⚙️ Mode: {mode_text}{auto_copy_hint}\n\n"
            f"What do you want to change?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Sources", callback_data=f"rule_chsrc_{rule_id}")],
                [InlineKeyboardButton("📤 Destinations", callback_data=f"rule_chdst_{rule_id}")],
                [mode_btn],
                [InlineKeyboardButton("🚫 Filters", callback_data=f"rule_chfilter_{rule_id}")],
                [InlineKeyboardButton("✂️ Cleaner", callback_data=f"rule_chclean_{rule_id}")],
                [InlineKeyboardButton("✏️ Modify", callback_data=f"rule_chmodify_{rule_id}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"rule_view_{rule_id}")]
            ])
        )
    
    elif action == "chsrc":
        # Change sources
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        if not rule:
            await query.answer("Rule not found")
            return
        
        lock = await get_connect_lock(user.id)
        async with lock:
            state = ConnectState()
            state.step = ConnectState.EDIT_RULE_SOURCE
            state.edit_rule_id = rule_id
            state.phone = rule['phone']
            state.sources = rule.get('source_list', [])
            state.destinations = rule.get('dest_list', [])
            state.forward_mode = rule.get('forward_mode', 'forward')
            state.filters = rule.get('filters', DEFAULT_FILTERS.copy())
            state.modify = rule.get('modify', DEFAULT_MODIFY.copy())
            connect_states[user.id] = state
        
        current_sources = ', '.join(rule.get('source_list', [])[:5])
        await query.edit_message_text(
            f"📥 Change Sources\n\n"
            f"Current sources:\n{current_sources}\n\n"
            f"Send new sources (comma-separated):\n"
            f"-1001234567890, @channel\n\n"
            f"Or send 'keep' to keep current sources.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"rule_change_{rule_id}")]
            ])
        )
    
    elif action == "chdst":
        # Change destinations
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        if not rule:
            await query.answer("Rule not found")
            return
        
        lock = await get_connect_lock(user.id)
        async with lock:
            state = ConnectState()
            state.step = ConnectState.EDIT_RULE_DEST
            state.edit_rule_id = rule_id
            state.phone = rule['phone']
            state.sources = rule.get('source_list', [])
            state.destinations = rule.get('dest_list', [])
            state.forward_mode = rule.get('forward_mode', 'forward')
            state.filters = rule.get('filters', DEFAULT_FILTERS.copy())
            state.modify = rule.get('modify', DEFAULT_MODIFY.copy())
            connect_states[user.id] = state
        
        current_dests = ', '.join(rule.get('dest_list', [])[:5])
        await query.edit_message_text(
            f"📤 Change Destinations\n\n"
            f"Current destinations:\n{current_dests}\n\n"
            f"Send new destinations (comma-separated):\n"
            f"-1001234567890, @channel\n\n"
            f"Or send 'keep' to keep current destinations.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"rule_change_{rule_id}")]
            ])
        )
    
    elif action.startswith("chmode_"):
        # Change forward mode
        new_mode = action.replace("chmode_", "")  # "forward" or "copy"
        success = await db.update_rule_mode(user.id, rule_id, new_mode)
        if success:
            mode_text = "📤 Forward" if new_mode == "forward" else "📋 Copy"
            await query.answer(f"Mode changed to {mode_text}")
            # Reload handler
            rules = await db.get_user_rules(user.id)
            rule = next((r for r in rules if r['id'] == rule_id), None)
            if rule:
                await session_manager.attach_forward_handler(rule['phone'])
        await handle_rule_callback(query, user, f"rule_change_{rule_id}")
    
    elif action == "chfilter":
        # Change filters
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        if not rule:
            await query.answer("Rule not found")
            return
        
        lock = await get_connect_lock(user.id)
        async with lock:
            state = ConnectState()
            state.step = ConnectState.ADD_RULE_FILTERS
            state.edit_rule_id = rule_id
            state.phone = rule['phone']
            state.sources = rule.get('source_list', [])
            state.destinations = rule.get('dest_list', [])
            state.forward_mode = rule.get('forward_mode', 'forward')
            state.filters = rule.get('filters', DEFAULT_FILTERS.copy())
            state.modify = rule.get('modify', DEFAULT_MODIFY.copy())
            connect_states[user.id] = state
        
        await show_filters_keyboard(query, user, state)
    
    elif action == "chclean":
        # Change cleaner
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        if not rule:
            await query.answer("Rule not found")
            return
        
        lock = await get_connect_lock(user.id)
        async with lock:
            state = ConnectState()
            state.step = ConnectState.ADD_RULE_CLEANER
            state.edit_rule_id = rule_id
            state.phone = rule['phone']
            state.sources = rule.get('source_list', [])
            state.destinations = rule.get('dest_list', [])
            state.forward_mode = rule.get('forward_mode', 'forward')
            state.filters = rule.get('filters', DEFAULT_FILTERS.copy())
            state.modify = rule.get('modify', DEFAULT_MODIFY.copy())
            connect_states[user.id] = state
        
        await show_cleaner_keyboard(query, user, state)
    
    elif action == "chmodify":
        # Change modify content
        rules = await db.get_user_rules(user.id)
        rule = next((r for r in rules if r['id'] == rule_id), None)
        if not rule:
            await query.answer("Rule not found")
            return
        
        lock = await get_connect_lock(user.id)
        async with lock:
            state = ConnectState()
            state.step = ConnectState.ADD_RULE_MODIFY
            state.edit_rule_id = rule_id
            state.phone = rule['phone']
            state.sources = rule.get('source_list', [])
            state.destinations = rule.get('dest_list', [])
            state.forward_mode = rule.get('forward_mode', 'forward')
            state.filters = rule.get('filters', DEFAULT_FILTERS.copy())
            state.modify = rule.get('modify', DEFAULT_MODIFY.copy())
            connect_states[user.id] = state
        
        await show_modify_keyboard(query, user, state)

async def start_add_rule(query, user):
    accounts = await db.get_user_accounts(user.id)
    
    if not accounts:
        await query.edit_message_text(
            "❌ No accounts connected.\n\nConnect an account first!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Connect", callback_data="connect")],
                [InlineKeyboardButton("🔙 Back", callback_data="main")]
            ])
        )
        return
    
    buttons = []
    for acc in accounts:
        phone = acc['phone']
        buttons.append([InlineKeyboardButton(f"📱 {phone}", callback_data=f"selphone_{phone}")])
    
    buttons.append([InlineKeyboardButton("🔙 Cancel", callback_data="main")])
    
    await query.edit_message_text(
        "➕ Add Rule\n\nSelect account:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def start_add_rule_for_phone(query, user, phone: str):
    client = session_manager.clients.get(phone)
    if not client or not client.is_connected():
        await query.edit_message_text(
            f"❌ Account {phone} not connected.",
            reply_markup=back_kb("accounts")
        )
        return
    
    lock = await get_connect_lock(user.id)
    async with lock:
        state = ConnectState()
        state.step = ConnectState.ADD_RULE_SOURCE
        state.phone = phone
        state.sources = []
        connect_states[user.id] = state
    
    await query.edit_message_text(
        f"➕ Add Rule for {phone}\n\n"
        "*Step 1:* Enter SOURCES\n\n"
        "Where messages come FROM.\n"
        "You can enter multiple IDs separated by commas:\n\n"
        "`-1001234567890, -1009876543210, @channel`\n\n"
        "*Format:*\n"
        "• `@channelname`\n"
        "• `-1001234567890`",
        reply_markup=cancel_kb()
    )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    user = update.effective_user
    if not user:
        return

    # Get text if available (for text-based states)
    text = update.message.text.strip() if update.message.text else ""

    # Skip commands
    if text and text.startswith('/'):
        return

    lock = await get_connect_lock(user.id)
    async with lock:
        state = connect_states.get(user.id)
        if not state:
            return

        # For watermark logo state, accept media without text
        if state.step == ConnectState.MODIFY_WATERMARK_LOGO:
            # Check if media was sent
            if not (update.message.photo or update.message.sticker or
                    update.message.animation or update.message.document):
                # No media sent, ask for it
                await update.message.reply_text(
                    "❌ Please send an image, sticker, GIF, or image document for the watermark logo.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Back to Watermark", callback_data="modify_watermark")]
                    ])
                )
                return
        else:
            # For all other states, we need text
            if not text:
                return

        try:
            if state.step == ConnectState.WAITING_PHONE:
                await handle_phone_input(update, user, state, text)
            elif state.step == ConnectState.WAITING_CODE:
                await handle_code_input(update, user, state, text)
            elif state.step == ConnectState.WAITING_PASSWORD:
                await handle_password_input(update, user, state, text)
            elif state.step == ConnectState.ADD_RULE_SOURCE:
                await handle_source_input(update, user, state, text)
            elif state.step == ConnectState.ADD_RULE_DEST:
                await handle_dest_input(update, user, state, text)
            # Modify content input handlers
            elif state.step == ConnectState.MODIFY_RENAME:
                await handle_modify_rename_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_BLOCK_WORDS:
                await handle_modify_block_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_WHITELIST:
                await handle_modify_whitelist_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_REPLACE:
                await handle_modify_replace_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_HEADER:
                await handle_modify_header_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_FOOTER:
                await handle_modify_footer_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_CAPTION:
                await handle_modify_caption_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_BUTTONS:
                await handle_modify_buttons_input(update, user, state, text)
            elif state.step == ConnectState.MODIFY_WATERMARK_TEXT:
                await handle_watermark_text_message(update, user, state, text)
            elif state.step == ConnectState.MODIFY_WATERMARK_LOGO:
                await handle_watermark_logo_message(update, context, user, state)
            # Edit rule handlers
            elif state.step == ConnectState.EDIT_RULE_SOURCE:
                await handle_edit_source_input(update, user, state, text)
            elif state.step == ConnectState.EDIT_RULE_DEST:
                await handle_edit_dest_input(update, user, state, text)
        except Exception as e:
            log.exception(f"Message handler error: {e}")
            connect_states.pop(user.id, None)
            await update.message.reply_text(f"❌ Error: {e}", reply_markup=main_menu_kb())

async def handle_phone_input(update, user, state, phone: str):
    if not phone.startswith("+") or len(phone) < 8:
        await update.message.reply_text(
            "❌ Invalid format.\n\nUse: `+919876543210`",
            reply_markup=cancel_kb()
        )
        return
    
    state.phone = phone
    await update.message.reply_text(f"📤 Sending code to {phone}...")
    
    try:
        client = await session_manager.get_or_create_client(user.id, phone)
        sent = await client.send_code_request(phone)
        
        state.phone_code_hash = sent.phone_code_hash
        state.step = ConnectState.WAITING_CODE
        
        await update.message.reply_text(
            "✅ Code sent!\n\nEnter the code:",
            reply_markup=cancel_kb()
        )
    except errors.FloodWaitError as e:
        connect_states.pop(user.id, None)
        await update.message.reply_text(f"⚠️ Rate limited. Wait {e.seconds}s", reply_markup=main_menu_kb())
    except Exception as e:
        log.exception("send_code failed")
        connect_states.pop(user.id, None)
        await update.message.reply_text(f"❌ Failed: {e}", reply_markup=main_menu_kb())

async def handle_code_input(update, user, state, code: str):
    code = code.replace(" ", "").replace("-", "")
    phone = state.phone
    
    try:
        client = await session_manager.get_or_create_client(user.id, phone)
        await client.sign_in(phone=phone, code=code, phone_code_hash=state.phone_code_hash)
        
        me = await client.get_me()
        name = f"{me.first_name or ''} {me.last_name or ''}".strip() or phone
        
        await db.add_connected_account(user.id, phone, name)
        await session_manager.attach_forward_handler(phone)
        
        connect_states.pop(user.id, None)
        
        await update.message.reply_text(
            f"✅ Connected!\n\nAccount: {name}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Rule", callback_data=f"selphone_{phone}")],
                [InlineKeyboardButton("🏠 Menu", callback_data="main")]
            ])
        )
    except errors.SessionPasswordNeededError:
        state.step = ConnectState.WAITING_PASSWORD
        await update.message.reply_text("🔐 Enter 2FA password:", reply_markup=cancel_kb())
    except errors.PhoneCodeInvalidError:
        await update.message.reply_text("❌ Invalid code. Try again:", reply_markup=cancel_kb())
    except errors.PhoneCodeExpiredError:
        connect_states.pop(user.id, None)
        await update.message.reply_text("❌ Code expired.", reply_markup=main_menu_kb())
    except Exception as e:
        log.exception("sign_in failed")
        connect_states.pop(user.id, None)
        await update.message.reply_text(f"❌ Failed: {e}", reply_markup=main_menu_kb())

async def handle_password_input(update, user, state, password: str):
    phone = state.phone
    
    try:
        client = await session_manager.get_or_create_client(user.id, phone)
        await client.sign_in(password=password)
        
        me = await client.get_me()
        name = f"{me.first_name or ''} {me.last_name or ''}".strip() or phone
        
        await db.add_connected_account(user.id, phone, name)
        await session_manager.attach_forward_handler(phone)
        
        connect_states.pop(user.id, None)
        
        await update.message.reply_text(
            f"✅ Connected with 2FA!\n\nAccount: {name}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Rule", callback_data=f"selphone_{phone}")],
                [InlineKeyboardButton("🏠 Menu", callback_data="main")]
            ])
        )
    except Exception as e:
        log.exception("2FA failed")
        connect_states.pop(user.id, None)
        await update.message.reply_text(f"❌ 2FA failed: {e}", reply_markup=main_menu_kb())

async def handle_source_input(update, user, state, text: str):
    phone = state.phone
    
    # Parse multiple sources
    sources = parse_multi_ids(text)
    
    if not sources:
        await update.message.reply_text(
            "❌ No valid sources found.\n\n"
            "Enter IDs separated by commas:\n"
            "`-1001234567890, @channel, -1009876543210`",
            reply_markup=cancel_kb()
        )
        return
    
    # Validate format
    invalid = []
    valid = []
    for src in sources:
        is_username = src.startswith('@')
        is_numeric = src.lstrip('-').isdigit()
        if is_username or is_numeric:
            valid.append(src)
        else:
            invalid.append(src)
    
    if invalid:
        await update.message.reply_text(
            f"⚠️ Invalid format: {', '.join(invalid)}\n\n"
            "Use `@username` or `-1001234567890`",
            reply_markup=cancel_kb()
        )
        return
    
    # Verify sources (optional)
    await update.message.reply_text(f"🔍 Validating {len(valid)} sources...")
    
    verified = []
    failed = []
    for src in valid:
        success, entity, error = await session_manager.resolve_entity(phone, src)
        if success:
            verified.append(src)
        else:
            failed.append(f"{src}: {error}")
    
    state.sources = valid  # Keep all, even unverified
    state.step = ConnectState.ADD_RULE_DEST
    
    msg = f"✅ Sources: {len(valid)} total\n"
    if verified:
        msg += f"✓ Verified: {len(verified)}\n"
    if failed:
        msg += f"⚠️ Could not verify: {len(failed)}\n"
        for f in failed[:3]:
            msg += f"  • {f}\n"
    
    msg += "\n*Step 2:* Enter DESTINATIONS\n\n"
    msg += "Where messages will be forwarded TO.\n"
    msg += "Enter multiple IDs separated by commas:\n\n"
    msg += "`@dest1, -1001111111111, 123456789`"
    
    await update.message.reply_text(msg, reply_markup=cancel_kb())

async def handle_dest_input(update, user, state, text: str):
    phone = state.phone
    sources = state.sources
    
    # Parse multiple destinations
    destinations = parse_multi_ids(text)
    
    if not destinations:
        await update.message.reply_text(
            "❌ No valid destinations found.\n\n"
            "Enter IDs separated by commas:\n"
            "`@dest1, -1001111111111, 123456789`",
            reply_markup=cancel_kb()
        )
        return
    
    # Validate format
    invalid = []
    valid = []
    for dst in destinations:
        is_username = dst.startswith('@')
        is_numeric = dst.lstrip('-').isdigit()
        if is_username or is_numeric:
            valid.append(dst)
        else:
            invalid.append(dst)
    
    if invalid:
        await update.message.reply_text(
            f"⚠️ Invalid format: {', '.join(invalid)}\n\n"
            "Use `@username` or `-1001234567890`",
            reply_markup=cancel_kb()
        )
        return
    
    # Verify destinations
    await update.message.reply_text(f"🔍 Validating {len(valid)} destinations...")
    
    verified = []
    failed = []
    for dst in valid:
        success, entity, error = await session_manager.resolve_entity(phone, dst)
        if success:
            verified.append(dst)
        else:
            failed.append(f"{dst}: {error}")
    
    if failed:
        msg = f"⚠️ Could not verify {len(failed)} destinations:\n"
        for f in failed[:3]:
            msg += f"  • {f}\n"
        await update.message.reply_text(msg)
    
    # Save destinations and go to Step 3
    state.destinations = valid
    state.step = ConnectState.ADD_RULE_MODE
    
    await update.message.reply_text(
        f"✅ Destinations: {len(valid)} total\n\n"
        f"Step 3: Select Forward Mode\n\n"
        "Choose how messages should be sent:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Forward", callback_data="mode_forward")],
            [InlineKeyboardButton("📋 Copy", callback_data="mode_copy")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
        ])
    )
    
    # Send explanation
    await update.message.reply_text(
        "*📤 Forward:*\n"
        "• Keeps original sender info\n"
        "• Shows 'Forwarded from' header\n"
        "• Fast, no re-upload needed\n\n"
        "*📋 Copy:*\n"
        "• Sends as YOUR message\n"
        "• No forward header shown\n"
        "• Downloads & re-uploads media\n"
        "• Appears as original content"
    )

# ==================== MODIFY INPUT HANDLERS ====================

async def handle_modify_rename_input(update, user, state, text: str):
    """Handle rename pattern input."""
    state.modify['rename_pattern'] = text.strip()
    state.modify['rename_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY
    
    await update.message.reply_text(
        f"✅ Rename pattern set: `{text.strip()}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ]),
        parse_mode='Markdown'
    )

async def handle_modify_block_input(update, user, state, text: str):
    """Handle block words input."""
    words = [w.strip() for w in text.replace('\n', ',').split(',') if w.strip()]
    existing = state.modify.get('block_words', [])
    state.modify['block_words'] = list(set(existing + words))
    state.modify['block_words_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY
    
    await update.message.reply_text(
        f"✅ Added {len(words)} block words.\nTotal: {len(state.modify['block_words'])}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_modify_whitelist_input(update, user, state, text: str):
    """Handle whitelist keywords input."""
    words = [w.strip() for w in text.replace('\n', ',').split(',') if w.strip()]
    existing = state.modify.get('whitelist_words', [])
    state.modify['whitelist_words'] = list(set(existing + words))
    state.modify['whitelist_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY
    
    await update.message.reply_text(
        f"✅ Added {len(words)} whitelist keywords.\nTotal: {len(state.modify['whitelist_words'])}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_modify_replace_input(update, user, state, text: str):
    """Handle word replacement input."""
    lines = text.strip().split('\n')
    pairs = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        if ' -> ' in line:
            parts = line.split(' -> ', 1)
        elif ' => ' in line:
            parts = line.split(' => ', 1)
        elif '->' in line:
            parts = line.split('->', 1)
        elif '=>' in line:
            parts = line.split('=>', 1)
        else:
            continue
        
        if len(parts) == 2:
            pairs.append({'from': parts[0].strip(), 'to': parts[1].strip(), 'regex': False})
    
    if pairs:
        existing = state.modify.get('replace_pairs', [])
        state.modify['replace_pairs'] = existing + pairs
        state.modify['replace_enabled'] = True
    
    state.step = ConnectState.ADD_RULE_MODIFY
    
    await update.message.reply_text(
        f"✅ Added {len(pairs)} replacement pairs.\nTotal: {len(state.modify.get('replace_pairs', []))}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_modify_header_input(update, user, state, text: str):
    """Handle header text input."""
    header = text.strip().replace('{newline}', '\n')
    state.modify['header_text'] = header
    state.modify['header_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY
    
    await update.message.reply_text(
        f"✅ Header set",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_modify_footer_input(update, user, state, text: str):
    """Handle footer text input."""
    footer = text.strip().replace('{newline}', '\n')
    state.modify['footer_text'] = footer
    state.modify['footer_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY

    await update.message.reply_text(
        f"✅ Footer set",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_modify_caption_input(update, user, state, text: str):
    """Handle caption text input."""
    # Parse markdown to plain text and entities
    plain_text, entities = parse_markdown_to_entities(text.strip())

    # Store both text and serialized entities (for database persistence)
    state.modify['caption_text'] = plain_text
    state.modify['caption_entities'] = serialize_entities(entities)
    state.modify['caption_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY

    # Preview formatted caption
    preview = plain_text[:100] + '...' if len(plain_text) > 100 else plain_text
    entity_count = len(entities)
    entity_info = f" ({entity_count} format{'s' if entity_count != 1 else ''})" if entity_count > 0 else ""

    await update.message.reply_text(
        f"✅ Caption set{entity_info}\n\n"
        f"Preview: {preview}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_modify_buttons_input(update, user, state, text: str):
    """Handle link buttons input."""
    lines = text.strip().split('\n')
    buttons_rows = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        row_buttons = []
        parts = line.split('&&')
        
        for part in parts:
            part = part.strip()
            if ' - ' in part:
                btn_parts = part.split(' - ', 1)
                if len(btn_parts) == 2:
                    btn_text = btn_parts[0].strip()
                    btn_url = btn_parts[1].strip()
                    if btn_url.startswith('http') or btn_url.startswith('t.me') or btn_url.startswith('tg://'):
                        if not btn_url.startswith('http'):
                            btn_url = 'https://' + btn_url
                        row_buttons.append({'text': btn_text, 'url': btn_url})
        
        if row_buttons:
            buttons_rows.append(row_buttons)
    
    if buttons_rows:
        state.modify['buttons'] = buttons_rows
        state.modify['buttons_enabled'] = True
    
    state.step = ConnectState.ADD_RULE_MODIFY
    
    await update.message.reply_text(
        f"✅ Added {len(buttons_rows)} button rows",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Modify", callback_data="modify_back_to_main")]
        ])
    )

async def handle_watermark_text_message(update, user, state, text: str):
    """Handle watermark text input."""
    watermark_text = text.strip()
    state.modify['watermark_text'] = watermark_text
    state.modify['watermark_enabled'] = True
    state.step = ConnectState.ADD_RULE_MODIFY

    await update.message.reply_text(
        f"✅ Watermark text set: `{watermark_text[:50]}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Watermark", callback_data="modify_watermark")]
        ]),
        parse_mode='Markdown'
    )

async def handle_watermark_logo_message(update, context, user, state):
    """Handle watermark logo image/sticker/GIF upload."""
    import tempfile
    import os
    from pathlib import Path

    # Check what type of media was sent
    file_to_download = None
    file_id = None
    media_type = None

    if update.message.photo:
        # Photo sent
        photo = update.message.photo[-1]  # Get highest resolution
        file_id = photo.file_id
        media_type = "photo"
        log.info(f"📷 Watermark logo: Photo received (file_id: {file_id})")
    elif update.message.sticker:
        # Sticker sent
        sticker = update.message.sticker
        file_id = sticker.file_id
        media_type = "sticker"
        log.info(f"🎨 Watermark logo: Sticker received (file_id: {file_id})")
    elif update.message.animation:
        # GIF/animation sent
        animation = update.message.animation
        file_id = animation.file_id
        media_type = "gif"
        log.info(f"🎬 Watermark logo: GIF/Animation received (file_id: {file_id})")
    elif update.message.document:
        # Document (image file) sent
        document = update.message.document
        if document.mime_type and document.mime_type.startswith('image/'):
            file_id = document.file_id
            media_type = "document"
            log.info(f"📄 Watermark logo: Image document received (file_id: {file_id})")
        else:
            await update.message.reply_text(
                "❌ Please send an image, sticker, or GIF.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back to Watermark", callback_data="modify_watermark")]
                ])
            )
            return
    else:
        await update.message.reply_text(
            "❌ Please send an image, sticker, or GIF.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Watermark", callback_data="modify_watermark")]
            ])
        )
        return

    # Download and save the logo
    try:
        log.info(f"💾 Downloading watermark logo (type: {media_type})...")

        file = await context.bot.get_file(file_id)

        # Create a persistent directory for watermark logos (not temp)
        # This ensures logos persist across sessions
        logo_dir = Path.home() / ".telegram_bot" / "watermark_logos"
        logo_dir.mkdir(parents=True, exist_ok=True)

        # Determine file extension based on media type
        if media_type == "sticker":
            # Stickers are usually WebP, but we'll convert to PNG
            ext = ".png"
        elif media_type == "gif":
            # For GIFs, we'll save as PNG (first frame) for watermarking
            ext = ".png"
        else:
            # For photos/documents, preserve extension or use PNG
            ext = ".png"

        logo_path = str(logo_dir / f"watermark_logo_{user.id}{ext}")

        # Download the file to a temporary location first
        temp_download = str(logo_dir / f"temp_logo_{user.id}")
        await file.download_to_drive(temp_download)

        log.info(f"✅ Watermark logo downloaded to: {temp_download}")

        # Convert to PNG using FFmpeg (works for all media types)
        # This ensures animated stickers/GIFs are converted to static PNG
        try:
            import subprocess

            log.info(f"🎬 Converting logo to static PNG using FFmpeg...")

            # Use FFmpeg to extract first frame and save as PNG
            # -frames:v 1 = extract only first frame
            # -update 1 = allow writing single image
            # -f image2 = force image output format
            ffmpeg_cmd = [
                'ffmpeg', '-i', temp_download,
                '-frames:v', '1',     # Extract first frame only
                '-update', '1',       # Allow overwriting single image
                '-f', 'image2',       # Force image format
                '-y',                 # Overwrite output
                logo_path
            ]

            log.info(f"🎬 FFmpeg command: {' '.join(ffmpeg_cmd)}")
            result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                log.info(f"✅ Logo converted to PNG successfully")
                # Remove temp file
                try:
                    os.remove(temp_download)
                except (OSError, IOError) as e:
                    log.warning(f"Failed to cleanup temp download {temp_download}: {e}")
            else:
                # If FFmpeg fails, just rename the temp file
                log.warning(f"⚠️ FFmpeg conversion failed, using original file")
                log.warning(f"⚠️ FFmpeg error: {result.stderr}")
                os.rename(temp_download, logo_path)

        except Exception as conv_error:
            log.warning(f"⚠️ Could not convert logo with FFmpeg: {conv_error}")
            # If conversion fails, just use the downloaded file
            try:
                os.rename(temp_download, logo_path)
            except (OSError, IOError) as e:
                log.error(f"Failed to rename temp file to logo path: {e}")

        # Save to state
        state.modify['watermark_logo_file_id'] = file_id
        state.modify['watermark_logo_path'] = logo_path
        state.modify['watermark_type'] = 'logo'  # Automatically set to logo type
        state.modify['watermark_enabled'] = True
        state.step = ConnectState.ADD_RULE_MODIFY

        # Verify file exists
        if os.path.exists(logo_path):
            file_size = os.path.getsize(logo_path)
            log.info(f"✅ Logo file verified: {file_size} bytes")

            await update.message.reply_text(
                f"✅ Logo uploaded successfully!\n"
                f"📁 Saved as: {Path(logo_path).name}\n"
                f"📊 Size: {file_size} bytes\n"
                f"🎨 Type: {media_type.upper()}\n\n"
                f"Logo will be applied to both images and videos.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back to Watermark", callback_data="modify_watermark")]
                ])
            )
        else:
            raise Exception("Logo file not found after download")

    except Exception as e:
        log.error(f"❌ Failed to download/process logo: {e}")
        import traceback
        log.error(f"❌ Logo error traceback: {traceback.format_exc()}")

        await update.message.reply_text(
            f"❌ Failed to upload logo: {e}\n\n"
            f"Please try again with a different image.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Watermark", callback_data="modify_watermark")]
            ])
        )

# ==================== EDIT RULE INPUT HANDLERS ====================

async def handle_edit_source_input(update, user, state, text: str):
    """Handle edited sources input."""
    rule_id = state.edit_rule_id
    
    if text.lower() == 'keep':
        # Keep current sources
        connect_states.pop(user.id, None)
        await update.message.reply_text(
            "✅ Sources unchanged.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"rule_change_{rule_id}")]
            ])
        )
        return
    
    # Parse new sources
    sources = parse_multi_ids(text)
    if not sources:
        await update.message.reply_text(
            "❌ No valid sources found.\n\nTry again or send `keep`.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"rule_change_{rule_id}")]
            ])
        )
        return
    
    # Update database
    success = await db.update_rule_sources(user.id, rule_id, sources)
    connect_states.pop(user.id, None)
    
    if success:
        # Refresh handler
        await session_manager.attach_forward_handler(state.phone)
        await update.message.reply_text(
            f"✅ Sources updated! ({len(sources)} sources)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"rule_view_{rule_id}")]
            ])
        )
    else:
        await update.message.reply_text("❌ Failed to update.", reply_markup=main_menu_kb())

async def handle_edit_dest_input(update, user, state, text: str):
    """Handle edited destinations input."""
    rule_id = state.edit_rule_id
    
    if text.lower() == 'keep':
        connect_states.pop(user.id, None)
        await update.message.reply_text(
            "✅ Destinations unchanged.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"rule_change_{rule_id}")]
            ])
        )
        return
    
    destinations = parse_multi_ids(text)
    if not destinations:
        await update.message.reply_text(
            "❌ No valid destinations found.\n\nTry again or send `keep`.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data=f"rule_change_{rule_id}")]
            ])
        )
        return
    
    success = await db.update_rule_destinations(user.id, rule_id, destinations)
    connect_states.pop(user.id, None)
    
    if success:
        await session_manager.attach_forward_handler(state.phone)
        await update.message.reply_text(
            f"✅ Destinations updated! ({len(destinations)} destinations)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data=f"rule_view_{rule_id}")]
            ])
        )
    else:
        await update.message.reply_text("❌ Failed to update.", reply_markup=main_menu_kb())

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error(f"Exception: {context.error}")
    try:
        if update and hasattr(update, 'effective_message') and update.effective_message:
            await update.effective_message.reply_text("❌ An error occurred. Use /start")
    except Exception:
        pass

# ==================== LIFECYCLE ====================
async def on_startup(app):
    global db, session_manager, health_server_instance

    log.info("🚀 Starting bot...")

    # Start health check server
    try:
        import health_server
        health_port = int(os.getenv('HEALTH_PORT', '8080'))
        health_server_instance = health_server.start_health_server(port=health_port)
        log.info(f"✅ Health server started on port {health_port}")
    except Exception as e:
        log.warning(f"⚠️ Health server failed to start: {e}")
        health_server_instance = None

    db = DatabaseManager(DATABASE_FILE)
    await db.ensure_initialized()
    session_manager = UserSessionManager(db)

    if TELETHON_AVAILABLE:
        await session_manager.load_existing_sessions()

        # Update health metrics
        if health_server_instance:
            try:
                import health_server
                health_server.update_active_sessions(len(session_manager.clients))
                health_server.set_database_health(True)
                health_server.set_telegram_health(True)
            except:
                pass

    log.info(f"✅ Ready. {len(session_manager.clients)} sessions loaded.")

async def on_shutdown(app):
    global health_server_instance

    log.info("🛑 Shutting down...")

    if session_manager:
        await session_manager.cleanup()

    # Stop health server
    if health_server_instance:
        try:
            health_server_instance.stop()
            log.info("✅ Health server stopped")
        except:
            pass

    log.info("✅ Done.")

# ==================== MAIN ====================
def main():
    """Main entry point for the bot."""
    # Check dependencies
    if not TELEGRAM_AVAILABLE:
        print("❌ python-telegram-bot not installed!")
        print("   Install: pip install python-telegram-bot")
        return 1
    
    if not TELETHON_AVAILABLE:
        print("⚠️ Telethon not installed - forwarding features disabled")
        print("   Install: pip install telethon")
    
    # Validate configuration
    if not Config.validate():
        print("")
        print("=" * 50)
        print("Configuration Error!")
        print("=" * 50)
        print("")
        print("Please set the required environment variables:")
        print("")
        print("  Option 1: Export variables")
        print("    export TELEGRAM_API_ID=your_api_id")
        print("    export TELEGRAM_API_HASH=your_api_hash")
        print("    export TELEGRAM_BOT_TOKEN=your_bot_token")
        print("")
        print("  Option 2: Create .env file")
        print("    TELEGRAM_API_ID=your_api_id")
        print("    TELEGRAM_API_HASH=your_api_hash")
        print("    TELEGRAM_BOT_TOKEN=your_bot_token")
        print("")
        print("Get API credentials from: https://my.telegram.org")
        print("Get bot token from: @BotFather on Telegram")
        print("")
        return 1
    
    # Build application
    try:
        app = (ApplicationBuilder()
               .token(Config.BOT_TOKEN)
               .post_init(on_startup)
               .post_shutdown(on_shutdown)
               .build())
    except Exception as e:
        log.error(f"❌ Failed to create bot application: {e}")
        return 1
    
    # Add handlers
    app.add_error_handler(error_handler)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("rules", cmd_rules))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(callback_handler))
    # Accept text messages, photos, stickers, animations (GIFs), and documents
    app.add_handler(MessageHandler(
        (filters.TEXT | filters.PHOTO | filters.ANIMATION | filters.Sticker.ALL | filters.Document.ALL) & ~filters.COMMAND,
        message_handler
    ))
    
    log.info("=" * 50)
    log.info("🤖 Telegram Auto-Forward Bot")
    log.info("=" * 50)
    log.info("🚀 Starting polling...")
    
    try:
        app.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        log.info("👋 Bot stopped by user")
    except Exception as e:
        log.exception(f"❌ Bot crashed: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    import sys
    # Optional keep_alive for services like Replit
    try:
        from keep_alive import keep_alive
        keep_alive()
    except ImportError:
        pass  # keep_alive module not available, continue without it
    sys.exit(main())
