# ============================================================
# math_tekshiruvchi_bot — main.py
# Part 1/2
# ============================================================

from __future__ import annotations

import asyncio
import html
import io
import json
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    and_,
    desc,
    func,
    select,
)
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    File,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


# ============================================================
# Settings
# ============================================================

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    BOT_TOKEN: str = ""
    ADMIN_ID: int = 0

    GEMINI_API_KEY: str = ""
    OPENAI_API_KEY: str = ""

    DATABASE_URL: str = "sqlite+aiosqlite:///./math_tekshiruvchi_bot.db"
    REDIS_URL: str = ""

    WEBHOOK_URL: str = ""
    WEBHOOK_PATH: str = "/webhook"
    PORT: int = 8000

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    # Optional safety defaults
    MAX_TEXT_LEN: int = 3500
    DAILY_CHALLENGE_MIN_XP: int = 10
    CERTIFICATE_THRESHOLD: int = 85
    REQUEST_COOLDOWN_SEC: int = 2


settings = Settings()


# ============================================================
# Logger
# ============================================================

class SensitiveFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        secrets = [settings.BOT_TOKEN, settings.GEMINI_API_KEY, settings.OPENAI_API_KEY]
        for sec in secrets:
            if sec and sec in msg:
                msg = msg.replace(sec, "***MASKED***")
        record.msg = msg
        record.args = ()
        return True


logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("math_tekshiruvchi_bot")
logger.addFilter(SensitiveFilter())


# ============================================================
# Helpers
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def today_utc() -> date:
    return utcnow().date()


def clamp_text(text: str, limit: int = 250) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def html_escape(text: Any) -> str:
    return html.escape(str(text if text is not None else ""))


def ensure_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def normalize_answer(value: str) -> str:
    if value is None:
        return ""
    value = str(value).strip().upper()
    value = value.replace("OPTION", "").replace("VARIANT", "")
    value = re.sub(r"[^A-Z0-9]", "", value)
    mapping = {
        "1": "A",
        "2": "B",
        "3": "C",
        "4": "D",
        "А": "A",
        "В": "B",
        "С": "C",
        "D": "D",
    }
    return mapping.get(value, value[:1])


def parse_answers_text(text: str) -> dict[int, str]:
    """
    Supports:
    1A 2B 3C
    1:A,2:B,3:C
    1)A 2)B
    """
    raw = (text or "").strip().upper()
    pairs = re.findall(r"(\d+)\s*[:\)\-\.\s]?\s*([A-D1-4])", raw)
    result: dict[int, str] = {}
    for qn, ans in pairs:
        result[int(qn)] = normalize_answer(ans)
    return result


def score_level_from_xp(xp: int) -> int:
    return max(1, (xp // 100) + 1)


def percent_color_label(percent: float) -> str:
    if percent >= 90:
        return "🔥 A’lo"
    if percent >= 75:
        return "✅ Yaxshi"
    if percent >= 50:
        return "🟡 O‘rtacha"
    return "⚠️ Kuchaytirish kerak"


def safe_json_loads(value: str | None, fallback: Any):
    try:
        if not value:
            return fallback
        return json.loads(value)
    except Exception:
        return fallback


def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def rate_limit_key(user_id: int) -> str:
    return f"{user_id}"


# ============================================================
# Database
# ============================================================

class Base(AsyncAttrs, DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("tg_id", name="uq_users_tg_id"),
        Index("ix_users_xp", "xp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    username: Mapped[str | None] = mapped_column(String(128), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    language: Mapped[str | None] = mapped_column(String(16), nullable=True)

    xp: Mapped[int] = mapped_column(Integer, default=0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    streak: Mapped[int] = mapped_column(Integer, default=0)
    best_streak: Mapped[int] = mapped_column(Integer, default=0)

    total_tests: Mapped[int] = mapped_column(Integer, default=0)
    total_correct: Mapped[int] = mapped_column(Integer, default=0)
    total_wrong: Mapped[int] = mapped_column(Integer, default=0)
    avg_percent: Mapped[float] = mapped_column(Integer, default=0)

    badge: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_active_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    results = relationship("Result", back_populates="user")


class Test(Base):
    __tablename__ = "tests"
    __table_args__ = (
        UniqueConstraint("code", name="uq_tests_code"),
        Index("ix_tests_category", "category"),
        Index("ix_tests_topic", "topic"),
        Index("ix_tests_difficulty", "difficulty"),
        Index("ix_tests_test_date", "test_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(128), default="Umumiy")
    topic: Mapped[str] = mapped_column(String(128), default="Umumiy")
    difficulty: Mapped[str] = mapped_column(String(32), default="Oson")
    test_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    questions_json: Mapped[str] = mapped_column(Text, nullable=False)
    pdf_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    results = relationship("Result", back_populates="test")


class Result(Base):
    __tablename__ = "results"
    __table_args__ = (
        UniqueConstraint("user_id", "test_id", name="uq_results_user_test"),
        Index("ix_results_user_id", "user_id"),
        Index("ix_results_test_id", "test_id"),
        Index("ix_results_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), nullable=False)

    correct_count: Mapped[int] = mapped_column(Integer, default=0)
    wrong_count: Mapped[int] = mapped_column(Integer, default=0)
    total_questions: Mapped[int] = mapped_column(Integer, default=0)
    percent: Mapped[float] = mapped_column(Integer, default=0)
    score: Mapped[int] = mapped_column(Integer, default=0)
    duration_sec: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(64), default="completed")
    answers_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    analysis_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    user = relationship("User", back_populates="results")
    test = relationship("Test", back_populates="results")


class Attempt(Base):
    __tablename__ = "attempts"
    __table_args__ = (
        UniqueConstraint("user_id", "test_id", name="uq_attempts_user_test"),
        Index("ix_attempts_user_id", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    raw_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    submitted_answers: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="started")


class MessageLog(Base):
    __tablename__ = "messages"
    __table_args__ = (Index("ix_messages_created_at", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    from_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    to_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    direction: Mapped[str] = mapped_column(String(16), default="user_to_admin")
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    tg_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AdminLog(Base):
    __tablename__ = "admin_logs"
    __table_args__ = (Index("ix_admin_logs_created_at", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    admin_tg_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Analytics(Base):
    __tablename__ = "analytics"
    __table_args__ = (
        Index("ix_analytics_name", "event_name"),
        Index("ix_analytics_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    event_name: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Certificate(Base):
    __tablename__ = "certificates"
    __table_args__ = (
        UniqueConstraint("user_id", "test_id", name="uq_certificates_user_test"),
        Index("ix_certificates_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), nullable=False)
    result_id: Mapped[int] = mapped_column(ForeignKey("results.id", ondelete="CASCADE"), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Badge(Base):
    __tablename__ = "badges"
    __table_args__ = (
        UniqueConstraint("user_id", "badge_code", name="uq_badges_user_code"),
        Index("ix_badges_user_id", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    badge_code: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(128), nullable=False)
    earned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Streak(Base):
    __tablename__ = "streaks"
    __table_args__ = (UniqueConstraint("user_id", name="uq_streaks_user_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    current: Mapped[int] = mapped_column(Integer, default=0)
    best: Mapped[int] = mapped_column(Integer, default=0)
    last_day: Mapped[date | None] = mapped_column(Date, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Favorite(Base):
    __tablename__ = "favorites"
    __table_args__ = (
        UniqueConstraint("user_id", "test_id", name="uq_favorites_user_test"),
        Index("ix_favorites_user_id", "user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (Index("ix_notifications_user_id", "user_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


# ============================================================
# DB Engine
# ============================================================

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ============================================================
# FSM States
# ============================================================

class ContactStates(StatesGroup):
    waiting_text = State()


class AIStates(StatesGroup):
    waiting_prompt = State()


class TestSearchStates(StatesGroup):
    waiting_query = State()
    waiting_filter = State()


class TestSolveStates(StatesGroup):
    waiting_code = State()
    waiting_answers = State()


class AdminStates(StatesGroup):
    waiting_test_json = State()
    waiting_broadcast = State()
    waiting_delete_code = State()


# ============================================================
# Global in-memory guards (lightweight anti-spam / duplicate click)
# ============================================================

RECENT_REQUESTS: dict[int, float] = {}
POLLED_TASK: asyncio.Task | None = None
SCHEDULER = AsyncIOScheduler(timezone="UTC")


def request_allowed(user_id: int) -> bool:
    now = datetime.now().timestamp()
    last = RECENT_REQUESTS.get(user_id, 0.0)
    if now - last < settings.REQUEST_COOLDOWN_SEC:
        return False
    RECENT_REQUESTS[user_id] = now
    return True


# ============================================================
# Telegram core
# ============================================================

if not settings.BOT_TOKEN:
    logger.warning("BOT_TOKEN bo‘sh. Bot ishga tushmaydi.")
bot = Bot(token=settings.BOT_TOKEN, parse_mode=ParseMode.HTML) if settings.BOT_TOKEN else None
dp = Dispatcher()
router = Router()
dp.include_router(router)


# ============================================================
# Keyboard helpers
# ============================================================

def nav_kb(back_cb: str = "menu:home", home_cb: str = "menu:home") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Orqaga", callback_data=back_cb)
    kb.button(text="🏠 Bosh menyu", callback_data=home_cb)
    kb.adjust(2)
    return kb.as_markup()


def main_menu_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    buttons = [
        ("📚 Testlar ro‘yxati", "menu:tests"),
        ("🧪 Test tekshirish", "menu:solve"),
        ("🤖 AI ustoz", "menu:ai"),
        ("📊 Natijalarim", "menu:results"),
        ("👤 Profilim", "menu:profile"),
        ("⭐ Favorites", "menu:favorites"),
        ("🏆 Reyting", "menu:leaderboard"),
        ("🎯 Daily challenge", "menu:daily"),
        ("💬 Bog‘lanish", "menu:contact"),
    ]
    if is_admin:
        buttons.append(("🛠 Admin panel", "menu:admin"))
    for text, data in buttons:
        kb.button(text=text, callback_data=data)
    kb.adjust(2, 2, 2, 2, 1)
    return kb.as_markup()


def tests_actions_kb(test_id: int, has_pdf: bool = False, is_fav: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🧾 Preview", callback_data=f"test:preview:{test_id}")
    if has_pdf:
        kb.button(text="📄 PDF", callback_data=f"test:pdf:{test_id}")
    kb.button(text="⭐ Saqlash" if not is_fav else "💛 Saqlangan", callback_data=f"fav:toggle:{test_id}")
    kb.button(text="✅ Tekshirish", callback_data="menu:solve")
    kb.button(text="⬅️ Orqaga", callback_data="menu:tests")
    kb.button(text="🏠 Bosh menyu", callback_data="menu:home")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


def paginated_tests_kb(page: int, total_pages: int, extra_prefix: str = "tests:page") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if page > 1:
        kb.button(text="◀️ Oldingi", callback_data=f"{extra_prefix}:{page-1}")
    if page < total_pages:
        kb.button(text="Keyingi ▶️", callback_data=f"{extra_prefix}:{page+1}")
    kb.button(text="🔎 Qidiruv", callback_data="tests:search")
    kb.button(text="🎚 Filtr", callback_data="tests:filter")
    kb.button(text="⬅️ Orqaga", callback_data="menu:home")
    kb.button(text="🏠 Bosh menyu", callback_data="menu:home")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


def back_home_kb(back_cb: str = "menu:home") -> InlineKeyboardMarkup:
    return nav_kb(back_cb=back_cb, home_cb="menu:home")


def simple_ok_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Bosh menyu", callback_data="menu:home")
    return kb.as_markup()


# ============================================================
# Text renderers
# ============================================================

def render_home_text(user: User | None = None) -> str:
    name = html_escape(user.first_name if user and user.first_name else "Foydalanuvchi")
    level = user.level if user else 1
    xp = user.xp if user else 0
    badge = html_escape(user.badge or "—")
    return (
        f"👋 <b>Salom, {name}!</b>\n\n"
        f"Bu yerda testlar, AI ustoz, natijalar, profil va admin panel bir joyda.\n\n"
        f"⭐ <b>Level:</b> {level}\n"
        f"⚡ <b>XP:</b> {xp}\n"
        f"🏅 <b>Badge:</b> {badge}\n"
    )


def render_test_card(test: Test) -> str:
    q_count = len(safe_json_loads(test.questions_json, []))
    test_date = test.test_date.isoformat() if test.test_date else "—"
    return (
        f"📘 <b>{html_escape(test.title)}</b>\n\n"
        f"🆔 <b>Kod:</b> <code>{html_escape(test.code)}</code>\n"
        f"🏷 <b>Kategoriya:</b> {html_escape(test.category)}\n"
        f"📚 <b>Mavzu:</b> {html_escape(test.topic)}\n"
        f"⭐ <b>Qiyinlik:</b> {html_escape(test.difficulty)}\n"
        f"📅 <b>Sana:</b> {html_escape(test_date)}\n"
        f"❓ <b>Savollar:</b> {q_count}\n\n"
        f"{html_escape(clamp_text(test.description or ''))}"
    )


def render_result_card(result: Result, test: Test) -> str:
    percent = float(result.percent or 0)
    return (
        f"📊 <b>Natija kartasi</b>\n\n"
        f"🧾 <b>Test:</b> {html_escape(test.title)}\n"
        f"✅ <b>To‘g‘ri:</b> {result.correct_count}\n"
        f"❌ <b>Noto‘g‘ri:</b> {result.wrong_count}\n"
        f"📈 <b>Foiz:</b> {percent:.1f}% — {percent_color_label(percent)}\n"
        f"🎯 <b>Ball:</b> {result.score}\n"
        f"⏱ <b>Vaqt:</b> {result.duration_sec} s\n"
        f"📌 <b>Status:</b> {html_escape(result.status)}\n"
    )


def render_profile_card(user: User, streak_best: int = 0) -> str:
    return (
        f"👤 <b>Profilim</b>\n\n"
        f"🆔 <b>User ID:</b> <code>{user.tg_id}</code>\n"
        f"👤 <b>Ism:</b> {html_escape(user.first_name or '—')}\n"
        f"🔖 <b>Username:</b> @{html_escape(user.username) if user.username else '—'}\n"
        f"⭐ <b>Level:</b> {user.level}\n"
        f"⚡ <b>XP:</b> {user.xp}\n"
        f"🏅 <b>Badge:</b> {html_escape(user.badge or '—')}\n"
        f"🔥 <b>Streak:</b> {user.streak}\n"
        f"🏆 <b>Best streak:</b> {streak_best}\n"
        f"📚 <b>Testlar:</b> {user.total_tests}\n"
        f"✅ <b>To‘g‘ri:</b> {user.total_correct}\n"
        f"❌ <b>Xato:</b> {user.total_wrong}\n"
        f"📈 <b>O‘rtacha:</b> {float(user.avg_percent or 0):.1f}%\n"
    )


def render_help_text() -> str:
    return (
        "🤖 <b>AI ustoz</b>\n\n"
        "Savolni yubor: men o‘zbek tilida bosqichma-bosqich tushuntiraman.\n"
        "Rasm yoki ovoz yuborsang ham tahlil qilishga urinaman.\n\n"
        "Misol:\n"
        "• 12x + 4 = 28\n"
        "• bu masalani tushuntir\n"
    )


# ============================================================
# PDF helpers
# ============================================================

def build_certificate_pdf(student_name: str, test_title: str, percent: float, issued_at: datetime) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    c.setTitle("Certificate")
    c.setFont("Helvetica-Bold", 22)
    c.drawString(70, height - 80, "CERTIFICATE")
    c.setFont("Helvetica", 13)
    c.drawString(70, height - 120, f"Name: {student_name}")
    c.drawString(70, height - 145, f"Test: {test_title}")
    c.drawString(70, height - 170, f"Result: {percent:.1f}%")
    c.drawString(70, height - 195, f"Date: {issued_at.strftime('%Y-%m-%d %H:%M UTC')}")
    c.drawString(70, height - 235, "Congratulations on completing the test.")
    c.showPage()
    c.save()
    return buffer.getvalue()


def build_test_preview_pdf(test: Test) -> bytes:
    questions = safe_json_loads(test.questions_json, [])
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    c.setTitle(test.title)
    y = height - 60
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, test.title)
    y -= 25
    c.setFont("Helvetica", 11)
    c.drawString(50, y, f"Code: {test.code}")
    y -= 20
    c.drawString(50, y, f"Category: {test.category} | Topic: {test.topic} | Difficulty: {test.difficulty}")
    y -= 30
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Questions preview:")
    y -= 20
    c.setFont("Helvetica", 10)
    for i, q in enumerate(questions[:12], start=1):
        line = f"{i}. {q.get('q', '')}"
        c.drawString(50, y, line[:105])
        y -= 16
        if y < 70:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)
    c.showPage()
    c.save()
    return buffer.getvalue()


# ============================================================
# AI service
# ============================================================

class AIService:
    def __init__(self) -> None:
        self.openai_key = settings.OPENAI_API_KEY.strip()
        self.gemini_key = settings.GEMINI_API_KEY.strip()

    async def explain_text(self, prompt: str, user_hint: str = "") -> str:
        prompt = (prompt or "").strip()
        if not prompt:
            return "Savol bo‘sh. Qayta yuboring."

        system = (
            "Sen o‘zbek tilida tushuntiradigan matematik ustozsan. "
            "Javobni sodda, bosqichma-bosqich, aniq va muloyim yoz. "
            "Agar savol noaniq bo‘lsa, mantiqan eng ehtimoliy talqin bilan yech."
        )
        combined = f"{system}\n\nSavol:\n{prompt}\n\nQo‘shimcha:\n{user_hint}".strip()

        if self.openai_key:
            try:
                async with httpx.AsyncClient(timeout=45) as client:
                    resp = await client.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {self.openai_key}"},
                        json={
                            "model": "gpt-4o-mini",
                            "messages": [
                                {"role": "system", "content": system},
                                {"role": "user", "content": combined},
                            ],
                            "temperature": 0.2,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return data["choices"][0]["message"]["content"].strip()
            except Exception as e:
                logger.exception("OpenAI explain failed: %s", e)

        if self.gemini_key:
            try:
                url = (
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"gemini-1.5-flash:generateContent?key={self.gemini_key}"
                )
                async with httpx.AsyncClient(timeout=45) as client:
                    resp = await client.post(
                        url,
                        json={
                            "contents": [
                                {"parts": [{"text": combined}]}
                            ],
                            "generationConfig": {
                                "temperature": 0.2,
                                "maxOutputTokens": 1024,
                            },
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        if parts:
                            return parts[0].get("text", "").strip()
            except Exception as e:
                logger.exception("Gemini explain failed: %s", e)

        return (
            "🤖 AI hozircha ulanmadi, lekin qisqa yordam:\n\n"
            f"Savol: {prompt}\n\n"
            "Yechimni bosqichlarga bo‘lib yozing, noma’lumlarni ajrating, "
            "so‘ng tenglamani soddalashtiring."
        )

    async def analyze_image(self, image_bytes: bytes, prompt: str) -> str:
        if not image_bytes:
            return "Rasm topilmadi."
        if self.openai_key:
            try:
                b64 = __import__("base64").b64encode(image_bytes).decode("utf-8")
                async with httpx.AsyncClient(timeout=60) as client:
                    resp = await client.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {self.openai_key}"},
                        json={
                            "model": "gpt-4o-mini",
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "Sen o‘zbek tilida rasm, formula va matematika masalalarini tahlil qilasan.",
                                },
                                {
                                    "role": "user",
                                    "content": [
                                        {"type": "text", "text": prompt or "Rasmni tahlil qil."},
                                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                                    ],
                                },
                            ],
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return data["choices"][0]["message"]["content"].strip()
            except Exception as e:
                logger.exception("OpenAI image analysis failed: %s", e)

        return "Rasm tahlili uchun AI tayyor emas. Matn bilan yuboring yoki keyinroq qayta urinib ko‘ring."

    async def transcribe_voice(self, audio_bytes: bytes, filename: str = "voice.ogg") -> str:
        if not audio_bytes:
            return ""
        if self.openai_key:
            try:
                form = httpx.MultipartData(
                    {
                        "model": "whisper-1",
                        "file": (filename, audio_bytes, "application/octet-stream"),
                    }
                )
                async with httpx.AsyncClient(timeout=60) as client:
                    resp = await client.post(
                        "https://api.openai.com/v1/audio/transcriptions",
                        headers={"Authorization": f"Bearer {self.openai_key}"},
                        files={"file": (filename, audio_bytes, "application/octet-stream")},
                        data={"model": "whisper-1"},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return data.get("text", "").strip()
            except Exception as e:
                logger.exception("Voice transcription failed: %s", e)
        return ""

    async def multimodal_answer(
        self,
        text: str = "",
        image_bytes: bytes | None = None,
        voice_bytes: bytes | None = None,
        voice_name: str = "voice.ogg",
    ) -> str:
        if voice_bytes:
            transcript = await self.transcribe_voice(voice_bytes, voice_name)
            if transcript:
                text = f"{text}\n\n[Ovozli xabar matni]: {transcript}".strip()

        if image_bytes:
            return await self.analyze_image(image_bytes, text or "Rasmni tahlil qil.")

        return await self.explain_text(text)


ai_service = AIService()


# ============================================================
# DB service helpers
# ============================================================

async def get_or_create_user(session: AsyncSession, tg_user) -> User:
    stmt = select(User).where(User.tg_id == tg_user.id)
    res = await session.execute(stmt)
    user = res.scalar_one_or_none()
    if user:
        user.username = tg_user.username
        user.first_name = tg_user.first_name
        user.last_active_at = utcnow()
        return user

    user = User(
        tg_id=tg_user.id,
        username=tg_user.username,
        first_name=tg_user.first_name,
        language=getattr(tg_user, "language_code", None),
        xp=0,
        level=1,
        streak=0,
        best_streak=0,
        total_tests=0,
        total_correct=0,
        total_wrong=0,
        avg_percent=0,
        badge="Newbie",
        last_active_at=utcnow(),
    )
    session.add(user)
    await session.flush()
    return user


async def log_event(session: AsyncSession, user_id: int | None, event_name: str, payload: dict | None = None) -> None:
    session.add(
        Analytics(
            user_id=user_id,
            event_name=event_name,
            payload_json=safe_json_dumps(payload or {}),
        )
    )


async def log_admin_action(session: AsyncSession, admin_tg_id: int, action: str, details: str | None = None) -> None:
    session.add(AdminLog(admin_tg_id=admin_tg_id, action=action, details=details))


async def save_message_log(
    session: AsyncSession,
    from_user_id: int | None,
    to_user_id: int | None,
    direction: str,
    text: str | None,
    tg_message_id: int | None = None,
) -> None:
    session.add(
        MessageLog(
            from_user_id=from_user_id,
            to_user_id=to_user_id,
            direction=direction,
            text=text,
            tg_message_id=tg_message_id,
        )
    )


async def ensure_streak_row(session: AsyncSession, user_id: int) -> Streak:
    res = await session.execute(select(Streak).where(Streak.user_id == user_id))
    streak = res.scalar_one_or_none()
    if streak:
        return streak
    streak = Streak(user_id=user_id, current=0, best=0, last_day=None)
    session.add(streak)
    await session.flush()
    return streak


def calculate_xp(percent: float, correct_count: int, total_questions: int) -> int:
    base = 10 + correct_count * 4
    bonus = int(percent // 10) * 2
    speed_bonus = 5 if total_questions and correct_count == total_questions else 0
    return max(settings.DAILY_CHALLENGE_MIN_XP, base + bonus + speed_bonus)


def choose_badge(total_tests: int, avg_percent: float, streak: int) -> str:
    if avg_percent >= 95 and total_tests >= 10:
        return "Math Legend"
    if avg_percent >= 90:
        return "Top Solver"
    if streak >= 7:
        return "Streak Master"
    if total_tests >= 5:
        return "Active Learner"
    return "Newbie"


async def update_user_stats(
    session: AsyncSession,
    user: User,
    percent: float,
    correct: int,
    wrong: int,
) -> None:
    user.total_tests += 1
    user.total_correct += correct
    user.total_wrong += wrong
    user.xp += calculate_xp(percent, correct, max(1, correct + wrong))
    user.level = score_level_from_xp(user.xp)
    total = user.total_tests
    if total > 0:
        user.avg_percent = ((user.avg_percent * (total - 1)) + percent) / total
    user.badge = choose_badge(user.total_tests, float(user.avg_percent or 0), user.streak)


async def update_streak(session: AsyncSession, user: User) -> Streak:
    streak = await ensure_streak_row(session, user.id)
    today = today_utc()
    yesterday = today - timedelta(days=1)

    if streak.last_day == today:
        return streak

    if streak.last_day == yesterday:
        streak.current += 1
    else:
        streak.current = 1

    streak.best = max(streak.best, streak.current)
    streak.last_day = today
    streak.updated_at = utcnow()
    user.streak = streak.current
    user.best_streak = streak.best
    return streak


async def unlock_badges(session: AsyncSession, user: User) -> list[str]:
    unlocked = []
    rules = [
        ("first_win", "First Win", user.total_tests >= 1),
        ("streak_3", "Streak 3", user.streak >= 3),
        ("streak_7", "Streak 7", user.streak >= 7),
        ("accuracy_90", "Accuracy 90+", float(user.avg_percent or 0) >= 90),
        ("accuracy_95", "Accuracy 95+", float(user.avg_percent or 0) >= 95),
    ]
    for code, title, ok in rules:
        if not ok:
            continue
        exists = await session.execute(
            select(Badge).where(Badge.user_id == user.id, Badge.badge_code == code)
        )
        if exists.scalar_one_or_none():
            continue
        session.add(Badge(user_id=user.id, badge_code=code, title=title))
        unlocked.append(title)
    return unlocked


async def create_certificate_if_needed(session: AsyncSession, user: User, test: Test, result: Result) -> str | None:
    if float(result.percent or 0) < settings.CERTIFICATE_THRESHOLD:
        return None

    existing = await session.execute(
        select(Certificate).where(
            Certificate.user_id == user.id,
            Certificate.test_id == test.id,
        )
    )
    if existing.scalar_one_or_none():
        return None

    out_dir = Path("certificates")
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"cert_{user.tg_id}_{test.code}_{result.id}.pdf"

    pdf_bytes = build_certificate_pdf(
        student_name=user.first_name or f"User {user.tg_id}",
        test_title=test.title,
        percent=float(result.percent or 0),
        issued_at=utcnow(),
    )
    file_path.write_bytes(pdf_bytes)

    session.add(
        Certificate(
            user_id=user.id,
            test_id=test.id,
            result_id=result.id,
            file_path=str(file_path),
        )
    )
    return str(file_path)


async def seed_demo_tests(session: AsyncSession) -> None:
    cnt = await session.scalar(select(func.count(Test.id)))
    if cnt and cnt > 0:
        return

    demo = [
        Test(
            code="MT-001",
            title="Arifmetika asoslari",
            category="Matematika",
            topic="Sonlar",
            difficulty="Oson",
            test_date=today_utc(),
            description="Oddiy arifmetik savollar.",
            questions_json=safe_json_dumps([
                {"q": "2 + 2 nechiga teng?", "options": ["3", "4", "5", "6"], "answer": "B", "explanation": "2+2=4"},
                {"q": "10 - 3 nechiga teng?", "options": ["5", "6", "7", "8"], "answer": "C", "explanation": "10-3=7"},
                {"q": "3 × 3 nechiga teng?", "options": ["6", "8", "9", "12"], "answer": "C", "explanation": "3×3=9"},
            ]),
            pdf_url=None,
            active=True,
        ),
        Test(
            code="MT-002",
            title="Kasrlar va foizlar",
            category="Matematika",
            topic="Kasr",
            difficulty="O‘rtacha",
            test_date=today_utc(),
            description="Kasr va foiz bo‘yicha test.",
            questions_json=safe_json_dumps([
                {"q": "1/2 ning foizi nechchi?", "options": ["25%", "50%", "75%", "100%"], "answer": "B", "explanation": "1/2 = 50%"},
                {"q": "25% bu nechanchi qism?", "options": ["1/2", "1/3", "1/4", "1/5"], "answer": "C", "explanation": "25% = 1/4"},
            ]),
            active=True,
        ),
    ]
    session.add_all(demo)
    await session.flush()


async def get_tests(
    session: AsyncSession,
    page: int = 1,
    per_page: int = 5,
    search: str = "",
    category: str = "",
    topic: str = "",
    difficulty: str = "",
) -> tuple[list[Test], int]:
    stmt = select(Test).where(Test.active == True)  # noqa: E712
    if search:
        like = f"%{search.strip()}%"
        stmt = stmt.where(
            and_(
                func.lower(Test.title).like(like.lower()) | func.lower(Test.code).like(like.lower())
            )
        )
    if category:
        stmt = stmt.where(func.lower(Test.category) == category.lower())
    if topic:
        stmt = stmt.where(func.lower(Test.topic) == topic.lower())
    if difficulty:
        stmt = stmt.where(func.lower(Test.difficulty) == difficulty.lower())

    total = await session.scalar(select(func.count()).select_from(stmt.subquery()))
    total = int(total or 0)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    stmt = stmt.order_by(desc(Test.created_at)).offset((page - 1) * per_page).limit(per_page)
    rows = (await session.execute(stmt)).scalars().all()
    return list(rows), pages


async def get_user_by_tg_id(session: AsyncSession, tg_id: int) -> User | None:
    res = await session.execute(select(User).where(User.tg_id == tg_id))
    return res.scalar_one_or_none()


async def get_test_by_code(session: AsyncSession, code: str) -> Test | None:
    res = await session.execute(select(Test).where(func.lower(Test.code) == code.lower().strip()))
    return res.scalar_one_or_none()


async def get_test_by_id(session: AsyncSession, test_id: int) -> Test | None:
    res = await session.execute(select(Test).where(Test.id == test_id))
    return res.scalar_one_or_none()


async def get_result(session: AsyncSession, user_id: int, test_id: int) -> Result | None:
    res = await session.execute(select(Result).where(Result.user_id == user_id, Result.test_id == test_id))
    return res.scalar_one_or_none()


async def count_favorites(session: AsyncSession, user_id: int, test_id: int) -> bool:
    res = await session.execute(select(Favorite).where(Favorite.user_id == user_id, Favorite.test_id == test_id))
    return res.scalar_one_or_none() is not None


def score_test(test: Test, answers: dict[int, str]) -> dict[str, Any]:
    questions = safe_json_loads(test.questions_json, [])
    details = []
    correct = 0
    wrong = 0

    for idx, q in enumerate(questions, start=1):
        user_ans = normalize_answer(answers.get(idx, ""))
        right_ans = normalize_answer(q.get("answer", ""))
        ok = user_ans == right_ans and right_ans != ""
        if ok:
            correct += 1
        else:
            wrong += 1
        details.append(
            {
                "no": idx,
                "question": q.get("q", ""),
                "user_answer": user_ans or "—",
                "correct_answer": right_ans or "—",
                "explanation": q.get("explanation", ""),
                "is_correct": ok,
            }
        )

    total = max(1, len(questions))
    percent = (correct / total) * 100.0
    score = int(round(percent))
    return {
        "correct": correct,
        "wrong": wrong,
        "total": total,
        "percent": percent,
        "score": score,
        "details": details,
    }


def build_review_text(details: list[dict[str, Any]]) -> str:
    lines = ["🧾 <b>Quiz review mode</b>\n"]
    for d in details[:30]:
        lines.append(
            f"{d['no']}. {html_escape(d['question'])}\n"
            f"   Sening javob: <b>{html_escape(d['user_answer'])}</b>\n"
            f"   To‘g‘ri javob: <b>{html_escape(d['correct_answer'])}</b>\n"
            f"   Sabab: {html_escape(d['explanation'] or '—')}\n"
        )
    return "\n".join(lines)


# ============================================================
# AI / utility download
# ============================================================

async def download_telegram_file_to_bytes(bot: Bot, file_id: str) -> bytes:
    tg_file: File = await bot.get_file(file_id)
    buf = io.BytesIO()
    await bot.download_file(tg_file.file_path, destination=buf)
    return buf.getvalue()


def is_admin_tg(user_id: int) -> bool:
    return settings.ADMIN_ID and user_id == settings.ADMIN_ID


def guard(handler):
    @wraps(handler)
    async def wrapper(*args, **kwargs):
        try:
            return await handler(*args, **kwargs)
        except Exception as e:
            logger.exception("Handler error: %s", e)
            event = args[0] if args else None
            if isinstance(event, Message):
                await event.answer(
                    "⚠️ Ichki xatolik yuz berdi. Bot ishlashda davom etadi. Keyinroq qayta urinib ko‘ring.",
                    reply_markup=simple_ok_kb(),
                )
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer("Xatolik yuz berdi", show_alert=False)
                except Exception:
                    pass
                if event.message:
                    await event.message.answer(
                        "⚠️ Ichki xatolik yuz berdi. Bot ishlashda davom etadi.",
                        reply_markup=simple_ok_kb(),
                    )
            return None

    return wrapper
  # ============================================================
# main.py
# Part 2/2
# ============================================================

# ============================================================
# App
# ============================================================

app = FastAPI(title="math_tekshiruvchi_bot", version="1.0.0")
scheduler = AsyncIOScheduler(timezone="UTC")


@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <html>
      <head><title>math_tekshiruvchi_bot</title></head>
      <body style="font-family: Arial, sans-serif; padding: 24px;">
        <h2>math_tekshiruvchi_bot is running</h2>
        <p>Health: <a href="/healthz">/healthz</a></p>
      </body>
    </html>
    """


@app.get("/healthz", response_class=JSONResponse)
async def healthz():
    return {
        "ok": True,
        "service": "math_tekshiruvchi_bot",
        "env": settings.APP_ENV,
        "time": utcnow().isoformat(),
    }


@app.post(settings.WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if not bot:
        return JSONResponse({"ok": False, "error": "BOT_TOKEN not configured"}, status_code=503)
    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return JSONResponse({"ok": True})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled app exception: %s", exc)
    return PlainTextResponse("Internal server error", status_code=500)


# ============================================================
# Session helper
# ============================================================

async def with_session(fn, *args, **kwargs):
    async with SessionLocal() as session:
        try:
            result = await fn(session, *args, **kwargs)
            await session.commit()
            return result
        except Exception:
            await session.rollback()
            raise


# ============================================================
# Menu / common renderers
# ============================================================

async def show_home(message_or_cb, session: AsyncSession):
    user_tg = message_or_cb.from_user
    user = await get_or_create_user(session, user_tg)
    await log_event(session, user.id, "home_opened", {})
    text = render_home_text(user)
    kb = main_menu_kb(is_admin=is_admin_tg(user.tg_id))
    if isinstance(message_or_cb, Message):
        await message_or_cb.answer(text, reply_markup=kb)
    else:
        await message_or_cb.message.edit_text(text, reply_markup=kb)
        await message_or_cb.answer()


async def answer_user(event, text: str, kb: InlineKeyboardMarkup | None = None):
    if isinstance(event, Message):
        return await event.answer(text, reply_markup=kb)
    if isinstance(event, CallbackQuery):
        if event.message:
            return await event.message.edit_text(text, reply_markup=kb)
        return await event.answer(text)
    return None


# ============================================================
# Command handlers
# ============================================================

@router.message(CommandStart())
@guard
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    async with SessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        await log_event(session, user.id, "start", {})
        await session.commit()
    await message.answer(
        render_home_text(user),
        reply_markup=main_menu_kb(is_admin=is_admin_tg(message.from_user.id)),
    )


@router.message(Command("menu"))
@guard
async def cmd_menu(message: Message, state: FSMContext):
    await state.clear()
    async with SessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        await session.commit()
    await message.answer(
        render_home_text(user),
        reply_markup=main_menu_kb(is_admin=is_admin_tg(message.from_user.id)),
    )


@router.message(Command("help"))
@guard
async def cmd_help(message: Message):
    await message.answer(render_help_text(), reply_markup=back_home_kb())


# ============================================================
# Callback navigation
# ============================================================

@router.callback_query(F.data == "menu:home")
@guard
async def cb_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    async with SessionLocal() as session:
        user = await get_or_create_user(session, call.from_user)
        await session.commit()
        await call.message.edit_text(
            render_home_text(user),
            reply_markup=main_menu_kb(is_admin=is_admin_tg(call.from_user.id)),
        )
        await call.answer()


@router.callback_query(F.data == "menu:tests")
@guard
async def cb_tests(call: CallbackQuery, state: FSMContext):
    async with SessionLocal() as session:
        await get_or_create_user(session, call.from_user)
        data = await state.get_data()
        page = int(data.get("tests_page", 1))
        search = data.get("tests_search", "")
        category = data.get("tests_category", "")
        topic = data.get("tests_topic", "")
        difficulty = data.get("tests_difficulty", "")
        tests, pages = await get_tests(session, page, 5, search, category, topic, difficulty)
        await log_event(session, call.from_user.id, "tests_opened", {"page": page})
        await session.commit()

    if not tests:
        await call.message.edit_text(
            "📭 Test topilmadi.\n\nQidiruv yoki filtrni o‘zgartiring.",
            reply_markup=paginated_tests_kb(1, 1),
        )
        await call.answer()
        return

    lines = [
        "📚 <b>Testlar ro‘yxati</b>\n",
        f"📄 Sahifa: <b>{page}/{pages}</b>\n",
    ]
    for t in tests:
        q_count = len(safe_json_loads(t.questions_json, []))
        lines.append(
            f"• <b>{html_escape(t.code)}</b> — {html_escape(t.title)}\n"
            f"  {html_escape(t.category)} | {html_escape(t.topic)} | {html_escape(t.difficulty)} | {q_count} savol\n"
        )
    await call.message.edit_text(
        "\n".join(lines),
        reply_markup=paginated_tests_kb(page, pages),
    )
    await call.answer()


@router.callback_query(F.data.startswith("tests:page:"))
@guard
async def cb_tests_page(call: CallbackQuery, state: FSMContext):
    page = int(call.data.split(":")[-1])
    async with SessionLocal() as session:
        data = await state.get_data()
        search = data.get("tests_search", "")
        category = data.get("tests_category", "")
        topic = data.get("tests_topic", "")
        difficulty = data.get("tests_difficulty", "")
        tests, pages = await get_tests(session, page, 5, search, category, topic, difficulty)
        await state.update_data(tests_page=page)
        await session.commit()

    if not tests:
        await call.answer("Test topilmadi", show_alert=False)
        return

    lines = [
        "📚 <b>Testlar ro‘yxati</b>\n",
        f"📄 Sahifa: <b>{page}/{pages}</b>\n",
    ]
    for t in tests:
        q_count = len(safe_json_loads(t.questions_json, []))
        lines.append(
            f"• <b>{html_escape(t.code)}</b> — {html_escape(t.title)}\n"
            f"  {html_escape(t.category)} | {html_escape(t.topic)} | {html_escape(t.difficulty)} | {q_count} savol\n"
        )

    await call.message.edit_text(
        "\n".join(lines),
        reply_markup=paginated_tests_kb(page, pages),
    )
    await call.answer()


@router.callback_query(F.data == "tests:search")
@guard
async def cb_tests_search(call: CallbackQuery, state: FSMContext):
    await state.set_state(TestSearchStates.waiting_query)
    await call.message.edit_text(
        "🔎 Qidiruv matnini yuboring.\n\nMasalan: <code>algebra</code>",
        reply_markup=back_home_kb("menu:tests"),
    )
    await call.answer()


@router.callback_query(F.data == "tests:filter")
@guard
async def cb_tests_filter(call: CallbackQuery, state: FSMContext):
    await state.set_state(TestSearchStates.waiting_filter)
    await call.message.edit_text(
        "🎚 Filtr yuboring.\n\nFormat:\n"
        "<code>category=Matematika; topic=Kasr; difficulty=O‘rtacha</code>",
        reply_markup=back_home_kb("menu:tests"),
    )
    await call.answer()


@router.callback_query(F.data.startswith("test:preview:"))
@guard
async def cb_test_preview(call: CallbackQuery):
    test_id = int(call.data.split(":")[-1])
    async with SessionLocal() as session:
        test = await get_test_by_id(session, test_id)
        fav = await count_favorites(session, call.from_user.id, test_id)
        await session.commit()

    if not test:
        await call.answer("Test topilmadi", show_alert=True)
        return

    await call.message.answer(
        render_test_card(test),
        reply_markup=tests_actions_kb(test.id, has_pdf=True, is_fav=fav),
    )
    await call.answer()


@router.callback_query(F.data.startswith("test:pdf:"))
@guard
async def cb_test_pdf(call: CallbackQuery):
    test_id = int(call.data.split(":")[-1])
    async with SessionLocal() as session:
        test = await get_test_by_id(session, test_id)
        await session.commit()

    if not test:
        await call.answer("Test topilmadi", show_alert=True)
        return

    pdf_bytes = build_test_preview_pdf(test)
    file = BufferedInputFile(pdf_bytes, filename=f"{test.code}.pdf")
    await call.message.answer_document(file, caption=f"📄 {test.title} — PDF preview")
    await call.answer()


@router.callback_query(F.data.startswith("fav:toggle:"))
@guard
async def cb_fav_toggle(call: CallbackQuery):
    test_id = int(call.data.split(":")[-1])
    async with SessionLocal() as session:
        user = await get_or_create_user(session, call.from_user)
        test = await get_test_by_id(session, test_id)
        if not test:
            await call.answer("Test topilmadi", show_alert=True)
            return

        exists = await session.execute(
            select(Favorite).where(Favorite.user_id == user.id, Favorite.test_id == test.id)
        )
        fav = exists.scalar_one_or_none()
        if fav:
            await session.delete(fav)
            msg = "⭐ Favorites’dan olib tashlandi."
        else:
            session.add(Favorite(user_id=user.id, test_id=test.id))
            msg = "⭐ Favorites’ga qo‘shildi."
        await session.commit()

    await call.answer(msg, show_alert=False)


@router.callback_query(F.data == "menu:solve")
@guard
async def cb_solve(call: CallbackQuery, state: FSMContext):
    await state.set_state(TestSolveStates.waiting_code)
    await call.message.edit_text(
        "🧪 Test kodini yuboring.\n\nMasalan: <code>MT-001</code>",
        reply_markup=back_home_kb("menu:home"),
    )
    await call.answer()


@router.callback_query(F.data == "menu:ai")
@guard
async def cb_ai(call: CallbackQuery, state: FSMContext):
    await state.set_state(AIStates.waiting_prompt)
    await call.message.edit_text(
        render_help_text(),
        reply_markup=back_home_kb("menu:home"),
    )
    await call.answer()


@router.callback_query(F.data == "menu:results")
@guard
async def cb_results(call: CallbackQuery):
    async with SessionLocal() as session:
        user = await get_or_create_user(session, call.from_user)
        results = (
            await session.execute(
                select(Result)
                .where(Result.user_id == user.id)
                .order_by(desc(Result.created_at))
                .limit(10)
            )
        ).scalars().all()

        if not results:
            text = "📭 Hozircha natija yo‘q."
        else:
            avg = float(user.avg_percent or 0)
            best = max([float(r.percent or 0) for r in results], default=0)
            last = float(results[0].percent or 0)
            text = (
                f"📊 <b>Natijalarim</b>\n\n"
                f"📈 O‘rtacha natija: <b>{avg:.1f}%</b>\n"
                f"🏆 Eng yaxshi natija: <b>{best:.1f}%</b>\n"
                f"🕒 Oxirgi natija: <b>{last:.1f}%</b>\n"
                f"🔁 Ishlangan testlar: <b>{len(results)}</b>\n\n"
            )
            for r in results:
                t = await get_test_by_id(session, r.test_id)
                text += (
                    f"• {html_escape(t.code if t else '—')} — {html_escape(t.title if t else '—')} : "
                    f"<b>{float(r.percent or 0):.1f}%</b> ({r.correct_count}/{r.total_questions})\n"
                )

        await session.commit()

    await call.message.edit_text(text, reply_markup=main_menu_kb(is_admin=is_admin_tg(call.from_user.id)))
    await call.answer()


@router.callback_query(F.data == "menu:profile")
@guard
async def cb_profile(call: CallbackQuery):
    async with SessionLocal() as session:
        user = await get_or_create_user(session, call.from_user)
        streak = await ensure_streak_row(session, user.id)
        await session.commit()
    await call.message.edit_text(
        render_profile_card(user, streak.best),
        reply_markup=main_menu_kb(is_admin=is_admin_tg(call.from_user.id)),
    )
    await call.answer()


@router.callback_query(F.data == "menu:favorites")
@guard
async def cb_favorites(call: CallbackQuery):
    async with SessionLocal() as session:
        user = await get_or_create_user(session, call.from_user)
        favs = (
            await session.execute(
                select(Favorite).where(Favorite.user_id == user.id).order_by(desc(Favorite.created_at))
            )
        ).scalars().all()

        if not favs:
            text = "⭐ Favorites bo‘sh."
        else:
            lines = ["⭐ <b>Favorites</b>\n"]
            for fav in favs[:20]:
                t = await get_test_by_id(session, fav.test_id)
                if t:
                    lines.append(f"• <b>{html_escape(t.code)}</b> — {html_escape(t.title)}")
            text = "\n".join(lines)
        await session.commit()

    await call.message.edit_text(text, reply_markup=main_menu_kb(is_admin=is_admin_tg(call.from_user.id)))
    await call.answer()


@router.callback_query(F.data == "menu:leaderboard")
@guard
async def cb_leaderboard(call: CallbackQuery):
    async with SessionLocal() as session:
        top = (
            await session.execute(
                select(User).order_by(desc(User.xp)).limit(10)
            )
        ).scalars().all()
        await session.commit()

    lines = ["🏆 <b>Global leaderboard</b>\n"]
    for i, u in enumerate(top, start=1):
        lines.append(
            f"{i}. {html_escape(u.first_name or '—')} — <b>{u.xp}</b> XP | {u.level} lvl | {float(u.avg_percent or 0):.1f}%"
        )
    await call.message.edit_text("\n".join(lines), reply_markup=main_menu_kb(is_admin=is_admin_tg(call.from_user.id)))
    await call.answer()


@router.callback_query(F.data == "menu:daily")
@guard
async def cb_daily(call: CallbackQuery):
    async with SessionLocal() as session:
        tests = (await session.execute(select(Test).where(Test.active == True).order_by(desc(Test.created_at)).limit(3))).scalars().all()  # noqa: E712
        if not tests:
            text = "📭 Daily challenge uchun test yo‘q."
        else:
            t = tests[0]
            text = (
                "🎯 <b>Daily challenge</b>\n\n"
                f"Bugungi mini-test: <b>{html_escape(t.title)}</b>\n"
                f"Kod: <code>{html_escape(t.code)}</code>\n\n"
                f"Tez va to‘g‘ri ishlasang XP bonus olasan."
            )
        await session.commit()
    await call.message.edit_text(text, reply_markup=main_menu_kb(is_admin=is_admin_tg(call.from_user.id)))
    await call.answer()


@router.callback_query(F.data == "menu:contact")
@guard
async def cb_contact(call: CallbackQuery, state: FSMContext):
    await state.set_state(ContactStates.waiting_text)
    await call.message.edit_text(
        "💬 Adminga yozmoqchi bo‘lgan xabarni yuboring.\n\nXabar log qilinadi va adminga yetkaziladi.",
        reply_markup=back_home_kb("menu:home"),
    )
    await call.answer()


@router.callback_query(F.data == "menu:admin")
@guard
async def cb_admin(call: CallbackQuery):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Test qo‘shish", callback_data="admin:addtest")
    kb.button(text="🗑 Test o‘chirish", callback_data="admin:deltest")
    kb.button(text="📣 Ommaviy xabar", callback_data="admin:broadcast")
    kb.button(text="📊 Statistikalar", callback_data="admin:stats")
    kb.button(text="👥 Foydalanuvchilar", callback_data="admin:users")
    kb.button(text="🧾 Loglar", callback_data="admin:logs")
    kb.button(text="🏠 Bosh menyu", callback_data="menu:home")
    kb.adjust(2, 2, 2, 1)

    await call.message.edit_text(
        "🛠 <b>Admin panel</b>\n\nBoshqaruv bo‘limlari:",
        reply_markup=kb.as_markup(),
    )
    await call.answer()


# ============================================================
# Search / filter input handlers
# ============================================================

@router.message(TestSearchStates.waiting_query)
@guard
async def tests_search_input(message: Message, state: FSMContext):
    query = (message.text or "").strip()
    async with SessionLocal() as session:
        await get_or_create_user(session, message.from_user)
        await state.update_data(tests_search=query, tests_page=1)
        await session.commit()
    await state.clear()
    await message.answer(f"🔎 Qidiruv saqlandi: <b>{html_escape(query)}</b>")
    await cb_tests(CallbackQuery(id="tmp", from_user=message.from_user, chat_instance="x", message=message, data="menu:tests"))


@router.message(TestSearchStates.waiting_filter)
@guard
async def tests_filter_input(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    data: dict[str, str] = {"tests_page": 1}
    for part in parts:
        if "=" in part:
            k, v = [x.strip() for x in part.split("=", 1)]
            k = k.lower()
            if k in {"category", "topic", "difficulty"}:
                data[f"tests_{k}"] = v

    async with SessionLocal() as session:
        await get_or_create_user(session, message.from_user)
        await state.update_data(**data)
        await session.commit()
    await state.clear()
    await message.answer("🎚 Filtr saqlandi.")
    await cb_tests(CallbackQuery(id="tmp", from_user=message.from_user, chat_instance="x", message=message, data="menu:tests"))


# ============================================================
# Test solving flow
# ============================================================

@router.message(TestSolveStates.waiting_code)
@guard
async def test_code_input(message: Message, state: FSMContext):
    code = (message.text or "").strip()
    if not code:
        await message.answer("Kod bo‘sh. Qayta yuboring.")
        return

    async with SessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        test = await get_test_by_code(session, code)
        if not test:
            await session.commit()
            await message.answer("❌ Test topilmadi. Kodni tekshiring.", reply_markup=back_home_kb())
            return

        existing = await get_result(session, user.id, test.id)
        if existing:
            await session.commit()
            await message.answer(
                "ℹ️ Bu test siz tomonidan allaqachon ishlangan.\n"
                "Qayta topshirish ruxsat etilmagan.",
                reply_markup=back_home_kb(),
            )
            return

        attempt = Attempt(
            user_id=user.id,
            test_id=test.id,
            raw_code=code,
            started_at=utcnow(),
            status="waiting_answers",
        )
        session.add(attempt)
        await session.commit()

    await state.update_data(test_code=code)
    await state.set_state(TestSolveStates.waiting_answers)
    await message.answer(
        "Endi javoblaringizni yuboring.\n\nFormat:\n<code>1A 2B 3C</code>\n"
        "yoki\n<code>1:A 2:B 3:C</code>",
        reply_markup=back_home_kb(),
    )


@router.message(TestSolveStates.waiting_answers)
@guard
async def test_answers_input(message: Message, state: FSMContext):
    answers_text = (message.text or "").strip()
    if not answers_text:
        await message.answer("Javoblar bo‘sh. Qayta yuboring.")
        return

    data = await state.get_data()
    code = data.get("test_code", "")
    answers = parse_answers_text(answers_text)

    async with SessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        test = await get_test_by_code(session, code)
        if not test:
            await session.commit()
            await message.answer("Test topilmadi.", reply_markup=back_home_kb())
            return

        existing = await get_result(session, user.id, test.id)
        if existing:
            await session.commit()
            await message.answer(
                "ℹ️ Bu test allaqachon ishlangan. Qayta yuborish yopilgan.",
                reply_markup=back_home_kb(),
            )
            return

        await update_streak(session, user)

        scored = score_test(test, answers)
        result = Result(
            user_id=user.id,
            test_id=test.id,
            correct_count=scored["correct"],
            wrong_count=scored["wrong"],
            total_questions=scored["total"],
            percent=scored["percent"],
            score=scored["score"],
            duration_sec=0,
            status="completed",
            answers_json=safe_json_dumps(answers),
            analysis_text="",
        )
        session.add(result)

        attempt = (
            await session.execute(
                select(Attempt).where(Attempt.user_id == user.id, Attempt.test_id == test.id)
            )
        ).scalar_one_or_none()
        if attempt:
            attempt.finished_at = utcnow()
            attempt.submitted_answers = answers_text
            attempt.status = "completed"

        await update_user_stats(session, user, scored["percent"], scored["correct"], scored["wrong"])
        unlocked = await unlock_badges(session, user)

        analysis = []
        for d in scored["details"]:
            if not d["is_correct"]:
                analysis.append(
                    f"• {d['no']}. {html_escape(d['question'])}\n"
                    f"  Sening javob: <b>{html_escape(d['user_answer'])}</b>\n"
                    f"  To‘g‘ri javob: <b>{html_escape(d['correct_answer'])}</b>\n"
                    f"  Sabab: {html_escape(d['explanation'] or '—')}\n"
                )

        result.analysis_text = "\n".join(analysis)

        certificate_path = await create_certificate_if_needed(session, user, test, result)
        await log_event(
            session,
            user.id,
            "test_completed",
            {
                "test_id": test.id,
                "percent": scored["percent"],
                "correct": scored["correct"],
                "wrong": scored["wrong"],
            },
        )
        await session.commit()

    result_card = render_result_card(result, test)
    extra = ""
    if unlocked:
        extra += "\n🏅 Yangi badge: " + ", ".join(unlocked)
    if certificate_path:
        extra += "\n🎓 Sertifikat tayyorlandi."

    await message.answer(
        result_card + extra,
        reply_markup=main_menu_kb(is_admin=is_admin_tg(message.from_user.id)),
    )

    review_text = build_review_text(scored["details"])
    await message.answer(review_text, reply_markup=back_home_kb())

    if certificate_path:
        try:
            await message.answer_document(
                FSInputFile(certificate_path),
                caption="🎓 Sertifikat",
            )
        except Exception as e:
            logger.exception("Certificate send failed: %s", e)

    await state.clear()


# ============================================================
# AI tutor flow
# ============================================================

@router.message(AIStates.waiting_prompt, F.photo)
@guard
async def ai_image_input(message: Message):
    photo = message.photo[-1]
    image_bytes = await download_telegram_file_to_bytes(message.bot, photo.file_id)
    text = await ai_service.multimodal_answer(text=message.caption or "", image_bytes=image_bytes)
    await message.answer(clamp_text(text, settings.MAX_TEXT_LEN), reply_markup=back_home_kb())


@router.message(AIStates.waiting_prompt, F.voice)
@guard
async def ai_voice_input(message: Message):
    voice = message.voice
    voice_bytes = await download_telegram_file_to_bytes(message.bot, voice.file_id)
    transcript = await ai_service.transcribe_voice(voice_bytes, "voice.ogg")
    answer = await ai_service.multimodal_answer(text=transcript or (message.caption or ""))
    await message.answer(
        f"📝 <b>Matn:</b>\n{html_escape(transcript or '—')}\n\n"
        f"🤖 <b>Javob:</b>\n{html_escape(clamp_text(answer, settings.MAX_TEXT_LEN))}",
        reply_markup=back_home_kb(),
    )


@router.message(AIStates.waiting_prompt)
@guard
async def ai_text_input(message: Message):
    text = (message.text or "").strip()
    if not text:
        await message.answer("Savol bo‘sh. Matn, rasm yoki ovoz yuboring.")
        return
    answer = await ai_service.explain_text(text)
    await message.answer(clamp_text(answer, settings.MAX_TEXT_LEN), reply_markup=back_home_kb())


# ============================================================
# Contact / admin message flow
# ============================================================

@router.message(ContactStates.waiting_text)
@guard
async def contact_input(message: Message):
    text = (message.text or "").strip()
    if not text:
        await message.answer("Xabar bo‘sh. Qayta yuboring.")
        return

    async with SessionLocal() as session:
        user = await get_or_create_user(session, message.from_user)
        await save_message_log(
            session,
            from_user_id=user.tg_id,
            to_user_id=settings.ADMIN_ID,
            direction="user_to_admin",
            text=text,
            tg_message_id=message.message_id,
        )
        await log_event(session, user.id, "contact_message", {"len": len(text)})

        if settings.ADMIN_ID:
            try:
                await bot.send_message(
                    settings.ADMIN_ID,
                    f"💬 <b>Yangi xabar</b>\n\n"
                    f"User: <code>{user.tg_id}</code>\n"
                    f"Ism: {html_escape(user.first_name or '—')}\n"
                    f"Username: @{html_escape(user.username) if user.username else '—'}\n\n"
                    f"{html_escape(text)}\n\n"
                    f"Javob: <code>/reply {user.tg_id} ...</code>",
                )
            except Exception as e:
                logger.exception("Admin notify failed: %s", e)

        await session.commit()

    await message.answer("✅ Xabaringiz adminga yuborildi.", reply_markup=main_menu_kb(is_admin=is_admin_tg(message.from_user.id)))


@router.message(Command("reply"))
@guard
async def admin_reply(message: Message):
    if not is_admin_tg(message.from_user.id):
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Format: <code>/reply USER_ID message</code>")
        return

    to_user_id = ensure_int(parts[1], 0)
    reply_text = parts[2].strip()
    if not to_user_id or not reply_text:
        await message.answer("User ID yoki matn bo‘sh.")
        return

    try:
        await bot.send_message(to_user_id, f"📩 <b>Admin javobi:</b>\n\n{html_escape(reply_text)}")
        async with SessionLocal() as session:
            await save_message_log(
                session,
                from_user_id=settings.ADMIN_ID,
                to_user_id=to_user_id,
                direction="admin_to_user",
                text=reply_text,
                tg_message_id=message.message_id,
            )
            await log_admin_action(session, settings.ADMIN_ID, "reply", f"to={to_user_id}")
            await session.commit()
        await message.answer("✅ Javob yuborildi.")
    except Exception as e:
        logger.exception("Reply failed: %s", e)
        await message.answer("❌ Javob yuborib bo‘lmadi.")


# ============================================================
# Admin commands
# ============================================================

@router.callback_query(F.data == "admin:addtest")
@guard
async def admin_addtest(call: CallbackQuery, state: FSMContext):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_test_json)
    await call.message.edit_text(
        "➕ Yangi test JSON yuboring.\n\n"
        "Majburiy maydonlar:\n"
        "<code>code, title, category, topic, difficulty, questions_json</code>\n\n"
        "Misol:\n"
        "<code>{\"code\":\"MT-003\",\"title\":\"...\",\"category\":\"Matematika\",\"topic\":\"Algebra\",\"difficulty\":\"O‘rtacha\",\"questions\":[...]}</code>",
        reply_markup=back_home_kb("menu:admin"),
    )
    await call.answer()


@router.message(AdminStates.waiting_test_json)
@guard
async def admin_test_json_input(message: Message, state: FSMContext):
    if not is_admin_tg(message.from_user.id):
        return
    raw = (message.text or "").strip()
    try:
        data = json.loads(raw)
        questions = data.get("questions", [])
        if not isinstance(questions, list) or not questions:
            raise ValueError("questions bo‘sh")
        test = Test(
            code=str(data["code"]).strip(),
            title=str(data["title"]).strip(),
            category=str(data.get("category", "Umumiy")).strip(),
            topic=str(data.get("topic", "Umumiy")).strip(),
            difficulty=str(data.get("difficulty", "Oson")).strip(),
            test_date=today_utc(),
            description=str(data.get("description", "")).strip(),
            questions_json=safe_json_dumps(questions),
            pdf_url=str(data.get("pdf_url", "")).strip() or None,
            active=bool(data.get("active", True)),
        )
    except Exception as e:
        await message.answer(
            f"❌ JSON xato.\n\nSabab: {html_escape(str(e))}",
            reply_markup=back_home_kb("menu:admin"),
        )
        return

    async with SessionLocal() as session:
        existing = await get_test_by_code(session, test.code)
        if existing:
            await session.commit()
            await message.answer("ℹ️ Bu kodli test allaqachon mavjud.")
            await state.clear()
            return
        session.add(test)
        await log_admin_action(session, settings.ADMIN_ID, "add_test", f"code={test.code}")
        await session.commit()

    await state.clear()
    await message.answer("✅ Test qo‘shildi.", reply_markup=back_home_kb("menu:admin"))


@router.callback_query(F.data == "admin:deltest")
@guard
async def admin_deltest(call: CallbackQuery, state: FSMContext):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_delete_code)
    await call.message.edit_text(
        "🗑 O‘chirish uchun test kodini yuboring.",
        reply_markup=back_home_kb("menu:admin"),
    )
    await call.answer()


@router.message(AdminStates.waiting_delete_code)
@guard
async def admin_delete_code_input(message: Message, state: FSMContext):
    if not is_admin_tg(message.from_user.id):
        return
    code = (message.text or "").strip()
    async with SessionLocal() as session:
        test = await get_test_by_code(session, code)
        if not test:
            await session.commit()
            await message.answer("Test topilmadi.", reply_markup=back_home_kb("menu:admin"))
            return
        await session.delete(test)
        await log_admin_action(session, settings.ADMIN_ID, "delete_test", f"code={code}")
        await session.commit()
    await state.clear()
    await message.answer("✅ Test o‘chirildi.", reply_markup=back_home_kb("menu:admin"))


@router.callback_query(F.data == "admin:broadcast")
@guard
async def admin_broadcast(call: CallbackQuery, state: FSMContext):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_broadcast)
    await call.message.edit_text(
        "📣 Ommaviy xabar matnini yuboring.",
        reply_markup=back_home_kb("menu:admin"),
    )
    await call.answer()


@router.message(AdminStates.waiting_broadcast)
@guard
async def admin_broadcast_input(message: Message, state: FSMContext):
    if not is_admin_tg(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("Matn bo‘sh.")
        return

    sent = 0
    async with SessionLocal() as session:
        users = (await session.execute(select(User))).scalars().all()
        for u in users:
            try:
                await bot.send_message(u.tg_id, f"📣 <b>Admin e’loni</b>\n\n{html_escape(text)}")
                sent += 1
            except Exception:
                continue
        await log_admin_action(session, settings.ADMIN_ID, "broadcast", f"sent={sent}")
        await session.commit()

    await state.clear()
    await message.answer(f"✅ Ommaviy xabar yuborildi: {sent} ta user.", reply_markup=back_home_kb("menu:admin"))


@router.callback_query(F.data == "admin:stats")
@guard
async def admin_stats(call: CallbackQuery):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return

    async with SessionLocal() as session:
        users_count = await session.scalar(select(func.count(User.id)))
        tests_count = await session.scalar(select(func.count(Test.id)))
        results_count = await session.scalar(select(func.count(Result.id)))
        avg_percent = await session.scalar(select(func.avg(Result.percent)))
        top_users = (await session.execute(select(User).order_by(desc(User.xp)).limit(5))).scalars().all()
        top_tests = (
            await session.execute(
                select(Test, func.count(Result.id).label("cnt"))
                .join(Result, Result.test_id == Test.id, isouter=True)
                .group_by(Test.id)
                .order_by(desc("cnt"))
                .limit(5)
            )
        ).all()
        await session.commit()

    lines = [
        "📊 <b>Statistikalar</b>\n",
        f"👥 Users: <b>{users_count or 0}</b>",
        f"📚 Tests: <b>{tests_count or 0}</b>",
        f"📈 Results: <b>{results_count or 0}</b>",
        f"🧮 Avg percent: <b>{float(avg_percent or 0):.1f}%</b>\n",
        "🏆 Top users:",
    ]
    for i, u in enumerate(top_users, start=1):
        lines.append(f"{i}. {html_escape(u.first_name or '—')} — {u.xp} XP")
    lines.append("\n🔥 Top tests:")
    for row in top_tests:
        t = row[0]
        cnt = row[1]
        lines.append(f"• {html_escape(t.code)} — {html_escape(t.title)} ({cnt})")

    await call.message.edit_text("\n".join(lines), reply_markup=back_home_kb("menu:admin"))
    await call.answer()


@router.callback_query(F.data == "admin:users")
@guard
async def admin_users(call: CallbackQuery):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return

    async with SessionLocal() as session:
        users = (await session.execute(select(User).order_by(desc(User.xp)).limit(20))).scalars().all()
        await session.commit()

    lines = ["👥 <b>Foydalanuvchilar</b>\n"]
    for u in users:
        lines.append(
            f"• <code>{u.tg_id}</code> | {html_escape(u.first_name or '—')} | "
            f"{u.xp} XP | lvl {u.level} | {float(u.avg_percent or 0):.1f}%"
        )
    await call.message.edit_text("\n".join(lines), reply_markup=back_home_kb("menu:admin"))
    await call.answer()


@router.callback_query(F.data == "admin:logs")
@guard
async def admin_logs(call: CallbackQuery):
    if not is_admin_tg(call.from_user.id):
        await call.answer("Ruxsat yo‘q", show_alert=True)
        return

    async with SessionLocal() as session:
        logs = (await session.execute(select(AdminLog).order_by(desc(AdminLog.created_at)).limit(20))).scalars().all()
        await session.commit()

    lines = ["🧾 <b>Admin loglar</b>\n"]
    for log in logs:
        lines.append(
            f"• {log.created_at.strftime('%Y-%m-%d %H:%M')} | {html_escape(log.action)} | "
            f"{html_escape(log.details or '')}"
        )
    await call.message.edit_text("\n".join(lines), reply_markup=back_home_kb("menu:admin"))
    await call.answer()


# ============================================================
# Generic message handling
# ============================================================

@router.message()
@guard
async def fallback_text_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await message.answer(
            "Xabar qabul qilindi. Kerakli formatda yuboring yoki menyudan tanlang.",
            reply_markup=back_home_kb(),
        )
        return

    # Default assistant-like fallback without crashing
    text = (message.text or "").strip()
    if text:
        async with SessionLocal() as session:
            user = await get_or_create_user(session, message.from_user)
            await log_event(session, user.id, "plain_text_fallback", {"len": len(text)})
            await session.commit()
        answer = await ai_service.explain_text(text)
        await message.answer(clamp_text(answer, settings.MAX_TEXT_LEN), reply_markup=back_home_kb())
    else:
        await message.answer("Menyudan tanlang yoki savol yuboring.", reply_markup=main_menu_kb(is_admin=is_admin_tg(message.from_user.id)))


# ============================================================
# Background jobs
# ============================================================

async def daily_challenge_job():
    async with SessionLocal() as session:
        tests = (await session.execute(select(Test).where(Test.active == True).order_by(desc(Test.created_at)).limit(1))).scalars().all()  # noqa: E712
        if not tests:
            return
        test = tests[0]
        users = (await session.execute(select(User))).scalars().all()
        for u in users:
            session.add(
                Notification(
                    user_id=u.tg_id,
                    title="Daily challenge",
                    body=f"Bugungi mini-test: {test.title} ({test.code})",
                    is_read=False,
                )
            )
        await log_event(session, None, "daily_challenge_created", {"test_id": test.id})
        await session.commit()

    if bot:
        for u in users[:1000]:
            try:
                await bot.send_message(
                    u.tg_id,
                    f"🎯 <b>Daily challenge</b>\n\nBugungi mini-test: <b>{html_escape(test.title)}</b>\nKod: <code>{html_escape(test.code)}</code>",
                    reply_markup=main_menu_kb(is_admin=False),
                )
            except Exception:
                continue


async def seed_and_prepare():
    async with SessionLocal() as session:
        await init_db()
        await seed_demo_tests(session)
        await session.commit()


# ============================================================
# Startup / shutdown
# ============================================================

async def start_polling_background():
    if not bot:
        return
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("Polling crashed: %s", e)


@app.on_event("startup")
async def on_startup():
    global POLLED_TASK
    await seed_and_prepare()

    scheduler.add_job(daily_challenge_job, "cron", hour=7, minute=0)
    if not scheduler.running:
        scheduler.start()

    if bot and settings.WEBHOOK_URL:
        try:
            await bot.set_webhook(
                url=f"{settings.WEBHOOK_URL.rstrip('/')}{settings.WEBHOOK_PATH}",
                drop_pending_updates=True,
                allowed_updates=dp.resolve_used_update_types(),
            )
            logger.info("Webhook set: %s%s", settings.WEBHOOK_URL, settings.WEBHOOK_PATH)
        except Exception as e:
            logger.exception("Webhook setup failed: %s", e)

    if bot and not settings.WEBHOOK_URL:
        if POLLED_TASK is None or POLLED_TASK.done():
            POLLED_TASK = asyncio.create_task(start_polling_background())


@app.on_event("shutdown")
async def on_shutdown():
    global POLLED_TASK
    if POLLED_TASK and not POLLED_TASK.done():
        POLLED_TASK.cancel()
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass
    if bot:
        try:
            await bot.session.close()
        except Exception:
            pass
    try:
        await engine.dispose()
    except Exception:
        pass


# ============================================================
# Main entry
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=False,
        log_level=settings.LOG_LEVEL.lower(),
    )
