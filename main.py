from __future__ import annotations

import ast
import asyncio
import html
import io
import json
import logging
import math
import os
import re
import secrets
import tempfile
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    and_,
    func,
    or_,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

# =========================================================
# SETTINGS
# =========================================================

class Settings:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()
    ADMIN_ID_RAW: str = os.getenv("ADMIN_ID", "").strip()
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "").strip()
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "").strip()
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db").strip()
    REDIS_URL: str = os.getenv("REDIS_URL", "").strip()
    WEBHOOK_URL: str = os.getenv("WEBHOOK_URL", "").strip().rstrip("/")
    WEBHOOK_PATH: str = os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip()
    WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "").strip()
    PORT: int = int(os.getenv("PORT", "10000"))
    APP_ENV: str = os.getenv("APP_ENV", "production").strip().lower()
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    AI_PROVIDER: str = os.getenv("AI_PROVIDER", "auto").strip().lower()
    APP_NAME: str = os.getenv("APP_NAME", "math_tekshiruvchi_bot").strip()
    MAX_TESTS_PER_PAGE: int = int(os.getenv("MAX_TESTS_PER_PAGE", "6"))
    RATE_LIMIT_MESSAGES: int = int(os.getenv("RATE_LIMIT_MESSAGES", "12"))
    RATE_LIMIT_WINDOW_SECONDS: int = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "8"))
    CERTIFICATE_THRESHOLD: int = int(os.getenv("CERTIFICATE_THRESHOLD", "85"))
    XP_PER_CORRECT: int = int(os.getenv("XP_PER_CORRECT", "10"))

    @property
    def admin_ids(self) -> set[int]:
        ids: set[int] = set()
        for raw in re.split(r"[,\s]+", self.ADMIN_ID_RAW or ""):
            raw = raw.strip()
            if raw.isdigit():
                ids.add(int(raw))
        return ids


settings = Settings()

# =========================================================
# LOGGING
# =========================================================


def setup_logging() -> logging.Logger:
    logger = logging.getLogger(settings.APP_NAME)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL, logging.INFO))
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    if not logger.handlers:
        logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = setup_logging()

# =========================================================
# HELPERS
# =========================================================


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def clamp(n: int, low: int, high: int) -> int:
    return max(low, min(high, n))


def esc(text: Any) -> str:
    return html.escape("" if text is None else str(text))


def sanitize_text(text: str) -> str:
    return re.sub(r"[ \t]+", " ", (text or "")).strip()


def percent(correct: int, total: int) -> int:
    return 0 if total <= 0 else round((correct / total) * 100)


def level_from_xp(xp: int) -> Tuple[int, int, int]:
    lvl = max(1, xp // 100 + 1)
    current = (lvl - 1) * 100
    next_ = lvl * 100
    return lvl, xp - current, next_ - current


def badge_from_level(level: int) -> str:
    if level >= 20:
        return "🏆 Legend"
    if level >= 15:
        return "💎 Elite"
    if level >= 10:
        return "🔥 Pro"
    if level >= 5:
        return "⭐ Rising"
    return "🌱 Starter"


def parse_answers(raw: str) -> List[str]:
    raw = sanitize_text(raw).upper()
    if not raw:
        return []
    return re.findall(r"[A-D]|[1-9]\d*", raw)


def parse_test_code_and_answers(raw: str) -> Tuple[str, str]:
    raw = sanitize_text(raw)
    if not raw:
        return "", ""
    parts = raw.split(maxsplit=1)
    if len(parts) == 1:
        return parts[0].upper(), ""
    return parts[0].upper(), parts[1]


def safe_json_loads(value: Any, default: Any) -> Any:
    try:
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        return json.loads(value)
    except Exception:
        return default

# =========================================================
# DATABASE
# =========================================================

Base = declarative_base()
engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    full_name: Mapped[str] = mapped_column(String(255), default="")
    username: Mapped[str] = mapped_column(String(255), default="")
    language: Mapped[str] = mapped_column(String(32), default="uz")
    xp: Mapped[int] = mapped_column(Integer, default=0)
    level: Mapped[int] = mapped_column(Integer, default=1)
    streak: Mapped[int] = mapped_column(Integer, default=0)
    best_streak: Mapped[int] = mapped_column(Integer, default=0)
    badge: Mapped[str] = mapped_column(String(128), default="🌱 Starter")
    avg_percent: Mapped[float] = mapped_column(Float, default=0.0)
    best_percent: Mapped[int] = mapped_column(Integer, default=0)
    last_active_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    results: Mapped[List["Result"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    favorites: Mapped[List["Favorite"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class Test(Base):
    __tablename__ = "tests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    category: Mapped[str] = mapped_column(String(128), index=True, default="General")
    topic: Mapped[str] = mapped_column(String(128), index=True, default="General")
    difficulty: Mapped[str] = mapped_column(String(64), index=True, default="Medium")
    pdf_url: Mapped[str] = mapped_column(String(500), default="")
    preview_text: Mapped[str] = mapped_column(Text, default="")
    answer_key: Mapped[list] = mapped_column(JSON, default=list)
    max_score: Mapped[int] = mapped_column(Integer, default=20)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    results: Mapped[List["Result"]] = relationship(back_populates="test", cascade="all, delete-orphan")
    favorites: Mapped[List["Favorite"]] = relationship(back_populates="test", cascade="all, delete-orphan")


class Result(Base):
    __tablename__ = "results"
    __table_args__ = (UniqueConstraint("user_id", "test_id", name="uq_user_test_result"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), index=True)
    correct_count: Mapped[int] = mapped_column(Integer, default=0)
    wrong_count: Mapped[int] = mapped_column(Integer, default=0)
    percent_value: Mapped[int] = mapped_column(Integer, default=0)
    score: Mapped[int] = mapped_column(Integer, default=0)
    duration_sec: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(64), default="done")
    mistakes: Mapped[list] = mapped_column(JSON, default=list)
    raw_answers: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    user: Mapped["User"] = relationship(back_populates="results")
    test: Mapped["Test"] = relationship(back_populates="results")


class Attempt(Base):
    __tablename__ = "attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, index=True)
    test_code: Mapped[str] = mapped_column(String(64), index=True)
    raw_answer: Mapped[str] = mapped_column(Text, default="")
    duplicate: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class MessageLog(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, index=True)
    direction: Mapped[str] = mapped_column(String(16), default="in")
    text: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AdminLog(Base):
    __tablename__ = "admin_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    admin_id: Mapped[int] = mapped_column(BigInteger, index=True)
    action: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AnalyticsEvent(Base):
    __tablename__ = "analytics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    data: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Certificate(Base):
    __tablename__ = "certificates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), index=True)
    cert_code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    pdf_path: Mapped[str] = mapped_column(String(500), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Badge(Base):
    __tablename__ = "badges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Streak(Base):
    __tablename__ = "streaks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    current: Mapped[int] = mapped_column(Integer, default=0)
    best: Mapped[int] = mapped_column(Integer, default=0)
    last_active_day: Mapped[str] = mapped_column(String(16), default="")


class Favorite(Base):
    __tablename__ = "favorites"
    __table_args__ = (UniqueConstraint("user_id", "test_id", name="uq_user_favorite"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    user: Mapped["User"] = relationship(back_populates="favorites")
    test: Mapped["Test"] = relationship(back_populates="favorites")


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(255), default="")
    body: Mapped[str] = mapped_column(Text, default="")
    read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


DEFAULT_TESTS: List[dict] = [
    {
        "code": "MATH-001",
        "title": "Kasrlar va amallar",
        "category": "Matematika",
        "topic": "Kasrlar",
        "difficulty": "Oson",
        "pdf_url": "",
        "preview_text": "Kasrlar bo‘yicha 1-topshiriq. Bir nechta misollar va qisqa test.",
        "max_score": 20,
        "answer_key": [
            {"question": "1/2 + 1/4 = ?", "options": ["1/3", "3/4", "2/4", "5/4"], "correct": "B", "explanation": "1/2 = 2/4, 2/4 + 1/4 = 3/4."},
            {"question": "3/5 - 1/5 = ?", "options": ["2/5", "4/5", "1/5", "3/10"], "correct": "A", "explanation": "Maxraj bir xil, suratlar ayiriladi."},
            {"question": "2/3 × 3/4 = ?", "options": ["1/2", "5/12", "6/7", "1/4"], "correct": "A", "explanation": "2/3 × 3/4 = 6/12 = 1/2."},
        ],
    },
    {
        "code": "ALG-001",
        "title": "Algebra asoslari",
        "category": "Matematika",
        "topic": "Algebra",
        "difficulty": "O‘rta",
        "pdf_url": "",
        "preview_text": "Algebra bo‘yicha asosiy qoida va tenglama testlari.",
        "max_score": 30,
        "answer_key": [
            {"question": "x + 5 = 12, x = ?", "options": ["5", "6", "7", "8"], "correct": "C", "explanation": "x = 12 - 5 = 7."},
            {"question": "2x = 14, x = ?", "options": ["5", "6", "7", "8"], "correct": "C", "explanation": "Ikkala tomonni 2 ga bo‘lamiz."},
            {"question": "x^2 = 49, x ning musbat qiymati?", "options": ["5", "6", "7", "8"], "correct": "C", "explanation": "7^2 = 49."},
        ],
    },
    {
        "code": "PHY-001",
        "title": "Fizika: kuch va ish",
        "category": "Fizika",
        "topic": "Mexanika",
        "difficulty": "O‘rta",
        "pdf_url": "",
        "preview_text": "Mexanik ish va kuch bo‘yicha qisqa test.",
        "max_score": 30,
        "answer_key": [
            {"question": "Ish formulasi qaysi?", "options": ["A = F / s", "A = F × s", "A = m × g", "A = v / t"], "correct": "B", "explanation": "Ish = kuch × yo‘l."},
            {"question": "Kuch birligi?", "options": ["Joul", "Nyuton", "Vatt", "Metr"], "correct": "B", "explanation": "Kuch birligi — Nyuton."},
        ],
    },
]

# =========================================================
# APP STATE
# =========================================================

recent_messages: Dict[int, List[float]] = defaultdict(list)
user_filter_state: Dict[int, Dict[str, Any]] = defaultdict(dict)
pending_contact: Dict[int, bool] = defaultdict(bool)

# =========================================================
# AI SERVICE
# =========================================================

class AIService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def detect_provider(self) -> str:
        if self.settings.AI_PROVIDER in {"openai", "gemini"}:
            return self.settings.AI_PROVIDER
        if self.settings.OPENAI_API_KEY:
            return "openai"
        if self.settings.GEMINI_API_KEY:
            return "gemini"
        return "offline"

    def solve_math(self, text: str) -> Optional[str]:
        cleaned = text.lower().replace("=", " = ")
        m = re.search(r"([0-9\.\+\-\*\/\(\)\s\^]+)", cleaned)
        if not m:
            return None
        expr = m.group(1).strip().replace("^", "**")
        if not re.fullmatch(r"[0-9\.\+\-\*\/\(\)\s\*]+", expr):
            return None
        try:
            node = ast.parse(expr, mode="eval")
            value = self._safe_eval(node.body)
            return (
                "🧠 <b>Matematik yechim</b>\n\n"
                f"<b>Amal:</b> <code>{esc(expr)}</code>\n"
                f"<b>Natija:</b> <b>{esc(value)}</b>\n\n"
                "Qisqacha: ifodani qadam-baqadam soddalashtirdim."
            )
        except Exception:
            return None

    def _safe_eval(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.BinOp):
            left = self._safe_eval(node.left)
            right = self._safe_eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Pow):
                return left ** right
        if isinstance(node, ast.UnaryOp):
            operand = self._safe_eval(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.USub):
                return -operand
        raise ValueError("Unsupported expression")

    def offline_answer(self, prompt: str) -> str:
        return (
            "📘 <b>AI Ustoz</b>\n\n"
            f"<b>Savol:</b> {esc(prompt)}\n\n"
            "Men hozir lokal fallback rejimida ishlayapman.\n"
            "Shunga qaramay, savolni quyidagicha tushuntiraman:\n"
            "1) Avval mavzuni aniqlang.\n"
            "2) Berilganlarni alohida yozing.\n"
            "3) Formula yoki qoida tanlang.\n"
            "4) Hisoblang va tekshiring.\n\n"
            "Agar xohlasangiz, savolni aniqroq yozing yoki rasm/ovoz yuboring."
        )

    async def answer_text(self, prompt: str, context: str = "") -> str:
        prompt = sanitize_text(prompt)
        if not prompt:
            return "Savolni yuboring, men bosqichma-bosqich tushuntiraman."

        math_result = self.solve_math(prompt)
        if math_result:
            return math_result

        provider = self.detect_provider()
        if provider == "openai":
            result = await self._openai_text(prompt, context=context)
            if result:
                return result
        elif provider == "gemini":
            result = await self._gemini_text(prompt, context=context)
            if result:
                return result

        return self.offline_answer(prompt)

    async def _openai_text(self, prompt: str, context: str = "") -> Optional[str]:
        if not self.settings.OPENAI_API_KEY:
            return None
        try:
            headers = {
                "Authorization": f"Bearer {self.settings.OPENAI_API_KEY}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                "messages": [
                    {"role": "system", "content": "Sen o'zbek tilida sodda va bosqichma-bosqich tushuntiradigan matematik ustozsan."},
                    {"role": "user", "content": f"{context}\n\nSavol: {prompt}".strip()},
                ],
                "temperature": 0.3,
            }
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
                if r.status_code >= 300:
                    return None
                data = r.json()
                return data["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.warning("OpenAI fallback: %s", e)
            return None

    async def _gemini_text(self, prompt: str, context: str = "") -> Optional[str]:
        if not self.settings.GEMINI_API_KEY:
            return None
        try:
            model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.settings.GEMINI_API_KEY}"
            payload = {
                "contents": [{"role": "user", "parts": [{"text": f"{context}\n\nSavol: {prompt}".strip()}]}],
                "generationConfig": {"temperature": 0.3},
            }
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(url, json=payload)
                if r.status_code >= 300:
                    return None
                data = r.json()
                candidates = data.get("candidates") or []
                if not candidates:
                    return None
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(p.get("text", "") for p in parts).strip()
                return text or None
        except Exception as e:
            logger.warning("Gemini fallback: %s", e)
            return None

    async def transcribe_voice(self, file_path: str) -> Optional[str]:
        if not self.settings.OPENAI_API_KEY:
            return None
        try:
            headers = {"Authorization": f"Bearer {self.settings.OPENAI_API_KEY}"}
            data = {"model": os.getenv("OPENAI_STT_MODEL", "whisper-1")}
            with open(file_path, "rb") as f:
                files = {"file": f}
                async with httpx.AsyncClient(timeout=60) as client:
                    r = await client.post("https://api.openai.com/v1/audio/transcriptions", headers=headers, data=data, files=files)
                    if r.status_code >= 300:
                        return None
                    return r.json().get("text")
        except Exception as e:
            logger.warning("Voice fallback: %s", e)
            return None

    async def analyze_image(self, file_path: str, prompt: str = "") -> str:
        ocr_text = None
        try:
            from PIL import Image  # type: ignore
            import pytesseract  # type: ignore

            ocr_text = pytesseract.image_to_string(Image.open(file_path), lang="uzb+eng")
            ocr_text = sanitize_text(ocr_text)
        except Exception:
            ocr_text = None

        context = ""
        if ocr_text:
            context += f"Rasm ichidagi matn:\n{ocr_text}\n\n"
        if prompt:
            context += f"Foydalanuvchi so'rovi: {prompt}\n"

        provider = self.detect_provider()
        if provider == "openai":
            result = await self._openai_text(prompt or ocr_text or "Rasmni tahlil qil", context=context)
            if result:
                return result
        if provider == "gemini":
            result = await self._gemini_text(prompt or ocr_text or "Rasmni tahlil qil", context=context)
            if result:
                return result

        if ocr_text:
            return (
                "🖼 <b>Rasm tahlili</b>\n\n"
                f"<b>Topilgan matn:</b>\n<code>{esc(ocr_text[:3500])}</code>\n\n"
                "AI provider yoqilmaganligi sababli, faqat OCR natijasini ko‘rsatdim."
            )

        return (
            "🖼 <b>Rasm tahlili</b>\n\n"
            "Rasmni lokal tekshiruvdan o‘tkazdim, lekin OCR/AI provider ulanmagan.\n"
            "Qulayroq natija uchun savolni matn ko‘rinishida yuboring."
        )


ai_service = AIService(settings)

# =========================================================
# CERTIFICATES
# =========================================================

def generate_certificate_pdf(username: str, full_name: str, test_title: str, percent_value: int) -> Optional[str]:
    try:
        from reportlab.lib.pagesizes import A4  # type: ignore
        from reportlab.pdfgen import canvas  # type: ignore
    except Exception:
        return None

    cert_dir = Path(tempfile.gettempdir()) / "math_tekshiruvchi_bot_certs"
    cert_dir.mkdir(parents=True, exist_ok=True)
    cert_code = f"CERT-{secrets.token_hex(4).upper()}"
    file_path = cert_dir / f"{cert_code}.pdf"

    c = canvas.Canvas(str(file_path), pagesize=A4)
    width, height = A4
    c.setTitle("Certificate")
    c.setFont("Helvetica-Bold", 22)
    c.drawCentredString(width / 2, height - 120, "SERTIFIKAT")
    c.setFont("Helvetica", 13)
    c.drawCentredString(width / 2, height - 165, "Ushbu sertifikat quyidagi ishtirokchiga berildi:")
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(width / 2, height - 210, full_name or username or "Foydalanuvchi")
    c.setFont("Helvetica", 13)
    c.drawCentredString(width / 2, height - 250, f"Test: {test_title}")
    c.drawCentredString(width / 2, height - 275, f"Natija: {percent_value}%")
    c.drawCentredString(width / 2, height - 300, f"Sana: {date.today().isoformat()}")
    c.drawCentredString(width / 2, height - 325, f"Code: {cert_code}")
    c.setFont("Helvetica-Oblique", 10)
    c.drawCentredString(width / 2, 50, "math_tekshiruvchi_bot")
    c.showPage()
    c.save()
    return str(file_path)

# =========================================================
# DB OPS
# =========================================================

async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def seed_default_tests() -> None:
    async with SessionLocal() as session:
        existing = (await session.execute(select(func.count(Test.id)))).scalar_one()
        if existing and existing > 0:
            return
        for item in DEFAULT_TESTS:
            session.add(Test(**item))
        await session.commit()


async def log_event(name: str, data: dict) -> None:
    try:
        async with SessionLocal() as session:
            session.add(AnalyticsEvent(name=name, data=data))
            await session.commit()
    except Exception as e:
        logger.warning("log_event failed: %s", e)


async def log_message(telegram_id: int, direction: str, text: str) -> None:
    try:
        async with SessionLocal() as session:
            session.add(MessageLog(telegram_id=telegram_id, direction=direction, text=text[:4000]))
            await session.commit()
    except Exception as e:
        logger.warning("log_message failed: %s", e)


async def log_admin(admin_id: int, action: str, payload: dict) -> None:
    try:
        async with SessionLocal() as session:
            session.add(AdminLog(admin_id=admin_id, action=action, payload=payload))
            await session.commit()
    except Exception as e:
        logger.warning("log_admin failed: %s", e)


async def ensure_user(session: AsyncSession, tg_user: types.User) -> User:
    q = await session.execute(select(User).where(User.telegram_id == tg_user.id))
    user = q.scalar_one_or_none()
    if not user:
        user = User(
            telegram_id=tg_user.id,
            full_name=(tg_user.full_name or "").strip(),
            username=(tg_user.username or "").strip(),
            language=(tg_user.language_code or "uz")[:32],
        )
        session.add(user)
        await session.flush()
        session.add(Streak(user_id=user.id, current=0, best=0, last_active_day=""))
    else:
        user.full_name = (tg_user.full_name or "").strip()
        user.username = (tg_user.username or "").strip()
        user.language = (tg_user.language_code or "uz")[:32]
        user.last_active_at = utcnow()
    await session.commit()
    return user


async def get_user_by_tg(session: AsyncSession, telegram_id: int) -> Optional[User]:
    q = await session.execute(select(User).where(User.telegram_id == telegram_id))
    return q.scalar_one_or_none()


async def get_test_by_code(session: AsyncSession, code: str) -> Optional[Test]:
    q = await session.execute(select(Test).where(func.upper(Test.code) == code.upper()))
    return q.scalar_one_or_none()


async def get_test_by_id(session: AsyncSession, test_id: int) -> Optional[Test]:
    q = await session.execute(select(Test).where(Test.id == test_id))
    return q.scalar_one_or_none()


async def user_already_solved(session: AsyncSession, user_id: int, test_id: int) -> bool:
    q = await session.execute(select(Result.id).where(and_(Result.user_id == user_id, Result.test_id == test_id)))
    return q.scalar_one_or_none() is not None


async def update_streak(session: AsyncSession, user_id: int) -> Tuple[int, int]:
    q = await session.execute(select(Streak).where(Streak.user_id == user_id))
    streak = q.scalar_one_or_none()
    if not streak:
        streak = Streak(user_id=user_id, current=1, best=1, last_active_day=date.today().isoformat())
        session.add(streak)
        await session.commit()
        return 1, 1
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    if streak.last_active_day == today:
        return streak.current, streak.best
    if streak.last_active_day == yesterday:
        streak.current += 1
    else:
        streak.current = 1
    streak.best = max(streak.best, streak.current)
    streak.last_active_day = today
    await session.commit()
    return streak.current, streak.best


def compute_mistakes(answer_key: list, raw_answers: list) -> Tuple[int, int, list]:
    correct = 0
    mistakes: list = []
    for idx, item in enumerate(answer_key):
        expected = str(item.get("correct", "")).upper().strip()
        got = str(raw_answers[idx]).upper().strip() if idx < len(raw_answers) else ""
        if got == expected:
            correct += 1
        else:
            mistakes.append(
                {
                    "question": item.get("question", f"Savol {idx+1}"),
                    "your": got or "-",
                    "correct": expected or "-",
                    "options": item.get("options", []),
                    "explanation": item.get("explanation", ""),
                }
            )
    wrong = len(answer_key) - correct
    return correct, wrong, mistakes


def make_result_text(test: Test, result: Result) -> str:
    level, _, need = level_from_xp(result.score)
    mistake_text = "\n".join(
        [
            f"{i+1}) <b>{esc(m['question'])}</b>\n"
            f"   Siz: <code>{esc(m['your'])}</code> | To‘g‘ri: <code>{esc(m['correct'])}</code>\n"
            f"   Izoh: {esc(m.get('explanation','') or '—')}"
            for i, m in enumerate(result.mistakes[:10])
        ]
    ) or "Xato savollar yo‘q."
    return (
        "🧾 <b>Natija kartasi</b>\n\n"
        f"<b>Test:</b> {esc(test.code)} — {esc(test.title)}\n"
        f"<b>Status:</b> {esc(result.status)}\n"
        f"<b>To‘g‘ri:</b> {result.correct_count}\n"
        f"<b>Noto‘g‘ri:</b> {result.wrong_count}\n"
        f"<b>Foiz:</b> {result.percent_value}%\n"
        f"<b>Ball:</b> {result.score}/{test.max_score}\n"
        f"<b>Vaqt:</b> {result.duration_sec} soniya\n"
        f"<b>XP:</b> +{result.score}\n"
        f"<b>Level:</b> {level}\n"
        f"<b>Keyingi level uchun:</b> {need} XP\n\n"
        f"<b>Xato savollar:</b>\n{mistake_text}"
    )


def daily_test_index() -> int:
    return date.today().toordinal()

# =========================================================
# KEYBOARDS
# =========================================================


def kb_main(is_admin: bool = False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📚 Testlar ro‘yxati", callback_data="menu:tests")
    b.button(text="🧠 AI Ustoz", callback_data="menu:ai")
    b.button(text="📈 Natijalarim", callback_data="menu:results")
    b.button(text="👤 Profilim", callback_data="menu:profile")
    b.button(text="⭐ Favorites", callback_data="menu:favorites")
    b.button(text="🎯 Daily challenge", callback_data="menu:daily")
    b.button(text="🏆 Leaderboard", callback_data="menu:leaderboard")
    b.button(text="✉️ Bog‘lanish", callback_data="menu:contact")
    if is_admin:
        b.button(text="🛠 Admin panel", callback_data="menu:admin")
    b.adjust(2, 2, 2, 2, 1)
    return b.as_markup()


def kb_back_home(back: str = "nav:home") -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="⬅️ Orqaga", callback_data=back)
    b.button(text="🏠 Bosh menyu", callback_data="nav:home")
    b.adjust(2)
    return b.as_markup()


def kb_tests_page(tests: List[Test], page: int, total_pages: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for t in tests:
        b.button(text=f"{t.code} • {t.title[:18]}", callback_data=f"test:open:{t.id}")
    nav = InlineKeyboardBuilder()
    if page > 1:
        nav.button(text="⬅️", callback_data=f"tests:page:{page-1}")
    nav.button(text=f"{page}/{total_pages}", callback_data="noop")
    if page < total_pages:
        nav.button(text="➡️", callback_data=f"tests:page:{page+1}")
    nav.adjust(3)
    b.attach(nav)
    b.button(text="🔎 Qidirish", callback_data="tests:search")
    b.button(text="🎚 Filter", callback_data="tests:filter")
    b.button(text="🏠 Bosh menyu", callback_data="nav:home")
    b.adjust(1, 1, 1, 1, 1)
    return b.as_markup()


def kb_test_detail(test: Test, is_fav: bool = False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    if test.pdf_url:
        b.button(text="📄 PDF", url=test.pdf_url)
    b.button(text="🧪 Testni tekshirish", callback_data=f"test:submit:{test.id}")
    b.button(text=("⭐ Olib tashlash" if is_fav else "⭐ Saqlash"), callback_data=f"fav:toggle:{test.id}")
    b.button(text="📘 Review mode", callback_data=f"test:review:{test.id}")
    b.button(text="⬅️ Orqaga", callback_data="menu:tests")
    b.button(text="🏠 Bosh menyu", callback_data="nav:home")
    b.adjust(1, 1, 1, 1, 2)
    return b.as_markup()


def kb_admin() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Test qo‘shish", callback_data="admin:add_test")
    b.button(text="🗑 Test o‘chirish", callback_data="admin:delete_test")
    b.button(text="📣 Ommaviy xabar", callback_data="admin:broadcast")
    b.button(text="📊 Statistikalar", callback_data="admin:stats")
    b.button(text="🏅 Top userlar", callback_data="admin:top")
    b.button(text="🧾 Loglar", callback_data="admin:logs")
    b.button(text="⬅️ Orqaga", callback_data="nav:home")
    b.adjust(2, 2, 2, 1)
    return b.as_markup()

# =========================================================
# STATES
# =========================================================

class AddTestState(StatesGroup):
    waiting_json = State()


class DeleteTestState(StatesGroup):
    waiting_code = State()


class BroadcastState(StatesGroup):
    waiting_text = State()


class SearchState(StatesGroup):
    waiting_query = State()


class ContactState(StatesGroup):
    waiting_message = State()


class SubmitState(StatesGroup):
    waiting_code = State()
    waiting_answers = State()


class AIState(StatesGroup):
    waiting_question = State()

# =========================================================
# MIDDLEWARES
# =========================================================

class RateLimitMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user_id = None
        if hasattr(event, "from_user") and event.from_user:
            user_id = event.from_user.id
        if user_id:
            now = asyncio.get_event_loop().time()
            bucket = recent_messages[user_id]
            window = settings.RATE_LIMIT_WINDOW_SECONDS
            bucket[:] = [t for t in bucket if now - t < window]
            if len(bucket) >= settings.RATE_LIMIT_MESSAGES:
                if isinstance(event, Message):
                    await event.answer("⏳ Juda tez yuborildi. Bir oz sekinroq yuboring.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("⏳ Juda tez harakat. Bir oz kuting.", show_alert=False)
                return
            bucket.append(now)
        return await handler(event, data)


class SafeErrorMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception as e:
            logger.exception("Handler error: %s", e)
            try:
                if isinstance(event, Message):
                    await event.answer("⚠️ Kutilmagan xatolik yuz berdi. Bot ishlashda davom etadi.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("⚠️ Xatolik yuz berdi.", show_alert=False)
            except Exception:
                pass
            return None

# =========================================================
# GLOBALS
# =========================================================

router = Router()
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
polling_task: Optional[asyncio.Task] = None

# FastAPI app must exist before decorators below
app = FastAPI(title=settings.APP_NAME, version="3.0.0")

# =========================================================
# TELEGRAM HANDLERS
# =========================================================

async def load_tests(session: AsyncSession, page: int = 1, query: str = "", category: str = "", difficulty: str = "") -> Tuple[List[Test], int]:
    stmt = select(Test).where(Test.active == True)  # noqa: E712
    if query:
        like = f"%{query}%"
        stmt = stmt.where(or_(Test.code.ilike(like), Test.title.ilike(like), Test.topic.ilike(like), Test.category.ilike(like)))
    if category:
        stmt = stmt.where(Test.category.ilike(category))
    if difficulty:
        stmt = stmt.where(Test.difficulty.ilike(difficulty))
    stmt = stmt.order_by(Test.created_at.desc())
    q = await session.execute(stmt)
    items = list(q.scalars().all())
    total_pages = max(1, math.ceil(len(items) / settings.MAX_TESTS_PER_PAGE))
    page = clamp(page, 1, total_pages)
    start = (page - 1) * settings.MAX_TESTS_PER_PAGE
    end = start + settings.MAX_TESTS_PER_PAGE
    return items[start:end], total_pages


async def get_favorite_test_ids(session: AsyncSession, user_id: int) -> set[int]:
    q = await session.execute(select(Favorite.test_id).where(Favorite.user_id == user_id))
    return {x for x in q.scalars().all()}


async def get_user_stats(session: AsyncSession, user_id: int) -> dict:
    results = list((await session.execute(select(Result).where(Result.user_id == user_id))).scalars().all())
    avg = round(sum(r.percent_value for r in results) / len(results), 1) if results else 0.0
    best = max([r.percent_value for r in results], default=0)
    return {"avg": avg, "best": best, "count": len(results)}


async def get_leaderboard(session: AsyncSession, mode: str = "all") -> List[Tuple[User, int]]:
    stmt = select(User, func.coalesce(func.sum(Result.score), 0).label("score_sum")).outerjoin(
        Result, Result.user_id == User.id
    )
    if mode == "weekly":
        since = utcnow() - timedelta(days=7)
        stmt = stmt.where(or_(Result.created_at == None, Result.created_at >= since))  # noqa: E711
    elif mode == "monthly":
        since = utcnow() - timedelta(days=30)
        stmt = stmt.where(or_(Result.created_at == None, Result.created_at >= since))  # noqa: E711
    stmt = stmt.group_by(User.id).order_by(func.coalesce(func.sum(Result.score), 0).desc(), User.xp.desc())
    rows = (await session.execute(stmt)).all()
    return [(row[0], int(row[1] or 0)) for row in rows]


async def render_tests_page(user_id: int, page: int = 1) -> Tuple[str, InlineKeyboardMarkup]:
    state = user_filter_state[user_id]
    query = state.get("query", "")
    category = state.get("category", "")
    difficulty = state.get("difficulty", "")
    async with SessionLocal() as session:
        tests, total_pages = await load_tests(session, page=page, query=query, category=category, difficulty=difficulty)
        lines = [
            "📚 <b>Testlar ro‘yxati</b>",
            "",
            f"<b>Qidiruv:</b> {esc(query or '—')}",
            f"<b>Filter:</b> {esc(category or '—')} | {esc(difficulty or '—')}",
            "",
            "Testni tanlang:",
        ]
        if not tests:
            lines.append("\nHech narsa topilmadi.")
        return "\n".join(lines), kb_tests_page(tests, page, total_pages)


def build_profile_text(user: User, stats: dict) -> str:
    lvl, current, need = level_from_xp(user.xp)
    progress = 0 if need == 0 else round((current / need) * 100)
    return (
        "👤 <b>Profilim</b>\n\n"
        f"<b>Ism:</b> {esc(user.full_name or '-')}\n"
        f"<b>Username:</b> @{esc(user.username) if user.username else '-'}\n"
        f"<b>User ID:</b> <code>{user.telegram_id}</code>\n"
        f"<b>Level:</b> {lvl}\n"
        f"<b>XP:</b> {user.xp}\n"
        f"<b>Badge:</b> {esc(user.badge)}\n"
        f"<b>Streak:</b> {user.streak}\n"
        f"<b>Best streak:</b> {user.best_streak}\n"
        f"<b>O‘rtacha foiz:</b> {stats.get('avg', 0)}%\n"
        f"<b>Eng yaxshi natija:</b> {stats.get('best', 0)}%\n"
        f"<b>Testlar soni:</b> {stats.get('count', 0)}\n"
        f"<b>Level progress:</b> {progress}%\n"
    )


async def refresh_user_metrics(session: AsyncSession, user: User) -> None:
    results = list((await session.execute(select(Result).where(Result.user_id == user.id))).scalars().all())
    if results:
        user.avg_percent = round(sum(r.percent_value for r in results) / len(results), 1)
        user.best_percent = max(r.percent_value for r in results)
    else:
        user.avg_percent = 0.0
        user.best_percent = 0
    user.level, _, _ = level_from_xp(user.xp)
    user.badge = badge_from_level(user.level)
    await session.commit()


async def save_result(
    session: AsyncSession,
    user: User,
    test: Test,
    correct_count: int,
    wrong_count: int,
    duration_sec: int,
    raw_answers: list,
    mistakes: list,
) -> Result:
    p = percent(correct_count, len(test.answer_key))
    score = min(test.max_score, max(0, int((p / 100) * test.max_score)))
    res = Result(
        user_id=user.id,
        test_id=test.id,
        correct_count=correct_count,
        wrong_count=wrong_count,
        percent_value=p,
        score=score,
        duration_sec=duration_sec,
        status="done",
        mistakes=mistakes,
        raw_answers=raw_answers,
    )
    session.add(res)
    user.xp += score
    user.level, _, _ = level_from_xp(user.xp)
    user.badge = badge_from_level(user.level)
    user.streak, user.best_streak = await update_streak(session, user.id)
    await refresh_user_metrics(session, user)
    await session.commit()
    return res


def build_review_text(test: Test, result: Result) -> str:
    parts = [
        f"📘 <b>Review mode</b>\n\n<b>{esc(test.code)} — {esc(test.title)}</b>",
        f"<b>Foiz:</b> {result.percent_value}%",
        "",
    ]
    for idx, item in enumerate(test.answer_key, start=1):
        parts.append(f"<b>{idx}) {esc(item.get('question',''))}</b>")
        opts = item.get("options", [])
        for oi, opt in enumerate(opts):
            letter = chr(65 + oi)
            parts.append(f"   {letter}. {esc(opt)}")
        parts.append(f"   To‘g‘ri javob: <code>{esc(item.get('correct',''))}</code>")
        if result.raw_answers and idx - 1 < len(result.raw_answers):
            parts.append(f"   Sizning javobingiz: <code>{esc(result.raw_answers[idx-1])}</code>")
        if item.get("explanation"):
            parts.append(f"   Izoh: {esc(item.get('explanation'))}")
        parts.append("")
    return "\n".join(parts)


async def send_main_menu(target: Message | CallbackQuery, user: User) -> None:
    is_admin = user.telegram_id in settings.admin_ids
    text = (
        "🏠 <b>Bosh menyu</b>\n\n"
        "Bu yerda testlar, AI ustoz, natijalar, profil va admin bo‘limlari jamlangan.\n"
        "Har bir oqimda orqaga qaytish va bosh menyu tugmalari bor."
    )
    markup = kb_main(is_admin=is_admin)
    if isinstance(target, Message):
        await target.answer(text, reply_markup=markup)
    else:
        await target.message.edit_text(text, reply_markup=markup)


@router.message(CommandStart())
async def cmd_start(message: Message):
    if not message.from_user:
        return
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
    await log_message(message.from_user.id, "in", message.text or "")
    await log_event("start", {"user_id": message.from_user.id})
    await message.answer(
        "Assalomu alaykum! 👋\n\n"
        "Bu — premium math tekshiruvchi bot.\n"
        "Testlar, AI ustoz, natijalar, profil, leaderboard va admin panel bitta joyda.",
        reply_markup=kb_main(is_admin=message.from_user.id in settings.admin_ids),
    )


@router.message(Command("menu"))
async def cmd_menu(message: Message):
    if not message.from_user:
        return
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
    await send_main_menu(message, user)


@router.callback_query(F.data == "nav:home")
async def nav_home(callback: CallbackQuery):
    if not callback.from_user:
        return
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
    await callback.answer()
    await callback.message.edit_text(
        "🏠 <b>Bosh menyu</b>\n\nQuyidagi bo‘limlardan birini tanlang.",
        reply_markup=kb_main(is_admin=callback.from_user.id in settings.admin_ids),
    )


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data == "menu:tests")
async def menu_tests(callback: CallbackQuery):
    if not callback.from_user:
        return
    await callback.answer()
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
    text, markup = await render_tests_page(user.telegram_id, 1)
    await callback.message.edit_text(text, reply_markup=markup)


@router.callback_query(F.data.startswith("tests:page:"))
async def tests_page(callback: CallbackQuery):
    if not callback.from_user:
        return
    page = int(callback.data.split(":")[-1])
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
    text, markup = await render_tests_page(user.telegram_id, page)
    await callback.message.edit_text(text, reply_markup=markup)


@router.callback_query(F.data == "tests:search")
async def tests_search(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(SearchState.waiting_query)
    await callback.message.edit_text(
        "🔎 Qidiruv so‘zini yuboring.\n\nMasalan: algebra, fizika, MATH-001",
        reply_markup=kb_back_home("menu:tests"),
    )


@router.message(StateFilter(SearchState.waiting_query))
async def tests_search_input(message: Message, state: FSMContext):
    if not message.from_user:
        return
    q = sanitize_text(message.text or "")
    user_filter_state[message.from_user.id]["query"] = q
    await state.clear()
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
    text, markup = await render_tests_page(user.telegram_id, 1)
    await message.answer(text, reply_markup=markup)


@router.callback_query(F.data == "tests:filter")
async def tests_filter(callback: CallbackQuery):
    await callback.answer()
    b = InlineKeyboardBuilder()
    for cat in ["Matematika", "Fizika", "Kimyo", "Biologiya", "General"]:
        b.button(text=cat, callback_data=f"tests:cat:{cat}")
    for diff in ["Oson", "O‘rta", "Qiyin"]:
        b.button(text=diff, callback_data=f"tests:diff:{diff}")
    b.button(text="Filtrni tozalash", callback_data="tests:clear_filter")
    b.button(text="⬅️ Orqaga", callback_data="menu:tests")
    b.adjust(2, 2, 1, 1)
    await callback.message.edit_text("🎚 Filter tanlang:", reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("tests:cat:"))
async def tests_filter_cat(callback: CallbackQuery):
    cat = callback.data.split("tests:cat:", 1)[1]
    user_filter_state[callback.from_user.id]["category"] = cat
    await callback.answer(f"Filter: {cat}")
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
    text, markup = await render_tests_page(user.telegram_id, 1)
    await callback.message.edit_text(text, reply_markup=markup)


@router.callback_query(F.data.startswith("tests:diff:"))
async def tests_filter_diff(callback: CallbackQuery):
    diff = callback.data.split("tests:diff:", 1)[1]
    user_filter_state[callback.from_user.id]["difficulty"] = diff
    await callback.answer(f"Filter: {diff}")
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
    text, markup = await render_tests_page(user.telegram_id, 1)
    await callback.message.edit_text(text, reply_markup=markup)


@router.callback_query(F.data == "tests:clear_filter")
async def tests_clear_filter(callback: CallbackQuery):
    user_filter_state[callback.from_user.id] = {}
    await callback.answer("Filter tozalandi")
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
    text, markup = await render_tests_page(user.telegram_id, 1)
    await callback.message.edit_text(text, reply_markup=markup)


@router.callback_query(F.data.startswith("test:open:"))
async def test_open(callback: CallbackQuery):
    if not callback.from_user:
        return
    test_id = int(callback.data.split(":")[-1])
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        test = await get_test_by_id(session, test_id)
        if not test:
            await callback.answer("Test topilmadi", show_alert=True)
            return
        fav_ids = await get_favorite_test_ids(session, user.id)
        text = (
            f"🧪 <b>{esc(test.code)}</b>\n"
            f"<b>Sarlavha:</b> {esc(test.title)}\n"
            f"<b>Kategoriya:</b> {esc(test.category)}\n"
            f"<b>Mavzu:</b> {esc(test.topic)}\n"
            f"<b>Qiyinlik:</b> {esc(test.difficulty)}\n"
            f"<b>Sana:</b> {iso(test.created_at)}\n\n"
            f"{esc(test.preview_text or 'Preview mavjud emas.') }"
        )
        await callback.answer()
        await callback.message.edit_text(text, reply_markup=kb_test_detail(test, is_fav=test.id in fav_ids))


@router.callback_query(F.data.startswith("fav:toggle:"))
async def fav_toggle(callback: CallbackQuery):
    if not callback.from_user:
        return
    test_id = int(callback.data.split(":")[-1])
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        test = await get_test_by_id(session, test_id)
        if not test:
            await callback.answer("Test topilmadi", show_alert=True)
            return
        existing = (await session.execute(select(Favorite).where(and_(Favorite.user_id == user.id, Favorite.test_id == test_id)))).scalar_one_or_none()
        if existing:
            await session.delete(existing)
            await session.commit()
            await callback.answer("Favorites’dan olib tashlandi")
        else:
            session.add(Favorite(user_id=user.id, test_id=test_id))
            await session.commit()
            await callback.answer("Favorites’ga qo‘shildi")
        fav_ids = await get_favorite_test_ids(session, user.id)
        await callback.message.edit_reply_markup(reply_markup=kb_test_detail(test, is_fav=test.id in fav_ids))


@router.callback_query(F.data.startswith("test:submit:"))
async def submit_test_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    test_id = int(callback.data.split(":")[-1])
    async with SessionLocal() as session:
        test = await get_test_by_id(session, test_id)
        if not test:
            await callback.message.edit_text("Test topilmadi.", reply_markup=kb_back_home("menu:tests"))
            return
        await state.set_state(SubmitState.waiting_code)
        await state.update_data(test_id=test_id)
        await callback.message.edit_text(
            f"🧪 <b>{esc(test.code)} — {esc(test.title)}</b>\n\n"
            "Tekshirish uchun <b>test kodi va javoblarni</b> bitta xabarda yuboring.\n"
            "Misol: <code>MATH-001 B A C</code>\n\n"
            "Yoki faqat kod yuboring, keyin javoblarni alohida so‘rayman.",
            reply_markup=kb_back_home(f"test:open:{test_id}"),
        )


@router.message(StateFilter(SubmitState.waiting_code))
async def submit_code_input(message: Message, state: FSMContext):
    if not message.from_user:
        return
    raw = sanitize_text(message.text or "")
    code, rest = parse_test_code_and_answers(raw)
    if not code:
        await message.answer("Kod yuboring. Masalan: <code>MATH-001 B A C</code>", reply_markup=kb_back_home("menu:tests"))
        return
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
        test = await get_test_by_code(session, code)
        if not test:
            await message.answer("Test kodi topilmadi.", reply_markup=kb_back_home("menu:tests"))
            return
        if await user_already_solved(session, user.id, test.id):
            await message.answer("Bu test oldin ishlangan. Qayta ishlashga ruxsat yo‘q.", reply_markup=kb_back_home("menu:tests"))
            await state.clear()
            return
        if rest:
            await state.update_data(test_id=test.id, raw_answers=rest)
            await state.set_state(SubmitState.waiting_answers)
            await message.answer(
                "Javoblarni tekshirish uchun quyidagi formatda yuboring:\n"
                "<code>A B C</code>\n"
                "Yoki <code>1A 2B 3C</code>",
                reply_markup=kb_back_home("menu:tests"),
            )
        else:
            await state.update_data(test_id=test.id)
            await state.set_state(SubmitState.waiting_answers)
            await message.answer(
                "Endi javoblarni yuboring:\n"
                "<code>A B C</code> yoki <code>1A 2B 3C</code>",
                reply_markup=kb_back_home("menu:tests"),
            )


@router.message(StateFilter(SubmitState.waiting_answers))
async def submit_answers_input(message: Message, state: FSMContext):
    if not message.from_user:
        return
    data = await state.get_data()
    test_id = data.get("test_id")
    raw_answers_text = data.get("raw_answers") or (message.text or "")
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
        test = await get_test_by_id(session, int(test_id))
        if not test:
            await message.answer("Test topilmadi.", reply_markup=kb_back_home("menu:tests"))
            await state.clear()
            return
        if await user_already_solved(session, user.id, test.id):
            await message.answer("Bu test allaqachon ishlangan.", reply_markup=kb_back_home("menu:tests"))
            await state.clear()
            return
        raw_answers = parse_answers(raw_answers_text)
        if not raw_answers:
            await message.answer("Javob formatini to‘g‘ri yuboring: <code>A B C</code>", reply_markup=kb_back_home("menu:tests"))
            return
        correct_count, wrong_count, mistakes = compute_mistakes(test.answer_key, raw_answers)
        result = await save_result(session, user, test, correct_count, wrong_count, 0, raw_answers, mistakes)
        await log_event("test_completed", {"user_id": user.telegram_id, "test_id": test.id, "percent": result.percent_value})
        await log_message(user.telegram_id, "in", message.text or "")
        await state.clear()

        text = make_result_text(test, result)
        buttons = InlineKeyboardBuilder()
        buttons.button(text="📘 Review mode", callback_data=f"test:review:{test.id}")
        buttons.button(text="📈 Natijalarim", callback_data="menu:results")
        buttons.button(text="🏠 Bosh menyu", callback_data="nav:home")
        buttons.adjust(1, 1, 1)
        await message.answer(text, reply_markup=buttons.as_markup())

        if result.percent_value >= settings.CERTIFICATE_THRESHOLD:
            cert_code = f"CERT-{secrets.token_hex(4).upper()}"
            cert_path = generate_certificate_pdf(user.username or "", user.full_name or "", test.title, result.percent_value)
            async with SessionLocal() as cert_session:
                cert_session.add(Certificate(user_id=user.id, test_id=test.id, cert_code=cert_code, pdf_path=cert_path or ""))
                await cert_session.commit()
            if cert_path and Path(cert_path).exists():
                await message.answer_document(
                    FSInputFile(cert_path),
                    caption=f"🎓 Sertifikat tayyor: {test.title} — {result.percent_value}%",
                )


@router.callback_query(F.data.startswith("test:review:"))
async def test_review(callback: CallbackQuery):
    if not callback.from_user:
        return
    test_id = int(callback.data.split(":")[-1])
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        test = await get_test_by_id(session, test_id)
        if not test:
            await callback.answer("Topilmadi", show_alert=True)
            return
        result = (await session.execute(select(Result).where(and_(Result.user_id == user.id, Result.test_id == test.id)))).scalar_one_or_none()
        if not result:
            await callback.answer("Avval testni ishlang", show_alert=True)
            return
        await callback.answer()
        await callback.message.edit_text(build_review_text(test, result), reply_markup=kb_back_home(f"test:open:{test.id}"))


@router.callback_query(F.data == "menu:results")
async def menu_results(callback: CallbackQuery):
    if not callback.from_user:
        return
    await callback.answer()
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        q = await session.execute(select(Result, Test).join(Test, Test.id == Result.test_id).where(Result.user_id == user.id).order_by(Result.created_at.desc()).limit(10))
        rows = q.all()
        if not rows:
            await callback.message.edit_text("📈 <b>Natijalarim</b>\n\nHozircha natija yo‘q.", reply_markup=kb_back_home())
            return
        text = ["📈 <b>Natijalarim</b>\n"]
        for r, t in rows:
            text.append(f"• <b>{esc(t.code)}</b> — {r.percent_value}% | {r.correct_count}/{len(t.answer_key)} | ball {r.score}")
        stats = await get_user_stats(session, user.id)
        text.append("")
        text.append(f"<b>O‘rtacha:</b> {stats.get('avg', 0)}%")
        text.append(f"<b>Eng yaxshi:</b> {stats.get('best', 0)}%")
        text.append(f"<b>Oxirgi:</b> {rows[0][0].percent_value}%")
        await callback.message.edit_text("\n".join(text), reply_markup=kb_back_home())


@router.callback_query(F.data == "menu:profile")
async def menu_profile(callback: CallbackQuery):
    if not callback.from_user:
        return
    await callback.answer()
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        stats = await get_user_stats(session, user.id)
        text = build_profile_text(user, stats)
    await callback.message.edit_text(text, reply_markup=kb_back_home())


@router.callback_query(F.data == "menu:favorites")
async def menu_favorites(callback: CallbackQuery):
    if not callback.from_user:
        return
    await callback.answer()
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        q = await session.execute(select(Test).join(Favorite, Favorite.test_id == Test.id).where(Favorite.user_id == user.id).order_by(Favorite.created_at.desc()))
        tests = list(q.scalars().all())
        if not tests:
            await callback.message.edit_text("⭐ Favorites bo‘sh.", reply_markup=kb_back_home())
            return
        b = InlineKeyboardBuilder()
        for t in tests:
            b.button(text=f"{t.code} • {t.title[:18]}", callback_data=f"test:open:{t.id}")
        b.button(text="⬅️ Orqaga", callback_data="nav:home")
        b.adjust(1)
        await callback.message.edit_text("⭐ <b>Favorites</b>", reply_markup=b.as_markup())


@router.callback_query(F.data == "menu:daily")
async def menu_daily(callback: CallbackQuery):
    if not callback.from_user:
        return
    await callback.answer()
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        tests = list((await session.execute(select(Test).where(Test.active == True).order_by(Test.id.asc()))).scalars().all())  # noqa: E712
        if not tests:
            await callback.message.edit_text("Daily challenge uchun test yo‘q.", reply_markup=kb_back_home())
            return
        idx = daily_test_index() % len(tests)
        test = tests[idx]
        await callback.message.edit_text(
            f"🎯 <b>Daily challenge</b>\n\nBugungi test: <b>{esc(test.code)}</b> — {esc(test.title)}\nQiyinlik: {esc(test.difficulty)}",
            reply_markup=kb_test_detail(test),
        )


@router.callback_query(F.data == "menu:leaderboard")
async def menu_leaderboard(callback: CallbackQuery):
    if not callback.from_user:
        return
    await callback.answer()
    b = InlineKeyboardBuilder()
    b.button(text="Haftalik", callback_data="leaderboard:weekly")
    b.button(text="Oylik", callback_data="leaderboard:monthly")
    b.button(text="Umumiy", callback_data="leaderboard:all")
    b.button(text="⬅️ Orqaga", callback_data="nav:home")
    b.adjust(3, 1)
    await callback.message.edit_text("🏆 Reyting turini tanlang:", reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("leaderboard:"))
async def leaderboard_view(callback: CallbackQuery):
    if not callback.from_user:
        return
    mode = callback.data.split(":")[-1]
    await callback.answer()
    async with SessionLocal() as session:
        rows = await get_leaderboard(session, mode=mode)
        lines = [f"🏆 <b>Leaderboard ({mode})</b>\n"]
        if not rows:
            lines.append("Hali ma’lumot yo‘q.")
        else:
            for idx, (user, score_sum) in enumerate(rows[:10], start=1):
                lines.append(f"{idx}. {esc(user.full_name or user.username or 'User')} — XP: {user.xp} | score: {score_sum}")
        await callback.message.edit_text("\n".join(lines), reply_markup=kb_back_home())


@router.callback_query(F.data == "menu:contact")
async def menu_contact(callback: CallbackQuery, state: FSMContext):
    if not callback.from_user:
        return
    await callback.answer()
    pending_contact[callback.from_user.id] = True
    await state.set_state(ContactState.waiting_message)
    await callback.message.edit_text(
        "✉️ Adminga xabar yuboring.\n\nXabar matnini yozing, men uni admin panelga yuboraman.",
        reply_markup=kb_back_home(),
    )


@router.message(StateFilter(ContactState.waiting_message))
async def contact_message(message: Message, state: FSMContext):
    if not message.from_user:
        return
    text = sanitize_text(message.text or "")
    if not text:
        await message.answer("Xabar matni bo‘sh.", reply_markup=kb_back_home())
        return
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
    for admin_id in settings.admin_ids:
        try:
            await bot.send_message(
                admin_id,
                f"✉️ <b>Yangi xabar</b>\n\n<b>From:</b> {esc(user.full_name)}\n<b>User ID:</b> <code>{user.telegram_id}</code>\n<b>Username:</b> @{esc(user.username) if user.username else '-'}\n\n{esc(text)}",
            )
        except Exception as e:
            logger.warning("contact forward failed: %s", e)
    await state.clear()
    await message.answer("✅ Xabar adminga yuborildi.", reply_markup=kb_back_home())


@router.callback_query(F.data == "menu:ai")
async def menu_ai(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AIState.waiting_question)
    await callback.message.edit_text(
        "🧠 <b>AI Ustoz</b>\n\nSavolingizni matn ko‘rinishida yuboring.\nRasm yoki ovoz yuborsangiz ham urunib ko‘raman.",
        reply_markup=kb_back_home(),
    )


@router.message(StateFilter(AIState.waiting_question), F.photo)
async def ai_photo(message: Message, state: FSMContext):
    if not message.photo:
        return
    file = await bot.get_file(message.photo[-1].file_id)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
        await bot.download_file(file.file_path, destination=tmp)
        path = tmp.name
    caption = message.caption or ""
    result = await ai_service.analyze_image(path, prompt=caption)
    await message.answer(result, reply_markup=kb_back_home())
    await state.clear()


@router.message(StateFilter(AIState.waiting_question), F.voice)
async def ai_voice(message: Message, state: FSMContext):
    if not message.voice:
        return
    file = await bot.get_file(message.voice.file_id)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".ogg") as tmp:
        await bot.download_file(file.file_path, destination=tmp)
        path = tmp.name
    text = await ai_service.transcribe_voice(path)
    if not text:
        await message.answer("Ovoz matnga aylantirilmadi. Iltimos, savolni matn ko‘rinishida yuboring.")
        return
    result = await ai_service.answer_text(text)
    await message.answer(f"🎙 <b>Transcription</b>\n\n{esc(text)}\n\n{result}", reply_markup=kb_back_home())
    await state.clear()


@router.message(StateFilter(AIState.waiting_question))
async def ai_text(message: Message, state: FSMContext):
    if not message.from_user:
        return
    text = sanitize_text(message.text or "")
    if not text:
        await message.answer("Savol yuboring.", reply_markup=kb_back_home())
        return
    result = await ai_service.answer_text(text)
    await message.answer(result, reply_markup=kb_back_home())


@router.callback_query(F.data == "menu:admin")
async def admin_menu(callback: CallbackQuery):
    if not callback.from_user or callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    await callback.message.edit_text("🛠 <b>Admin panel</b>", reply_markup=kb_admin())


@router.callback_query(F.data == "admin:add_test")
async def admin_add_test(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    await state.set_state(AddTestState.waiting_json)
    await callback.message.edit_text(
        "➕ <b>Test qo‘shish</b>\n\nTestni JSON formatda yuboring.\n\n<code>{\"code\":\"MATH-002\",\"title\":\"...\",\"category\":\"Matematika\",\"topic\":\"Algebra\",\"difficulty\":\"O‘rta\",\"pdf_url\":\"\",\"preview_text\":\"...\",\"max_score\":20,\"answer_key\":[{\"question\":\"...\",\"options\":[\"A\",\"B\",\"C\",\"D\"],\"correct\":\"B\",\"explanation\":\"...\"}]}</code>",
        reply_markup=kb_back_home("menu:admin"),
    )


@router.message(StateFilter(AddTestState.waiting_json))
async def admin_add_test_input(message: Message, state: FSMContext):
    if not message.from_user or message.from_user.id not in settings.admin_ids:
        return
    raw = message.text or ""
    try:
        data = json.loads(raw)
        for key in ["code", "title", "answer_key"]:
            if key not in data:
                raise ValueError(f"{key} missing")
        async with SessionLocal() as session:
            existing = await get_test_by_code(session, str(data["code"]))
            if existing:
                await message.answer("Bu kod allaqachon bor.", reply_markup=kb_back_home("menu:admin"))
                await state.clear()
                return
            test = Test(
                code=str(data["code"]).strip().upper(),
                title=str(data.get("title", "")).strip(),
                category=str(data.get("category", "General")).strip(),
                topic=str(data.get("topic", "General")).strip(),
                difficulty=str(data.get("difficulty", "Medium")).strip(),
                pdf_url=str(data.get("pdf_url", "")).strip(),
                preview_text=str(data.get("preview_text", "")).strip(),
                answer_key=data.get("answer_key", []),
                max_score=int(data.get("max_score", 20)),
                active=bool(data.get("active", True)),
            )
            session.add(test)
            await session.commit()
        await log_admin(message.from_user.id, "add_test", {"code": data["code"]})
        await state.clear()
        await message.answer("✅ Test qo‘shildi.", reply_markup=kb_back_home("menu:admin"))
    except Exception as e:
        await message.answer(f"⚠️ JSON noto‘g‘ri.\n\n{esc(str(e))}", reply_markup=kb_back_home("menu:admin"))


@router.callback_query(F.data == "admin:delete_test")
async def admin_delete_test(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    await state.set_state(DeleteTestState.waiting_code)
    await callback.message.edit_text(
        "🗑 <b>Test o‘chirish</b>\n\nTest kodini yuboring.\nMasalan: <code>MATH-001</code>",
        reply_markup=kb_back_home("menu:admin"),
    )


@router.message(StateFilter(DeleteTestState.waiting_code))
async def admin_delete_test_input(message: Message, state: FSMContext):
    if not message.from_user or message.from_user.id not in settings.admin_ids:
        return
    code = sanitize_text(message.text or "").upper()
    async with SessionLocal() as session:
        test = await get_test_by_code(session, code)
        if not test:
            await message.answer("Test topilmadi.", reply_markup=kb_back_home("menu:admin"))
            return
        await session.delete(test)
        await session.commit()
    await log_admin(message.from_user.id, "delete_test", {"code": code})
    await state.clear()
    await message.answer("✅ Test o‘chirildi.", reply_markup=kb_back_home("menu:admin"))


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    await state.set_state(BroadcastState.waiting_text)
    await callback.message.edit_text(
        "📣 Ommaviy xabar matnini yuboring.\n\nXabar barcha foydalanuvchilarga yuboriladi.",
        reply_markup=kb_back_home("menu:admin"),
    )


@router.message(StateFilter(BroadcastState.waiting_text))
async def admin_broadcast_input(message: Message, state: FSMContext):
    if not message.from_user or message.from_user.id not in settings.admin_ids:
        return
    text = sanitize_text(message.text or "")
    if not text:
        await message.answer("Xabar bo‘sh.", reply_markup=kb_back_home("menu:admin"))
        return
    async with SessionLocal() as session:
        users = list((await session.execute(select(User))).scalars().all())
    ok = 0
    for user in users:
        try:
            await bot.send_message(user.telegram_id, f"📣 <b>Admin xabari</b>\n\n{esc(text)}")
            ok += 1
        except Exception:
            pass
    await log_admin(message.from_user.id, "broadcast", {"sent": ok})
    await state.clear()
    await message.answer(f"✅ Yuborildi: {ok} ta user.", reply_markup=kb_back_home("menu:admin"))


@router.callback_query(F.data == "admin:stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    async with SessionLocal() as session:
        users_count = (await session.execute(select(func.count(User.id)))).scalar_one()
        tests_count = (await session.execute(select(func.count(Test.id)))).scalar_one()
        results_count = (await session.execute(select(func.count(Result.id)))).scalar_one()
        avg_percent = (await session.execute(select(func.avg(Result.percent_value)))).scalar_one() or 0
        top_test = (
            await session.execute(
                select(Test.title, func.count(Result.id).label("cnt"))
                .join(Result, Result.test_id == Test.id)
                .group_by(Test.id)
                .order_by(func.count(Result.id).desc())
                .limit(1)
            )
        ).first()
        text = (
            "📊 <b>Statistika</b>\n\n"
            f"<b>Users:</b> {users_count}\n"
            f"<b>Tests:</b> {tests_count}\n"
            f"<b>Results:</b> {results_count}\n"
            f"<b>O‘rtacha foiz:</b> {round(float(avg_percent or 0), 1)}%\n"
            f"<b>Eng ko‘p ishlangan test:</b> {esc(top_test[0]) if top_test else '-'}"
        )
    await callback.message.edit_text(text, reply_markup=kb_back_home("menu:admin"))


@router.callback_query(F.data == "admin:top")
async def admin_top(callback: CallbackQuery):
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    async with SessionLocal() as session:
        rows = await get_leaderboard(session, mode="all")
        text = "🏅 <b>Top userlar</b>\n\n"
        for idx, (user, score_sum) in enumerate(rows[:10], start=1):
            text += f"{idx}. {esc(user.full_name or user.username or 'User')} — XP: {user.xp} | score: {score_sum}\n"
    await callback.message.edit_text(text, reply_markup=kb_back_home("menu:admin"))


@router.callback_query(F.data == "admin:logs")
async def admin_logs(callback: CallbackQuery):
    if callback.from_user.id not in settings.admin_ids:
        await callback.answer("Ruxsat yo‘q", show_alert=True)
        return
    await callback.answer()
    async with SessionLocal() as session:
        rows = list((await session.execute(select(AdminLog).order_by(AdminLog.created_at.desc()).limit(10))).scalars().all())
        text = "🧾 <b>Loglar</b>\n\n"
        if not rows:
            text += "Log yo‘q."
        else:
            for r in rows:
                text += f"• {iso(r.created_at)} — {esc(r.action)}\n"
    await callback.message.edit_text(text, reply_markup=kb_back_home("menu:admin"))


@router.message()
async def catch_all(message: Message):
    if not message.from_user:
        return
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
    await log_message(message.from_user.id, "in", message.text or message.caption or "")
    await message.answer(
        "Men bu xabarni maxsus oqimdan tashqarida qabul qildim.\n"
        "Testlar, AI, profil yoki admin paneldan birini tanlang.",
        reply_markup=kb_main(is_admin=message.from_user.id in settings.admin_ids),
    )

# =========================================================
# FASTAPI + WEBHOOK
# =========================================================

@app.get("/health")
async def health():
    return JSONResponse(
        {
            "status": "ok",
            "app": settings.APP_NAME,
            "env": settings.APP_ENV,
            "bot_token": bool(settings.BOT_TOKEN),
            "webhook": bool(settings.WEBHOOK_URL),
        }
    )


@app.get("/")
async def root():
    return PlainTextResponse("math_tekshiruvchi_bot is running")


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    global bot, dp
    if not bot or not dp:
        raise HTTPException(status_code=503, detail="Bot not ready")
    if settings.WEBHOOK_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if secret != settings.WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Forbidden")
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return JSONResponse({"ok": True})

# =========================================================
# LIFESPAN
# =========================================================

async def setup_bot() -> None:
    global bot, dp
    if not settings.BOT_TOKEN:
        logger.warning("BOT_TOKEN not set. Bot will not start, but web server will stay alive.")
        return

    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(RateLimitMiddleware())
    dp.update.middleware(SafeErrorMiddleware())
    dp.include_router(router)

    if settings.WEBHOOK_URL:
        webhook_full = f"{settings.WEBHOOK_URL}{settings.WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_full,
            secret_token=settings.WEBHOOK_SECRET if settings.WEBHOOK_SECRET else None,
            drop_pending_updates=True,
        )
        logger.info("Webhook set: %s", webhook_full)
    else:
        logger.info("Polling fallback mode will be used.")


async def start_polling_background() -> None:
    global bot, dp
    if not bot or not dp:
        return
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.exception("Polling crashed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot, dp, polling_task

    await init_db()
    await seed_default_tests()
    await setup_bot()

    if settings.BOT_TOKEN and not settings.WEBHOOK_URL and dp and bot:
        polling_task = asyncio.create_task(start_polling_background())
        logger.info("Polling task started.")

    yield

    if polling_task and not polling_task.done():
        polling_task.cancel()
    if bot and settings.WEBHOOK_URL:
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception as e:
            logger.warning("delete_webhook failed: %s", e)
    if bot:
        await bot.session.close()


app.router.lifespan_context = lifespan

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=False,
        log_level=settings.LOG_LEVEL.lower(),
        workers=1,
    )
