# A#!/usr/bin/env python3
import io as _io
from typing import Any, Callable, Dict, List, Optional, Tuple
import uuid
import time
import threading
import logging
import json
import asyncio
import asyncio
import json
import os
import signal
import sys
import time
import base64
import hashlib
import socket
import uuid
import threading
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
import requests
from colorama import init, Fore, Style
from PIL import Image, ImageDraw, ImageFont, ImageFilter

from telethon import Button as TelethonButton

def telethon_button(text, callback_data):
    """Return a Telethon inline button."""
    return TelethonButton.inline(text, callback_data)

# --- Telethon Imports ---
from telethon import TelegramClient, Button
from telethon.tl.types import MessageEntityCustomEmoji as TelethonCustomEmoji, MessageEntityBold as TelethonBold

# --- Telegram Bot API Imports (Manual Compatibility Layer) ---
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    MessageEntity
)

# SERVER KO DHOKA DENE KE LIYE (Imports Crash Fix)
try:
    from telegram import KeyboardButtonStyle
except ImportError:
    try:
        from telegram.constants import KeyboardButtonStyle
    except ImportError:
        # Agar dono jagah na mile to manually class bana rahe hain taake crash
        # na ho
        class KeyboardButtonStyle:
            DEFAULT = 'default'
            PRIMARY = 'primary'
            SECONDARY = 'secondary'
            SUCCESS = 'success'
            DANGER = 'danger'

from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
)

# =============================================
#              24/7 UPTIME LAYER
# =============================================


def run_uptime_server():

    pass

    try:
        import os
        from flask import Flask
        app = Flask(__name__)
        @app.route('/')
        def home(): return "SMZX AI MODE BOT IS LIVE 24/7", 200

        # Render dynamic port use karta hai, isliye os.environ zaroori hai
        port = int(os.environ.get("PORT", 8080))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        print(f"Flask Server Error: {e}")


threading.Thread(target=run_uptime_server, daemon=True).start()
# =============================================

# ════════════════════════════════════════════════════════════════
#  TradoWix client (merged from tradowix_client.py — single-file)
# ════════════════════════════════════════════════════════════════


try:
    import websockets
    import websockets.sync.client as ws_sync
except ImportError:
    websockets = None

logger = logging.getLogger("tradowix")

API_BASE = "https://api.tradowix.com"
WS_URL = "wss://api.tradowix.com/ws"
FRONTEND_BASE = "https://tradowix.com"
ORIGIN = "https://tradowix.com"


class TradoWixError(Exception):
    pass


class AuthenticationError(TradoWixError):
    pass


class TradeError(TradoWixError):
    pass


class TradoWixClient:
    """
    TradoWix trading client.

    Students only need to provide email + password.
    Session token is obtained automatically via login.
    """

    def __init__(
            self,
            email: Optional[str] = None,
            password: Optional[str] = None):
        self.email = email
        self.password = password
        self.session_token: Optional[str] = None
        self.user_id: Optional[str] = None
        self.trader_id: Optional[int] = None
        self.user_info: Optional[Dict] = None

        # WebSocket state
        self._ws = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._connected = False
        self._authenticated = False
        self._stop_event = threading.Event()

        # Data storage
        self.instruments: List[Dict] = []
        self.balance: Dict = {}
        self._candle_history: Dict[str, List] = {}
        self._tick_buffers: Dict[str, List] = {}
        self._subscribed_symbols: set = set()

        # Callbacks
        self._tick_callbacks: Dict[str, List[Callable]] = {}
        self._trade_opened_callbacks: List[Callable] = []
        self._trade_result_callbacks: List[Callable] = []
        self._balance_callbacks: List[Callable] = []
        self._candle_callbacks: Dict[str, List[Callable]] = {}

        # Pending RPC/trade responses
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._candle_events: Dict[str, threading.Event] = {}

        # HTTP session
        self._http = requests.Session()
        self._http.headers.update({
            "Content-Type": "application/json",
            "Origin": ORIGIN,
            "Referer": f"{FRONTEND_BASE}/trading",
        })

    # ─────────────────────────────────────────────
    #  1. AUTHENTICATION (email/password → token)
    # ─────────────────────────────────────────────

    def login(self, email: Optional[str] = None,
              password: Optional[str] = None) -> Dict:
        """
        Login with email/password. Returns user info dict.
        Session token is stored internally — no need to copy-paste tokens.
        """
        email = email or self.email
        password = password or self.password
        if not email or not password:
            raise AuthenticationError("Email and password are required")

        self.email = email
        self.password = password

        resp = self._http.post(
            f"{FRONTEND_BASE}/api/auth/login",
            json={"email": email, "password": password},
            timeout=15,
        )

        if resp.status_code != 200:
            raise AuthenticationError(f"Login failed: HTTP {resp.status_code}")

        data = resp.json()
        if not data.get("success"):
            msg = data.get("message") or data.get("error") or "Login failed"
            raise AuthenticationError(msg)

        self.session_token = data["sessionToken"]
        self.user_info = data.get("user", {})
        self.user_id = self.user_info.get("id")
        self.trader_id = self.user_info.get("traderId")

        # Set cookie for future REST calls
        self._http.cookies.set(
            "session-token",
            self.session_token,
            domain=".tradowix.com")

        logger.info(
            "Logged in as %s (trader %s)",
            self.user_info.get("displayName"),
            self.trader_id)
        return self.user_info

    def login_with_token(self, session_token: str) -> Dict:
        """Login using an existing session token (for advanced users)."""
        self.session_token = session_token
        self._http.cookies.set(
            "session-token",
            session_token,
            domain=".tradowix.com")

        resp = self._http.get(f"{API_BASE}/api/auth/me", timeout=10)
        if resp.status_code != 200:
            raise AuthenticationError(
                f"Token invalid: HTTP {
                    resp.status_code}")

        data = resp.json()
        if not data.get("success"):
            raise AuthenticationError("Token validation failed")

        self.user_info = data.get("user", {})
        self.user_id = self.user_info.get("id")
        self.trader_id = self.user_info.get("traderId")

        logger.info(
            "Token login: %s (trader %s)",
            self.user_info.get("displayName"),
            self.trader_id)
        return self.user_info

    # ─────────────────────────────────────────────
    #  2. WEBSOCKET CONNECTION
    # ─────────────────────────────────────────────

    def connect(self, blocking: bool = False):
        """
        Connect to TradoWix WebSocket.
        If blocking=False (default), runs in background thread.
        If blocking=True, blocks the current thread.
        """
        if not self.session_token:
            raise AuthenticationError("Login first before connecting")

        if websockets is None:
            raise ImportError("Install websockets: pip install websockets")

        if blocking:
            asyncio.run(self._ws_main_loop())
        else:
            self._stop_event.clear()
            self._ws_thread = threading.Thread(
                target=self._run_ws_thread, daemon=True)
            self._ws_thread.start()
            # Wait for connection + auth + instruments
            for _ in range(150):
                if self._authenticated and self.instruments:
                    break
                time.sleep(0.1)
            if not self._authenticated:
                raise ConnectionError("WebSocket authentication timed out")

    def _run_ws_thread(self):
        self._ws_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._ws_loop)
        try:
            self._ws_loop.run_until_complete(self._ws_main_loop())
        except Exception as e:
            logger.error("WebSocket thread error: %s", e)
        finally:
            self._connected = False
            self._authenticated = False

    async def _ws_main_loop(self):
        url = f"{WS_URL}?token={self.session_token}"
        retry_delay = 1

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(url, origin=ORIGIN, ping_interval=20, ping_timeout=10) as ws:
                    self._ws = ws
                    self._connected = True
                    retry_delay = 1
                    logger.info("WebSocket connected")

                    await self._handle_messages(ws)

            except (websockets.exceptions.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning(
                    "WebSocket disconnected: %s. Reconnecting in %ds...",
                    e,
                    retry_delay)
                self._connected = False
                self._authenticated = False
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
            except asyncio.CancelledError:
                break

    async def _handle_messages(self, ws):
        async for raw in ws:
            if self._stop_event.is_set():
                break
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")
            data = msg.get("data")

            if msg_type == "authRequired":
                await ws.send(json.dumps({"type": "authenticate", "token": self.session_token}))

            elif msg_type == "authenticated":
                self._authenticated = True
                logger.info("WebSocket authenticated")
                # Resubscribe
                for sym in list(self._subscribed_symbols):
                    await self._send_subscribe_ticks(ws, sym)

            elif msg_type == "instruments":
                self.instruments = data if isinstance(data, list) else []
                logger.info("Received %d instruments", len(self.instruments))

            elif msg_type == "balanceUpdate":
                self.balance = data.get("balance", {}) if data else {}
                for cb in self._balance_callbacks:
                    self._safe_call(cb, self.balance)

            elif msg_type == "candleHistory":
                if data:
                    symbol = (data.get("symbol") or "").upper()
                    candles_raw = data.get("candles", [])
                    timeframe = data.get("timeframe", 60)
                    current_ticks = data.get("currentPeriodTicks", [])
                    candles = self._parse_candles(candles_raw, timeframe)
                    candles = self._fill_missing_candles(candles, timeframe)
                    self._candle_history[symbol] = candles
                    if symbol in self._candle_events:
                        self._candle_events[symbol].set()
                    for cb in self._candle_callbacks.get(symbol, []):
                        self._safe_call(cb, candles, symbol)

            elif msg_type == "tickUpdate":
                if data:
                    symbol = (data.get("symbol") or "").upper()
                    tick = data.get("tick", [])
                    if len(tick) >= 2:
                        price, ts = tick[0], tick[1]
                        if symbol not in self._tick_buffers:
                            self._tick_buffers[symbol] = []
                        self._tick_buffers[symbol].append(
                            {"price": price, "timestamp": ts})
                        # Keep buffer reasonable
                        if len(self._tick_buffers[symbol]) > 5000:
                            self._tick_buffers[symbol] = self._tick_buffers[symbol][-3000:]
                        for cb in self._tick_callbacks.get(symbol, []):
                            self._safe_call(cb, price, ts, symbol)
                        # Update last candle in history
                        self._update_live_candle(symbol, price, ts)

            elif msg_type == "tickSubscribed":
                if data:
                    sym = (data.get("symbol") or "").upper()
                    logger.info("Subscribed to ticks: %s", sym)

            elif msg_type == "quote":
                pass  # Lightweight quote, handled via tickUpdate

            elif msg_type == "tradeOpened":
                if data:
                    for cb in self._trade_opened_callbacks:
                        self._safe_call(cb, data)

            elif msg_type == "tradeResult":
                if data:
                    for cb in self._trade_result_callbacks:
                        self._safe_call(cb, data)

            elif msg_type == "tradeFailed":
                error = data.get(
                    "error", "Trade failed") if data else "Trade failed"
                req_id = msg.get("requestId")
                if req_id and req_id in self._pending_responses:
                    self._pending_responses[req_id] = {"error": error}
                logger.warning("Trade failed: %s", error)

            elif msg_type == "tradeCancelled":
                if data:
                    trade_id = data.get("tradeId")
                    logger.info("Trade cancelled: %s", trade_id)

            elif msg_type == "openTrades":
                pass  # Can be handled via callbacks

            elif msg_type == "tradeHistory":
                pass

            elif msg_type == "pong":
                pass

            elif msg_type == "error":
                error = data.get(
                    "error", "Unknown error") if data else "Unknown error"
                req_id = msg.get("requestId")
                if req_id and req_id in self._pending_responses:
                    self._pending_responses[req_id] = {"error": error}
                logger.warning("WS error: %s", error)

    async def _send_subscribe_ticks(
            self,
            ws,
            symbol: str,
            lookback: int = 300,
            timeframe: int = 60):
        await ws.send(json.dumps({
            "type": "subscribeTicks",
            "symbol": symbol.upper(),
            "lookbackMinutes": lookback,
            "timeframe": timeframe,
            "chartType": "candle",
        }))

    def _send_ws_message(self, msg: dict):
        if self._ws_loop and self._ws and self._connected:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps(msg)),
                    self._ws_loop,
                ).result(timeout=5)  # Wait for message to be sent
            except Exception as e:
                logger.error("WS send error: %s", e)

    @staticmethod
    def _safe_call(cb, *args):
        try:
            cb(*args)
        except Exception as e:
            logger.error("Callback error: %s", e)

    # ─────────────────────────────────────────────
    #  3. OHLC CANDLE DATA (gap-filled)
    # ─────────────────────────────────────────────

    @staticmethod
    def _parse_candles(raw_candles: list, timeframe: int = 60) -> List[Dict]:
        """
        Convert raw candle arrays [timestamp, O, H, L, C] → list of dicts.
        Compatible with aimode3.py candle format: {open, high, low, close, volume, time}
        """
        candles = []
        for c in raw_candles:
            if isinstance(c, list) and len(c) >= 5:
                candles.append({
                    "time": int(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5]) if len(c) > 5 else 1,
                })
        return candles

    @staticmethod
    def _fill_missing_candles(
            candles: List[Dict],
            timeframe: int = 60) -> List[Dict]:
        """
        Fill gaps in candle data. If consecutive candles have timestamp gap > timeframe,
        insert synthetic candles using the previous close as OHLC values.
        This fixes the 4-5 missing candle issue on 1m timeframes.
        """
        if not candles or len(candles) < 2:
            return candles

        timeframe_ms = timeframe * 1000
        filled = [candles[0]]

        for i in range(1, len(candles)):
            prev = filled[-1]
            curr = candles[i]
            expected_ts = prev["time"] + timeframe_ms

            # Fill gaps
            while expected_ts < curr["time"] - (timeframe_ms // 2):
                filled.append({
                    "time": expected_ts,
                    "open": prev["close"],
                    "high": prev["close"],
                    "low": prev["close"],
                    "close": prev["close"],
                    "volume": 0,
                })
                prev = filled[-1]
                expected_ts = prev["time"] + timeframe_ms

            filled.append(curr)

        return filled

    def _update_live_candle(self, symbol: str, price: float, timestamp: int):
        """Update the latest candle or create a new one from live ticks."""
        candles = self._candle_history.get(symbol)
        if not candles:
            return

        last = candles[-1]
        timeframe_ms = 60000  # default 1m

        if timestamp >= last["time"] + timeframe_ms:
            new_ts = (timestamp // timeframe_ms) * timeframe_ms
            # Fill any gap between last candle and new one
            expected_ts = last["time"] + timeframe_ms
            while expected_ts < new_ts:
                candles.append({
                    "time": expected_ts,
                    "open": last["close"],
                    "high": last["close"],
                    "low": last["close"],
                    "close": last["close"],
                    "volume": 0,
                })
                last = candles[-1]
                expected_ts += timeframe_ms

            # New candle with actual tick
            new_candle = {
                "time": new_ts,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1,
            }
            candles.append(new_candle)
        else:
            # Update existing candle
            last["close"] = price
            if price > last["high"]:
                last["high"] = price
            if price < last["low"]:
                last["low"] = price
            last["volume"] += 1

    def subscribe(
            self,
            symbol: str,
            lookback_minutes: int = 300,
            timeframe: int = 60):
        """Subscribe to a symbol's tick stream and candle history."""
        symbol = symbol.upper()
        self._subscribed_symbols.add(symbol)
        self._candle_events[symbol] = threading.Event()
        self._send_ws_message({
            "type": "subscribeTicks",
            "symbol": symbol,
            "lookbackMinutes": lookback_minutes,
            "timeframe": timeframe,
            "chartType": "candle",
        })

    def unsubscribe(self, symbol: str):
        """Unsubscribe from a symbol's tick stream."""
        symbol = symbol.upper()
        self._subscribed_symbols.discard(symbol)
        self._send_ws_message({"type": "unsubscribeTicks", "symbol": symbol})

    def get_candles(
            self,
            symbol: str,
            timeframe: int = 60,
            count: int = 200,
            lookback_minutes: int = 0,
            timeout: float = 10.0) -> List[Dict]:
        """
        Get OHLC candle data for a symbol.
        Returns list of dicts: [{time, open, high, low, close, volume}, ...]
        Automatically fills missing candles.
        If not already subscribed, subscribes and waits for data.

        Args:
            symbol: Instrument symbol (e.g., "EURUSD-OTC")
            timeframe: Candle period in seconds (60 = 1min, 300 = 5min)
            count: Number of candles desired
            lookback_minutes: How many minutes of history (0 = auto-calculate)
            timeout: Max seconds to wait for data
        """
        symbol = symbol.upper()

        if lookback_minutes <= 0:
            lookback_minutes = max((count * timeframe) // 60 + 30, 200)

        if symbol not in self._subscribed_symbols:
            self.subscribe(symbol, lookback_minutes, timeframe)

        # Wait for candle data
        event = self._candle_events.get(symbol)
        if event:
            event.wait(timeout=timeout)

        candles = self._candle_history.get(symbol, [])
        if count and len(candles) > count:
            candles = candles[-count:]

        return candles

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get the latest price for a symbol."""
        symbol = symbol.upper()
        ticks = self._tick_buffers.get(symbol, [])
        if ticks:
            return ticks[-1]["price"]
        candles = self._candle_history.get(symbol, [])
        if candles:
            return candles[-1]["close"]
        return None

    # ─────────────────────────────────────────────
    #  4. TRADING
    # ─────────────────────────────────────────────

    def place_trade(self, symbol: str, direction: str, amount: float,
                    duration_minutes: int = 1, is_demo: bool = True,
                    mode: str = "turbo", duration_seconds: int = 0,
                    tournament_id: Optional[str] = None) -> str:
        """
        Place a binary options trade.

        Args:
            symbol: e.g., "EURUSD-OTC"
            direction: "higher" or "lower" (also accepts "call"/"put")
            amount: Trade amount in USD
            duration_minutes: Expiry in minutes (for turbo mode: 1,2,3,4,5,10,15,30)
            is_demo: True for demo account
            mode: "turbo" (minutes) or "blitz" (seconds)
            duration_seconds: Expiry in seconds (for blitz mode: 60,90,120,150,300)
            tournament_id: Optional tournament ID

        Returns:
            requestId string for tracking
        """
        direction = direction.lower()
        if direction in ("higher", "up", "buy"):
            direction = "call"
        elif direction in ("lower", "down", "sell"):
            direction = "put"

        if direction not in ("call", "put"):
            raise TradeError(
                f"Invalid direction: {direction}. Use 'call'/'put' or 'higher'/'lower'")

        request_id = f"trade-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"

        msg = {
            "type": "placeTrade",
            "requestId": request_id,
            "symbol": symbol.upper(),
            "direction": direction,
            "amount": amount,
            "expirationMode": mode,
            "isDemo": is_demo,
        }

        if mode == "turbo":
            msg["turboMinutes"] = duration_minutes
        elif mode == "blitz":
            msg["duration"] = duration_seconds or (duration_minutes * 60)

        if tournament_id:
            msg["tournamentId"] = tournament_id

        self._send_ws_message(msg)
        logger.info(
            "Trade placed: %s %s %s $%s (%s)",
            request_id,
            symbol,
            direction,
            amount,
            mode)
        return request_id

    def cancel_trade(self, trade_id: str):
        """Cancel an active trade by ID."""
        self._send_ws_message({
            "type": "cancelTrade",
            "requestId": f"cancel-{int(time.time() * 1000)}",
            "tradeId": trade_id,
        })

    def get_open_trades(self, is_demo: bool = True):
        """Request the list of currently open trades."""
        self._send_ws_message({"type": "getOpenTrades", "isDemo": is_demo})

    def get_trade_history(
            self,
            is_demo: bool = True,
            page: int = 1,
            page_size: int = 50):
        """Request trade history via WebSocket."""
        self._send_ws_message({
            "type": "getTradeHistory",
            "isDemo": is_demo,
            "page": page,
            "pageSize": page_size,
        })

    # ─────────────────────────────────────────────
    #  5. EVENT CALLBACKS
    # ─────────────────────────────────────────────

    def on_tick(self, symbol: str, callback: Callable):
        """
        Register a callback for live tick updates.
        callback(price: float, timestamp: int, symbol: str)
        """
        symbol = symbol.upper()
        if symbol not in self._tick_callbacks:
            self._tick_callbacks[symbol] = []
        self._tick_callbacks[symbol].append(callback)
        if symbol not in self._subscribed_symbols:
            self.subscribe(symbol)

    def on_candle(self, symbol: str, callback: Callable):
        """
        Register a callback for candle history updates.
        callback(candles: list, symbol: str)
        """
        symbol = symbol.upper()
        if symbol not in self._candle_callbacks:
            self._candle_callbacks[symbol] = []
        self._candle_callbacks[symbol].append(callback)

    def on_trade_opened(self, callback: Callable):
        """callback(trade_data: dict)"""
        self._trade_opened_callbacks.append(callback)

    def on_trade_result(self, callback: Callable):
        """callback(result_data: dict)"""
        self._trade_result_callbacks.append(callback)

    def on_balance_update(self, callback: Callable):
        """callback(balance: dict)"""
        self._balance_callbacks.append(callback)

    # ─────────────────────────────────────────────
    #  6. REST API HELPERS
    # ─────────────────────────────────────────────

    def get_balance(self) -> Dict:
        """Fetch current balance via REST API."""
        resp = self._http.get(f"{API_BASE}/api/user/balance", timeout=10)
        if resp.status_code == 200:
            self.balance = resp.json()
            return self.balance
        return {}

    def get_user_info(self) -> Dict:
        """Fetch current user profile."""
        resp = self._http.get(f"{API_BASE}/api/auth/me", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            self.user_info = data.get("user", {})
            return self.user_info
        return {}

    def reset_demo(self) -> bool:
        """Reset demo account balance to default."""
        resp = self._http.get(f"{API_BASE}/api/user/demo/reset", timeout=10)
        return resp.status_code == 200

    def get_payment_methods(self) -> Dict:
        """Get available payment/withdrawal methods."""
        resp = self._http.get(f"{API_BASE}/api/payment/methods", timeout=10)
        return resp.json() if resp.status_code == 200 else {}

    def get_user_settings(self) -> Dict:
        """Get user settings (default amounts, favorites, etc.)."""
        resp = self._http.get(f"{API_BASE}/api/user-settings", timeout=10)
        return resp.json() if resp.status_code == 200 else {}

    def get_instruments_list(self) -> List[Dict]:
        """Return cached instruments list (from WebSocket)."""
        return self.instruments

    def find_instrument(self, symbol: str) -> Optional[Dict]:
        """Find an instrument by symbol name."""
        symbol = symbol.upper()
        for inst in self.instruments:
            if inst.get("symbol", "").upper() == symbol:
                return inst
        return None

    def get_payout(self, symbol: str) -> float:
        """Get the turbo payout rate for a symbol (e.g., 0.92 = 92%)."""
        inst = self.find_instrument(symbol)
        if inst:
            return inst.get(
                "effectiveTurboPayoutRate", inst.get(
                    "turboPayoutRate", 0))
        return 0

    # ─────────────────────────────────────────────
    #  7. DISCONNECT
    # ─────────────────────────────────────────────

    def disconnect(self):
        """Close WebSocket connection and stop background thread."""
        self._stop_event.set()
        if self._ws and self._ws_loop:
            asyncio.run_coroutine_threadsafe(self._ws.close(), self._ws_loop)
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=5)
        self._connected = False
        self._authenticated = False
        logger.info("Disconnected")

    @property
    def is_connected(self) -> bool:
        return self._connected and self._authenticated

    # ─────────────────────────────────────────────
    #  8. CONVENIENCE — fetch_data() compatible
    # ─────────────────────────────────────────────

    def fetch_data(self,
                   pair: str,
                   limit: int = 600) -> Tuple[Optional[List[Dict]],
                                              Optional[float],
                                              str]:
        """
        Drop-in replacement for SMZXBot.fetch_data().
        Returns (candles, current_price, payout_str) — same format as quotex proxy.

        Example:
            candles, price, payout = client.fetch_data("EURUSD-OTC", 600)
        """
        symbol = self._normalize_symbol(pair)
        candles = self.get_candles(symbol, timeframe=60, count=limit)
        if not candles:
            return None, None, "0"

        current_price = candles[-1]["close"]
        payout_rate = self.get_payout(symbol)
        payout_str = str(int(payout_rate * 100)) if payout_rate else "92"

        return candles, current_price, payout_str

    @staticmethod
    def _normalize_symbol(pair: str) -> str:
        """
        Convert various pair formats to TradoWix symbol format.
        EURUSD_OTC → EURUSD-OTC
        EURUSD → EURUSD
        EUR/USD (OTC) → EURUSD-OTC
        """
        pair = pair.strip().upper()
        pair = pair.replace("/", "").replace(" ", "")
        pair = pair.replace("(OTC)", "-OTC")
        pair = pair.replace("_OTC", "-OTC")
        pair = pair.replace("_", "")
        return pair


# ════════════ end TradoWix client ════════════

# ====================== USER TELEGRAM SENDER (Premium Account) ======================

class UserTelegramSender:
    def __init__(self):
        self.client = None
        self.loop = None
        self.ready = False

    def start(self):
        from telethon.sessions import StringSession
        import os

        session_string = os.environ.get("TG_SESSION_STRING")
        if not session_string:
            raise RuntimeError("TG_SESSION_STRING environment variable not set!")

        async def init():
            self.client = TelegramClient(StringSession(session_string), int(USER_API_ID), USER_API_HASH)
            await self.client.start()
            self.ready = True
            print(f"{Fore.GREEN}[✓] User Telegram (Premium) ready – using StringSession.{Style.RESET_ALL}")
            while True:
                await asyncio.sleep(60)

        def run_loop():
            if sys.platform == 'win32':
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(init())
            self.loop.run_forever()

        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
        timeout = 30
        start_time = time.time()
        while not self.ready and time.time() - start_time < timeout:
            time.sleep(0.5)
        if not self.ready:
            raise RuntimeError("User Telegram init timeout")

    def _run_async(self, coro, timeout=30):
        if not self.ready:
            return None
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def _build_entities(self, text, add_bold=False):
      entities = []
      offset = 0
      for ch in text:
        clen = len(ch.encode('utf-16-le')) // 2
        eid = PREMIUM_EMOJI_IDS.get(ch) or FORMAT2_EMOJI_IDS.get(ch)
        if eid:
            entities.append(TelethonCustomEmoji(offset=offset, length=clen, document_id=eid))
        offset += clen
      if add_bold and text:
        total_len = len(text.encode('utf-16-le')) // 2
        entities.append(TelethonBold(offset=0, length=total_len))
      return entities

    def send_message(self, chat_id, text):
        if not self.ready:
            print(f"{Fore.RED}[!] User Telegram not ready.{Style.RESET_ALL}")
            return False
        async def _send():
            entities = self._build_entities(text, add_bold=False)
            await self.client.send_message(chat_id, text, formatting_entities=entities)
        return self._run_async(_send())

    def send_bold_message(self, chat_id, text):
        if not self.ready:
            print(f"{Fore.RED}[!] User Telegram not ready.{Style.RESET_ALL}")
            return False
        async def _send():
            entities = self._build_entities(text, add_bold=True)
            await self.client.send_message(chat_id, text, formatting_entities=entities)
        return self._run_async(_send())

    def send_file(self, chat_id, file_path, caption):
        if not self.ready:
            print(f"{Fore.RED}[!] User Telegram not ready.{Style.RESET_ALL}")
            return False
        async def _send():
            entities = self._build_entities(caption, add_bold=False)
            await self.client.send_file(chat_id, file_path, caption=caption, formatting_entities=entities, force_document=False, supports_streaming=True)
        return self._run_async(_send())

user_sender = UserTelegramSender()


SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImprbGlianlqemltY2pscHZza3Z3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQxMTE0NzEsImV4cCI6MjA4OTY4NzQ3MX0.aPMtnplXCpMenfdpDAPFcdMd4ccptM2L3C5oCWWC4X4"
SUPABASE_URL = "https://jklibjyjzimcjlpvskvw.supabase.co"
# ================================================================

# ══════════════ CONFIG ══════════════
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")
USER_API_ID = os.environ.get("USER_API_ID")
USER_API_HASH = os.environ.get("USER_API_HASH")
USER_PHONE = os.environ.get("USER_PHONE")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

def is_authorized(uid: int) -> bool:
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json"}
    url = f"{SUPABASE_URL}/rest/v1/bot_access"
    params = {
        "telegram_id": f"eq.{uid}",
        "is_active": "eq.true",
        "select": "id"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=3)
        if resp.status_code == 200:
            return len(resp.json()) > 0
    except Exception as e:
        print(f"Auth Bypass Check: {e}")
        return True
    return False

# ══════════════ MULTI‑USER STATE (updated) ══════════════


class UserState:
    def __init__(self):
        self.strategy = 1
        self.market_type = "OTC"
        self.pairs = []
        self.telegram_format = 1
        self.running = False
        self.stop_requested = False
        self.stats = {"wins": 0, "losses": 0}
        self.signal_history = []
        self.last_signal_pair = None
        self.same_pair_count = 0
        self.last_loss = {}
        self.loss_cooldown_minutes = 3
        self.strategy2_filters = None
        self.strategy3_min_accuracy = 75
        self.strategy3_lookback = 20
        self.strategy4_min_accuracy = 60
        self.strategy5_min_score = 80
        self.strategy6_min_score = 85
        self.strategy6_min_candles = 50
        self.ai_mode = False
        # Advanced MM
        self.mm_enabled = False
        self.mm_balance = 0.0
        self.mm_current_balance = 0.0
        self.mm_tp = 0.0
        self.mm_sl = 0.0
        self.mm_risk_percent = 2.0
        self.mm_win_streak = 0
        self.mm_loss_streak = 0
        self.mm_base_amount = 0.0
        self.mm_pnl = 0.0
        self.last_trade_amount = 0.0   # previous trade amount for martingale exact double
        # AI Mode customization
        self.ai_min_consensus = 2
        self.ai_required_strategies = []


user_states: Dict[int, UserState] = {}


def get_state(uid: int) -> UserState:
    if uid not in user_states:
        user_states[uid] = UserState()
    return user_states[uid]


# ══════════════ AUTO TRADE STATE (Per User) ══════════════
AUTO_DEFAULT_OTC_PAIRS = [
    "EURUSD-OTC", "EURAUD-OTC", "USDBRL-OTC", "USDARS-OTC",
    "USDCOP-OTC", "USDNGN-OTC", "USDBDT-OTC", "USDTRY-OTC",
    "USDPKR-OTC", "USDINR-OTC", "EURCAD-OTC", "EURGBP-OTC",
    "NZDCAD-OTC", "USDMXN-OTC", "USDEGP-OTC", "USDDZD-OTC",
]

ALCOHOL_ASSETS = [
    "AUD_NZD","BCH_USD","BTC_USD","CAD_CHF","ETH_USD","EUR_NZD",
    "GBP_NZD","LTC_USD","NZD_CAD","NZD_CHF","NZD_JPY","NZD_USD","XAG_USD",
    "XAU_USD","USD_MXN","USD_ZAR","USD_ARS","USD_BDT","USD_COP","USD_DZD",
    "USD_EGP","USD_IDR","USD_INR","USD_NGN","USD_PHP","USD_PKR","ATO_USD",
    "AVA_USD","AXS_USD","BNB_USD","BRL_USD","DAS_USD","DOT_USD","ETC_USD",
    "LIN_USD","SOL_USD","TON_USD","TRU_USD","UKBRENT","USCRUDE","XRP_USD",
    "ZEC_USD","AXJ_AUD","CHI_A50","F40_EUR","FTS_GBP","HSI_HKD","IBX_EUR",
    "JPX_JPY","STX_EUR"
]

class AutoTradeState:
    def __init__(self, uid: int):
        self.uid = uid
        self.email = None
        self.password = None
        self.client = None
        self.is_demo = True
        self.balance = 0.0
        self.starting_balance = 0.0
        self.tp_target = 0.0
        self.sl_target = 0.0
        self.risk_percent = 2.0
        self.mtg_enabled = False
        self.strategy = 1
        self.strategy_name = "RSI basic"
        self.running = False
        self.trade_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.pairs = list(AUTO_DEFAULT_OTC_PAIRS)
        # live controls / stats (status card, pause-resume, daily report)
        self.paused = False
        self.start_time = None
        self.tie_count = 0
        self.win_streak = 0
        self.loss_streak = 0
        self.peak_pnl = 0.0
        self._thread = None
        # trade-result correlation (initialised when the loop starts)
        self._last_opened = None
        self._opened_evt = None
        self._results = {}


auto_traders: Dict[int, AutoTradeState] = {}


def get_auto_trader(uid: int) -> AutoTradeState:
    if uid not in auto_traders:
        auto_traders[uid] = AutoTradeState(uid)
    return auto_traders[uid]


# ══════════════ PREMIUM EMOJI IDs ══════════════
PREMIUM_EMOJI_IDS = {
    "👑": 5217822164362739968, "📊": 6145248943807667330,
    "⏳": 6062063510412599114, "🔰": 6147725220087077904,
    "📉": 6064347140228912866, "📈": 6062085844242537125,
    "💎": 6104975752732612597, "😈": 6062153953833917531,
    "✅": 6147440218942218700, "✨": 6145352194821462834,
    "🏆": 6145546134069714639, "❌": 6145317070578916456,
    "📳": 5321305265306348161, "🐲": 5319156849650441091,
    "🤖": 5314391089514291948, "🔥": 5424972470023104089,
    "⬇️": 5260651934720740549, "⏰": 6145553439809084250,
    "🤭": 6062294201696000196, "🔍": 5212985021870123409,
    "⚠️": 6147840110462245787, "🗓": 5413879192267805083,
    "💲": 6145449239607515472, "🔅": 6102445273965926934,
    "📋": 6147840110462245787,
    "🕐": 5215484787325676090, "📝": 6145248943807667330,
    "🎨": 5314391089514291948, "➡️": 5260651934720740549,
    "🟢": 6102581171026140784, "🔴": 5215313353706057331,
    "💪": 5316681209026191987, "🚀": 6147654280112248427,
    "🏐": 5217911744495624141, "🖥": 5282843764451195532,
    "🎥": 6264778055454036969, "🎇": 5229228004068057251,
    "⛈": 6102795674577803992, "⚙️": 5316977664848837418,
    "📺": 5314406675950608695, "🐶": 5319301933645707826,
    "🌀": 6282685788450721937, "🥷": 6217370240800527004,
    "🚨": 5972051363939487192, "📸": 5854710508065658472,
    "🗓": 6102906733842144545,  "💞": 6215041273309434461,
    "🧠": 5965469803299738005, "🥸": 6201834820104882435,
    "👋":5440431182602842059,  "🫠": 6292042928755839133,
    "🦶":6147945749477857953,  "😷": 6086858751749396920,
    "🤢":5462927083132970373,   "👇": 5764981807959250147,
    "😘":5287684458881756303,  "😔":5231200819986047254,
    "🤮":5987932121480563856,  "👌":6217569656132079210,
    "😩":5965496174398934487,  "🤝":5276025009947551999,
    "🙃":6021518426432869078,  "😖":5323642109767460983,
    "🤞":6035189951581129197,  "😇":6129805886383723340,
    "🌝":5283055978785285857,  "🌚":5267419403019886452,
    "⏱":5947290074319162163,   "➡️":5416117059207572332,
    "💿":5341715473882955310,  "😊":6217713374327738118,
    "🤠":5213107179329953547,  "😵":5213101024641821742,
    "🩸":6266973397922616654,  "🎩":6217521170246274833,
}

FORMAT2_EMOJI_IDS = {
    "📊": 5231200819986047254, "⏰": 6285240160120477644,
    "⏳": 5212985021870123409, "🇵🇰": 5269660289321679111,
    "💀": 6204172639523572930, "👿": 6104776659523607556,
    "📺": 4927197721900614739, "🏆": 6145546134069714639,
    "🤔": 5370919202796348364, "🕐": 5215484787325676090,
    "📉": 6064347140228912866, "📈": 6062085844242537125,
    "🦇": 6136515548718045689, "✅": 6147440218942218700,
    "❌": 6102581171026140784, "🤓":6273721156616852730,
}


def fancy_font(text):

    pass

    mapping = {
        'A': '𝙰',
        'B': '𝙱',
        'C': '𝙲',
        'D': '𝙳',
        'E': '𝙴',
        'F': '𝙵',
        'G': '𝙶',
        'H': '𝙷',
        'I': '𝙸',
        'J': '𝙹',
        'K': '𝙺',
        'L': '𝙻',
        'M': '𝙼',
        'N': '𝙽',
        'O': '𝙾',
        'P': '𝙿',
        'Q': '𝚀',
        'R': '𝚁',
        'S': '𝚂',
        'T': '𝚃',
        'U': '𝚄',
        'V': '𝚅',
        'W': '𝚆',
        'X': '𝚇',
        'Y': '𝚈',
        'Z': '𝚉',
        'a': '𝚊',
        'b': '𝚋',
        'c': '𝚌',
        'd': '𝚍',
        'e': '𝚎',
        'f': '𝚏',
        'g': '𝚐',
        'h': '𝚑',
        'i': '𝚒',
        'j': '𝚓',
        'k': '𝚔',
        'l': '𝚕',
        'm': '𝚖',
        'n': '𝚗',
        'o': '𝚘',
        'p': '𝚙',
        'q': '𝚚',
        'r': '𝚛',
        's': '𝚜',
        't': '𝚝',
        'u': '𝚞',
        'v': '𝚟',
        'w': '𝚠',
        'x': '𝚡',
        'y': '𝚢',
        'z': '𝚣',
        '0': '𝟶',
        '1': '𝟷',
        '2': '𝟸',
        '3': '𝟹',
        '4': '𝟺',
        '5': '𝟻',
        '6': '𝟼',
        '7': '𝟽',
        '8': '𝟾',
        '9': '𝟿',
        ':': '：',
        '.': '．',
        '/': '╱',
        '-': '—',
        '_': '＿',
        '@': '＠',
        '!': '！',
        '?': '？',
        '(': '（',
        ')': '）',
        '[': '【',
        ']': '】',
        '{': '｛',
        '}': '｝',
        '<': '＜',
        '>': '＞',
        '=': '＝',
        '+': '＋',
        '*': '＊',
        '&': '＆',
        '^': '＾',
        '$': '＄',
        '#': '＃',
        '~': '～'}
    return "".join(mapping.get(c, c) for c in str(text))


def normalize_fancy(text: str) -> str:
    """Convert fancy Unicode characters (digits, letters, colon, arrow, underscore) to normal ASCII."""
    # Fancy digits 𝟶-𝟿 → 0-9
    fancy_digits = str.maketrans("𝟶𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿", "0123456789")
    # Fancy uppercase 𝙰-𝚉 → A-Z
    fancy_upper = str.maketrans(
        "𝙰𝙱𝙲𝙳𝙴𝙵𝙶𝙷𝙸𝙹𝙺𝙻𝙼𝙽𝙾𝙿𝚀𝚁𝚂𝚃𝚄𝚅𝚆𝚇𝚈𝚉",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    # Fancy lowercase 𝚊-𝚣 → a-z
    fancy_lower = str.maketrans(
        "𝚊𝚋𝚌𝚍𝚎𝚏𝚐𝚑𝚒𝚓𝚔𝚕𝚖𝚗𝚘𝚙𝚚𝚛𝚜𝚝𝚞𝚟𝚠𝚡𝚢𝚣",
        "abcdefghijklmnopqrstuvwxyz")
    # Fullwidth colon ： → :
    colon_trans = str.maketrans("：", ":")
    # Fullwidth underscore ＿ → _
    underscore_trans = str.maketrans("＿", "_")
    # Apply all translations
    text = text.translate(fancy_digits)
    text = text.translate(fancy_upper)
    text = text.translate(fancy_lower)
    text = text.translate(colon_trans)
    text = text.translate(underscore_trans)
    # Replace fancy arrow ➪ with normal arrow -> (or just space)
    text = text.replace("➪", " ")
    # Replace fancy bullet ❒ with space
    text = text.replace("❒", "")
    # Replace any other common fancy symbols
    text = text.replace("➡️", "")
    return text

def parse_blackout_signal_line(line: str):
    """Extract pair and time from a line, ignoring direction."""
    line = normalize_fancy(line)
    line = re.sub(r'^M\d+\s*', '', line)
    time_match = re.search(r'(\d{2}:\d{2})', line)
    if not time_match:
        return None, None
    time_str = time_match.group(1)
    pair_match = re.search(r'([A-Z0-9]+[_-]?[A-Z0-9]*(?:[_-]OTC)?)', line)
    if not pair_match:
        return None, None
    pair_raw = pair_match.group(1).upper()
    pair_raw = pair_raw.replace('_OTC', '-OTC')
    if 'OTC' not in pair_raw:
        pair_raw = pair_raw + '-OTC'
    return pair_raw, time_str


def build_custom_emoji_entities(text: str) -> list:
    entities = []
    offset = 0
    for ch in text:
        clen = len(ch.encode('utf-16-le')) // 2
        eid = PREMIUM_EMOJI_IDS.get(ch) or FORMAT2_EMOJI_IDS.get(ch)
        if eid:
            entities.append(
                MessageEntity(
                    type='custom_emoji',
                    offset=offset,
                    length=clen,
                    custom_emoji_id=eid))
        offset += clen
    return entities

async def font_style_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    original_text = context.user_data.get('font_text', '')
    if not original_text:
        await query.edit_message_text("❌ No text found. Please start again.")
        return
    if data == "font_mono":
        formatted_lines = [f"<code>{line}</code>" for line in original_text.split('\n')]
        formatted = "\n".join(formatted_lines)
        await query.edit_message_text("✅ Monospace style applied!")
        await context.bot.send_message(chat_id=uid, text=formatted, parse_mode='HTML')
    elif data == "font_sans_bold":
        formatted_lines = [f"<b>{line}</b>" for line in original_text.split('\n')]
        formatted = "\n".join(formatted_lines)
        await query.edit_message_text("✅ Sans‑Serif Bold applied!")
        await context.bot.send_message(chat_id=uid, text=formatted, parse_mode='HTML')
    elif data == "font_sans_mono":
        formatted_lines = [fancy_font(line) for line in original_text.split('\n')]
        formatted = "\n".join(formatted_lines)
        await query.edit_message_text("✅ Sans‑Serif Mono applied!")
        await context.bot.send_message(chat_id=uid, text=formatted)
    context.user_data['state'] = None

# ══════════════ INDICATORS (full) ══════════════


def calculate_ema(prices, period):

    pass

    if len(prices) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    ema_val = sum(prices[-period:]) / period
    for i in range(-period + 1, 0):
        ema_val = prices[i] * alpha + ema_val * (1 - alpha)
    return ema_val


def calculate_rsi(prices, period=14):

    pass

    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(len(prices) - period, len(prices) - 1):
        change = prices[i + 1] - prices[i]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    if not gains or not losses:
        return 50.0
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calculate_williams_r(prices, period=14):

    pass

    if len(prices) < period:
        return -50
    highest = max(prices[-period:])
    lowest = min(prices[-period:])
    if highest == lowest:
        return -50
    return -100 * (highest - prices[-1]) / (highest - lowest)


def calculate_bollinger(prices, period=20, std_dev=2):

    pass

    if len(prices) < period:
        return None, None, None
    ma = sum(prices[-period:]) / period
    variance = sum((p - ma)**2 for p in prices[-period:]) / period
    std = variance**0.5
    upper = ma + std_dev * std
    lower = ma - std_dev * std
    return ma, upper, lower


def calculate_atr(candles, period=14):

    pass

    if len(candles) < period + 1:
        return 0
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    closes = [c['close'] for c in candles]
    tr = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
              abs(lows[i] - closes[i - 1])) for i in range(1, len(candles))]
    return sum(tr[-period:]) / period


def calculate_adx(candles, period=14):

    pass

    if len(candles) < period + 1:
        return 0, 0, 0
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    closes = [c['close'] for c in candles]
    tr, plus_dm, minus_dm = [], [], []
    for i in range(1, len(candles)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        tr.append(max(hl, hc, lc))
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0)
        minus_dm.append(down_move if down_move >
                        up_move and down_move > 0 else 0)
    atr_val = sum(tr[:period]) / period
    plus_di = (sum(plus_dm[:period]) / period) / \
        atr_val * 100 if atr_val > 0 else 0
    minus_di = (sum(minus_dm[:period]) / period) / \
        atr_val * 100 if atr_val > 0 else 0
    dx = abs(plus_di - minus_di) / (plus_di + minus_di) * \
        100 if (plus_di + minus_di) > 0 else 0
    adx_vals = [dx]
    for i in range(period, len(tr)):
        atr_val = (atr_val * (period - 1) + tr[i]) / period
        plus_di = (plus_di * (period - 1) + plus_dm[i]) / period
        minus_di = (minus_di * (period - 1) + minus_dm[i]) / period
        plus_di = (plus_di / atr_val * 100) if atr_val > 0 else 0
        minus_di = (minus_di / atr_val * 100) if atr_val > 0 else 0
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * \
            100 if (plus_di + minus_di) > 0 else 0
        adx_vals.append(dx)
    return adx_vals[-1], plus_di, minus_di


def calculate_stochastic(candles, k_period=8, d_period=3):

    pass

    if len(candles) < k_period:
        return 50, 50
    recent = candles[-k_period:]
    highest = max(c['high'] for c in recent)
    lowest = min(c['low'] for c in recent)
    current_close = candles[-1]['close']
    raw_k = 50 if highest == lowest else (
        current_close - lowest) / (highest - lowest) * 100
    k_vals = []
    for i in range(len(candles) - k_period + 1):
        window = candles[i:i + k_period]
        h = max(c['high'] for c in window)
        l = min(c['low'] for c in window)
        c_close = window[-1]['close']
        k_vals.append(50 if h == l else (c_close - l) / (h - l) * 100)
    d_val = sum(k_vals[-d_period:]) / \
        d_period if len(k_vals) >= d_period else raw_k
    return raw_k, d_val


def calculate_support_resistance_levels(prices, lookback=20):

    pass

    if len(prices) < lookback:
        return None, None
    recent = prices[-lookback:]
    return min(recent), max(recent)


def detect_price_action_patterns(candles):

    pass

    if len(candles) < 5:
        return []
    patterns = []
    for i in range(2, len(candles) - 2):
        c = candles[i]
        p1 = candles[i - 1]
        p2 = candles[i - 2] if i >= 2 else None
        o, cl, h, l = float(
            c['open']), float(
            c['close']), float(
            c['high']), float(
                c['low'])
        po, pc1 = float(p1['open']), float(p1['close'])
        body = abs(cl - o)
        lower_wick = min(o, cl) - l
        upper_wick = h - max(o, cl)
        if pc1 < po and cl > o and o <= pc1 and cl >= po:
            patterns.append({'type': 'BULLISH_ENGULFING',
                            'candle_index': i, 'strength': 0.9})
        elif pc1 > po and cl < o and o >= pc1 and cl <= po:
            patterns.append({'type': 'BEARISH_ENGULFING',
                            'candle_index': i, 'strength': 0.9})
        elif po < pc1 and o > pc1 and cl < po:
            patterns.append({'type': 'BULLISH_HARAMI',
                            'candle_index': i, 'strength': 0.7})
        elif po > pc1 and o < pc1 and cl > po:
            patterns.append({'type': 'BEARISH_HARAMI',
                            'candle_index': i, 'strength': 0.7})
        if body > 0 and lower_wick >= 2 * body and upper_wick <= 0.3 * body:
            patterns.append(
                {'type': 'HAMMER', 'candle_index': i, 'strength': 0.8})
        if body > 0 and upper_wick >= 2 * body and lower_wick <= 0.3 * body:
            patterns.append(
                {'type': 'SHOOTING_STAR', 'candle_index': i, 'strength': 0.8})
        if p2:
            p2o, p2c = float(p2['open']), float(p2['close'])
            doji_p1 = abs(
                pc1 - po) <= (float(p1['high']) - float(p1['low'])) * 0.3
            if p2c < p2o and doji_p1 and cl > o and cl > (p2o + p2c) / 2:
                patterns.append(
                    {'type': 'MORNING_STAR', 'candle_index': i, 'strength': 0.95})
            if p2c > p2o and doji_p1 and cl < o and cl < (p2o + p2c) / 2:
                patterns.append(
                    {'type': 'EVENING_STAR', 'candle_index': i, 'strength': 0.95})
        if i >= 3:
            c3 = candles[i - 2]
            c2 = candles[i - 1]
            c1 = candles[i]
            if (
                float(
                    c1['close']) > float(
                    c1['open']) and float(
                    c2['close']) > float(
                    c2['open']) and float(
                        c3['close']) > float(
                            c3['open']) and float(
                                c1['close']) > float(
                                    c2['close']) > float(
                                        c3['close'])):
                patterns.append({'type': 'THREE_WHITE_SOLDIERS',
                                'candle_index': i, 'strength': 0.9})
            if (
                float(
                    c1['close']) < float(
                    c1['open']) and float(
                    c2['close']) < float(
                    c2['open']) and float(
                        c3['close']) < float(
                            c3['open']) and float(
                                c1['close']) < float(
                                    c2['close']) < float(
                                        c3['close'])):
                patterns.append({'type': 'THREE_BLACK_CROWS',
                                'candle_index': i, 'strength': 0.9})
    return patterns


def calculate_supertrend(candles, period=10, multiplier=3):

    pass

    if len(candles) < period:
        return [], []
    high = [c['high'] for c in candles]
    low = [c['low'] for c in candles]
    close = [c['close'] for c in candles]
    tr = [max(high[i] - low[i], abs(high[i] - close[i - 1]),
              abs(low[i] - close[i - 1])) for i in range(1, len(high))]
    atr_val = sum(tr[:period]) / period
    atr = [atr_val]
    for i in range(period, len(tr)):
        atr_val = (atr_val * (period - 1) + tr[i]) / period
        atr.append(atr_val)
    supertrend, trend = [], []
    for i in range(len(candles)):
        if i < period:
            supertrend.append(None)
            trend.append(None)
            continue
        hl2 = (high[i] + low[i]) / 2
        upper_band = hl2 + multiplier * atr[i - period]
        lower_band = hl2 - multiplier * atr[i - period]
        if i == period:
            supertrend.append(upper_band)
            trend.append(1)
        else:
            if close[i] > supertrend[-1]:
                current_trend = 1
                supertrend.append(
                    max(lower_band, supertrend[-1]) if trend[-1] == 1 else lower_band)
            else:
                current_trend = -1
                supertrend.append(
                    min(upper_band, supertrend[-1]) if trend[-1] == -1 else upper_band)
            trend.append(current_trend)
    return supertrend, trend


def detect_fvg_gaps(candles, threshold=0.001):

    pass

    if len(candles) < 3:
        return []
    fvg_gaps = []
    for i in range(1, len(candles) - 1):
        prev = candles[i - 1]
        curr = candles[i]
        nxt = candles[i + 1]
        if (curr['high'] > prev['low'] and nxt['low'] > curr['low'] and abs(
                curr['high'] - prev['low']) / prev['low'] > threshold):
            fvg_gaps.append({'type': 'BULLISH_FVG',
                             'start_price': prev['low'],
                             'end_price': curr['high'],
                             'candle_index': i,
                             'strength': (curr['high'] - prev['low']) / prev['low']})
        if (curr['low'] < prev['high'] and nxt['high'] < curr['high'] and abs(
                prev['high'] - curr['low']) / prev['high'] > threshold):
            fvg_gaps.append({'type': 'BEARISH_FVG',
                             'start_price': prev['high'],
                             'end_price': curr['low'],
                             'candle_index': i,
                             'strength': (prev['high'] - curr['low']) / prev['high']})
    return fvg_gaps


def check_trend_reverse(candles, direction):

    pass

    if len(candles) < 30:
        return True
    closes = [c['close'] for c in candles]
    cur = closes[-1]
    ema20 = calculate_ema(closes, 20) if len(closes) >= 20 else None
    ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else None
    rsi = calculate_rsi(closes, 14)
    support, resistance = calculate_support_resistance_levels(closes, 20)
    if ema20 and ema50:
        if cur > ema20 and ema20 > ema50:
            if rsi > 70 and resistance and abs(
                    cur - resistance) / resistance < 0.005:
                return direction == "PUT"
        elif cur < ema20 and ema20 < ema50:
            if rsi < 30 and support and abs(cur - support) / support < 0.005:
                return direction == "CALL"
    return True


class Strategy2Filters:
    def __init__(self):
        self.use_trend = False
        self.use_bollinger = False
        self.use_support_resistance = False
        self.use_price_action = False
        self.use_supertrend = False
        self.use_fvg = False
        self.use_trend_reverse = False
        self.min_accuracy = 75

    def check_trend(self, candles, direction):
        if len(candles) < 5:
            return True
        closes = [c['close'] for c in candles[-5:]]
        trend_score = sum(
            1 if closes[i] > closes[i - 1] else -1 for i in range(1, 5))
        if trend_score >= 3:
            return direction == "CALL"
        elif trend_score <= -3:
            return direction == "PUT"
        return True

    def check_bollinger(self, candles, direction):
        if len(candles) < 20:
            return True
        closes = [c['close'] for c in candles]
        ma, upper, lower = calculate_bollinger(closes)
        if ma is None:
            return True
        cur, prev = closes[-1], closes[-2] if len(closes) >= 2 else cur
        if direction == "CALL":
            return cur < lower and prev >= lower
        else:
            return cur > upper and prev <= upper

    def check_support_resistance(self, candles, direction):
        if len(candles) < 20:
            return True
        closes = [c['close'] for c in candles]
        sup, res = calculate_support_resistance_levels(closes)
        if sup is None or res is None:
            return True
        cur, prev = closes[-1], closes[-2] if len(closes) >= 2 else cur
        if direction == "CALL":
            if cur > res and prev <= res:
                return True
            if abs(cur - sup) / sup < 0.001 and cur > prev:
                return True
        else:
            if cur < sup and prev >= sup:
                return True
            if abs(cur - res) / res < 0.001 and cur < prev:
                return True
        return False

    def check_price_action(self, candles, direction):
        if len(candles) < 5:
            return True
        patterns = detect_price_action_patterns(candles)
        recent = [p for p in patterns if p['candle_index'] >= len(candles) - 3]
        for p in recent:
            if p['type'] in [
                'BULLISH_ENGULFING',
                'HAMMER',
                'BULLISH_HARAMI',
                'MORNING_STAR',
                    'THREE_WHITE_SOLDIERS']:
                return direction == "CALL"
            if p['type'] in [
                'BEARISH_ENGULFING',
                'SHOOTING_STAR',
                'BEARISH_HARAMI',
                'EVENING_STAR',
                    'THREE_BLACK_CROWS']:
                return direction == "PUT"
        return True

    def check_supertrend(self, candles, direction):
        if len(candles) < 20:
            return True
        st, tr = calculate_supertrend(candles, 10, 3)
        if st[-1] is None or tr[-1] is None:
            return True
        cur = candles[-1]['close']
        if direction == "CALL":
            return tr[-1] == 1 and cur > st[-1]
        else:
            return tr[-1] == -1 and cur < st[-1]

    def check_fvg(self, candles, direction):
        if len(candles) < 10:
            return True
        fvg = detect_fvg_gaps(candles)
        cur = candles[-1]['close']
        for f in fvg:
            if f['candle_index'] >= len(candles) - 5:
                if f['type'] == 'BULLISH_FVG' and cur > f['end_price']:
                    return direction == "CALL"
                if f['type'] == 'BEARISH_FVG' and cur < f['end_price']:
                    return direction == "PUT"
        return True

    def check_trend_reverse(self, candles, direction):
        return check_trend_reverse(candles, direction)


def analyze_strategy1(candles, min_accuracy=75):

    pass

    if not candles or len(candles) < 20:
        return None, None, None
    closes = [c['close'] for c in candles]
    cur = closes[-1]
    prev = closes[-2] if len(closes) > 1 else cur
    rsi = calculate_rsi(closes, 14)
    direction, conf = None, 0
    if cur > prev and rsi < 70:
        direction = "CALL"
        conf = 70 + (rsi / 2)
    elif cur < prev and rsi > 30:
        direction = "PUT"
        conf = 70 + ((100 - rsi) / 2)
    if direction and conf >= min_accuracy:
        entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
        entry_dt = entry_dt.replace(
            second=0, microsecond=0) + timedelta(minutes=1)
        return direction, entry_dt, conf
    return None, None, None


def analyze_strategy2(candles, filters):

    pass

    if not candles or len(candles) < max(10, 14) + 5:
        return None, None, None
    closes = [c['close'] for c in candles]
    cur = closes[-1]
    ema = calculate_ema(closes, 10)
    if ema is None:
        return None, None, None
    rsi = calculate_rsi(closes, 14)
    direction, score = None, 0
    if cur > ema and 50 < rsi < 70:
        direction = "CALL"
        score = 5
    elif cur < ema and 30 < rsi < 50:
        direction = "PUT"
        score = 5
    elif rsi > 80:
        direction = "PUT"
        score = 4
    elif rsi < 20:
        direction = "CALL"
        score = 4
    if direction is None:
        return None, None, None
    if len(closes) >= 3:
        recent_up = sum(1 for i in range(-3, 0) if closes[i] > closes[i - 1])
        if direction == "CALL" and recent_up < 2:
            score -= 1
        elif direction == "PUT" and recent_up > 1:
            score -= 1
    if score < 4:
        return None, None, None
    if filters.use_trend and not filters.check_trend(candles, direction):
        return None, None, None
    if filters.use_bollinger and not filters.check_bollinger(
            candles, direction):
        return None, None, None
    if filters.use_support_resistance and not filters.check_support_resistance(
            candles, direction):
        return None, None, None
    if filters.use_price_action and not filters.check_price_action(
            candles, direction):
        return None, None, None
    if filters.use_supertrend and not filters.check_supertrend(
            candles, direction):
        return None, None, None
    if filters.use_fvg and not filters.check_fvg(candles, direction):
        return None, None, None
    if filters.use_trend_reverse and not filters.check_trend_reverse(
            candles, direction):
        return None, None, None
    confidence = (score / 5) * 100
    if confidence < filters.min_accuracy:
        return None, None, None
    entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_dt = entry_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return direction, entry_dt, confidence


def analyze_strategy3(candles, min_accuracy=75, lookback=20):

    pass

    if not candles or len(candles) < lookback + 5:
        return None, None, None
    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    n = len(closes)
    wr_vals = []
    for i in range(n):
        if i < 14:
            wr_vals.append(-50)
        else:
            window = closes[i - 13:i + 1]
            highest = max(window)
            lowest = min(window)
            wr_vals.append(-50 if highest == lowest else -100 *
                           (highest - closes[i]) / (highest - lowest))
    start_idx = max(0, n - lookback - 2)
    price_highs, price_lows, wr_highs, wr_lows = [], [], [], []
    for i in range(start_idx + 2, n - 2):
        if highs[i] > highs[i - 1] and highs[i] > highs[i -
                                                        2] and highs[i] > highs[i + 1] and highs[i] > highs[i + 2]:
            price_highs.append((i, highs[i]))
            wr_highs.append((i, wr_vals[i]))
        if lows[i] < lows[i - 1] and lows[i] < lows[i -
                                                    2] and lows[i] < lows[i + 1] and lows[i] < lows[i + 2]:
            price_lows.append((i, lows[i]))
            wr_lows.append((i, wr_vals[i]))
    direction, confidence = None, 75
    if len(price_lows) >= 2 and len(wr_lows) >= 2:
        last_pl = price_lows[-1][1]
        prev_pl = price_lows[-2][1]
        last_wrl = wr_lows[-1][1]
        prev_wrl = wr_lows[-2][1]
        if last_pl < prev_pl and last_wrl > prev_wrl:
            direction = "CALL"
            confidence += 10 if wr_vals[-1] < -80 else 0
    if len(price_highs) >= 2 and len(wr_highs) >= 2:
        last_ph = price_highs[-1][1]
        prev_ph = price_highs[-2][1]
        last_wrh = wr_highs[-1][1]
        prev_wrh = wr_highs[-2][1]
        if last_ph > prev_ph and last_wrh < prev_wrh:
            direction = "PUT"
            confidence += 10 if wr_vals[-1] > -20 else 0
    if direction is None:
        return None, None, None
    confidence = min(100, max(50, confidence))
    if confidence < min_accuracy:
        return None, None, None
    entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_dt = entry_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return direction, entry_dt, confidence


def analyze_strategy4(candles, min_accuracy=60):

    pass

    if not candles or len(candles) < max(14, 8) + 5:
        return None, None, None
    adx, plus_di, minus_di = calculate_adx(candles, 14)
    if adx < 15:
        return None, None, None
    current_k, current_d = calculate_stochastic(candles, 8, 3)
    prev_k, prev_d = calculate_stochastic(candles[:-1], 8, 3)
    crossover_up = (prev_k <= prev_d and current_k > current_d)
    crossover_down = (prev_k >= prev_d and current_k < current_d)
    is_green = candles[-1]['close'] > candles[-1]['open']
    is_red = not is_green
    direction, confidence = None, 65
    if crossover_up and current_k < 30 and is_green:
        direction = "CALL"
        confidence += 10
    elif crossover_down and current_k > 70 and is_red:
        direction = "PUT"
        confidence += 10
    else:
        return None, None, None
    if adx >= 25:
        confidence += 5
    confidence = min(95, confidence)
    if confidence < min_accuracy:
        return None, None, None
    entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_dt = entry_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return direction, entry_dt, confidence


# ========== CONFLUENCE ENGINE (Strategy 5) ==========
BULL_PATTERNS = {
    'BULLISH_ENGULFING',
    'HAMMER',
    'BULLISH_HARAMI',
    'MORNING_STAR',
    'THREE_WHITE_SOLDIERS',
    'BULLISH_PINBAR',
    'TWEEZER_BOTTOM',
    'BULLISH_MARUBOZU'}
BEAR_PATTERNS = {
    'BEARISH_ENGULFING',
    'SHOOTING_STAR',
    'BEARISH_HARAMI',
    'EVENING_STAR',
    'THREE_BLACK_CROWS',
    'BEARISH_PINBAR',
    'TWEEZER_TOP',
    'BEARISH_MARUBOZU'}


def cf_calc_ema(prices, period):

    pass

    if len(prices) < period:
        return [None] * len(prices)
    alpha = 2.0 / (period + 1)
    ema_series = [None] * len(prices)
    ema_series[period - 1] = sum(prices[:period]) / period
    for i in range(period, len(prices)):
        ema_series[i] = prices[i] * alpha + ema_series[i - 1] * (1 - alpha)
    return ema_series


def cf_last_ema(prices, period):

    pass

    series = cf_calc_ema(prices, period)
    for v in reversed(series):
        if v is not None:
            return v
    return None


def cf_calc_rsi(prices, period=14):

    pass

    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1 + rs)


def cf_calc_macd(prices, fast=12, slow=26, signal=9):

    pass

    if len(prices) < slow + signal:
        return None, None, None
    ema_fast = cf_calc_ema(prices, fast)
    ema_slow = cf_calc_ema(prices, slow)
    macd_line = []
    for f, s in zip(ema_fast, ema_slow):
        if f is not None and s is not None:
            macd_line.append(f - s)
        else:
            macd_line.append(None)
    valid_macd = [v for v in macd_line if v is not None]
    if len(valid_macd) < signal:
        return None, None, None
    sig_series = cf_calc_ema(valid_macd, signal)
    sig_val = next((v for v in reversed(sig_series) if v is not None), None)
    macd_val = valid_macd[-1]
    if sig_val is None:
        return None, None, None
    return macd_val, sig_val, macd_val - sig_val


def cf_calc_stoch_rsi(prices, rsi_period=14, stoch_period=14, k=3, d=3):

    pass

    needed = rsi_period + stoch_period + max(k, d) + 5
    if len(prices) < needed:
        return None, None
    rsi_series = [cf_calc_rsi(prices[:i + 1], rsi_period)
                  for i in range(rsi_period, len(prices))]
    if len(rsi_series) < stoch_period:
        return None, None
    k_vals = []
    for i in range(stoch_period - 1, len(rsi_series)):
        window = rsi_series[i - stoch_period + 1:i + 1]
        lo, hi = min(window), max(window)
        k_vals.append(50.0 if hi == lo else (
            rsi_series[i] - lo) / (hi - lo) * 100)
    if len(k_vals) < max(k, d):
        return None, None
    return sum(k_vals[-k:]) / k, sum(k_vals[-d:]) / d


def cf_calc_bb(prices, period=20, std_mult=2.0):

    pass

    if len(prices) < period:
        return None, None, None, None, None
    window = prices[-period:]
    mid = sum(window) / period
    variance = sum((p - mid)**2 for p in window) / period
    std = math.sqrt(variance)
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    cur = prices[-1]
    pct_b = (cur - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
    bw = (upper - lower) / mid if mid != 0 else 0
    return upper, mid, lower, pct_b, bw


def cf_calc_atr(candles, period=14):

    pass

    if len(candles) < period + 1:
        return 0.0
    trs = [max(c['high'] - c['low'],
               abs(c['high'] - candles[i - 1]['close']),
               abs(c['low'] - candles[i - 1]['close'])) for i in range(1,
                                                                       len(candles))]
    return sum(trs[-period:]) / \
        period if len(trs) >= period else sum(trs) / len(trs)


def cf_calc_adx(candles, period=14):

    pass

    if len(candles) < period * 2:
        return None, None, None
    asc = sorted(candles, key=lambda x: x['time'])
    plus_dm_list, minus_dm_list, tr_list = [], [], []
    for i in range(1, len(asc)):
        h = float(asc[i]['high'])
        l = float(asc[i]['low'])
        ph = float(asc[i - 1]['high'])
        pl = float(asc[i - 1]['low'])
        pc = float(asc[i - 1]['close'])
        up_move = h - ph
        down_move = pl - l
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0
        tr = max(h - l, abs(h - pc), abs(l - pc))
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)
        tr_list.append(tr)
    tr14 = sum(tr_list[:period])
    pdm14 = sum(plus_dm_list[:period])
    mdm14 = sum(minus_dm_list[:period])
    dx_list = []
    plus_di = 100 * pdm14 / tr14 if tr14 else 0
    minus_di = 100 * mdm14 / tr14 if tr14 else 0
    if plus_di + minus_di:
        dx_list.append(100 * abs(plus_di - minus_di) / (plus_di + minus_di))
    for i in range(period, len(tr_list)):
        tr14 = tr14 - tr14 / period + tr_list[i]
        pdm14 = pdm14 - pdm14 / period + plus_dm_list[i]
        mdm14 = mdm14 - mdm14 / period + minus_dm_list[i]
        plus_di = 100 * pdm14 / tr14 if tr14 else 0
        minus_di = 100 * mdm14 / tr14 if tr14 else 0
        if plus_di + minus_di:
            dx_list.append(100 * abs(plus_di - minus_di) /
                           (plus_di + minus_di))
    if not dx_list:
        return None, None, None
    adx = sum(dx_list[-period:]) / min(len(dx_list), period)
    return adx, plus_di, minus_di


def cf_detect_patterns(candles):

    pass

    if len(candles) < 5:
        return []
    patterns = []
    for i in range(2, len(candles)):
        c = candles[i]
        p1 = candles[i - 1]
        p2 = candles[i - 2] if i >= 2 else None
        o, cl, h, l = float(
            c['open']), float(
            c['close']), float(
            c['high']), float(
                c['low'])
        po, pc1 = float(p1['open']), float(p1['close'])
        body = abs(cl - o)
        candle_range = h - l
        lower_wick = min(o, cl) - l
        upper_wick = h - max(o, cl)
        if body > 0 and candle_range > 0 and body / candle_range > 0.85:
            t = 'BULLISH_MARUBOZU' if cl > o else 'BEARISH_MARUBOZU'
            patterns.append({'type': t, 'index': i, 'strength': 0.85})
        if body > 0 and lower_wick >= 2.5 * body and upper_wick <= 0.4 * body:
            patterns.append({'type': 'HAMMER', 'index': i, 'strength': 0.80})
        if body > 0 and lower_wick >= 3 * body:
            patterns.append({'type': 'BULLISH_PINBAR',
                            'index': i, 'strength': 0.85})
        if body > 0 and upper_wick >= 2.5 * body and lower_wick <= 0.4 * body:
            patterns.append(
                {'type': 'SHOOTING_STAR', 'index': i, 'strength': 0.80})
        if body > 0 and upper_wick >= 3 * body:
            patterns.append({'type': 'BEARISH_PINBAR',
                            'index': i, 'strength': 0.85})
        if pc1 < po and cl > o and o <= pc1 and cl >= po:
            patterns.append({'type': 'BULLISH_ENGULFING',
                            'index': i, 'strength': 0.90})
        if pc1 > po and cl < o and o >= pc1 and cl <= po:
            patterns.append({'type': 'BEARISH_ENGULFING',
                            'index': i, 'strength': 0.90})
        if po < pc1 and o > pc1 and cl < po:
            patterns.append({'type': 'BULLISH_HARAMI',
                            'index': i, 'strength': 0.70})
        if po > pc1 and o < pc1 and cl > po:
            patterns.append({'type': 'BEARISH_HARAMI',
                            'index': i, 'strength': 0.70})
        if p2 and abs(float(p1['high']) - h) < candle_range * \
                0.05 and cl < o and pc1 > po:
            patterns.append(
                {'type': 'TWEEZER_TOP', 'index': i, 'strength': 0.75})
        if p2 and abs(float(p1['low']) - l) < candle_range * \
                0.05 and cl > o and pc1 < po:
            patterns.append({'type': 'TWEEZER_BOTTOM',
                            'index': i, 'strength': 0.75})
        if p2:
            p2o, p2c = float(p2['open']), float(p2['close'])
            doji_p1 = abs(
                pc1 - po) <= (float(p1['high']) - float(p1['low'])) * 0.3
            if p2c < p2o and doji_p1 and cl > o and cl > (p2o + p2c) / 2:
                patterns.append(
                    {'type': 'MORNING_STAR', 'index': i, 'strength': 0.95})
            if p2c > p2o and doji_p1 and cl < o and cl < (p2o + p2c) / 2:
                patterns.append(
                    {'type': 'EVENING_STAR', 'index': i, 'strength': 0.95})
        if i >= 2:
            c3 = candles[i - 2]
            c2 = candles[i - 1]
            c1 = candles[i]
            if (
                float(
                    c1['close']) > float(
                    c1['open']) and float(
                    c2['close']) > float(
                    c2['open']) and float(
                        c3['close']) > float(
                            c3['open']) and float(
                                c1['close']) > float(
                                    c2['close']) > float(
                                        c3['close'])):
                patterns.append({'type': 'THREE_WHITE_SOLDIERS',
                                'index': i, 'strength': 0.90})
            if (
                float(
                    c1['close']) < float(
                    c1['open']) and float(
                    c2['close']) < float(
                    c2['open']) and float(
                        c3['close']) < float(
                            c3['open']) and float(
                                c1['close']) < float(
                                    c2['close']) < float(
                                        c3['close'])):
                patterns.append({'type': 'THREE_BLACK_CROWS',
                                'index': i, 'strength': 0.90})
    return patterns


def cf_aggregate_candles(candles, minutes):

    pass

    if not candles:
        return []
    asc = sorted(candles, key=lambda x: x['time'])
    result, group = [], []
    base_time = None
    for c in asc:
        ct = datetime.fromtimestamp(c['time'])
        if base_time is None:
            base_time = ct
            group = [c]
        elif (ct - base_time).total_seconds() < minutes * 60:
            group.append(c)
        else:
            result.append({'time': group[0]['time'],
                           'open': float(group[0]['open']),
                           'high': max(float(c['high']) for c in group),
                           'low': min(float(c['low']) for c in group),
                           'close': float(group[-1]['close'])})
            base_time = ct
            group = [c]
    if group:
        result.append({'time': group[0]['time'],
                       'open': float(group[0]['open']),
                       'high': max(float(c['high']) for c in group),
                       'low': min(float(c['low']) for c in group),
                       'close': float(group[-1]['close'])})
    return sorted(result, key=lambda x: x['time'], reverse=True)


def cf_htf_direction(candles_htf):

    pass

    if not candles_htf or len(candles_htf) < 25:
        return None
    asc = sorted(candles_htf, key=lambda x: x['time'])
    closes = [float(c['close']) for c in asc]
    e9 = cf_last_ema(closes, 9)
    e21 = cf_last_ema(closes, 21)
    return "CALL" if (
        e9 and e21 and e9 > e21) else (
        "PUT" if e9 and e21 else None)


def cf_run_confluence_engine(candles_1m, candles_5m, candles_15m):

    pass

    if len(candles_1m) < 50:
        return None, 0, {}
    asc = sorted(candles_1m, key=lambda x: x['time'])
    closes = [float(c['close']) for c in asc]
    details = {}
    votes_call, votes_put = 0, 0
    e9 = cf_last_ema(closes, 9)
    e21 = cf_last_ema(closes, 21)
    e50 = cf_last_ema(closes, 50)
    e200 = cf_last_ema(closes, 200)
    cur = closes[-1]
    ema_score, ema_dir = 0, None
    if e9 and e21 and e50:
        if cur > e9 > e21 > e50:
            ema_score, ema_dir = 18, "CALL"
        elif cur < e9 < e21 < e50:
            ema_score, ema_dir = 18, "PUT"
        elif cur > e21 and e9 > e21:
            ema_score, ema_dir = 10, "CALL"
        elif cur < e21 and e9 < e21:
            ema_score, ema_dir = 10, "PUT"
    if ema_dir == "CALL":
        votes_call += ema_score
    elif ema_dir == "PUT":
        votes_put += ema_score
    details['EMA_stack'] = {'dir': ema_dir, 'score': ema_score}
    e200_dir = None
    if e200:
        if cur > e200:
            e200_dir = "CALL"
            votes_call += 8
        else:
            e200_dir = "PUT"
            votes_put += 8
    details['EMA200'] = {'dir': e200_dir}
    macd_val, sig_val, hist = cf_calc_macd(closes)
    macd_dir, macd_score = None, 0
    if macd_val is not None:
        if macd_val > sig_val and hist > 0:
            macd_dir, macd_score = "CALL", 16 if macd_val > 0 else 10
        elif macd_val < sig_val and hist < 0:
            macd_dir, macd_score = "PUT", 16 if macd_val < 0 else 10
    if macd_dir == "CALL":
        votes_call += macd_score
    elif macd_dir == "PUT":
        votes_put += macd_score
    details['MACD'] = {'dir': macd_dir, 'score': macd_score}
    rsi = cf_calc_rsi(closes, 14)
    rsi_dir, rsi_score = None, 0
    if rsi < 35:
        rsi_dir, rsi_score = "CALL", 12
    elif rsi > 65:
        rsi_dir, rsi_score = "PUT", 12
    elif 40 <= rsi <= 50:
        rsi_dir, rsi_score = "CALL", 6
    elif 50 < rsi <= 60:
        rsi_dir, rsi_score = "PUT", 6
    if rsi_dir == "CALL":
        votes_call += rsi_score
    elif rsi_dir == "PUT":
        votes_put += rsi_score
    details['RSI'] = {'dir': rsi_dir, 'rsi': round(rsi, 2)}
    k_val, d_val = cf_calc_stoch_rsi(closes)
    stoch_dir, stoch_score = None, 0
    if k_val is not None:
        if k_val < 20 and d_val < 20 and k_val > d_val:
            stoch_dir, stoch_score = "CALL", 10
        elif k_val > 80 and d_val > 80 and k_val < d_val:
            stoch_dir, stoch_score = "PUT", 10
        elif k_val < 50 and k_val > d_val:
            stoch_dir, stoch_score = "CALL", 5
        elif k_val > 50 and k_val < d_val:
            stoch_dir, stoch_score = "PUT", 5
    if stoch_dir == "CALL":
        votes_call += stoch_score
    elif stoch_dir == "PUT":
        votes_put += stoch_score
    details['StochRSI'] = {
        'dir': stoch_dir, 'K': round(
            k_val, 2) if k_val else None, 'D': round(
            d_val, 2) if d_val else None}
    bb_upper, bb_mid, bb_lower, pct_b, bw = cf_calc_bb(closes, 20, 2.0)
    bb_dir, bb_score = None, 0
    if pct_b is not None:
        if pct_b < 0.05:
            bb_dir, bb_score = "CALL", 10
        elif pct_b > 0.95:
            bb_dir, bb_score = "PUT", 10
        elif pct_b < 0.30:
            bb_dir, bb_score = "CALL", 5
        elif pct_b > 0.70:
            bb_dir, bb_score = "PUT", 5
    if bw is None or bw < 0.0015:
        bb_dir, bb_score = None, 0
    if bb_dir == "CALL":
        votes_call += bb_score
    elif bb_dir == "PUT":
        votes_put += bb_score
    details['BB'] = {
        'dir': bb_dir,
        'pct_b': round(
            pct_b,
            3) if pct_b else None}
    adx_val, plus_di, minus_di = cf_calc_adx(asc, 14)
    adx_dir, adx_score = None, 0
    if adx_val is not None and adx_val >= 18:
        if plus_di > minus_di:
            adx_dir, adx_score = "CALL", 8
        else:
            adx_dir, adx_score = "PUT", 8
    if adx_dir == "CALL":
        votes_call += adx_score
    elif adx_dir == "PUT":
        votes_put += adx_score
    details['ADX'] = {
        'dir': adx_dir,
        'adx': round(
            adx_val,
            2) if adx_val else None}
    patterns = cf_detect_patterns(asc[-15:])
    recent_patterns = [p for p in patterns if p['index'] >= len(asc[-15:]) - 3]
    pat_dir, pat_score, best_strength = None, 0, 0
    for p in recent_patterns:
        if p['strength'] > best_strength:
            if p['type'] in BULL_PATTERNS:
                pat_dir, best_strength = "CALL", p['strength']
            elif p['type'] in BEAR_PATTERNS:
                pat_dir, best_strength = "PUT", p['strength']
    if pat_dir:
        pat_score = int(best_strength * 12)
    if pat_dir == "CALL":
        votes_call += pat_score
    elif pat_dir == "PUT":
        votes_put += pat_score
    details['Pattern'] = {
        'dir': pat_dir,
        'score': pat_score,
        'patterns': [
            p['type'] for p in recent_patterns]}
    htf_dir, htf_score = None, 0
    results_5m = cf_htf_direction(candles_5m)
    results_15m = cf_htf_direction(candles_15m)
    if results_5m and results_15m:
        htf_dir, htf_score = (
            results_5m, 10) if results_5m == results_15m else (
            results_5m, 5)
    elif results_5m:
        htf_dir, htf_score = results_5m, 5
    if htf_dir == "CALL":
        votes_call += htf_score
    elif htf_dir == "PUT":
        votes_put += htf_score
    details['HTF'] = {'dir': htf_dir, 'score': htf_score}
    mom_dir, mom_score = None, 0
    if len(asc) >= 5:
        last5 = asc[-5:]
        bull_count = sum(
            1 for c in last5 if float(
                c['close']) > float(
                c['open']))
        if bull_count >= 4:
            mom_dir, mom_score = "CALL", 6
        elif bull_count <= 1:
            mom_dir, mom_score = "PUT", 6
        elif bull_count == 3:
            mom_dir, mom_score = "CALL", 3
        elif bull_count == 2:
            mom_dir, mom_score = "PUT", 3
    if mom_dir == "CALL":
        votes_call += mom_score
    elif mom_dir == "PUT":
        votes_put += mom_score
    details['Momentum'] = {'dir': mom_dir, 'score': mom_score}
    total = votes_call + votes_put
    if total == 0:
        return None, 0, details
    dominant, raw_score = (
        "CALL", (votes_call / total) * 100) if votes_call >= votes_put else (
        "PUT", (votes_put / total) * 100)
    details['votes_call'] = votes_call
    details['votes_put'] = votes_put
    details['raw_score'] = round(raw_score, 1)
    if dominant == "CALL" and rsi > 75:
        return None, 0, {**details, 'reject': 'RSI_OVERBOUGHT'}
    if dominant == "PUT" and rsi < 35:
        return None, 0, {**details, 'reject': 'RSI_OVERSOLD'}
    if adx_val is not None and adx_val < 18 * 0.7:
        return None, 0, {**details, 'reject': 'ADX_WEAK_MARKET'}
    return dominant, raw_score, details


def analyze_strategy5(candles, min_accuracy=72):

    pass

    if not candles or len(candles) < 50:
        return None, None, None
    candles_5m = cf_aggregate_candles(candles, 5)
    candles_15m = cf_aggregate_candles(candles, 15)
    direction, score, details = cf_run_confluence_engine(
        candles, candles_5m, candles_15m)
    if direction is None or score < min_accuracy:
        return None, None, None
    entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_dt = entry_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return direction, entry_dt, score

# ========== STRATEGY 6 ==========


def detect_liquidity_sweep(candles, lookback=20):

    pass

    if len(candles) < lookback + 1:
        return False, False, None, None
    highs = [c['high'] for c in candles[-lookback - 1:]]
    lows = [c['low'] for c in candles[-lookback - 1:]]
    closes = [c['close'] for c in candles[-lookback - 1:]]
    recent_high = max(highs[:-1])
    recent_low = min(lows[:-1])
    cur_high, cur_low, cur_close = highs[-1], lows[-1], closes[-1]
    bearish_sweep = (cur_high > recent_high) and (cur_close < recent_high)
    bullish_sweep = (cur_low < recent_low) and (cur_close > recent_low)
    return bearish_sweep, bullish_sweep, recent_high if bearish_sweep else None, recent_low if bullish_sweep else None


def detect_fvg(candles):

    pass

    if len(candles) < 3:
        return False, False
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    bullish = (c2['low'] > c1['high']) and (c3['close'] < c2['low'])
    bearish = (c2['high'] < c1['low']) and (c3['close'] > c2['high'])
    return bullish, bearish


def analyze_strategy6(candles, min_score=20, min_candles=10):

    pass

    if len(candles) < min_candles:
        return None, None, None
    closes = [c['close'] for c in candles]
    trend_bias = "BULLISH" if closes[-1] > closes[-2] else "BEARISH"
    bear_sweep, bull_sweep, _, _ = detect_liquidity_sweep(candles)
    call_score, put_score = 0, 0
    if bear_sweep:
        put_score += 40
    if bull_sweep:
        call_score += 40
    candle = candles[-1]
    body = abs(candle['close'] - candle['open'])
    if body > 0:
        upper_wick = candle['high'] - max(candle['open'], candle['close'])
        lower_wick = min(candle['open'], candle['close']) - candle['low']
        if upper_wick / body >= 1.2:
            put_score += 30
        if lower_wick / body >= 1.2:
            call_score += 30
    if trend_bias == "BEARISH":
        put_score += 10
    else:
        call_score += 10
    bull_fvg, bear_fvg = detect_fvg(candles)
    if bear_fvg:
        put_score += 15
    if bull_fvg:
        call_score += 15
    if call_score >= min_score and call_score > put_score:
        direction = "CALL"
        conf = call_score
    elif put_score >= min_score and put_score > call_score:
        direction = "PUT"
        conf = put_score
    else:
        return None, None, None
    entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_dt = entry_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return direction, entry_dt, conf

# ========== SMZ HACKING MODE (LOCAL, STRATEGY 1) ==========
import math

def _calc_sma(prices, period):
    if len(prices) < period:
        return [None]*len(prices)
    sma = [None]*len(prices)
    for i in range(period-1, len(prices)):
        sma[i] = sum(prices[j] for j in range(i-period+1, i+1)) / period
    return sma

def _calc_rsi(prices, period=14):
    if len(prices) < period+1:
        return [50]*len(prices)
    rsi = [50]*len(prices)
    gains = losses = 0
    for i in range(1, period+1):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains += change
        else:
            losses -= change
    avg_gain = gains / period if period else 0
    avg_loss = losses / period if period else 0
    for i in range(period, len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            avg_gain = (avg_gain*(period-1) + change) / period
            avg_loss = (avg_loss*(period-1)) / period
        else:
            avg_gain = (avg_gain*(period-1)) / period
            avg_loss = (avg_loss*(period-1) - change) / period
        if avg_loss == 0:
            rsi[i] = 100 if avg_gain > 0 else 50
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100/(1+rs))
    return rsi

def _calc_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return [None]*len(prices), [None]*len(prices), [None]*len(prices)
    ema_fast = [None]*len(prices)
    ema_slow = [None]*len(prices)
    mult_f = 2/(fast+1)
    mult_s = 2/(slow+1)
    ema_fast[0] = prices[0]
    ema_slow[0] = prices[0]
    for i in range(1, len(prices)):
        ema_fast[i] = (prices[i] - ema_fast[i-1]) * mult_f + ema_fast[i-1]
        ema_slow[i] = (prices[i] - ema_slow[i-1]) * mult_s + ema_slow[i-1]
    macd_line = [efa - esl for efa, esl in zip(ema_fast, ema_slow)]
    ema_signal = [None]*len(prices)
    mult_sig = 2/(signal+1)
    ema_signal[0] = macd_line[0]
    for i in range(1, len(prices)):
        ema_signal[i] = (macd_line[i] - ema_signal[i-1]) * mult_sig + ema_signal[i-1]
    return macd_line, ema_signal, [macd_line[i] - ema_signal[i] for i in range(len(prices))]

def _calc_stochastic(candles, period=14):
    if len(candles) < period:
        return [50]*len(candles), [50]*len(candles)
    k = [50]*len(candles)
    for i in range(period-1, len(candles)):
        window = candles[i-period+1:i+1]
        low = min(c['low'] for c in window)
        high = max(c['high'] for c in window)
        if high - low != 0:
            k[i] = 100 * (candles[i]['close'] - low) / (high - low)
    # D line: 3-period SMA of K
    d = [50]*len(candles)
    for i in range(2, len(candles)):
        d[i] = (k[i-2] + k[i-1] + k[i]) / 3
    return k, d

def _calc_bollinger(candles, period=20, std_dev=2):
    if len(candles) < period:
        return [None]*len(candles), [None]*len(candles), [None]*len(candles)
    mid = [None]*len(candles)
    upper = [None]*len(candles)
    lower = [None]*len(candles)
    for i in range(period-1, len(candles)):
        window = [c['close'] for c in candles[i-period+1:i+1]]
        mean = sum(window)/period
        var = sum((x-mean)**2 for x in window)/period
        std = math.sqrt(var)
        mid[i] = mean
        upper[i] = mean + std_dev * std
        lower[i] = mean - std_dev * std
    return mid, upper, lower

def run_smz_hacking_mode(uid, days, start_time, end_time, tf, selected_pairs):
    """Local Strategy 1 – fixed parameters: 2 days, 75% accuracy, tolerance 5 min, consistency loose."""
    from datetime import datetime, timedelta, timezone
    BACKTEST_DAYS = 2
    MIN_ACCURACY = 75
    TIMEFRAME_MINUTES = 1
    candles_per_day = 24 * 60 // TIMEFRAME_MINUTES
    required_candles = BACKTEST_DAYS * candles_per_day + 200

    raw_signals = []

    for pair in selected_pairs:
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/get-candles?pair={pair}&timeframe=M1&limit={required_candles}"
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code != 200:
                continue
            data = resp.json()
            candles = data.get('candles', [])
            if len(candles) < 100:
                continue
            candles.sort(key=lambda x: x['time'])
            if len(candles) > BACKTEST_DAYS * candles_per_day:
                candles = candles[-(BACKTEST_DAYS * candles_per_day):]

            closes = [c['close'] for c in candles]
            rsi = _calc_rsi(closes, 14)
            ma50 = _calc_sma(closes, 50)
            macd, sig, _ = _calc_macd(closes)
            k, d = _calc_stochastic(candles)
            bb_mid, bb_upper, bb_lower = _calc_bollinger(candles)

            for idx in range(50, len(candles)-1):
                ts = candles[idx]['time']
                dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
                dt_pk = dt_utc + timedelta(hours=5)
                time_str = dt_pk.strftime("%H:%M")
                if not (start_time <= time_str <= end_time):
                    continue

                filters = 0
                dir_pred = 'CALL' if candles[idx]['close'] > candles[idx]['open'] else 'PUT'
                if dir_pred == 'CALL':
                    if 30 <= rsi[idx] <= 50 and candles[idx]['close'] > ma50[idx]:
                        filters += 1
                else:
                    if 50 <= rsi[idx] <= 70 and candles[idx]['close'] < ma50[idx]:
                        filters += 1
                if dir_pred == 'CALL':
                    if macd[idx] > sig[idx] and macd[idx-1] <= sig[idx-1]:
                        filters += 1
                else:
                    if macd[idx] < sig[idx] and macd[idx-1] >= sig[idx-1]:
                        filters += 1
                if dir_pred == 'CALL':
                    if candles[idx]['close'] <= bb_lower[idx] * 1.01:
                        filters += 1
                else:
                    if candles[idx]['close'] >= bb_upper[idx] * 0.99:
                        filters += 1
                if dir_pred == 'CALL':
                    if k[idx] < 30 and k[idx] > d[idx]:
                        filters += 1
                else:
                    if k[idx] > 70 and k[idx] < d[idx]:
                        filters += 1
                acc = (filters / 4) * 100
                if acc >= MIN_ACCURACY:
                    raw_signals.append({
                        'pair': pair,
                        'time': time_str,
                        'dir': dir_pred,
                        'acc': acc,
                        'ts': ts
                    })
        except Exception as e:
            print(f"Error with {pair}: {e}")
            continue

    if not raw_signals:
        sender.send_message(uid, "❌ No signals found.")
        return

    TOLERANCE_MINUTES = 5
    def floor_time(t_str):
        h, m = map(int, t_str.split(':'))
        total = h*60 + m
        floor = (total // TOLERANCE_MINUTES) * TOLERANCE_MINUTES
        return f"{floor//60:02d}:{floor%60:02d}"

    groups = {}
    for sig in raw_signals:
        key = (sig['pair'], floor_time(sig['time']))
        groups.setdefault(key, []).append(sig)

    final_signals = []
    for key, sigs in groups.items():
        if len(sigs) >= 1:
            best = max(sigs, key=lambda x: x['acc'])
            final_signals.append(best)

    unique = {}
    for s in final_signals:
        uk = (s['pair'], s['time'])
        if uk not in unique or s['acc'] > unique[uk]['acc']:
            unique[uk] = s
    final = list(unique.values())
    final.sort(key=lambda x: x['time'])

    if not final:
        sender.send_message(uid, "❌ No signals after consistency check.")
        return

    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    date_str = now_pk.strftime("%Y-%m-%d")
    # Use fancy_font for non‑signal lines
    header = (
        f"{fancy_font('🏐 𝚂𝙼𝚉 𝙱𝙾𝚃 𝙵𝚄𝚃𝚄𝚁𝙴 🏐')}\n\n"
        f"{fancy_font('🗓 ')}{fancy_font(date_str)}\n\n"
        f"{fancy_font('💎 Timezone: UTC +05:00')}\n\n"
        f"{fancy_font('⏳ 𝚃𝙸𝙼𝙴𝙵𝚁𝙰𝙼𝙴: 𝙼𝟷')}\n"
        f"{fancy_font('⏰ 𝚄𝚂𝙴 𝙼𝚃𝙶 𝙾𝙽𝙴 𝙸𝙵 𝚁𝙴𝚀𝚄𝙸𝚁𝙴𝙳')}\n\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
    )
    body = "\n".join([f"M1 {s['pair']} {s['time']} {s['dir']}" for s in final])
    footer = (
        f"\n{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n\n"
        f"{fancy_font('𝚄𝚂𝙴 𝚂𝙰𝙵𝙴𝚃𝚈 𝙵𝙾𝚁 𝙱𝙴𝚃𝚃𝙴𝚁 𝚁𝙴𝚂𝚄𝙻𝚃 🔥')}"
    )
    full_msg = header + body + footer
    sender.send_message(uid, full_msg)

# ══════════════ TELEGRAM SENDER (Telethon) ══════════════


class TelegramSender:
    def __init__(self):
        self.client = None
        self.loop = None
        self.ready = False

    def start_with_bot_token(self, api_id, api_hash, bot_token):
        async def init():
            self.client = TelegramClient('finorix_session', api_id, api_hash)
            await self.client.start(bot_token=bot_token)
            self.ready = True
            print(f"{Fore.GREEN}[✓] Telethon ready.{Style.RESET_ALL}")
            while True:
                await asyncio.sleep(60)

        def run_loop():
            if sys.platform == 'win32':
                asyncio.set_event_loop_policy(
                    asyncio.WindowsSelectorEventLoopPolicy())
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(init())
            self.loop.run_forever()
        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
        timeout = 30
        start_time = time.time()
        while not self.ready and time.time() - start_time < timeout:
            time.sleep(0.5)
        if not self.ready:
            raise RuntimeError("Telethon init timeout")

    def _run_async(self, coro, timeout=30):
        if not self.ready:
            return None
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def _build_entities(self, text, add_bold=False):
      entities = []
      offset = 0
      for ch in text:
        clen = len(ch.encode('utf-16-le')) // 2
        eid = PREMIUM_EMOJI_IDS.get(ch) or FORMAT2_EMOJI_IDS.get(ch)
        if eid:
            entities.append(TelethonCustomEmoji(offset=offset, length=clen, document_id=eid))
        offset += clen
      if add_bold and text:
        total_len = len(text.encode('utf-16-le')) // 2
        entities.append(TelethonBold(offset=0, length=total_len))
      return entities

    def send_message(self, chat_id, text, buttons=None):
        async def _send():
            entities = self._build_entities(text)
            if buttons:
                return await self.client.send_message(chat_id, text, formatting_entities=entities, buttons=buttons)
            return await self.client.send_message(chat_id, text, formatting_entities=entities)
        return self._run_async(_send())

    def edit_message(self, chat_id, msg_id, text, buttons=None):
        async def _edit():
            entities = self._build_entities(text)
            if buttons:
                return await self.client.edit_message(chat_id, msg_id, text, formatting_entities=entities, buttons=buttons)
            return await self.client.edit_message(chat_id, msg_id, text, formatting_entities=entities)
        return self._run_async(_edit())

    def send_file(self, chat_id, file_path, caption):
        async def _send():
            entities = self._build_entities(caption)
            return await self.client.send_file(chat_id, file_path, caption=caption, formatting_entities=entities, force_document=False, supports_streaming=True)
        return self._run_async(_send())


sender = TelegramSender()


def fetch_blackout_signals(pair, start_time_utc5, end_time_utc5):
    api_start = convert_time_offset(start_time_utc5, 5, 6)
    api_end = convert_time_offset(end_time_utc5, 5, 6)
    api_pair = pair.replace("-OTC", "_otc").lower()
    url = f"https://blackoutsignal-qxapi.poghen-dx.workers.dev/pairs={api_pair}?start_time={api_start}&end_time={api_end}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("status") != "success":
            return None
        signals_raw = data.get("signals", [])
        times_utc5 = []
        for s in signals_raw:
            t = s.get("time")
            if t:
                utc5_time = convert_time_offset(t, 6, 5)
                times_utc5.append(utc5_time)
        return times_utc5
    except Exception as e:
        print(f"Blackout API error {pair}: {e}")
        return None

def _build_blackout_pair_page(page=0, per_page=15, selected=None):
    if selected is None:
        selected = set()
    total = len(SMZ_ALL_PAIRS)
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total)
    page_pairs = SMZ_ALL_PAIRS[start_idx:end_idx]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        short = pair.replace("-OTC", "")
        label = f"✅ {short}" if pair in selected else short
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"blk_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"blk_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"blk_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "blk_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

async def run_blackout_fs(uid, start_time, end_time, pairs_list):
    """Generate blackout signals for given pairs and time range."""
    from datetime import datetime, timezone, timedelta

    all_signals = []  # list of (pair, time)
    for pair in pairs_list:
        times = fetch_blackout_signals(pair, start_time, end_time)
        if times:
            for t in times:
                all_signals.append((pair, t))
    if not all_signals:
        sender.send_message(uid, "❌ No blackout signals found for the given criteria.")
        return

    # Sort by time
    all_signals.sort(key=lambda x: time_to_min(x[1]))

    # Build output
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    date_str = now_pk.strftime("%Y.%m.%d")
    header = (
        f"🐲 𝚂𝙼𝚉 𝙰𝙸 𝙿𝚁𝙾 𝙱𝙻𝙰𝙲𝙺𝙾𝚄𝚃 🐲\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💿 𝙼𝙾𝙳𝙴: 𝟷 𝚂𝚃𝙴𝙿 𝙼𝚃𝙶\n"
        f"⏱ 𝚃𝙵: 𝙼𝟷\n"
        f"🖥 𝚄𝚃𝙲+𝟻 (𝙿𝙺 𝚃𝙸𝙼𝙴)\n"
        f"━━━━━━━━━━━━━━━\n"
    )
    body = "\n".join([f"M1;{pair};{t}" for pair, t in all_signals])
    footer = (
        f"\n━━━━━━━━━━━━━━━\n"
        f"🚨 𝙴𝙽𝚃𝚁𝚈 𝙼𝙴𝚃𝙷𝙾𝙳:\n"
        f"➡️ 𝚈𝚘𝚞 𝚑𝚊𝚟𝚎 𝚝𝚘 𝚝𝚊𝚔𝚎 𝚝𝚑𝚎 𝚘𝚙𝚙𝚘𝚜𝚒𝚝𝚎 𝚝𝚛𝚊𝚍𝚎 𝚘𝚏 𝚝𝚑𝚎 𝚙𝚛𝚎𝚟𝚒𝚘𝚞𝚜 𝚌𝚊𝚗𝚍𝚕𝚎."
    )
    full_msg = header + body + footer
    sender.send_message(uid, full_msg)

def run_blackout_checker_worker(uid, date_str, signals, mtg_level, context):
    """Background thread: verify each signal using live candles (timestamp-based)."""
    tz_pk = timezone(timedelta(hours=5))
    now_utc5 = datetime.now(tz_pk)
    results = []  # (signal_line, result_icon, error_msg)
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    
    for pair, time_str in signals:
        # Check if signal time is in the future (make aware)
        signal_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        signal_dt = signal_dt.replace(tzinfo=tz_pk)
        if signal_dt > now_utc5:
            results.append((f"M1 {pair} {time_str}", "⏳", ""))
            continue

        api_pair = pair + "q"   # Keep -OTC
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={api_pair}&interval=1m&limit=2000:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                results.append((f"M1 {pair} {time_str}", "❌", "API error"))
                continue
            data = resp.json()
            candles = data.get('candles', [])
            if not candles:
                results.append((f"M1 {pair} {time_str}", "❌", "no candles"))
                continue

            # Build lookup: key = date (YYYY-MM-DD) -> dict of HH:MM -> candle
            lookup = {}
            for c in candles:
                ts = c.get('time')
                if not ts:
                    continue
                dt_local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz_pk)
                date_key = dt_local.strftime("%Y-%m-%d")
                time_key = dt_local.strftime("%H:%M")
                if date_key not in lookup:
                    lookup[date_key] = {}
                lookup[date_key][time_key] = c

            day_lookup = lookup.get(date_str, {})
            if not day_lookup:
                results.append((f"M1 {pair} {time_str}", "❌", "no candles for date"))
                continue

            t_h, t_m = map(int, time_str.split(':'))
            entry_key = time_str

            # Previous minute (T-1)
            if t_m == 0:
                prev_key = f"{t_h-1:02d}:59"
                prev_date = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                prev_candle = lookup.get(prev_date, {}).get(prev_key)
            else:
                prev_key = f"{t_h:02d}:{t_m-1:02d}"
                prev_candle = day_lookup.get(prev_key)
                prev_date = date_str

            if not prev_candle:
                results.append((f"M1 {pair} {time_str}", "❌", "no previous candle"))
                continue

            prev_dir = "UP" if prev_candle['close'] > prev_candle['open'] else "DOWN"
            expected_dir = "DOWN" if prev_dir == "UP" else "UP"

            # Entry candle
            entry_candle = day_lookup.get(entry_key)
            if not entry_candle:
                results.append((f"M1 {pair} {time_str}", "❌", "no entry candle"))
                continue

            entry_dir = "UP" if entry_candle['close'] > entry_candle['open'] else "DOWN"
            result_icon = "❌"
            if entry_dir == expected_dir:
                result_icon = "✅"
            else:
                # MTG 1
                if mtg_level >= 1:
                    if t_m == 59:
                        next_key = f"{t_h+1:02d}:00"
                        next_date = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                        next_candle = lookup.get(next_date, {}).get(next_key)
                    else:
                        next_key = f"{t_h:02d}:{t_m+1:02d}"
                        next_candle = day_lookup.get(next_key)
                    if next_candle:
                        next_dir = "UP" if next_candle['close'] > next_candle['open'] else "DOWN"
                        if next_dir == expected_dir:
                            result_icon = "✅¹"
                # MTG 2
                if result_icon == "❌" and mtg_level >= 2:
                    total_min = t_h * 60 + t_m + 2
                    new_h = (total_min // 60) % 24
                    new_m = total_min % 60
                    next2_key = f"{new_h:02d}:{new_m:02d}"
                    if total_min >= 1440:
                        next2_date = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                        next2_candle = lookup.get(next2_date, {}).get(next2_key)
                    else:
                        next2_candle = day_lookup.get(next2_key)
                    if next2_candle:
                        next2_dir = "UP" if next2_candle['close'] > next2_candle['open'] else "DOWN"
                        if next2_dir == expected_dir:
                            result_icon = "✅²"
            results.append((f"M1 {pair} {time_str}", result_icon, ""))
        except Exception as e:
            results.append((f"M1 {pair} {time_str}", "❌", str(e)[:50]))

    # Count wins, losses, pending (exclude pending from win/loss)
    wins = sum(1 for _, icon, _ in results if icon.startswith("✅"))
    losses = sum(1 for _, icon, _ in results if icon == "❌")
    pending = sum(1 for _, icon, _ in results if icon == "⏳")
    total_checked = wins + losses
    total_signals = len(results)

    header = (
        f"{fancy_font('▰▱▱ 𝚂𝙼𝚉 𝙱𝙻𝙰𝙲𝙺𝙾𝚄𝚃 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 ▱▱▰')}\n"
        f"{fancy_font('              ┏━━━━━━━━━━━┓')}\n"
        f"{fancy_font('                 🗓 - ')}{fancy_font(date_str)}{fancy_font('          ')}\n"
        f"{fancy_font('              ┗━━━━━━━━━━━┛')}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
    )
    body = "\n".join([f"{sig} {icon}" for sig, icon, _ in results])
    summary = (
        f"\n{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
        f"{fancy_font('🏆 Total : ')}{fancy_font(str(total_signals))}\n"
        f"{fancy_font('✅ Win: ')}{fancy_font(str(wins))}\n"
        f"{fancy_font('✖ Loss: ')}{fancy_font(str(losses))}\n"
        f"{fancy_font('⏳ Pending: ')}{fancy_font(str(pending))}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}"
    )
    final_msg = header + body + summary
    sender.send_message(uid, final_msg)


def progress_bar_text(pct: int) -> str:
    filled = int(pct / 10)
    bar = "█" * filled + "░" * (10 - filled)
    return f"[{bar}] {pct}%"

# ══════════════ MESSAGE BUILDERS (MISSING FROM PART 1) ══════════════


def build_signal_message(pair, entry_time, direction, payout, trend_text):

    pass

    dir_emoji = "📉" if direction == "CALL" else "📈"
    return (
        f"❀° ┄────────=─────────╮\n"
        f"   👑 𝚂𝙼𝚉𝚇-𝙰𝙸 𝚅𝟺.𝟹 👑\n"
        f"╰────────=───=─────┄ °❀\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"📊 Pair∶— {fancy_font(pair)}\n"
        f"⏳ TimeFrame∶— 𝙼𝟷\n"
        f"🔰 TradeTime∶— {fancy_font(entry_time)}\n"
        f"{dir_emoji} Direction∶— {fancy_font(direction)}\n"
        f"┗─── ⊹˚───────────♡───┛\n"
        f"💎 Payout∶— {fancy_font(payout)}% 📊 Trend∶— 📈 {fancy_font(trend_text)}\n"
        f"•❅✦──────✧❅✦❅✧──────✦❅•\n"
        f"😈 𝚂𝙼𝚉𝚇 𝚅𝟺.𝟹 - 𝙰𝙸 𝚃𝚁𝙰𝙳𝙸𝙽𝙶 𝚂𝙾𝙵𝚃𝚆𝙰𝚁𝙴"
    )


def build_result_message_first_win(pair, entry_time, payout, wins, losses):

    pass

    win_rate = int((wins / (wins + losses)) *
                   100) if (wins + losses) > 0 else 100
    return (
        f"•❅✦─𝚂𝙼𝚉𝚇 𝚁𝙴𝚂𝚄𝙻𝚃𝚂 𝚅𝟺.𝟹─✦❅•\n\n" f"┏━⋅━⋅━━⋅༻  ᵔᴗᵔ  ༺⋅━━⋅━⋅━┓\n" f"  {
            fancy_font(pair)} ➛ {
            fancy_font(entry_time)} ➛ {
                fancy_font(payout)}%\n" f"✅✅✅ 𝚂𝚄𝚁𝙴𝚂𝙷𝙾𝚃!! ✅✅✅\n" f"┗━⋅━⋅━━⋅༻ıllıʬıllı༺⋅━━⋅━⋅━┛\n" f"✅ 𝚆𝚒𝚗: {
                    fancy_font(
                        str(wins))} | |✨ 𝙻𝚘𝚜𝚜: {
                            fancy_font(
                                str(losses))} |🏆 ({
                                    fancy_font(
                                        str(win_rate))}%)\n\n" f"💎Developer∶— @Rohailtrader")


def build_result_message_second_win(pair, entry_time, payout, wins, losses):

    pass

    win_rate = int((wins / (wins + losses)) *
                   100) if (wins + losses) > 0 else 100
    return (
        f"•❅✦─𝚂𝙼𝚉𝚇 𝚁𝙴𝚂𝚄𝙻𝚃𝚂 𝚅𝟺.𝟹─✦❅•\n\n" f"┏━⋅━⋅━━⋅༻  ᵔᴗᵔ  ༺⋅━━⋅━⋅━┓\n" f"  {
            fancy_font(pair)} ➛ {
            fancy_font(entry_time)} ➛ {
                fancy_font(payout)}%\n" f"✅✅✅ 𝚆𝙸𝙽 — 𝙶𝟷 ✅✅✅\n" f"┗━⋅━⋅━━⋅༻ıllıʬıllı༺⋅━━⋅━⋅━┛\n" f"✅ 𝚆𝚒𝚗: {
                    fancy_font(
                        str(wins))} | |✨ 𝙻𝚘𝚜𝚜: {
                            fancy_font(
                                str(losses))} |🏆 ({
                                    fancy_font(
                                        str(win_rate))}%)\n\n" f"💎Developer∶— @Rohailtrader")


def build_result_message_loss(pair, entry_time, payout, wins, losses):

    pass

    win_rate = int((wins / (wins + losses)) *
                   100) if (wins + losses) > 0 else 100
    return (
        f"•❅✦─𝚂𝙼𝚉𝚇 𝚁𝙴𝚂𝚄𝙻𝚃𝚂 𝚅𝟺.𝟹─✦❅•\n\n" f"┏━⋅━⋅━━⋅༻  ᵔᴗᵔ  ༺⋅━━⋅━⋅━┓\n" f"  {
            fancy_font(pair)} ➛ {
            fancy_font(entry_time)} ➛ {
                fancy_font(payout)}%\n" f"❌❌❌ 𝙻𝙾𝚂𝚂 ❌❌❌\n" f"┗━⋅━⋅━━⋅༻ıllıʬıllı༺⋅━━⋅━⋅━┛\n" f"✅ 𝚆𝚒𝚗: {
                    fancy_font(
                        str(wins))} | |✨ 𝙻𝚘𝚜𝚜: {
                            fancy_font(
                                str(losses))} |🏆 ({
                                    fancy_font(
                                        str(win_rate))}%)\n\n" f"💎Developer∶— @Rohailtrader")


def build_future_signal_header(signal_list):

    pass

    lines = [
        "📊 UTC +6",
        "💎 MAX MARTINGALE： 01",
        "🔅 1 MINUTE",
        "     🤖 Software： SMZX4.3 🏆",
        ""
    ]
    for sig in signal_list:
        dir_text = "𝙲𝙰𝙻𝙻" if sig['dir'] == "CALL" else "𝙿𝚄𝚃"
        lines.append(
            f"❒ {
                fancy_font(
                    sig['pair'])} ➪ {
                fancy_font(
                    sig['time'])} ➪ {dir_text}")
    return "\n".join(lines)

def build_signal_format2(pair, entry_time, direction):
    dir_emoji = "📉" if direction == "CALL" else "📈"
    return (
        f"📊{pair}\n\n"
        f"⏰Time : {entry_time} (+5 UTC) 🇵🇰\n\n"
        f"⏳Time : M1🤔\n\n"
        f"💀 GO FOR {'UP' if direction == 'CALL' else 'DOWN'} {dir_emoji}\n\n"
        f"👿AVOID DOJI CANDLES 👿\n\n"
        f"🤓1 STEP MTG\n\n"
        f"🏆 OWNER @Rohailtrader 🦇"
    )

def build_result_first_win_format2(pair, entry_time):
    return (
        f"𒆜==== RESULTS ====𒆜\n\n"
        f"📊 {pair}\n"
        f"🕐 {entry_time}\n\n"
        f"✅ ! NON MTG SURESHOT ! ✅\n\n"
        f"🏆 FEEDBACK:- @Rohailtrader 🦇"
    )

def build_result_second_win_format2(pair, entry_time):
    return (
        f"𒆜==== RESULT ====𒆜\n\n"
        f"📊 {pair}\n"
        f"🕐 {entry_time}\n\n"
        f"✅ ! MTG SURESHORT ! ✅\n\n"
        f"🏆 FEEDBACK:- @Rohailtrader 🦇"
    )

def build_result_loss_format2(pair, entry_time):
    return (
        f"𒆜==== RESULTS ====𒆜\n\n"
        f"📊 {pair}\n"
        f"🕐 {entry_time}\n\n"
        f"❌ ! LOSS ! ❌\n\n"
    )

# ══════════════ AI MODE – Multi-strategy consensus engine (MODIFIED with


def _ai_analyze_pair(pair, candles, payout_num):

    pass

    hits = []
    s2_filters = Strategy2Filters()
    analyzers = [
        (2, lambda c: analyze_strategy2(c, s2_filters)),
        (3, lambda c: analyze_strategy3(c, 65, 20)),
        (4, lambda c: analyze_strategy4(c, 55)),
        (5, lambda c: analyze_strategy5(c, 60)),
        (6, lambda c: analyze_strategy6(c, 20, 10)),
    ]
    for strat_id, analyzer in analyzers:
        try:
            direction, entry_dt, score = analyzer(candles)
            if direction and score:
                hits.append({
                    'strategy': strat_id,
                    'direction': direction,
                    'entry_dt': entry_dt,
                    'score': score,
                    'pair': pair,
                    'payout': payout_num,
                })
        except Exception:
            pass
    return hits


def _ai_rank_signals(all_hits, uid):

    pass

    st = get_state(uid)
    min_consensus = st.ai_min_consensus
    required = set(st.ai_required_strategies)
    grouped = {}
    for h in all_hits:
        key = (h['pair'], h['direction'])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(h)
    ranked = []
    for (pair, direction), strats in grouped.items():
        n_agree = len(strats)
        if n_agree < min_consensus:
            continue
        if required:
            present_strats = {s['strategy'] for s in strats}
            if not required.issubset(present_strats):
                continue
        avg_score = sum(s['score'] for s in strats) / n_agree
        best = max(strats, key=lambda s: s['score'])
        consensus_bonus = (n_agree - 1) * 8
        final_score = min(99, avg_score + consensus_bonus)
        ranked.append({
            'pair': pair,
            'direction': direction,
            'final_score': round(final_score, 1),
            'avg_score': round(avg_score, 1),
            'best_strategy': best['strategy'],
            'best_score': round(best['score'], 1),
            'n_strategies': n_agree,
            'strategies': sorted([s['strategy'] for s in strats]),
            'entry_dt': best['entry_dt'],
            'payout': best['payout'],
        })
    ranked.sort(key=lambda x: x['final_score'], reverse=True)
    return ranked


def _ai_build_analysis_msg(ranked, scan_time_sec, uid):

    pass

    st = get_state(uid)
    min_cons = st.ai_min_consensus
    req_strats = st.ai_required_strategies
    if not ranked:
        return (
            "❀° ┄────────=─────────╮\n"
            "   🤖 𝙰𝙸 𝙼𝙾𝙳𝙴 — 𝚂𝙼𝚉𝚇 🤖\n"
            "╰────────=───=─────┄ °❀\n\n"
            "❌ No signals found across all strategies.\n"
            "⏳ Try again in 1 minute.\n"
        )
    top = ranked[0]
    strat_names = {
        1: "RSI",
        2: "EMA",
        3: "WR",
        4: "ADX",
        5: "Confluence",
        6: "IROF"}
    strat_list = " + ".join(strat_names.get(s,
                                            f"ST{s}") for s in top['strategies'])
    dir_emoji = "📉" if top['direction'] == "CALL" else "📈"
    stars = "⭐" * min(top['n_strategies'], 6)
    msg = (
        f"❀° ┄────────=─────────╮\n"
        f"   🤖 𝙰𝙸 𝙼𝙾𝙳𝙴 — 𝚂𝙼𝚉𝚇 🤖\n"
        f"╰────────=───=─────┄ °❀\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"📊 Pair∶— {fancy_font(top['pair'])}\n"
        f"{dir_emoji} Direction∶— {fancy_font(top['direction'])}\n"
        f"💎 AI Score∶— {fancy_font(str(top['final_score']) + '%')}\n"
        f"⏰ Entry∶— {fancy_font(top['entry_dt'].strftime('%H:%M'))}\n"
        f"💲 Payout∶— {fancy_font(str(top['payout']) + '%')}\n"
        f"┗───˚⊹ ─────────♡───┛\n\n"
        f"🔰 Min consensus required: {min_cons} strategies\n"
    )
    if req_strats:
        req_str = ", ".join(f"ST{s}" for s in req_strats)
        msg += f"🎯 Required strategies: {req_str}\n"
    msg += (
        f"\n🔥 Strategy Consensus\n" f"✅ {
            top['n_strategies']}/5 strategies agree {stars}\n" f"🔰 Strategies∶ {
            fancy_font(strat_list)}\n" f"🏆 Best∶ {
                fancy_font(
                    ', '.join(
                        f'ST{s}' for s in top['strategies']))} ({
                            fancy_font(
                                str(
                                    top['final_score']) +
                                '%')})\n" f"📊 Average∶ {
            top['avg_score']}%\n\n")
    if len(ranked) > 1:
        msg += "💪 Other Signals Found\n"
        for i, r in enumerate(ranked[1:5], 2):
            s_list = ",".join(str(s) for s in r['strategies'])
            r_emoji = "📉" if r['direction'] == "CALL" else "📈"
            msg += f"  {r_emoji} #{i} {
                r['pair']} {
                r['direction']} {
                r['final_score']}% (ST{s_list})\n"
        msg += "\n"
    msg += (
        f"⏳ Scan time∶ {scan_time_sec:.1f}s | {len(ranked)} signals found\n"
        f"✨ ©OWNER @Rohailtrader ✨"
    )
    return msg


def _ai_build_result_msg(
        pair,
        direction,
        result,
        score,
        n_strats,
        wins,
        losses):
    if result == "WIN":
        r_emoji = "✅"
        r_text = "WIN"
    elif result == "MTG WIN":
        r_emoji = "✅"
        r_text = "MTG WIN"
    else:
        r_emoji = "❌"
        r_text = "LOSS"
    total = wins + losses
    wr = (wins / total * 100) if total > 0 else 0
    return (
        f"❀° ┄────────=─────────╮\n"
        f"   🤖 𝙰𝙸 𝚁𝙴𝚂𝚄𝙻𝚃 — 𝚂𝙼𝚉𝚇 🤖\n"
        f"╰────────=───=─────┄ °❀\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"📊 Pair∶— {fancy_font(pair)}\n"
        f"{r_emoji} Result∶— {fancy_font(r_text)}\n"
        f"💎 AI Score∶— {fancy_font(str(score) + '%')}\n"
        f"🏆 Win Rate∶— {fancy_font(f'{wr:.0f}%')} ({wins}W/{losses}L)\n"
        f"🔰 Strategies∶ {n_strats}/5 agreed\n"
        f"┗───˚⊹ ─────────♡───┛\n\n"
        f"✨ ©OWNER @Rohailtrader ✨"
    )


def run_ai_mode(uid):

    pass

    """AI Mode: scan all pairs with ST2-6, pick best signal with consensus."""
    st = get_state(uid)
    st.running = True
    st.stop_requested = False

    progress_msg = sender.send_message(
        uid, "🤖 AI Mode activated!\n"
        "⏳ Scanning all pairs with 5 strategies (ST2-6)...\n"
        "💎 Finding the best signal for you...\n\n" f"{
            progress_bar_text(0)}")
    if not progress_msg:
        st.running = False
        return
    progress_id = progress_msg.id

    bot = SMZXBot(uid)
    pairs = bot.pairs
    all_hits = []
    scan_start = time.time()

    for idx, pair in enumerate(pairs):
        if st.stop_requested:
            break
        pct = int((idx + 1) / len(pairs) * 100)
        sender.edit_message(uid, progress_id,
                            f"🤖 AI Mode — Scanning...\n"
                            f"📊 Analyzing {pair}\n"
                            f"🔥 Running ST2-6 analysis\n"
                            f"✅ {len(all_hits)} signals found so far\n\n"
                            f"{progress_bar_text(pct)}"
                            )
        candles, price, payout = bot.fetch_data(pair, limit=600)
        if not candles:
            continue
        try:
            payout_num = int(payout) if payout != "!" else 77
        except (ValueError, TypeError):
            payout_num = 0
        if bot.market_type == "OTC" and payout_num < 77:
            continue
        if pair in st.last_loss:
            now = datetime.now(timezone.utc) + timedelta(hours=5)
            if (now - st.last_loss[pair]
                ).total_seconds() < st.loss_cooldown_minutes * 60:
                continue
        hits = _ai_analyze_pair(pair, candles, payout_num)
        all_hits.extend(hits)

    scan_time = time.time() - scan_start

    if st.stop_requested:
        sender.edit_message(uid, progress_id, "🤖 AI Mode stopped.")
        st.running = False
        return

    # ----- FIXED: pass uid to ranking function -----
    ranked = _ai_rank_signals(all_hits, uid)

    if not ranked:
        sender.edit_message(
            uid, progress_id, "🤖 AI Mode — Scan complete\n"
            "❌ No valid signals found.\n" f"⏳ Scanned {
                len(pairs)} pairs in {
                scan_time:.1f}s\n\n" "Try again in 1 minute or use /stop to return.")
        st.running = False
        return

    top = ranked[0]

    # Recalculate entry time to the NEXT minute from NOW
    fresh_entry = datetime.now(timezone.utc) + timedelta(hours=5)
    fresh_entry = fresh_entry.replace(
        second=0, microsecond=0) + timedelta(minutes=1)
    top['entry_dt'] = fresh_entry

    # ----- FIXED: pass uid to message builder -----
    analysis_msg = _ai_build_analysis_msg(ranked, scan_time, uid)
    sender.edit_message(
        uid, progress_id, f"🤖 AI Mode — ✅ Best signal found!\n" f"📊 {
            top['pair']} → {
            top['direction']}\n" f"💎 AI Score: {
                top['final_score']}% ({
                    top['n_strategies']}/5 strategies)\n\n" "Sending chart...")

    candles, price, payout = bot.fetch_data(top['pair'], limit=600)
    if not candles:
        sender.send_message(uid, "❌ Failed to fetch chart data. Try again.")
        st.running = False
        return

    try:
        payout_pct = float(str(top['payout']).replace("%", ""))
    except (ValueError, TypeError):
        payout_pct = 92.0

    entry_t = top['entry_dt'].strftime("%H:%M")
    chart_path = draw_neon_chart(
        candles,
        top['pair'],
        entry_t,
        top['direction'],
        top['payout'],
        confidence=top['final_score'],
        wins=st.stats['wins'],
        losses=st.stats['losses'],
        strategy=top['best_strategy'],
        martingale_steps=1,
        signal_history=st.signal_history)
    if chart_path and os.path.exists(chart_path):
        sender.send_file(uid, chart_path, analysis_msg)
        try:
            os.remove(chart_path)
        except Exception:
            pass
    else:
        sender.send_message(uid, analysis_msg)

    # Send MM signal message if enabled
    if st.mm_enabled:
        sender.send_message(
            uid, mm_build_signal_msg(
                st, top['pair'], top['direction']))

    entry_dt_utc5 = top['entry_dt']
    direction = top['direction']
    pair = top['pair']
    payout_str = str(top['payout'])

    close_time_1 = entry_dt_utc5 + timedelta(minutes=1)
    bot.sleep_until(close_time_1)
    if st.stop_requested:
        st.running = False
        return
    candles, _, _ = bot.fetch_data(pair, limit=750)
    if not candles:
        st.running = False
        return
    first = bot.get_candle_at_time(candles, entry_dt_utc5)
    if not first:
        st.running = False
        return

    win1 = (
        first['close'] > first['open']) if direction == "CALL" else (
        first['close'] < first['open'])
    trade_type = "NON-MTG"
    st.signal_history.append({
        'pair': pair, 'direction': direction,
        'time': entry_dt_utc5.strftime('%H:%M'),
        'result': "WIN" if win1 else "LOSS",
        'type': trade_type
    })
    if not win1:
        st.last_loss[pair] = datetime.now(timezone.utc) + timedelta(hours=5)
    if win1:
        st.stats['wins'] += 1
        result_msg = _ai_build_result_msg(
            pair,
            direction,
            "WIN",
            top['final_score'],
            top['n_strategies'],
            st.stats['wins'],
            st.stats['losses'])
        chart_path = draw_result_chart(
            candles,
            pair,
            top['payout'],
            "WIN",
            first,
            wins=st.stats['wins'],
            losses=st.stats['losses'],
            strategy=top['best_strategy'],
            direction=direction,
            entry_time_str=entry_t,
            signal_history=st.signal_history)
        if chart_path and os.path.exists(chart_path):
            sender.send_file(uid, chart_path, result_msg)
            try:
                os.remove(chart_path)
            except Exception:
                pass
        else:
            sender.send_message(uid, result_msg)
        if st.mm_enabled:
            pl, old_bal, tp_hit, sl_hit = mm_update_after_result(
                st, "WIN", payout_pct)
            sender.send_message(
                uid, mm_build_result_msg(
                    st, "WIN", pl, old_bal))
            if tp_hit or sl_hit:
                st.mm_enabled = False
        sender.send_message(
            uid, "🤖 AI Mode — Use /continue for next AI signal, or /stop to return.")
        st.running = False
        return

    close_time_2 = entry_dt_utc5 + timedelta(minutes=2)
    bot.sleep_until(close_time_2)
    if st.stop_requested:
        st.running = False
        return
    candles2, _, _ = bot.fetch_data(pair, limit=750)
    if not candles2:
        st.running = False
        return
    second = bot.get_candle_at_time(
        candles2, entry_dt_utc5 + timedelta(minutes=1))
    if not second:
        st.running = False
        return
    win2 = (
        second['close'] > second['open']) if direction == "CALL" else (
        second['close'] < second['open'])
    if win2:
        st.signal_history[-1]['result'] = "WIN"
        st.signal_history[-1]['type'] = "MTG"
        st.stats['wins'] += 1
        result_msg = _ai_build_result_msg(
            pair,
            direction,
            "MTG WIN",
            top['final_score'],
            top['n_strategies'],
            st.stats['wins'],
            st.stats['losses'])
        chart_path = draw_result_chart(
            candles2,
            pair,
            top['payout'],
            "MTG WIN",
            first,
            second,
            wins=st.stats['wins'],
            losses=st.stats['losses'],
            strategy=top['best_strategy'],
            direction=direction,
            entry_time_str=entry_t,
            signal_history=st.signal_history)
        if chart_path and os.path.exists(chart_path):
            sender.send_file(uid, chart_path, result_msg)
            try:
                os.remove(chart_path)
            except Exception:
                pass
        else:
            sender.send_message(uid, result_msg)
        if st.mm_enabled:
            pl, old_bal, tp_hit, sl_hit = mm_update_after_result(
                st, "MTG WIN", payout_pct)
            sender.send_message(
                uid, mm_build_result_msg(
                    st, "MTG WIN", pl, old_bal))
            if tp_hit or sl_hit:
                st.mm_enabled = False
    else:
        st.stats['losses'] += 1
        result_msg = _ai_build_result_msg(
            pair,
            direction,
            "LOSS",
            top['final_score'],
            top['n_strategies'],
            st.stats['wins'],
            st.stats['losses'])
        chart_path = draw_result_chart(
            candles2,
            pair,
            top['payout'],
            "LOSS",
            first,
            wins=st.stats['wins'],
            losses=st.stats['losses'],
            strategy=top['best_strategy'],
            direction=direction,
            entry_time_str=entry_t,
            signal_history=st.signal_history)
        if chart_path and os.path.exists(chart_path):
            sender.send_file(uid, chart_path, result_msg)
            try:
                os.remove(chart_path)
            except Exception:
                pass
        else:
            sender.send_message(uid, result_msg)
        if st.mm_enabled:
            pl, old_bal, tp_hit, sl_hit = mm_update_after_result(
                st, "LOSS", payout_pct)
            sender.send_message(
                uid, mm_build_result_msg(
                    st, "LOSS", pl, old_bal))
            if tp_hit or sl_hit:
                st.mm_enabled = False

    sender.send_message(
        uid, "🤖 AI Mode — Use /continue for next AI signal, or /stop to return.")
    st.running = False

def draw_loss_chart(pair, entry_time_str, direction, candles):
    """
    Draw a simple candlestick chart for loss analysis.
    candles: list of dicts with keys: time (timestamp), open, high, low, close
    entry_time_str: the actual entry time used by the checker (HH:MM)
    Returns path to saved PNG image.
    """
    from PIL import Image, ImageDraw, ImageFont
    import uuid
    from datetime import datetime, timezone, timedelta

    if not candles or len(candles) < 2:
        return None

    n = len(candles)                     # number of candles to draw
    width, height = 800, 500
    margin_left, margin_right = 60, 40
    margin_top, margin_bottom = 40, 60
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom

    # Candle width: leave a 2px gap between candles
    candle_width = chart_width // n - 2
    if candle_width < 3:
        candle_width = 3

    # Determine price range with 5% padding
    prices = [c['high'] for c in candles] + [c['low'] for c in candles]
    p_min = min(prices)
    p_max = max(prices)
    padding = (p_max - p_min) * 0.05
    p_min -= padding
    p_max += padding

    # Create white background
    img = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(img)

    # Load fonts (fallback if custom not found)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
        font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
    except:
        font = ImageFont.load_default()
        font_bold = font

    def price_to_y(p):
        return margin_top + chart_height - ((p - p_min) / (p_max - p_min) * chart_height)

    # Draw grid lines and price labels (5 horizontal lines)
    step = (p_max - p_min) / 5
    for i in range(6):
        y = price_to_y(p_min + i * step)
        draw.line([(margin_left, y), (width - margin_right, y)], fill='lightgray', width=1)
        price_text = f"{p_min + i * step:.4f}".rstrip('0').rstrip('.')
        draw.text((margin_left - 35, y - 6), price_text, fill='black', font=font)

    # Convert candle times to UTC+5 for x-axis labels
    tz_pk = timezone(timedelta(hours=5))
    times = []
    for c in candles:
        ts = c.get('time')
        if ts:
            dt_local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz_pk)
            times.append(dt_local.strftime("%H:%M"))
        else:
            times.append("??:??")

    # Draw each candle
    for idx, c in enumerate(candles):
        x_center = margin_left + idx * (chart_width / n) + (chart_width / n) / 2
        x_left = x_center - candle_width // 2
        x_right = x_center + candle_width // 2

        o = c['open']
        h = c['high']
        l = c['low']
        cl = c['close']

        y_open = price_to_y(o)
        y_close = price_to_y(cl)
        y_high = price_to_y(h)
        y_low = price_to_y(l)

        # Wick
        draw.line([(x_center, y_high), (x_center, y_low)], fill='black', width=1)

        # Candle body
        if cl >= o:
            fill_color = '#00ff00'       # green
            outline = 'darkgreen'
            top = y_close
            bottom = y_open
        else:
            fill_color = '#ff3333'       # red
            outline = 'darkred'
            top = y_open
            bottom = y_close
        draw.rectangle([x_left, top, x_right, bottom], fill=fill_color, outline=outline, width=1)

        # Mark the entry candle (the one whose time == entry_time_str)
        # Note: we compare with entry_time_str passed from checker
        if times[idx] == entry_time_str:
            rect_padding = 5
            draw.rectangle([x_left - rect_padding, top - rect_padding,
                            x_right + rect_padding, bottom + rect_padding],
                           outline='red', width=2)

    # X-axis time labels
    for idx, t in enumerate(times):
        x_center = margin_left + idx * (chart_width / n) + (chart_width / n) / 2
        draw.text((x_center - 15, height - margin_bottom + 5), t, fill='black', font=font)

    # Title
    title = f"{pair}  {direction}  LOSS"
    draw.text((width//2 - 80, 10), title, fill='red', font=font_bold)

    # Footer
    draw.text((width//2 - 80, height - 20), "POWERED BY SMZ", fill='gray', font=font)

    # Save and return
    path = f"loss_chart_{uuid.uuid4().hex[:8]}.png"
    img.save(path)
    return path

# ══════════════ MONEY MANAGEMENT HELPERS ══════════════


def mm_calculate_base_amount(balance, sl_amount):

    pass

    """Calculate safe base trade amount considering 3-level martingale."""
    max_steps = 3
    total_multiplier = sum(2**i for i in range(max_steps))  # 1+2+4 = 7
    base = sl_amount / total_multiplier
    base = math.floor(base * 100) / 100
    base = max(0.50, base)
    cap = balance * 0.05
    base = min(base, cap)
    return round(base, 2)


def mm_get_trade_amount(st):

    pass

    """Get current trade amount considering consecutive losses (cross-signal martingale)."""
    multiplier = 2 ** min(st.mm_consecutive_losses, 3)
    amount = st.mm_base_amount * multiplier
    max_allowed = st.mm_current_balance * 0.25
    amount = min(amount, max_allowed)
    return round(max(0.50, amount), 2)


def mm_build_signal_msg(st, pair, direction):

    pass

    """Build MM info message to send alongside signal."""
    trade_amt = mm_get_trade_amount(st)
    mtg_amt = round(trade_amt * 2, 2)
    pnl_sign = "+" if st.mm_pnl >= 0 else ""
    pnl_emoji = "📈" if st.mm_pnl >= 0 else "📉"
    tp_pct = min(100, abs(st.mm_pnl / st.mm_tp * 100)) if st.mm_tp > 0 else 0
    sl_remaining = st.mm_sl - abs(min(0, st.mm_pnl))
    step_label = f"Step {st.mm_consecutive_losses + 1}"
    return (
        f"💎 𝚂𝙼𝚉𝚇 𝙼𝙾𝙽𝙴𝚈 𝙼𝙰𝙽𝙰𝙶𝙴𝙼𝙴𝙽𝚃\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"💲 Trade Amount∶— ${trade_amt:.2f}\n"
        f"💎 Balance∶— ${st.mm_current_balance:.2f}\n"
        f"🏆 TP Target∶— ${st.mm_tp:.2f} ({tp_pct:.0f}% done)\n"
        f"🔰 SL Limit∶— ${st.mm_sl:.2f} (${sl_remaining:.2f} left)\n"
        f"{pnl_emoji} P&L∶— {pnl_sign}${st.mm_pnl:.2f}\n"
        f"💪 MTG∶— {step_label} (if loss → ${mtg_amt:.2f})\n"
        f"┗───˚⊹ ─────────♡───┛\n"
        f"✨ ©OWNER @Rohailtrader ✨"
    )


def mm_build_result_msg(st, result, profit_loss, old_balance):

    pass

    """Build MM update message after trade result."""
    pnl_sign = "+" if st.mm_pnl >= 0 else ""
    pnl_emoji = "📈" if st.mm_pnl >= 0 else "📉"
    tp_pct = min(100, abs(st.mm_pnl / st.mm_tp * 100)) if st.mm_tp > 0 else 0
    sl_remaining = st.mm_sl - abs(min(0, st.mm_pnl))
    next_amt = mm_get_trade_amount(st)
    pl_sign = "+" if profit_loss >= 0 else ""
    r_emoji = "✅" if profit_loss >= 0 else "❌"

    msg = (
        f"💎 𝚂𝙼𝚉𝚇 𝙼𝙼 𝚄𝙿𝙳𝙰𝚃𝙴\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"{r_emoji} {result} — {pl_sign}${profit_loss:.2f}\n"
        f"💲 Balance∶— ${st.mm_current_balance:.2f}\n"
        f"{pnl_emoji} Today P&L∶— {pnl_sign}${st.mm_pnl:.2f}\n"
        f"🏆 TP∶— ${st.mm_tp:.2f} ({tp_pct:.0f}%)\n"
        f"🔰 SL∶— ${sl_remaining:.2f} remaining\n"
        f"💪 Next Trade∶— ${next_amt:.2f}\n"
        f"┗───˚⊹ ─────────♡───┛\n"
    )

    if st.mm_pnl >= st.mm_tp:
        msg += (
            f"\n🏆🏆🏆 𝚃𝙿 𝙷𝙸𝚃! 🏆🏆🏆\n"
            f"🔥 Target reached! +${st.mm_pnl:.2f}\n"
            f"✅ Great trading session!\n"
            f"💎 Final Balance∶ ${st.mm_current_balance:.2f}\n"
        )
    elif abs(min(0, st.mm_pnl)) >= st.mm_sl:
        msg += (
            f"\n⚠️⚠️⚠️ 𝚂𝙻 𝙷𝙸𝚃! ⚠️⚠️⚠️\n"
            f"❌ Stop loss reached! -${abs(st.mm_pnl):.2f}\n"
            f"🔰 Session stopped to protect capital.\n"
            f"💎 Final Balance∶ ${st.mm_current_balance:.2f}\n"
        )
    elif st.mm_consecutive_losses >= 2:
        msg += f"⚠️ Warning∶ {
            st.mm_consecutive_losses} consecutive losses — stay careful!\n"

    msg += f"✨ ©OWNER @Rohailtrader ✨"
    return msg


def mm_update_after_result(st, result, payout_pct):

    pass

    """Update MM balance after a trade result. Returns (profit_loss, old_balance, tp_hit, sl_hit)."""
    trade_amt = mm_get_trade_amount(st)
    old_balance = st.mm_current_balance
    payout_ratio = payout_pct / 100.0

    if result == "WIN":
        profit = trade_amt * payout_ratio
        st.mm_current_balance += profit
        st.mm_pnl += profit
        st.mm_consecutive_losses = 0
        # 🔁 Recalculate base amount after balance change
        st.mm_base_amount = mm_calculate_base_amount(
            st.mm_current_balance, st.mm_sl)
        return (profit, old_balance, st.mm_pnl >= st.mm_tp, False)
    elif result == "MTG WIN":
        mtg_amt = trade_amt * 2
        net = (mtg_amt * payout_ratio) - trade_amt
        st.mm_current_balance += net
        st.mm_pnl += net
        st.mm_consecutive_losses = 0
        # 🔁 Recalculate base amount after balance change
        st.mm_base_amount = mm_calculate_base_amount(
            st.mm_current_balance, st.mm_sl)
        return (net, old_balance, st.mm_pnl >= st.mm_tp, False)
    else:  # LOSS
        total_loss = trade_amt + (trade_amt * 2)
        st.mm_current_balance -= total_loss
        st.mm_pnl -= total_loss
        st.mm_consecutive_losses += 1
        # 🔁 Recalculate base amount after balance change
        st.mm_base_amount = mm_calculate_base_amount(
            st.mm_current_balance, st.mm_sl)
        sl_hit = abs(min(0, st.mm_pnl)) >= st.mm_sl
        return (-total_loss, old_balance, False, sl_hit)


# ══════════════ CHART DRAWING (SMZX PRO) – full V4 chart ══════════════
STRATEGY_NAMES = {
    1: "RSI basic",
    2: "EMA filtered",
    3: "WR divergence",
    4: "ADX stochastic",
    5: "ultra accurate",
    6: "IROF pro"}
_SS = 2
def _sf(v): return int(v * _SS)


def _get_chart_font(size, bold=False, medium=False):

    pass

    sz = _sf(size)
    if bold:
        paths = [
            "/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Bold.ttf",
            "/data/data/com.termux/files/usr/share/fonts/TTF/JetBrainsMono-Bold.ttf",
            os.path.expanduser("~/.local/share/fonts/JetBrainsMono-Bold.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
    elif medium:
        paths = [
            "/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Medium.ttf",
            "/data/data/com.termux/files/usr/share/fonts/TTF/JetBrainsMono-Medium.ttf",
            os.path.expanduser("~/.local/share/fonts/JetBrainsMono-Medium.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    else:
        paths = [
            "/usr/share/fonts/truetype/jetbrains-mono/JetBrainsMono-Regular.ttf",
            "/data/data/com.termux/files/usr/share/fonts/TTF/JetBrainsMono-Regular.ttf",
            os.path.expanduser("~/.local/share/fonts/JetBrainsMono-Regular.ttf"),
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    for p in paths:
        if os.path.exists(p):
            return ImageFont.truetype(p, sz)
    return ImageFont.load_default()


def _fmt_pair(pair): return pair.replace("_OTC", " (OTC)").replace("_", " ")


def _draw_v4_chart(
        candles,
        pair,
        direction,
        confidence,
        payout,
        entry_time_str,
        current_price,
        wins,
        losses,
        strategy=1,
        martingale_steps=1,
        signal_history=None,
        result_mode=False,
        result_type=None,
        entry_idx=None,
        second_idx=None):
    W_OUT, H_OUT = 1560, 780
    W, H = _sf(W_OUT), _sf(H_OUT)
    HEADER_H = _sf(54)
    SIDEBAR_W = _sf(310)
    CHART_LEFT = _sf(80)
    CHART_RIGHT = W - SIDEBAR_W - _sf(20)
    CHART_TOP = HEADER_H + _sf(24)
    CHART_BOTTOM = H - _sf(225)
    EMA_LEGEND_Y = CHART_BOTTOM + _sf(15)
    VOLUME_TOP = CHART_BOTTOM + _sf(48)
    VOLUME_BOTTOM = H - _sf(42)
    TIME_Y = H - _sf(28)
    BG_HEADER = (3, 6, 15)
    BG_CHART = (7, 11, 23)
    SIDEBAR_BG = (7, 11, 23)
    CANDLE_GREEN = (0, 213, 127)
    CANDLE_RED = (234, 59, 88)
    WICK_GREEN = (0, 68, 41)
    WICK_RED = (125, 47, 69)
    EMA9_CLR = (255, 185, 50)
    EMA21_CLR = (0, 215, 255)
    EMA56_CLR = (155, 115, 215)
    EMA9_LBL = (0, 215, 255)
    EMA21_LBL = (105, 155, 255)
    EMA56_LBL = (155, 115, 215)
    GRID = (20, 25, 38)
    HEADER_LINE = (26, 31, 51)
    TXT_GRAY = (110, 118, 135)
    TXT_WHITE = (240, 245, 255)
    GREEN = (0, 213, 127)
    RED = (234, 59, 88)
    CYAN = (0, 215, 255)
    GOLD = (232, 183, 52)
    SECTION_HDR = (85, 95, 115)
    SB_BORDER = (26, 31, 51)
    BRAND_YELLOW = (235, 210, 86)
    BRAND_CYAN = (39, 189, 226)
    BAR_BG = (22, 28, 42)
    f_header = _get_chart_font(15, medium=True)
    f_price = _get_chart_font(11)
    f_small = _get_chart_font(10)
    f_sidebar_ttl = _get_chart_font(10)
    f_sidebar_lbl = _get_chart_font(12)
    f_sidebar_val = _get_chart_font(12, bold=True)
    f_ema = _get_chart_font(10, bold=True)
    f_vol = _get_chart_font(10)
    f_time = _get_chart_font(10)
    f_brand = _get_chart_font(18, bold=True)
    f_brand_sm = _get_chart_font(10)
    f_badge = _get_chart_font(13, bold=True)
    f_hl = _get_chart_font(9)
    f_conf = _get_chart_font(14, bold=True)
    f_marker = _get_chart_font(10, bold=True)
    img = Image.new('RGB', (W, H), BG_CHART)
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, W, HEADER_H], fill=BG_HEADER)
    draw.line([(0, HEADER_H), (W - SIDEBAR_W, HEADER_H)],
              fill=HEADER_LINE, width=_SS)
    n_disp = min(50, len(candles))
    display = candles[-n_disp:]
    n = len(display)
    closes = [float(c['close']) for c in display]
    opens = [float(c['open']) for c in display]
    highs = [float(c['high']) for c in display]
    lows = [float(c['low']) for c in display]
    vols = [float(c.get('volume', 1)) for c in display]
    p_min = min(lows)
    p_max = max(highs)
    p_rng = p_max - p_min or 0.0001
    pad = p_rng * 0.08
    p_min -= pad
    p_max += pad
    p_rng = p_max - p_min
    all_cl = [float(c['close']) for c in candles]
    si = len(candles) - n
    ema9 = cf_calc_ema(all_cl, 9)[si:]
    ema21 = cf_calc_ema(all_cl, 21)[si:]
    ema56 = cf_calc_ema(all_cl, 56)[si:]
    sample = f"{p_max:.10f}".rstrip('0')
    dp = max(2, min(len(sample.split('.')[1]) if '.' in sample else 2, 5))
    chart_w = CHART_RIGHT - CHART_LEFT
    chart_h = CHART_BOTTOM - CHART_TOP
    def p2y(p): return int(CHART_TOP + chart_h -
                           ((p - p_min) / p_rng) * chart_h)
    ctw = chart_w / n
    cbw = max(_sf(6), int(ctw * 0.58))
    cgap = (ctw - cbw) / 2
    def cx(i): return int(CHART_LEFT + i * ctw + cgap)
    def ccx(i): return int(CHART_LEFT + i * ctw + ctw / 2)
    if current_price is None:
        current_price = closes[-1]
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    date_s = now_pk.strftime("%Y.%m.%d")
    arrow = "\u25b2" if direction == "CALL" else "\u25bc"
    hdr_pair = _fmt_pair(pair)
    if result_mode:
        res_disp = "WIN" if result_type and "WIN" in result_type else "LOSS"
        hdr_txt = f"SMZX PRO    {hdr_pair}    RESULT: {res_disp}    PAYOUT: {payout}%    {date_s}    {entry_time_str}:00"
    else:
        hdr_txt = f"SMZX PRO    {hdr_pair}    {arrow} {direction} {
            confidence:.1f}%    {date_s}    {entry_time_str}:00"
    hw = draw.textlength(hdr_txt, font=f_header)
    hdr_x = (W - SIDEBAR_W - hw) / 2
    hdr_y = (HEADER_H - _sf(15)) / 2
    draw.text((hdr_x, hdr_y), hdr_txt, fill=TXT_WHITE, font=f_header)
    mag = 10**(-dp)
    raw_step = p_rng / 7
    p_step = max(mag, round(raw_step / mag) * mag)
    gp = math.floor(p_min / p_step) * p_step
    while gp <= p_max + p_step:
        if p_min <= gp <= p_max:
            y = p2y(gp)
            if CHART_TOP + _sf(5) < y < CHART_BOTTOM - _sf(5):
                x = CHART_LEFT
                dash = _sf(3)
                gap = _sf(5)
                while x < CHART_RIGHT:
                    x2 = min(x + dash, CHART_RIGHT)
                    draw.line([(x, y), (x2, y)], fill=GRID, width=1)
                    x += dash + gap
                draw.text((_sf(8), y - _sf(7)),
                          f"{gp:.{dp}f}", fill=TXT_GRAY, font=f_price)
        gp += p_step
    draw.text((CHART_LEFT + _sf(5), CHART_TOP + _sf(2)),
              f"H: {max(highs):.{min(2, dp)}f}", fill=TXT_GRAY, font=f_hl)
    draw.text((CHART_LEFT + _sf(5), CHART_BOTTOM - _sf(14)),
              f"L: {min(lows):.{min(2, dp)}f}", fill=TXT_GRAY, font=f_hl)
    for i in range(n):
        x = cx(i)
        cxx = ccx(i)
        o = opens[i]
        h = highs[i]
        l = lows[i]
        c = closes[i]
        green = c >= o
        bcol = CANDLE_GREEN if green else CANDLE_RED
        wcol = WICK_GREEN if green else WICK_RED
        bt = p2y(max(o, c))
        bb = p2y(min(o, c))
        if bb - bt < _SS:
            bb = bt + _SS
        draw.line([(cxx, p2y(h)), (cxx, p2y(l))], fill=wcol, width=max(1, _SS))
        draw.rectangle([x, bt, x + cbw, bb], fill=bcol)

    def draw_ema(vals, color, w=_sf(2)):
        pts = [(ccx(i), p2y(vals[i])) for i in range(n) if vals[i]
               is not None and p_min <= vals[i] <= p_max]
        for j in range(len(pts) - 1):
            draw.line([pts[j], pts[j + 1]], fill=color, width=w)
    draw_ema(ema56, EMA56_CLR)
    draw_ema(ema21, EMA21_CLR)
    draw_ema(ema9, EMA9_CLR)
    if signal_history:
        for sh in signal_history:
            sh_pair = sh.get('pair', '')
            sh_time = sh.get('time', '')
            if sh_pair and sh_pair != pair:
                continue
            if not sh_time:
                continue
            for i, cd in enumerate(display):
                if 'time' in cd:
                    try:
                        ct = (
                            datetime.fromtimestamp(
                                cd['time'],
                                tz=timezone.utc) +
                            timedelta(
                                hours=5)).strftime("%H:%M")
                    except BaseException:
                        ct = ""
                    if ct == sh_time:
                        if sh.get('result') == 'WIN':
                            tw_w = draw.textlength("W", font=f_marker)
                            draw.text(
                                (ccx(i) - tw_w / 2, p2y(highs[i]) - _sf(18)), "W", fill=GREEN, font=f_marker)
                        elif sh.get('result') == 'LOSS':
                            tw_l = draw.textlength("L", font=f_marker)
                            draw.text(
                                (ccx(i) - tw_l / 2, p2y(highs[i]) - _sf(18)), "L", fill=RED, font=f_marker)
                        sd = sh.get('direction', '')
                        ax = ccx(i)
                        if sd == 'CALL':
                            ay = p2y(lows[i]) + _sf(6)
                            draw.polygon([(ax, ay), (ax -
                                                     _sf(3), ay +
                                                     _sf(5)), (ax +
                                          _sf(3), ay +
                                _sf(5))], fill=(100, 110, 125))
                        elif sd == 'PUT':
                            ay = p2y(highs[i]) - _sf(6)
                            draw.polygon([(ax, ay), (ax -
                                                     _sf(3), ay -
                                                     _sf(5)), (ax +
                                          _sf(3), ay -
                                _sf(5))], fill=(100, 110, 125))
                        break
    if result_mode and entry_idx is not None:
        def _draw_result_box(idx, box_rgba, label, label_color):
            if idx < 0 or idx >= n:
                return
            cxx_m = ccx(idx)
            x_l = cx(idx) - _sf(4)
            x_r = cx(idx) + cbw + _sf(4)
            y_t = p2y(highs[idx]) - _sf(8)
            y_b = p2y(lows[idx]) + _sf(8)
            if y_b - y_t < _sf(20):
                y_b = y_t + _sf(20)
            overlay = Image.new('RGBA', (W, H), (0, 0, 0, 0))
            ov_draw = ImageDraw.Draw(overlay)
            ov_draw.rounded_rectangle([x_l, y_t, x_r, y_b], radius=_sf(3), fill=box_rgba, outline=(
                box_rgba[0], box_rgba[1], box_rgba[2], min(255, box_rgba[3] * 3)), width=_SS)
            nonlocal img, draw
            base_rgba = img.convert('RGBA')
            img = Image.alpha_composite(base_rgba, overlay).convert('RGB')
            draw = ImageDraw.Draw(img)
            x_c = cx(idx)
            o = opens[idx]
            h = highs[idx]
            l = lows[idx]
            c = closes[idx]
            is_green = c >= o
            bcol = CANDLE_GREEN if is_green else CANDLE_RED
            wcol = WICK_GREEN if is_green else WICK_RED
            bt_c = p2y(max(o, c))
            bb_c = p2y(min(o, c))
            if bb_c - bt_c < _SS:
                bb_c = bt_c + _SS
            draw.line([(cxx_m, p2y(h)), (cxx_m, p2y(l))],
                      fill=wcol, width=max(1, _SS))
            draw.rectangle([x_c, bt_c, x_c + cbw, bb_c], fill=bcol)
            tw_l = draw.textlength(label, font=f_marker)
            draw.text((cxx_m - tw_l / 2, y_t - _sf(18)),
                      label, fill=label_color, font=f_marker)
        if result_type == "WIN":
            _draw_result_box(entry_idx, (0, 213, 127, 45), "W", GREEN)
        elif result_type == "LOSS":
            _draw_result_box(entry_idx, (234, 59, 88, 45), "L", RED)
        elif result_type == "MTG WIN":
            _draw_result_box(entry_idx, (234, 59, 88, 45), "L", RED)
            if second_idx is not None:
                _draw_result_box(second_idx, (0, 213, 127, 45), "W", GREEN)
                x1 = ccx(entry_idx)
                x2 = ccx(second_idx)
                ya = min(p2y(highs[entry_idx]), p2y(
                    highs[second_idx])) - _sf(28)
                draw.line([(x1, ya), (x2, ya)], fill=GOLD, width=_sf(2))
                draw.polygon([(x2, ya), (x2 - _sf(5), ya - _sf(4)),
                             (x2 - _sf(5), ya + _sf(4))], fill=GOLD)
                mtw = draw.textlength("MTG", font=f_small)
                draw.text(((x1 + x2) / 2 - mtw / 2, ya - _sf(14)),
                          "MTG", fill=GOLD, font=f_small)
    if not result_mode and direction:
        last_cxv = ccx(n - 1)
        glow_half = _sf(24)
        glow_img = Image.new('RGBA', (W, H), (0, 0, 0, 0))
        gd = ImageDraw.Draw(glow_img)
        glow_color = (0, 213, 127) if direction == "CALL" else (234, 59, 88)
        for dx in range(-glow_half, glow_half + 1):
            alpha = int(55 * (1 - abs(dx) / glow_half)**2)
            gd.line([(last_cxv + dx, CHART_TOP), (last_cxv + dx, CHART_BOTTOM)],
                    fill=(glow_color[0], glow_color[1], glow_color[2], alpha), width=1)
        base_rgba = img.convert('RGBA')
        img = Image.alpha_composite(base_rgba, glow_img).convert('RGB')
        draw = ImageDraw.Draw(img)
        i = n - 1
        x = cx(i)
        cxx = ccx(i)
        o = opens[i]
        h = highs[i]
        l = lows[i]
        c = closes[i]
        green = c >= o
        bcol = CANDLE_GREEN if green else CANDLE_RED
        wcol = WICK_GREEN if green else WICK_RED
        bt = p2y(max(o, c))
        bb = p2y(min(o, c))
        if bb - bt < _SS:
            bb = bt + _SS
        draw.line([(cxx, p2y(h)), (cxx, p2y(l))], fill=wcol, width=max(1, _SS))
        draw.rectangle([x, bt, x + cbw, bb], fill=bcol)
        btxt = direction
        btw = draw.textlength(btxt, font=f_badge) + _sf(20)
        bh = _sf(26)
        if direction == "CALL":
            by = p2y(lows[n - 1]) + _sf(14)
            bcl = (0, 185, 100)
            draw.polygon([(last_cxv, by -
                           _sf(12)), (last_cxv -
                                      _sf(5), by -
                                      _sf(3)), (last_cxv +
                          _sf(5), by -
                          _sf(3))], fill=TXT_WHITE)
        else:
            by = p2y(highs[n - 1]) - bh - _sf(14)
            bcl = (220, 50, 70)
            draw.polygon([(last_cxv, by +
                           bh +
                           _sf(12)), (last_cxv -
                                      _sf(5), by +
                                      bh +
                                      _sf(3)), (last_cxv +
                          _sf(5), by +
                          bh +
                          _sf(3))], fill=TXT_WHITE)
        bx = int(last_cxv - btw / 2)
        draw.rounded_rectangle(
            [bx, by, bx + int(btw), by + bh], radius=_sf(4), fill=bcl)
        tw_i = draw.textlength(btxt, font=f_badge)
        draw.text((bx + (int(btw) - tw_i) / 2, by + _sf(4)),
                  btxt, fill=TXT_WHITE, font=f_badge)
    cp_y = p2y(current_price)
    x = CHART_LEFT
    while x < CHART_RIGHT:
        x2 = min(x + _sf(4), CHART_RIGHT)
        draw.line([(x, cp_y), (x2, cp_y)], fill=(50, 58, 72), width=1)
        x += _sf(4) + _sf(4)
    cp_txt = f"{current_price:.{dp}f}"
    cp_tw = draw.textlength(cp_txt, font=f_price) + _sf(10)
    tag_x = CHART_RIGHT - int(cp_tw) - _sf(2)
    draw.rounded_rectangle([tag_x,
                            cp_y - _sf(9),
                            tag_x + int(cp_tw),
                            cp_y + _sf(9)],
                           radius=_sf(3),
                           fill=(8,
                                 16,
                                 30),
                           outline=CYAN,
                           width=_SS)
    draw.text((tag_x + _sf(5), cp_y - _sf(6)), cp_txt, fill=CYAN, font=f_price)
    draw.text((CHART_LEFT, EMA_LEGEND_Y), "EMA 9", fill=EMA9_LBL, font=f_ema)
    draw.text((CHART_LEFT + _sf(80), EMA_LEGEND_Y),
              "EMA 21", fill=EMA21_LBL, font=f_ema)
    draw.text((CHART_LEFT + _sf(170), EMA_LEGEND_Y),
              "EMA 56", fill=EMA56_LBL, font=f_ema)
    draw.line([(CHART_LEFT, VOLUME_TOP - _sf(6)), (CHART_RIGHT,
              VOLUME_TOP - _sf(6))], fill=HEADER_LINE, width=1)
    draw.text((_sf(18), VOLUME_TOP - _sf(2)), "VOL", fill=TXT_GRAY, font=f_vol)
    vol_h = VOLUME_BOTTOM - VOLUME_TOP
    mx_vol = max(vols) if vols else 1
    for i in range(n):
        x = cx(i)
        v = vols[i]
        bh = max(_sf(2), int((v / mx_vol) * vol_h * 0.70))
        green = closes[i] >= opens[i]
        out_c = CANDLE_GREEN if green else CANDLE_RED
        fill_c = (0, 75, 45) if green else (90, 22, 35)
        bt = VOLUME_BOTTOM - bh
        draw.rectangle([x, bt, x + cbw, VOLUME_BOTTOM], outline=out_c, width=1)
        if bh > _sf(2):
            draw.rectangle([x + 1, bt + 1, x + cbw - 1,
                           VOLUME_BOTTOM - 1], fill=fill_c)
    step = max(1, n // 9)
    for i in range(0, n, step):
        ts = ""
        if 'time' in display[i]:
            try:
                ts = (
                    datetime.fromtimestamp(
                        display[i]['time'],
                        tz=timezone.utc) +
                    timedelta(
                        hours=5)).strftime("%H:%M")
            except BaseException:
                pass
        if ts:
            tw_t = draw.textlength(ts, font=f_time)
            draw.text((ccx(i) - tw_t / 2, TIME_Y),
                      ts, fill=TXT_GRAY, font=f_time)
    vol_chg = abs((vols[-1] - vols[-2]) / vols[-2] *
                  100) if len(vols) >= 2 and vols[-2] > 0 else 0
    draw.text((CHART_LEFT + _sf(5), CHART_TOP - _sf(14)),
              f"VOL {vol_chg:.2f}%", fill=GREEN, font=f_small)
    cb_txt = f"{confidence:.0f}%"
    cb_x = CHART_RIGHT - _sf(70)
    cb_y = CHART_TOP - _sf(14)
    badge_w = _sf(60)
    badge_h = _sf(26)
    draw.rounded_rectangle(
        [cb_x, cb_y, cb_x + badge_w, cb_y + badge_h], radius=_sf(4), fill=GOLD)
    tri_x = cb_x + _sf(12)
    tri_y = cb_y + _sf(6)
    draw.polygon([(tri_x, tri_y), (tri_x - _sf(5), tri_y + _sf(11)),
                 (tri_x + _sf(5), tri_y + _sf(11))], fill=TXT_WHITE)
    draw.text((cb_x + _sf(22), cb_y + _sf(4)),
              cb_txt, fill=TXT_WHITE, font=f_conf)
    sb_x = W - SIDEBAR_W
    draw.rectangle([sb_x, 0, W, H], fill=SIDEBAR_BG)
    draw.line([(sb_x, 0), (sb_x, H)], fill=SB_BORDER, width=_SS)
    sb_cx = sb_x + SIDEBAR_W // 2
    lbl_x = sb_x + _sf(20)
    val_x = W - _sf(18)
    rh = _sf(28)
    dir_color = GREEN if direction == "CALL" else RED

    def sb_row(y, label, value, vcol=TXT_WHITE):
        draw.text((lbl_x, y), label, fill=TXT_GRAY, font=f_sidebar_lbl)
        vw = draw.textlength(str(value), font=f_sidebar_val)
        draw.text((val_x - vw, y), str(value), fill=vcol, font=f_sidebar_val)
    sy = _sf(58)
    if result_mode:
        shdr = "\u2014 RESULT \u2014"
    else:
        shdr = "\u2014 SIGNAL \u2014"
    shw = draw.textlength(shdr, font=f_sidebar_ttl)
    draw.text((sb_cx - shw / 2, sy), shdr,
              fill=SECTION_HDR, font=f_sidebar_ttl)
    draw.line([(lbl_x, sy + _sf(16)), (val_x, sy + _sf(16))],
              fill=SB_BORDER, width=1)
    ry = sy + _sf(28)
    if result_mode:
        res_disp = "WIN" if result_type and "WIN" in result_type else "LOSS"
        res_col = GREEN if "WIN" in (result_type or "") else RED
        sb_row(ry, "Result", res_disp, res_col)
        sb_row(ry + rh, "Direction", direction or "", dir_color)
        sb_row(ry + rh * 2, "Payout", f"{payout}%", TXT_WHITE)
        sb_row(ry + rh * 3, "Time", entry_time_str, TXT_WHITE)
    else:
        sb_row(ry, "Direction", direction, dir_color)
        sb_row(ry + rh, "Confidence", f"{confidence:.1f}%", TXT_WHITE)
        sb_row(ry + rh * 2, "Price",
               f"{current_price:.{min(2, dp)}f}", TXT_WHITE)
        sb_row(ry + rh * 3, "Time", entry_time_str, TXT_WHITE)
    total = wins + losses
    wr = (wins / total * 100) if total > 0 else 0
    py = ry + rh * 4 + _sf(8)
    phdr = "\u2014 PERFORMANCE \u2014"
    phw = draw.textlength(phdr, font=f_sidebar_ttl)
    draw.text((sb_cx - phw / 2, py), phdr,
              fill=SECTION_HDR, font=f_sidebar_ttl)
    draw.line([(lbl_x, py + _sf(16)), (val_x, py + _sf(16))],
              fill=SB_BORDER, width=1)
    pry = py + _sf(24)
    sb_row(pry, "Win Rate", f"{wr:.1f}%", GREEN)
    bar_x = lbl_x
    bar_y = pry + rh
    bar_w = SIDEBAR_W - _sf(40)
    bar_h = _sf(12)
    draw.rounded_rectangle(
        [bar_x, bar_y, bar_x + bar_w, bar_y + bar_h], radius=_sf(4), fill=BAR_BG)
    filled = int(bar_w * wr / 100)
    if filled > 0:
        draw.rounded_rectangle(
            [bar_x, bar_y, bar_x + filled, bar_y + bar_h], radius=_sf(4), fill=GREEN)
    sb_row(pry + rh + _sf(18), "Wins", str(wins), GREEN)
    sb_row(pry + rh * 2 + _sf(18), "Losses", str(losses), RED)
    sb_row(pry + rh * 3 + _sf(18), "Streak", f"{wins}W/{losses}L", TXT_WHITE)
    ssy = pry + rh * 4 + _sf(24)
    ss_hdr = "\u2014 SESSION \u2014"
    ssw = draw.textlength(ss_hdr, font=f_sidebar_ttl)
    draw.text((sb_cx - ssw / 2, ssy), ss_hdr,
              fill=SECTION_HDR, font=f_sidebar_ttl)
    draw.line([(lbl_x, ssy + _sf(16)), (val_x, ssy + _sf(16))],
              fill=SB_BORDER, width=1)
    sry = ssy + _sf(24)
    disp_pair = _fmt_pair(pair)
    sb_row(sry, "Signals", str(max(1, total + 1)), TXT_WHITE)
    sb_row(sry + rh, "Pair", disp_pair, CYAN)
    sb_row(sry + rh * 2, "Mode", STRATEGY_NAMES.get(strategy, "auto"), GREEN)
    sb_row(
        sry + rh * 3,
        "Martingale",
        f"{martingale_steps} Step(s)",
        TXT_WHITE)
    br_w = _sf(240)
    br_h = _sf(60)
    br_x = W - br_w - _sf(22)
    br_y = H - br_h - _sf(14)
    draw.rounded_rectangle([br_x,
                            br_y,
                            br_x + br_w,
                            br_y + br_h],
                           radius=_sf(5),
                           fill=BG_CHART,
                           outline=BRAND_YELLOW,
                           width=_SS)
    bt_txt = "SMZX PRO"
    btw2 = draw.textlength(bt_txt, font=f_brand)
    draw.text((br_x + (br_w - btw2) / 2, br_y + _sf(8)),
              bt_txt, fill=BRAND_YELLOW, font=f_brand)
    cr_txt = "\u2666 @Rohailtrader \u2666"
    ctw2 = draw.textlength(cr_txt, font=f_brand_sm)
    draw.text((br_x + (br_w - ctw2) / 2, br_y + _sf(36)),
              cr_txt, fill=BRAND_CYAN, font=f_brand_sm)
    img = img.resize((W_OUT, H_OUT), Image.LANCZOS)
    path = f"smzx_chart_{uuid.uuid4().hex[:8]}.png"
    img.save(path, quality=100, subsampling=0)
    return path


def draw_neon_chart(
        candles,
        pair,
        trade_time,
        direction,
        payout,
        confidence=80,
        wins=0,
        losses=0,
        strategy=1,
        martingale_steps=1,
        signal_history=None):
    return _draw_v4_chart(candles,
                          pair,
                          direction,
                          confidence,
                          payout,
                          trade_time,
                          candles[-1]['close'] if candles else 0,
                          wins,
                          losses,
                          strategy=strategy,
                          martingale_steps=martingale_steps,
                          signal_history=signal_history)


def draw_result_chart(
        candles,
        pair,
        payout,
        result_type,
        entry_candle,
        second_candle=None,
        wins=0,
        losses=0,
        strategy=1,
        confidence=80,
        direction=None,
        entry_time_str="",
        signal_history=None):
    n_disp = min(50, len(candles))
    display = candles[-n_disp:]
    entry_idx = None
    second_idx = None
    for i, c in enumerate(display):
        if 'time' in c and entry_candle and 'time' in entry_candle and c[
                'time'] == entry_candle['time']:
            entry_idx = i
        if second_candle and 'time' in c and 'time' in second_candle and c[
                'time'] == second_candle['time']:
            second_idx = i
    if entry_idx is None:
        entry_idx = len(display) - 1
    return _draw_v4_chart(candles,
                          pair,
                          direction or "CALL",
                          confidence,
                          payout,
                          entry_time_str,
                          candles[-1]['close'] if candles else 0,
                          wins,
                          losses,
                          strategy=strategy,
                          martingale_steps=1,
                          signal_history=signal_history,
                          result_mode=True,
                          result_type=result_type,
                          entry_idx=entry_idx,
                          second_idx=second_idx)


# ══════════════ SMZXBot (UPDATED with new API key and advanced MM) ══════
LIVE_PAIRS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "AUDUSD",
    "USDCAD",
    "EURJPY",
    "GBPJPY",
    "EURAUD",
    "GBPCAD",
    "AUDJPY",
    "NZDJPY",
    "EURCHF",
    "GBPCHF"]
DEFAULT_OTC_PAIRS = [
    "USDBDT_OTC",
    "USDARS_OTC",
    "USDINR_OTC",
    "USDMXN_OTC",
    "USDNGN_OTC",
    "USDEGP_OTC",
    "USDPKR_OTC",
    "USDIDR_OTC",
    "BRLUSD_OTC",
    "NZDUSD_OTC",
    "EURNZD_OTC",
    "FB_OTC",
    "NZDCAD_OTC",
    "CADCHF_OTC",
    "NZDCHF_OTC",
    "AUDNZD_OTC",
    "BTCUSD_OTC",
    "MSFT_OTC",
    "XAUUSD_OTC",
    "JNJ_OTC",
    "MCD_OTC",
    "USDCHF_OTC",
    "EURCHF_OTC",
    "EURCAD_OTC",
    "USDDZD_OTC"]


class SMZXBot:
    def __init__(self, uid):
        self.uid = uid
        st = get_state(uid)
        self.market_type = st.market_type
        self.pairs = st.pairs if st.pairs else DEFAULT_OTC_PAIRS.copy()
        self.base_url = "https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={}&interval=1m&limit=600"
        self.telegram_format = st.telegram_format
        self.strategy = st.strategy
        self.strategy2_filters = st.strategy2_filters if st.strategy2_filters else Strategy2Filters()
        self.strategy3_min_accuracy = st.strategy3_min_accuracy
        self.strategy3_lookback = st.strategy3_lookback
        self.strategy4_min_accuracy = st.strategy4_min_accuracy
        self.strategy5_min_score = st.strategy5_min_score
        self.strategy6_min_score = st.strategy6_min_score
        self.strategy6_min_candles = st.strategy6_min_candles
        self.stats = st.stats
        self.signal_history = st.signal_history
        self.last_signal_pair = None
        self.same_pair_count = 0
        self.last_loss = st.last_loss

    def format_pair_for_api(self, pair):
        return pair.upper() if self.market_type == "LIVE" else pair.replace("_", "-") + "q"

    def fetch_data(self, pair, limit=600):
        # UPDATED API KEY – only one valid key used everywhere
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={
            self.format_pair_for_api(pair)}&interval=1m&limit={limit}:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
        headers = {"apikey": SUPABASE_ANON_KEY,
                   "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        try:
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                payout = str(data.get("payout", "92")).replace("%", "")
                if 'candles' in data and data['candles'] and len(
                        data['candles']) > 0:
                    for c in data['candles']:
                        if 'volume' not in c:
                            c['volume'] = 1
                    return data['candles'], data['candles'][-1]['close'], payout
        except Exception as e:
            print(f"Cloud API Lag: {e}")
        return None, None, "0"

    def analyze(self, candles):
        if self.strategy == 1:
            return analyze_strategy1(candles, 75)
        elif self.strategy == 2:
            return analyze_strategy2(candles, self.strategy2_filters)
        elif self.strategy == 3:
            return analyze_strategy3(
                candles,
                self.strategy3_min_accuracy,
                self.strategy3_lookback)
        elif self.strategy == 4:
            return analyze_strategy4(candles, self.strategy4_min_accuracy)
        elif self.strategy == 5:
            return analyze_strategy5(candles, self.strategy5_min_score)
        elif self.strategy == 6:
            return analyze_strategy6(
                candles,
                self.strategy6_min_score,
                self.strategy6_min_candles)
        else:
            return analyze_strategy3(candles, 75, 20)

    def get_trend_text(self, candles, direction):
        if len(candles) >= 10:
            closes = [c['close'] for c in candles]
            ema = calculate_ema(closes, 10)
            if ema:
                return "Bullish" if closes[-1] > ema else "Bearish"
        return "Bullish" if direction == "CALL" else "Bearish"

    def send_signal_with_chart(
            self,
            pair,
            price,
            bias,
            entry_t,
            candles,
            payout,
            confidence=80):
        direction = "CALL" if bias == "CALL" else "PUT"
        trend_text = self.get_trend_text(candles, direction)
        signal_text = build_signal_message(
            pair, entry_t, direction, payout, trend_text)
        st = get_state(self.uid)
        chart_path = draw_neon_chart(
            candles,
            pair,
            entry_t,
            direction,
            payout,
            confidence=confidence,
            wins=st.stats['wins'],
            losses=st.stats['losses'],
            strategy=self.strategy,
            martingale_steps=1,
            signal_history=st.signal_history)
        if chart_path and os.path.exists(chart_path):
            sender.send_file(self.uid, chart_path, signal_text)
            try:
                os.remove(chart_path)
            except BaseException:
                pass
        else:
            sender.send_message(self.uid, signal_text)

    def send_result_with_chart(
            self,
            pair,
            entry_time,
            entry_candle,
            second_candle,
            payout,
            result_type,
            candles,
            direction=None):
        if result_type == "WIN":
            msg = build_result_message_first_win(
                pair, entry_time, payout, self.stats['wins'], self.stats['losses'])
        elif result_type == "MTG WIN":
            msg = build_result_message_second_win(
                pair, entry_time, payout, self.stats['wins'], self.stats['losses'])
        else:
            msg = build_result_message_loss(
                pair,
                entry_time,
                payout,
                self.stats['wins'],
                self.stats['losses'])
        st = get_state(self.uid)
        chart_path = draw_result_chart(
            candles,
            pair,
            payout,
            result_type,
            entry_candle,
            second_candle,
            wins=st.stats['wins'],
            losses=st.stats['losses'],
            strategy=self.strategy,
            direction=direction,
            entry_time_str=entry_time,
            signal_history=st.signal_history)
        if chart_path and os.path.exists(chart_path):
            sender.send_file(self.uid, chart_path, msg)
            try:
                os.remove(chart_path)
            except BaseException:
                pass
        else:
            sender.send_message(self.uid, msg)

    def sleep_until(self, target_utc5):
        while not get_state(self.uid).stop_requested:
            if (datetime.now(timezone.utc) + timedelta(hours=5)) >= target_utc5:
                break
            time.sleep(0.2)

    def get_candle_at_time(self, candles, target_dt_utc5):
        target = int((target_dt_utc5 - timedelta(hours=5)).timestamp())
        for c in candles:
            if 'time' in c and abs(c['time'] - target) < 30:
                return c
        return None

    def run_single_signal(self):
        uid = self.uid
        st = get_state(uid)
        st.running = True
        st.stop_requested = False
        progress_msg = sender.send_message(
            uid, "⏳ Scanning for a signal... 0%")
        if not progress_msg:
            return
        progress_id = progress_msg.id
        signal_found = False
        try:
            for idx, pair in enumerate(self.pairs):
                if st.stop_requested:
                    break
                pct = int((idx + 1) / len(self.pairs) * 100)
                bar_text = f"⏳ Scanning {pair}... {progress_bar_text(pct)}"
                sender.edit_message(uid, progress_id, bar_text)
                candles, price, payout = self.fetch_data(pair, limit=200)
                if not candles:
                    continue
                try:
                    payout_num = int(payout) if payout != "!" else 77
                except BaseException:
                    payout_num = 0
                if self.market_type == "OTC" and payout_num < 77:
                    continue
                now = datetime.now(timezone.utc) + timedelta(hours=5)
                if pair in st.last_loss:
                    if (now - st.last_loss[pair]
                        ).total_seconds() < st.loss_cooldown_minutes * 60:
                        continue
                try:
                    bias, entry_dt, score = self.analyze(candles)
                except Exception as e:
                    print(f"Analysis error for {pair}: {e}")
                    continue
                if bias:
                    if pair == self.last_signal_pair:
                        self.same_pair_count += 1
                    else:
                        self.last_signal_pair = pair
                        self.same_pair_count = 1
                    if self.same_pair_count > 2:
                        continue
                    entry_t = entry_dt.strftime("%H:%M")
                    sender.edit_message(
                        uid, progress_id, "✅ Signal found! Sending...")
                    self.send_signal_with_chart(
                        pair, price, bias, entry_t, candles, payout, confidence=score)
                    if st.mm_enabled:
                        sender.send_message(
                            uid, mm_build_signal_msg(
                                st, pair, bias))
                    sender.edit_message(
                        uid, progress_id, "⏳ Monitoring result...")
                    self.handle_signal_result(
                        pair, entry_dt, bias, payout, candles)
                    signal_found = True
                    break
            if signal_found:
                sender.edit_message(uid, progress_id, "✅ Scanning complete.")
                sender.send_message(
                    uid,
                    "✅ Signal completed.\nUse /continue for next signal, or /stop to return to main menu.")
            else:
                sender.edit_message(uid, progress_id, "❌ No signal found.")
        except Exception as e:
            sender.send_message(uid, f"❌ Error: {e}")
        finally:
            st.running = False

    def handle_signal_result(
            self,
            pair,
            entry_dt_utc5,
            direction,
            payout,
            initial_candles):
        st = get_state(self.uid)
        try:
            payout_pct = float(str(payout).replace("%", ""))
        except BaseException:
            payout_pct = 92.0
        close_time_1 = entry_dt_utc5 + timedelta(minutes=1)
        self.sleep_until(close_time_1)
        if st.stop_requested:
            return
        candles, _, _ = self.fetch_data(pair, limit=750)
        if not candles:
            return
        first = self.get_candle_at_time(candles, entry_dt_utc5)
        if not first:
            return
        win1 = (
            first['close'] > first['open']) if direction == "CALL" else (
            first['close'] < first['open'])
        trade_type = "NON-MTG"
        st.signal_history.append({'pair': pair,
                                  'direction': direction,
                                  'time': entry_dt_utc5.strftime('%H:%M'),
                                  'result': "WIN" if win1 else "LOSS",
                                  'type': trade_type})
        if not win1:
            st.last_loss[pair] = datetime.now(
                timezone.utc) + timedelta(hours=5)
        if win1:
            st.stats['wins'] += 1
            self.send_result_with_chart(
                pair,
                entry_dt_utc5.strftime('%H:%M'),
                first,
                None,
                payout,
                "WIN",
                candles,
                direction=direction)
            if st.mm_enabled:
                pl, old_bal, tp_hit, sl_hit = mm_update_after_result(
                    st, "WIN", payout_pct)
                sender.send_message(
                    self.uid, mm_build_result_msg(
                        st, "WIN", pl, old_bal))
                if tp_hit or sl_hit:
                    st.mm_enabled = False
            return
        # Martingale step 1 (only 1 step)
        close_time_2 = entry_dt_utc5 + timedelta(minutes=2)
        self.sleep_until(close_time_2)
        if st.stop_requested:
            return
        candles2, _, _ = self.fetch_data(pair, limit=750)
        if not candles2:
            return
        second = self.get_candle_at_time(
            candles2, entry_dt_utc5 + timedelta(minutes=1))
        if not second:
            return
        win2 = (
            second['close'] > second['open']) if direction == "CALL" else (
            second['close'] < second['open'])
        if win2:
            st.signal_history[-1]['result'] = "WIN"
            st.signal_history[-1]['type'] = "MTG"
            st.stats['wins'] += 1
            self.send_result_with_chart(
                pair,
                entry_dt_utc5.strftime('%H:%M'),
                first,
                second,
                payout,
                "MTG WIN",
                candles2,
                direction=direction)
            if st.mm_enabled:
                pl, old_bal, tp_hit, sl_hit = mm_update_after_result(
                    st, "MTG WIN", payout_pct)
                sender.send_message(
                    self.uid, mm_build_result_msg(
                        st, "MTG WIN", pl, old_bal))
                if tp_hit or sl_hit:
                    st.mm_enabled = False
        else:
            st.stats['losses'] += 1
            self.send_result_with_chart(
                pair,
                entry_dt_utc5.strftime('%H:%M'),
                first,
                None,
                payout,
                "LOSS",
                candles2,
                direction=direction)
            if st.mm_enabled:
                pl, old_bal, tp_hit, sl_hit = mm_update_after_result(
                    st, "LOSS", payout_pct)
                sender.send_message(
                    self.uid, mm_build_result_msg(
                        st, "LOSS", pl, old_bal))
                if tp_hit or sl_hit:
                    st.mm_enabled = False

# ══════════════ LIVE CHECKER (flexible format parser + sio.tools) ═══════


def clean_int_input(text: str) -> str:
    return text.strip().replace(
        '\n',
        '').replace(
        '\r',
        '').replace(
            ' ',
            '').replace(
                '\u200b',
        '')


def parse_signal_line(line: str):

    pass

    line = line.strip()
    if not line:
        return None, None, None

    # Normalize fancy characters FIRST
    line = normalize_fancy(line)

    # Remove leading M1 etc.
    line = re.sub(r'^M\d+\s*', '', line)

    # Find time (HH:MM) – now digits are normal
    time_match = re.search(r'(\d{2}:\d{2})', line)
    if not time_match:
        return None, None, None
    time_str = time_match.group(1)

    # Find direction (CALL or PUT)
    dir_match = re.search(r'\b(CALL|PUT)\b', line, re.IGNORECASE)
    if not dir_match:
        return None, None, None
    direction = dir_match.group(1).upper()

    # Extract pair: remove time and direction and clean
    rest = line
    rest = rest.replace(time_str, '')
    rest = rest.replace(direction, '')
    rest = rest.replace('CALL', '').replace('PUT', '')
    # Remove common separators
    rest = re.sub(r'[;,_\-\.]', ' ', rest)
    rest = re.sub(r'[➪→]', ' ', rest)
    # Remove extra spaces and arrows
    rest = re.sub(r'\s+', ' ', rest).strip()

    # The remaining should be the pair (e.g., "USDINR OTC" or "USDINR_OTC")
    pair = rest.strip()
    if not pair:
        return None, None, None

    # Normalize pair: remove spaces, add _OTC if missing
    pair = pair.replace(' ', '_').upper()
    if 'OTC' not in pair and 'OTC' not in pair.upper():
        pair += "_OTC"
    # Ensure proper format
    pair = pair.replace('-OTC', '_OTC')
    return pair, time_str, direction

async def run_checker_local(update: Update, context: ContextTypes.DEFAULT_TYPE, date_str: str, signals_text: str):
    from datetime import datetime, timedelta, timezone
    import re

    uid = update.effective_user.id
    date_str_norm = normalize_fancy(date_str)
    date_str_norm = re.sub(r'[—–−]', '-', date_str_norm)
    try:
        date_utc5 = datetime.strptime(date_str_norm, "%Y-%m-%d")
    except:
        sender.send_message(uid, f"❌ Invalid date: {date_str}\nUse YYYY-MM-DD")
        return

    context.user_data['checker_date'] = date_str_norm

    raw_lines = [l.strip() for l in signals_text.strip().split('\n') if l.strip()]
    if not raw_lines:
        sender.send_message(uid, "❌ No signals.")
        return

    def parse_signal_line(line):
        line = normalize_fancy(line)
        line = re.sub(r'^M\d+\s*', '', line)
        pair_match = re.search(r'([A-Z0-9]+[_-]?[A-Z0-9]*(?:[_-]OTC)?)', line)
        if not pair_match:
            return None, None, None
        pair_raw = pair_match.group(1).upper()
        time_match = re.search(r'(\d{2}:\d{2})', line)
        if not time_match:
            return None, None, None
        time_str = time_match.group(1)
        dir_match = re.search(r'(CALL|PUT|BUY|SELL|UP|DOWN)', line, re.IGNORECASE)
        if not dir_match:
            return None, None, None
        dir_raw = dir_match.group(1).upper()
        if dir_raw in ("BUY", "UP"):
            direction = "CALL"
        elif dir_raw in ("SELL", "DOWN"):
            direction = "PUT"
        else:
            direction = dir_raw
        return pair_raw, time_str, direction

    signals = []
    for line in raw_lines:
        pair_raw, time_str, direction = parse_signal_line(line)
        if not pair_raw:
            continue
        base = pair_raw.replace("_OTC", "").replace("-OTC", "").replace("_", "").replace("-", "")
        if "OTC" in pair_raw:
            api_pair = f"{base}-OTCq"
        else:
            api_pair = f"{base}q"
        signals.append({
            'display': pair_raw,
            'api_pair': api_pair,
            'time': time_str,
            'dir': direction,
        })

    if not signals:
        sender.send_message(uid, "❌ No valid signals found.")
        return

    tz_pk = timezone(timedelta(hours=5))
    now_utc5 = datetime.now(tz_pk)

    # --- Fetch candles once per unique API pair ---
    cache = {}          # api -> dict of time -> candle (for checking, by end time)
    full_lookup = {}    # api -> dict of date -> dict of time -> candle (timestamp-based, for loss charts)
    for sig in signals:
        api = sig['api_pair']
        if api in cache:
            continue
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={api}&interval=1m&limit=2000:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
        headers = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                cache[api] = None
                full_lookup[api] = None
                continue
            data = resp.json()
            candles = data.get('candles', [])
            if not candles:
                cache[api] = None
                full_lookup[api] = None
                continue

            # Build candle_by_end (using readable_time) – for checker logic
            candle_by_end = {}
            # Build timestamp-based lookup (date -> time -> candle) – for loss charts
            ts_lookup = {}
            for c in candles:
                # For checker
                rt = c.get('readable_time', '')
                match = re.search(r', (\d{2}:\d{2}):', rt)
                if match:
                    end_time = match.group(1)
                    candle_by_end[end_time] = c

                # For loss charts (timestamp-based, reliable)
                ts = c.get('time')
                if ts:
                    dt_local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz_pk)
                    date_key = dt_local.strftime("%Y-%m-%d")
                    time_key = dt_local.strftime("%H:%M")
                    if date_key not in ts_lookup:
                        ts_lookup[date_key] = {}
                    ts_lookup[date_key][time_key] = c

            cache[api] = candle_by_end
            full_lookup[api] = ts_lookup
        except Exception as e:
            print(f"Error: {e}")
            cache[api] = None
            full_lookup[api] = None

    # --- Check each signal and collect loss candles ---
    results = []
    pending_count = 0
    win_count = 0
    loss_count = 0
    loss_data_list = []   # will store full loss candle data for later

    for sig in signals:
        display = sig['display']
        api = sig['api_pair']
        signal_time = sig['time']
        direction = sig['dir']

        # Future check
        signal_dt = datetime.strptime(f"{date_str_norm} {signal_time}", "%Y-%m-%d %H:%M")
        signal_dt = signal_dt.replace(tzinfo=tz_pk)
        if signal_dt > now_utc5:
            results.append((f"M1 {display} {signal_time} {direction}", "⏳"))
            pending_count += 1
            continue

        candle_dict = cache.get(api)
        if not candle_dict:
            results.append((f"M1 {display} {signal_time} {direction}", "❌"))
            loss_count += 1
            continue

        # Find entry candle (first time >= signal_time)
        sorted_times = sorted(candle_dict.keys())
        actual_entry_time = None
        for t in sorted_times:
            if t >= signal_time:
                actual_entry_time = t
                break
        if not actual_entry_time:
            results.append((f"M1 {display} {signal_time} {direction}", "❌"))
            loss_count += 1
            continue

        entry = candle_dict[actual_entry_time]
        candle_dir = entry.get('direction', '').lower()
        win1 = (direction == "CALL" and candle_dir == "up") or (direction == "PUT" and candle_dir == "down")
        if win1:
            results.append((f"M1 {display} {signal_time} {direction}", "✅"))
            win_count += 1
            continue

        # MTG: next minute after actual_entry_time
        h, m = map(int, actual_entry_time.split(':'))
        next_dt = datetime(2000, 1, 1, h, m) + timedelta(minutes=1)
        next_hhmm = next_dt.strftime("%H:%M")
        next_candle = candle_dict.get(next_hhmm)
        if not next_candle:
            results.append((f"M1 {display} {signal_time} {direction}", "❌"))
            loss_count += 1
            # This is a loss – collect loss candles from timestamp lookup
            day_lookup = full_lookup.get(api, {}).get(date_str_norm, {})
            if day_lookup:
                loss_data = collect_loss_candles(day_lookup, actual_entry_time, direction, display)
                if loss_data:
                    loss_data_list.append(loss_data)
            continue

        next_dir = next_candle.get('direction', '').lower()
        win2 = (direction == "CALL" and next_dir == "up") or (direction == "PUT" and next_dir == "down")
        if win2:
            results.append((f"M1 {display} {signal_time} {direction}", "✅¹"))
            win_count += 1
        else:
            results.append((f"M1 {display} {signal_time} {direction}", "❌"))
            loss_count += 1
            # Loss – collect loss candles
            day_lookup = full_lookup.get(api, {}).get(date_str_norm, {})
            if day_lookup:
                loss_data = collect_loss_candles(day_lookup, actual_entry_time, direction, display)
                if loss_data:
                    loss_data_list.append(loss_data)

    # Store the full loss data (including candles) for the button
    context.user_data['loss_signals'] = loss_data_list

    # --- Build output message ---
    total_signals = len(results)
    header = (
        f"{fancy_font('▰▱▱ 𝚂𝙼𝚉 𝙱𝙾𝚃 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 ▱▱▰')}\n"
        f"{fancy_font('              ┏━━━━━━━━━━━┓')}\n"
        f"{fancy_font('                 🗓 - ')}{fancy_font(date_str_norm)}{fancy_font('          ')}\n"
        f"{fancy_font('              ┗━━━━━━━━━━━┛')}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
    )
    body = "\n".join([f"{sig} {icon}" for sig, icon in results])
    summary = (
        f"\n{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
        f"{fancy_font('🏆 Total : ')}{fancy_font(str(total_signals))}\n"
        f"{fancy_font('✅ Win: ')}{fancy_font(str(win_count))}\n"
        f"{fancy_font('✖ Loss: ')}{fancy_font(str(loss_count))}\n"
        f"{fancy_font('⏳ Pending: ')}{fancy_font(str(pending_count))}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}"
    )
    final_msg = header + body + summary

    entities = build_custom_emoji_entities(final_msg)

    buttons = []
    if loss_data_list:
        buttons.append([colored_button(" Show Loss Candles", "show_loss_candles", KeyboardButtonStyle.PRIMARY, "5809816842713174497")])
    buttons.append([colored_button(" Home", "back_to_main", KeyboardButtonStyle.SUCCESS, "5416041192905265756")])
    reply_markup = InlineKeyboardMarkup(buttons)

    await context.bot.send_message(
        chat_id=uid,
        text=final_msg,
        entities=entities,
        reply_markup=reply_markup
    )

def collect_loss_candles(day_lookup, entry_time_str, direction, pair_display):
    t_h, t_m = map(int, entry_time_str.split(':'))
    entry_min = t_h * 60 + t_m
    next_available = []
    for i in range(1, 6):
        n_min = entry_min + i
        n_h = n_min // 60
        n_m = n_min % 60
        n_key = f"{n_h:02d}:{n_m:02d}"
        if n_key in day_lookup:
            next_available.append(n_key)
        else:
            break
    next_count = len(next_available)
    prev_needed = max(0, 6 - next_count)
    prev_keys = []
    for i in range(1, prev_needed + 1):
        p_min = entry_min - i
        if p_min < 0:
            break
        p_h = p_min // 60
        p_m = p_min % 60
        p_key = f"{p_h:02d}:{p_m:02d}"
        if p_key in day_lookup:
            prev_keys.insert(0, p_key)
    candles = []
    for k in prev_keys:
        candles.append(day_lookup[k])
    candles.append(day_lookup[entry_time_str])
    for k in next_available:
        candles.append(day_lookup[k])
    return {
        'pair': pair_display,
        'entry_time': entry_time_str,
        'direction': direction,
        'candles': candles,
        'prev_count': len(prev_keys),
        'next_count': next_count
    }

# ========== LOCAL BACKTEST (replaces SIO) ==========


async def run_backtest_local(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        days: int,
        signals_text: str,
        martingale_level: int):
    uid = update.effective_user.id
    from datetime import datetime, timedelta, timezone
    import re

    def parse_signal_line(line):
        line = normalize_fancy(line)
        line = re.sub(r'^M\d+\s*', '', line)
        pair_match = re.search(r'([A-Z0-9]+[_-]?[A-Z0-9]*(?:[_-]OTC)?)', line)
        if not pair_match:
            return None, None, None
        pair_raw = pair_match.group(1).upper()
        # Normalize pair: replace _OTC with -OTC (API expects hyphen)
        pair_raw = pair_raw.replace('_OTC', '-OTC')
        if 'OTC' not in pair_raw:
            pair_raw += '-OTC'   # assume OTC if missing? but user's list already has -OTC
        time_match = re.search(r'(\d{2}:\d{2})', line)
        if not time_match:
            return None, None, None
        time_str = time_match.group(1)
        dir_match = re.search(
            r'(CALL|PUT|BUY|SELL|UP|DOWN)',
            line,
            re.IGNORECASE)
        if not dir_match:
            return None, None, None
        dir_raw = dir_match.group(1).upper()
        if dir_raw in ("BUY", "UP"):
            direction = "CALL"
        elif dir_raw in ("SELL", "DOWN"):
            direction = "PUT"
        else:
            direction = dir_raw
        return pair_raw, time_str, direction

    # Parse signals
    raw_lines = [l.strip() for l in signals_text.split('\n') if l.strip()]
    signals = []
    for line in raw_lines:
        pair_raw, time_str, direction = parse_signal_line(line)
        if not pair_raw:
            continue
        # For backtest API, use the pair as is (e.g., USDBDT-OTC)
        api_pair = pair_raw   # no extra 'q'
        signals.append({
            'display': pair_raw,
            'api_pair': api_pair,
            'time': time_str,
            'dir': direction,
        })
    if not signals:
        await update.message.reply_text("❌ No valid signals found.")
        return

    # Determine date range: last 'days' calendar days, excluding current day
    now_utc5 = datetime.now(timezone(timedelta(hours=5)))
    date_list = []
    for i in range(1, days + 1):
        d = now_utc5 - timedelta(days=i)
        date_list.append(d.strftime("%Y-%m-%d"))

    # Fetch candles per pair (use the new get-candles endpoint)
    cache = {}
    for sig in signals:
        api = sig['api_pair']
        if api in cache:
            continue
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/get-candles?pair={api}&timeframe=M1&limit=12000"
        headers = {"apikey": SUPABASE_ANON_KEY,
                   "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code != 200:
                print(f"Backtest: HTTP {resp.status_code} for {api}")
                cache[api] = None
                continue
            data = resp.json()
            candles = data.get('candles', [])
            if not candles:
                print(f"Backtest: No candles for {api}")
                cache[api] = None
                continue
            # Build lookup dict: key = (date_ymd, hhmm) -> candle
            lookup = {}
            for c in candles:
                rt = c.get('readable_time', '')
                parts = rt.split(',')
                if len(parts) >= 2:
                    date_part = parts[0].strip()
                    time_part = parts[1].strip().split(':')[:2]
                    hhmm = ':'.join(time_part)
                    try:
                        d_obj = datetime.strptime(date_part, "%d-%B-%Y")
                        date_ymd = d_obj.strftime("%Y-%m-%d")
                        lookup[(date_ymd, hhmm)] = c
                    except Exception as e:
                        print(f"Date parse error: {date_part} - {e}")
            cache[api] = lookup
            # Debug: print first few keys
            print(f"Backtest: Loaded {len(lookup)} candles for {api}")
            if len(lookup) > 0:
                sample_keys = list(lookup.keys())[:3]
                print(f"Sample keys: {sample_keys}")
        except Exception as e:
            print(f"Backtest error {api}: {e}")
            cache[api] = None

    # Process each signal with majority voting (threshold 65%)
    win_signals = []
    loss_signals = []
    THRESHOLD = 0.75

    for sig in signals:
        display = sig['display']
        api = sig['api_pair']
        time_str = sig['time']
        direction = sig['dir']

        lookup = cache.get(api)
        if not lookup:
            print(f"No lookup for {api}")
            continue

        win_count = 0
        total_count = 0
        for date_ymd in date_list:
            total_count += 1
            entry = lookup.get((date_ymd, time_str))
            if not entry:
                # No data for this exact minute -> treat as loss for this day
                continue
            found = False
            # Check entry candle
            candle_dir = entry.get('direction', '').lower()
            if direction == "CALL" and candle_dir == "up":
                found = True
            elif direction == "PUT" and candle_dir == "down":
                found = True
            if not found and martingale_level > 0:
                h, m = map(int, time_str.split(':'))
                for step in range(1, martingale_level + 1):
                    next_dt = datetime(2000, 1, 1, h, m) + \
                        timedelta(minutes=step)
                    next_hhmm = next_dt.strftime("%H:%M")
                    next_candle = lookup.get((date_ymd, next_hhmm))
                    if next_candle:
                        ndir = next_candle.get('direction', '').lower()
                        if direction == "CALL" and ndir == "up":
                            found = True
                            break
                        elif direction == "PUT" and ndir == "down":
                            found = True
                            break
            if found:
                win_count += 1
        if total_count == 0:
            continue
        win_ratio = win_count / total_count
        if win_ratio >= THRESHOLD:
            win_signals.append(f"M1 {display} {time_str} {direction}")
        else:
            loss_signals.append(f"M1 {display} {time_str} {direction}")

    if not win_signals and not loss_signals:
        sender.send_message(
            uid,
            "⚠️ No signals could be processed. Possibly unsupported pairs or no historical data for selected days.\nCheck that your API returns data for the given pair and days.")
        return

    win_msg = "✅ 𝚆𝙸𝙽 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n━━━━━━━━━━━━━━━━━\n" + \
        "\n".join(win_signals) if win_signals else "✅ No winning signals"
    loss_msg = "❌ 𝙻𝙾𝚂𝚂 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n━━━━━━━━━━━━━━━━━\n" + \
        "\n".join(loss_signals) if loss_signals else "❌ No losing signals"
    sender.send_message(uid, win_msg)
    sender.send_message(uid, loss_msg)

    total_signals = len(win_signals) + len(loss_signals)
    if total_signals == 0:
        return
    acceptance_rate = len(win_signals) / total_signals * 100
    summary = f"📊 𝚂𝚄𝙼𝙼𝙰𝚁𝚈\n━━━━━━━━━━━━━━━━━\n✅ Accepted: {
        len(win_signals)}\n❌ Rejected: {
        len(loss_signals)}\n📈 Acceptance Rate: {
            acceptance_rate:.1f}% (threshold 75%)\n🎯 Martingale Level: {martingale_level}\n📅 Days tested: {days}"
    sender.send_message(uid, summary)


def run_ai_filter_pattern_match(uid, signals_text, threshold, context):

    pass

    """AI Filter using pattern matching (KNN-like) on historical candles."""
    from datetime import datetime, timedelta
    import re
    import math
    import time as t

    # ----- Helper: flexible signal parser (same as checker) -----
    def parse_signal_line(line):
        line = normalize_fancy(line)
        line = re.sub(r'^M\d+\s*', '', line)
        pair_match = re.search(r'([A-Z0-9]+[_-]?[A-Z0-9]*(?:[_-]OTC)?)', line)
        if not pair_match:
            return None, None, None
        pair_raw = pair_match.group(1).upper()
        pair_raw = pair_raw.replace('_OTC', '-OTC')
        time_match = re.search(r'(\d{2}:\d{2})', line)
        if not time_match:
            return None, None, None
        time_str = time_match.group(1)
        dir_match = re.search(
            r'(CALL|PUT|BUY|SELL|UP|DOWN)',
            line,
            re.IGNORECASE)
        if not dir_match:
            return None, None, None
        dir_raw = dir_match.group(1).upper()
        if dir_raw in ("BUY", "UP"):
            direction = "CALL"
        elif dir_raw in ("SELL", "DOWN"):
            direction = "PUT"
        else:
            direction = dir_raw
        return pair_raw, time_str, direction

    # ----- Parse signals -----
    raw_lines = [l.strip() for l in signals_text.split('\n') if l.strip()]
    signals = []
    for line in raw_lines:
        pair_raw, time_str, direction = parse_signal_line(line)
        if not pair_raw:
            continue
        signals.append({
            'display': pair_raw,
            'api_pair': pair_raw,  # API expects e.g., USDBDT-OTC (without q)
            'time': time_str,
            'dir': direction,
        })
    if not signals:
        sender.send_message(uid, "❌ No valid signals found.")
        context.user_data['state'] = None
        return

    # ----- Fetch historical candles for each unique pair (use quotex-proxy en
    cache = {}
    for sig in signals:
        api = sig['api_pair']
        if api in cache:
            continue
        # Use the 2000-candle endpoint
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={api}&interval=1m&limit=2000:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
        headers = {"apikey": SUPABASE_ANON_KEY,
                   "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                cache[api] = None
                continue
            data = resp.json()
            candles = data.get('candles', [])
            if not candles:
                cache[api] = None
                continue
            candles.sort(key=lambda x: x['time'])
            for c in candles:
                rt = c.get('readable_time', '')
                match = re.search(r', (\d{2}:\d{2}):', rt)
                c['hhmm'] = match.group(1) if match else ''
            cache[api] = candles
        except Exception as e:
            print(f"AI Filter error {api}: {e}")
            cache[api] = None

    # ----- Helper: normalize a 20-candle segment for pattern comparison -----
    def normalize_pattern(segment):
        if len(segment) < 20:
            return None
        first_open = segment[0]['open']
        if first_open == 0:
            return None
        pattern = []
        for c in segment:
            pattern.append((c['open'] - first_open) / first_open)
            pattern.append((c['high'] - first_open) / first_open)
            pattern.append((c['low'] - first_open) / first_open)
            pattern.append((c['close'] - first_open) / first_open)
        return pattern

    def similarity(pattern1, pattern2):
        if not pattern1 or not pattern2:
            return 1e9
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(pattern1, pattern2)))

    # ----- Process each signal -----
    accepted = []
    rejected = []
    progress_msg = sender.send_message(uid, "⏳ Processing signals... 0%")
    if not progress_msg:
        context.user_data['state'] = None
        return
    progress_id = progress_msg.id

    for idx, sig in enumerate(signals):
        pct = int((idx + 1) / len(signals) * 100)
        sender.edit_message(
            uid, progress_id, f"⏳ Processing signal {
                idx + 1}/{
                len(signals)}... {
                progress_bar_text(pct)}")

        display = sig['display']
        api = sig['api_pair']
        time_str = sig['time']
        direction = sig['dir']

        candles = cache.get(api)
        if not candles or len(candles) < 200:
            rejected.append(f"M1 {display} {time_str} {direction}")
            continue

        target_idx = None
        for i, c in enumerate(candles):
            if c.get('hhmm') == time_str:
                target_idx = i
                break
        if target_idx is None or target_idx < 20:
            rejected.append(f"M1 {display} {time_str} {direction}")
            continue

        pattern_start = target_idx - 20
        if pattern_start < 0:
            rejected.append(f"M1 {display} {time_str} {direction}")
            continue
        pattern_candles = candles[pattern_start:target_idx]
        current_pattern = normalize_pattern(pattern_candles)
        if current_pattern is None:
            rejected.append(f"M1 {display} {time_str} {direction}")
            continue

        similarities = []
        for i in range(0, len(candles) - 20):
            if i == pattern_start:
                continue
            seg = candles[i:i + 20]
            pat = normalize_pattern(seg)
            if pat is None:
                continue
            dist = similarity(current_pattern, pat)
            similarities.append((dist, i))
        similarities.sort(key=lambda x: x[0])
        top_matches = similarities[:15]

        if not top_matches:
            rejected.append(f"M1 {display} {time_str} {direction}")
            continue

        win_count = 0
        total_count = 0
        for dist, idx_match in top_matches:
            outcome_idx = idx_match + 20
            if outcome_idx >= len(candles):
                continue
            outcome_candle = candles[outcome_idx]
            if direction == "CALL":
                if outcome_candle['close'] > outcome_candle['open']:
                    win_count += 1
            else:
                if outcome_candle['close'] < outcome_candle['open']:
                    win_count += 1
            total_count += 1

        if total_count == 0:
            rejected.append(f"M1 {display} {time_str} {direction}")
            continue

        win_rate = (win_count / total_count) * 100
        if win_rate >= threshold:
            accepted.append(f"M1 {display} {time_str} {direction}")
        else:
            rejected.append(f"M1 {display} {time_str} {direction}")

    sender.edit_message(uid, progress_id, "✅ AI Filter complete!")

    win_msg = "✅ 𝙰𝙲𝙲𝙴𝙿𝚃𝙴𝙳 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n━━━━━━━━━━━━━━━━━\n" + \
        "\n".join(accepted) if accepted else "✅ No accepted signals"
    loss_msg = "❌ 𝚁𝙴𝙹𝙴𝙲𝚃𝙴𝙳 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n━━━━━━━━━━━━━━━━━\n" + \
        "\n".join(rejected) if rejected else "❌ No rejected signals"

    sender.send_message(uid, win_msg)
    sender.send_message(uid, loss_msg)

    summary = f"📊 𝙰𝙸 𝙵𝙸𝙻𝚃𝙴𝚁 𝚁𝙴𝚂𝚄𝙻𝚃𝚂\n━━━━━━━━━━━━━━━━━\n📈 Confidence threshold: {threshold}%\n✅ Accepted: {
        len(accepted)}\n❌ Rejected: {
        len(rejected)}\n🎯 Win rate required: ≥{threshold}%"
    sender.send_message(uid, summary)

    context.user_data['state'] = None


# ══════════════ FUTURE SIGNAL FUNCTIONS ══════════════
FUT_PAIRS = [
    "AUDCAD_OTC", "AUDJPY_OTC", "AUDNZD_OTC", "AUDUSD_OTC", "BRLUSD_OTC",
    "CADCHF_OTC", "CADJPY_OTC", "CHFJPY_OTC", "EURAUD_OTC", "EURCAD_OTC",
    "EURCHF_OTC", "EURGBP_OTC", "EURJPY_OTC", "EURNZD_OTC", "EURSGD_OTC",
    "EURUSD_OTC", "GBPAUD_OTC", "GBPCAD_OTC", "GBPCHF_OTC", "GBPJPY_OTC",
    "GBPUSD_OTC", "NZDUSD_OTC", "USDARS_OTC", "USDBDT_OTC", "USDCAD_OTC",
    "USDCHF_OTC", "USDEGP_OTC", "USDGBP_OTC", "USDIDR_OTC", "USDINR_OTC",
    "USDJPY_OTC", "USDMXN_OTC", "USDNGN_OTC", "USDPKR_OTC", "USDTRY_OTC",
    "USDZAR_OTC", "USDPHP_OTC"
]
SUPPORTED_LIVE_PAIRS = [
    "USDBDT_OTC", "USDARS_OTC", "USDINR_OTC", "USDMXN_OTC", "USDNGN_OTC",
    "USDEGP_OTC", "USDPKR_OTC", "USDIDR_OTC", "BRLUSD_OTC", "NZDUSD_OTC",
    "GBPNZD_OTC", "EURNZD_OTC", "NZDCAD_OTC", "CADCHF_OTC", "NZDJPY_OTC",
    "NZDCHF_OTC", "AUDNZD_OTC", "BTCUSD_OTC", "XAUUSD_OTC", "EURUSD_OTC",
    "GBPUSD_OTC", "USDJPY_OTC", "EURJPY_OTC", "AUDUSD_OTC", "USDCAD_OTC",
    "USDCHF_OTC", "EURGBP_OTC", "EURCHF_OTC", "GBPJPY_OTC", "AUDJPY_OTC",
    "GBPCAD_OTC", "EURCAD_OTC", "AUDCAD_OTC", "USDDZD_OTC", "MSFT_OTC",
    "FB_OTC", "MCD_OTC", "INTC_OTC"
]


def time_to_min(t):

    pass

    try:
        h, m = map(int, t.split(':'))
        return h * 60 + m
    except BaseException:
        return 0

def convert_time_offset(time_str, from_offset, to_offset):
    """Convert time string from one offset to another. Offsets in hours."""
    h, m = map(int, time_str.split(':'))
    total_min = h * 60 + m + (to_offset - from_offset) * 60
    total_min %= 24 * 60
    return f"{total_min // 60:02d}:{total_min % 60:02d}"

def generate_future_signals(uid, min_conf=75, start_time="00:00", end_time="23:59", selected_pairs=None):
    if selected_pairs is None:
        selected_pairs = FUT_PAIRS
    all_signals = []
    # Convert start/end times from UTC+5 (user) to UTC+6 (API)
    api_start = convert_time_offset(start_time, 5, 6)
    api_end = convert_time_offset(end_time, 5, 6)

    for pair in selected_pairs:
        pair_api = pair.replace("_OTC", "_otc")
        url = f"https://quotexotc-futureapi.poghen-dx.workers.dev/pairs={pair_api}?start_time={api_start}&end_time={api_end}"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "success":
                continue
            for sig in data.get("signals", []):
                try:
                    acc = int(sig.get("accuracy", "0%").rstrip('%'))
                except:
                    acc = 0
                if acc >= min_conf:
                    # Signal time is in UTC+6, convert to UTC+5 for display
                    sig_time_utc6 = sig.get("time", "??:??")
                    display_time = convert_time_offset(sig_time_utc6, 6, 5)
                    all_signals.append({
                        'time': display_time,
                        'pair': pair,
                        'dir': sig.get("direction", "?").upper(),
                        'acc': acc
                    })
        except:
            pass
    if not all_signals:
        return None
    # Sort by display time (UTC+5)
    all_signals.sort(key=lambda x: time_to_min(x['time']))

    # Build output with fancy font for everything except signal lines
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    date_str = now_pk.strftime("%Y-%m-%d")
    header = (
        f"{fancy_font('🏐 𝚂𝙼𝚉 𝙱𝙾𝚃 𝙵𝚄𝚃𝚄𝚁𝙴 🏐')}\n\n"
        f"{fancy_font('🗓 ')}{fancy_font(date_str)}\n\n"
        f"{fancy_font('💎 𝚃𝚒𝚖𝚎𝚣𝚘𝚗𝚎: 𝚄𝚃𝙲 ＋𝟶𝟻：𝟶𝟶')}\n\n"
        f"{fancy_font('⏳ 𝚃𝙸𝙼𝙴𝙵𝚁𝙰𝙼𝙴: 𝙼𝟷')}\n"
        f"{fancy_font('⏰ 𝚄𝚂𝙴 𝙼𝚃𝙶 𝙾𝙽𝙴 𝙸𝙵 𝚁𝙴𝚀𝚄𝙸𝚁𝙴𝙳')}\n\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
    )
    body = "\n".join([f"M1 {sig['pair']} {sig['time']} {sig['dir']}" for sig in all_signals])
    footer = (
        f"\n{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n\n"
        f"{fancy_font('𝚄𝚂𝙴 𝚂𝙰𝙵𝙴𝚃𝚈 𝙵𝙾𝚁 𝙱𝙴𝚃𝚃𝙴𝚁 𝚁𝙴𝚂𝚄𝙻𝚃 🔥')}"
    )
    return header + body + footer

# ══════════════ SMZ HACKING MODE (via sio.tools catalog) ══════════════
SMZ_HACK_DEFAULT_ASSETS = [
    "ATOUSD-OTC", "AXSUSD-OTC", "BNBUSD-OTC", "BTCUSD-OTC",
    "DOTUSD-OTC", "ETHUSD-OTC", "INTC-OTC", "MCD-OTC",
    "PFE-OTC", "TRUUSD-OTC"
]

SIO_ALL_PAIRS = [
    "ATOUSD-OTC", "AUDCAD-OTC", "AUDCHF-OTC", "AUDJPY-OTC", "AUDNZD-OTC",
    "AUDUSD-OTC", "AVAUSD-OTC", "AXP-OTC", "AXSUSD-OTC", "BA-OTC",
    "BCHUSD-OTC", "BNBUSD-OTC", "BTCUSD-OTC", "CADCHF-OTC", "CADJPY-OTC",
    "CHFJPY-OTC", "DASUSD-OTC", "DOTUSD-OTC", "ETCUSD-OTC", "ETHUSD-OTC",
    "EURAUD-OTC", "EURCAD-OTC", "EURCHF-OTC", "EURGBP-OTC", "EURJPY-OTC",
    "EURNZD-OTC", "EURUSD-OTC", "FB-OTC", "GBPAUD-OTC", "GBPCAD-OTC",
    "GBPCHF-OTC", "GBPJPY-OTC", "GBPNZD-OTC", "GBPUSD-OTC", "INTC-OTC",
    "JNJ-OTC", "LINUSD-OTC", "LTCUSD-OTC", "MCD-OTC", "MSFT-OTC",
    "NZDCAD-OTC", "NZDCHF-OTC", "NZDJPY-OTC", "NZDUSD-OTC", "PFE-OTC",
    "SOLUSD-OTC", "TONUSD-OTC", "TRUUSD-OTC", "UKBrent-OTC", "USCrude-OTC",
    "USDARS-OTC", "USDBDT-OTC", "USDBRL-OTC", "USDCAD-OTC", "USDCHF-OTC",
    "USDCOP-OTC", "USDDZD-OTC", "USDEGP-OTC", "USDIDR-OTC", "USDINR-OTC",
    "USDJPY-OTC", "USDMXN-OTC", "USDNGN-OTC", "USDPHP-OTC", "USDPKR-OTC",
    "USDZAR-OTC", "XAUUSD-OTC", "XRPUSD-OTC", "ZECUSD-OTC"
]


def _smz_hack_time_user_to_api(time_str, user_tz=5, api_tz=-3):

    pass

    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        user_dt = datetime.strptime(
            f"{today.date()} {time_str}", "%Y-%m-%d %H:%M")
        api_dt = user_dt - timedelta(hours=(user_tz - api_tz))
        return api_dt.strftime("%H:%M")
    except BaseException:
        return time_str


def _smz_hack_time_api_to_user(time_str, user_tz=5, api_tz=-3):

    pass

    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        api_dt = datetime.strptime(
            f"{today.date()} {time_str}", "%Y-%m-%d %H:%M")
        user_dt = api_dt + timedelta(hours=(user_tz - api_tz))
        return user_dt.strftime("%H:%M")
    except BaseException:
        return time_str


def _fix_timeframe(tf):

    pass

    mapping = {"1": "M1", "5": "M5", "15": "M15", "60": "H1", "30": "M30",
               "m1": "M1", "m5": "M5", "m15": "M15", "h1": "H1", "m30": "M30"}
    return mapping.get(str(tf).strip().lower(), tf.upper() if tf else "M1")


# ══════════════ NEW MODES: TREND FILTER, TEXT FORMATTER, FONT CHANGER, ET
def process_trend_filter(uid, signals_text):

    pass

    lines = [l.strip() for l in signals_text.strip().split('\n') if l.strip()]
    if not lines:
        return "❌ No signals provided."
    accepted = []
    rejected = []
    supported = SUPPORTED_LIVE_PAIRS
    for line in lines:
        parsed = parse_signal_line(line)
        if not parsed[0]:
            rejected.append(f"⚠️ Invalid format: {line}")
            continue
        pair, time_str, direction = parsed
        if pair not in supported:
            rejected.append(f"⚠️ Unsupported pair: {pair}")
            continue
        date_str = (
            datetime.now(
                timezone.utc) +
            timedelta(
                hours=5)).strftime("%Y-%m-%d")
        pair_api = pair.replace("_", "-") + "q"
        url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={pair_api}&interval=1m&limit=2000:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                rejected.append(f"❌ No data for {pair}")
                continue
            data = r.json()
            candles = data.get('candles', [])
            if not candles:
                rejected.append(f"❌ No candles for {pair}")
                continue
            try:
                signal_dt = datetime.strptime(
                    f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                signal_dt = signal_dt.replace(
                    tzinfo=timezone(timedelta(hours=5)))
            except BaseException:
                rejected.append(f"⚠️ Invalid date/time: {date_str} {time_str}")
                continue
            target_ts = int((signal_dt - timedelta(hours=5)).timestamp())
            signal_candle = None
            for c in candles:
                if abs(int(c['time']) - target_ts) < 30:
                    signal_candle = c
                    break
            if not signal_candle:
                rejected.append(f"❌ Candle not found for {pair} {time_str}")
                continue
            prev_ts = target_ts - 3600
            prev_candle = None
            for c in candles:
                if abs(int(c['time']) - prev_ts) < 30:
                    prev_candle = c
                    break
            if not prev_candle:
                rejected.append(
                    f"❌ Not enough history (1h) for {pair} {time_str}")
                continue
            curr_close = float(signal_candle['close'])
            prev_close = float(prev_candle['close'])
            trend_up = curr_close > prev_close
            signal_emoji = '📉' if direction == 'CALL' else '📈'
            if direction == 'CALL' and trend_up:
                accepted.append(
                    f"✅ {pair} {time_str} {direction} {signal_emoji} (Trend: up)")
            elif direction == 'PUT' and not trend_up:
                accepted.append(
                    f"✅ {pair} {time_str} {direction} {signal_emoji} (Trend: down)")
            else:
                rejected.append(
                    f"❌ {pair} {time_str} {direction} {signal_emoji} (Trend: {
                        'up' if trend_up else 'down'})")
        except Exception as e:
            rejected.append(f"❌ Error: {e}")
    total = len(lines)
    acc_count = len(accepted)
    rej_count = len(rejected)
    result = "📉 **Trend Filter Results**\n\n"
    result += f"🔹 Accepted: {acc_count}/{total}\n"
    result += "\n".join(accepted) if accepted else "None"
    result += f"\n\n🔸 Rejected: {rej_count}/{total}\n"
    result += "\n".join(rejected) if rejected else "None"
    return result


def format_signals_with_template(original_lines, template):

    pass

    converted = []
    has_placeholders = any(
        p in template for p in [
            '<PAIR>',
            '<TIME>',
            '<DIRECTION>',
            '<pair>',
            '<time>',
            '<direction>'])
    example_parsed = None
    if not has_placeholders:
        example_parsed = parse_signal_line(template)

    for line in original_lines:
        parsed = parse_signal_line(line)
        if not parsed[0]:
            converted.append(f"⚠️ Could not parse: {line}")
            continue
        pair, time_str, direction = parsed

        if has_placeholders:
            result = template
            result = result.replace('<PAIR>', pair).replace('<pair>', pair)
            result = result.replace(
                '<TIME>', time_str).replace(
                '<time>', time_str)
            result = result.replace(
                '<DIRECTION>',
                direction).replace(
                '<direction>',
                direction)
            result = result.replace(
                '<DIR>', direction).replace(
                '<dir>', direction)
        else:
            if example_parsed and example_parsed[0]:
                ex_pair, ex_time, ex_dir = example_parsed
                result = template
                if ex_pair:
                    result = result.replace(ex_pair, pair, 1)
                if ex_time:
                    result = result.replace(ex_time, time_str, 1)
                if ex_dir:
                    result = result.replace(ex_dir.upper(), direction, 1)
                    result = result.replace(ex_dir.lower(), direction, 1)
            else:
                result = f"{pair} {time_str} {direction}"

        # ✅ CONVERT _OTC TO -OTC IN THE FINAL OUTPUT
        result = result.replace('_OTC', '-OTC')

        converted.append(result)

    return "\n".join(converted)


def fetch_payout_live(pair):

    pass

    pair_api = pair.replace("_", "-") + "q"
    url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={pair_api}&interval=1m&limit=1:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            payout = data.get("payout", "!")
            if isinstance(payout, str):
                payout = payout.replace('%', '')
            try:
                return int(payout)
            except BaseException:
                return payout
    except BaseException:
        pass
    return "!"


def run_pair_payout(uid, context):

    pass

    loading_msg = sender.send_message(uid, "⏳ Loading pair payouts...")
    result_lines = []
    for pair in FUT_PAIRS:
        payout = fetch_payout_live(pair)
        if payout != "!" and isinstance(payout, (int, float)):
            result_lines.append(f"🎥 {pair} : 💲 {payout}%")
        time.sleep(0.3)
    if not result_lines:
        sender.send_message(uid, "❌ No supported pairs found.")
    else:
        text = "📊 **Pair Payout%**\n\n" + "\n".join(result_lines)
        sender.send_message(uid, text)
    try:
        sender.edit_message(uid, loading_msg.id, "✅ Payout list ready.")
    except BaseException:
        pass


def get_trend_from_candles(pair):

    pass

    pair_api = pair.replace("_", "-") + "q"
    url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={pair_api}&interval=1m&limit=180:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            candles = data.get("candles", [])
            if len(candles) >= 2:
                first_close = float(candles[0]['close'])
                last_close = float(candles[-1]['close'])
                change_pct = (last_close - first_close) / first_close * 100
                if change_pct > 0.2:
                    return 'up'
                elif change_pct < -0.2:
                    return 'down'
                else:
                    return 'sideways'
    except BaseException:
        pass
    return None


def run_market_trend(uid, context):

    pass

    loading_msg = sender.send_message(uid, "⏳ Loading market trends...")
    result_lines = []
    for pair in FUT_PAIRS:
        trend = get_trend_from_candles(pair)
        if trend is None:
            continue
        if trend == 'up':
            emoji = '📉'
        elif trend == 'down':
            emoji = '📈'
        else:
            emoji = '➡️'
        result_lines.append(f"🎥 {pair} : {emoji} {trend}")
        time.sleep(0.3)
    if not result_lines:
        sender.send_message(uid, "❌ No supported pairs found.")
    else:
        text = "📈 **Market Trend (last 3 hours)**\n\n" + \
            "\n".join(result_lines)
        sender.send_message(uid, text)
    try:
        sender.edit_message(uid, loading_msg.id, "✅ Trend list ready.")
    except BaseException:
        pass


def fetch_recent_candles(pair, limit=6):

    pass

    pair_api = pair.replace("_OTC", "-OTC") + "q"
    url = f"https://ikszeynptbmwkaaldfad.supabase.co/functions/v1/quotex-proxy?symbol={pair_api}&interval=1m&limit={limit}:qx_fxbd1pmgumxe8xo8j9mgz8nbeiabq3p3"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get('candles', [])
    except BaseException:
        pass
    return None


def run_candle_colors(uid, context):

    pass

    loading_msg = sender.send_message(uid, "⏳ Loading candle colors...")
    pairs = FUT_PAIRS
    result_lines = []
    for pair in pairs:
        candles = fetch_recent_candles(pair, limit=6)
        if candles:
            colors = []
            for c in candles:
                if c['close'] >= c['open']:
                    colors.append('🟢')
                else:
                    colors.append('🔴')
            color_str = ''.join(colors)
            result_lines.append(f"➡️ {pair} : {color_str}")
        time.sleep(0.3)
    if not result_lines:
        sender.send_message(uid, "❌ Could not fetch candle data.")
    else:
        text = "🕯️ **Candle Colors (last 6)**\n\n" + "\n".join(result_lines)
        sender.send_message(uid, text)
    try:
        sender.edit_message(uid, loading_msg.id, "✅ Candle colors ready.")
    except BaseException:
        pass


async def s6_score_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 70 <= val <= 100:
            get_state(uid).strategy6_min_score = val
            await update.message.reply_text("Enter minimum candles for analysis (30‑200):")
            return S6_MIN_CANDLES
        else:
            await update.message.reply_text("❌ Enter between 70‑100:")
            return S6_SCORE
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S6_SCORE


async def s6_candles_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 10 <= val <= 200:
            get_state(uid).strategy6_min_candles = val
            await update.message.reply_text(
                f"✅ Confluence score ≥ {get_state(uid).strategy6_min_score}, "
                f"min candles = {val}. Scanning..."
            )
            bot = SMZXBot(uid)
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 30‑200:")
            return S6_MIN_CANDLES
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S6_MIN_CANDLES

# SMZ Hacking Mode ke liye supported pairs (35 pairs)
SMZ_ALL_PAIRS = [
    "USDBDT-OTC", "USDARS-OTC", "USDINR-OTC", "USDMXN-OTC", "USDNGN-OTC", "USDEGP-OTC", "USDPKR-OTC",
    "USDIDR-OTC", "BRLUSD-OTC", "NZDUSD-OTC", "GBPNZD-OTC", "EURNZD-OTC", "NZDCAD-OTC", "CADCHF-OTC",
    "NZDJPY-OTC", "NZDCHF-OTC", "AUDNZD-OTC", "BTCUSD-OTC", "XAUUSD-OTC", "EURUSD-OTC", "GBPUSD-OTC",
    "USDJPY-OTC", "EURJPY-OTC", "AUDUSD-OTC", "USDCAD-OTC", "USDCHF-OTC", "EURGBP-OTC", "EURCHF-OTC",
    "GBPJPY-OTC", "AUDJPY-OTC", "GBPCAD-OTC", "EURCAD-OTC", "AUDCAD-OTC", "USDDZD-OTC"
]


# ══════════════ STATE CONSTANTS (must match previous parts) ══════════════
(S2_FILTER_CHOICE, S2_FILTER_TOGGLE, S2_ACCURACY,
 S3_ACCURACY, S3_LOOKBACK, S4_ACCURACY, S5_SCORE) = range(7)

STATE_CHECKER_CUSTOM_DATE, STATE_CHECKER_SIGNALS = range(7, 9)
STATE_FUT_MIN_CONF, STATE_FUT_START_TIME, STATE_FUT_END_TIME, STATE_FUT_CUSTOM_PAIRS = range(
    9, 13)
STATE_BACKTEST_START, STATE_BACKTEST_END, STATE_BACKTEST_SIGNALS = range(
    13, 16)
STATE_UTC_ORIG_OFFSET, STATE_UTC_TARGET_OFFSET, STATE_UTC_SIGNALS = range(
    16, 19)
STATE_FORMATTER_INPUT, STATE_FORMATTER_EXAMPLE = range(19, 21)
STATE_FONT_INPUT, STATE_FONT_STYLE = range(21, 23)
STATE_TREND_FILTER_INPUT = 23
S6_SCORE, S6_MIN_CANDLES = range(26, 28)
STATE_MM_PROMPT = 28
STATE_MM_BALANCE = 29
STATE_MM_TP = 30
STATE_MM_SL = 31
STATE_AI_MIN_CONSENSUS = 32
STATE_AI_REQUIRED_STRATS = 33
STATE_CHART_ANALYZER = 34
STATE_AI_FILTER_SIGNALS = 40
STATE_AI_FILTER_CONFIDENCE = 41

# Backtest Conversation States (new)
STATE_BACKTEST_LIST = 63
STATE_BACKTEST_MTG = 64
STATE_BACKTEST_DAYS = 65
STATE_BACKTEST_CUSTOM_DAYS = 66

# AI Filter States
STATE_AI_FILTER_SIGNALS = 69
STATE_AI_FILTER_CONFIDENCE = 70
STATE_AI_FILTER_RUNNING = 71

STATE_AUTO_SIGNAL_CHANNEL = 75
STATE_AUTO_SIGNAL_STRATEGY = 76
STATE_AUTO_SIGNAL_RUNNING = 77

# ══════════════ AUTO TRADE STATES ══════════════
STATE_AUTO_LOGIN_EMAIL = 42
STATE_AUTO_LOGIN_PASSWORD = 43
STATE_AUTO_TP = 44
STATE_AUTO_SL = 45
STATE_AUTO_MTG = 46
STATE_AUTO_STRATEGY = 47
STATE_AUTO_CONFIRM = 48
STATE_AUTO_RUNNING = 49
STATE_AUTO_ACCOUNT = 50
STATE_AUTO_RISK = 51
# per-strategy parameter states (mirror the main-menu Start-Trading flow)
STATE_AUTO_SIGNAL_CHANNEL = 75
STATE_AUTO_SIGNAL_STRATEGY = 76
STATE_AUTO_SIGNAL_RUNNING = 77
STATE_AUTO_S2_CHOICE = 78
STATE_AUTO_S2_FILTER_TOGGLE = 79
STATE_AUTO_S2_ACC = 80
STATE_AUTO_S3_ACC = 81
STATE_AUTO_S3_LB = 82
STATE_AUTO_S4_ACC = 83
STATE_AUTO_S5_SCORE = 84
STATE_AUTO_S6_SCORE = 85
STATE_AUTO_S6_CANDLES = 86

# Auto Signal specific states (to avoid conflict with Auto Trade)
STATE_AUTO_SIGNAL_S2_CHOICE = 91
STATE_AUTO_SIGNAL_S2_FILTER_TOGGLE = 92
STATE_AUTO_SIGNAL_S2_ACC = 93
STATE_AUTO_SIGNAL_S3_ACC = 94
STATE_AUTO_SIGNAL_S3_LB = 95
STATE_AUTO_SIGNAL_S4_ACC = 96
STATE_AUTO_SIGNAL_S5_SCORE = 97
STATE_AUTO_SIGNAL_S6_SCORE = 98
STATE_AUTO_SIGNAL_S6_CANDLES = 99

STATE_AUTO_SIGNAL_FORMAT = 101

STATE_BLACKOUT_START_TIME = 104
STATE_BLACKOUT_END_TIME = 105
STATE_BLACKOUT_PAIR_SELECT = 106

STATE_BLACKOUT_CHECKER_DATE = 110
STATE_BLACKOUT_CHECKER_SIGNALS = 111
STATE_BLACKOUT_CHECKER_MTG = 112

STATE_ALCOHOL_TF = 200
STATE_ALCOHOL_DIR = 201
STATE_ALCOHOL_DAYS = 202
STATE_ALCOHOL_CUSTOM_DAYS = 203
STATE_ALCOHOL_UTC = 204
STATE_ALCOHOL_PAIR_MODE = 205
STATE_ALCOHOL_CUSTOM_PAIR_SELECT = 206
STATE_ALCOHOL_GENERATING = 207
STATE_ALCOHOL_START_TIME = 208
STATE_ALCOHOL_END_TIME = 209

# Checker 2.0 States
STATE_CHECKER2_UTC = 210
STATE_CHECKER2_DATE = 211
STATE_CHECKER2_MTG = 212
STATE_CHECKER2_SIGNALS = 213

# Backtest 2.0 States
STATE_BACKTEST2_UTC = 220
STATE_BACKTEST2_DAYS = 221      # instead of STATE_BACKTEST2_DATE
STATE_BACKTEST2_MTG = 222
STATE_BACKTEST2_SIGNALS = 223

SIO_API_KEY = "cd4f82dcd34eec38b79eea0ed47212bdc5c4a852d192ab2d983655bb5ce2c4b2"
SIO_API_BASE = "https://sio.tools"
SIO_ORIGINAL_TZ = -3

# Common UTC offsets for user selection
UTC_OFFSETS = [
    -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
]

# ══════════════ HELPER FUNCTIONS FOR BUTTONS & ENTITIES ══════════════


def colored_button(
        text,
        callback_data,
        style=KeyboardButtonStyle.PRIMARY,
        emoji_id=None):
    if emoji_id:
        return InlineKeyboardButton(
            text=text,
            callback_data=callback_data,
            style=style,
            icon_custom_emoji_id=emoji_id)
    else:
        return InlineKeyboardButton(
            text=text, callback_data=callback_data, style=style)

def convert_time_offset(time_str, from_offset, to_offset):
    """Convert time string HH:MM from one UTC offset to another."""
    h, m = map(int, time_str.split(':'))
    total_min = h * 60 + m + (to_offset - from_offset) * 60
    total_min %= 24 * 60
    return f"{total_min // 60:02d}:{total_min % 60:02d}"

def utf16_offset(text: str, char_index: int) -> int:
    offset = 0
    for i, ch in enumerate(text):
        if i == char_index:
            break
        offset += len(ch.encode('utf-16-le')) // 2
    return offset

async def send_restriction_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🚫 ACCESS RESTRICTED\n\nYou need access. Choose option:\n\n✨ Unlock all features instantly"
    crown = PREMIUM_EMOJI_IDS.get("😇", 6129805886383723340)
    menu_emoji = PREMIUM_EMOJI_IDS.get("😖", 5323642109767460983)
    contact = InlineKeyboardButton(" Contact Owner", url="https://t.me/Rohailtrader", style=KeyboardButtonStyle.SUCCESS, icon_custom_emoji_id=crown)
    main_btn = InlineKeyboardButton(" Main Menu", callback_data="restricted_main_menu", style=KeyboardButtonStyle.PRIMARY, icon_custom_emoji_id=menu_emoji)
    markup = InlineKeyboardMarkup([[contact], [main_btn]])
    
    # Always send as new message, never edit existing one
    if update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=markup)
    else:
        await update.message.reply_text(text, reply_markup=markup)

async def restricted_main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await start_cmd(update, context)

# ══════════════ CHECKER 2.0 (SIO.tools

# ══════════════ CHECKER 2.0 (SIO.tools) – requests version ══════════════

def _sio_checker_request(signals: list, date: str, gale: int) -> Optional[str]:
    if not signals:
        return None
    tf = signals[0].split(";")[0] if signals else "M1"
    minutes_map = {"M1": 1, "M2": 2, "M4": 4, "M5": 5, "M15": 15, "M30": 30}
    minutes = minutes_map.get(tf, 1)
    payload = {
        "info": {"broker": "quotex", "date": date, "gale": gale, "time": minutes},
        "signals": signals
    }
    headers = {
        "X-API-Key": SIO_API_KEY,
        "Content-Type": "application/json",
        "Referer": "https://sio.tools/"
    }
    try:
        resp = requests.post(f"{SIO_API_BASE}/quotex/check", json=payload, headers=headers, timeout=30)
        if resp.status_code != 200:
            # No print, just return None
            return None
        data = resp.json()
        if not data.get("status"):
            return None
        return data["id"]
    except Exception:
        return None

def _sio_checker_poll(check_id: str, max_retries=30, delay=3) -> Optional[dict]:
    headers = {
        "X-API-Key": SIO_API_KEY,
        "Referer": "https://sio.tools/"
    }
    for _ in range(max_retries):
        try:
            resp = requests.get(f"{SIO_API_BASE}/quotex/check/{check_id}", headers=headers, timeout=15)
            if resp.status_code != 200:
                time.sleep(delay)
                continue
            data = resp.json()
            if data.get("status") == "finished":
                return data
            time.sleep(delay)
        except Exception:
            time.sleep(delay)
    return None

def _sio_checker_poll(check_id: str, max_retries=30, delay=3) -> Optional[dict]:
    headers = {
        "X-API-Key": SIO_API_KEY,
        "Referer": "https://sio.tools/"
    }
    for _ in range(max_retries):
        try:
            resp = requests.get(f"{SIO_API_BASE}/quotex/check/{check_id}", headers=headers, timeout=15)
            if resp.status_code != 200:
                time.sleep(delay)
                continue
            data = resp.json()
            if data.get("status") == "finished":
                return data
            time.sleep(delay)
        except Exception as e:
            print(f"SIO poll error: {e}")
            time.sleep(delay)
    return None

def _sio_convert_time(time_str: str, from_offset: int, to_offset: int) -> str:
    h, m = map(int, time_str.split(':'))
    total_min = h * 60 + m + (to_offset - from_offset) * 60
    total_min %= 24 * 60
    return f"{total_min // 60:02d}:{total_min % 60:02d}"

def _chk2_parse_signals(signals_text: str, from_offset: int, base_date: str) -> list:
    """
    Parse signals in any flexible format using parse_signal_line.
    Returns list of dicts with keys: display_line, api_line, api_date, original_time, etc.
    """
    from datetime import datetime, timedelta, timezone
    lines = [l.strip() for l in signals_text.strip().split('\n') if l.strip()]
    tz_user = timezone(timedelta(hours=from_offset))
    tz_sio = timezone(timedelta(hours=SIO_ORIGINAL_TZ))
    base_date_obj = datetime.strptime(base_date, "%Y-%m-%d").date()
    parsed = []

    for line in lines:
        # Use existing flexible parser to extract pair, time, direction
        pair_raw, time_str, direction = parse_signal_line(line)
        if not pair_raw or not time_str or not direction:
            continue
        
        # Normalize pair: ensure -OTC format (SIO expects hyphen)
        pair = pair_raw.replace('_OTC', '-OTC')
        if 'OTC' not in pair:
            pair = pair + '-OTC'
        
        # Default timeframe to M1 (SIO only works with M1? Actually supports M1,M2... but we keep M1)
        tf = "M1"
        
        # Build display line (for output)
        display_line = f"{tf} {pair} {time_str} {direction}"
        
        # Combine user date with time to get user datetime
        naive_dt = datetime.combine(base_date_obj, datetime.strptime(time_str, "%H:%M").time())
        user_dt = naive_dt.replace(tzinfo=tz_user)
        sio_dt = user_dt.astimezone(tz_sio)
        api_date = sio_dt.strftime("%Y-%m-%d")
        api_time = sio_dt.strftime("%H:%M")
        api_line = f"{tf};{pair};{api_time};{direction}"
        
        parsed.append({
            'display_line': display_line,
            'api_line': api_line,
            'api_date': api_date,
            'original_time': time_str,
            'tf': tf,
            'pair': pair,
            'direction': direction
        })
    return parsed

def _run_sio_checker_thread(uid: int, context, signals_text: str):
    import threading
    import time as ttime
    from datetime import datetime, timedelta, timezone

    user_date_str = context.user_data.get('chk2_date')
    mtg = context.user_data.get('chk2_mtg', 1)
    utc_offset = context.user_data.get('chk2_utc', 5)

    # Parse signals with UTC-3 conversion
    parsed = _chk2_parse_signals(signals_text, utc_offset, user_date_str)
    if not parsed:
        sender.send_message(uid, "❌ No valid signals found.")
        return

    # Group by UTC-3 date
    groups = {}
    for sig in parsed:
        groups.setdefault(sig['api_date'], []).append(sig)

    # Send initial loading message with spinner and elapsed time
    loading_msg = sender.send_message(uid, "🔍 Check in progress...\n🩸 0:00\nStage: Collecting signals")
    if not loading_msg:
        return
    msg_id = loading_msg.id
    stop_anim = threading.Event()

    # Spinner frames (9 frames)
    spinner_frames = ['⣾', '⣽', '⣻', '⢿', '⡿', '⟯', '⣟', '⣯', '⣷']
    # Stages (cycle every 15 seconds)
    stages = [
        "Collecting signals",
        "Verifying candles",
        "Calculating outcomes",
        "Finalizing results"
    ]

    def animate():
        start = ttime.time()
        frame_idx = 0
        last_stage_update = start
        stage_idx = 0
        while not stop_anim.is_set():
            elapsed = int(ttime.time() - start)
            minutes = elapsed // 60
            seconds = elapsed % 60
            time_str = f"{minutes}:{seconds:02d}"
            spinner = spinner_frames[frame_idx % len(spinner_frames)]
            frame_idx += 1
            if ttime.time() - last_stage_update >= 15:
                stage_idx = (stage_idx + 1) % len(stages)
                last_stage_update = ttime.time()
            stage = stages[stage_idx]
            text = f"🔍 {spinner} Check in progress...\n⏳ {time_str}\nStage: {stage}"
            try:
                sender.edit_message(uid, msg_id, text)
            except:
                pass
            ttime.sleep(0.3)
    anim_thread = threading.Thread(target=animate, daemon=True)
    anim_thread.start()

    # Process each date group
    all_results = []   # (api_line, result_icon)
    error_occurred = False
    for api_date, sig_list in groups.items():
        api_signals = [s['api_line'] for s in sig_list]
        check_id = _sio_checker_request(api_signals, api_date, mtg)
        if not check_id:
            error_occurred = True
            break
        result = _sio_checker_poll(check_id, max_retries=90, delay=2)
        if not result:
            error_occurred = True
            break
        raw_signals = result.get("signals", [])
        for s in raw_signals:
            parts = s.split(";")
            if len(parts) < 5:
                continue
            tf, pair, tm, direc, status = parts[:5]
            api_line = f"{tf};{pair};{tm};{direc}"
            if status == "WIN":
                all_results.append((api_line, "✅"))
            elif status == "G1":
                all_results.append((api_line, "✅¹"))
            elif status == "LOSS":
                all_results.append((api_line, "❌"))
            else:
                all_results.append((api_line, "⏳"))

    stop_anim.set()
    anim_thread.join(timeout=1)

    if error_occurred:
        try:
            sender.edit_message(uid, msg_id, "❌ Server busy. Try again later.")
        except:
            sender.send_message(uid, "❌ Server busy. Try again later.")
        return

    # Map results
    result_map = {core: icon for core, icon in all_results}
    wins = sum(1 for icon in result_map.values() if icon in ("✅", "✅¹"))
    losses = sum(1 for icon in result_map.values() if icon == "❌")
    pending = sum(1 for icon in result_map.values() if icon == "⏳")
    total = wins + losses + pending

    # Build output lines with original user times
    body_lines = []
    for sig in parsed:
        icon = result_map.get(sig['api_line'], "⏳")
        body_lines.append(f"{sig['display_line']} {icon}")

    # Update loading message to completion
    try:
        sender.edit_message(uid, msg_id, "🎩 Check complete!")
    except:
        sender.send_message(uid, "🎩 Check complete!")

    # Send final output
    header = (
        f"{fancy_font('▰▱▱ 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 2.0 (𝚂𝙼𝚉) ▱▱▰')}\n"
        f"{fancy_font('              ┏━━━━━━━━━━━┓')}\n"
        f"{fancy_font('                 🗓 - ')}{fancy_font(user_date_str)}{fancy_font('          ')}\n"
        f"{fancy_font('              ┗━━━━━━━━━━━┛')}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
    )
    body = "\n".join(body_lines)
    summary = (
        f"\n{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
        f"{fancy_font('🏆 Total : ')}{fancy_font(str(total))}\n"
        f"{fancy_font('✅ Win: ')}{fancy_font(str(wins))}\n"
        f"{fancy_font('✖ Loss: ')}{fancy_font(str(losses))}\n"
        f"{fancy_font('⏳ Pending: ')}{fancy_font(str(pending))}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}"
    )
    final_msg = header + body + summary
    sender.send_message(uid, final_msg)

# ==================== CHECKER 2.0 CALLBACKS ====================

async def chk2_utc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("chk2_utc_page_"):
        page = int(data.split("_")[-1])
        msg = "🌐 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 2.0 (𝚂𝙸𝙾)\n\n⏰ 𝚂𝚎𝚕𝚎𝚌𝚝 𝚢𝚘𝚞𝚛 𝚄𝚃𝙲 𝚝𝚒𝚖𝚎𝚣𝚘𝚗𝚎:"
        buttons = build_utc_keyboard("chk2_utc_", page)
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))
        return
    offset_str = data.replace("chk2_utc_", "")
    try:
        offset = int(offset_str)
    except ValueError:
        return
    context.user_data['chk2_utc'] = offset
    context.user_data['state'] = STATE_CHECKER2_DATE
    msg = (
        f"✅ 𝚄𝚃𝙲 {offset:+d} 𝚜𝚎𝚕𝚎𝚌𝚝𝚎𝚍\n\n"
        f"📅 𝚂𝚎𝚕𝚎𝚌𝚝 𝚍𝚊𝚝𝚎:"
    )
    buttons = [
        [colored_button(" Today ", "chk2_date_today", KeyboardButtonStyle.SUCCESS, "6145553439809084250")],
        [colored_button(" Yesterday ", "chk2_date_yesterday", KeyboardButtonStyle.PRIMARY, "6147654280112248427")],
        [colored_button(" Custom Date ", "chk2_date_custom", KeyboardButtonStyle.PRIMARY, "5217822164362739968")],
        [colored_button(" Cancel ", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def chk2_date_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "chk2_date_today":
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        context.user_data['chk2_date'] = date_str
        await _show_chk2_mtg(update, context)
    elif data == "chk2_date_yesterday":
        date_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        context.user_data['chk2_date'] = date_str
        await _show_chk2_mtg(update, context)
    elif data == "chk2_date_custom":
        context.user_data['state'] = STATE_CHECKER2_DATE
        msg = "📅 𝙴𝚗𝚝𝚎𝚛 𝚍𝚊𝚝𝚎 (𝚈𝚈𝚈𝚈-𝙼𝙼-𝙳𝙳):"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)

async def _show_chk2_mtg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    context.user_data['state'] = STATE_CHECKER2_MTG
    msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝚃𝙸𝙽𝙶𝙰𝙻𝙴 𝙻𝙴𝚅𝙴𝙻"
    buttons = [
        [colored_button(" MTG 0 (entry only)", "chk2_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
        [colored_button(" MTG 1 (entry+1)", "chk2_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
        [colored_button(" MTG 2 (entry+2)", "chk2_mtg_2", KeyboardButtonStyle.PRIMARY, "6145248943807667330")],
        [colored_button(" MTG 3 (entry+3)", "chk2_mtg_3", KeyboardButtonStyle.PRIMARY, "5316681209026191987")],
        [colored_button(" Cancel", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def chk2_mtg_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    mtg = int(data.split("_")[-1])
    context.user_data['chk2_mtg'] = mtg
    context.user_data['state'] = STATE_CHECKER2_SIGNALS
    msg = (
        "🩸 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 2.0 — 𝙿𝙰𝚂𝚃𝙴 𝚈𝙾𝚄𝚁 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n\n"
        "📝 𝙵𝚘𝚛𝚖𝚊𝚝 (𝙼𝟷;𝙿𝙰𝙸𝚁;𝙷𝙷:𝙼𝙼;𝙳𝙸𝚁𝙴𝙲𝚃𝙸𝙾𝙽):\n"
        "𝙼𝟷;𝙴𝚄𝚁𝚄𝚂𝙳-𝙾𝚃𝙲;𝟶𝟾:𝟸𝟺;𝙲𝙰𝙻𝙻\n"
        "𝙼𝟷;𝙶𝙱𝙿𝚄𝚂𝙳-𝙾𝚃𝙲;𝟶𝟿:𝟷𝟻;𝙿𝚄𝚃\n\n"
        "⏰ 𝚄𝚜𝚎 𝚢𝚘𝚞𝚛 𝚜𝚎𝚕𝚎𝚌𝚝𝚎𝚍 𝚝𝚒𝚖𝚎𝚣𝚘𝚗𝚎\n"
        "📌 𝙿𝚊𝚜𝚝𝚎 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚗𝚘𝚠:"
    )
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities)

async def _show_utc_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str, prefix: str, page: int = 0):
    query = update.callback_query
    buttons = build_utc_keyboard(prefix, page)
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    if query:
        await query.edit_message_text(msg, entities=entities, reply_markup=markup)
    else:
        await update.message.reply_text(msg, entities=entities, reply_markup=markup)

def build_utc_keyboard(prefix: str, page: int = 0, per_page: int = 28):
    offsets = UTC_OFFSETS  # make sure UTC_OFFSETS is defined
    start = page * per_page
    end = min(start + per_page, len(offsets))
    page_offsets = offsets[start:end]
    total_pages = (len(offsets) + per_page - 1) // per_page
    buttons = []
    row = []
    for off in page_offsets:
        sign = "+" if off >= 0 else ""
        label = f"UTC{sign}{off}"
        cb = f"{prefix}{off}"
        if off == -3:
            label = f" UTC{sign}{off}"
        row.append(InlineKeyboardButton(text=label, callback_data=cb, style=KeyboardButtonStyle.PRIMARY))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}page_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}page_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)
    buttons.append([colored_button(" Cancel", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")])
    return buttons

# ══════════════ BACKTEST 2.0 (SIO.tools) - Placeholder callbacks ═══════

# ==================== BACKTEST 2.0 – WORKING CURL VERSION ====================
import subprocess, json, time, re
from datetime import datetime, timedelta, timezone

def _bt2_curl(url, method="GET", data=None, headers=None, timeout=60):
    cmd = ["curl", "-s", "--max-time", str(timeout)]
    if method == "POST":
        cmd += ["-X", "POST"]
    if data:
        cmd += ["-d", json.dumps(data)]
    cmd += ["-H", f"X-API-Key: {SIO_API_KEY}"]
    if data:
        cmd += ["-H", "Content-Type: application/json"]
    if headers:
        for k, v in headers.items():
            cmd += ["-H", f"{k}: {v}"]
    cmd += ["-H", "Referer: https://sio.tools/"]
    cmd.append(url)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout+10)
    return r.returncode, r.stdout, r.stderr

def _bt2_run_backtest(signals, start_date, end_date, gale, days, mode="geral"):
    if not signals:
        return None
    sd = datetime.strptime(start_date, "%Y-%m-%d")
    ed = datetime.strptime(end_date, "%Y-%m-%d")
    diff = max(days, (ed - sd).days + 1)
    payload = {
        "info": {
            "broker": "quotex",
            "brokerLabel": "Quotex",
            "startDate": start_date,
            "endDate": end_date,
            "gale": gale,
            "days": diff,
            "mode": mode
        },
        "signals": signals
    }
    rc, out, err = _bt2_curl(f"{SIO_API_BASE}/quotex/backtest", method="POST", data=payload, timeout=60)
    if rc != 0:
        return None
    try:
        d = json.loads(out)
    except:
        return None
    if not d.get("status"):
        return None
    return d["id"]

def _bt2_get_result(bt_id, max_retries=300, delay=3):
    for i in range(max_retries):
        rc, out, err = _bt2_curl(f"{SIO_API_BASE}/quotex/backtest/{bt_id}", timeout=30)
        if rc != 0:
            time.sleep(delay)
            continue
        try:
            d = json.loads(out)
        except:
            time.sleep(delay)
            continue
        if d.get("status") == "finished":
            return d
        time.sleep(delay)
    return None

def _bt2_format_signals(signals_text: str) -> list:
    """Convert flexible format to M1;PAIR-OTC;HH:MM;DIRECTION"""
    lines = [l.strip() for l in signals_text.strip().split('\n') if l.strip()]
    sio_signals = []
    for line in lines:
        pair_raw, time_str, direction = parse_signal_line(line)
        if not pair_raw or not time_str or not direction:
            parts = line.replace(';', ' ').split()
            for p in parts:
                if re.match(r'^\d{2}:\d{2}$', p):
                    time_str = p
                elif p.upper() in ('CALL','PUT'):
                    direction = p.upper()
                else:
                    pair_raw = p
        if not (pair_raw and time_str and direction):
            continue
        pair = pair_raw.replace('_OTC', '-OTC').replace('_', '-')
        if not pair.endswith('-OTC'):
            pair += '-OTC'
        sio_signals.append(f"M1;{pair};{time_str};{direction}")
    return sio_signals

def _run_sio_backtest_thread(uid: int, context, signals_text: str):
    import threading
    import time as ttime
    from datetime import datetime, timedelta, timezone

    user_offset = context.user_data.get('bt2_utc')
    days = context.user_data.get('bt2_days')
    gale = context.user_data.get('bt2_mtg')
    if user_offset is None or days is None or gale is None:
        sender.send_message(uid, "❌ Settings missing. Please start again from menu.")
        return

    tz_user = timezone(timedelta(hours=user_offset))
    today_user = datetime.now(tz_user).date()
    start_date = (today_user - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = (today_user - timedelta(days=1)).strftime("%Y-%m-%d")

    sio_signals = _bt2_format_signals(signals_text)
    if not sio_signals:
        sender.send_message(uid, "❌ No valid signals found.")
        return

    # Send initial loading message
    loading_msg = sender.send_message(uid, "🔍 Backtest started...\n🩸 0:00\nStage: Collecting signals")
    if not loading_msg:
        return
    msg_id = loading_msg.id
    stop_anim = threading.Event()

    # Spinner frames (9 frames for smooth rotation)
    spinner_frames = ['⣾', '⣽', '⣻', '⢿', '⡿', '⟯', '⣟', '⣯', '⣷']
    # Stages (will cycle every 15 seconds)
    stages = [
        "Collecting signals",
        "Verifying days outcomes",
        "Calculating outcomes",
        "Finalizing results"
    ]

    def animate():
        start = ttime.time()
        frame_idx = 0
        last_stage_update = start
        stage_idx = 0
        while not stop_anim.is_set():
            elapsed = int(ttime.time() - start)
            minutes = elapsed // 60
            seconds = elapsed % 60
            time_str = f"{minutes}:{seconds:02d}"
            # Rotate spinner
            spinner = spinner_frames[frame_idx % len(spinner_frames)]
            frame_idx += 1
            # Change stage every 15 seconds
            if ttime.time() - last_stage_update >= 15:
                stage_idx = (stage_idx + 1) % len(stages)
                last_stage_update = ttime.time()
            stage = stages[stage_idx]
            text = f"🔍 {spinner} Backtest in progress...\n🩸 {time_str}\nStage: {stage}"
            try:
                sender.edit_message(uid, msg_id, text)
            except:
                pass
            ttime.sleep(0.3)   # smooth spinner
    anim_thread = threading.Thread(target=animate, daemon=True)
    anim_thread.start()

    # Run actual backtest
    bt_id = _bt2_run_backtest(sio_signals, start_date, end_date, gale, days, "geral")
    if not bt_id:
        stop_anim.set()
        anim_thread.join(timeout=1)
        try:
            sender.edit_message(uid, msg_id, "❌ Server busy. Try again later.")
        except:
            sender.send_message(uid, "❌ Server busy. Try again later.")
        return

    result = _bt2_get_result(bt_id, max_retries=300, delay=3)
    stop_anim.set()
    anim_thread.join(timeout=1)

    if not result:
        try:
            sender.edit_message(uid, msg_id, "❌ Backtest timeout. Please try again.")
        except:
            sender.send_message(uid, "❌ Backtest timeout. Please try again.")
        return

    # Backtest finished – update message
    try:
        sender.edit_message(uid, msg_id, "🎩 Backtest Completed!")
    except:
        sender.send_message(uid, "🎩 Backtest Completed!")

    # Parse winners and losers (unchanged)
    raw_signals = result.get("signals", [])
    wins = []
    losses = []

    for s in raw_signals:
        if s.endswith(";WIN"):
            clean = s[:-4].replace(';', ' ')
            wins.append(clean)
        elif s.endswith(";LOSS"):
            clean = s[:-5].replace(';', ' ')
            losses.append(clean)

    loss_list = result.get("loss_list", [])
    for s in loss_list:
        if s.endswith(";LOSS"):
            clean = s[:-5].replace(';', ' ')
            losses.append(clean)

    wins = list(dict.fromkeys(wins))
    losses = list(dict.fromkeys(losses))

    if wins:
        win_msg = "✅ 𝚆𝙸𝙽 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n━━━━━━━━━━━━━━━━━\n" + "\n".join(wins)
        sender.send_message(uid, win_msg)
    else:
        sender.send_message(uid, "✅ No winning signals")

    if losses:
        loss_msg = "❌ 𝙻𝙾𝚂𝚂 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n━━━━━━━━━━━━━━━━━\n" + "\n".join(losses)
        sender.send_message(uid, loss_msg)
    else:
        sender.send_message(uid, "❌ No losing signals")

    total = len(wins) + len(losses)
    summary = (
        f"━━━━━━━━━━━━━━━━━\n"
        f"🏆 Total: {total}\n"
        f"✅ Win: {len(wins)}\n"
        f"✖ Loss: {len(losses)}\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    sender.send_message(uid, summary)

async def bt2_utc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("bt2_utc_page_"):
        page = int(data.split("_")[-1])
        msg = "🤝 𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃 2.0\n\n⏰ 𝚂𝚎𝚕𝚎𝚌𝚝 𝚢𝚘𝚞𝚛 𝚄𝚃𝙲 𝚘𝚏𝚏𝚜𝚎𝚝 (𝚏𝚘𝚛 𝚜𝚒𝚐𝚗𝚊𝚕 𝚝𝚒𝚖𝚎𝚜):"
        buttons = build_utc_keyboard("bt2_utc_", page)
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))
        return
    offset_str = data.replace("bt2_utc_", "")
    try:
        offset = int(offset_str)
    except ValueError:
        return
    context.user_data['bt2_utc'] = offset
    context.user_data['state'] = STATE_BACKTEST2_DAYS
    msg = "🤝 𝚂𝙴𝙻𝙴𝙲𝚃 𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃 𝙳𝙰𝚈𝚂 (𝚎𝚡𝚌𝚕𝚞𝚍𝚒𝚗𝚐 𝚝𝚘𝚍𝚊𝚢)"
    buttons = [
        [colored_button(" 3 Days ", "bt2_days_3", KeyboardButtonStyle.SUCCESS, "6145553439809084250"),
         colored_button(" 5 Days ", "bt2_days_5", KeyboardButtonStyle.PRIMARY, "6147654280112248427")],
        [colored_button(" 7 Days ", "bt2_days_7", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
         colored_button(" Custom (2-30) ", "bt2_days_custom", KeyboardButtonStyle.PRIMARY, "5217822164362739968")],
        [colored_button(" Cancel ", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def bt2_days_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "bt2_days_custom":
        context.user_data['state'] = STATE_BACKTEST2_DAYS   # wait for number input
        msg = "🔢 𝙴𝚗𝚝𝚎𝚛 𝚗𝚞𝚖𝚋𝚎𝚛 𝚘𝚏 𝚍𝚊𝚢𝚜 (𝟸-𝟹𝟶):"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return
    # preset days: 3,5,7
    days = int(data.split("_")[-1])
    context.user_data['bt2_days'] = days
    await _show_bt2_mtg(update, context)

async def _show_bt2_mtg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    context.user_data['state'] = STATE_BACKTEST2_MTG
    msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝚃𝙸𝙽𝙶𝙰𝙻𝙴 𝙻𝙴𝚅𝙴𝙻 (𝙶𝙰𝙻𝙴)"
    buttons = [
        [colored_button(" Gale 0 (none)", "bt2_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
         colored_button(" Gale 1", "bt2_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
        [colored_button(" Gale 2", "bt2_mtg_2", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
         colored_button(" Gale 3", "bt2_mtg_3", KeyboardButtonStyle.PRIMARY, "5316681209026191987")],
        [colored_button(" Cancel", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    # Send as new message (not edit)
    await query.message.reply_text(msg, entities=entities, reply_markup=markup)
    # Delete the previous message (the one with UTC buttons)
    try:
        await query.message.delete()
    except:
        pass

async def bt2_mtg_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    mtg = int(data.split("_")[-1])
    context.user_data['bt2_mtg'] = mtg
    context.user_data['state'] = STATE_BACKTEST2_SIGNALS
    msg = (
        "🩸 𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃 2.0 — 𝙿𝙰𝚂𝚃𝙴 𝚈𝙾𝚄𝚁 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n\n"
        "📝 𝙰𝚗𝚢 𝚏𝚕𝚎𝚡𝚒𝚋𝚕𝚎 𝚏𝚘𝚛𝚖𝚊𝚝 (𝚘𝚗𝚎 𝚙𝚎𝚛 𝚕𝚒𝚗𝚎):\n"
        "   𝙼𝟷;𝙴𝚄𝚁𝚄𝚂𝙳-𝙾𝚃𝙲;𝟶𝟾:𝟸𝟺;𝙲𝙰𝙻𝙻\n"
        "   𝙴𝚄𝚁𝚄𝚂𝙳-𝙾𝚃𝙲 𝟶𝟾:𝟸𝟺 𝙲𝙰𝙻𝙻\n"
        "   𝟶𝟾:𝟸𝟺 𝙴𝚄𝚁𝚄𝚂𝙳-𝙾𝚃𝙲 𝙿𝚄𝚃\n\n"
        "⏰ 𝚄𝚜𝚎 𝚝𝚑𝚎 𝚝𝚒𝚖𝚎𝚣𝚘𝚗𝚎 𝚢𝚘𝚞 𝚜𝚎𝚕𝚎𝚌𝚝𝚎𝚍\n"
        "📌 𝙿𝚊𝚜𝚝𝚎 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚗𝚘𝚠:"
    )
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities)

def _bt2_parse_signals(signals_text: str) -> list:
    """
    Parse signals from any flexible format into SIO format M1;PAIR-OTC;HH:MM;DIRECTION.
    No timezone conversion – we'll convert the whole date range later.
    """
    lines = [l.strip() for l in signals_text.strip().split('\n') if l.strip()]
    sio_signals = []
    for line in lines:
        pair_raw, time_str, direction = parse_signal_line(line)
        if not pair_raw or not time_str or not direction:
            continue
        pair = pair_raw.replace('_OTC', '-OTC')
        if 'OTC' not in pair:
            pair = pair + '-OTC'
        sio_signals.append(f"M1;{pair};{time_str};{direction}")
    return sio_signals

# ══════════════ BOT HANDLERS ══════════════

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    text = (
        f"🥸Assalamu Alaikum @{username} 👋\n\n"
        f"Welcome to our most advanced binary market tool🫠\n\n"
        f"🦶 Your all-in-one Binary Trading Tools Bot is here to help you trade smarter, faster, and more efficiently.\n\n"
        f"😷 Get accurate signals, powerful tools, and real-time insights to boost your trading experience.\n\n"
        f"🤢Tap below to get started and explore the features! 👇\n\n"
    )

    # Build premium emoji entities
    entities = build_custom_emoji_entities(text)

    # Add bold entity for the whole text
    text_utf16_len = len(text.encode('utf-16-le')) // 2
    entities.append(MessageEntity(type='bold', offset=0, length=text_utf16_len))

    buttons = [
    [colored_button("        AI OTC        ", "menu_ai_mode", KeyboardButtonStyle.SUCCESS, "5314391089514291948"),
     colored_button("     OTC LIVE MODE     ", "menu_analysis", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
    [colored_button("     Signal Checker     ", "menu_checker", KeyboardButtonStyle.SUCCESS, "6145553439809084250"),
     colored_button("        Backtest        ", "menu_backtest", KeyboardButtonStyle.SUCCESS, "6147840110462245787")],
    [colored_button("       Auto Trade       ", "auto_trade_start", KeyboardButtonStyle.SUCCESS, "5316681209026191987"),
     colored_button("    Future Signals     ", "menu_futuresignal", KeyboardButtonStyle.SUCCESS, "6062153953833917531")],
    [colored_button("    CHECKER 2.0        ", "menu_checker2", KeyboardButtonStyle.SUCCESS, "6147440218942218700"),
     colored_button("   BACKTEST 2.0        ", "menu_backtest2", KeyboardButtonStyle.SUCCESS, "6145546134069714639")],
    [colored_button("     BLACKOUT FS         ", "menu_blackout_fs", KeyboardButtonStyle.SUCCESS, "6282889047778007721"),
     colored_button(" BLACKOUT CHECKER ", "menu_blackout_checker", KeyboardButtonStyle.SUCCESS, "6282889047778007721")],
    [colored_button("     UTC Converter      ", "menu_utc_converter", KeyboardButtonStyle.SUCCESS, "5413879192267805083"),
     colored_button("     Pair Payout%      ", "menu_pair_payout", KeyboardButtonStyle.SUCCESS, "6145449239607515472")],
    [colored_button("     Market Trend      ", "menu_market_trend", KeyboardButtonStyle.SUCCESS, "6147654280112248427"),
     colored_button("     Candle Colors     ", "menu_candle_colors", KeyboardButtonStyle.SUCCESS, "5217911744495624141")],
    [colored_button("    Text Formatter     ", "menu_text_formatter", KeyboardButtonStyle.SUCCESS, "5282843764451195532"),
     colored_button("     Font Changer      ", "menu_font_changer", KeyboardButtonStyle.SUCCESS, "6282685788450721937")],
    [colored_button("     Trend Filter      ", "menu_trend_filter", KeyboardButtonStyle.SUCCESS, "6086858751749396920"),
     colored_button("      Auto Signal      ", "menu_auto_signal", KeyboardButtonStyle.SUCCESS, "5965469803299738005")],
    [colored_button("       AI Filter FS     ", "menu_ai_filter", KeyboardButtonStyle.SUCCESS, "6217370240800527004"),
     colored_button("   AI Chart Analyzer   ", "menu_chart_analyzer", KeyboardButtonStyle.SUCCESS, "5854710508065658472")],
    [colored_button("         About          ", "menu_admin", KeyboardButtonStyle.DANGER, "6035189951581129197")],
]

    reply_markup = InlineKeyboardMarkup(buttons)

    await context.bot.send_message(chat_id=uid, text=text, entities=entities, reply_markup=reply_markup)
    context.user_data['strategy_active'] = False
    context.user_data['state'] = None

async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data  # ← YEH LINE IMPORTANT HAI
    
    # About button ke liye koi restriction nahi
    if data != "menu_admin" and not is_authorized(uid):
        await send_restriction_message(update, context)
        return

    # ==================== PUBLIC BUTTONS (no authorization) ====================
    if data == "menu_admin":
        msg = (
            "😘 𝗔𝗕𝗢𝗨𝗧 𝗦𝗠𝗭𝗫 𝗡𝗜𝗚𝗛𝗧𝗠𝗔𝗥𝗘 𝗔𝗜 𝗕𝗢𝗧\n\n"
            "🧠 Owner  : @Rohailtrader\n"
            "😔 Broker : QUOTEX, TRADOWIX\n"
            "🤮 Timeframe : M1\n\n"
            "👌 Real-time market data\n"
            "👌 AI technical analysis\n"
            "👌 Live candlestick chart\n"
            "👌 Automatic result tracking\n"
            "👌 Win / Loss statistics\n"
            "👌 Auto Trading Supported\n"
            "👌 Future Signal List Generation\n\n"
            "😩 And Many More !! Check Out✓"
        )
        entities = build_custom_emoji_entities(msg)
        msg_utf16_len = len(msg.encode('utf-16-le')) // 2
        entities.append(MessageEntity(type='bold', offset=0, length=msg_utf16_len))
        
        # Sirf do buttons - Developer aur Channel
        buttons = [
            [InlineKeyboardButton(" Developer", url="https://t.me/Rohailtrader", style=KeyboardButtonStyle.SUCCESS, icon_custom_emoji_id="5276025009947551999")],
            [InlineKeyboardButton(" Channel", url="https://t.me/tradewithrohail", style=KeyboardButtonStyle.SUCCESS, icon_custom_emoji_id="6021518426432869078")],
        ]
        reply_markup = InlineKeyboardMarkup(buttons)
        
        # Naye message ke roop mein bhejo (edit nahi karna)
        await query.message.reply_text(msg, entities=entities, reply_markup=reply_markup)
        return

    elif data == "back_to_main":
        await query.message.delete()
        await start_cmd(update, context)
        return

    # ==================== AUTHORIZATION FOR ALL OTHER BUTTONS ====================
    if not is_authorized(uid):
        await query.answer("⛔ Access denied. Contact Admin to get access.", show_alert=True)
        return

    # ==================== REST OF YOUR MENU (including Auto Trade) ====================
    if data == "auto_trade_start":
        await auto_trade_start(update, context)
        return

    elif data == "menu_ai_mode":
        st = get_state(uid)
        if st.running:
            text = "⏳ Already running a signal. Use /stop first."
            entities = build_custom_emoji_entities(text)
            await query.message.reply_text(text, entities=entities)
            return
        st.ai_mode = True
        context.user_data['state'] = STATE_AI_MIN_CONSENSUS
        context.user_data['uid'] = uid
        msg = "🤖 𝙰𝙸 𝙼𝙾𝙳𝙴 𝚂𝙴𝚃𝚄𝙿\n\n🔰 𝙴𝚗𝚝𝚎𝚛 𝚖𝚒𝚗𝚒𝚖𝚞𝚖 𝚗𝚞𝚖𝚋𝚎𝚛 𝚘𝚏 𝚜𝚝𝚛𝚊𝚝𝚎𝚐𝚒𝚎𝚜 𝚝𝚑𝚊𝚝 𝚖𝚞𝚜𝚝 𝚊𝚐𝚛𝚎𝚎 (𝟸‑𝟻):"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data == "menu_analysis":
        # Direct strategy selection – no IN BOT / OTH CHANNEL step
        text = "🤖 𝚂𝙴𝙻𝙴𝙲𝚃 𝚂𝚃𝚁𝙰𝚃𝙴𝙶𝚈 (1-6):"
        buttons = []
        for i in range(1, 7):
            style = KeyboardButtonStyle.SUCCESS if i % 2 else KeyboardButtonStyle.PRIMARY
            buttons.append([InlineKeyboardButton(f"Strategy {i}", callback_data=f"strat_{i}", style=style)])
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(text)
        await query.message.reply_text(text, entities=entities, reply_markup=markup)
        return

    elif data == "menu_checker2":
        context.user_data['state'] = STATE_CHECKER2_UTC
        msg = "🌐 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 2.0 (𝚂𝙼𝚉)\n\n⏰ 𝚂𝚎𝚕𝚎𝚌𝚝 𝚢𝚘𝚞𝚛 𝚄𝚃𝙲 𝚘𝚏𝚏𝚜𝚎𝚝:"
        buttons = build_utc_keyboard("chk2_utc_", 0)
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "menu_backtest2":
        context.user_data['state'] = STATE_BACKTEST2_UTC
        msg = "🌐 𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃 2.0 (𝚂𝙼𝚉)\n\n⏰ 𝚂𝚎𝚕𝚎𝚌𝚝 𝚢𝚘𝚞𝚛 𝚄𝚃𝙲 𝚘𝚏𝚏𝚜𝚎𝚝:"
        buttons = build_utc_keyboard("bt2_utc_", 0)
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "menu_blackout_fs":
        # Start the flow: ask for start time
        context.user_data['blk_step'] = 'start_time'
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data == "blk_pair_all":
        # All pairs
        start_time = context.user_data.get('blk_start_time', '00:00')
        end_time = context.user_data.get('blk_end_time', '23:59')
        await run_blackout_fs(uid, start_time, end_time, SMZ_ALL_PAIRS)
        return

    elif data == "blk_pair_custom":
        # Start paginated custom pair selection
        context.user_data['blk_selected_pairs'] = set()
        context.user_data['blk_pair_page'] = 0
        # Reuse the same pagination builder but with different callback prefix
        buttons, page, total_pages = _build_blackout_pair_page(0, selected=set())
        selected_count = 0
        msg = f"🎯 Select pairs (Page 1/{total_pages}):\n\n💎 Tap pairs to select/deselect, then press Done\n📊 Selected: {selected_count} pairs"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))
        return

    elif data.startswith("blk_pairpage_"):
        page = int(data.replace("blk_pairpage_", ""))
        context.user_data['blk_pair_page'] = page
        selected = context.user_data.get('blk_selected_pairs', set())
        buttons, page, total_pages = _build_blackout_pair_page(page, selected=selected)
        selected_count = len(selected)
        msg = f"🎯 Select pairs (Page {page+1}/{total_pages}):\n\n💎 Tap pairs to select/deselect, then press Done\n📊 Selected: {selected_count} pairs"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))
        return

    elif data.startswith("blk_pickpair_"):
        pair = data.replace("blk_pickpair_", "")
        selected = context.user_data.get('blk_selected_pairs', set())
        if pair in selected:
            selected.discard(pair)
        else:
            selected.add(pair)
        context.user_data['blk_selected_pairs'] = selected
        page = context.user_data.get('blk_pair_page', 0)
        buttons, page, total_pages = _build_blackout_pair_page(page, selected=selected)
        selected_count = len(selected)
        msg = f"🎯 Select pairs (Page {page+1}/{total_pages}):\n\n💎 Tap pairs to select/deselect, then press Done\n📊 Selected: {selected_count} pairs"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))
        return

    elif data == "blk_pair_done":
        selected = context.user_data.get('blk_selected_pairs', set())
        if not selected:
            await query.message.reply_text("❌ Please select at least 1 pair!")
            return
        pairs_list = list(selected)
        start_time = context.user_data.get('blk_start_time', '00:00')
        end_time = context.user_data.get('blk_end_time', '23:59')
        context.user_data['blk_step'] = None
        context.user_data['blk_selected_pairs'] = None
        await query.edit_message_text(f"⏳ Generating blackout signals for {len(pairs_list)} custom pairs...\n🕒 {start_time} - {end_time}")
        await run_blackout_fs(uid, start_time, end_time, pairs_list)
        return

    elif data == "menu_blackout_checker":
        context.user_data['state'] = STATE_BLACKOUT_CHECKER_DATE
        msg = "📅 𝙱𝙻𝙰𝙲𝙺𝙾𝚄𝚃 𝙲𝙷𝙴𝙲𝙺𝙴𝚁\n\n𝚂𝚎𝚕𝚎𝚌𝚝 𝚍𝚊𝚝𝚎:"
        buttons = [
            [colored_button(" Today ", "bl_check_today", KeyboardButtonStyle.SUCCESS, "6145553439809084250")],
            [colored_button(" Yesterday ", "bl_check_yesterday", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
            [colored_button(" Custom Date ", "bl_check_custom", KeyboardButtonStyle.PRIMARY, "5217822164362739968")],
            [colored_button("  Cancel ", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "bl_check_today":
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        context.user_data['bl_checker_date'] = date_str
        await send_blackout_prompt(update, context)
        return

    elif data == "bl_check_yesterday":
        date_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        context.user_data['bl_checker_date'] = date_str
        await send_blackout_prompt(update, context)
        return

    elif data == "bl_check_custom":
        context.user_data['state'] = STATE_BLACKOUT_CHECKER_DATE
        msg = "📅 𝙴𝚗𝚝𝚎𝚛 𝚍𝚊𝚝𝚎 (𝚈𝚈𝚈𝚈-𝙼𝙼-𝙳𝙳):"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data.startswith("bl_mtg_"):
        mtg_level = int(data.split("_")[-1])
        context.user_data['bl_checker_mtg'] = mtg_level
        signals = context.user_data.get('bl_checker_signals', [])
        date_str = context.user_data.get('bl_checker_date')
        if not signals or not date_str:
            await query.message.reply_text("❌ 𝙳𝚊𝚝𝚊 𝚖𝚒𝚜𝚜𝚒𝚗𝚐. 𝙿𝚕𝚎𝚊𝚜𝚎 𝚜𝚝𝚊𝚛𝚝 𝚊𝚐𝚊𝚒𝚗.")
            return
        await query.message.reply_text(f"⏳ 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐 {len(signals)} 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 (𝙼𝚃𝙶={mtg_level})...\n⏰ 𝚃𝚑𝚒𝚜 𝚖𝚊𝚢 𝚝𝚊𝚔𝚎 𝚊 𝚏𝚎𝚠 𝚜𝚎𝚌𝚘𝚗𝚍𝚜.")
        threading.Thread(target=run_blackout_checker_worker, args=(query.from_user.id, date_str, signals, mtg_level, context), daemon=True).start()
        context.user_data['state'] = None
        return

    elif data == "menu_checker":
        buttons = [
            [colored_button(" Today", "checker_today", KeyboardButtonStyle.SUCCESS, "6102795674577803992")],
            [colored_button(" Yesterday", "checker_yesterday", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        text = "🔮 **Signal Checker**\nSelect date:"
        entities = build_custom_emoji_entities(text)
        await query.message.reply_text(text, entities=entities, reply_markup=markup)
        return

    elif data == "menu_futuresignal":
        context.user_data['strategy_active'] = False
        fut_text = (
            "🔥 𝙵𝚄𝚃𝚄𝚁𝙴 𝚂𝙸𝙶𝙽𝙰𝙻𝚂\n\n"
            "📊 Select Strategy:\n\n"
            "🚀 Strategy 1 – SMZ Future OTC\n"
            "   └ Generate signals from SMZ API\n\n"
            "🥷 Strategy 2 – SMZ Hacking Mode\n"
            "   └ Advanced signals from SMZ API\n"
        )
        fut_buttons = [
            [colored_button(" Strategy 1 – SMZ Future", "fut_strategy_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
            [colored_button(" Strategy 2 – SMZ Hacking", "fut_strategy_2", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
            [colored_button(" Strategy 3 – SMZ ALCOHOL", "fut_strategy_3", KeyboardButtonStyle.PRIMARY, "6267149259653518704")],
        ]
        entities = build_custom_emoji_entities(fut_text)
        await query.message.reply_text(fut_text, entities=entities, reply_markup=InlineKeyboardMarkup(fut_buttons))
        return

    elif data == "show_loss_candles":
        uid = query.from_user.id
        loss_data_list = context.user_data.get('loss_signals', [])
        if not loss_data_list:
            await query.answer("No loss signals to display.", show_alert=True)
            return

        await query.answer("Generating loss charts...")
        for idx, loss_data in enumerate(loss_data_list, 1):
            try:
                pair = loss_data['pair']
                entry_time = loss_data['entry_time']
                direction = loss_data['direction']
                candles = loss_data['candles']
                prev_count = loss_data['prev_count']
                next_count = loss_data['next_count']

                img_path = draw_loss_chart(pair, entry_time, direction, candles)
                if img_path and os.path.exists(img_path):
                    direction_emoji = "🔺" if direction == "CALL" else "🔻"
                    caption = (
                        f"📊 Loss Candle #{idx}\n"
                        f"Entry Time  : {entry_time}\n"
                        f"Market      : {pair}\n"
                        f"Direction   : {direction}{direction_emoji}\n"
                        f"Analysis    : Previous {prev_count} candle + Entry candle + Next {next_count} candles"
                    )
                    await context.bot.send_photo(chat_id=uid, photo=open(img_path, 'rb'), caption=caption)
                    os.remove(img_path)
                else:
                    await context.bot.send_message(uid, f"Failed to generate chart for {pair} at {entry_time}")
            except Exception as e:
                await context.bot.send_message(uid, f"Error generating chart: {e}")
        # Do not clear the list if you want the user to be able to click again
        # context.user_data['loss_signals'] = []
        return

    elif data == "fut_strategy_3":
        # Start alcohol strategy conversation
        context.user_data['uid'] = query.from_user.id
        context.user_data['state'] = STATE_ALCOHOL_TF
        msg = "🥃 𝚂𝙼𝚉 𝙰𝙻𝙲𝙾𝙷𝙾𝙻 𝚂𝚃𝚁𝙰𝚃𝙴𝙶𝚈\n━━━━━━━━━━━━━━━━━━\n𝚂𝚎𝚕𝚎𝚌𝚝 𝚃𝚒𝚖𝚎𝚏𝚛𝚊𝚖𝚎:"
        buttons = [
            [colored_button(" M1 ", "alc_tf_M1", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
             colored_button(" M2 ", "alc_tf_M2", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
            [colored_button(" M5 ", "alc_tf_M5", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
             colored_button(" M15 ", "alc_tf_M15", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
            [colored_button(" M30 ", "alc_tf_M30", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "fut_strategy_1":
        # Strategy 1 – SMZ Future OTC
        context.user_data['state'] = STATE_FUT_MIN_CONF
        context.user_data['uid'] = uid
        msg = "😈 Enter minimum confidence % (0-100):"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data == "fut_strategy_2":
        # Strategy 2 – SMZ Hacking Mode
        context.user_data['smz_step'] = 'start_time'
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data == "menu_backtest":
        context.user_data['state'] = STATE_BACKTEST_LIST
        context.user_data['uid'] = uid
        msg = "📋 𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃\n\nPaste your signal list (one per line).\nSupports formats like:\n• M1 USDDZD-OTC 16:15 BUY\n• USDINR-OTC;16:19;PUT\n• any flexible format"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data == "menu_utc_converter":
        context.user_data['state'] = STATE_UTC_ORIG_OFFSET
        sender.send_message(uid, "🕐 Enter original timezone offset (e.g., +0 for UTC, +5 for Pakistan):")
        return

    elif data == "menu_pair_payout":
        threading.Thread(target=run_pair_payout, args=(uid, context), daemon=True).start()
        return

    elif data == "menu_market_trend":
        threading.Thread(target=run_market_trend, args=(uid, context), daemon=True).start()
        return

    elif data == "menu_candle_colors":
        threading.Thread(target=run_candle_colors, args=(uid, context), daemon=True).start()
        return

    elif data == "menu_text_formatter":
        context.user_data['state'] = STATE_FORMATTER_INPUT
        sender.send_message(uid, "📝 **Text Formatter**\n\nSend me your signal list (one per line).\nFormat can be anything – I'll extract pair, time, direction.\nThen send an example of your desired output with placeholders like <PAIR>, <TIME>, <DIRECTION>.")
        return

    elif data == "menu_font_changer":
        context.user_data['state'] = STATE_FONT_INPUT
        sender.send_message(uid, "📱 **TEXT FONT CHANGER**\n\n📝 Please paste your signals or text below:\n\n✨ Premium emojis will be preserved!")
        return

    elif data == "menu_trend_filter":
        context.user_data['state'] = STATE_TREND_FILTER_INPUT
        msg = (
            "📉  𝚃𝚁𝙴𝙽𝙳 𝙵𝙸𝙻𝚃𝙴𝚁\n\n"
            "    Paste your signals below (one per line)\n"
            "📋 Format: M1;PAIR;HH:MM;DIRECTION\n"
            "📝 Example:\n"
            "   M1;GBPJPY-OTC;08:24;CALL\n"
            "   M1;EURUSD-OTC;09:15;PUT\n\n"
            "⏰ Use UTC+5 time\n"
            "📌 Paste your signals now..."
        )
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data == "menu_auto_signal":
        context.user_data['uid'] = uid
        context.user_data['state'] = STATE_AUTO_SIGNAL_FORMAT
        msg = "📝 𝚂𝙴𝙻𝙴𝙲𝚃 𝚂𝙸𝙶𝙽𝙰𝙻 𝙵𝙾𝚁𝙼𝙰𝚃\n\nChoose how you want signals to be sent:"
        buttons = [
            [colored_button(" Format 1 ", "auto_signal_fmt1", KeyboardButtonStyle.SUCCESS, "5283055978785285857")],
            [colored_button(" Format 2 ", "auto_signal_fmt2", KeyboardButtonStyle.PRIMARY, "5267419403019886452")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "menu_chart_analyzer":
        context.user_data['state'] = STATE_CHART_ANALYZER
        context.user_data['uid'] = uid
        msg = (
            "📸 𝙰𝙸 𝙲𝙷𝙰𝚁𝚃 𝙰𝙽𝙰𝙻𝚈𝚉𝙴𝚁\n\n"
            "🔰 𝚂𝚎𝚗𝚍 𝚖𝚎 𝚊 𝚌𝚑𝚊𝚛𝚝 𝚜𝚌𝚛𝚎𝚎𝚗𝚜𝚑𝚘𝚝 𝚊𝚗𝚍 𝙸 𝚠𝚒𝚕𝚕 𝚊𝚗𝚊𝚕𝚢𝚣𝚎 𝚒𝚝!\n\n"
            "💎 I will detect:\n"
            "  📊 Candlestick patterns\n"
            "  📈 Trend direction\n"
            "  🔥 Support & Resistance\n"
            "  🤖 Next 1-min signal (CALL/PUT)\n\n"
            "📸 Send your chart screenshot now..."
        )
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        print(f"🔍 DEBUG: menu_callback finished for data = '{data}'")
        return

    elif data.startswith("alc_tf_"):
        tf = data.split("_")[-1]  # e.g., M1, M5, M15
        context.user_data['alcohol_tf'] = tf
        context.user_data['state'] = STATE_ALCOHOL_DIR
        msg = f"✅ Timeframe: {tf}\n━━━━━━━━━━━━━━━━━━\n📊 𝚂𝚎𝚕𝚎𝚌𝚝 𝙳𝚒𝚛𝚎𝚌𝚝𝚒𝚘𝚗:"
        buttons = [
            [colored_button(" CALL ", "alc_dir_CALL", KeyboardButtonStyle.SUCCESS, "6064347140228912866"),
             colored_button(" PUT ", "alc_dir_PUT", KeyboardButtonStyle.DANGER, "6062085844242537125"),
             colored_button(" BOTH ", "alc_dir_BOTH", KeyboardButtonStyle.PRIMARY, "6147654280112248427")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.edit_text(msg, entities=entities, reply_markup=markup)
        return

    elif data.startswith("alc_dir_"):
        direction = data.split("_")[-1]  # CALL, PUT, BOTH
        context.user_data['alcohol_dir'] = direction
        context.user_data['state'] = STATE_ALCOHOL_DAYS
        msg = "📅 𝚂𝚎𝚕𝚎𝚌𝚝 𝙳𝚊𝚢𝚜 𝚝𝚘 𝙰𝚗𝚊𝚕𝚢𝚣𝚎:"
        buttons = [
            [colored_button(" 5 Days ", "alc_days_5", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
             colored_button(" 7 Days ", "alc_days_7", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
             colored_button(" 10 Days ", "alc_days_10", KeyboardButtonStyle.PRIMARY, "6145248943807667330")],
            [colored_button(" Custom (1-30) ", "alc_days_custom", KeyboardButtonStyle.SUCCESS, "5217822164362739968")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.edit_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "alc_days_custom":
        context.user_data['state'] = STATE_ALCOHOL_CUSTOM_DAYS
        msg = "🔢 𝙴𝚗𝚝𝚎𝚛 𝚗𝚞𝚖𝚋𝚎𝚛 𝚘𝚏 𝚍𝚊𝚢𝚜 (𝟷-𝟹𝟶):"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return

    elif data.startswith("alc_days_"):
        days = int(data.split("_")[-1])
        context.user_data['alcohol_days'] = days
        await proceed_to_utc_selection(update, context)
        return

    elif data.startswith("alc_utc_"):
        offset = float(data.split("_")[-1])
        context.user_data['alcohol_utc'] = offset
        # Now ask for start time
        context.user_data['state'] = STATE_ALCOHOL_START_TIME
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝙸𝙽 𝚈𝙾𝚄𝚁 𝚂𝙴𝙻𝙴𝙲𝚃𝙴𝙳 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.message.edit_text(msg, entities=entities)
        return

    elif data == "alc_pair_all":
        # Use all OTC-supported assets
        assets = list(ALCOHOL_ASSETS)  # ✅ Defined earlier
        context.user_data['alcohol_assets'] = assets
        await generate_alcohol_signals_wrapper(update, context)
        return

    elif data == "alc_pair_custom":
        context.user_data['state'] = STATE_ALCOHOL_CUSTOM_PAIR_SELECT
        context.user_data['alc_selected_pairs'] = set()
        context.user_data['alc_pair_page'] = 0
        await show_alc_pair_page(update, context)
        return

    elif data.startswith("alc_pairpage_"):
        # Pagination for custom pair selection
        page = int(data.split("_")[-1])
        context.user_data['alc_pair_page'] = page
        await show_alc_pair_page(update, context)
        return

    elif data.startswith("alc_pickpair_"):
        pair = data.replace("alc_pickpair_", "")
        selected = context.user_data.get('alc_selected_pairs', set())
        if pair in selected:
            selected.discard(pair)
        else:
            selected.add(pair)
        context.user_data['alc_selected_pairs'] = selected
        # Refresh the same page
        await show_alc_pair_page(update, context)
        return

    elif data == "alc_pair_done":
        selected = context.user_data.get('alc_selected_pairs', set())
        if not selected:
            await query.answer("❌ Select at least one pair!", show_alert=True)
            return
        context.user_data['alcohol_assets'] = list(selected)
        await generate_alcohol_signals_wrapper(update, context)
        return

    # Fallback – agar koi unrecognized callback aaye to ignore
    else:
        pass

# ----- Strategy selection (with MM prompt) -----

async def proceed_to_utc_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Get chat_id safely
    chat_id = update.effective_chat.id
    query = update.callback_query
    
    # Only answer if it's a callback query
    if query:
        await query.answer()
    
    context.user_data['state'] = STATE_ALCOHOL_UTC

    # UTC options (full list)
    utc_options = [
        (-12,"UTC-12"),(-11,"UTC-11"),(-10,"UTC-10"),(-9.5,"UTC-9:30"),
        (-9,"UTC-9"),(-8,"UTC-8"),(-7,"UTC-7"),(-6,"UTC-6"),(-5,"UTC-5"),
        (-4,"UTC-4"),(-3,"UTC-3"),(-2,"UTC-2"),(-1,"UTC-1"),(0,"UTC+0"),
        (1,"UTC+1"),(2,"UTC+2"),(3,"UTC+3"),(3.5,"UTC+3:30"),(4,"UTC+4"),
        (4.5,"UTC+4:30"),(5,"UTC+5"),(5.5,"UTC+5:30"),(5.75,"UTC+5:45"),
        (6,"UTC+6"),(6.5,"UTC+6:30"),(7,"UTC+7"),(8,"UTC+8"),
        (8.75,"UTC+8:45"),(9,"UTC+9"),(9.5,"UTC+9:30"),(10,"UTC+10"),
        (10.5,"UTC+10:30"),(11,"UTC+11"),(12,"UTC+12"),(12.75,"UTC+12:45"),
        (13,"UTC+13"),(14,"UTC+14")
    ]

    # Build buttons (3 per row)
    buttons = []
    row = []
    for offset, label in utc_options:
        row.append(colored_button(label, f"alc_utc_{offset}", KeyboardButtonStyle.PRIMARY, "6147654280112248427"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    markup = InlineKeyboardMarkup(buttons)
    msg = "🕐 𝚂𝚎𝚕𝚎𝚌𝚝 𝚈𝚘𝚞𝚛 𝚃𝚒𝚖𝚎𝚣𝚘𝚗𝚎:"
    entities = build_custom_emoji_entities(msg)

    # Send or edit depending on context
    if query:
        await query.edit_message_text(msg, entities=entities, reply_markup=markup)
    else:
        await context.bot.send_message(chat_id=chat_id, text=msg, entities=entities, reply_markup=markup)

async def show_alc_pair_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = context.user_data.get('alc_pair_page', 0)
    per_page = 15
    total = len(ALCOHOL_ASSETS)
    start = page * per_page
    end = min(start + per_page, total)
    page_pairs = ALCOHOL_ASSETS[start:end]
    total_pages = (total + per_page - 1) // per_page

    selected = context.user_data.get('alc_selected_pairs', set())

    buttons = []
    row = []
    for pair in page_pairs:
        short = pair.replace("_", "")
        label = f"✅ {short}" if pair in selected else short
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"alc_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"alc_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"alc_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "alc_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    msg = f"📊 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛𝚜 (𝙿𝚊𝚐𝚎 {page+1}/{total_pages})\n\n💎 𝚃𝚊𝚙 𝚝𝚘 𝚝𝚘𝚐𝚐𝚕𝚎, 𝚝𝚑𝚎𝚗 𝙳𝚘𝚗𝚎"
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))

async def generate_alcohol_signals_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    tf = context.user_data.get('alcohol_tf', 'M5')
    direction = context.user_data.get('alcohol_dir', 'PUT')
    days = context.user_data.get('alcohol_days', 7)
    utc_offset = context.user_data.get('alcohol_utc', 5.0)
    start_time = context.user_data.get('alcohol_start_time', '00:00')
    end_time = context.user_data.get('alcohol_end_time', '23:59')
    assets = context.user_data.get('alcohol_assets', [])

    if not assets:
        await query.edit_message_text("❌ No assets selected.")
        return

    loading_msg = await query.message.reply_text("⏳ Generating signals...\n⏰ Please wait, this may take 30-60 seconds.")

    def target():
        try:
            signals = generate_alcohol_signals(assets, tf, direction, days, utc_offset, start_time, end_time)
            if signals:
                # Date in DD.MM.YYYY format (Pakistan time)
                now_pk = datetime.now(timezone(timedelta(hours=5)))
                date_str = now_pk.strftime("%d.%m.%Y")

                # Format UTC offset (e.g., 5 -> +5:00, 5.5 -> +5:30)
                off = utc_offset
                off_h = int(off)
                off_m = int((off - off_h) * 60)
                off_str = f"{'+' if off_h >= 0 else '-'}{abs(off_h):02d}:{off_m:02d}"

                # Build header exactly as requested
                header = (
                    f"😊{date_str}\n\n"
                    f"🤠UTC {off_str}\n\n"
                    f"😵MAX MARTINGALE:1\n\n"
                    f"🏐PREMIUM SIGNALS SMZ\n\n"
                    f"Broker Quotex\n\n"
                )
                body = "\n".join(signals)
                footer = f"\n\n🖥  @Rohailtrader"
                final_msg = header + body + footer
                sender.send_message(uid, final_msg)
            else:
                sender.send_message(uid, "❌ No signals found. Try different settings.")
        except Exception as e:
            sender.send_message(uid, f"❌ Failed to generate signals.\n🔍 Error: {str(e)[:200]}")
        finally:
            # Optional: delete loading message
            try:
                sender.delete_message(loading_msg.chat_id, loading_msg.id)
            except:
                pass

    threading.Thread(target=target, daemon=True).start()


async def send_alcohol_error(uid, chat_id, loading_msg_id, error_msg):
    try:
        await sender.bot.delete_message(chat_id=chat_id, message_id=loading_msg_id)
    except:
        pass
    await sender.bot.send_message(chat_id=uid, text=f"❌ 𝙵𝚊𝚒𝚕𝚎𝚍 𝚝𝚘 𝚐𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝚜𝚒𝚐𝚗𝚊𝚕𝚜.\n🔍 𝙴𝚛𝚛𝚘𝚛: {error_msg[:200]}")

import requests
from datetime import datetime, timedelta, timezone
import random
import time

def firebase_signup_requests():
    """Create a fresh Firebase account and return ID token using requests."""
    rand_str = f"{int(time.time())}{random.randint(100,999)}"
    email = f"sio{rand_str}@siotmp.com"
    pwd = f"Temp{rand_str}!"
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=AIzaSyBPjKqD9v8ISapsLllSmjyufgoj5_X6h0E"
    payload = {"email": email, "password": pwd, "returnSecureToken": True}
    headers = {"Content-Type": "application/json", "Referer": "https://sio.tools/"}
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise Exception(f"Signup failed: {resp.text}")
    data = resp.json()
    if "error" in data:
        raise Exception(data["error"]["message"])
    return data["idToken"]

def sio_search(instrument, granularity, from_date, to_date, order_type, gale, percentage_min, utc_offset, token):
    url = "https://sio.tools/api/a/"
    payload = {
        "instrument": instrument,
        "granularity": granularity,
        "from": from_date,
        "to": to_date,
        "orderType": order_type,
        "gale": gale,
        "percentageMin": percentage_min,
        "utcOffset": utc_offset
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Referer": "https://sio.tools/cataloger-quotex/"
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if "error" in data:
            return []
        return data.get("groups", [])
    except Exception as e:
        print(f"sio_search error for {instrument}: {e}")
        return []   # return empty list on timeout/error

def generate_alcohol_signals(assets, timeframe, direction, days, utc_offset, start_time="00:00", end_time="23:59"):
    """
    assets: list of asset names (e.g., ["AUD_NZD", "BTC_USD"])
    timeframe: "M1","M2","M4","M5","M15","M30"
    direction: "CALL","PUT","BOTH"
    days: int 1-30
    utc_offset: float (e.g., 5.0)
    start_time, end_time: "HH:MM" in selected UTC offset
    Returns list of strings in format "M1;PAIR-OTC;HH:MM;DIRECTION"
    """
    # Map asset to OTC instrument
    otc_map = {
        "ATO_USD":"ATOUSD-OTC", "AUD_NZD":"AUDNZD-OTC", "AVA_USD":"AVAUSD-OTC",
        "AXS_USD":"AXSUSD-OTC", "BCH_USD":"BCHUSD-OTC", "BNB_USD":"BNBUSD-OTC",
        "BRL_USD":"BRLUSD-OTC", "BTC_USD":"BTCUSD-OTC", "CAD_CHF":"CADCHF-OTC",
        "DAS_USD":"DASUSD-OTC", "DOT_USD":"DOTUSD-OTC", "ETC_USD":"ETCUSD-OTC",
        "ETH_USD":"ETHUSD-OTC", "EUR_NZD":"EURNZD-OTC", "GBP_NZD":"GBPNZD-OTC",
        "LIN_USD":"LINUSD-OTC", "LTC_USD":"LTCUSD-OTC", "NZD_CAD":"NZDCAD-OTC",
        "NZD_CHF":"NZDCHF-OTC", "NZD_JPY":"NZDJPY-OTC", "NZD_USD":"NZDUSD-OTC",
        "SOL_USD":"SOLUSD-OTC", "TON_USD":"TONUSD-OTC", "TRU_USD":"TRUUSD-OTC",
        "UKBRENT":"UKBrent-OTC", "USCRUDE":"USCrude-OTC", "USD_ARS":"USDARS-OTC",
        "USD_BDT":"USDBDT-OTC", "USD_COP":"USDCOP-OTC", "USD_DZD":"USDDZD-OTC",
        "USD_EGP":"USDEGP-OTC", "USD_IDR":"USDIDR-OTC", "USD_INR":"USDINR-OTC",
        "USD_MXN":"USDMXN-OTC", "USD_NGN":"USDNGN-OTC", "USD_PHP":"USDPHP-OTC",
        "USD_PKR":"USDPKR-OTC", "USD_ZAR":"USDZAR-OTC", "XAG_USD":"XAGUSD-OTC",
        "XAU_USD":"XAUUSD-OTC", "XRP_USD":"XRPUSD-OTC", "ZEC_USD":"ZECUSD-OTC",
        "AXJ_AUD":"AXJAUD", "CHI_A50":"CHIA50", "F40_EUR":"F40EUR",
        "FTS_GBP":"FTSGBP", "HSI_HKD":"HSIHKD", "IBX_EUR":"IBXEUR",
        "JPX_JPY":"JPXJPY", "STX_EUR":"STXEUR"
    }
    def get_instrument(asset):
        return otc_map.get(asset, asset if asset.endswith("-OTC") else None)

    # Firebase auth
    token = firebase_signup_requests()

    # Date calculations
    now = datetime.now(timezone.utc)
    minutes = {"M1":1,"M2":2,"M4":4,"M5":5,"M15":15,"M30":30}[timeframe]
    def floor_time(dt, mins):
        total = dt.hour*60 + dt.minute
        rounded = (total // mins) * mins
        return dt.replace(hour=rounded//60, minute=rounded%60, second=0, microsecond=0)
    def skip_weekends(dt, days_back):
        result = dt
        count = 0
        while count < days_back:
            result -= timedelta(days=1)
            if result.weekday() < 5:
                count += 1
        return result
    to_date = floor_time(now - timedelta(days=1), minutes)
    to_date -= timedelta(seconds=1)
    past = skip_weekends(now, days)
    from_str = past.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_signals = []
    for asset in assets:
        instr = get_instrument(asset)
        if not instr:
            continue
        dirs = ["CALL", "PUT"] if direction == "BOTH" else [direction]
        for d in dirs:
            groups = sio_search(instr, timeframe, from_str, to_str, d, 1, 100, utc_offset, token)
            for group in groups:
                all_signals.append({
                    "pair": asset,
                    "time": group["time"],
                    "direction": d,
                    "winrate": group.get("winrate", 100)
                })
        time.sleep(1)

    # ---- TIME FILTER (start_time / end_time) ----
    sh, sm = map(int, start_time.split(":"))
    eh, em = map(int, end_time.split(":"))
    start_min = sh*60 + sm
    end_min = eh*60 + em
    def time_in_range(t):
        hh, mm = map(int, t.split(":"))
        tm = hh*60 + mm
        if start_min <= end_min:
            return start_min <= tm <= end_min
        else:
            return tm >= start_min or tm <= end_min   # wrap midnight
    filtered_signals = [s for s in all_signals if time_in_range(s["time"])]
    # -----------------------------------------

    # Convert to output format
    result = []
    for s in filtered_signals:
        otc_pair = get_instrument(s["pair"]) or s["pair"].replace("_", "")
        result.append(f"M1;{otc_pair};{s['time']};{s['direction']}")

    # Remove duplicates
    seen = set()
    unique = []
    for sig in result:
        if sig not in seen:
            seen.add(sig)
            unique.append(sig)

    unique.sort(key=lambda x: x.split(";")[2])
    return unique

async def send_blackout_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    context.user_data['state'] = STATE_BLACKOUT_CHECKER_SIGNALS
    msg = (
        "🔮  𝙱𝙻𝙰𝙲𝙺𝙾𝚄𝚃 𝙲𝙷𝙴𝙲𝙺𝙴𝚁\n\n"
        "    𝙿𝚊𝚜𝚝𝚎 𝚢𝚘𝚞𝚛 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚋𝚎𝚕𝚘𝚠 (𝚘𝚗𝚎 𝚙𝚎𝚛 𝚕𝚒𝚗𝚎)\n"
        "📋 𝙵𝚘𝚛𝚖𝚊𝚝: 𝙼𝟷;𝙿𝙰𝙸𝚁;𝙷𝙷:𝙼𝙼  (𝚊𝚗𝚢 𝚏𝚕𝚎𝚡𝚒𝚋𝚕𝚎 𝚏𝚘𝚛𝚖𝚊𝚝)\n"
        "📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎:\n"
        "   𝙼𝟷;𝙶𝙱𝙿𝙹𝙿𝚈-𝙾𝚃𝙲;08:24\n"
        "   𝙼𝟷;𝙴𝚄𝚁𝚄𝚂𝙳-𝙾𝚃𝙲;09:15\n\n"
        "⏰ 𝚄𝚜𝚎 𝚄𝚃𝙲+𝟻 𝚝𝚒𝚖𝚎\n"
        "📌 𝙿𝚊𝚜𝚝𝚎 𝚢𝚘𝚞𝚛 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚗𝚘𝚠..."
    )
    entities = build_custom_emoji_entities(msg)
    await context.bot.send_message(chat_id=uid, text=msg, entities=entities)


async def strategy_callback(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    await query.answer()
    data = query.data
    strat = int(data.split("_")[1])
    st = get_state(uid)
    st.strategy = strat
    context.user_data['uid'] = uid
    context.user_data['strategy_active'] = True
    context.user_data['selected_strategy'] = strat
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(
        "✅ Yes", callback_data="mm_yes"), InlineKeyboardButton("❌ No", callback_data="mm_no")]])
    text = f"💎 𝚂𝙼𝚉𝚇 𝙼𝙾𝙽𝙴𝚈 𝙼𝙰𝙽𝙰𝙶𝙴𝙼𝙴𝙽𝚃\n\n🔰 Enable Money Management for ST{strat}?\n📊 Track balance, TP, SL & smart martingale\n💪 Auto-calculate trade amounts\n\n🤖 Choose below:"
    entities = build_custom_emoji_entities(text)
    await query.message.reply_text(text, entities=entities, reply_markup=kb)
    return STATE_MM_PROMPT


async def mm_prompt_callback(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    await query.answer()
    data = query.data
    strat = context.user_data.get('selected_strategy', 1)
    st = get_state(uid)
    if data == "mm_yes":
        st.mm_enabled = True
        text = "💲 Enter your account balance (e.g. 100):"
        entities = build_custom_emoji_entities(text)
        await query.edit_message_text(text, entities=entities)
        return STATE_MM_BALANCE
    else:
        st.mm_enabled = False
        text = f"✅ MM disabled. Proceeding with Strategy {strat}..."
        entities = build_custom_emoji_entities(text)
        await query.edit_message_text(text, entities=entities)
        # Proceed to strategy specific flow
        return await _proceed_with_strategy(query, context, strat, uid, st)


async def _proceed_with_strategy(query, context, strat, uid, st):

    pass

    if strat == 1:
        text = "✅ Strategy 1 selected. Scanning..."
        await query.message.reply_text(text)
        bot = SMZXBot(uid)
        threading.Thread(target=bot.run_single_signal, daemon=True).start()
        context.user_data['strategy_active'] = False
        return ConversationHandler.END
    elif strat == 2:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes", callback_data="s2_filters_yes"),
            InlineKeyboardButton("❌ No", callback_data="s2_filters_no")
        ]])
        await query.message.reply_text("🔰 Strategy 2: Enable additional filters?", reply_markup=kb)
        return S2_FILTER_CHOICE
    elif strat == 3:
        text = "✅ Strategy 3 selected. Enter min accuracy % (50-100):"
        await query.message.reply_text(text)
        return S3_ACCURACY
    elif strat == 4:
        text = "✅ Strategy 4 selected. Enter min accuracy % (50-100):"
        await query.message.reply_text(text)
        return S4_ACCURACY
    elif strat == 5:
        text = "✅ Strategy 5 selected. Enter min score (50-100):"
        await query.message.reply_text(text)
        return S5_SCORE
    elif strat == 6:
        text = "✅ Strategy 6 selected. Enter minimum confluence score (70‑100):"
        await query.message.reply_text(text)
        return S6_SCORE
    return ConversationHandler.END

# ----- Money Management input handlers -----


async def mm_balance_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st = get_state(uid)
    text = update.message.text.strip().replace("$", "").replace(",", "")
    try:
        balance = float(text)
        if balance < 1:
            msg = "❌ Balance must be at least $1. Enter again:"
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
            return STATE_MM_BALANCE
        st.mm_balance = balance
        st.mm_current_balance = balance
        msg = "🏆 𝙴𝚗𝚝𝚎𝚛 𝚢𝚘𝚞𝚛 𝚍𝚊𝚒𝚕𝚢 𝚃𝚊𝚔𝚎 𝙿𝚛𝚘𝚏𝚒𝚝 𝚝𝚊𝚛𝚐𝚎𝚝 (𝚎.𝚐., 𝟷𝟻):"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities)
        return STATE_MM_TP
    except ValueError:
        msg = "❌ Invalid number. Enter your balance (e.g. 100):"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities)
        return STATE_MM_BALANCE


async def mm_tp_received(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    uid = update.effective_user.id
    st = get_state(uid)
    text = update.message.text.strip().replace("$", "").replace(",", "")
    try:
        tp = float(text)
        if tp <= 0:
            msg = "❌ TP must be positive. Enter again:"
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
            return STATE_MM_TP
        st.mm_tp = tp
        msg = "🔰 𝙴𝚗𝚝𝚎𝚛 𝚢𝚘𝚞𝚛 𝚍𝚊𝚒𝚕𝚢 𝚂𝚝𝚘𝚙 𝙻𝚘𝚜𝚜 𝚕𝚒𝚖𝚒𝚝 (𝚎.𝚐., 𝟾):"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities)
        return STATE_MM_SL
    except ValueError:
        msg = "❌ Invalid number. Enter your TP target (e.g. 15):"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities)
        return STATE_MM_TP


async def mm_sl_received(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    """Handle SL input for MM — then proceed to strategy."""
    uid = update.effective_user.id
    st = get_state(uid)
    strat = context.user_data.get('selected_strategy', 1)
    text = update.message.text.strip().replace("$", "").replace(",", "")
    try:
        sl = float(text)
        if sl <= 0:
            sender.send_message(uid, "❌ SL must be positive. Enter again:")
            return STATE_MM_SL
        st.mm_sl = sl
        st.mm_pnl = 0.0
        st.mm_consecutive_losses = 0
        # ✅ FIX: pass two arguments (balance and sl)
        st.mm_base_amount = mm_calculate_base_amount(st.mm_balance, st.mm_sl)
        trade_amt = mm_get_trade_amount(st)
        max_steps = 3
        summary = (
            f"💎 𝚂𝙼𝚉𝚇 𝙼𝙼 𝙰𝙲𝚃𝙸𝚅𝙰𝚃𝙴𝙳\n"
            f"┏───♡─────────── ⊹˚───┓\n"
            f"💲 Balance∶— ${st.mm_balance:.2f}\n"
            f"🏆 TP Target∶— ${st.mm_tp:.2f}\n"
            f"🔰 SL Limit∶— ${st.mm_sl:.2f}\n"
            f"💪 Trade Amount∶— ${trade_amt:.2f}\n"
            f"📊 Max MTG Steps∶— {max_steps}\n"
            f"🔥 Risk per signal∶— ${trade_amt * 7:.2f} max\n"
            f"┗───˚⊹ ─────────♡───┛\n\n"
            f"✅ Proceeding with Strategy {strat}...\n"
            f"✨ ©OWNER @Rohailtrader ✨"
        )
        sender.send_message(uid, summary)
        # Now proceed with the original strategy flow
        if strat == 1:
            bot = SMZXBot(uid)
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        elif strat == 2:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes", callback_data="s2_filters_yes"),
                InlineKeyboardButton("❌ No", callback_data="s2_filters_no")
            ]])
            await update.message.reply_text("🔰 Strategy 2: Enable additional filters?", reply_markup=kb)
            return S2_FILTER_CHOICE
        elif strat == 3:
            sender.send_message(
                uid, "✅ Strategy 3 — Enter min accuracy % (50-100):")
            return S3_ACCURACY
        elif strat == 4:
            sender.send_message(
                uid, "✅ Strategy 4 — Enter min accuracy % (50-100):")
            return S4_ACCURACY
        elif strat == 5:
            sender.send_message(
                uid, "✅ Strategy 5 — Enter min score (50-100):")
            return S5_SCORE
        elif strat == 6:
            sender.send_message(
                uid, "✅ Strategy 6 — Enter minimum confluence score (70‑100):")
            return S6_SCORE
        return ConversationHandler.END
    except ValueError:
        sender.send_message(
            uid, "❌ Invalid number. Enter your SL limit (e.g. 8):")
        return STATE_MM_SL

# ----- Strategy 2 filter handlers -----


def build_s2_filter_message(filters):

    pass

    def status(x): return "✅" if x else "❌"
    text = f"🎯 Toggle filters:\n\n{
        status(
            filters.use_trend)} Trend\n{
        status(
            filters.use_bollinger)} Bollinger\n{
        status(
            filters.use_support_resistance)} S/R\n{
        status(
            filters.use_price_action)} Price Action\n{
        status(
            filters.use_supertrend)} Supertrend\n{
        status(
            filters.use_fvg)} FVG\n{
        status(
            filters.use_trend_reverse)} Trend Reverse\n\nTap a filter to toggle, then 'Done'."
    buttons = [
        [InlineKeyboardButton(f"{status(filters.use_trend)} Trend", callback_data="s2_trend")],
        [InlineKeyboardButton(f"{status(filters.use_bollinger)} Bollinger", callback_data="s2_bb")],
        [InlineKeyboardButton(f"{status(filters.use_support_resistance)} S/R", callback_data="s2_sr")],
        [InlineKeyboardButton(f"{status(filters.use_price_action)} Price Action", callback_data="s2_pa")],
        [InlineKeyboardButton(f"{status(filters.use_supertrend)} Supertrend", callback_data="s2_st")],
        [InlineKeyboardButton(f"{status(filters.use_fvg)} FVG", callback_data="s2_fvg")],
        [InlineKeyboardButton(f"{status(filters.use_trend_reverse)} Trend Reverse", callback_data="s2_tr")],
        [InlineKeyboardButton("✅ Done", callback_data="s2_done")],
    ]
    return text, InlineKeyboardMarkup(buttons)


async def s2_filter_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    query = update.callback_query
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return ConversationHandler.END
    await query.answer()
    data = query.data
    uid = context.user_data['uid']
    st = get_state(uid)
    if data == "s2_filters_no":
        st.strategy2_filters = Strategy2Filters()
        await query.edit_message_text("✅ Filters disabled. Enter min accuracy (50-100):")
        return S2_ACCURACY
    else:
        filters = Strategy2Filters()
        context.user_data['filters'] = filters
        text, markup = build_s2_filter_message(filters)
        await query.edit_message_text(text, reply_markup=markup)
        return S2_FILTER_TOGGLE


async def s2_filter_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    query = update.callback_query
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return ConversationHandler.END
    await query.answer()
    data = query.data
    filters = context.user_data['filters']
    toggle_map = {
        "s2_trend": "use_trend",
        "s2_bb": "use_bollinger",
        "s2_sr": "use_support_resistance",
        "s2_pa": "use_price_action",
        "s2_st": "use_supertrend",
        "s2_fvg": "use_fvg",
        "s2_tr": "use_trend_reverse"}
    if data in toggle_map:
        attr = toggle_map[data]
        setattr(filters, attr, not getattr(filters, attr))
        text, markup = build_s2_filter_message(filters)
        await query.edit_message_text(text, reply_markup=markup)
        return S2_FILTER_TOGGLE
    elif data == "s2_done":
        get_state(uid).strategy2_filters = filters
        await query.edit_message_text("✅ Filters saved. Enter min accuracy (50-100):")
        return S2_ACCURACY


async def s2_accuracy_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return ConversationHandler.END
    st = get_state(uid)
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 50 <= val <= 100:
            st.strategy2_filters.min_accuracy = val
            sender.send_message(
                uid, f"✅ Min accuracy set to {val}%.\nStarting analysis...")
            bot = SMZXBot(uid)
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 50-100:")
            return S2_ACCURACY
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S2_ACCURACY


async def s3_accuracy_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return ConversationHandler.END
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 50 <= val <= 100:
            get_state(uid).strategy3_min_accuracy = val
            await update.message.reply_text("Enter lookback period (10-30):")
            return S3_LOOKBACK
        else:
            await update.message.reply_text("❌ Enter between 50-100:")
            return S3_ACCURACY
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S3_ACCURACY


async def s3_lookback_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return ConversationHandler.END
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 10 <= val <= 30:
            get_state(uid).strategy3_lookback = val
            sender.send_message(
                uid, f"✅ Lookback set to {val}. Starting analysis...")
            bot = SMZXBot(uid)
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 10-30:")
            return S3_LOOKBACK
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Enter a number.")
        return S3_LOOKBACK


async def s4_accuracy_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return ConversationHandler.END
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 50 <= val <= 100:
            get_state(uid).strategy4_min_accuracy = val
            sender.send_message(uid, f"✅ Accuracy set. Starting analysis...")
            bot = SMZXBot(uid)
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter 50-100:")
            return S4_ACCURACY
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S4_ACCURACY


async def s5_score_received(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return ConversationHandler.END
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 50 <= val <= 100:
            get_state(uid).strategy5_min_score = val
            sender.send_message(uid, f"✅ Score set. Starting analysis...")
            bot = SMZXBot(uid)
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter 50-100:")
            return S5_SCORE
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S5_SCORE

# ----- Checker date callback -----


async def checker_date_callback(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    data = query.data
    if data == "checker_today":
        context.user_data['checker_date'] = datetime.now(
            timezone.utc).strftime("%Y-%m-%d")
    elif data == "checker_yesterday":
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        context.user_data['checker_date'] = yesterday.strftime("%Y-%m-%d")
    elif data == "checker_custom":
        context.user_data['state'] = STATE_CHECKER_CUSTOM_DATE
        text = "📅 Enter the date (YYYY-MM-DD):"
        await query.edit_message_text(text)
        return
    context.user_data['state'] = STATE_CHECKER_SIGNALS
    text = (
        "🔮  𝚂𝙸𝙶𝙽𝙰𝙻 𝙲𝙷𝙴𝙲𝙺𝙴𝚁\n\n"
        "    Paste your signals below (one per line)\n"
        "📋 Format: M1;PAIR;HH:MM;DIRECTION\n"
        "📝 Example:\n"
        "   M1;GBPJPY-OTC;08:24;CALL\n"
        "   M1;EURUSD-OTC;09:15;PUT\n\n"
        "⏰ Use UTC+5 time\n"
        "📌 Paste your signals now..."
    )
    entities = build_custom_emoji_entities(text)
    await query.edit_message_text(text, entities=entities)

# ----- Continue and Stop commands -----


async def continue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
    st = get_state(uid)
    if st.running:
        await update.message.reply_text("Already running a signal. Wait for it to finish.")
        return
    if st.ai_mode:
        threading.Thread(target=run_ai_mode, args=(uid,), daemon=True).start()
        sender.send_message(
            uid, "🤖 AI Mode — Scanning for next best signal...")
    else:
        bot = SMZXBot(uid)
        threading.Thread(target=bot.run_single_signal, daemon=True).start()
        sender.send_message(uid, "Continuing with next signal...")


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
    st = get_state(uid)
    st.stop_requested = True
    st.running = False
    st.ai_mode = False
    st.stats = {"wins": 0, "losses": 0}
    st.signal_history = []
    st.mm_enabled = False
    st.mm_pnl = 0.0
    st.mm_win_streak = 0
    st.mm_loss_streak = 0
    st.ai_min_consensus = 2
    st.ai_required_strategies = []
    sender.send_message(
        uid, "🤖 Stopping. Returning to main menu. Use /start to see options.")

    # Also stop auto trade if running
    at = get_auto_trader(uid)
    at.running = False

# ══════════════ AUTO TRADE HANDLERS (high-accuracy, WS-only) ══════════════

STRATEGY_NAMES_AUTO = {
    1: "RSI basic", 2: "EMA filtered", 3: "WR divergence",
    4: "ADX stochastic", 5: "Ultra accurate", 6: "IROF pro"
}

AUTO_MAX_PAIRS = 24         # how many high-payout OTC pairs to scan each minute
# seconds a pair is skipped after it was just traded (no back-to-back repeats)
AUTO_PAIR_COOLDOWN = 240
_UTC5 = timezone(timedelta(hours=5))


def _auto_int(text, lo, hi):

    pass

    """Parse a bounded integer from user text (reuses clean_int_input)."""
    try:
        v = int(clean_int_input(text))
        if lo <= v <= hi:
            return v
    except Exception:
        pass
    return None


def _auto_tp_prompt_body():

    pass

    return f"💲 Enter Take Profit (TP) in $  (e.g. 10):"


def _auto_run_strategy(strategy_id, candles, st):

    pass

    """Run a strategy with the SAME parameters as the main-menu signal mode
    (SMZXBot.analyze). Returns (direction, entry_dt, conf)."""
    try:
        filters = st.strategy2_filters if st.strategy2_filters else Strategy2Filters()
        if strategy_id == 1:
            return analyze_strategy1(candles, 75)
        if strategy_id == 2:
            return analyze_strategy2(candles, filters)
        if strategy_id == 3:
            return analyze_strategy3(
                candles,
                st.strategy3_min_accuracy,
                st.strategy3_lookback)
        if strategy_id == 4:
            return analyze_strategy4(candles, st.strategy4_min_accuracy)
        if strategy_id == 5:
            return analyze_strategy5(candles, st.strategy5_min_score)
        if strategy_id == 6:
            return analyze_strategy6(
                candles,
                st.strategy6_min_score,
                st.strategy6_min_candles)
        return analyze_strategy3(candles, 75, 20)
    except Exception:
        pass
    return None, None, None


def _auto_select_pairs(client, limit=AUTO_MAX_PAIRS):

    pass

    """Pick the highest-payout, currently-open OTC pairs from the live instrument list."""
    scored = []
    for inst in (client.instruments or []):
        try:
            if not inst.get("isOTC"):
                continue
            if inst.get("isOpen") is False:
                continue
            payout = inst.get("effectiveTurboPayoutRate") or inst.get(
                "turboPayoutRate") or 0
            sym = inst.get("symbol")
            if sym and payout >= 0.80:
                scored.append((payout, sym))
        except Exception:
            continue
    scored.sort(key=lambda x: x[0], reverse=True)
    pairs = [s for _, s in scored[:limit]]
    return pairs or list(AUTO_DEFAULT_OTC_PAIRS)


def _auto_acct_balance(trader):

    pass

    """Current balance for the selected account (demo / real)."""
    try:
        b = trader.client.balance or {}
        v = b.get("demoBalance") if trader.is_demo else b.get("realBalance")
        if v is None:
            v = b.get("currentBalance")
        if v is None:
            rb = trader.client.get_balance() or {}
            v = (rb.get("demoBalance") if trader.is_demo else rb.get("realBalance"))
            if v is None:
                v = rb.get("currentBalance")
        return float(v or 0)
    except Exception:
        return float(trader.balance or 0)


def _auto_classify(res, side):

    pass

    """Decode a broker tradeResult into 'win' | 'loss' | 'tie'.

    Confirmed from LIVE demo data, the broker is inconsistent: the `result`
    field is sometimes a number and sometimes a string, e.g.
        {"result": 1,      "profit":  0.92}   -> win
        {"result": 2,      "profit": -1}      -> loss   (NOT a tie!)
        {"result": "loss", "profit": -1}      -> loss
    `profit` is always present and unambiguous, so we trust it first:
        profit > 0 -> win,  profit < 0 -> loss,  profit == 0 -> tie (refund).
    """
    # 1) profit is the most reliable signal
    try:
        profit = float(res.get("profit"))
    except (TypeError, ValueError):
        profit = None
    if profit is not None:
        if profit > 0:
            return "win"
        if profit < 0:
            return "loss"
        return "tie"

    # 2) explicit result field (string or numeric)
    rc = res.get("result")
    if isinstance(rc, str):
        s = rc.strip().lower()
        if s in ("win", "won"):
            return "win"
        if s in ("loss", "lose", "lost"):
            return "loss"
        if s in ("tie", "draw", "equal", "refund"):
            return "tie"
    if rc == 1:
        return "win"
    if rc == 2:                       # numeric 2 == LOSS on this broker
        return "loss"

    # 3) price-based fallback (flat == loss on this broker)
    op = res.get("openPrice")
    cp = res.get("closePrice")
    if op is not None and cp is not None:
        if cp > op:
            return "win" if side == "call" else "loss"
        if cp < op:
            return "win" if side == "put" else "loss"
    return "loss"


def _auto_signal_card(
        trader,
        pair,
        direction,
        conf,
        entry_label,
        expiry_label,
        amount):
    arrow = "📉" if direction == "CALL" else "📈"
    dword = "UP  /  CALL" if direction == "CALL" else "DOWN  /  PUT"
    title = fancy_font("SMZX AUTO TRADE")
    return (
        f"👑 {title} 👑\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Pair      : {pair}\n"
        f"{arrow} Signal    : {dword}\n"
        f"⏰ Entry     : {entry_label}\n"
        f"🕐 Expiry    : {expiry_label}\n"
        f"💲 Amount    : ${amount:.2f}  ({trader.risk_percent:.1f}%)\n"
        f"🔰 Accuracy  : {conf:.0f}%\n"
        f"🤖 Strategy  : {trader.strategy}. {trader.strategy_name}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Placing at entry time...\n"
        f"✨ Powered by SMZX ✨"
    )


def auto_trade_loop(trader, context):

    pass

    """High-accuracy auto trade loop — TradoWix WebSocket only (candles + execution)."""
    import time as _t
    import asyncio as _aio

    loop = _aio.new_event_loop()
    _aio.set_event_loop(loop)
    client = trader.client

    # ----- trade-result correlation via WS callbacks -----
    trader._results = {}
    trader._last_opened = None
    trader._opened_evt = threading.Event()

    def _on_open(data):
        trader._last_opened = data
        trader._opened_evt.set()

    def _on_res(data):
        tid = data.get("tradeId") or data.get("id")
        if tid:
            trader._results[tid] = data

    client.on_trade_opened(_on_open)
    client.on_trade_result(_on_res)

    def fire(pair, side, amount):
        """Put the placeTrade message on the wire RIGHT NOW (fast WS send, no wait).
        Returns None on success or an error string."""
        trader._opened_evt.clear()
        try:
            client.place_trade(
                pair,
                side,
                amount,
                duration_minutes=1,
                is_demo=trader.is_demo)
            return None
        except Exception as e:
            return f"place failed: {e}"

    def confirm_opened():
        """Wait for the broker's tradeOpened (run this in an executor)."""
        if not trader._opened_evt.wait(timeout=8):
            return {"error": "no tradeOpened"}
        opened = dict(trader._last_opened or {})
        tid = opened.get("id") or opened.get("tradeId")
        if not tid:
            return {"error": "no trade id"}
        return opened

    def wait_result(tid):
        """Block until the broker settles trade `tid`."""
        deadline = _t.time() + 240
        while _t.time() < deadline:
            if tid in trader._results:
                return trader._results.pop(tid)
            _t.sleep(0.1)
        return {"error": "result timeout"}

    # NOTE: this loop runs in its OWN event loop on a background thread. We must
    # NOT touch `context.bot` here — its httpx client is bound to the main event
    # loop, and using it from this loop (then closing this loop) corrupts the main
    # bot (causing "Event loop is closed" on the next /start). We use the Telethon
    # `sender` instead (thread-safe via run_coroutine_threadsafe), run in an
    # executor so we never block this loop on network I/O.
    def _send_sync(msg):
        try:
            return sender.send_message(trader.uid, msg)
        except Exception:
            return None

    def _edit_sync(mid, msg):
        try:
            sender.edit_message(trader.uid, mid, msg)
            return True
        except Exception:
            return False

    async def send(msg):
        try:
            await loop.run_in_executor(None, _send_sync, msg)
        except Exception:
            pass

    async def sleep_until(ts):
        # coarse async sleep until ~30ms out, then a tight spin for precise
        # firing
        while True:
            d = ts - _t.time()
            if d <= 0.03:
                break
            await _aio.sleep(min(d - 0.02, 0.2))
        while _t.time() < ts:
            pass

    # editable "live" status line (scanning heartbeat) so we don't flood the
    # chat
    _status = {"id": None}

    async def status(msg):
        try:
            if _status["id"] is not None:
                ok = await loop.run_in_executor(None, _edit_sync, _status["id"], msg)
                if ok:
                    return
            m = await loop.run_in_executor(None, _send_sync, msg)
            _status["id"] = getattr(m, "id", None)
        except Exception:
            pass

    def reset_status():
        _status["id"] = None

    async def episode_pause(seconds=32):
        """After a completed signal/trade episode, pause before scanning again."""
        if not trader.running:
            return
        reset_status()
        target = _t.time() + seconds
        while trader.running and _t.time() < target:
            left = int(round(target - _t.time()))
            await status(
                f"⏳ {fancy_font('PAUSED')}\n"
                f"🤖 {trader.strategy}. {trader.strategy_name}\n"
                f"🚀 Resuming scan in {left}s..."
            )
            await _aio.sleep(2)
        reset_status()

    async def run():
        # SAME config as the main-menu signal mode
        st = get_state(trader.uid)
        st.strategy = trader.strategy
        trader._loss_cooldown = {}            # pair -> epoch until which we skip it
        trader.pairs = _auto_select_pairs(client)
        for p in trader.pairs:
            try:
                client.subscribe(p, lookback_minutes=240, timeframe=60)
            except Exception:
                pass
        await send(
            f"🚀 {fancy_font('AUTO TRADE STARTED')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💎 Balance   : ${trader.balance:.2f}\n"
            f"🔰 TP : ${trader.tp_target:.2f}    🔰 SL : ${trader.sl_target:.2f}\n"
            f"💲 Risk      : {trader.risk_percent:.1f}% / trade\n"
            f"🔥 Martingale: {'ON (1-step)' if trader.mtg_enabled else 'OFF'}\n"
            f"🤖 Strategy  : {trader.strategy}. {trader.strategy_name}\n"
            f"🔍 Scanning  : {len(trader.pairs)} OTC pairs\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Waiting for first signal... (/stop to stop)"
        )
        await _aio.sleep(6)  # let candle history load

        trader.balance = _auto_acct_balance(trader)
        trader.starting_balance = trader.balance

        while trader.running:
            # per-iteration guard: one user's transient error must never kill the
            # session (and each user runs on its OWN thread/loop/client →
            # isolated)
            try:
                # ---- PAUSE: hold here (no scanning, no trading) until resumed ----
                if getattr(trader, "paused", False):
                    reset_status()
                    # notify ONCE (no repeating spam), then wait quietly
                    await send(
                        f"⏳ {fancy_font('PAUSED')}\n"
                        f"🚀 Tap Resume to continue scanning."
                    )
                    while trader.running and getattr(trader, "paused", False):
                        await _aio.sleep(1)
                    if not trader.running:
                        break
                    await send(f"🚀 {fancy_font('RESUMED')}\nScanning again...")
                    reset_status()

                # ---- AUTO-RECONNECT: wait out a dropped WebSocket before scanning ----
                if not getattr(client, "_authenticated", True):
                    reset_status()
                    # notify ONCE, then wait quietly for the link to come back
                    await send(
                        f"🚨 {fancy_font('RECONNECTING')}\n"
                        f"🔍 Lost broker link — restoring connection..."
                    )
                    for _ in range(60):
                        if not trader.running or getattr(
                                client, "_authenticated", False):
                            break
                        await _aio.sleep(1)
                    if not trader.running:
                        break
                    if getattr(client, "_authenticated", False):
                        await send(f"✅ {fancy_font('RECONNECTED')}\nResuming scan...")
                    else:
                        continue  # still down → retry the guard

                # ---- TP / SL check ----
                trader.balance = _auto_acct_balance(trader)
                pnl = trader.balance - trader.starting_balance
                if pnl > getattr(trader, "peak_pnl", 0.0):
                    trader.peak_pnl = pnl
                if trader.tp_target > 0 and pnl >= trader.tp_target:
                    await send(
                        f"🏆 {fancy_font('TARGET REACHED')}\n"
                        f"💎 ${trader.starting_balance:.2f} → ${trader.balance:.2f}\n"
                        f"📈 Profit : +${pnl:.2f}\n"
                        f"📊 {trader.win_count}W / {trader.loss_count}L"
                    )
                    trader.running = False
                    break
                if trader.sl_target > 0 and pnl <= -trader.sl_target:
                    await send(
                        f"🔰 {fancy_font('STOP LOSS HIT')}\n"
                        f"💎 ${trader.starting_balance:.2f} → ${trader.balance:.2f}\n"
                        f"📉 Loss : -${abs(pnl):.2f}\n"
                        f"📊 {trader.win_count}W / {trader.loss_count}L"
                    )
                    trader.running = False
                    break

                # ---- pre-boundary SCAN so we can place EXACTLY at the boundary (zero delay) ----
                now = _t.time()
                boundary = (int(now // 60) + 1) * 60
                # wake 3s before the candle opens
                await sleep_until(boundary - 3)
                if not trader.running:
                    break

                entry_label = datetime.fromtimestamp(
                    boundary, tz=_UTC5).strftime("%H:%M")
                await status(
                    f"🔍 {fancy_font('SCANNING')}\n"
                    f"🤖 {trader.strategy}. {trader.strategy_name}\n"
                    f"📊 {len(trader.pairs)} OTC pairs\n"
                    f"⏰ Next entry : {entry_label}"
                )

                # menu-style: take the FIRST pair that fires a signal (same as
                # signal mode)
                chosen = None
                for pair in trader.pairs:
                    if not trader.running:
                        break
                    if trader._loss_cooldown.get(pair, 0) > _t.time():
                        continue
                    try:
                        candles = client.get_candles(
                            pair, timeframe=60, count=210, lookback_minutes=240, timeout=1.0)
                    except Exception:
                        candles = []
                    if not candles or len(candles) < 30:
                        continue
                    direction, _edt, conf = _auto_run_strategy(
                        trader.strategy, candles, st)
                    if direction:
                        chosen = (pair, direction, conf or 0)
                        break

                if not chosen:
                    await status(
                        f"🔍 {fancy_font('NO SIGNAL')}\n"
                        f"🤖 {trader.strategy}. {trader.strategy_name}\n"
                        f"⏰ {entry_label} — waiting next candle..."
                    )
                    await sleep_until(boundary + 1.0)
                    continue

                pair, direction, conf = chosen
                # mark this pair so it is NOT picked again back-to-back (no repeated
                # signals on the same pair); it becomes eligible again after
                # cooldown
                trader._loss_cooldown[pair] = _t.time() + AUTO_PAIR_COOLDOWN
                side = "call" if direction == "CALL" else "put"
                entry_label = datetime.fromtimestamp(
                    boundary, tz=_UTC5).strftime("%H:%M:%S")
                expiry_label = datetime.fromtimestamp(
                    boundary + 60, tz=_UTC5).strftime("%H:%M:%S")
                base_amt = round(
                    max(1.0, trader.balance * trader.risk_percent / 100.0), 2)

                reset_status()

                # fire EXACTLY at the boundary with minimal latency: put the order on the
                # wire INLINE (microseconds) the instant the candle opens — announce and
                # confirm AFTER, so Telegram I/O never delays the entry.
                await sleep_until(boundary)
                place_err = fire(pair, side, base_amt)
                trader.trade_count += 1
                await send(_auto_signal_card(trader, pair, direction, conf, entry_label, expiry_label, base_amt))
                if place_err:
                    await send(f"⚠️ Trade error: {place_err}")
                    await episode_pause()
                    continue
                opened = await loop.run_in_executor(None, confirm_opened)
                if opened.get("error"):
                    await send(f"⚠️ Trade error: {opened['error']}")
                    await episode_pause()
                    continue
                tid = opened.get("id") or opened.get("tradeId")
                await send(
                    f"✅ {fancy_font('TRADE OPENED')}\n"
                    f"📊 {pair}   {'UP/CALL' if side == 'call' else 'DOWN/PUT'}\n"
                    f"💲 ${base_amt:.2f}   ⏰ {entry_label} → {expiry_label}\n"
                    f"⏳ Waiting for candle to close..."
                )
                res = await loop.run_in_executor(None, wait_result, tid)
                if res.get("error"):
                    await send(f"⚠️ Result error: {res['error']}")
                    await episode_pause()
                    continue

                outcome = _auto_classify(res, side)

                if outcome != "loss":
                    # win / tie → refresh real balance (credit lands via
                    # balanceUpdate)
                    await _aio.sleep(0.8)
                    trader.balance = _auto_acct_balance(trader)
                    pnl = trader.balance - trader.starting_balance
                    if outcome == "win":
                        trader.win_count += 1
                        trader.win_streak += 1
                        trader.loss_streak = 0
                        profit = float(
                            res.get("profit") or round(
                                base_amt * 0.9, 2))
                        await send(
                            f"✅ {fancy_font('WIN')}  +${abs(profit):.2f}\n"
                            f"💎 Balance : ${trader.balance:.2f}\n"
                            f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                        )
                    else:
                        trader.tie_count += 1
                        await send(
                            f"🔅 {fancy_font('TIE')}  (refund)\n"
                            f"💎 Balance : ${trader.balance:.2f}\n"
                            f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                        )
                    await episode_pause()
                    continue

                # ---- LOSS (stake already deducted → balance is final) ----
                trader.balance = _auto_acct_balance(trader)
                pnl = trader.balance - trader.starting_balance

                if not trader.mtg_enabled:
                    # no recovery step → this IS the episode loss: count +
                    # announce it
                    trader.loss_count += 1
                    trader.loss_streak += 1
                    trader.win_streak = 0
                    await send(
                        f"❌ {fancy_font('LOSS')}  -${base_amt:.2f}\n"
                        f"💎 Balance : ${trader.balance:.2f}\n"
                        f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                    )
                    # respect a short per-pair cooldown after a loss (same as
                    # menu)
                    trader._loss_cooldown[pair] = _t.time() + 180
                    await episode_pause()
                    continue

                # MTG ON → skip the base-loss message entirely and go STRAIGHT to the
                # Martingale step. The episode loss is counted ONCE, only if the MTG
                # step also fails (so an MTG-recovered episode is not counted
                # as a loss).

                # ---- ZERO-DELAY 1-STEP MARTINGALE (same side, double, on the candle
                #      immediately after the entry candle). The base result only arrives
                #      after the entry candle [boundary, boundary+60] closes, so we are
                # already inside the next candle → place RIGHT NOW, no delay.
                # ----
                mtg_amt = round(base_amt * 2, 2)
                # current (next) candle start
                mtg_candle = int(_t.time() // 60) * 60
                if mtg_candle < boundary + 60:
                    mtg_candle = boundary + 60
                # ensure the next candle has opened
                await sleep_until(mtg_candle)
                mtg_entry = datetime.fromtimestamp(
                    mtg_candle, tz=_UTC5).strftime("%H:%M:%S")
                mtg_expiry = datetime.fromtimestamp(
                    mtg_candle + 60, tz=_UTC5).strftime("%H:%M:%S")
                # fire INLINE at the candle open (zero delay), announce +
                # confirm after
                mtg_err = fire(pair, side, mtg_amt)
                trader.trade_count += 1
                await send(
                    f"🔥 {fancy_font('MARTINGALE')}  (Step 1)\n"
                    f"📊 {pair}   {'UP/CALL' if side == 'call' else 'DOWN/PUT'}\n"
                    f"💲 ${mtg_amt:.2f}  (2x)    ⏰ {mtg_entry} → {mtg_expiry}\n"
                    f"🔥 Zero-delay placed."
                )
                if mtg_err:
                    await send(f"⚠️ Martingale error: {mtg_err}")
                    await episode_pause()
                    continue
                opened2 = await loop.run_in_executor(None, confirm_opened)
                if opened2.get("error"):
                    await send(f"⚠️ Martingale error: {opened2['error']}")
                    await episode_pause()
                    continue
                tid2 = opened2.get("id") or opened2.get("tradeId")
                await send(
                    f"✅ {fancy_font('MTG OPENED')}\n"
                    f"📊 {pair}   {'UP/CALL' if side == 'call' else 'DOWN/PUT'}\n"
                    f"💲 ${mtg_amt:.2f}   ⏰ {mtg_entry} → {mtg_expiry}\n"
                    f"⏳ Waiting for candle to close..."
                )
                res2 = await loop.run_in_executor(None, wait_result, tid2)
                if res2.get("error"):
                    await send(f"⚠️ Martingale result error: {res2['error']}")
                    await episode_pause()
                    continue
                out2 = _auto_classify(res2, side)
                await _aio.sleep(0.8)
                trader.balance = _auto_acct_balance(trader)
                pnl = trader.balance - trader.starting_balance
                if out2 == "win":
                    trader.win_count += 1
                    trader.win_streak += 1
                    trader.loss_streak = 0
                    p2 = float(res2.get("profit") or round(mtg_amt * 0.9, 2))
                    await send(
                        f"✅ {fancy_font('MTG WIN')}  +${abs(p2):.2f}\n"
                        f"💎 Balance : ${trader.balance:.2f}\n"
                        f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                    )
                elif out2 == "tie":
                    # MTG refunded but the base candle still lost → net loss for the
                    # episode → count it ONCE as the episode loss
                    trader.loss_count += 1
                    trader.loss_streak += 1
                    trader.win_streak = 0
                    await send(
                        f"🔅 {fancy_font('MTG TIE')}  (base lost, MTG refund)\n"
                        f"💎 Balance : ${trader.balance:.2f}\n"
                        f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                    )
                else:
                    trader.loss_count += 1
                    trader.loss_streak += 1
                    trader.win_streak = 0
                    await send(
                        f"❌ {fancy_font('MTG LOSS')}  -${mtg_amt:.2f}\n"
                        f"💎 Balance : ${trader.balance:.2f}\n"
                        f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                    )
                # true 1-step → back to base amount on the next cycle
                await episode_pause()
            except Exception as _e:
                # contain transient errors so the session keeps running
                try:
                    await send(f"⚠️ {fancy_font('SKIPPED')} a cycle\n🔍 {str(_e)[:120]}")
                except Exception:
                    pass
                await _aio.sleep(2)
                continue

        # final premium daily/session report card
        await send(_auto_report_card(trader, "SESSION REPORT"))

    try:
        loop.run_until_complete(run())
    except Exception as e:
        try:
            loop.run_until_complete(send(f"⚠️ Auto loop crashed: {e}"))
        except Exception:
            pass
    finally:
        try:
            loop.close()
        except Exception:
            pass


# ══════════════ AUTO TRADE — conversation handlers (state-routed) ═══════
def _auto_account_message(trader):

    pass

    """Account-selection message + DEMO/REAL keyboard (reused on first login and re-entry)."""
    demo_b = real_b = 0.0
    try:
        b = trader.client.balance or {}
        ui = trader.client.user_info or {}
        demo_b = ui.get("demoBalance", b.get("demoBalance", 0)) or 0
        real_b = ui.get("realBalance", b.get("realBalance", 0)) or 0
    except Exception:
        pass
    msg = (
        f"✅ {fancy_font('LOGGED IN')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💎 Demo : ${float(demo_b or 0):.2f}\n"
        f"💲 Real : ${float(real_b or 0):.2f}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💎 Select account:"
    )
    buttons = [[InlineKeyboardButton("🧪 DEMO", callback_data="atx_acc_demo"),
                InlineKeyboardButton("💲 REAL", callback_data="atx_acc_real")]]
    return msg, InlineKeyboardMarkup(buttons)


def _auto_strategy_keyboard():

    pass

    """Colored strategy boxes (1-6), same look as the main-menu Start-Trading flow."""
    rows = []
    for i in range(1, 7):
        style = KeyboardButtonStyle.PRIMARY if i % 2 else KeyboardButtonStyle.SUCCESS
        rows.append([colored_button(f"Strategy {i}", f"atx_strat_{i}", style)])
    return InlineKeyboardMarkup(rows)


def _build_auto_s2_filters(filters):

    pass

    """Strategy-2 optional filter toggles (auto-mode, atx_ prefixed callbacks)."""
    def status(x): return "✅" if x else "❌"
    text = (
        f"🔍 {fancy_font('TOGGLE FILTERS')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{status(filters.use_trend)} Trend\n"
        f"{status(filters.use_bollinger)} Bollinger\n"
        f"{status(filters.use_support_resistance)} S/R\n"
        f"{status(filters.use_price_action)} Price Action\n"
        f"{status(filters.use_supertrend)} Supertrend\n"
        f"{status(filters.use_fvg)} FVG\n"
        f"{status(filters.use_trend_reverse)} Trend Reverse\n\n"
        f"Tap to toggle, then Done."
    )
    buttons = [
        [InlineKeyboardButton(f"{status(filters.use_trend)} Trend", callback_data="atx_s2_trend")],
        [InlineKeyboardButton(f"{status(filters.use_bollinger)} Bollinger", callback_data="atx_s2_bb")],
        [InlineKeyboardButton(f"{status(filters.use_support_resistance)} S/R", callback_data="atx_s2_sr")],
        [InlineKeyboardButton(f"{status(filters.use_price_action)} Price Action", callback_data="atx_s2_pa")],
        [InlineKeyboardButton(f"{status(filters.use_supertrend)} Supertrend", callback_data="atx_s2_st")],
        [InlineKeyboardButton(f"{status(filters.use_fvg)} FVG", callback_data="atx_s2_fvg")],
        [InlineKeyboardButton(f"{status(filters.use_trend_reverse)} Trend Reverse", callback_data="atx_s2_tr")],
        [InlineKeyboardButton("✅ Done", callback_data="atx_s2_done")],
    ]
    return text, InlineKeyboardMarkup(buttons)


def _auto_session_pnl(trader):

    pass

    try:
        return float(trader.balance) - float(trader.starting_balance)
    except Exception:
        return 0.0


def _auto_streak_str(trader):

    pass

    if trader.win_streak:
        return f"🔥 {trader.win_streak} win streak"
    if trader.loss_streak:
        return f"📉 {trader.loss_streak} loss streak"
    return "—"


def _auto_control_keyboard(trader):

    pass

    """Live control panel (Pause / Resume / Stop) for a running auto session."""
    if getattr(trader, "paused", False):
        toggle = InlineKeyboardButton("🚀 Resume", callback_data="atx_resume")
    else:
        toggle = InlineKeyboardButton("⏳ Pause", callback_data="atx_pause")
    return InlineKeyboardMarkup([[toggle,
                                  InlineKeyboardButton("📊 Status",
                                                       callback_data="atx_status")],
                                 [InlineKeyboardButton("🔴 Stop",
                                                       callback_data="atx_stop")]])


def _auto_status_card(trader):

    pass

    """Premium-styled live status card (used by /status and the Status button)."""
    import time as _t
    won, lost = trader.win_count, trader.loss_count
    decided = won + lost
    wr = (won / decided * 100) if decided else 0.0
    pnl = _auto_session_pnl(trader)
    if not trader.running:
        state = "❌ STOPPED"
    elif getattr(trader, "paused", False):
        state = "⏳ PAUSED"
    else:
        state = "✅ RUNNING"
    up = "—"
    if getattr(trader, "start_time", None):
        s = max(0, int(_t.time() - trader.start_time))
        up = f"{s // 3600}h {s % 3600 // 60}m"
    return (
        f"📊 {fancy_font('AUTO STATUS')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ State    : {state}\n"
        f"🤖 Strategy : {trader.strategy}. {trader.strategy_name}\n"
        f"💎 Balance  : ${float(trader.balance or 0):.2f}\n"
        f"📈 Session  : {pnl:+.2f}\n"
        f"🏆 Win rate : {wr:.0f}%   ({won}W / {lost}L)\n"
        f"🔥 Streak   : {_auto_streak_str(trader)}\n"
        f"📊 Trades   : {trader.trade_count}\n"
        f"🕐 Uptime   : {up}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )


def _auto_report_card(trader, title="DAILY REPORT"):

    pass

    """Premium-styled end-of-session / daily summary card."""
    won, lost, tie = trader.win_count, trader.loss_count, getattr(
        trader, "tie_count", 0)
    decided = won + lost
    wr = (won / decided * 100) if decided else 0.0
    pnl = _auto_session_pnl(trader)
    peak = getattr(trader, "peak_pnl", 0.0)
    head = "🏆" if pnl >= 0 else "📉"
    return (
        f"{head} {fancy_font(title)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Strategy : {trader.strategy}. {trader.strategy_name}\n"
        f"💎 Balance  : ${float(trader.starting_balance or 0):.2f} → ${float(trader.balance or 0):.2f}\n"
        f"📈 Net P&L  : {pnl:+.2f}\n"
        f"🚀 Peak P&L : {peak:+.2f}\n"
        f"🏆 Win rate : {wr:.0f}%   ({won}W / {lost}L / {tie}T)\n"
        f"📊 Trades   : {trader.trade_count}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✨ Trade smart. Stay disciplined."
    )

async def auto_trade_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        uid = query.from_user.id
        if not is_authorized(uid):
            await query.answer("⛔ Access denied. Contact Admin.", show_alert=True)
            return
        trader = get_auto_trader(uid)
        if trader.running:
            msg = "⚠️ Auto Trade is already running!\nUse /stop first."
            await query.message.reply_text(msg, entities=build_custom_emoji_entities(msg))
            return
        context.user_data['strategy_active'] = False
        if trader.client is not None:
            trader.balance = _auto_acct_balance(trader)
            context.user_data['state'] = STATE_AUTO_ACCOUNT
            msg, markup = _auto_account_message(trader)
            await query.message.reply_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=markup)
            return
        context.user_data['state'] = STATE_AUTO_LOGIN_EMAIL
        msg = (
            f"🚀 {fancy_font('AUTO TRADE SETUP')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📧 Enter your TradoWix email:"
        )
        await query.message.reply_text(msg, entities=build_custom_emoji_entities(msg))
    except Exception as e:
        print(f"Auto trade error: {e}")
        await update.callback_query.message.reply_text(f"❌ Error: {e}")

async def auto_signal_format_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    
    if data == "auto_signal_fmt1":
        context.user_data['auto_signal_format'] = 1
        context.user_data['state'] = STATE_AUTO_SIGNAL_CHANNEL
        msg = "📢 𝙴𝚗𝚝𝚎𝚛 𝚝𝚑𝚎 𝚌𝚑𝚊𝚗𝚗𝚎𝚕 𝙸𝙳 𝚘𝚛 𝚞𝚜𝚎𝚛𝚗𝚊𝚖𝚎:\n(e.g., @my_channel or -100123456789)"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return
        
    elif data == "auto_signal_fmt2":
        context.user_data['auto_signal_format'] = 2
        context.user_data['state'] = STATE_AUTO_SIGNAL_CHANNEL
        msg = "📢 𝙴𝚗𝚝𝚎𝚛 𝚝𝚑𝚎 𝚌𝚑𝚊𝚗𝚗𝚎𝚕 𝙸𝙳 𝚘𝚛 𝚞𝚜𝚎𝚛𝚗𝚊𝚖𝚎:\n(e.g., @my_channel or -100123456789)"
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities)
        return


async def auto_account_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    trader.is_demo = query.data.endswith("demo")
    trader.balance = _auto_acct_balance(trader)
    trader.starting_balance = trader.balance
    context.user_data['state'] = STATE_AUTO_STRATEGY
    msg = (
        f"💎 Balance : ${trader.balance:.2f}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🤖 {fancy_font('SELECT STRATEGY')}"
    )
    await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg),
                                  reply_markup=_auto_strategy_keyboard())


async def auto_strategy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    """Strategy chosen → collect that strategy's particular parameters (like the main menu)."""
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    s = int(query.data.split("_")[-1])
    trader.strategy = s
    trader.strategy_name = STRATEGY_NAMES_AUTO.get(s, "Unknown")
    st = get_state(uid)
    st.strategy = s
    head = (
        f"🤖 {fancy_font('STRATEGY')} : {s}. {trader.strategy_name}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    if s == 1:
        # no parameters → straight to Take Profit
        context.user_data['state'] = STATE_AUTO_TP
        msg = head + _auto_tp_prompt_body()
        await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))
        return
    if s == 2:
        trader._s2_filters = Strategy2Filters()
        msg = head + "🔍 Enable additional filters?"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes",
                                                         callback_data="atx_s2_filters_yes"),
                                    InlineKeyboardButton("❌ No",
                                                         callback_data="atx_s2_filters_no")]])
        await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=kb)
        return
    if s == 3:
        context.user_data['state'] = STATE_AUTO_S3_ACC
        msg = head + "🔅 Enter min accuracy %  (50-100):"
    elif s == 4:
        context.user_data['state'] = STATE_AUTO_S4_ACC
        msg = head + "🔅 Enter min accuracy %  (50-100):"
    elif s == 5:
        context.user_data['state'] = STATE_AUTO_S5_SCORE
        msg = head + "🔅 Enter min score  (50-100):"
    else:  # s == 6
        context.user_data['state'] = STATE_AUTO_S6_SCORE
        msg = head + "🔅 Enter min confluence score  (70-100):"
    await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))


async def auto_s2_filter_choice(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    trader._s2_filters = Strategy2Filters()
    if query.data.endswith("_no"):
        get_state(uid).strategy2_filters = trader._s2_filters
        context.user_data['state'] = STATE_AUTO_S2_ACC
        m = "🔅 Filters off.\n🔅 Enter min accuracy %  (50-100):"
        await query.message.edit_text(m, entities=build_custom_emoji_entities(m))
        return
    text, markup = _build_auto_s2_filters(trader._s2_filters)
    await query.message.edit_text(text, entities=build_custom_emoji_entities(text), reply_markup=markup)


async def auto_s2_filter_toggle(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    filters = getattr(trader, "_s2_filters", None) or Strategy2Filters()
    trader._s2_filters = filters
    toggle_map = {
        "atx_s2_trend": "use_trend", "atx_s2_bb": "use_bollinger",
        "atx_s2_sr": "use_support_resistance", "atx_s2_pa": "use_price_action",
        "atx_s2_st": "use_supertrend", "atx_s2_fvg": "use_fvg",
        "atx_s2_tr": "use_trend_reverse",
    }
    if query.data in toggle_map:
        attr = toggle_map[query.data]
        setattr(filters, attr, not getattr(filters, attr))
        text, markup = _build_auto_s2_filters(filters)
        await query.message.edit_text(text, entities=build_custom_emoji_entities(text), reply_markup=markup)
        return
    if query.data == "atx_s2_done":
        get_state(uid).strategy2_filters = filters
        context.user_data['state'] = STATE_AUTO_S2_ACC
        m = "🔅 Filters saved.\n🔅 Enter min accuracy %  (50-100):"
        await query.message.edit_text(m, entities=build_custom_emoji_entities(m))


async def auto_mtg_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    trader.mtg_enabled = query.data.endswith("on")
    context.user_data['state'] = STATE_AUTO_CONFIRM
    msg = (
        f"🔰 {fancy_font('CONFIRM AUTO TRADE')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💎 Balance   : ${trader.balance:.2f}\n"
        f"🤖 Strategy  : {trader.strategy}. {trader.strategy_name}\n"
        f"🔰 TP : ${trader.tp_target:.2f}    🔰 SL : ${trader.sl_target:.2f}\n"
        f"💲 Risk      : {trader.risk_percent:.1f}% / trade\n"
        f"🔥 Martingale: {'ON (1-step)' if trader.mtg_enabled else 'OFF'}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Ready?"
    )
    buttons = [
        [InlineKeyboardButton("✅ START AUTO TRADE", callback_data="atx_start")],
        [InlineKeyboardButton("❌ Cancel", callback_data="atx_cancel")],
    ]
    await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg),
                                  reply_markup=InlineKeyboardMarkup(buttons))


async def auto_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    if not trader.client:
        msg = "⚠️ Not logged in. Use /start → Auto Trade again."
        await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))
        return
    trader.running = True
    trader.paused = False
    trader.trade_count = 0
    trader.win_count = 0
    trader.loss_count = 0
    trader.tie_count = 0
    trader.win_streak = 0
    trader.loss_streak = 0
    trader.peak_pnl = 0.0
    trader.start_time = time.time()
    context.user_data['state'] = None
    msg = f"🚀 {
        fancy_font('STARTING')}...\n⏳ Scanning pairs for the next signal..."
    await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))
    # live control panel (Pause / Resume / Status / Stop)
    panel = f"⚙️ {
        fancy_font('CONTROL PANEL')}\nManage your live auto session below."
    await query.message.reply_text(panel, entities=build_custom_emoji_entities(panel),
                                   reply_markup=_auto_control_keyboard(trader))
    t = threading.Thread(
        target=auto_trade_loop, args=(
            trader, context), daemon=True)
    trader._thread = t
    t.start()


async def auto_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    query = update.callback_query
    await query.answer()
    context.user_data['state'] = None
    msg = "❌ Auto Trade cancelled.\nUse /start to begin again."
    await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))


async def auto_pause_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    """Pause / Resume the running auto session (loop honours trader.paused)."""
    query = update.callback_query
    uid = query.from_user.id
    trader = get_auto_trader(uid)
    if not trader.running:
        await query.answer("Not running", show_alert=False)
        return
    trader.paused = (query.data == "atx_pause")
    await query.answer("⏳ Paused" if trader.paused else "🚀 Resumed")
    state = "⏳ PAUSED" if trader.paused else "🚀 RUNNING"
    panel = f"⚙️ {fancy_font('CONTROL PANEL')}\n📊 {state}"
    try:
        await query.message.edit_text(panel, entities=build_custom_emoji_entities(panel),
                                      reply_markup=_auto_control_keyboard(trader))
    except Exception:
        pass


async def auto_status_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    """Status button → show a live status card."""
    query = update.callback_query
    await query.answer()
    trader = get_auto_trader(query.from_user.id)
    card = _auto_status_card(trader)
    await query.message.reply_text(card, entities=build_custom_emoji_entities(card),
                                   reply_markup=_auto_control_keyboard(trader))


async def auto_stop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    """Stop button → end the running auto session (loop sends the final report)."""
    query = update.callback_query
    await query.answer("🔴 Stopping...")
    trader = get_auto_trader(query.from_user.id)
    trader.paused = False
    trader.running = False
    msg = f"🔴 {
        fancy_font('STOPPING')}...\nFinishing the current trade, then a final report."
    try:
        await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))
    except Exception:
        pass


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    pass

    """/status — live auto-trade status card."""
    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
    trader = get_auto_trader(uid)
    card = _auto_status_card(trader)
    kb = _auto_control_keyboard(trader) if trader.running else None
    await update.message.reply_text(card, entities=build_custom_emoji_entities(card), reply_markup=kb)

# ========== BACKTEST CALLBACKS ==========


async def backtest_mtg_callback(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    mtg = int(query.data.split('_')[-1])
    context.user_data['backtest_mtg'] = mtg
    context.user_data['state'] = STATE_BACKTEST_DAYS

    msg = "📅 𝚂𝙴𝙻𝙴𝙲𝚃 𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃 𝙳𝙰𝚈𝚂\n\nChoose number of days (excluding today):"
    buttons = [[colored_button(" 3 Days",
                               "backtest_days_3",
                               KeyboardButtonStyle.SUCCESS,
                               "6145553439809084250"),
                colored_button(" 5 Days",
                               "backtest_days_5",
                               KeyboardButtonStyle.PRIMARY,
                               "6145553439809084250")],
               [colored_button(" 7 Days",
                               "backtest_days_7",
                               KeyboardButtonStyle.PRIMARY,
                               "6145553439809084250"),
                colored_button(" Custom (1-7)",
                               "backtest_days_custom",
                               KeyboardButtonStyle.PRIMARY,
                               "5217822164362739968")],
               ]
    markup = InlineKeyboardMarkup(buttons)
    await query.edit_message_text(msg, reply_markup=markup)


async def backtest_days_callback(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "backtest_days_custom":
        context.user_data['state'] = STATE_BACKTEST_CUSTOM_DAYS
        msg = "🔢 Enter number of days (1-7):"
        await query.edit_message_text(msg)
    else:
        days = int(data.split('_')[-1])
        context.user_data['backtest_days'] = days
        context.user_data['state'] = None
        # Run backtest
        signals_text = context.user_data.get('backtest_signals', '')
        mtg = context.user_data.get('backtest_mtg', 0)
        await query.edit_message_text(f"⏳ Running backtest for {days} days (excluding today), Martingale level {mtg}...")
        # Call the local backtest function (will be defined next)
        await run_backtest_local(update, context, days, signals_text, mtg)

# ══════════════ GLOBAL TEXT HANDLER (all states) ══════════════

async def global_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import re   # <-- ADD THIS LINE
    if not update.effective_user:
        return
    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
    if context.user_data.get('strategy_active'):
        return
    text = update.message.text.strip()
    state = context.user_data.get('state')

    print(
        f"[DEBUG 2] User {uid} - State: {state}, Has photo: {bool(update.message.photo)}")
    # ---- AI Mode Setup states ----
    if state == STATE_AI_MIN_CONSENSUS:
        cleaned = clean_int_input(text)
        try:
            val = int(cleaned)
            if 2 <= val <= 5:
                context.user_data['ai_min_consensus'] = val
                context.user_data['state'] = STATE_AI_REQUIRED_STRATS
                msg = "🔰 Do you want to require specific strategies to be in agreement?\nSend 'yes' or 'no':"
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities)
            else:
                msg = "❌ Enter a number between 2 and 5:"
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities)
        except ValueError:
            msg = "❌ Invalid number. Enter a number between 2 and 5:"
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
        return

    # ---- AI Filter States ----
    elif state == STATE_AI_FILTER_SIGNALS:
        context.user_data['ai_filter_signals'] = text
        context.user_data['state'] = STATE_AI_FILTER_CONFIDENCE
        msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙲𝙾𝙽𝙵𝙸𝙳𝙴𝙽𝙲𝙴 𝙻𝙴𝚅𝙴𝙻"
        conf_buttons = [
            [colored_button(" Low ", "aifilter_conf_low", KeyboardButtonStyle.SUCCESS, "6145553439809084250")],
            [colored_button(" Medium ", "aifilter_conf_medium", KeyboardButtonStyle.PRIMARY, "6147654280112248427")],
            [colored_button(" High ", "aifilter_conf_high", KeyboardButtonStyle.DANGER, "6145248943807667330")],
            [colored_button("❌ Cancel", "aifilter_conf_cancel", KeyboardButtonStyle.PRIMARY, "6145317070578916456")],
        ]
        markup = InlineKeyboardMarkup(conf_buttons)
        await update.message.reply_text(msg, reply_markup=markup)
        return

    elif state == STATE_AI_REQUIRED_STRATS:
        # Agar strategy list ka wait kar rahe hain
        if context.user_data.get('waiting_for_strat_list'):
            parts = text.split(',')
            strat_list = []
            for p in parts:
                try:
                    num = int(p.strip())
                    if 2 <= num <= 6:
                        strat_list.append(num)
                    else:
                        msg = f"❌ Strategy {num} is not between 2-6. Try again:"
                        entities = build_custom_emoji_entities(msg)
                        await update.message.reply_text(msg, entities=entities)
                        return
                except BaseException:
                    msg = "❌ Invalid number. Send comma-separated list like: 2,5"
                    entities = build_custom_emoji_entities(msg)
                    await update.message.reply_text(msg, entities=entities)
                    return
            if not strat_list:
                msg = "❌ No valid strategies. Try again:"
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities)
                return
            uid2 = context.user_data.get('uid')
            st = get_state(uid2)
            st.ai_min_consensus = context.user_data.get('ai_min_consensus', 2)
            st.ai_required_strategies = strat_list
            st.ai_mode = True
            req_str = ", ".join(f"ST{s}" for s in strat_list)
            msg = f"✅ Settings saved:\n🔰 Min consensus = {
                st.ai_min_consensus}\n🎯 Required strategies = {req_str}\n🚀 Starting AI Mode..."
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
            threading.Thread(
                target=run_ai_mode, args=(
                    uid2,), daemon=True).start()
            context.user_data['state'] = None
            context.user_data['waiting_for_strat_list'] = False
            return
        else:
            # Normal yes/no response
            answer = text.strip().lower()
            if answer == 'yes':
                context.user_data['waiting_for_strat_list'] = True
                msg = "📊 Enter strategy numbers separated by comma (e.g., `2,5` or `3,4,6`):"
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities)
            elif answer == 'no':
                uid2 = context.user_data.get('uid')
                st = get_state(uid2)
                st.ai_min_consensus = context.user_data.get(
                    'ai_min_consensus', 2)
                st.ai_required_strategies = []
                st.ai_mode = True
                msg = f"✅ Settings saved:\n🔰 Min consensus = {
                    st.ai_min_consensus}\n🎯 No required strategies\n🚀 Starting AI Mode..."
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities)
                threading.Thread(
                    target=run_ai_mode, args=(
                        uid2,), daemon=True).start()
                context.user_data['state'] = None
            else:
                msg = "❌ Please answer 'yes' or 'no'."
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities)
            return

    if context.user_data.get('blk_step') == 'start_time':
        if not re.match(r'^\d{2}:\d{2}$', text):
            await update.message.reply_text("❌ Invalid format. Use HH:MM")
            return
        context.user_data['blk_start_time'] = text
        context.user_data['blk_step'] = 'end_time'
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:30"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities)
        return

    elif context.user_data.get('blk_step') == 'end_time':
        if not re.match(r'^\d{2}:\d{2}$', text):
            await update.message.reply_text("❌ Invalid format. Use HH:MM")
            return
        context.user_data['blk_end_time'] = text
        context.user_data['blk_step'] = None
        # Show pair selection buttons
        pair_msg = (
            "🥷 𝙱𝙻𝙰𝙲𝙺𝙾𝚄𝚃 𝙵𝚂\n\n"
            "💎 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛 𝙼𝚘𝚍𝚎:\n\n"
            "🔹 𝙰𝚕𝚕 𝙿𝚊𝚒𝚛𝚜 – 𝚜𝚌𝚊𝚗 𝚊𝚕𝚕 𝟹𝟻 𝙾𝚃𝙲 𝚙𝚊𝚒𝚛𝚜\n"
            "🔹 𝙲𝚞𝚜𝚝𝚘𝚖 𝙿𝚊𝚒𝚛 – 𝚌𝚑𝚘𝚘𝚜𝚎 𝚜𝚙𝚎𝚌𝚒𝚏𝚒𝚌"
        )
        buttons = [
            [colored_button(" All Pairs (35)", "blk_pair_all", KeyboardButtonStyle.SUCCESS, "6145553439809084250")],
            [colored_button(" Custom Pair", "blk_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(pair_msg)
        await update.message.reply_text(pair_msg, entities=entities, reply_markup=markup)
        return

    elif state == STATE_BLACKOUT_CHECKER_DATE:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
            context.user_data['bl_checker_date'] = text
            context.user_data['state'] = STATE_BLACKOUT_CHECKER_SIGNALS
            await send_blackout_prompt(update, context)
        else:
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚍𝚊𝚝𝚎. 𝚄𝚜𝚎 𝚈𝚈𝚈𝚈-𝙼𝙼-𝙳𝙳")
        return

    elif state == STATE_BLACKOUT_CHECKER_SIGNALS:
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if not lines:
            await update.message.reply_text("❌ 𝙽𝚘 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚛𝚎𝚌𝚎𝚒𝚟𝚎𝚍. 𝙿𝚕𝚎𝚊𝚜𝚎 𝚜𝚎𝚗𝚍 𝚊𝚐𝚊𝚒𝚗.")
            return
        parsed_signals = []
        for line in lines:
            pair, time_str = parse_blackout_signal_line(line)
            if pair and time_str:
                parsed_signals.append((pair, time_str))
            else:
                await update.message.reply_text(f"⚠️ 𝙸𝚐𝚗𝚘𝚛𝚒𝚗𝚐 𝚒𝚗𝚟𝚊𝚕𝚒𝚍 𝚕𝚒𝚗𝚎: {line}")
        if not parsed_signals:
            await update.message.reply_text("❌ 𝙽𝚘 𝚟𝚊𝚕𝚒𝚍 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚏𝚘𝚞𝚗𝚍.")
            return
        context.user_data['bl_checker_signals'] = parsed_signals
        context.user_data['state'] = STATE_BLACKOUT_CHECKER_MTG
        msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝚃𝙸𝙽𝙶𝙰𝙻𝙴 𝙻𝙴𝚅𝙴𝙻"
        buttons = [
            [colored_button(" 𝙼𝚊𝚛𝚝𝚒𝚗𝚐𝚊𝚕𝚎 0 ", "bl_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
            [colored_button(" 𝙼𝚊𝚛𝚝𝚒𝚗𝚐𝚊𝚕𝚎 1 ", "bl_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
            [colored_button(" 𝙼𝚊𝚛𝚝𝚒𝚗𝚐𝚊𝚕𝚎 2 ", "bl_mtg_2", KeyboardButtonStyle.DANGER, "6145248943807667330")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    # ---- Auto Trade setup states ----
    if state == STATE_AUTO_LOGIN_EMAIL:
        trader = get_auto_trader(uid)
        trader.email = text
        context.user_data['state'] = STATE_AUTO_LOGIN_PASSWORD
        m = "🔰 Enter your TradoWix password:"
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_LOGIN_PASSWORD:
        trader = get_auto_trader(uid)
        trader.password = text
        try:
            await update.message.delete()
        except Exception:
            pass
        wait = "⏳ Logging in to TradoWix..."
        loading = await update.message.reply_text(wait, entities=build_custom_emoji_entities(wait))
        try:
            def _do_login():
                c = TradoWixClient()
                c.login(trader.email, trader.password)
                c.connect()
                return c
            client = await asyncio.to_thread(_do_login)
            trader.client = client
            await asyncio.sleep(1.5)
            ui = client.user_info or {}
            b = client.balance or {}
            demo_b = ui.get("demoBalance")
            real_b = ui.get("realBalance")
            if demo_b is None:
                demo_b = b.get("demoBalance", 0)
            if real_b is None:
                real_b = b.get("realBalance", 0)
            context.user_data['state'] = STATE_AUTO_ACCOUNT
            m = (
                f"✅ {fancy_font('LOGIN OK')}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💎 Demo : ${float(demo_b or 0):.2f}\n"
                f"💲 Real : ${float(real_b or 0):.2f}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💎 Select account:"
            )
            buttons = [[InlineKeyboardButton("🧪 DEMO",
                                             callback_data="atx_acc_demo"),
                        InlineKeyboardButton("💲 REAL",
                                             callback_data="atx_acc_real")]]
            await loading.edit_text(m, entities=build_custom_emoji_entities(m),
                                    reply_markup=InlineKeyboardMarkup(buttons))
        except Exception as e:
            context.user_data['state'] = None
            err = f"❌ Login failed: {e}\nUse /start to try again."
            await loading.edit_text(err, entities=build_custom_emoji_entities(err))
        return

    elif state == STATE_AUTO_TP:
        trader = get_auto_trader(uid)
        try:
            tp = float(text.strip())
            if tp <= 0:
                raise ValueError()
        except Exception:
            m = "❌ Invalid TP. Enter a positive number (e.g. 10):"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        trader.tp_target = tp
        context.user_data['state'] = STATE_AUTO_SL
        m = f"✅ TP : ${tp:.2f}\n\n🔰 Enter Stop Loss (SL) in $ (e.g. 15):"
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_SL:
        trader = get_auto_trader(uid)
        try:
            sl = float(text.strip())
            if sl <= 0:
                raise ValueError()
        except Exception:
            m = "❌ Invalid SL. Enter a positive number (e.g. 15):"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        trader.sl_target = sl
        context.user_data['state'] = STATE_AUTO_RISK
        m = f"✅ SL : ${
            sl:.2f}\n\n💲 Enter Risk per trade in % of balance (e.g. 2):"
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_RISK:
        trader = get_auto_trader(uid)
        try:
            r = float(text.strip())
            if r <= 0 or r > 100:
                raise ValueError()
        except Exception:
            m = "❌ Invalid risk. Enter a number between 0 and 100 (e.g. 2):"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        trader.risk_percent = r
        context.user_data['state'] = STATE_AUTO_MTG
        m = f"✅ Risk : {
            r:.1f}% / trade\n\n🔥 1-step Martingale (loss pe same-side double)?"
        buttons = [[InlineKeyboardButton("✅ MTG ON",
                                         callback_data="atx_mtg_on"),
                    InlineKeyboardButton("🚫 MTG OFF",
                                         callback_data="atx_mtg_off")]]
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m),
                                        reply_markup=InlineKeyboardMarkup(buttons))
        return

    # ---- Auto Trade per-strategy parameter states (mirror main-menu params) ----
    elif state == STATE_AUTO_S2_ACC:
        st = get_state(uid)
        v = _auto_int(text, 50, 100)
        if v is None:
            m = "❌ Enter a number between 50-100:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        if st.strategy2_filters is None:
            st.strategy2_filters = Strategy2Filters()
        st.strategy2_filters.min_accuracy = v
        context.user_data['state'] = STATE_AUTO_TP
        m = f"✅ Min accuracy : {v}%\n\n" + _auto_tp_prompt_body()
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_S3_ACC:
        st = get_state(uid)
        v = _auto_int(text, 50, 100)
        if v is None:
            m = "❌ Enter a number between 50-100:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        st.strategy3_min_accuracy = v
        context.user_data['state'] = STATE_AUTO_S3_LB
        m = f"✅ Min accuracy : {v}%\n\n🔅 Enter lookback period  (10-30):"
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_S3_LB:
        st = get_state(uid)
        v = _auto_int(text, 10, 30)
        if v is None:
            m = "❌ Enter a number between 10-30:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        st.strategy3_lookback = v
        context.user_data['state'] = STATE_AUTO_TP
        m = f"✅ Lookback : {v}\n\n" + _auto_tp_prompt_body()
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_S4_ACC:
        st = get_state(uid)
        v = _auto_int(text, 50, 100)
        if v is None:
            m = "❌ Enter a number between 50-100:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        st.strategy4_min_accuracy = v
        context.user_data['state'] = STATE_AUTO_TP
        m = f"✅ Min accuracy : {v}%\n\n" + _auto_tp_prompt_body()
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_S5_SCORE:
        st = get_state(uid)
        v = _auto_int(text, 50, 100)
        if v is None:
            m = "❌ Enter a number between 50-100:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        st.strategy5_min_score = v
        context.user_data['state'] = STATE_AUTO_TP
        m = f"✅ Min score : {v}\n\n" + _auto_tp_prompt_body()
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_S6_SCORE:
        st = get_state(uid)
        v = _auto_int(text, 70, 100)
        if v is None:
            m = "❌ Enter a number between 70-100:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        st.strategy6_min_score = v
        context.user_data['state'] = STATE_AUTO_S6_CANDLES
        m = f"✅ Min score : {v}\n\n🔅 Enter min candles  (10-200):"
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_S6_CANDLES:
        st = get_state(uid)
        v = _auto_int(text, 10, 200)
        if v is None:
            m = "❌ Enter a number between 10-200:"
            await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
            return
        st.strategy6_min_candles = v
        context.user_data['state'] = STATE_AUTO_TP
        m = f"✅ Min candles : {v}\n\n" + _auto_tp_prompt_body()
        await update.message.reply_text(m, entities=build_custom_emoji_entities(m))
        return

    elif state == STATE_AUTO_SIGNAL_CHANNEL:
        channel_input = text.strip()
        # Resolve to integer chat ID using context.bot.get_chat
        try:
            if channel_input.startswith('@'):
                channel_input = channel_input[1:]
            chat = await context.bot.get_chat(chat_id=channel_input)
            chat_id = chat.id
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to resolve channel. Make sure bot is admin and channel is public/accessible.\nError: {e}")
            return
        # Send test message using python-telegram-bot (not Telethon)
        try:
            test_msg = f"✅ 𝙰𝚞𝚝𝚘 𝚂𝚒𝚐𝚗𝚊𝚕 𝚖𝚘𝚍𝚎 𝚊𝚌𝚝𝚒𝚟𝚊𝚝𝚎𝚍. {chr(10)} 𝙲𝚑𝚊𝚗𝚗𝚎𝚕 𝚌𝚘𝚗𝚗𝚎𝚌𝚝𝚎𝚍."
            await context.bot.send_message(chat_id=chat_id, text=test_msg, parse_mode='HTML')
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to send test message. Bot may not be admin.\nError: {e}")
            return
        context.user_data['auto_target_id'] = chat_id   # store integer ID
        context.user_data['state'] = STATE_AUTO_SIGNAL_STRATEGY
        # Show strategy selection buttons
        buttons = [[colored_button(f" Strategy {i}", f"autostrat_{i}", KeyboardButtonStyle.PRIMARY if i%2 else KeyboardButtonStyle.SUCCESS)] for i in range(1,7)]
        markup = InlineKeyboardMarkup(buttons)
        text = "👑 𝚂𝚎𝚕𝚎𝚌𝚝 𝚜𝚝𝚛𝚊𝚝𝚎𝚐𝚢:"
        entities = build_custom_emoji_entities(text)
        await update.message.reply_text(text, entities=entities, reply_markup=markup)
        return

    elif state == STATE_AUTO_S3_ACC:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                get_state(uid).strategy3_min_accuracy = val
                await update.message.reply_text("Enter lookback period (10-30):")
                context.user_data['state'] = STATE_AUTO_S3_LB
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_S3_LB:
        try:
            val = int(clean_int_input(text))
            if 10 <= val <= 30:
                get_state(uid).strategy3_lookback = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 3), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 10-30:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_S4_ACC:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                get_state(uid).strategy4_min_accuracy = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 4), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_S5_SCORE:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                get_state(uid).strategy5_min_score = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 5), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_S6_SCORE:
        try:
            val = int(clean_int_input(text))
            if 70 <= val <= 100:
                get_state(uid).strategy6_min_score = val
                await update.message.reply_text("Enter minimum candles for analysis (10-200):")
                context.user_data['state'] = STATE_AUTO_S6_CANDLES
            else:
                await update.message.reply_text("❌ Enter between 70-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_S6_CANDLES:
        try:
            val = int(clean_int_input(text))
            if 10 <= val <= 200:
                get_state(uid).strategy6_min_candles = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 6), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 10-200:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_S2_ACC:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                st = get_state(uid)
                if st.strategy2_filters is None:
                    st.strategy2_filters = Strategy2Filters()
                st.strategy2_filters.min_accuracy = val
                await update.message.reply_text(f"✅ Min accuracy set to {val}%. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 2), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S3_ACC:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                get_state(uid).strategy3_min_accuracy = val
                await update.message.reply_text("Enter lookback period (10-30):")
                context.user_data['state'] = STATE_AUTO_SIGNAL_S3_LB
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S3_LB:
        try:
            val = int(clean_int_input(text))
            if 10 <= val <= 30:
                get_state(uid).strategy3_lookback = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 3), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 10-30:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S4_ACC:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                get_state(uid).strategy4_min_accuracy = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 4), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S5_SCORE:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                get_state(uid).strategy5_min_score = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 5), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S6_SCORE:
        try:
            val = int(clean_int_input(text))
            if 70 <= val <= 100:
                get_state(uid).strategy6_min_score = val
                await update.message.reply_text("Enter minimum candles for analysis (10-200):")
                context.user_data['state'] = STATE_AUTO_SIGNAL_S6_CANDLES
            else:
                await update.message.reply_text("❌ Enter between 70-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S6_CANDLES:
        try:
            val = int(clean_int_input(text))
            if 10 <= val <= 200:
                get_state(uid).strategy6_min_candles = val
                await update.message.reply_text("✅ Parameters saved. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 6), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 10-200:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    elif state == STATE_AUTO_SIGNAL_S2_ACC:
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                st = get_state(uid)
                if st.strategy2_filters is None:
                    st.strategy2_filters = Strategy2Filters()
                st.strategy2_filters.min_accuracy = val
                await update.message.reply_text(f"✅ Min accuracy set to {val}%. Starting Auto Signal...")
                start_auto_signal_session(uid, context.user_data.get('auto_strategy', 2), context)
                context.user_data['state'] = None
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
        return

    # ---- Other existing states ----
    if state == STATE_CHECKER_CUSTOM_DATE:
        context.user_data['checker_date'] = text
        context.user_data['state'] = STATE_CHECKER_SIGNALS
        sender.send_message(
            uid,
            "🔮  𝚂𝙸𝙶𝙽𝙰𝙻 𝙲𝙷𝙴𝙲𝙺𝙴𝚁\n\n    Paste your signals below (one per line)\n📋 Format: M1;PAIR;HH:MM;DIRECTION\n📝 Example:\n   M1;GBPJPY-OTC;08:24;CALL\n   M1;EURUSD-OTC;09:15;PUT\n\n⏰ Use UTC+5 time\n📌 Paste your signals now...")
    elif state == STATE_CHECKER_SIGNALS:
        date_str = context.user_data.get('checker_date')
        await run_checker_local(update, context, date_str, text)
        context.user_data['state'] = None

    # ===== Checker 2.0 States =====
    elif state == STATE_CHECKER2_DATE:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
            context.user_data['chk2_date'] = text
            context.user_data['state'] = STATE_CHECKER2_MTG
            msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝚃𝙸𝙽𝙶𝙰𝙻𝙴 𝙻𝙴𝚅𝙴𝙻"
            buttons = [
                [colored_button(" MTG 0 (entry only)", "chk2_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
                [colored_button(" MTG 1 (entry+1)", "chk2_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
                [colored_button(" MTG 2 (entry+2)", "chk2_mtg_2", KeyboardButtonStyle.DANGER, "6145248943807667330")],
                [colored_button(" MTG 3 (entry+3)", "chk2_mtg_3", KeyboardButtonStyle.PRIMARY, "5316681209026191987")],
                [colored_button(" Cancel", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
            ]
            markup = InlineKeyboardMarkup(buttons)
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities, reply_markup=markup)
        else:
            await update.message.reply_text("❌ Invalid date. Use YYYY-MM-DD")
        return
    elif state == STATE_CHECKER2_SIGNALS:
        context.user_data['state'] = None
        threading.Thread(target=_run_sio_checker_thread, args=(uid, context, text), daemon=True).start()
        return

    # ===== Backtest 2.0 States =====
    elif state == STATE_BACKTEST2_DAYS:
        # expecting a number (custom days)
        import re
        match = re.search(r'\b(\d+)\b', text)
        if match:
            days = int(match.group(1))
            if 2 <= days <= 30:
                context.user_data['bt2_days'] = days
                # Show martingale selection
                msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝚃𝙸𝙽𝙶𝙰𝙻𝙴 𝙻𝙴𝚅𝙴𝙻 (𝙶𝙰𝙻𝙴)"
                buttons = [
                    [colored_button(" Gale 0 (none)", "bt2_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
                     colored_button(" Gale 1", "bt2_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
                    [colored_button(" Gale 2", "bt2_mtg_2", KeyboardButtonStyle.DANGER, "6145248943807667330"),
                     colored_button(" Gale 3", "bt2_mtg_3", KeyboardButtonStyle.PRIMARY, "5316681209026191987")],
                    [colored_button(" Cancel", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
                ]
                markup = InlineKeyboardMarkup(buttons)
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities, reply_markup=markup)
                context.user_data['state'] = STATE_BACKTEST2_MTG
                return
            else:
                await update.message.reply_text("❌ Please enter a number between 2 and 30.")
                return
        else:
            await update.message.reply_text("❌ Invalid input. Send a number (2-30).")
        return

    elif state == STATE_BACKTEST2_SIGNALS:
        # User pasted signals after selecting days and gale
        context.user_data['state'] = None   # clear state to avoid loop
        # Start backtest in background thread
        threading.Thread(target=_run_sio_backtest_thread, args=(uid, context, text), daemon=True).start()
        return

    elif state == STATE_FUT_MIN_CONF:
        cleaned = clean_int_input(text)
        try:
            val = int(cleaned)
            if 0 <= val <= 100:
                context.user_data['fut_min_conf'] = val
                context.user_data['state'] = STATE_FUT_START_TIME
                sender.send_message(uid, "Enter start time (HH:MM):")
            else:
                sender.send_message(uid, "Enter between 0-100:")
        except ValueError:
            sender.send_message(
                uid, "Invalid number. Enter min confidence (0-100):")

    elif state == STATE_ALCOHOL_CUSTOM_DAYS:
        import re
        # Extract first number from the text (e.g., "23", "5 days", " 7 ")
        match = re.search(r'\b(\d+)\b', text)
        if match:
            days = int(match.group(1))
            if 1 <= days <= 30:
                context.user_data['alcohol_days'] = days
                await proceed_to_utc_selection(update, context)
                return
            else:
                await update.message.reply_text("❌ Please enter a number between 1 and 30.")
        else:
            await update.message.reply_text("❌ Invalid input. Send a number (1-30).")
        return

    elif state == STATE_ALCOHOL_START_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['alcohol_start_time'] = text
            context.user_data['state'] = STATE_ALCOHOL_END_TIME
            msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝙸𝙽 𝚈𝙾𝚄𝚁 𝚂𝙴𝙻𝙴𝙲𝚃𝙴𝙳 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 17:00"
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
        else:
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 𝙷𝙷:𝙼𝙼 (𝚎.𝚐., 09:00)")
        return

    elif state == STATE_ALCOHOL_END_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['alcohol_end_time'] = text
            # Now proceed to pair selection
            context.user_data['state'] = STATE_ALCOHOL_PAIR_MODE
            msg = "🌐 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛 𝙼𝚘𝚍𝚎:"
            buttons = [
                [colored_button(" All OTC Pairs ", "alc_pair_all", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
                [colored_button(" Custom Pairs ", "alc_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
            ]
            markup = InlineKeyboardMarkup(buttons)
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities, reply_markup=markup)
        else:
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 𝙷𝙷:𝙼𝙼")
        return

    elif state == STATE_FUT_START_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['fut_start_time'] = text
            context.user_data['state'] = STATE_FUT_END_TIME
            sender.send_message(uid, "Enter end time (HH:MM):")
        else:
            sender.send_message(uid, "Invalid format. Use HH:MM.")
    elif state == STATE_FUT_END_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['fut_end_time'] = text
            buttons = [[InlineKeyboardButton("🟢 All Supported Pairs", callback_data="pair_all")], [
                InlineKeyboardButton("🟡 Custom Pairs", callback_data="pair_custom")]]
            await update.message.reply_text("📊 Pair selection:", reply_markup=InlineKeyboardMarkup(buttons))
            context.user_data['state'] = 'fut_pair_type'
        else:
            sender.send_message(uid, "Invalid format. Use HH:MM.")
    elif context.user_data.get('smz_step') == 'start_time':
        if not re.match(r'^\d{2}:\d{2}$', text):
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 HH:MM")
            return
        context.user_data['smz_start_time'] = text
        context.user_data['smz_step'] = 'end_time'
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:30"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities)
        return

    elif context.user_data.get('smz_step') == 'end_time':
        if not re.match(r'^\d{2}:\d{2}$', text):
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 HH:MM")
            return
        context.user_data['smz_end_time'] = text
        context.user_data['smz_step'] = None
        # Pair selection buttons
        pair_msg = (
            "🥷 𝚂𝙼𝚉 𝙷𝙰𝙲𝙺𝙸𝙽𝙶 𝙼𝙾𝙳𝙴\n\n"
            "💎 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛 𝙼𝚘𝚍𝚎:\n\n"
            "🔹 𝙰𝚕𝚕 𝙿𝚊𝚒𝚛𝚜 – 𝚜𝚌𝚊𝚗 𝚊𝚕𝚕 𝟹𝟻 𝙾𝚃𝙲 𝚙𝚊𝚒𝚛𝚜\n"
            "🔹 𝙲𝚞𝚜𝚝𝚘𝚖 𝙿𝚊𝚒𝚛 – 𝚌𝚑𝚘𝚘𝚜𝚎 𝚜𝚙𝚎𝚌𝚒𝚏𝚒𝚌"
        )
        buttons = [
            [colored_button(" All Pairs (35)", "smz_pair_all", KeyboardButtonStyle.SUCCESS, "6145553439809084250")],
            [colored_button(" Custom Pair", "smz_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(pair_msg)
        await update.message.reply_text(pair_msg, entities=entities, reply_markup=markup)
        return

    elif context.user_data.get('smz_step') == 'custom_pairs':
        custom_list = [p.strip().upper() for p in text.split(',')]
        valid_pairs = []
        invalid = []
        for p in custom_list:
            if p in SMZ_ALL_PAIRS:
                valid_pairs.append(p)
            else:
                invalid.append(p)
        if invalid:
            await update.message.reply_text(f"⚠️ 𝚃𝚑𝚎𝚜𝚎 𝚙𝚊𝚒𝚛𝚜 𝚊𝚛𝚎 𝚗𝚘𝚝 𝚜𝚞𝚙𝚙𝚘𝚛𝚝𝚎𝚍: {', '.join(invalid)}\n𝚂𝚔𝚒𝚙𝚙𝚒𝚗𝚐.")
        if not valid_pairs:
            await update.message.reply_text("❌ 𝙽𝚘 𝚟𝚊𝚕𝚒𝚍 𝚙𝚊𝚒𝚛𝚜. 𝚄𝚜𝚒𝚗𝚐 𝚊𝚕𝚕 𝚙𝚊𝚒𝚛𝚜.")
            valid_pairs = SMZ_ALL_PAIRS
        start_time = context.user_data.get('smz_start_time', '00:00')
        end_time = context.user_data.get('smz_end_time', '23:59')
        context.user_data['smz_step'] = None
        await update.message.reply_text(f"⏳ 𝚁𝚞𝚗𝚗𝚒𝚗𝚐 𝚏𝚘𝚛 {len(valid_pairs)} 𝚙𝚊𝚒𝚛𝚜...\n🕒 {start_time} - {end_time}")
        threading.Thread(target=run_smz_hacking_mode, args=(uid, 2, start_time, end_time, "M1", valid_pairs), daemon=True).start()
        return
    elif state == STATE_BACKTEST_START:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
            context.user_data['backtest_start'] = text
            context.user_data['state'] = STATE_BACKTEST_END
            sender.send_message(uid, "Enter end date (YYYY-MM-DD):")
        else:
            sender.send_message(uid, "Invalid date format. Use YYYY-MM-DD:")
    elif state == STATE_BACKTEST_END:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
            context.user_data['backtest_end'] = text
            context.user_data['state'] = STATE_BACKTEST_SIGNALS
            sender.send_message(
                uid,
                "📺  𝙱𝙰𝙲𝙺𝚃𝙴𝚂𝚃\n\n    Paste your signals below (one per line)\n📋 Format: M1;PAIR;HH:MM;DIRECTION\n📝 Example:\n   M1;GBPJPY-OTC;08:24;CALL\n   M1;EURUSD-OTC;09:15;PUT\n\n⏰ Use UTC+5 time\n📌 Paste your signals now...")
        else:
            sender.send_message(uid, "Invalid date format. Use YYYY-MM-DD:")
    elif state == STATE_BACKTEST_SIGNALS:
        start_date = context.user_data.get('backtest_start')
        end_date = context.user_data.get('backtest_end')
        run_backtest_sio(uid, start_date, end_date, text)
        context.user_data['state'] = None
    elif state == STATE_UTC_ORIG_OFFSET:
        try:
            orig_off = int(text)
            context.user_data['utc_orig'] = orig_off
            context.user_data['state'] = STATE_UTC_TARGET_OFFSET
            sender.send_message(
                uid, "Enter target timezone offset (e.g., +5 for Pakistan):")
        except ValueError:
            sender.send_message(uid, "⚠️ Invalid offset. Enter a number.")
    elif state == STATE_UTC_TARGET_OFFSET:
        try:
            target_off = int(text)
            context.user_data['utc_target'] = target_off
            context.user_data['state'] = STATE_UTC_SIGNALS
            sender.send_message(
                uid,
                "📋 Now paste your signal list (one per line).\nType `done` on a new line when you are finished.")
        except ValueError:
            sender.send_message(uid, "⚠️ Invalid offset.")
    elif state == STATE_UTC_SIGNALS:
        lines_to_add = text.split('\n')
        finish = any(line.strip().lower() == 'done' for line in lines_to_add)
        if finish:
            lines_to_add = [
                l for l in lines_to_add if l.strip().lower() != 'done']
            if 'utc_signals' not in context.user_data:
                context.user_data['utc_signals'] = []
            context.user_data['utc_signals'].extend(lines_to_add)
            orig_off = context.user_data.get('utc_orig', 0)
            target_off = context.user_data.get('utc_target', 0)
            all_lines = context.user_data['utc_signals']
            if not all_lines:
                sender.send_message(uid, "❌ No signals provided.")
            else:
                diff = target_off - orig_off
                converted = []
                for line in all_lines:
                    m = re.search(r'(\d{2}:\d{2})', line)
                    if m:
                        time_str = m.group(1)
                        try:
                            h, minute = map(int, time_str.split(':'))
                            total_min = h * 60 + minute + diff * 60
                            total_min %= 24 * 60
                            new_h, new_m = divmod(total_min, 60)
                            new_time = f"{new_h:02d}:{new_m:02d}"
                            line = line.replace(time_str, new_time, 1)
                        except BaseException:
                            pass
                    converted.append(line)
                sender.send_message(uid, "\n".join(converted))
            context.user_data['utc_signals'] = []
            context.user_data['state'] = None
        else:
            if 'utc_signals' not in context.user_data:
                context.user_data['utc_signals'] = []
            context.user_data['utc_signals'].extend(lines_to_add)
            sender.send_message(
                uid, f"✅ Received {
                    len(lines_to_add)} line(s). Continue pasting or type 'done' to finish.")
    elif state == STATE_TREND_FILTER_INPUT:
        result = process_trend_filter(uid, text)
        sender.send_message(uid, result)
        context.user_data['state'] = None
    elif state == STATE_AI_FILTER_SIGNALS:
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if not lines:
            sender.send_message(
                uid, "❌ No signals received. Paste your signals (one per line).")
            return
        context.user_data['ai_filter_signals'] = lines
        context.user_data['state'] = STATE_AI_FILTER_CONFIDENCE
        conf_buttons = [
            [colored_button("  Low ", "aifilter_conf_Baixa", KeyboardButtonStyle.SUCCESS, ),
             colored_button("  Medium ", "aifilter_conf_Média", KeyboardButtonStyle.PRIMARY, )],
            [colored_button("  High ", "aifilter_conf_Alta", KeyboardButtonStyle.DANGER, )],
        ]
        msg = f"✅ Got {len(lines)} signals!\n\n💎 Select AI confidence level:"
        entities = build_custom_emoji_entities(msg)
        await update.message.reply_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(conf_buttons))
    elif state == STATE_FORMATTER_INPUT:
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        if not lines:
            sender.send_message(uid, "❌ No signals received.")
            return
        context.user_data['formatter_signals'] = lines
        context.user_data['state'] = STATE_FORMATTER_EXAMPLE
        sender.send_message(
            uid,
            f"✅ Got {
                len(lines)} signals!\n\n📋 Now send me an example of your desired output format with placeholders:\n<PAIR>, <TIME>, <DIRECTION>\n\n👑 Examples:\n⧉ <PAIR> - <TIME> - <DIRECTION>\n❒ <PAIR> ➪ <TIME> ➪ <DIRECTION>\n| <TIME> = <PAIR> = <DIRECTION> |\nM1;<PAIR>;<TIME>;<DIRECTION>")
    elif state == STATE_FORMATTER_EXAMPLE:
        original_lines = context.user_data.get('formatter_signals', [])
        if not original_lines:
            sender.send_message(
                uid, "❌ No signals stored. Please start again.")
            context.user_data['state'] = None
            return
        template = text.strip()
        result = format_signals_with_template(original_lines, template)
        sender.send_message(uid, result)
        context.user_data['state'] = None
    elif state == STATE_FONT_INPUT:
        context.user_data['font_text'] = text
        context.user_data['state'] = STATE_FONT_STYLE
        keyboard = [
            [
                InlineKeyboardButton(
                    "1️⃣ Monospace (Code)", callback_data="font_mono")], [
                InlineKeyboardButton(
                    "2️⃣ Sans‑Serif Bold", callback_data="font_sans_bold")], [
                        InlineKeyboardButton(
                            "3️⃣ Sans‑Serif Mono", callback_data="font_sans_mono")]]
        await update.message.reply_text("🎨 Choose a font style:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif state == STATE_FONT_STYLE:
        pass
    elif state == STATE_FUT_CUSTOM_PAIRS:
        pairs_text = text.upper()
        pairs_list = [p.strip() for p in pairs_text.split(",") if p.strip()]
        min_conf = context.user_data.get('fut_min_conf', 75)
        start = context.user_data.get('fut_start_time', '08:00')
        end = context.user_data.get('fut_end_time', '23:59')
        result = generate_future_signals(uid, min_conf, start, end, selected_pairs=pairs_list)
        if result:
            sender.send_message(uid, result)
        else:
            sender.send_message(uid, "❌ No future signals found.")
        context.user_data['state'] = None
    elif state == STATE_BACKTEST_LIST:
        # Store signals and ask for martingale level
        context.user_data['backtest_signals'] = text
        context.user_data['state'] = STATE_BACKTEST_MTG
        msg = "🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝚃𝙸𝙽𝙶𝙰𝙻𝙴 𝙻𝙴𝚅𝙴𝙻"
        mtg_buttons = [
            [colored_button(" Mtg 0 (entry only)", "backtest_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
            [colored_button(" Mtg 1 (entry+1)", "backtest_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
            [colored_button(" Mtg 2 (entry+2)", "backtest_mtg_2", KeyboardButtonStyle.DANGER, "6145248943807667330")],
            [colored_button(" Mtg 3 (entry+3)", "backtest_mtg_3", KeyboardButtonStyle.PRIMARY, "6204172639523572930")],
        ]
        markup = InlineKeyboardMarkup(mtg_buttons)
        await update.message.reply_text(msg, reply_markup=markup)
        return

    elif state == STATE_BACKTEST_CUSTOM_DAYS:
        try:
            days = int(clean_int_input(text))
            if 1 <= days <= 7:
                context.user_data['backtest_days'] = days
                context.user_data['state'] = None
                signals_text = context.user_data.get('backtest_signals', '')
                mtg = context.user_data.get('backtest_mtg', 0)
                await update.message.reply_text(f"⏳ Running backtest for {days} days (excluding today), Martingale level {mtg}...")
                await run_backtest_local(update, context, days, signals_text, mtg)
            else:
                await update.message.reply_text("❌ Please enter a number between 1 and 7.")
                return STATE_BACKTEST_CUSTOM_DAYS
        except BaseException:
            await update.message.reply_text("❌ Invalid number. Enter days (1-7):")
            return STATE_BACKTEST_CUSTOM_DAYS


# ----- Additional callbacks for future pairs and font style -----
async def fut_pair_callback(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    await query.answer()
    data = query.data
    if data == "pair_all":
        min_conf = context.user_data.get('fut_min_conf', 75)
        start = context.user_data.get('fut_start_time', '08:00')
        end = context.user_data.get('fut_end_time', '23:59')
        result = generate_future_signals(uid, min_conf, start, end)
        if result:
            sender.send_message(uid, result)
        else:
            sender.send_message(uid, "❌ No future signals found.")
        context.user_data['state'] = None
    elif data == "pair_custom":
        await query.edit_message_text("📊 Enter pairs (comma-separated), e.g., EURUSD_OTC,GBPUSD_OTC:")
        context.user_data['state'] = STATE_FUT_CUSTOM_PAIRS


async def smz_tf_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    # Start the flow: ask for start time
    context.user_data['smz_step'] = 'start_time'
    msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities)

def _build_smz_pair_page(page=0, per_page=15, selected=None):
    if selected is None:
        selected = set()
    total = len(SMZ_ALL_PAIRS)   # <-- SMZ_ALL_PAIRS use karo (35 pairs)
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total)
    page_pairs = SMZ_ALL_PAIRS[start_idx:end_idx]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        short = pair.replace("-OTC", "")
        label = f"✅ {short}" if pair in selected else short
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"smz_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"smz_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"smz_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "smz_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

def _build_blackout_pair_page(page=0, per_page=15, selected=None):
    if selected is None:
        selected = set()
    total = len(SMZ_ALL_PAIRS)
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total)
    page_pairs = SMZ_ALL_PAIRS[start_idx:end_idx]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        short = pair.replace("-OTC", "")
        label = f"✅ {short}" if pair in selected else short
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"blk_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"blk_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"blk_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "blk_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

async def auto_strategy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    strat = int(data.split("_")[1])
    context.user_data['auto_strategy'] = strat

    if strat == 1:
        await query.message.reply_text("✅ Strategy 1 selected. Starting Auto Signal...")
        start_auto_signal_session(uid, strat, context)
        context.user_data['state'] = None
    elif strat == 2:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes", callback_data="auto_s2_filters_yes"),
            InlineKeyboardButton("❌ No", callback_data="auto_s2_filters_no")
        ]])
        await query.message.reply_text("🔰 Strategy 2: Enable additional filters?", reply_markup=kb)
        context.user_data['state'] = STATE_AUTO_SIGNAL_S2_CHOICE
    elif strat == 3:
        await query.message.reply_text("✅ Strategy 3 selected. Enter min accuracy % (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S3_ACC
    elif strat == 4:
        await query.message.reply_text("✅ Strategy 4 selected. Enter min accuracy % (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S4_ACC
    elif strat == 5:
        await query.message.reply_text("✅ Strategy 5 selected. Enter min score (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S5_SCORE
    elif strat == 6:
        await query.message.reply_text("✅ Strategy 6 selected. Enter minimum confluence score (70‑100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S6_SCORE

def build_auto_s2_filter_message(filters):
    def status(x): return "✅" if x else "❌"
    text = f"🎯 Toggle filters:\n\n{status(filters.use_trend)} Trend\n{status(filters.use_bollinger)} Bollinger\n{status(filters.use_support_resistance)} S/R\n{status(filters.use_price_action)} Price Action\n{status(filters.use_supertrend)} Supertrend\n{status(filters.use_fvg)} FVG\n{status(filters.use_trend_reverse)} Trend Reverse\n\nTap a filter to toggle, then 'Done'."
    buttons = [
        [InlineKeyboardButton(f"{status(filters.use_trend)} Trend", callback_data="auto_s2_trend")],
        [InlineKeyboardButton(f"{status(filters.use_bollinger)} Bollinger", callback_data="auto_s2_bb")],
        [InlineKeyboardButton(f"{status(filters.use_support_resistance)} S/R", callback_data="auto_s2_sr")],
        [InlineKeyboardButton(f"{status(filters.use_price_action)} Price Action", callback_data="auto_s2_pa")],
        [InlineKeyboardButton(f"{status(filters.use_supertrend)} Supertrend", callback_data="auto_s2_st")],
        [InlineKeyboardButton(f"{status(filters.use_fvg)} FVG", callback_data="auto_s2_fvg")],
        [InlineKeyboardButton(f"{status(filters.use_trend_reverse)} Trend Reverse", callback_data="auto_s2_tr")],
        [InlineKeyboardButton("✅ Done", callback_data="auto_s2_done")],
    ]
    return text, InlineKeyboardMarkup(buttons)

async def auto_s2_filter_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    if data == "auto_s2_filters_no":
        get_state(uid).strategy2_filters = Strategy2Filters()
        await query.edit_message_text("✅ Filters disabled. Enter min accuracy (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S2_ACC
        return
    else:
        filters = Strategy2Filters()
        context.user_data['auto_filters'] = filters
        text, markup = build_auto_s2_filter_message(filters)
        await query.edit_message_text(text, reply_markup=markup)
        context.user_data['state'] = STATE_AUTO_SIGNAL_S2_FILTER_TOGGLE
        return

async def auto_s2_filter_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    filters = context.user_data.get('auto_filters', Strategy2Filters())
    toggle_map = {
        "auto_s2_trend": "use_trend", "auto_s2_bb": "use_bollinger", "auto_s2_sr": "use_support_resistance",
        "auto_s2_pa": "use_price_action", "auto_s2_st": "use_supertrend", "auto_s2_fvg": "use_fvg",
        "auto_s2_tr": "use_trend_reverse"
    }
    if data in toggle_map:
        attr = toggle_map[data]
        setattr(filters, attr, not getattr(filters, attr))
        text, markup = build_auto_s2_filter_message(filters)
        await query.edit_message_text(text, reply_markup=markup)
        return
    elif data == "auto_s2_done":
        get_state(uid).strategy2_filters = filters
        await query.edit_message_text("✅ Filters saved. Enter min accuracy (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S2_ACC
        return

def start_auto_signal_session(uid, strategy_id, context):
    st = get_state(uid)
    st.running = True
    st.stop_requested = False
    st.paused = False
    st.session_stats = {'wins': 0, 'losses': 0}
    st.signal_history = []
    target_id = context.user_data.get('auto_target_id')
    if not target_id:
        sender.send_message(uid, "❌ Target channel not set. Use /start again.")
        return
    st.target_chat = target_id

    # Start user_sender if not ready (for premium emojis)
    if not user_sender.ready:
        try:
            user_sender.start()
        except Exception as e:
            sender.send_message(uid, f"❌ Failed to start user account: {e}")
            return
        time.sleep(3)

    # Control panel (sent to user's private chat via bot sender)
    buttons = [
        [telethon_button("📊 Partial Results", "autopartial")],
        [telethon_button("⏸️ Pause", "autopause"), telethon_button("▶️ Continue", "autocontinue")],
        [telethon_button("🔴 Stop", "autostop")],
    ]
    control_msg = (
        f"💎 𝚂𝙼𝚉𝚇 𝙰𝚄𝚃𝙾 𝚂𝙸𝙶𝙽𝙰𝙻\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Strategy: {strategy_id}\n"
        f"📊 Status: ✅ RUNNING\n"
        f"📢 Channel connected (premium emojis active)\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    sender.send_message(uid, control_msg, buttons=buttons)

    threading.Thread(target=auto_signal_loop, args=(uid, strategy_id, context), daemon=True).start()

def auto_signal_loop(uid, strategy_id, context):
    import time as t
    from datetime import timedelta

    st = get_state(uid)
    bot = SMZXBot(uid)
    bot.strategy = strategy_id
    pairs = bot.pairs
    st.session_stats = {'wins': 0, 'losses': 0}
    st.signal_history = []
    st.running = True
    st.paused = False
    st.stop_requested = False

    # Cooldown tracking: pair -> timestamp (seconds) until which it is blocked
    pair_cooldown = {}
    last_signal_pair = None   # track the last pair that produced a signal

    while st.running:
        if st.stop_requested:
            break
        if st.paused:
            t.sleep(1)
            continue

        signal_found = False
        now = t.time()

        for pair in pairs:
            if st.stop_requested or st.paused:
                break

            # Skip pair if it's in cooldown (loss or back-to-back)
            if pair in pair_cooldown and now < pair_cooldown[pair]:
                continue

            candles, price, payout = bot.fetch_data(pair, limit=200)
            if not candles:
                continue

            # Payout filter: skip if payout < 77%
            try:
                payout_num = int(payout) if payout != "!" else 0
            except:
                payout_num = 0
            if payout_num < 77:
                continue

            try:
                bias, entry_dt, score = bot.analyze(candles)
            except:
                continue

            if bias:
                signal_found = True
                entry_t = entry_dt.strftime("%H:%M")
                direction = "CALL" if bias == "CALL" else "PUT"

                # --- Back-to-back block: if same pair as last signal, block for 2 minutes ---
                if last_signal_pair == pair:
                    pair_cooldown[pair] = t.time() + 120   # 2 minutes
                    # Optionally notify (uncomment if needed)
                    # sender.send_message(uid, f"⚠️ {pair} blocked for 2 min (back-to-back signal)")
                last_signal_pair = pair

                # Send signal (same as before)
                if context.user_data.get('auto_signal_format') == 2:
                    signal_text = build_signal_format2(pair, entry_t, direction)
                    user_sender.send_bold_message(st.target_chat, signal_text)
                else:
                    signal_text = build_signal_message(pair, entry_t, direction, payout, bot.get_trend_text(candles, direction))
                    chart_path = draw_neon_chart(candles, pair, entry_t, direction, payout,
                                                 confidence=score,
                                                 wins=st.session_stats['wins'],
                                                 losses=st.session_stats['losses'],
                                                 strategy=strategy_id,
                                                 martingale_steps=1,
                                                 signal_history=st.signal_history)
                    if chart_path and os.path.exists(chart_path):
                        user_sender.send_file(st.target_chat, chart_path, signal_text)
                        try:
                            os.remove(chart_path)
                        except:
                            pass
                    else:
                        user_sender.send_message(st.target_chat, signal_text)

                # Wait for 1‑minute candle
                bot.sleep_until(entry_dt + timedelta(minutes=1))
                if st.stop_requested:
                    break
                candles2, _, _ = bot.fetch_data(pair, limit=750)
                if not candles2:
                    continue
                first = bot.get_candle_at_time(candles2, entry_dt)
                if not first:
                    continue
                win1 = (first['close'] > first['open']) if direction == "CALL" else (first['close'] < first['open'])

                # ========== RESULT SENDING & COOLDOWN ON LOSS ==========
                if context.user_data.get('auto_signal_format') == 2:
                    if win1:
                        result_msg = build_result_first_win_format2(pair, entry_t)
                        user_sender.send_bold_message(st.target_chat, result_msg)
                        st.session_stats['wins'] += 1
                        st.signal_history.append({
                            'pair': pair, 'direction': direction, 'time': entry_t,
                            'result': 'WIN', 'type': 'NON-MTG'
                        })
                    else:
                        # Martingale step
                        close_time_2 = entry_dt + timedelta(minutes=2)
                        bot.sleep_until(close_time_2)
                        if st.stop_requested:
                            break
                        candles3, _, _ = bot.fetch_data(pair, limit=750)
                        if not candles3:
                            continue
                        second = bot.get_candle_at_time(candles3, entry_dt + timedelta(minutes=1))
                        if not second:
                            continue
                        win2 = (second['close'] > second['open']) if direction == "CALL" else (second['close'] < second['open'])
                        if win2:
                            result_msg = build_result_second_win_format2(pair, entry_t)
                            user_sender.send_message(st.target_chat, result_msg)
                            st.session_stats['wins'] += 1
                            st.signal_history.append({
                                'pair': pair, 'direction': direction, 'time': entry_t,
                                'result': 'WIN', 'type': 'MTG'
                            })
                        else:
                            result_msg = build_result_loss_format2(pair, entry_t)
                            user_sender.send_message(st.target_chat, result_msg)
                            st.session_stats['losses'] += 1
                            st.signal_history.append({
                                'pair': pair, 'direction': direction, 'time': entry_t,
                                'result': 'LOSS', 'type': 'NON-MTG'
                            })
                            # Loss → block pair for 4 minutes
                            pair_cooldown[pair] = t.time() + 240
                else:
                    # Format 1: with chart
                    if win1:
                        st.session_stats['wins'] += 1
                        result_msg = build_result_message_first_win(pair, entry_t, payout, st.session_stats['wins'], st.session_stats['losses'])
                        st.signal_history.append({
                            'pair': pair, 'direction': direction, 'time': entry_t,
                            'result': 'WIN', 'type': 'NON-MTG'
                        })
                        chart_path = draw_result_chart(candles2, pair, payout, "WIN", first, None,
                                                       wins=st.session_stats['wins'],
                                                       losses=st.session_stats['losses'],
                                                       strategy=strategy_id,
                                                       direction=direction,
                                                       entry_time_str=entry_t,
                                                       signal_history=st.signal_history)
                        if chart_path and os.path.exists(chart_path):
                            user_sender.send_file(st.target_chat, chart_path, result_msg)
                            try: os.remove(chart_path)
                            except: pass
                        else:
                            user_sender.send_message(st.target_chat, result_msg)
                    else:
                        close_time_2 = entry_dt + timedelta(minutes=2)
                        bot.sleep_until(close_time_2)
                        if st.stop_requested:
                            break
                        candles3, _, _ = bot.fetch_data(pair, limit=750)
                        if not candles3:
                            continue
                        second = bot.get_candle_at_time(candles3, entry_dt + timedelta(minutes=1))
                        if not second:
                            continue
                        win2 = (second['close'] > second['open']) if direction == "CALL" else (second['close'] < second['open'])
                        if win2:
                            st.session_stats['wins'] += 1
                            result_msg = build_result_message_second_win(pair, entry_t, payout, st.session_stats['wins'], st.session_stats['losses'])
                            st.signal_history.append({
                                'pair': pair, 'direction': direction, 'time': entry_t,
                                'result': 'WIN', 'type': 'MTG'
                            })
                            chart_path = draw_result_chart(candles3, pair, payout, "MTG WIN", first, second,
                                                           wins=st.session_stats['wins'],
                                                           losses=st.session_stats['losses'],
                                                           strategy=strategy_id,
                                                           direction=direction,
                                                           entry_time_str=entry_t,
                                                           signal_history=st.signal_history)
                            if chart_path and os.path.exists(chart_path):
                                user_sender.send_file(st.target_chat, chart_path, result_msg)
                                try: os.remove(chart_path)
                                except: pass
                            else:
                                user_sender.send_message(st.target_chat, result_msg)
                        else:
                            st.session_stats['losses'] += 1
                            result_msg = build_result_message_loss(pair, entry_t, payout, st.session_stats['wins'], st.session_stats['losses'])
                            st.signal_history.append({
                                'pair': pair, 'direction': direction, 'time': entry_t,
                                'result': 'LOSS', 'type': 'NON-MTG'
                            })
                            chart_path = draw_result_chart(candles3, pair, payout, "LOSS", first, None,
                                                           wins=st.session_stats['wins'],
                                                           losses=st.session_stats['losses'],
                                                           strategy=strategy_id,
                                                           direction=direction,
                                                           entry_time_str=entry_t,
                                                           signal_history=st.signal_history)
                            if chart_path and os.path.exists(chart_path):
                                user_sender.send_file(st.target_chat, chart_path, result_msg)
                                try: os.remove(chart_path)
                                except: pass
                            else:
                                user_sender.send_message(st.target_chat, result_msg)
                            # Loss → block pair for 4 minutes
                            pair_cooldown[pair] = t.time() + 240

                # Pause 15 seconds between signals (existing)
                for _ in range(15):
                    if st.stop_requested or st.paused:
                        break
                    t.sleep(1)
                break   # after a signal, break out of the pair loop to start fresh scan

        if not signal_found and not st.stop_requested and not st.paused:
            t.sleep(10)

    # Final stats (unchanged)
    total = st.session_stats['wins'] + st.session_stats['losses']
    wr = (st.session_stats['wins'] / total * 100) if total > 0 else 0
    final_msg = (
        f"🔴 𝙰𝚄𝚃𝙾 𝚂𝙸𝙶𝙽𝙰𝙻 𝚂𝚃𝙾𝙿𝙿𝙴𝙳\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ Wins: {st.session_stats['wins']}\n"
        f"❌ Losses: {st.session_stats['losses']}\n"
        f"📊 Win Rate: {wr:.1f}%\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    user_sender.send_message(st.target_chat, final_msg)
    sender.send_message(uid, "🔴 Auto Signal session stopped. Use /start to begin again.")
    st.running = False
    st.target_chat = None
    st.session_stats = None
    st.signal_history = None

async def auto_control_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    st = get_state(uid)

    if data == "autopause":
        st.paused = True
        # Delete old message and send fresh control panel with "Paused" status
        await query.message.delete()
        fresh_buttons = [
            [telethon_button("📊 Partial Results", "autopartial")],
            [telethon_button("▶️ Continue", "autocontinue")],
            [telethon_button("🔴 Stop", "autostop")],
        ]
        fresh_msg = (
            f"💎 𝚂𝙼𝚉𝚇 𝙰𝚄𝚃𝙾 𝚂𝙸𝙶𝙽𝙰𝙻\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Strategy: {st.strategy}\n"
            f"📊 Status: ⏸️ PAUSED\n"
            f"━━━━━━━━━━━━━━━━━━"
        )
        sender.send_message(uid, fresh_msg, buttons=fresh_buttons)
        return

    elif data == "autocontinue":
        st.paused = False
        await query.message.delete()
        fresh_buttons = [
            [telethon_button("📊 Partial Results", "autopartial")],
            [telethon_button("⏸️ Pause", "autopause")],
            [telethon_button("🔴 Stop", "autostop")],
        ]
        fresh_msg = (
            f"💎 𝚂𝙼𝚉𝚇 𝙰𝚄𝚃𝙾 𝚂𝙸𝙶𝙽𝙰𝙻\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Strategy: {st.strategy}\n"
            f"📊 Status: ✅ RUNNING\n"
            f"━━━━━━━━━━━━━━━━━━"
        )
        sender.send_message(uid, fresh_msg, buttons=fresh_buttons)
        return

    elif data == "autostop":
        st.stop_requested = True
        st.running = False
        await query.edit_message_text("🔴 𝚂𝚝𝚘𝚙𝚙𝚒𝚗𝚐 𝚜𝚎𝚜𝚜𝚒𝚘𝚗...")
        return

    elif data == "autopartial":
        wins = st.session_stats.get('wins', 0)
        losses = st.session_stats.get('losses', 0)
        total = wins + losses
        wr = (wins / total * 100) if total > 0 else 0
        now_date = datetime.now(timezone.utc) + timedelta(hours=5)
        date_str = now_date.strftime("%Y.%m.%d")
        # Build all signals from history
        body_lines = []
        for trade in st.signal_history:
            dir_text = "𝙱𝚄𝚈" if trade['direction'] == 'CALL' else "𝙿𝚄𝚃"
            if trade['result'] == 'WIN':
                if trade.get('type') == 'MTG':
                    result_icon = "✅¹"
                else:
                    result_icon = "✅"
            else:
                result_icon = "❌"
            body_lines.append(f"𝙼𝟷 {fancy_font(trade['pair'])} {trade['time']} {dir_text} {result_icon}")
        body = "\n".join(body_lines) if body_lines else "—— 𝙽𝙾 𝚁𝙴𝙰𝙻 𝚂𝙸𝙶𝙽𝙰𝙻𝚂 𝙿𝙻𝙰𝙲𝙴𝙳 ——"
        partial_msg = (
            f"=========== 𝙿𝙰𝚁𝚃𝙸𝙰𝙻 ===========\n\n"
            f"━━━━━━━━━━━・━━━━━━━━━━━\n"
            f"                  🗓 {date_str}\n"
            f"━━━━━━━━━━━・━━━━━━━━━━━\n"
            f"                  💞 Total:{total}\n"
            f"━━━━━━━━━━━・━━━━━━━━━━━\n"
            f"{body}\n"
            f"━━━━━━━━━━━・━━━━━━━━━━━\n"
            f"🔥 Win: {wins} | ❌ Loss: {losses} | 🏆 -> ({wr:.1f}%)\n"
            f"━━━━━━━━━━━・━━━━━━━━━━━\n"
            f"🤖 Partial Sent Successfully"
        )
        # Send to target channel using user_sender
        if st.target_chat:
            user_sender.send_message(st.target_chat, partial_msg)
        else:
            sender.send_message(uid, "❌ Target channel not set.")
        await query.answer("Partial results sent to channel.")

async def smz_pair_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if not is_authorized(uid):
        await query.answer("⛔ Access denied.", show_alert=True)
        return
    data = query.data
    start_time = context.user_data.get('smz_start_time', '00:00')
    end_time = context.user_data.get('smz_end_time', '23:59')

    if data == "smz_pair_all":
        await query.edit_message_text(f"⏳ Processing {len(SMZ_ALL_PAIRS)} pairs...\n🕒 {start_time} - {end_time}")
        threading.Thread(target=run_smz_hacking_mode, args=(uid, 2, start_time, end_time, "M1", SMZ_ALL_PAIRS), daemon=True).start()
        context.user_data['smz_step'] = None
        return

    elif data == "smz_pair_custom":
        # Paginated selection start
        context.user_data['smz_selected_pairs'] = set()
        context.user_data['smz_pair_page'] = 0
        buttons, page, total_pages = _build_smz_pair_page(0, selected=set())
        selected_count = 0
        msg = f"🎯 Select pairs (Page 1/{total_pages}):\n\nTap pairs to select/deselect, then press Done\nSelected: {selected_count} pairs"
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        return

    # Handle pagination navigation
    elif data.startswith("smz_pairpage_"):
        page = int(data.replace("smz_pairpage_", ""))
        context.user_data['smz_pair_page'] = page
        selected = context.user_data.get('smz_selected_pairs', set())
        buttons, page, total_pages = _build_smz_pair_page(page, selected=selected)
        selected_count = len(selected)
        msg = f"🎯 Select pairs (Page {page+1}/{total_pages}):\n\nTap pairs to select/deselect, then press Done\nSelected: {selected_count} pairs"
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        return

    # Handle pair selection toggling
    elif data.startswith("smz_pickpair_"):
        pair = data.replace("smz_pickpair_", "")
        selected = context.user_data.get('smz_selected_pairs', set())
        if pair in selected:
            selected.discard(pair)
        else:
            selected.add(pair)
        context.user_data['smz_selected_pairs'] = selected
        page = context.user_data.get('smz_pair_page', 0)
        buttons, page, total_pages = _build_smz_pair_page(page, selected=selected)
        selected_count = len(selected)
        msg = f"🎯 Select pairs (Page {page+1}/{total_pages}):\n\nTap pairs to select/deselect, then press Done\nSelected: {selected_count} pairs"
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        return

    # Done button
    elif data == "smz_pair_done":
        selected = context.user_data.get('smz_selected_pairs', set())
        if not selected:
            msg = "❌ 𝙿𝚕𝚎𝚊𝚜𝚎 𝚜𝚎𝚕𝚎𝚌𝚝 𝚊𝚝 𝚕𝚎𝚊𝚜𝚝 1 𝚙𝚊𝚒𝚛!"
            entities = build_custom_emoji_entities(msg)
            await query.message.reply_text(msg, entities=entities)
            return
        pairs_list = list(selected)
        # Confirm which pairs were selected
        confirm_msg = f"✅ 𝚂𝚎𝚕𝚎𝚌𝚝𝚎𝚍 𝚌𝚞𝚜𝚝𝚘𝚖 𝚙𝚊𝚒𝚛𝚜: {', '.join(pairs_list)}"
        confirm_entities = build_custom_emoji_entities(confirm_msg)
        await query.message.reply_text(confirm_msg, entities=confirm_entities)
        start_time = context.user_data.get('smz_start_time', '00:00')
        end_time = context.user_data.get('smz_end_time', '23:59')
        context.user_data['smz_step'] = None
        processing_msg = f"⏳ 𝙿𝚛𝚘𝚌𝚎𝚜𝚜𝚒𝚗𝚐 {len(pairs_list)} 𝚌𝚞𝚜𝚝𝚘𝚖 𝚙𝚊𝚒𝚛𝚜...\n🕒 {start_time} - {end_time}"
        processing_entities = build_custom_emoji_entities(processing_msg)
        await query.edit_message_text(processing_msg, entities=processing_entities)
        threading.Thread(target=run_smz_hacking_mode, args=(uid, 2, start_time, end_time, "M1", pairs_list), daemon=True).start()
        return


# ══════════════ AI CHART ANALYZER (OpenRouter Vision API) ══════════════

OPENROUTER_MODELS = [
    "google/gemma-4-31b-it:free",
    "nvidia/nemotron-nano-12b-v2-vl:free",
    "google/gemma-4-26b-a4b-it:free",
]

_chart_analyzer_cooldown: Dict[int, float] = {}
_chart_analyzer_daily_usage: Dict[int, list] = {}
CHART_DAILY_LIMIT = 15

CHART_ANALYSIS_PROMPT = (
    "You are an elite binary options analyst with 15+ years experience on Quotex. "
    "Analyze this 1-minute chart screenshot for the NEXT candle direction.\n\n"
    "CRITICAL ANALYSIS METHOD:\n"
    "1. READ the pair name from chart header/title area.\n"
    "2. Focus on the LAST 3-5 candles — their body size, wick length, and color pattern.\n"
    "3. Identify the IMMEDIATE micro-trend (last 5 candles direction, not overall trend).\n"
    "4. Look for reversal signals: long wicks rejecting a level, engulfing patterns, doji at extremes.\n"
    "5. Check if price is at a key support/resistance zone (where price bounced before).\n"
    "6. If visible, check indicators: RSI overbought(>70)=PUT, oversold(<30)=CALL. MACD crossover direction. EMA crossovers.\n"
    "7. Check for momentum exhaustion: shrinking candle bodies = trend weakening = possible reversal.\n\n"
    "DECISION RULES FOR HIGH ACCURACY:\n"
    "- Strong trend with big candles in same direction → follow the trend (CALL if bullish, PUT if bearish)\n"
    "- Price hit support + bullish candle pattern → CALL\n"
    "- Price hit resistance + bearish candle pattern → PUT\n"
    "- Doji/hammer after a downtrend → CALL (reversal)\n"
    "- Shooting star/inverted hammer after uptrend → PUT (reversal)\n"
    "- 3+ consecutive same-color candles with shrinking bodies → expect reversal\n"
    "- Long wick rejection candle → trade OPPOSITE direction of the wick\n"
    "- If signals conflict, choose the direction supported by most evidence and lower your confidence.\n"
    "- Be honest: if chart is unclear, set confidence to 50-60. Only use 80+ when multiple signals align.\n\n"
    "RESPOND in EXACTLY this format (one item per line, no extra text):\n"
    "DIRECTION: CALL or PUT\n"
    "CONFIDENCE: number between 50-99\n"
    "PATTERNS: detected patterns (e.g., Bullish Engulfing, Pin Bar rejection)\n"
    "TREND: Bullish or Bearish or Sideways (based on last 5-10 candles)\n"
    "SUPPORT: price level (e.g., 1.0845)\n"
    "RESISTANCE: price level (e.g., 1.0890)\n"
    "INDICATORS: visible indicators and readings (e.g., RSI: 28 oversold)\n"
    "REASON: concise explanation of WHY this direction, referencing specific patterns and price action\n"
    "PAIR: pair name from chart (e.g., EUR/USD, BTCUSD-OTC)\n")


def _compress_image(photo_bytes: bytes) -> bytes:
    """Compress image to reduce size before sending to API. Returns raw bytes."""
    try:
        img = Image.open(_io.BytesIO(photo_bytes))
        max_dim = 1024
        if img.width > max_dim or img.height > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        if img.mode == 'RGBA':
            img = img.convert('RGB')
        buf = _io.BytesIO()
        img.save(buf, format='JPEG', quality=75)
        return buf.getvalue()
    except Exception as e:
        print(f"Image compression failed, using raw: {e}")
        return photo_bytes


def _parse_analysis_response(text_response: str) -> dict:
    """Parse structured fields from AI text response."""
    result = {"raw": text_response}
    for line in text_response.split("\n"):
        line = line.strip()
        if line.upper().startswith("DIRECTION:"):
            val = line.split(":", 1)[1].strip().upper()
            result["direction"] = "CALL" if "CALL" in val else "PUT" if "PUT" in val else val
        elif line.upper().startswith("CONFIDENCE:"):
            try:
                num = re.search(r'\d+', line.split(":", 1)[1])
                result["confidence"] = int(num.group()) if num else 75
            except Exception:
                result["confidence"] = 75
        elif line.upper().startswith("PATTERNS:"):
            result["patterns"] = line.split(":", 1)[1].strip()
        elif line.upper().startswith("TREND:"):
            result["trend"] = line.split(":", 1)[1].strip()
        elif line.upper().startswith("SUPPORT:"):
            result["support"] = line.split(":", 1)[1].strip()
        elif line.upper().startswith("RESISTANCE:"):
            result["resistance"] = line.split(":", 1)[1].strip()
        elif line.upper().startswith("INDICATORS:"):
            result["indicators"] = line.split(":", 1)[1].strip()
        elif line.upper().startswith("REASON:"):
            result["reason"] = line.split(":", 1)[1].strip()
        elif line.upper().startswith("PAIR:"):
            result["pair"] = line.split(":", 1)[1].strip()
    if "direction" not in result:
        result["direction"] = "CALL" if "CALL" in text_response.upper() else "PUT"
    if "confidence" not in result:
        result["confidence"] = 75
    return result


def _gemini_analyze_chart(image_bytes: bytes) -> dict:
    """Send chart image to OpenRouter Vision API with fallback models."""
    if not OPENROUTER_API_KEY:
        return {
            "error": "OPENROUTER_API_KEY not set. Add it as environment variable on Render."}
    compressed = _compress_image(image_bytes)
    b64 = base64.b64encode(compressed).decode('utf-8')
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    last_error = None
    for model_name in OPENROUTER_MODELS:
        for attempt in range(2):
            try:
                print(f"Trying {model_name} (attempt {attempt + 1}/2)...")
                resp = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json={
                        "model": model_name,
                        "messages": [{"role": "user", "content": [
                            {"type": "text", "text": CHART_ANALYSIS_PROMPT},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
                        ]}],
                        "max_tokens": 1024,
                        "temperature": 0.3,
                    },
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("choices"):
                        text_response = data["choices"][0]["message"]["content"]
                        if text_response:
                            print(
                                f"OpenRouter success on {model_name} (attempt {
                                    attempt + 1})")
                            return _parse_analysis_response(text_response)
                        last_error = "Empty response"
                        continue
                if resp.status_code == 429:
                    wait_sec = (attempt + 1) * 5
                    print(
                        f"OpenRouter 429 on {model_name}, waiting {wait_sec}s")
                    time.sleep(wait_sec)
                    continue
                last_error = f"HTTP {resp.status_code}: {resp.text[:150]}"
                print(f"OpenRouter {model_name} error: {last_error}")
                break
            except requests.exceptions.Timeout:
                print(
                    f"OpenRouter {model_name} timeout, attempt {
                        attempt + 1}/2")
                last_error = "Request timeout"
                continue
            except Exception as e:
                last_error = str(e)
                print(f"OpenRouter {model_name} error: {e}")
                break
        print(f"Model {model_name} failed, trying next...")
    return {"error": f"All AI models failed. Last error: {last_error}"}


def _build_chart_analyzer_msg(result: dict) -> str:
    """Build formatted message from Gemini chart analysis result."""
    if "error" in result:
        return (
            "❀° ┄────────=─────────╮\n"
            "   📸 𝙲𝙷𝙰𝚁𝚃 𝙰𝙽𝙰𝙻𝚈𝚉𝙴𝚁 📸\n"
            "╰────────=───=─────┄ °❀\n\n"
            f"❌ Analysis failed: {result['error']}\n"
            "⏳ Please try again with a clearer chart screenshot."
        )
    direction = result.get("direction", "CALL")
    confidence = result.get("confidence", 75)
    patterns = result.get("patterns", "None detected")
    trend = result.get("trend", "Unknown")
    support = result.get("support", "N/A")
    resistance = result.get("resistance", "N/A")
    indicators = result.get("indicators", "N/A")
    reason = result.get("reason", "Based on chart analysis")
    pair = result.get("pair", "UNKNOWN")
    dir_emoji = "📉" if direction == "CALL" else "📈"
    now_utc5 = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_time = (
        now_utc5.replace(
            second=0,
            microsecond=0) +
        timedelta(
            minutes=1)).strftime("%H:%M")
    conf_bar = "█" * (confidence // 10) + "░" * (10 - confidence // 10)
    msg = (
        f"❀° ┄────────=─────────╮\n"
        f"   📸 𝙲𝙷𝙰𝚁𝚃 𝙰𝙽𝙰𝙻𝚈𝚉𝙴𝚁 📸\n"
        f"╰────────=───=─────┄ °❀\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"📊 Pair∶— {fancy_font(pair)}\n"
        f"{dir_emoji} Direction∶— {fancy_font(direction)}\n"
        f"💎 Confidence∶— {fancy_font(str(confidence) + '%')}\n"
        f"⏰ Entry∶— {fancy_font(entry_time)}\n"
        f"📊 [{conf_bar}] {confidence}%\n"
        f"┗───˚⊹ ─────────♡───┛\n\n"
        f"🔥 𝙰𝚗𝚊𝚕𝚢𝚜𝚒𝚜 𝙳𝚎𝚝𝚊𝚒𝚕𝚜\n"
        f"📈 Trend∶ {fancy_font(trend)}\n"
        f"🔰 Patterns∶ {fancy_font(patterns)}\n"
        f"💲 Support∶ {fancy_font(support)}\n"
        f"🚀 Resistance∶ {fancy_font(resistance)}\n"
        f"📺 Indicators∶ {fancy_font(indicators)}\n\n"
        f"🤖 𝚁𝚎𝚊𝚜𝚘𝚗\n"
        f"💪 {reason}\n\n"
        f"⚠️ Trade on next 1-min candle at {fancy_font(entry_time)}\n"
        f"✨ ©OWNER @Rohailtrader ✨"
    )
    return msg


async def chart_analyzer_photo_handler(
        update: Update,
        context: ContextTypes.DEFAULT_TYPE):
    """Handle photo messages when user is in Chart Analyzer mode."""
    if not update.effective_user:
        return
    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
    state = context.user_data.get('state')
    if state != STATE_CHART_ANALYZER:
        return
    if not update.message.photo:
        return
    last_use = _chart_analyzer_cooldown.get(uid, 0)
    if time.time() - last_use < 40:
        wait_left = int(40 - (time.time() - last_use))
        cool_msg = f"⏳ Please wait {wait_left} seconds before sending another chart."
        entities = build_custom_emoji_entities(cool_msg)
        await update.message.reply_text(cool_msg, entities=entities)
        return
    _chart_analyzer_cooldown[uid] = time.time()
    today = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d")
    if uid in _chart_analyzer_daily_usage:
        _chart_analyzer_daily_usage[uid] = [
            d for d in _chart_analyzer_daily_usage[uid] if d == today]
    else:
        _chart_analyzer_daily_usage[uid] = []
    if len(_chart_analyzer_daily_usage[uid]) >= CHART_DAILY_LIMIT:
        limit_msg = f"\u26a0\ufe0f Daily limit reached! You can analyze {CHART_DAILY_LIMIT} charts per day.\n\u23f3 Limit resets at midnight (UTC+5)."
        entities = build_custom_emoji_entities(limit_msg)
        await update.message.reply_text(limit_msg, entities=entities)
        return
    _chart_analyzer_daily_usage[uid].append(today)
    remaining = CHART_DAILY_LIMIT - len(_chart_analyzer_daily_usage[uid])
    wait_msg = f"\U0001f4f8 \U0001d672\U0001d691\U0001d68a\U0001d69b\U0001d69d \U0001d69b\U0001d68e\U0001d68c\U0001d68e\U0001d692\U0001d69f\U0001d68e\U0001d68d! \U0001f916 \U0001d670\U0001d697\U0001d68a\U0001d695\U0001d6a2\U0001d6a3\U0001d692\U0001d697\U0001d690 \U0001d6a0\U0001d692\U0001d69d\U0001d691 \U0001d682\U0001d67c\U0001d689 \U0001d670\U0001d678...\n\u23f3 Please wait 10-20 seconds... ({remaining} analyses left today)"
    entities = build_custom_emoji_entities(wait_msg)
    processing_msg = await update.message.reply_text(wait_msg, entities=entities)
    try:
        photo = update.message.photo[-1]
        photo_file = await context.bot.get_file(photo.file_id)
        photo_bytes = await photo_file.download_as_bytearray()
        result = _gemini_analyze_chart(bytes(photo_bytes))
        analysis_msg = _build_chart_analyzer_msg(result)
        entities = build_custom_emoji_entities(analysis_msg)
        await context.bot.send_message(chat_id=uid, text=analysis_msg, entities=entities)
        try:
            await processing_msg.delete()
        except Exception:
            pass
        follow_up = "📸 Send another chart screenshot to analyze, or use /stop to return to menu."
        f_entities = build_custom_emoji_entities(follow_up)
        await context.bot.send_message(chat_id=uid, text=follow_up, entities=f_entities)
    except Exception as e:
        print(f"Chart analyzer error: {e}")
        err_msg = f"❌ Error analyzing chart: {
            str(e)}\n⏳ Please try again with a different screenshot."
        entities = build_custom_emoji_entities(err_msg)
        await context.bot.send_message(chat_id=uid, text=err_msg, entities=entities)


# ══════════════ MAIN FUNCTION ══════════════
async def aifilter_conf_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    if data == "aifilter_conf_cancel":
        context.user_data["state"] = None
        await query.edit_message_text("❌ AI Filter cancelled.")
        return
    if data == "aifilter_conf_low":
        threshold = 65
    elif data == "aifilter_conf_medium":
        threshold = 75
    else:
        threshold = 85
    signals_text = context.user_data.get("ai_filter_signals", "")
    if not signals_text:
        await query.edit_message_text("❌ No signals found. Please start again.")
        context.user_data["state"] = None
        return
    context.user_data["state"] = STATE_AI_FILTER_RUNNING
    await query.edit_message_text(f"⏳ Running LOCAL AI Filter with {threshold}% confidence threshold...\n⏰ This may take 20-30 seconds depending on number of signals.")
    threading.Thread(target=run_ai_filter_pattern_match, args=(uid, signals_text, threshold, context), daemon=True).start()

def main():
    global bot_instance
    init(autoreset=True)
    print(f"{Fore.CYAN}{'█' * 100}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}✅ Access Granted!{Style.RESET_ALL}")

    # Use environment variables (already defined at top of file)
    if not BOT_TOKEN or not USER_API_ID or not USER_API_HASH:
        raise ValueError("Missing required environment variables: BOT_TOKEN, USER_API_ID, USER_API_HASH")

    API_ID = int(USER_API_ID)
    API_HASH = USER_API_HASH
    BOT_TOKEN_SENDER = BOT_TOKEN
    sender.start_with_bot_token(API_ID, API_HASH, BOT_TOKEN_SENDER)

    app = Application.builder().token(BOT_TOKEN).build()
    bot_instance = app.bot

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^blk_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^bl_"))
    app.add_handler(
        CallbackQueryHandler(
            checker_date_callback,
            pattern="^checker_"))

    # Conversation handler for strategy selection (includes MM and all
    # strategy inputs)
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(strategy_callback, pattern=r"^strat_")],
        states={
            STATE_MM_PROMPT: [CallbackQueryHandler(mm_prompt_callback, pattern=r"^mm_")],
            STATE_MM_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, mm_balance_received)],
            STATE_MM_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, mm_tp_received)],
            STATE_MM_SL: [MessageHandler(filters.TEXT & ~filters.COMMAND, mm_sl_received)],
            S2_FILTER_CHOICE: [CallbackQueryHandler(s2_filter_choice, pattern=r"^s2_filters_")],
            S2_FILTER_TOGGLE: [CallbackQueryHandler(s2_filter_toggle, pattern=r"^s2_")],
            S2_ACCURACY: [MessageHandler(filters.TEXT & ~filters.COMMAND, s2_accuracy_received)],
            S3_ACCURACY: [MessageHandler(filters.TEXT & ~filters.COMMAND, s3_accuracy_received)],
            S3_LOOKBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, s3_lookback_received)],
            S4_ACCURACY: [MessageHandler(filters.TEXT & ~filters.COMMAND, s4_accuracy_received)],
            S5_SCORE: [MessageHandler(filters.TEXT & ~filters.COMMAND, s5_score_received)],
            S6_SCORE: [MessageHandler(filters.TEXT & ~filters.COMMAND, s6_score_received)],
            S6_MIN_CANDLES: [MessageHandler(filters.TEXT & ~filters.COMMAND, s6_candles_received)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )
    app.add_handler(conv_handler)

    # Other command handlers
    async def checker_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_authorized(uid):
            await update.message.reply_text("⛔ Access denied.")
            return
        context.user_data['state'] = STATE_CHECKER_CUSTOM_DATE
        context.user_data['strategy_active'] = False
        await update.message.reply_text("📅 Enter the date for verification (YYYY-MM-DD):")

    async def future_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not is_authorized(uid):
            await update.message.reply_text("⛔ Access denied.")
            return
        context.user_data['state'] = STATE_FUT_MIN_CONF
        context.user_data['strategy_active'] = False
        await update.message.reply_text("😈 Enter minimum confidence % (0-100):")
    app.add_handler(CommandHandler("checker", checker_cmd))
    app.add_handler(CommandHandler("futuresignal", future_cmd))
    app.add_handler(CallbackQueryHandler(auto_signal_format_callback, pattern="^auto_signal_fmt"))
# ONLY this handler for testing
    app.add_handler(CallbackQueryHandler(auto_trade_start, pattern="^auto_trade_start$"))
    app.add_handler(
        MessageHandler(
            filters.PHOTO,
            chart_analyzer_photo_handler))
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            global_text_handler))
    app.add_handler(
        CallbackQueryHandler(
            menu_callback,
            pattern="^fut_strategy_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^alc_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^fut_strategy_3$"))
    app.add_handler(CallbackQueryHandler(fut_pair_callback, pattern="^pair_"))
    app.add_handler(CallbackQueryHandler(smz_tf_callback, pattern="^smz_tf_"))
    app.add_handler(
        CallbackQueryHandler(
            smz_pair_callback,
            pattern="^smz_pair_"))
    app.add_handler(CallbackQueryHandler(auto_strategy_callback, pattern="^autostrat_"))
    app.add_handler(
        CallbackQueryHandler(
            smz_pair_callback,
            pattern="^smz_pickpair_"))
    app.add_handler(CallbackQueryHandler(auto_strategy_callback, pattern="^autostrat_"))
    app.add_handler(CallbackQueryHandler(auto_s2_filter_choice, pattern="^auto_s2_filters_"))
    app.add_handler(CallbackQueryHandler(auto_s2_filter_toggle, pattern="^auto_s2_"))
    app.add_handler(CallbackQueryHandler(auto_control_callback, pattern="^auto"))
    app.add_handler(
        CallbackQueryHandler(
            smz_pair_callback,
            pattern="^smz_pairpage_"))
    app.add_handler(
        CallbackQueryHandler(
            aifilter_conf_callback,
            pattern="^aifilter_conf_"))
    app.add_handler(
        CallbackQueryHandler(
            font_style_callback,
            pattern="^font_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^show_loss_candles$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^back_to_main$"))
    app.add_handler(CommandHandler("continue", continue_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(
        CallbackQueryHandler(
            backtest_mtg_callback,
            pattern="^backtest_mtg_"))
    app.add_handler(
        CallbackQueryHandler(
            backtest_days_callback,
            pattern="^backtest_days_"))
    app.add_handler(CallbackQueryHandler(restricted_main_menu_callback, pattern="^restricted_main_menu$"))
    app.add_handler(
        CallbackQueryHandler(
            aifilter_conf_callback,
            pattern="^aifilter_conf_"))
    # Auto Trade Handlers
    app.add_handler(
        CallbackQueryHandler(
            auto_account_cb,
            pattern=r"^atx_acc_(demo|real)$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_strategy_cb,
            pattern=r"^atx_strat_\d+$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_s2_filter_choice,
            pattern=r"^atx_s2_filters_(yes|no)$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_s2_filter_toggle,
            pattern=r"^atx_s2_(trend|bb|sr|pa|st|fvg|tr|done)$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_mtg_cb,
            pattern=r"^atx_mtg_(on|off)$"))
    app.add_handler(CallbackQueryHandler(auto_start_cb, pattern="^atx_start$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_cancel_cb,
            pattern="^atx_cancel$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_pause_cb,
            pattern="^atx_(pause|resume)$"))
    app.add_handler(
        CallbackQueryHandler(
            auto_status_cb,
            pattern="^atx_status$"))
    app.add_handler(CallbackQueryHandler(auto_stop_cb, pattern="^atx_stop$"))

    # Checker 2.0 & Backtest 2.0 Handlers
    app.add_handler(CallbackQueryHandler(chk2_utc_callback, pattern="^chk2_utc_"))
    app.add_handler(CallbackQueryHandler(chk2_date_callback, pattern="^chk2_date_"))
    app.add_handler(CallbackQueryHandler(chk2_mtg_callback, pattern="^chk2_mtg_"))
    app.add_handler(CallbackQueryHandler(bt2_utc_callback, pattern="^bt2_utc_"))
    app.add_handler(CallbackQueryHandler(bt2_days_callback, pattern="^bt2_days_"))
    app.add_handler(CallbackQueryHandler(bt2_mtg_callback, pattern="^bt2_mtg_"))

    print(f"{Fore.GREEN}[✓] Bot polling...{Style.RESET_ALL}")
    app.run_polling()


if __name__ == "__main__":
   # keep_alive()   # start uptime server thread
    main()



async def font_style_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    original_text = context.user_data.get('font_text', '')
    if not original_text:
        await query.edit_message_text("❌ No text found. Please start again.")
        return
    if data == "font_mono":
        formatted_lines = [f"<code>{line}</code>" for line in original_text.split('\n')]
        formatted = "\n".join(formatted_lines)
        await query.edit_message_text("✅ Monospace style applied!")
        await context.bot.send_message(chat_id=uid, text=formatted, parse_mode='HTML')
    elif data == "font_sans_bold":
        formatted_lines = [f"<b>{line}</b>" for line in original_text.split('\n')]
        formatted = "\n".join(formatted_lines)
        await query.edit_message_text("✅ Sans‑Serif Bold applied!")
        await context.bot.send_message(chat_id=uid, text=formatted, parse_mode='HTML')
    elif data == "font_sans_mono":
        formatted_lines = [fancy_font(line) for line in original_text.split('\n')]
        formatted = "\n".join(formatted_lines)
        await query.edit_message_text("✅ Sans‑Serif Mono applied!")
        await context.bot.send_message(chat_id=uid, text=formatted)
    context.user_data['state'] = None

