#!/usr/bin/env python3
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
from telegram.error import RetryAfter, TimedOut, NetworkError, Forbidden

MAIN_LOOP = None   # will be set in main()
import concurrent.futures as _cf
_CHART_RENDER_EXECUTOR = _cf.ThreadPoolExecutor(max_workers=4, thread_name_prefix="chart-render")

# =============================================
#     GLOBAL OUTBOUND SEND QUEUE (ANTI-FLOOD)
# =============================================
# All Telegram sends (message/photo/edit/delete) funnel through this single
# queue so that when many users run Multi Engine / Auto Signal at the same
# time, we never fire a burst that trips Telegram's flood control. One
# dispatcher thread paces every outgoing call:
#   - per-chat:  at least ~1.05s between two sends to the SAME chat
#   - global:    at least ~1/25s between ANY two sends (≈25 msgs/sec cap)
# This prevents floods proactively instead of just retrying after the fact,
# which is what was causing some users' loops to stall while others kept
# running smoothly.
import queue as _queue
_SEND_QUEUE = _queue.Queue()
_send_pacing_lock = threading.Lock()
_last_send_per_chat = {}
_last_global_send_time = [0.0]
_GLOBAL_MIN_INTERVAL = 1.0 / 25.0
_PER_CHAT_MIN_INTERVAL = 1.05

def _enqueue_send(chat_id, coro_factory, timeout=30, max_retries=6):
    """Queue a Telegram call and block (this calling thread only) until it's
    actually been sent and we have a result. Safe to call from any of the
    per-user background threads (multi engine, auto signal, etc.)."""
    fut = _cf.Future()
    _SEND_QUEUE.put((chat_id, coro_factory, fut, timeout, max_retries))
    try:
        # generous overall wait: queue delay + internal retry/backoff time
        return fut.result(timeout=timeout + max_retries * 12 + 30)
    except Exception as e:
        print(f"[QUEUE WAIT ERROR] chat={chat_id}: {e}")
        return None

def _send_dispatcher_loop():
    while True:
        chat_id, coro_factory, result_future, timeout, max_retries = _SEND_QUEUE.get()
        try:
            with _send_pacing_lock:
                now = time.monotonic()
                wait_chat = _PER_CHAT_MIN_INTERVAL - (now - _last_send_per_chat.get(chat_id, 0.0))
                wait_global = _GLOBAL_MIN_INTERVAL - (now - _last_global_send_time[0])
                wait_needed = max(wait_chat, wait_global, 0.0)
            if wait_needed > 0:
                time.sleep(wait_needed)

            result = sender._run_with_flood_retry(coro_factory, timeout=timeout, max_retries=max_retries)

            with _send_pacing_lock:
                t_now = time.monotonic()
                _last_send_per_chat[chat_id] = t_now
                _last_global_send_time[0] = t_now

            if not result_future.done():
                result_future.set_result(result)
        except Exception as e:
            print(f"[DISPATCH ERROR] chat={chat_id}: {e}")
            if not result_future.done():
                result_future.set_exception(e)
        finally:
            _SEND_QUEUE.task_done()

threading.Thread(target=_send_dispatcher_loop, daemon=True, name="send-dispatcher").start()

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
                async with websockets.connect(url, origin=ORIGIN, ping_interval=60, ping_timeout=30) as ws:
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
                # FIX: TradoWix now sends trade results via balanceUpdate with reason="tradeResult"
                # instead of a separate "tradeResult" message type
                if data and data.get("reason") == "tradeResult":
                    trade_id = data.get("tradeId")
                    if trade_id:
                        # Emit trade result callback with tradeId and new balance
                        result_data = {
                            "tradeId": trade_id,
                            "balance": self.balance,
                            "status": "settled"
                        }
                        # Also update the _results dict for backward compatibility
                        if hasattr(self, '_results') and self._results is not None:
                            self._results[trade_id] = result_data
                        for cb in self._trade_result_callbacks:
                            self._safe_call(cb, result_data)

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

            # FIX: TradoWix now sends trade results as "tradeResultsBatch"
            elif msg_type == "tradeResultsBatch":
                logger.debug(f"[DEBUG] tradeResultsBatch received: {msg}")
                if data and isinstance(data, list):
                    for result_item in data:
                        tid = result_item.get("tradeId")
                        if tid:
                            logger.info(f"[TRADE_RESULT] tradeId={tid}, result={result_item.get('result')}, profit={result_item.get('profit')}")
                            # Call trade result callbacks first
                            for cb in self._trade_result_callbacks:
                                self._safe_call(cb, result_item)

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
      """Send a message over the WebSocket. If it fails, log but do NOT raise."""
      if self._ws_loop and self._ws and self._connected:
         try:
            asyncio.run_coroutine_threadsafe(
                self._ws.send(json.dumps(msg)),
                self._ws_loop,
            ).result(timeout=5)
         except Exception as e:
            logger.error("WS send error: %s", e)
            # Do NOT raise – just log
      else:
        logger.warning("WS not connected or ready; message not sent.")

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
    def _safe_call(cb, *args):
      """Safely call a callback, catching and logging any exception."""
      try:
         cb(*args)
      except Exception as e:
         logger.error("Callback error: %s", e)

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


SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImprbGlianlqemltY2pscHZza3Z3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQxMTE0NzEsImV4cCI6MjA4OTY4NzQ3MX0.aPMtnplXCpMenfdpDAPFcdMd4ccptM2L3C5oCWWC4X4"
SUPABASE_URL = "https://jklibjyjzimcjlpvskvw.supabase.co"
# ================================================================

# ================================================================
#   NEW CANDLE DATA API (mrbeaxt.site) — replaces gm-vlni.onrender.com
#   Differences vs the old API, handled here so nothing downstream
#   has to change:
#     1) mrbeaxt.site returns candles NEWEST-FIRST (index 0 = latest).
#        We reverse it -> oldest-first, so candles[-1] is still "latest"
#        exactly like every existing function in this file expects.
#     2) mrbeaxt.site's 'epoch' field is a normal UTC unix timestamp
#        (same as the old API's 'time' field), so all existing +5/+6
#        hour offset math elsewhere in the file keeps working unchanged.
#        Its human-readable 'time' string, however, is UTC+6 — don't
#        parse that string directly anywhere, use 'epoch'/'time' (numeric).
# ================================================================
NEW_CANDLE_API_BASE = "https://mrbeaxt.site/Qxapi/qx.php"


def to_new_api_pair(pair: str) -> str:
    """Normalize any internal pair spelling (EURUSD_OTC / EURUSD-OTC / EURUSDq /
    EURUSD-OTCq ...) into what the new API expects.
    - OTC pairs  -> 'EURUSD_otc'
    - Forex/Live pairs (no OTC marker in the original name) -> 'EURUSD' (no suffix)
    mrbeaxt.site uses two different endpoints/formats:
      OTC:  ...qx.php?pair=EURUSD_otc&timeframe=M1&count=...
      LIVE: ...qx.php?pair=EURUSD&timeframe=M1&count=...
    Forcing '_otc' on every pair (the old behavior) silently sent Forex Live
    selections to the OTC feed, which is wrong."""
    p = (pair or "").strip().upper()
    is_otc = "OTC" in p
    p = p.replace("_OTC", "").replace("-OTC", "").replace("OTC", "")
    p = p.replace("_", "").replace("-", "")
    if p.endswith("Q"):
        p = p[:-1]
    return f"{p}_otc" if is_otc else p


def to_new_api_timeframe(tf) -> str:
    """Normalize '1m'/'M1'/'5'/'5m' etc into the 'M1'/'M5'/'M15'/'H1' format
    the new API expects."""
    s = str(tf or "M1").strip().upper().replace("MIN", "")
    m = re.match(r'^(\d+)M$', s)
    if m:
        return f"M{m.group(1)}"
    if s and (s[0] in "MH" or s[0] == "D") and not s.isdigit():
        return s
    return f"M{s}" if s.isdigit() else "M1"


def fetch_new_api_candles(pair: str, count: int = 1500, timeframe="M1",
                           retries: int = 3, timeout: int = 20):
    """
    Fetch candles from mrbeaxt.site and normalize the payload so it's a
    drop-in replacement for the old gm-vlni response:
        {'candles': [ {'time': <utc_epoch>, 'open':..,'high':..,'low':..,
                        'close':..,'volume':..,'payout':.., 'readable_time':..}, ... ],
         'payout': '<latest payout as string>'}
    candles is oldest -> newest (candles[-1] is the latest candle), matching
    every existing usage in this file.
    """
    api_pair = to_new_api_pair(pair)
    api_tf = to_new_api_timeframe(timeframe)
    url = f"{NEW_CANDLE_API_BASE}?pair={api_pair}&timeframe={api_tf}&count={count}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data.get('success') or not data.get('data'):
                continue
            raw = data['data']  # newest-first from mrbeaxt.site
            candles = []
            for c in reversed(raw):  # -> oldest-first
                epoch = c.get('epoch')
                if epoch is None:
                    continue
                dt_utc6 = datetime.fromtimestamp(
                    epoch, tz=timezone.utc) + timedelta(hours=6)
                candles.append({
                    'time': epoch,  # numeric UTC epoch — same convention as old API
                    'open': c.get('open'),
                    'high': c.get('high'),
                    'low': c.get('low'),
                    'close': c.get('close'),
                    'volume': c.get('volume', 1),
                    'payout': c.get('payout', 92),
                    # legacy compat for old code that regex-parsed readable_time
                    'readable_time': dt_utc6.strftime(", %H:%M:"),
                })
            if not candles:
                continue
            latest_payout = str(candles[-1].get('payout', 92))
            return {'candles': candles, 'payout': latest_payout}
        except Exception as e:
            print(f"[NEW-API] fetch error for {pair}: {e}")
            time.sleep(1)
    return None
# ================================================================

# ══════════════ CONFIG ═
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")
USER_API_ID = os.environ.get("USER_API_ID")
USER_API_HASH = os.environ.get("USER_API_HASH")
USER_PHONE = os.environ.get("USER_PHONE")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

OWNER_ID = 8520206066

def log_user_to_all_users(update: Update) -> None:
    """Insert or update user in all_users table."""
    user = update.effective_user
    if not user:
        return
    uid = user.id
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "telegram_id": uid,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "last_active": datetime.now(timezone.utc).isoformat()
    }
    # Upsert: update if exists, else insert
    url = f"{SUPABASE_URL}/rest/v1/all_users"
    try:
        requests.post(url, headers=headers, json=data, params={"on_conflict": "telegram_id"}, timeout=5)
    except Exception as e:
        print(f"Failed to log user {uid}: {e}")

def get_all_telegram_ids() -> List[int]:
    """Fetch all telegram_id from all_users table."""
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/all_users?select=telegram_id"
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return [row['telegram_id'] for row in data]
    except Exception:
        pass
    return []

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
        # Non-200 (rate limit, 5xx outage, etc.) is a Supabase/network problem,
        # NOT proof the user is unauthorized — fail OPEN so a transient API
        # hiccup never wipes an active user's session (was previously falling
        # through to `return False` below, which silently killed Multi
        # Engine / Auto Signal loops for random users whenever Supabase
        # hiccuped).
        print(f"Auth check non-200 ({resp.status_code}) for {uid} — failing open")
        return True
    except Exception as e:
        print(f"Auth Bypass Check: {e}")
        return True

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
        self.strategy7_min_score = 65   # Minimum confidence for Strategy 7
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
    st = user_states[uid]
    # Defensive repair: some code paths across features share this same
    # per-uid object, and a stale/duplicate background thread from an
    # older session could have nulled these out. Never hand back a state
    # whose list/dict fields are None — every caller expects to be able
    # to .append()/index into them without checking first.
    if st.signal_history is None:
        st.signal_history = []
    if st.stats is None:
        st.stats = {"wins": 0, "losses": 0}
    return st


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

LIVE_PAIRS_FS = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDUSD", "CADJPY", "CHFJPY",
    "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURUSD",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPUSD",
    "USDCAD", "USDCHF", "USDJPY"
]

# ---------- Backtest Supported Pairs (new API) ----------
BACKTEST_SUPPORTED_PAIRS = [
    "ATOUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "AUDUSD",
    "AVAUSD", "AXSUSD", "BCHUSD", "BNBUSD", "BRLUSD", "BTCUSD",
    "CADCHF", "CADJPY", "CHFJPY", "DASUSD", "DOTUSD", "ETCUSD",
    "ETHUSD", "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY",
    "EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY",
    "GBPNZD", "GBPUSD", "LINUSD", "LTCUSD", "NZDCAD", "NZDCHF",
    "NZDJPY", "NZDUSD", "SOLUSD", "TONUSD", "TRUUSD", "UKBrent",
    "USCrude", "USDARS", "USDBDT", "USDCAD", "USDCHF", "USDCOP",
    "USDDZD", "USDEGP", "USDIDR", "USDINR", "USDJPY", "USDMXN",
    "USDNGN", "USDPHP", "USDPKR", "USDZAR", "XAGUSD", "XAUUSD",
    "XRPUSD", "ZECUSD"
]

# ===== STRATEGY 4 – PATTERN REPLAY PAIR LISTS =====
FUT4_OTC_PAIRS = [
    "ATOUSD_otc", "AUDCAD_otc", "AUDCHF_otc", "AUDJPY_otc",
    "AUDNZD_otc", "AUDUSD_otc", "AVAUSD_otc", "AXSUSD_otc",
    "BCHUSD_otc", "BNBUSD_otc", "BRLUSD_otc", "BTCUSD_otc",
    "CADCHF_otc", "CADJPY_otc", "CHFJPY_otc", "DASUSD_otc",
    "DOTUSD_otc", "ETCUSD_otc", "ETHUSD_otc", "EURAUD_otc",
    "EURCAD_otc", "EURCHF_otc", "EURGBP_otc", "EURJPY_otc",
    "EURNZD_otc", "EURUSD_otc", "GBPAUD_otc", "GBPCAD_otc",
    "GBPCHF_otc", "GBPJPY_otc", "GBPNZD_otc", "GBPUSD_otc",
    "LINUSD_otc", "LTCUSD_otc", "NZDCAD_otc", "NZDCHF_otc",
    "NZDJPY_otc", "NZDUSD_otc", "SOLUSD_otc", "TONUSD_otc",
    "TRUUSD_otc", "UKBrent_otc", "USCrude_otc", "USDARS_otc",
    "USDBDT_otc", "USDCAD_otc", "USDCHF_otc", "USDCOP_otc",
    "USDDZD_otc", "USDEGP_otc", "USDIDR_otc", "USDINR_otc",
    "USDJPY_otc", "USDMXN_otc", "USDNGN_otc", "USDPHP_otc",
    "USDPKR_otc", "USDZAR_otc", "XAGUSD_otc", "XAUUSD_otc",
    "XRPUSD_otc", "ZECUSD_otc"
]

FUT4_LIVE_PAIRS = [
    "AUDCAD", "AUDCHF", "AUDJPY", "AUDUSD", "AXJAUD",
    "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
    "EURGBP", "EURJPY", "EURUSD", "F40EUR", "FTSGBP",
    "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPUSD",
    "HSIHKD", "IBXEUR", "JPXJPY", "STXEUR", "USDCAD",
    "USDCHF", "USDJPY", "XAGUSD", "XAUUSD"
]

FOREX_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "EURJPY", "GBPJPY", "EURAUD", "GBPCAD", "AUDJPY",
    "NZDJPY", "EURCHF", "GBPCHF", "USDCHF", "AUDCAD",
    "AUDCHF", "CADJPY", "CHFJPY", "EURNZD", "GBPAUD",
    "GBPNZD"
]

# ===== FOREX PAIRS FOR NEWS FILTER (LIVE) =====
NEWS_FOREX_PAIRS = {
    "USD": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD"],
    "EUR": ["EURUSD", "EURCAD", "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURNZD"],
    "GBP": ["GBPUSD", "GBPCAD", "GBPJPY", "GBPAUD", "GBPCHF", "GBPNZD"],
    "JPY": ["USDJPY", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY"],
    "CAD": ["USDCAD", "EURCAD", "GBPCAD", "AUDCAD", "CADCHF", "NZDCAD", "CADJPY"],
    "CHF": ["USDCHF", "EURCHF", "GBPCHF", "CADCHF", "NZDCHF", "AUDCHF", "CHFJPY"],
    "AUD": ["AUDUSD", "EURAUD", "GBPAUD", "AUDCAD", "AUDJPY", "AUDCHF", "AUDNZD"],
    "NZD": ["NZDUSD", "EURNZD", "GBPNZD", "AUDNZD", "NZDCAD", "NZDCHF", "NZDJPY"],
    "INR": ["USDINR"],
    "PKR": ["USDPKR"],
    "BDT": ["USDBDT"],
    "MXN": ["USDMXN"],
    "BRL": ["USDBRL"],
    "ZAR": ["USDZAR"],
    "IDR": ["USDIDR"],
    "PHP": ["USDPHP"],
    "NGN": ["USDNGN"],
    "EGP": ["USDEGP"],
    "ARS": ["USDARS"],
    "COP": ["USDCOP"],
    "DZD": ["USDDZD"],
}

# ─── Multi Engine Pair L
MULTI_ENGINE_OTC = [
    "USDBDT-OTC", "USDARS-OTC", "USDINR-OTC", "USDMXN-OTC", "USDNGN-OTC",
    "USDPKR-OTC", "USDIDR-OTC", "BRLUSD-OTC", "NZDUSD-OTC", "GBPNZD-OTC",
    "NZDCAD-OTC", "CADCHF-OTC", "NZDJPY-OTC", "NZDCHF-OTC", "AUDNZD-OTC",
    "XAUUSD-OTC", "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "EURJPY-OTC",
    "USDCAD-OTC", "USDCHF-OTC", "EURGBP-OTC", "EURCHF-OTC", "GBPJPY-OTC",
    "GBPCAD-OTC", "EURCAD-OTC", "AUDCAD-OTC", "USDDZD-OTC",
    "USDEGP-OTC", "USDCOP-OTC", "USDPHP-OTC", "USDZAR-OTC",
    "AUDUSD-OTC", "EURNZD-OTC", "ATOUSD-OTC", "AVAUSD-OTC",
    "AXSUSD-OTC", "BCHUSD-OTC", "BNBUSD-OTC", "ETCUSD-OTC",
    "ETHUSD-OTC", "LINUSD-OTC", "LTCUSD-OTC", "TONUSD-OTC",
    "TRUUSD-OTC", "XRPUSD-OTC", "ZECUSD-OTC", "XAGUSD-OTC"
]



MULTI_ENGINE_LIVE = [
    "AUDCAD","AUDCHF","AUDJPY","AUDUSD","CADJPY","CHFJPY",
    "EURAUD","EURCAD","EURCHF","EURGBP","EURJPY","EURUSD",
    "GBPAUD","GBPCAD","GBPCHF","GBPJPY","GBPUSD",
    "USDCAD","USDCHF","USDJPY"
]

# ─── OTC pairs that are only tradeable weekdays 2‑8 AM ──
LIMITED_HOURS_OTC_PAIRS = [
    "AUDNZD-OTC", "XAUUSD-OTC", "EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC",
    "EURJPY-OTC", "USDCAD-OTC", "USDCHF-OTC", "EURGBP-OTC", "EURCHF-OTC",
    "GBPJPY-OTC", "GBPCAD-OTC", "EURCAD-OTC", "AUDCAD-OTC"
]

# ==================== NEW PTB SENDER (no Telethon) ====================
class PTBSender:
    """Sends messages via the normal Bot API with custom emoji support."""

    def _run_with_flood_retry(self, coro_factory, timeout=30, max_retries=6):
        """Runs an async Telegram call on MAIN_LOOP with automatic handling of
        flood-wait (RetryAfter) and transient network errors — used by every
        send/edit/delete so 50+ concurrent users don't produce 'Send error:'
        when Telegram briefly rate-limits us. coro_factory must be a
        zero-arg callable returning a fresh coroutine each call (a coroutine
        object can't be reused after it's run once)."""
        for attempt in range(max_retries):
            future = asyncio.run_coroutine_threadsafe(coro_factory(), MAIN_LOOP)
            try:
                return future.result(timeout=timeout)
            except RetryAfter as e:
                wait_s = float(getattr(e, 'retry_after', 3)) + 0.5
                print(f"[FLOOD WAIT] sleeping {wait_s}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait_s)
                continue
            except (TimedOut, NetworkError) as e:
                print(f"[SEND RETRY] {type(e).__name__}: {e} (attempt {attempt+1}/{max_retries})")
                time.sleep(1.5 * (attempt + 1))
                continue
            except Forbidden as e:
                print(f"[SEND BLOCKED] user has blocked the bot: {e}")
                return None
            except Exception as e:
                print(f"Send error: {e}")
                return None
        print("[SEND GAVE UP] max retries exceeded")
        return None

    def send_message(self, chat_id, text, buttons=None, entities=None):
        if BOT_INSTANCE is None:
            return None
        if entities is None:
            entities = build_custom_emoji_entities(text)
        def _factory():
            if buttons:
                return BOT_INSTANCE.send_message(
                    chat_id=chat_id,
                    text=text,
                    entities=entities,
                    reply_markup=buttons
                )
            return BOT_INSTANCE.send_message(
                chat_id=chat_id,
                text=text,
                entities=entities
            )
        return _enqueue_send(chat_id, _factory)

    def send_file(self, chat_id, file_path, caption, entities=None, add_bold=False):
        if BOT_INSTANCE is None:
            return None
        if entities is None:
            entities = build_custom_emoji_entities(caption)
        if add_bold and caption:
            bold_entity = MessageEntity(type='bold', offset=0, length=len(caption.encode('utf-16-le'))//2)
            entities.append(bold_entity)
        async def _do_send():
            with open(file_path, 'rb') as f:
                return await BOT_INSTANCE.send_photo(
                    chat_id=chat_id,
                    photo=f,
                    caption=caption,
                    caption_entities=entities
                )
        return _enqueue_send(chat_id, _do_send)

    def edit_message(self, chat_id, msg_id, text, entities=None, buttons=None):
        if BOT_INSTANCE is None:
            return None
        if entities is None:
            entities = build_custom_emoji_entities(text)
        def _factory():
            if buttons:
                return BOT_INSTANCE.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=text,
                    entities=entities,
                    reply_markup=buttons
                )
            return BOT_INSTANCE.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                entities=entities
            )
        return _enqueue_send(chat_id, _factory)

    def delete_message(self, chat_id, msg_id):
        if BOT_INSTANCE is None:
            return None
        def _factory():
            return BOT_INSTANCE.delete_message(chat_id, msg_id)
        return _enqueue_send(chat_id, _factory)

# Global sender object (will be used everywhere)
sender = PTBSender()


import xml.etree.ElementTree as ET

def parse_number(value: str) -> Optional[float]:
    """Extract a number from a string like '0.2%', '1.5M', '-0.1'."""
    if not value or value.strip() == "":
        return None
    try:
        # Remove non-numeric except dot and minus
        cleaned = re.sub(r'[^0-9.\-]', '', value.strip())
        return float(cleaned)
    except ValueError:
        return None

def get_sentiment(event) -> Optional[str]:
    """Return 'POSITIVE' or 'NEGATIVE' based on Actual vs Forecast."""
    title = event.find('title').text or ""
    actual_str = event.find('actual').text or ""
    forecast_str = event.find('forecast').text or ""

    actual = parse_number(actual_str)
    forecast = parse_number(forecast_str)

    if actual is not None and forecast is not None:
        is_positive = actual > forecast
        # Reverse for unemployment/claims
        if "unemployment" in title.lower() or "claims" in title.lower():
            is_positive = not is_positive
        return "POSITIVE" if is_positive else "NEGATIVE"
    return None  # cannot decide

def get_country_flag(currency: str) -> str:
    """Return a flag emoji for a currency code."""
    flags = {
        "USD": "🇺🇸", "EUR": "🇪🇺", "GBP": "🇬🇧", "JPY": "🇯🇵",
        "CAD": "🇨🇦", "CHF": "🇨🇭", "AUD": "🇦🇺", "NZD": "🇳🇿",
        "INR": "🇮🇳", "PKR": "🇵🇰", "BDT": "🇧🇩", "MXN": "🇲🇽",
        "BRL": "🇧🇷", "ZAR": "🇿🇦", "IDR": "🇮🇩", "PHP": "🇵🇭",
        "NGN": "🇳🇬", "EGP": "🇪🇬", "ARS": "🇦🇷", "COP": "🇨🇴",
        "DZD": "🇩🇿",
    }
    return flags.get(currency, "🏳️")

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
    "🚨": 6233314121475956159, "📸": 5854710508065658472,
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
    "🐶":6230840525256136996,  "🐱":6233009573934930514,
    "🐭":6230982053018475627,  "🐹":6217575613251719126,
    "🦊":6233223549205618145,  "🐻":6217521170246274833,
    "🦁":6230743532009691484,  "🪱":6233334230512834618,
    "👙":6230838304758046212,  "🤳":5278411976677014753,
    "👒":6233212399470516841,  "💟":5447666037833087990,
    "🗽":6102539088936575040,  "🗼":6142922561886888325,
    "🏰":6231262660411792851,  "🏯":6231121076814879723,
    "🐙":5276025009947551999,  "🐡":6061984066402524210,
    "🐠":6102488215048952369,  "🐟":6102524232644698570,
    "🐬":5229064374403998351,  "🐳":5965324783728989746,
    "🐊":6149867871896868774,  "🐅":6230853191114693625,
    "🦓":6102714680084537603,  "🍏":6147968770502566069,
    "🍎":6260516004787395610,  "🍐":6102473242792961206,
    "🍋":6242538646075873899,  "⛓":6307665627481903641,
    "🍉":6219650507657453840,  "🍇":6215194556397260680,
    "🍓":6075631367935238315,  "🫐":6147945749477857953,
    "🌶":6217631422056763363,   "🎬":6102870527267839267,
    "🪇":6102609195687747429,  "💰":6298686186600798589,
    "✍️":5458382591121964689,   "🔁":6246733841281586334,
    "🔫":6336818912004412825,
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

def build_bold_entities(text: str, phrases: list) -> list:
    """
    Build MessageEntity(type='bold') objects for each phrase found in text.
    Uses utf-16 code-unit offsets (required by Telegram) and searches for
    each phrase starting after the end of the previous match, so repeated
    phrases in the list are matched left-to-right in order.
    """
    entities = []
    search_from = 0
    for phrase in phrases:
        if not phrase:
            continue
        idx = text.find(phrase, search_from)
        if idx == -1:
            # fallback: try searching from the start in case order/overlap differs
            idx = text.find(phrase)
            if idx == -1:
                continue
        offset = len(text[:idx].encode('utf-16-le')) // 2
        length = len(phrase.encode('utf-16-le')) // 2
        entities.append(MessageEntity(type='bold', offset=offset, length=length))
        search_from = idx + len(phrase)
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

def analyze_strategy7(candles, min_accuracy=60):
    """
    Strategy 7 – Chandelier Exit + EMA20 + RSI (Triple Confirmation, No ADX)
    All three must agree for a signal.
    """
    if not candles or len(candles) < 23:
        return None, None, None

    closes = [float(c['close']) for c in candles]
    highs = [float(c['high']) for c in candles]
    lows = [float(c['low']) for c in candles]
    opens = [float(c['open']) for c in candles]

    # ---------- 1. Chandelier Exit ----------
    atr = calculate_atr(candles, 22)
    if atr is None or atr == 0:
        atr = 0.0001

    highest_high = max(highs[-22:])
    lowest_low = min(lows[-22:])

    long_exit = highest_high - (atr * 3)   # CALL trigger
    short_exit = lowest_low + (atr * 3)    # PUT trigger
    current_close = closes[-1]

    # ---------- 2. EMA 20 ----------
    ema20 = calculate_ema(closes, 20)
    if ema20 is None:
        return None, None, None

    # ---------- 3. RSI ----------
    rsi = calculate_rsi(closes, 14)
    if rsi is None:
        return None, None, None

    # ---------- Triple Confirmation ----------
    direction = None
    confidence = 0

    # CALL: Chandelier + EMA + RSI < 70 (not overbought)
    if current_close > long_exit and current_close > ema20 and rsi < 70:
        direction = "CALL"
        # Strength based on distance from Chandelier in ATR
        strength = (current_close - long_exit) / atr
        confidence = int(60 + min(30, strength * 8))
        # Bonus if RSI is oversold
        if rsi < 30:
            confidence += 5

    # PUT: Chandelier + EMA + RSI > 30 (not oversold)
    elif current_close < short_exit and current_close < ema20 and rsi > 30:
        direction = "PUT"
        strength = (short_exit - current_close) / atr
        confidence = int(60 + min(30, strength * 8))
        if rsi > 70:
            confidence += 5

    if direction is None:
        return None, None, None

    confidence = min(90, confidence)
    if confidence < min_accuracy:
        return None, None, None

    entry_dt = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_dt = entry_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)

    return direction, entry_dt, confidence

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

import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta, timezone
import asyncio

def run_news_filter(uid, context):
    """
    Fetch HIGH/MEDIUM impact news from ForexFactory XML feed.
    Prints all dates in the XML to console for debugging.
    """
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            sender.send_message(uid, f"❌ Failed to fetch news: HTTP {resp.status_code}")
            return

        root = ET.fromstring(resp.content)
        all_events = _parse_xml_events(root)

        # ---- DEBUG: print all dates in the XML ----
        if all_events:
            all_dates = sorted(set(e['event_date'].strftime('%Y-%m-%d') for e in all_events))
            print(f"📅 All dates in feed: {all_dates}")
        else:
            print("⚠️ XML feed contains 0 events.")

        # Filter future events (today to +30 days)
        now_utc = datetime.now(timezone.utc)
        today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = today_utc + timedelta(days=30)

        future_events = []
        for ev in all_events:
            if today_utc <= ev['event_date'] <= end_date:
                future_events.append(ev)

        # Remove duplicates
        seen = set()
        unique = []
        for ev in future_events:
            key = (ev['currency'], ev['title'], ev['event_date'].strftime('%Y-%m-%d'), ev['time_pk'])
            if key not in seen:
                seen.add(key)
                unique.append(ev)

        if not unique:
            if all_events:
                dates = sorted(set(e['event_date'].strftime('%Y-%m-%d') for e in all_events))
                msg = f"✅ No HIGH/MEDIUM impact news in the next 30 days.\n📅 Dates available in: {', '.join(dates)}"
            else:
                msg = "❌ No events found."
            sender.send_message(uid, msg)
            return

        _send_news_output(uid, context, unique)

    except Exception as e:
        sender.send_message(uid, f"❌ News filter error: {str(e)[:200]}")
        print(f"News filter error: {e}")

def _parse_xml_events(root):
    """Parse events from XML root."""
    events = []

    def safe_text(el, tag, default=""):
        child = el.find(tag)
        return child.text if child is not None else default

    def parse_news_date(date_str):
        if not date_str:
            return None
        date_str = date_str.strip()
        formats = [
            "%Y-%m-%d", "%m-%d-%Y", "%b %d, %Y", "%d %b %Y",
            "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def parse_number(value):
        if not value or value.strip() == "":
            return None
        try:
            cleaned = re.sub(r'[^0-9.\-]', '', value.strip())
            return float(cleaned)
        except ValueError:
            return None

    def get_currency(event):
        cur = safe_text(event, 'currency')
        if cur:
            return cur.upper()
        country = safe_text(event, 'country')
        if country:
            country_lower = country.lower()
            if "united states" in country_lower or "usa" in country_lower:
                return "USD"
            if "euro" in country_lower or "european" in country_lower:
                return "EUR"
            if "united kingdom" in country_lower or "uk" in country_lower:
                return "GBP"
            if "japan" in country_lower:
                return "JPY"
            if "canada" in country_lower:
                return "CAD"
            if "switzerland" in country_lower:
                return "CHF"
            if "australia" in country_lower:
                return "AUD"
            if "new zealand" in country_lower:
                return "NZD"
        iso = safe_text(event, 'iso')
        if iso:
            return iso.upper()
        title = safe_text(event, 'title')
        for code in ["USD", "EUR", "GBP", "JPY", "CAD", "CHF", "AUD", "NZD"]:
            if code in title.upper():
                return code
        return None

    for event in root.findall(".//event"):
        date_str = safe_text(event, 'date')
        event_date = parse_news_date(date_str)
        if not event_date:
            continue

        impact = safe_text(event, 'impact').lower()
        if impact not in ["high", "medium"]:
            continue

        currency = get_currency(event)
        if not currency:
            continue

        time_utc = safe_text(event, 'time', "00:00")
        title = safe_text(event, 'title')
        actual = safe_text(event, 'actual')
        forecast = safe_text(event, 'forecast')
        previous = safe_text(event, 'previous')

        # Convert GMT to UTC+5
        try:
            h, m = map(int, time_utc.split(':'))
            if h >= 24:
                h = 0
            pk_h = (h + 5) % 24
            time_pk = f"{pk_h:02d}:{m:02d}"
        except:
            time_pk = time_utc

        sentiment = None
        if actual and forecast:
            act_num = parse_number(actual)
            fore_num = parse_number(forecast)
            if act_num is not None and fore_num is not None:
                is_positive = act_num > fore_num
                if "unemployment" in title.lower() or "claims" in title.lower():
                    is_positive = not is_positive
                sentiment = "POSITIVE" if is_positive else "NEGATIVE"

        events.append({
            'currency': currency,
            'event_date': event_date,
            'time_pk': time_pk,
            'title': title,
            'impact': impact,
            'actual': actual,
            'forecast': forecast,
            'previous': previous,
            'sentiment': sentiment,
        })

    return events

def _send_news_output(uid, context, events):
    flags = {
        "USD": "🇺🇸", "EUR": "🇪🇺", "GBP": "🇬🇧", "JPY": "🇯🇵",
        "CAD": "🇨🇦", "CHF": "🇨🇭", "AUD": "🇦🇺", "NZD": "🇳🇿"
    }

    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today_utc + timedelta(days=30)

    events.sort(key=lambda x: (x['event_date'], x['time_pk']))
    date_range_str = f"{today_utc.strftime('%d %b')} – {end_date.strftime('%d %b %Y')}"

    lines = []
    lines.append("🗞 HIGH/MEDIUM IMPACT NEWS")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📅 {date_range_str} (UTC+5)")
    lines.append("")

    total = 0
    for ev in events:
        total += 1
        date_display = ""
        if ev['event_date'].date() != today_utc.date():
            date_display = f" [{ev['event_date'].strftime('%d %b')}]"

        flag = flags.get(ev['currency'], "🏳️")
        lines.append(f"⭐ {ev['time_pk']}{date_display} {flag} {ev['currency']}")
        lines.append(f"   📊 {ev['title']}  ({ev['impact'].upper()})")

        if ev['sentiment']:
            sentiment = ev['sentiment']
            pairs = NEWS_FOREX_PAIRS.get(ev['currency'], [])
            buy_list = []
            sell_list = []
            for pair in pairs:
                if len(pair) >= 6:
                    base = pair[:3]
                    quote = pair[3:]
                else:
                    base = pair[:3]
                    quote = pair[-3:]
                if base == ev['currency']:
                    if sentiment == "POSITIVE":
                        buy_list.append(pair)
                    else:
                        sell_list.append(pair)
                elif quote == ev['currency']:
                    if sentiment == "POSITIVE":
                        sell_list.append(pair)
                    else:
                        buy_list.append(pair)

            if buy_list:
                lines.append(f"   🟢 BUY  → {', '.join(buy_list)}")
            if sell_list:
                lines.append(f"   🔴 SELL → {', '.join(sell_list)}")
        else:
            if ev['event_date'] < now_utc:
                lines.append(f"   ❌ NO ACTUAL DATA")
            else:
                lines.append(f"   ⏳ PENDING (F: {ev['forecast'] or 'N/A'}, P: {ev['previous'] or 'N/A'})")

        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 {total} events found")
    lines.append("⭐ UTC+5 (PAKISTAN)")

    full_msg = "\n".join(lines)

    async def send():
        await context.bot.send_message(chat_id=uid, text=full_msg)

    asyncio.run_coroutine_threadsafe(send(), MAIN_LOOP)

def fetch_blackout_signals(pair, start_time_utc5, end_time_utc5):
    """
    Fetch blackout signal times for a pair within a UTC+5 time range,
    from the dedicated blackout signal API.
    """
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

        data = fetch_new_api_candles(pair, count=2000)
        if not data:
            results.append((f"M1 {pair} {time_str}", "❌", "API error"))
            continue
        try:
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

# ==================== WHITEOUT SIGNAL MODE (COMPLETE) ====================

# Use the same OTC pairs as Strategy 4 (supported by the API)
WHITEOUT_OTC_PAIRS = [pair.replace('_otc', '-OTC').upper() for pair in FUT4_OTC_PAIRS]

STATE_WHITEOUT_DAYS = 500
STATE_WHITEOUT_PAIR_MODE = 501
STATE_WHITEOUT_CUSTOM_PAIR_SELECT = 502
STATE_WHITEOUT_START_TIME = 503
STATE_WHITEOUT_END_TIME = 504

def _build_whiteout_pair_page(page=0, per_page=15, selected=None):
    if selected is None:
        selected = set()
    total = len(WHITEOUT_OTC_PAIRS)
    start = page * per_page
    end = min(start + per_page, total)
    page_pairs = WHITEOUT_OTC_PAIRS[start:end]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        short = pair.replace("-OTC", "")
        label = f"✅ {short}" if pair in selected else short
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"white_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"white_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"white_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "white_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

async def whiteout_days_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid = query.from_user.id

    if data == "white_days_custom":
        context.user_data['state'] = STATE_WHITEOUT_DAYS
        msg = "🔢 Enter number of days (1-7):"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return

    days = int(data.split("_")[-1])
    context.user_data['white_days'] = days
    context.user_data['state'] = STATE_WHITEOUT_PAIR_MODE
    msg = "📊 𝚂𝙴𝙻𝙴𝙲𝚃 𝙿𝙰𝙸𝚁 𝙼𝙾𝙳𝙴"
    buttons = [
        [colored_button(" All OTC Pairs ", "white_pair_all", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
        [colored_button(" Custom Pairs ", "white_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def white_pair_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "white_pair_all":
        context.user_data['white_pairs'] = WHITEOUT_OTC_PAIRS
        context.user_data['state'] = STATE_WHITEOUT_START_TIME
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return

    elif data == "white_pair_custom":
        context.user_data['white_selected_pairs'] = set()
        context.user_data['white_pair_page'] = 0
        await show_white_pair_page(update, context)
        return

    elif data.startswith("white_pairpage_"):
        page = int(data.split("_")[-1])
        context.user_data['white_pair_page'] = page
        await show_white_pair_page(update, context)
        return

    elif data.startswith("white_pickpair_"):
        pair = data.replace("white_pickpair_", "")
        selected = context.user_data.get('white_selected_pairs', set())
        if pair in selected:
            selected.discard(pair)
        else:
            selected.add(pair)
        context.user_data['white_selected_pairs'] = selected
        await show_white_pair_page(update, context)
        return

    elif data == "white_pair_done":
        selected = context.user_data.get('white_selected_pairs', set())
        if not selected:
            await query.answer("❌ Select at least one pair!", show_alert=True)
            return
        context.user_data['white_pairs'] = list(selected)
        context.user_data['state'] = STATE_WHITEOUT_START_TIME
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return

async def show_white_pair_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = context.user_data.get('white_pair_page', 0)
    selected = context.user_data.get('white_selected_pairs', set())
    buttons, page, total_pages = _build_whiteout_pair_page(page, selected=selected)
    msg = f"📊 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛𝚜 (𝙿𝚊𝚐𝚎 {page+1}/{total_pages})\n\n💎 𝚃𝚊𝚙 𝚝𝚘 𝚝𝚘𝚐𝚐𝚕𝚎, 𝚝𝚑𝚎𝚗 𝙳𝚘𝚗𝚎"
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))

def run_whiteout_signals(uid, context):
    """WHITEOUT: Only streak-based, per-pair limit 15."""
    try:
        import time as ttime
        days = context.user_data.get('white_days', 2)
        pairs = context.user_data.get('white_pairs', [])
        start_time = context.user_data.get('white_start_time', '00:00')
        end_time = context.user_data.get('white_end_time', '23:59')

        if not pairs:
            sender.send_message(uid, "❌ No pairs selected.")
            return

        now_utc5 = datetime.now(timezone(timedelta(hours=5)))
        date_list = []
        for i in range(1, days + 1):
            d = now_utc5 - timedelta(days=i)
            date_list.append(d.strftime("%Y-%m-%d"))

        sh, sm = map(int, start_time.split(':'))
        eh, em = map(int, end_time.split(':'))
        start_min = sh * 60 + sm
        end_min = eh * 60 + em

        progress_msg = sender.send_message(uid, "⏳ Whiteout scanning...")
        if not progress_msg:
            return
        msg_id = progress_msg.id

        # ---------- CONFIG ----------
        # Tier 1: Streak 4 (hard)
        HARD_STREAK = 9
        # Tier 2: Streak 3 (soft fallback)
        SOFT_STREAK = 3
        MAX_PER_PAIR = 7

        def fetch_candles(pair):
            api_base = pair.replace("-OTC", "")
            api_pair = api_base + "_otc"
            url = f"https://a39605-e545.a.jrnm.app/{api_pair}"
            for attempt in range(3):
                try:
                    resp = requests.get(url, timeout=15)
                    if resp.status_code == 200:
                        data = resp.json()
                        return data.get('candles', [])
                except:
                    time.sleep(1)
            return []

        def parse_candles(candles):
            lookup = {}
            for c in candles:
                time_str = c.get('time', '')
                if not time_str:
                    continue
                try:
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    date_key = dt.strftime("%Y-%m-%d")
                    hhmm = dt.strftime("%H:%M")
                    open_p = float(c.get('o', 0))
                    close_p = float(c.get('c', 0))
                    is_green = close_p > open_p
                    is_red = close_p < open_p
                    lookup[(date_key, hhmm)] = {
                        'is_green': is_green,
                        'is_red': is_red,
                    }
                except:
                    continue
            return lookup

        def scan_with_config(streak_req, label):
            candidates = []
            for pair_idx, pair in enumerate(pairs):
                candles = fetch_candles(pair)
                if not candles:
                    continue
                lookup = parse_candles(candles)
                if not lookup:
                    continue

                for date in date_list:
                    current_min = start_min
                    streak_color = None
                    streak_length = 0

                    while current_min <= end_min:
                        h = current_min // 60
                        m = current_min % 60
                        hhmm = f"{h:02d}:{m:02d}"
                        key = (date, hhmm)
                        candle = lookup.get(key)

                        if not candle:
                            current_min += 1
                            continue

                        # Update streak (no body filters)
                        if candle['is_green']:
                            if streak_color == 'green':
                                streak_length += 1
                            else:
                                streak_color = 'green'
                                streak_length = 1
                        elif candle['is_red']:
                            if streak_color == 'red':
                                streak_length += 1
                            else:
                                streak_color = 'red'
                                streak_length = 1
                        else:
                            streak_color = None
                            streak_length = 0

                        if streak_length >= streak_req:
                            direction = "CALL" if streak_color == 'green' else "PUT"
                            # Score: streak length
                            score = streak_length * 10
                            candidates.append({
                                'pair': pair,
                                'time': hhmm,
                                'score': score,
                                'direction': direction,
                                'label': label,
                            })
                            streak_color = None
                            streak_length = 0

                        current_min += 1

                # Progress
                pct = int((pair_idx + 1) / len(pairs) * 100)
                try:
                    sender.edit_message(uid, msg_id, 
                        f"⏳ {label} scanning... {pct}%\n"
                        f"📊 {pair} done\n"
                        f"💎 Found: {len(candidates)}"
                    )
                except:
                    pass

            return candidates

        # ---------- RUN TIER 1 (HARD) ----------
        candidates = scan_with_config(HARD_STREAK, "Hard")

        # ---------- IF ZERO, RUN TIER 2 (SOFT) ----------
        if not candidates:
            sender.edit_message(uid, msg_id, "⏳ No hard signals, trying soft mode...")
            candidates = scan_with_config(SOFT_STREAK, "Soft")

        if not candidates:
            sender.edit_message(uid, msg_id, "❌ No signals found.")
            return

        # ---------- SELECT TOP SIGNALS ----------
        candidates.sort(key=lambda x: x['score'], reverse=True)
        final_signals = []
        pair_count = {}

        for sig in candidates:
            pair = sig['pair']
            if pair not in pair_count:
                pair_count[pair] = 0
            if pair_count[pair] < MAX_PER_PAIR:
                final_signals.append(sig)
                pair_count[pair] += 1

        final_signals.sort(key=lambda x: x['time'])

        # ---------- BUILD OUTPUT ----------
        now_pk = datetime.now(timezone(timedelta(hours=5)))
        date_str = now_pk.strftime("%d-%b-%Y").upper()

        mode_used = "HARD" if final_signals[0].get('label') == "Hard" else "SOFT"

        header = (
            f"🐶{date_str}🐶\n"
            f"🤳WHITEOUT MOON ({mode_used})🤳\n\n"
            f"👒UTC +5:00\n"
            f"🪱TF > 01 MINUTES \n"
            f"🐭1 STEP MARTINGALE  \n\n"
        )

        body_lines = []
        for sig in final_signals:
            display_pair = sig['pair'].replace("-OTC", "") + "_otc"
            body_lines.append(f"☫ {display_pair}•{sig['time']} ;")

        body = "\n".join(body_lines)

        footer = (
            f"\n🚨Entry Rule : Take same direction trade of previous Candle\n\n"
            f"🐻DM @Rohailtrader"
        )

        full_msg = header + body + footer

        entities = build_custom_emoji_entities(full_msg)
        header_len = len(header.encode('utf-16-le')) // 2
        body_end = len((header + body).encode('utf-16-le')) // 2
        footer_len = len(footer.encode('utf-16-le')) // 2
        entities.append(MessageEntity(type='bold', offset=0, length=header_len))
        entities.append(MessageEntity(type='bold', offset=body_end, length=footer_len))

        async def send():
            await context.bot.send_message(chat_id=uid, text=full_msg, entities=entities)

        asyncio.run_coroutine_threadsafe(send(), MAIN_LOOP)

        sender.edit_message(uid, msg_id, 
            f"✅ Whiteout complete!\n"
            f"📊 {len(final_signals)} signals from {len(pair_count)} pairs\n"
            f"⚙️ Mode: {mode_used} (per-pair: {MAX_PER_PAIR})"
        )

    except Exception as e:
        sender.send_message(uid, f"❌ Whiteout error: {str(e)[:200]}")
        print(f"Whiteout error: {e}")

# ══════════════ MESSAGE BUILDERS (MISSING FROM PART 1) ══════════════


def build_signal_message(pair, entry_time, direction, payout, trend_text, tf="M1"):
    dir_emoji = "📉" if direction == "CALL" else "📈"
    return (
        f"❀° ┄────────=─────────╮\n"
        f"   👑 𝚂𝙼𝚉𝚇-𝙰𝙸 𝚅𝟺.𝟹 👑\n"
        f"╰────────=───=─────┄ °❀\n"
        f"┏───♡─────────── ⊹˚───┓\n"
        f"📊 Pair∶— {fancy_font(pair)}\n"
        f"⏳ TimeFrame∶— {fancy_font(tf)}\n"
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

def build_signal_format2(pair, entry_time, direction, tf="M1"):
    dir_emoji = "📉" if direction == "CALL" else "📈"
    return (
        f"📊{pair}\n\n"
        f"⏰Time : {entry_time} (+5 UTC) 🇵🇰\n\n"
        f"⏳Time : {tf}🤔\n\n"
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
    if st.running:
        sender.send_message(uid, "⚠️ A signal session is already running. Use /stop first.")
        return
    st.running = True
    st.stop_requested = False
    # NOTE: do NOT reset signal_history/stats here — this function is also
    # re-entered by /continue for the *next* AI signal within the same
    # session, and resetting here would wipe the running win/loss count.
    # get_state() already guarantees these are never None.

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
    candles, first = bot.fetch_candle_at_time_with_retry(pair, entry_dt_utc5)
    if not candles:
        sender.send_message(
            uid, f"⚠️ {pair}: couldn't fetch candles to verify result (API issue). Skipped.")
        st.running = False
        return
    if not first:
        sender.send_message(
            uid, f"⚠️ {pair}: couldn't find the entry candle to verify result. Skipped.")
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
    candles2, second = bot.fetch_candle_at_time_with_retry(
        pair, entry_dt_utc5 + timedelta(minutes=1))
    if not candles2:
        sender.send_message(
            uid, f"⚠️ {pair} MTG: couldn't fetch candles to verify result (API issue). Skipped.")
        st.running = False
        return
    if not second:
        sender.send_message(
            uid, f"⚠️ {pair} MTG: couldn't find the MTG candle to verify result. Skipped.")
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

def draw_loss_chart(pair, entry_time_str, direction, candles, entry_index=None, user_offset=5):
    """
    Draw a candlestick chart with time labels (in user's selected UTC offset)
    and a red box on entry candle.
    """
    from PIL import Image, ImageDraw, ImageFont
    import uuid
    from datetime import datetime, timezone, timedelta

    if not candles or len(candles) < 2:
        return None

    # Normalize data
    for c in candles:
        ts = c.get('epoch')
        if ts is None:
            ts = c.get('time')
        if ts is None:
            time_str = c.get('time_str') or c.get('time_string')
            if time_str:
                try:
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
                except:
                    ts = 0
            else:
                ts = 0
        try:
            c['time'] = int(ts)
        except (ValueError, TypeError):
            c['time'] = 0
        for key in ['open', 'high', 'low', 'close']:
            val = c.get(key, 0.0)
            try:
                c[key] = float(val)
            except (ValueError, TypeError):
                c[key] = 0.0

    n = len(candles)
    width, height = 800, 500
    margin_left, margin_right = 60, 40
    margin_top, margin_bottom = 40, 60
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom

    candle_width = chart_width // n - 2
    if candle_width < 3:
        candle_width = 3

    prices = [c['high'] for c in candles] + [c['low'] for c in candles]
    p_min = min(prices)
    p_max = max(prices)
    padding = (p_max - p_min) * 0.05
    p_min -= padding
    p_max += padding

    img = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
        font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
    except:
        font = ImageFont.load_default()
        font_bold = font

    def price_to_y(p):
        return margin_top + chart_height - ((p - p_min) / (p_max - p_min) * chart_height)

    # Grid
    step = (p_max - p_min) / 5
    for i in range(6):
        y = price_to_y(p_min + i * step)
        draw.line([(margin_left, y), (width - margin_right, y)], fill='lightgray', width=1)
        price_text = f"{p_min + i * step:.4f}".rstrip('0').rstrip('.')
        draw.text((margin_left - 35, y - 6), price_text, fill='black', font=font)

    # Convert timestamps to user_offset
    tz_user = timezone(timedelta(hours=user_offset))
    times = []
    for c in candles:
        ts = c.get('time', 0)
        if ts:
            dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt_user = dt_utc.astimezone(tz_user)
            times.append(dt_user.strftime("%H:%M"))
        else:
            times.append("??:??")

    # If entry_index not given, try to find by time (entry_time_str is in user's time)
    if entry_index is None:
        entry_index = None
        for idx, t in enumerate(times):
            if t == entry_time_str:
                entry_index = idx
                break

    # Draw candles
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

        draw.line([(x_center, y_high), (x_center, y_low)], fill='black', width=1)

        if cl >= o:
            fill_color = '#00ff00'
            outline = 'darkgreen'
            top = y_close
            bottom = y_open
        else:
            fill_color = '#ff3333'
            outline = 'darkred'
            top = y_open
            bottom = y_close
        draw.rectangle([x_left, top, x_right, bottom], fill=fill_color, outline=outline, width=1)

        # Red box for entry candle
        if idx == entry_index:
            pad = 5
            draw.rectangle([x_left - pad, top - pad, x_right + pad, bottom + pad],
                           outline='red', width=2)

    # Time labels
    for idx, t in enumerate(times):
        x_center = margin_left + idx * (chart_width / n) + (chart_width / n) / 2
        draw.text((x_center - 15, height - margin_bottom + 5), t, fill='black', font=font)

    # Title and footer
    title = f"{pair}  {direction}  LOSS"
    draw.text((width//2 - 80, 10), title, fill='red', font=font_bold)
    draw.text((width//2 - 80, height - 20), "POWERED BY SMZ", fill='gray', font=font)

    path = f"loss_chart_{uuid.uuid4().hex[:8]}.png"
    img.save(path)
    return path

def draw_pro_chart(candles, pair, entry_time, direction, payout, result=None, confidence=0,
                    tf="M1",
                    brand_name="SMZ X NIGHTMARE",
                    show_zones=True, show_levels=True, show_trendline=True, show_channel=True,
                    signal_candle_count=90, result_candle_count=30):
    """
    Pro chart – Signal (signal_candle_count) with arrow + CALL/PUT, Result (result_candle_count) with box.
    Rendered at 2x internal resolution then downsampled -> crisp, realistic look.
    Arrows are drawn as real polygons (not unicode glyphs) so they never fall back to
    a font's missing-character box/circle and always point the correct way.
    """
    from PIL import Image, ImageDraw, ImageFont
    import uuid
    import math

    if not candles or len(candles) < 10:
        return None

    is_result = result is not None
    if is_result:
        # Result charts stay clean like the reference screenshot - no zones/levels/channel clutter
        show_zones = False
        show_levels = False
        show_trendline = False
        show_channel = False
    max_candles = result_candle_count if is_result else signal_candle_count
    if len(candles) > max_candles:
        candles = candles[-max_candles:]
    n = len(candles)

    # ---- Supersampling for quality ----
    SS = 3
    BASE_W, BASE_H = 1560, 820          # final delivered size (matches reference chart proportions)
    W, H = BASE_W * SS, BASE_H * SS

    TOP_HEADER = 70 * SS
    BOTTOM_MARGIN = 50 * SS
    LEFT_MARGIN = 30 * SS
    RIGHT_MARGIN = 150 * SS
    PRICE_TOP = TOP_HEADER + 8 * SS
    PRICE_BOTTOM = H - BOTTOM_MARGIN

    # ---- Colors ----
    BG = (10, 10, 26)
    GRID = (45, 48, 70, 100)
    TXT_WHITE = (230, 235, 250)
    TXT_GRAY = (140, 150, 180)
    GREEN = (38, 166, 154)     # TradingView-style up candle (#26a69a) - matches reference chart
    RED = (239, 83, 80)        # TradingView-style down candle (#ef5350) - matches reference chart
    GOLD = (255, 210, 60)
    BRAND = (100, 200, 255)
    DOTTED_LINE = (255, 255, 255, 200)
    SUPPLY_FILL = (245, 70, 95, 40)
    SUPPLY_LINE = (245, 70, 95)
    DEMAND_FILL = (0, 150, 150, 40)
    DEMAND_LINE = (0, 180, 180)
    TREND_LINE_COL = (255, 170, 40)
    CHANNEL_COL = (120, 120, 245, 160)

    # ---- Fonts (sized for the supersampled canvas) ----
    def F(size):
        return size * SS

    try:
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", F(14))
        font_med = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", F(18))
        font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", F(20))
        font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", F(22))
        font_small_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", F(12))
        font_result = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", F(40))
        font_watermark = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", F(72))
        font_level = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", F(13))
    except:
        font_small = font_med = font_bold = font_big = font_small_bold = font_result = font_watermark = font_level = ImageFont.load_default()

    # ---- Data ----
    closes = [float(c['close']) for c in candles]
    opens = [float(c['open']) for c in candles]
    highs = [float(c['high']) for c in candles]
    lows = [float(c['low']) for c in candles]

    p_min = min(lows)
    p_max = max(highs)
    p_rng = p_max - p_min or 0.0001
    pad = p_rng * 0.08
    p_min -= pad
    p_max += pad
    p_rng = p_max - p_min
    sample = f"{p_max:.10f}"
    dp = max(4, len(sample.split('.')[1]) if '.' in sample else 2)

    img = Image.new('RGB', (W, H), BG)
    draw = ImageDraw.Draw(img)

    # ---- Helpers ----
    def price_to_y(price):
        return int(PRICE_TOP + (PRICE_BOTTOM - PRICE_TOP) - ((price - p_min) / p_rng) * (PRICE_BOTTOM - PRICE_TOP))

    def x_pos(i):
        return int(LEFT_MARGIN + (i / (n - 1)) * (W - LEFT_MARGIN - RIGHT_MARGIN)) if n > 1 else LEFT_MARGIN

    def draw_dashed_line(x1, y1, x2, y2, fill, width=2, dash=8, gap=6):
        dist = math.hypot(x2 - x1, y2 - y1)
        if dist == 0:
            return
        ux, uy = (x2 - x1) / dist, (y2 - y1) / dist
        pos = 0.0
        while pos < dist:
            sx, sy = x1 + ux * pos, y1 + uy * pos
            epos = min(pos + dash, dist)
            ex, ey = x1 + ux * epos, y1 + uy * epos
            draw.line([(sx, sy), (ex, ey)], fill=fill, width=width)
            pos += dash + gap

    def draw_triangle_up(cx, cy, size, color):
        draw.polygon([(cx, cy - size), (cx - size, cy + size), (cx + size, cy + size)], fill=color)

    def draw_triangle_down(cx, cy, size, color):
        draw.polygon([(cx - size, cy - size), (cx + size, cy - size), (cx, cy + size)], fill=color)

    def paste_region(base_img, overlay_rgba, box):
        x1, y1, x2, y2 = [int(v) for v in box]
        x1 = max(0, x1); y1 = max(0, y1)
        x2 = min(base_img.width, x2); y2 = min(base_img.height, y2)
        if x2 <= x1 or y2 <= y1:
            return
        crop = overlay_rgba.crop((x1, y1, x2, y2))
        base_img.paste(crop, (x1, y1), crop)

    # ---- Base grid (horizontal + vertical, denser - matches reference chart's fine grid) ----
    GRID_V = (40, 44, 66, 90)
    h_lines = 14
    for i in range(h_lines + 1):
        price = p_min + (p_rng * i / h_lines)
        y = price_to_y(price)
        if PRICE_TOP < y < PRICE_BOTTOM:
            draw.line([(LEFT_MARGIN, y), (W - RIGHT_MARGIN, y)], fill=GRID, width=1)
            if i % 2 == 0:
                label = f"{price:.{dp}f}"
                draw.text((W - RIGHT_MARGIN + F(6), y - F(7)), label, fill=TXT_GRAY, font=font_small)

    v_cols = 12
    for i in range(v_cols + 1):
        x = int(LEFT_MARGIN + (i / v_cols) * (W - LEFT_MARGIN - RIGHT_MARGIN))
        draw.line([(x, PRICE_TOP), (x, PRICE_BOTTOM)], fill=GRID_V, width=1)

    # subtle border framing the plot area, like the reference chart
    draw.rectangle([LEFT_MARGIN, PRICE_TOP, W - RIGHT_MARGIN, PRICE_BOTTOM], outline=(60, 64, 90), width=1)

    # ================= SUPPLY / DEMAND ZONES =================
    if show_zones:
        overlay = Image.new('RGBA', (W, H), (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)

        supply_top_price = p_max - p_rng * 0.02
        supply_bot_price = p_max - p_rng * 0.09
        demand_top_price = p_min + p_rng * 0.09
        demand_bot_price = p_min + p_rng * 0.03

        sy1, sy2 = price_to_y(supply_top_price), price_to_y(supply_bot_price)
        dy1, dy2 = price_to_y(demand_top_price), price_to_y(demand_bot_price)

        od.rectangle([LEFT_MARGIN, sy1, W - RIGHT_MARGIN, sy2], fill=SUPPLY_FILL)
        od.line([(LEFT_MARGIN, sy1), (W - RIGHT_MARGIN, sy1)], fill=SUPPLY_LINE, width=2 * SS)

        od.rectangle([LEFT_MARGIN, dy1, W - RIGHT_MARGIN, dy2], fill=DEMAND_FILL)
        od.line([(LEFT_MARGIN, dy2), (W - RIGHT_MARGIN, dy2)], fill=DEMAND_LINE, width=2 * SS)

        img = Image.alpha_composite(img.convert('RGBA'), overlay).convert('RGB')
        draw = ImageDraw.Draw(img)

        draw.text((LEFT_MARGIN + F(4), sy1 - F(16)), f"SUPPLY {supply_top_price:.{dp}f}", fill=SUPPLY_LINE, font=font_small_bold)
        draw.text((LEFT_MARGIN + F(4), dy2 + F(4)), f"DEMAND {demand_bot_price:.{dp}f}", fill=DEMAND_LINE, font=font_small_bold)

    # ---- Result chart: figure out entry/gale candle indices + draw glow band BEHIND candles ----
    chart_width = W - LEFT_MARGIN - RIGHT_MARGIN
    candle_width = max(3 * SS, chart_width // n - 2 * SS)

    entry_idx = None
    gale_idx = None
    if is_result and result:
        from PIL import ImageFilter
        for i, c in enumerate(candles):
            if 'time' in c:
                c_time = datetime.fromtimestamp(c['time'], tz=timezone.utc).astimezone(timezone(timedelta(hours=5)))
                date_str = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d")
                entry_dt = datetime.strptime(f"{date_str} {entry_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone(timedelta(hours=5)))
                if abs((c_time - entry_dt).total_seconds()) < 60:
                    entry_idx = i
                    break
        if entry_idx is None:
            entry_idx = n - 1
        gale_idx = min(entry_idx + 1, n - 1)

        is_win = result in ("WIN", "MTG WIN", "GALE WIN")
        glow_color = (0, 230, 140, 90) if is_win else (245, 70, 95, 90)

        if result == "WIN":
            glow_indices = [entry_idx]
        elif result in ("MTG WIN", "GALE WIN"):
            glow_indices = [gale_idx]
        else:  # LOSS - both attempts glow red
            glow_indices = [entry_idx, gale_idx]

        glow_half_w = candle_width * 1.3
        blur_radius = F(16)
        glow_margin = int(blur_radius * 3)  # extra padding so the blur has room to fade at the edges

        # Build and blur a small band-sized image per glow index (not a full-canvas
        # overlay). Allocating/filling a ~4700x2500 image is itself expensive on
        # low-CPU hosts (e.g. Render free tier); a tiny band is essentially free.
        band_h = PRICE_BOTTOM - PRICE_TOP
        for gi in glow_indices:
            gx = x_pos(gi)
            crop_x1 = max(0, int(gx - glow_half_w - glow_margin))
            crop_x2 = min(W, int(gx + glow_half_w + glow_margin))
            band_w = crop_x2 - crop_x1
            if band_w <= 0:
                continue
            band = Image.new('RGBA', (band_w, band_h), (0, 0, 0, 0))
            band_draw = ImageDraw.Draw(band)
            local_x1 = gx - glow_half_w - crop_x1
            local_x2 = gx + glow_half_w - crop_x1
            band_draw.rectangle([local_x1, 0, local_x2, band_h], fill=glow_color)
            band = band.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            img.paste(band, (crop_x1, PRICE_TOP), band)
        draw = ImageDraw.Draw(img)

    # ---- Candles ----
    for i in range(n):
        x = x_pos(i)
        o = opens[i]; h = highs[i]; l = lows[i]; c = closes[i]
        green = c >= o
        bcol = GREEN if green else RED
        wcol = bcol
        yh = price_to_y(h); yl = price_to_y(l)
        yopen = price_to_y(o); yclose = price_to_y(c)
        draw.line([(x, yh), (x, yl)], fill=wcol, width=2 * SS)
        ytop = min(yopen, yclose); ybottom = max(yopen, yclose)
        if ybottom - ytop < 2 * SS:
            ybottom = ytop + 2 * SS
        draw.rectangle([x - candle_width // 2, ytop, x + candle_width // 2, ybottom], fill=bcol, outline=bcol)

    # ================= R / S DOTTED LEVELS =================
    if show_levels:
        r3 = p_max - p_rng * 0.02   # sits on supply zone's top border (same as SUPPLY line)
        r1 = p_max - p_rng * 0.12   # just below supply zone, small gap
        s1 = p_min + p_rng * 0.42
        s2 = p_min + p_rng * 0.12   # just above demand zone, small gap
        s3 = p_min + p_rng * 0.01   # below demand zone's bottom edge, small gap

        levels = [("R3", r3, SUPPLY_LINE), ("R1", r1, TXT_WHITE),
                  ("S1", s1, TXT_WHITE), ("S2", s2, TXT_WHITE), ("S3", s3, TXT_WHITE)]

        for label, price, col in levels:
            y = price_to_y(price)
            if PRICE_TOP < y < PRICE_BOTTOM:
                x_start, x_end = LEFT_MARGIN, W - RIGHT_MARGIN
                x = x_start
                while x < x_end:
                    x2 = min(x + F(8), x_end)
                    draw.line([(x, y), (x2, y)], fill=DOTTED_LINE, width=SS)
                    x += F(14)
                lbl = f"{label} {price:.{dp}f}"
                draw.text((W - RIGHT_MARGIN + F(6), y - F(8)), lbl, fill=col, font=font_level)

    # ================= TREND LINE + CHANNEL =================
    if show_trendline and n > 2:
        xs = list(range(n))
        mean_x = sum(xs) / n
        mean_y = sum(closes) / n
        num = sum((xs[i] - mean_x) * (closes[i] - mean_y) for i in range(n))
        den = sum((xs[i] - mean_x) ** 2 for i in range(n)) or 1
        slope = num / den
        intercept = mean_y - slope * mean_x

        y_start_price = intercept
        y_end_price = slope * (n - 1) + intercept
        x1, y1 = x_pos(0), price_to_y(y_start_price)
        x2, y2 = x_pos(n - 1), price_to_y(y_end_price)
        draw.line([(x1, y1), (x2, y2)], fill=TREND_LINE_COL, width=2 * SS)

        if show_channel:
            residuals = [closes[i] - (slope * xs[i] + intercept) for i in range(n)]
            max_res = max(residuals) if residuals else 0
            min_res = min(residuals) if residuals else 0
            offset_up = max(max_res, p_rng * 0.09)
            offset_dn = min(min_res, -p_rng * 0.09)

            extend = (n - 1) * 0.35
            ext_x1i, ext_x2i = -extend, (n - 1) + extend

            def price_at(xi):
                return slope * xi + intercept

            up1_x, up1_y = int(LEFT_MARGIN + (ext_x1i / (n - 1)) * (W - LEFT_MARGIN - RIGHT_MARGIN)), price_to_y(price_at(ext_x1i) + offset_up)
            up2_x, up2_y = int(LEFT_MARGIN + (ext_x2i / (n - 1)) * (W - LEFT_MARGIN - RIGHT_MARGIN)), price_to_y(price_at(ext_x2i) + offset_up)
            dn1_x, dn1_y = int(LEFT_MARGIN + (ext_x1i / (n - 1)) * (W - LEFT_MARGIN - RIGHT_MARGIN)), price_to_y(price_at(ext_x1i) + offset_dn)
            dn2_x, dn2_y = int(LEFT_MARGIN + (ext_x2i / (n - 1)) * (W - LEFT_MARGIN - RIGHT_MARGIN)), price_to_y(price_at(ext_x2i) + offset_dn)

            draw_dashed_line(up1_x, up1_y, up2_x, up2_y, fill=CHANNEL_COL, width=SS)
            draw_dashed_line(dn1_x, dn1_y, dn2_x, dn2_y, fill=CHANNEL_COL, width=SS)

    # ---- Header: signal chart keeps its bordered brand box; result chart gets a clean centered title ----
    if is_result:
        title_text = pair.upper().replace('_', '-')
        tw = draw.textlength(title_text, font=font_big)
        draw.text(((W - tw) // 2, F(20)), title_text, fill=TXT_WHITE, font=font_big)
    else:
        box_x1, box_y1, box_x2, box_y2 = F(14), F(10), F(230), F(64)
        draw.rectangle([box_x1, box_y1, box_x2, box_y2], outline=BRAND, width=2 * SS)
        draw.text((box_x1 + F(8), box_y1 + F(6)), brand_name, fill=BRAND, font=font_bold)
        draw.text((box_x1 + F(8), box_y1 + F(30)), f"{pair} | {tf}", fill=TXT_GRAY, font=font_small)   # ← NOW SHOWS tf

    # ---- Current Price (label removed - was overlapping candles/signal arrow) ----
    cur_price = closes[-1]

    # ================= SIGNAL ARROW (real triangle, tight to candle) =================
    if not is_result and direction:
        last_x = x_pos(n - 1)
        last_low_y = price_to_y(lows[-1])
        last_high_y = price_to_y(highs[-1])
        tri_size = F(9)          # small, proportionate triangle
        gap = F(6)                # tight gap from wick
        text_gap = F(4)

        if direction == "CALL":
            tri_cy = last_low_y + gap + tri_size
            draw_triangle_up(last_x, tri_cy, tri_size, GREEN)
            tw = draw.textlength("CALL", font=font_big)
            draw.text((last_x - tw // 2, tri_cy + tri_size + text_gap), "CALL", fill=GREEN, font=font_big)
        else:
            tri_cy = last_high_y - gap - tri_size
            draw_triangle_down(last_x, tri_cy, tri_size, RED)
            tw = draw.textlength("PUT", font=font_big)
            draw.text((last_x - tw // 2, tri_cy - tri_size - text_gap - F(22)), "PUT", fill=RED, font=font_big)

    # ---- Result chart: entry line, direction arrow (on candle before entry), and result box/label ----
    if is_result and result:
        # dashed yellow horizontal line at the entry price (open of the trade candle)
        entry_price = opens[entry_idx]
        ey = price_to_y(entry_price)
        draw_dashed_line(LEFT_MARGIN, ey, W - RIGHT_MARGIN, ey, fill=(255, 205, 40, 220), width=SS + 1, dash=F(10), gap=F(6))

        # direction arrow sits on the candle just BEFORE the trade candle, not on the trade candle itself
        prev_idx = max(0, entry_idx - 1)
        prev_x = x_pos(prev_idx)
        tri_size = F(8)
        if direction == "CALL":
            tri_cy = price_to_y(lows[prev_idx]) + F(10) + tri_size
            draw_triangle_up(prev_x, tri_cy, tri_size, GREEN)
        else:
            tri_cy = price_to_y(highs[prev_idx]) - F(10) - tri_size
            draw_triangle_down(prev_x, tri_cy, tri_size, RED)

        # WIN -> box on the entry candle. GALE WIN -> box on the martingale (2nd attempt) candle.
        is_gale_result = result in ("MTG WIN", "GALE WIN")
        box_idx = gale_idx if is_gale_result else entry_idx
        box_x_center = x_pos(box_idx)
        y_high = price_to_y(highs[box_idx])
        y_low = price_to_y(lows[box_idx])
        y_mid = (y_high + y_low) // 2

        box_x1 = box_x_center - F(35); box_x2 = box_x_center + F(35)
        box_y1 = y_high - F(15); box_y2 = y_low + F(15)
        if box_y2 - box_y1 < F(50):
            box_y1 = y_mid - F(25); box_y2 = y_mid + F(25)

        if result == "WIN":
            result_text = "WIN"
            outline_color = (0, 230, 140); box_color = (0, 230, 140, 50)
        elif is_gale_result:
            result_text = "GALE WIN"
            outline_color = (0, 230, 140); box_color = (0, 230, 140, 50)
        else:
            result_text = "LOSS"
            outline_color = (245, 70, 95); box_color = (245, 70, 95, 50)

        pad = 3 * SS + 2
        bx1, by1 = int(box_x1 - pad), int(box_y1 - pad)
        bx2, by2 = int(box_x2 + pad), int(box_y2 + pad)
        overlay = Image.new('RGBA', (bx2 - bx1, by2 - by1), (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        od.rectangle([box_x1 - bx1, box_y1 - by1, box_x2 - bx1, box_y2 - by1], fill=box_color, outline=outline_color, width=3 * SS)
        img.paste(overlay, (bx1, by1), overlay)
        draw = ImageDraw.Draw(img)

        # small bordered label pill above (or below, for PUT) the box - matches the reference chart's compact tag
        tw = draw.textlength(result_text, font=font_bold)
        pad_x, pad_y = F(10), F(6)
        label_h = F(20) + pad_y * 2
        label_w = tw + pad_x * 2
        if direction == "CALL":
            label_y2 = box_y1 - F(8)
            label_y1 = label_y2 - label_h
        else:
            label_y1 = box_y2 + F(8)
            label_y2 = label_y1 + label_h
        label_x1 = box_x_center - label_w // 2
        label_x2 = box_x_center + label_w // 2

        lpad = 2 * SS + 2
        lx1, ly1 = int(label_x1 - lpad), int(label_y1 - lpad)
        lx2, ly2 = int(label_x2 + lpad), int(label_y2 + lpad)
        label_overlay = Image.new('RGBA', (lx2 - lx1, ly2 - ly1), (0, 0, 0, 0))
        ld = ImageDraw.Draw(label_overlay)
        ld.rectangle([label_x1 - lx1, label_y1 - ly1, label_x2 - lx1, label_y2 - ly1], fill=(10, 10, 26, 220), outline=outline_color, width=2 * SS)
        img.paste(label_overlay, (lx1, ly1), label_overlay)
        draw = ImageDraw.Draw(img)

        draw.text((box_x_center - tw // 2, label_y1 + pad_y), result_text, fill=outline_color, font=font_bold)

    # ---- Watermark ----
    watermark_text = "SMZ X"
    wm_color = (255, 255, 255, 22)
    tw = draw.textlength(watermark_text, font=font_watermark)
    wm_x = (W - tw) // 2
    wm_y = (H - F(100)) // 2
    wm_overlay = Image.new('RGBA', (int(tw) + F(20), F(120)), (0, 0, 0, 0))
    wm_draw = ImageDraw.Draw(wm_overlay)
    wm_draw.text((F(10), F(10)), watermark_text, fill=wm_color, font=font_watermark)
    img.paste(wm_overlay, (int(wm_x - F(10)), int(wm_y - F(10))), wm_overlay)
    draw = ImageDraw.Draw(img)

    # ---- Bottom scale + marker ----
    draw.text((LEFT_MARGIN, H - F(24)), "SCALE 100%", fill=TXT_GRAY, font=font_small)
    draw_triangle_up(W - RIGHT_MARGIN + F(50), H - F(20), F(9), GREEN)

    # ---- Time Labels ----
    step = max(1, n // 12)
    for i in range(0, n, step):
        c = candles[i]
        ts = datetime.fromtimestamp(c['time'], tz=timezone.utc).astimezone(timezone(timedelta(hours=5))).strftime("%H:%M")
        tw = draw.textlength(ts, font=font_small)
        draw.text((x_pos(i) - tw // 2, PRICE_BOTTOM + F(4)), ts, fill=TXT_GRAY, font=font_small)

    # ---- Downsample for crisp, realistic anti-aliased final image ----
    img = img.resize((BASE_W, BASE_H), Image.LANCZOS)

    path = f"pro_chart_{uuid.uuid4().hex[:8]}.png"
    img.save(path, quality=100, subsampling=0)
    return path

def _release_chart_memory():
    """
    Force Python + glibc to actually hand freed memory back to the OS.
    Without this, `del`-ing a big PIL/numpy chart buffer only marks it
    free in Python's own heap — glibc's allocator keeps that memory
    reserved for the process (RSS stays high) until something like this
    forces it back. Railway bills on measured RSS (GB-minute average),
    so this directly lowers cost between chart bursts WITHOUT touching
    render resolution, algorithm, or speed — it only runs AFTER a chart
    is already fully rendered/saved/sent.
    """
    import gc
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass  # non-Linux / no glibc — safe to skip, gc.collect() still ran

def safe_draw_pro_chart(*args, **kwargs):
    """
    Wraps draw_pro_chart so any failure (missing font, unexpected data, etc.
    on the deployed host) never kills the auto-signal background thread.
    On failure it prints the full traceback (visible in Render logs) and
    returns None, so the existing 'chart_path is falsy -> send text instead'
    fallback already in auto_signal_loop kicks in automatically.
    """
    import traceback
    try:
        return draw_pro_chart(*args, **kwargs)
    except Exception:
        print("[CHART ERROR] draw_pro_chart failed:")
        traceback.print_exc()
        return None
    finally:
        # Fire-and-forget: cleanup runs in its OWN thread so it can never
        # add even a millisecond of delay to returning chart_path / sending
        # the signal. It runs slightly AFTER the chart is already on its
        # way to the user.
        threading.Thread(target=_release_chart_memory, daemon=True).start()

def safe_draw_pro_chart_timeout(*args, timeout=20, **kwargs):
    """
    Same as safe_draw_pro_chart, but with a hard wall-clock timeout. On slow
    hosts (e.g. Render's free tier, ~0.1 CPU) chart rendering can occasionally
    stall well past what a signal loop should ever wait — since that's a
    HANG, not an exception, safe_draw_pro_chart's try/except alone can't
    catch it and the whole non-stop loop would look "stuck" forever with no
    further signals/results/status updates. This forces a return within
    `timeout` seconds no matter what; on timeout it just returns None so the
    caller falls back to sending the text-only signal/result instead of
    freezing.
    """
    import concurrent.futures
    future = _CHART_RENDER_EXECUTOR.submit(safe_draw_pro_chart, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        print(f"[CHART TIMEOUT] draw_pro_chart exceeded {timeout}s — sending text-only instead of hanging.")
        return None
    except Exception as _e:
        print(f"[CHART ERROR via timeout wrapper] {_e}")
        return None

def build_signal_format3(pair, entry_time, direction, payout, owner_name="@Rohailtrader", tf="M1"):
    """
    Format 3 signal message – complete bold, dynamic direction emoji.
    tf: 'M1' or 'M5' for display.
    """
    if direction == "CALL":
        dir_display = "🍋 𝙲𝙰𝙻𝙻📉"
    else:
        dir_display = "🌶 𝙿𝚄𝚃 📈"
    
    pair_display = pair.replace("-OTC", " OTC").replace("_", " ")
    minutes = tf.replace("M", "")  # e.g., "M1" → "1", "M5" → "5"
    
    msg = (
        f"╭━═━═━═━═━═━═━═━═━═━╮\n"
        f"   🦊 𝐒𝐌𝐙 𝐗 𝐍𝐈𝐆𝐇𝐓𝐌𝐀𝐑𝐄 🦊\n"
        f"╰━═━═━═━═━═━═━═━═━═━╯\n\n"
        f"🦓 𝙰𝚂𝚂𝙴𝚃 ➜ {pair_display}\n"
        f"🐶 𝙴𝙽𝚃𝚁𝚈 ➜ {entry_time}\n"
        f"🐡 𝙴𝚇𝙿𝙸𝚁𝚈 ➜ {minutes} 𝙼𝙸𝙽\n"           # ✅ fixed
        f"🐠 𝚃𝙸𝙼𝙴𝙵𝚁𝙰𝙼𝙴 ➜ {tf}\n"
        f"🐟 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴 ➜ 𝚄𝚃𝙲 +05:00\n"
        f"🐬 𝙿𝙰𝚈𝙾𝚄𝚃 ➜ {payout}%\n"
        f" 🐳𝙳𝙸𝚁𝙴𝙲𝚃𝙸𝙾𝙽 ➜ {dir_display}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🐊 𝙰𝚄𝚃𝙾 𝚁𝙴𝚂𝚄𝙻𝚃 𝙲𝙷𝙴𝙲𝙺\n"
        f"⏳ 𝚁𝙴𝚂𝚄𝙻𝚃 𝙰𝙵𝚃𝙴𝚁 {minutes} 𝙼𝙸𝙽𝚄𝚃𝙴\n"  # ✅ fixed
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🐅 𝙾𝚆𝙽𝙴𝚁 ➜ {owner_name}"
    )
    return msg

def build_result_format3(pair, entry_time, direction, result_type, open_price, close_price, candle_color, wins, losses, owner_name="@Rohailtrader"):
    """
    Format 3 result message – complete bold, dynamic direction emoji.
    owner_name can be customised via settings.
    """
    if direction == "CALL":
        dir_display = "🍋 𝙲𝙰𝙻𝙻📉"
    else:
        dir_display = "🌶 𝙿𝚄𝚃 📈"
    
    if result_type == "WIN":
        result_display = "✅✅✅ 𝚁𝚎𝚜𝚞𝚕𝚝 ✅✅✅"
        mtg_status = "𝗡𝗢𝗡 𝗠𝗧𝗚 𝗦𝗨𝗥𝗘𝗦𝗛𝗢𝗧"
    elif result_type == "MTG WIN":
        result_display = "✅✅✅ 𝚁𝚎𝚜𝚞𝚕𝚝 ✅✅✅"
        mtg_status = "1 𝗠𝗔𝗥𝗧𝗜𝗡𝗚𝗔𝗟𝗘 𝗪𝗜𝗡"
    else:
        result_display = "❌❌❌ 𝚁𝚎𝚜𝚞𝚕𝚝 ❌❌❌"
        mtg_status = "𝗛𝗜𝗧 𝗚𝗔𝗟𝗘"
    
    color_display = "📉 𝙶𝚛𝚎𝚎𝚗" if candle_color == "green" else "📈 𝚁𝚎𝚍"
    total = wins + losses
    win_loss = f"🐠𝚆𝙸𝙽 : {wins}  🍓𝙻𝙾𝚂𝚂 : {losses} [{(int((wins/total)*100) if total>0 else 0)}%]"
    pair_display = pair.replace("-OTC", " OTC").replace("_", " ")
    
    if result_type == "LOSS":
        mtg_display = f"\n{' ' * 10}⛓⛓⛓⛓⛓⛓⛓⛓⛓⛓\n{' ' * 12}{mtg_status}\n{' ' * 10}⛓⛓⛓⛓⛓⛓⛓⛓⛓⛓"
    else:
        mtg_display = f"\n⛓⛓⛓⛓⛓⛓⛓⛓⛓⛓\n🍉{mtg_status}🍉\n⛓⛓⛓⛓⛓⛓⛓⛓⛓⛓"
    
    msg = (
        f"🍏 𝐒𝐌𝐙 𝐗 𝐍𝐈𝐆𝐇𝐓𝐌𝐀𝐑𝐄 🍏\n\n"
        f"{result_display}\n\n"
        f"📊 𝙿𝙰𝙸𝚁              📊 {pair_display}\n"
        f"🍎 𝙴𝙽𝚃𝚁𝚈             🍐 {entry_time}\n"
        f"🗽𝙳𝙸𝚁𝙴𝙲𝚃𝙸𝙾𝙽      {dir_display}\n"
        f"{mtg_display}\n"
        f"┏━━━━━━━━━━━━━━━━━━━━┓\n"
        f"┃ 🍇 𝙾𝚙𝚎𝚗 𝙿𝚛𝚒𝚌𝚎 : {open_price:.5f}\n"
        f"┃ ⏰ 𝙲𝚕𝚘𝚜𝚎 𝙿𝚛𝚒𝚌𝚎 : {close_price:.5f}\n"
        f"┃ 🔰 𝙲𝚊𝚗𝚍𝚕𝚎 𝙲𝚘𝚕𝚘𝚞𝚛 : {color_display}\n"
        f"┗━━━━━━━━━━━━━━━━━━━━┛\n\n"
        f"{win_loss}\n\n"
        f"🫐 𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺 {owner_name}"
    )
    return msg

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
    "USDDZD_OTC",
    "USDCOP_OTC",
    "USDPHP_OTC",
    "USDZAR_OTC",
    "AUDUSD_OTC",
    "GBPNZD_OTC",
    "NZDJPY_OTC",
    "ATOUSD_OTC",
    "AVAUSD_OTC",
    "AXSUSD_OTC",
    "BCHUSD_OTC",
    "BNBUSD_OTC",
    "ETCUSD_OTC",
    "ETHUSD_OTC",
    "LINUSD_OTC",
    "LTCUSD_OTC",
    "TONUSD_OTC",
    "TRUUSD_OTC",
    "XRPUSD_OTC",
    "ZECUSD_OTC",
    "XAGUSD_OTC"]


class SMZXBot:
    def __init__(self, uid):
        self.uid = uid
        st = get_state(uid)
        self.market_type = st.market_type
        self.pairs = st.pairs if st.pairs else DEFAULT_OTC_PAIRS.copy()
        self.base_url = NEW_CANDLE_API_BASE  # unused by fetch_data(), kept for compat
        self.telegram_format = st.telegram_format
        self.strategy = st.strategy
        self.strategy2_filters = st.strategy2_filters if st.strategy2_filters else Strategy2Filters()
        self.strategy3_min_accuracy = st.strategy3_min_accuracy
        self.strategy3_lookback = st.strategy3_lookback
        self.strategy4_min_accuracy = st.strategy4_min_accuracy
        self.strategy5_min_score = st.strategy5_min_score
        self.strategy6_min_score = st.strategy6_min_score
        self.strategy6_min_candles = st.strategy6_min_candles
        self.strategy7_min_score = st.strategy7_min_score
        self.stats = st.stats
        self.signal_history = st.signal_history
        self.last_signal_pair = None
        self.same_pair_count = 0
        self.last_loss = st.last_loss

    def format_pair_for_api(self, pair):
       # Detect if pair is OTC (contains _OTC or -OTC)
       if "_OTC" in pair or "-OTC" in pair:
          return pair.replace("_", "-") + "q"
       else:
        # Forex / Live pairs: just uppercase
          return pair.upper()

    def fetch_data(self, pair, limit=600, timeframe="M1"):
        data = fetch_new_api_candles(pair, count=limit, timeframe=timeframe)
        if data and data.get('candles'):
            candles = data['candles']
            for c in candles:
                if 'volume' not in c:
                    c['volume'] = 1
            return candles, candles[-1]['close'], data.get('payout', '92')
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
        elif self.strategy == 7:
            print("[DEBUG] Inside analyze, calling analyze_strategy7")
            return analyze_strategy7(candles, self.strategy7_min_score)

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
            if 'time' in c and abs(c['time'] - target) < 45:
                return c
        return None

    def fetch_candle_at_time_with_retry(
            self, pair, target_dt_utc5, limit=750, attempts=15, delay=3, timeframe="M1"):
        """
        Right at the moment a candle closes, the API can take a couple of
        seconds to publish it. Retry a few times before giving up instead
        of silently treating it as missing.
        (attempts/delay raised — cloud hosts like Railway see slower/less
        reliable API responses than a local Termux run, so the old 5x2s=10s
        window was too short and was causing frequent false "couldn't find
        candle" skips even though the API itself was fine.)
        """
        candles = None
        for i in range(attempts):
            candles, _, _ = self.fetch_data(pair, limit=limit, timeframe=timeframe)
            if candles:
                found = self.get_candle_at_time(candles, target_dt_utc5)
                if found:
                    return candles, found
            if get_state(self.uid).stop_requested:
                break
            time.sleep(delay)
        return candles, None

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
        candles, first = self.fetch_candle_at_time_with_retry(
            pair, entry_dt_utc5)
        if not candles:
            sender.send_message(
                self.uid,
                f"⚠️ {pair} {entry_dt_utc5.strftime('%H:%M')}: couldn't fetch candles to verify result (API issue). Skipped.")
            return
        if not first:
            sender.send_message(
                self.uid,
                f"⚠️ {pair} {entry_dt_utc5.strftime('%H:%M')}: couldn't find the entry candle to verify result. Skipped.")
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
        candles2, second = self.fetch_candle_at_time_with_retry(
            pair, entry_dt_utc5 + timedelta(minutes=1))
        if not candles2:
            sender.send_message(
                self.uid,
                f"⚠️ {pair} {entry_dt_utc5.strftime('%H:%M')} MTG: couldn't fetch candles to verify result (API issue). Skipped.")
            return
        if not second:
            sender.send_message(
                self.uid,
                f"⚠️ {pair} {entry_dt_utc5.strftime('%H:%M')} MTG: couldn't find the MTG candle to verify result. Skipped.")
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
    """
    Universal signal parser – extracts pair, time, direction from any format.
    Returns (pair, time_str, direction) or (None, None, None).
    """
    if not line:
        return None, None, None

    # Normalize fancy Unicode characters
    line = normalize_fancy(line)

    # Extract time (HH:MM) – keep colon
    time_match = re.search(r'(\d{2}:\d{2})', line)
    if not time_match:
        return None, None, None
    time_str = time_match.group(1)

    # Extract direction (CALL, PUT, BUY, SELL, UP, DOWN) – case-insensitive
    dir_match = re.search(r'\b(CALL|PUT|BUY|SELL|UP|DOWN)\b', line, re.IGNORECASE)
    if not dir_match:
        return None, None, None
    raw_dir = dir_match.group(1).upper()
    if raw_dir in ("BUY", "UP"):
        direction = "CALL"
    elif raw_dir in ("SELL", "DOWN"):
        direction = "PUT"
    else:
        direction = raw_dir

    # Remove time and direction from the line
    rest = re.sub(r'\b' + re.escape(time_str) + r'\b', '', line)
    rest = re.sub(r'\b(CALL|PUT|BUY|SELL|UP|DOWN)\b', '', rest, flags=re.IGNORECASE)

    # Remove timeframe markers like M1, M5, etc.
    rest = re.sub(r'\bM\d+\b', '', rest)

    # Replace common separators (except colon, which we don't need anymore)
    rest = re.sub(r'[×;:,|]', ' ', rest)

    # Remove extra spaces
    rest = re.sub(r'\s+', ' ', rest).strip()

    # Split into tokens and find the pair token
    tokens = rest.split()
    pair = None
    for token in tokens:
        # A pair typically contains letters and digits, maybe underscores/hyphens
        if re.match(r'^[A-Za-z0-9]+(?:[-_][A-Za-z0-9]+)*$', token):
            pair = token.upper()
            break

    if not pair:
        return None, None, None

    # ─── Normalize pair to -OTC format ──────────────
    if "_OTC" in pair:
        pair = pair.replace("_OTC", "-OTC")
    elif "-OTC" in pair:
        pass  # already correct
    elif "OTC" in pair:
        # For rare cases like "BTCUSDO TC"? Not likely, just add -OTC
        pair = pair + "-OTC"

    return pair, time_str, direction

async def run_advanced_checker(update: Update, context: ContextTypes.DEFAULT_TYPE, signals, mtg_level, date_str, user_utc, payout_filter):
    from datetime import datetime, timedelta, timezone
    import re
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import time as ttime

    uid = update.effective_user.id
    date_str_norm = normalize_fancy(date_str)
    date_str_norm = re.sub(r'[—–−]', '-', date_str_norm)
    try:
        date_utc5 = datetime.strptime(date_str_norm, "%Y-%m-%d")
    except:
        sender.send_message(uid, f"❌ Invalid date: {date_str}\nUse YYYY-MM-DD")
        return

    print(f"🗓 CHECKER DATE: {date_str_norm}")
    print(f"🌐 User UTC: +{user_utc}, MTG: {mtg_level}, Payout Filter: {payout_filter}")

    # Convert signal times from user UTC to UTC+5
    tz_user = timezone(timedelta(hours=user_utc))
    tz_api = timezone(timedelta(hours=5))
    converted_signals = []
    for pair, time_str, direction in signals:
        try:
            dt_user = datetime.strptime(f"{date_str_norm} {time_str}", "%Y-%m-%d %H:%M")
            dt_user = dt_user.replace(tzinfo=tz_user)
            dt_api = dt_user.astimezone(tz_api)
            api_time = dt_api.strftime("%H:%M")
            converted_signals.append({
                'display_pair': pair,
                'display_time': time_str,          # original user time
                'api_time': api_time,
                'direction': direction,
                'api_pair': f"{pair.replace('_OTC', '').replace('-OTC', '').replace('_', '').replace('-', '')}_otc"
            })
        except Exception as e:
            print(f"Time conversion error for {pair} {time_str}: {e}")
            continue

    if not converted_signals:
        sender.send_message(uid, "❌ No signals after time conversion.")
        return

    tz_pk = timezone(timedelta(hours=5))
    now_utc5 = datetime.now(tz_pk)

    # ---- Fast fetching (no progress message) ----
    unique_pairs = list({sig['api_pair'] for sig in converted_signals})
    cache = {}
    full_lookup = {}
    lock = threading.Lock()

    def fetch_pair(api):
        result = fetch_new_api_candles(api, count=3000, retries=3, timeout=25)
        if not result:
            return api, None, None
        candles = result.get('candles', [])
        candle_by_start = {}
        ts_lookup = {}
        for c in candles:
            epoch = c.get('time')
            if epoch is None:
                continue
            dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
            dt_pk = dt_utc + timedelta(hours=5)
            date_key = dt_pk.strftime("%Y-%m-%d")
            start_time = dt_pk.strftime("%H:%M")
            c['direction'] = 'up' if c['close'] > c['open'] else 'down'
            c['payout'] = c.get('payout', 92)
            if date_key == date_str_norm:
                candle_by_start[start_time] = c
            if date_key not in ts_lookup:
                ts_lookup[date_key] = {}
            ts_lookup[date_key][start_time] = c
        print(f"🔍 {api} – {len(candle_by_start)} candles for {date_str_norm}")
        return api, candle_by_start, ts_lookup

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_pair, api): api for api in unique_pairs}
        for future in as_completed(futures):
            api, cbs, tsl = future.result()
            with lock:
                cache[api] = cbs
                full_lookup[api] = tsl

    # ---- Process signals ----
    def fmt_sig(pair, time_str, direction):
        return f"M1;{pair.replace('_', '-')};{time_str};{direction}"

    results = []
    pending_count = 0
    win_count = 0
    loss_count = 0
    skipped_count = 0
    loss_data_list = []

    for sig in converted_signals:
        display_pair = sig['display_pair']
        display_time = sig['display_time']          # user local time
        api_time = sig['api_time']
        direction = sig['direction']
        api = sig['api_pair']

        signal_dt = datetime.strptime(f"{date_str_norm} {api_time}", "%Y-%m-%d %H:%M")
        signal_dt = signal_dt.replace(tzinfo=tz_pk)
        if signal_dt > now_utc5:
            results.append((fmt_sig(display_pair, display_time, direction), "⏳"))
            pending_count += 1
            continue

        candle_dict = cache.get(api)
        if not candle_dict:
            results.append((fmt_sig(display_pair, display_time, direction), "❌"))
            loss_count += 1
            continue

        entry = candle_dict.get(api_time)
        if not entry:
            results.append((fmt_sig(display_pair, display_time, direction), "❌"))
            loss_count += 1
            day_lookup = full_lookup.get(api, {}).get(date_str_norm, {})
            if day_lookup:
                loss_data = collect_loss_candles(
                    day_lookup,
                    api_time,           # UTC+5
                    direction,
                    display_pair,
                    display_time=display_time,   # 🔥 user local time
                    user_offset=user_utc
                )
                if loss_data:
                    loss_data_list.append(loss_data)
            continue

        payout = entry.get('payout', 92)
        if payout_filter and payout < 80:
            results.append((f"{fmt_sig(display_pair, display_time, direction)} {payout}%", ""))
            skipped_count += 1
            continue

        candle_dir = entry.get('direction', '').lower()
        win1 = (direction == "CALL" and candle_dir == "up") or (direction == "PUT" and candle_dir == "down")
        if win1:
            results.append((fmt_sig(display_pair, display_time, direction), "✅"))
            win_count += 1
            continue

        # MTG recovery
        if mtg_level > 0:
            win2 = False
            for step in range(1, mtg_level + 1):
                h, m = map(int, api_time.split(':'))
                next_dt = datetime(2000, 1, 1, h, m) + timedelta(minutes=step)
                next_hhmm = next_dt.strftime("%H:%M")
                next_candle = candle_dict.get(next_hhmm)
                if next_candle:
                    next_dir = next_candle.get('direction', '').lower()
                    if (direction == "CALL" and next_dir == "up") or (direction == "PUT" and next_dir == "down"):
                        win2 = True
                        mtg_icon = f"✅{step}" if step > 1 else "✅¹"
                        results.append((fmt_sig(display_pair, display_time, direction), mtg_icon))
                        win_count += 1
                        break
            if win2:
                continue
            # no recovery win -> loss
            results.append((fmt_sig(display_pair, display_time, direction), "❌"))
            loss_count += 1
            day_lookup = full_lookup.get(api, {}).get(date_str_norm, {})
            if day_lookup:
                loss_data = collect_loss_candles(
                    day_lookup,
                    api_time,
                    direction,
                    display_pair,
                    display_time=display_time,
                    user_offset=user_utc
                )
                if loss_data:
                    loss_data_list.append(loss_data)
        else:
            # No MTG
            results.append((fmt_sig(display_pair, display_time, direction), "❌"))
            loss_count += 1
            day_lookup = full_lookup.get(api, {}).get(date_str_norm, {})
            if day_lookup:
                loss_data = collect_loss_candles(
                    day_lookup,
                    api_time,
                    direction,
                    display_pair,
                    display_time=display_time,
                    user_offset=user_utc
                )
                if loss_data:
                    loss_data_list.append(loss_data)

    # 🔥 Debug: print what we stored
    print(f"[DEBUG] Total loss_data_list: {len(loss_data_list)}")
    if loss_data_list:
        print(f"[DEBUG] First loss_data: {loss_data_list[0]}")

    context.user_data['loss_signals'] = loss_data_list

    # ---- Output ----
    total_signals = len(results)
    date_ddmmyyyy = date_utc5.strftime("%d/%m/%Y")
    header = (
        f"{fancy_font('𒆜═〔 SMZ AI CHECKER 〕═𒆜')}\n\n"
        f"{fancy_font('🗓 DATE : ')}{fancy_font(date_ddmmyyyy)}\n\n"
    )
    body = "\n".join([f"{sig} {icon}".rstrip() for sig, icon in results])
    extra_lines = ""
    if pending_count:
        extra_lines += f"\n{fancy_font('⏳ PENDING : ')}{fancy_font(str(pending_count))}"
    if skipped_count:
        extra_lines += f"\n{fancy_font('💲 SKIPPED : ')}{fancy_font(str(skipped_count))}"
    summary = (
        f"\n\n{fancy_font('🩸 TOTAL : ')}{fancy_font(str(total_signals))}\n\n"
        f"{fancy_font('✅ WIN:  ')}{fancy_font(str(win_count))} \n"
        f"{fancy_font('❌ LOSS :  ')}{fancy_font(str(loss_count))}"
        f"{extra_lines}\n\n"
        f"{fancy_font('💬Contact: ')}@Rohailtrader"
    )
    final_msg = header + body + summary

    entities = build_custom_emoji_entities(final_msg)

    # Make header and footer (summary) fully bold, same as the Strategy 4 signals format
    header_len = len(header.encode('utf-16-le')) // 2
    footer_start = len((header + body).encode('utf-16-le')) // 2
    footer_len = len(summary.encode('utf-16-le')) // 2
    entities.append(MessageEntity(type='bold', offset=0, length=header_len))
    entities.append(MessageEntity(type='bold', offset=footer_start, length=footer_len))

    buttons = []
    if loss_data_list:
        buttons.append([colored_button(" Show Loss Candles", "show_loss_candles", KeyboardButtonStyle.PRIMARY, "5809816842713174497")])
    if not payout_filter:
        buttons.append([colored_button(" Check Again With Payout Filter", "checker_recheck_payout", KeyboardButtonStyle.PRIMARY, "6104726047628990417")])
    buttons.append([colored_button(" Home", "back_to_main", KeyboardButtonStyle.SUCCESS, "6145546134069714639")])
    reply_markup = InlineKeyboardMarkup(buttons)

    await context.bot.send_message(
        chat_id=uid,
        text=final_msg,
        entities=entities,
        reply_markup=reply_markup
    )

def collect_loss_candles(day_lookup, entry_time_str, direction, pair_display, display_time=None, user_offset=5):
    """
    Collect up to 6 candles around the entry time.
    Returns a dict with entry_index, candles, and metadata.
    """
    t_h, t_m = map(int, entry_time_str.split(':'))
    entry_min = t_h * 60 + t_m

    # Next candles (up to 2 after entry)
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

    # Previous candles (fill up to total 6 candles)
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
    # entry candle
    entry_candle = day_lookup.get(entry_time_str)
    if entry_candle:
        candles.append(entry_candle)
    for k in next_available:
        candles.append(day_lookup[k])

    entry_index = len(prev_keys)

    # FORCE use display_time if provided, else fallback to entry_time_str
    final_display_time = display_time if display_time is not None else entry_time_str

    # Console debug
    print(f"[collect_loss] pair={pair_display}, entry_time_str={entry_time_str}, display_time={display_time}, final_display_time={final_display_time}, user_offset={user_offset}")

    return {
        'pair': pair_display,
        'entry_time': entry_time_str,           # UTC+5 (api time)
        'display_time': final_display_time,     # user's local time
        'direction': direction,
        'candles': candles,
        'prev_count': len(prev_keys),
        'next_count': next_count,
        'entry_index': entry_index,
        'user_offset': user_offset,
    }

async def show_loss_candles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    loss_data_list = context.user_data.get('loss_signals', [])
    if not loss_data_list:
        await query.answer("No loss signals to display.", show_alert=True)
        return

    # ✅ Force convert each loss_data: use display_time as entry_time
    for loss_data in loss_data_list:
        if 'display_time' in loss_data and loss_data['display_time'] is not None:
            loss_data['entry_time'] = loss_data['display_time']  # override!
        if 'user_offset' not in loss_data:
            loss_data['user_offset'] = context.user_data.get('checker_utc', 5)

    await query.answer("Generating loss charts...")

    for idx, loss_data in enumerate(loss_data_list, 1):
        try:
            pair = loss_data['pair']
            entry_time = loss_data['entry_time']   # ab yeh display_time hi hai
            direction = loss_data['direction']
            candles = loss_data['candles']
            prev_count = loss_data['prev_count']
            next_count = loss_data['next_count']
            entry_index = loss_data.get('entry_index')
            user_offset = loss_data.get('user_offset', 5)

            chart_candles = []
            for c in candles:
                ts = c.get('epoch')
                if ts is None:
                    ts = c.get('time', 0)
                try:
                    ts = int(ts)
                except (ValueError, TypeError):
                    ts = 0
                chart_candles.append({
                    'time': ts,
                    'open': float(c.get('open', 0)),
                    'high': float(c.get('high', 0)),
                    'low': float(c.get('low', 0)),
                    'close': float(c.get('close', 0))
                })

            img_path = draw_loss_chart(
                pair,
                entry_time,
                direction,
                chart_candles,
                entry_index=entry_index,
                user_offset=user_offset
            )

            if img_path and os.path.exists(img_path):
                direction_emoji = "🔺" if direction == "CALL" else "🔻"
                caption = (
                    f"📊 Loss Candle #{idx}\n"
                    f"Entry Time  : {entry_time}\n"
                    f"Market      : {pair}\n"
                    f"Direction   : {direction}{direction_emoji}\n"
                    f"Analysis    : Previous {prev_count} candle + Entry candle + Next {next_count} candles\n"
                    f"🔍 DEBUG: display_time was {loss_data.get('display_time')}"
                )
                await context.bot.send_photo(chat_id=uid, photo=open(img_path, 'rb'), caption=caption)
                os.remove(img_path)
            else:
                await context.bot.send_message(uid, f"Failed to generate chart for {pair} at {entry_time}")
        except Exception as e:
            await context.bot.send_message(uid, f"Error generating chart: {e}")

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
        """
        Same robust, order-independent parsing logic as the local checker
        (module-level parse_signal_line): extract time and direction FIRST,
        strip them out of the line, then look for the pair in what's left.
        This stops time digits (e.g. '10' from '10:30') or words like
        CALL/PUT from being mistaken for the pair when they appear before
        it, which used to cause valid signals to get rejected ("stuck").
        """
        if not line:
            return None, None, None

        line = normalize_fancy(line)

        # Extract time (HH:MM) first
        time_match = re.search(r'(\d{2}:\d{2})', line)
        if not time_match:
            return None, None, None
        time_str = time_match.group(1)

        # Extract direction first (word-boundary, case-insensitive)
        dir_match = re.search(r'\b(CALL|PUT|BUY|SELL|UP|DOWN)\b', line, re.IGNORECASE)
        if not dir_match:
            return None, None, None
        dir_raw = dir_match.group(1).upper()
        if dir_raw in ("BUY", "UP"):
            direction = "CALL"
        elif dir_raw in ("SELL", "DOWN"):
            direction = "PUT"
        else:
            direction = dir_raw

        # Remove time and direction from the line before searching for the pair
        rest = re.sub(r'\b' + re.escape(time_str) + r'\b', '', line)
        rest = re.sub(r'\b(CALL|PUT|BUY|SELL|UP|DOWN)\b', '', rest, flags=re.IGNORECASE)

        # Remove timeframe markers like M1, M5, etc. (anywhere in the line, not just at the start; case-insensitive)
        rest = re.sub(r'\bM\d+\b', '', rest, flags=re.IGNORECASE)

        # Replace common separators
        rest = re.sub(r'[×;:,|]', ' ', rest)
        rest = re.sub(r'\s+', ' ', rest).strip()

        # Find the pair token among what's left
        tokens = rest.split()
        pair_raw = None
        for token in tokens:
            if re.match(r'^[A-Za-z0-9]+(?:[-_][A-Za-z0-9]+)*$', token):
                pair_raw = token.upper()
                break

        if not pair_raw:
            return None, None, None

        # Remove OTC/underscore to get base symbol for API (e.g., ETHUSD)
        pair = pair_raw.replace('-OTC', '').replace('_OTC', '').replace('_', '')
        return pair, time_str, direction

    # Parse signals
    raw_lines = [l.strip() for l in signals_text.split('\n') if l.strip()]
    if not raw_lines:
        await update.message.reply_text("❌ No signals provided.")
        return

    signals = []
    for line in raw_lines:
        pair, time_str, direction = parse_signal_line(line)
        if not pair:
            continue
        if pair not in BACKTEST_SUPPORTED_PAIRS:
            await update.message.reply_text(f"⚠️ Pair {pair} not supported. Skipping.")
            continue
        # Use the base pair (without OTC) for API call
        api_pair = pair + '_otc'
        signals.append({
            'display': pair,          # base name (e.g., ETHUSD)
            'api_pair': api_pair,
            'time': time_str,
            'dir': direction,
        })
    if not signals:
        await update.message.reply_text("❌ No valid signals found.")
        return

    # Date range: last 'days' days excluding today (PK time)
    now_pk = datetime.now(timezone(timedelta(hours=5)))
    date_list = []
    for i in range(1, days + 1):
        d = now_pk - timedelta(days=i)
        date_list.append(d.strftime("%Y-%m-%d"))

    # Fetch candles per pair
    cache = {}
    for sig in signals:
        api = sig['api_pair']
        if api in cache:
            continue
        url = f"https://a39605-e545.a.jrnm.app/{api}"
        try:
            resp = requests.get(url, timeout=20)
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
            # Build lookup with PK time (already UTC+5, no conversion)
            lookup = {}
            for c in candles:
                time_str = c.get('time', '')
                if not time_str:
                    continue
                try:
                    dt_pk = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    date_ymd = dt_pk.strftime("%Y-%m-%d")
                    hhmm = dt_pk.strftime("%H:%M")
                    o = float(c.get('o', 0))
                    cl = float(c.get('c', 0))
                    direction = 'up' if cl > o else 'down'
                    lookup[(date_ymd, hhmm)] = {
                        'time': time_str,
                        'open': o,
                        'close': cl,
                        'high': float(c.get('h', 0)),
                        'low': float(c.get('l', 0)),
                        'direction': direction
                    }
                except Exception as e:
                    print(f"Parse error: {e}")
                    continue
            cache[api] = lookup
            print(f"Backtest: Loaded {len(lookup)} candles for {api}")
        except Exception as e:
            print(f"Backtest error {api}: {e}")
            cache[api] = None

    # Process each signal
    win_signals = []
    loss_signals = []
    THRESHOLD = 0.75

    for sig in signals:
        display = sig['display']          # base name (without OTC)
        api = sig['api_pair']
        time_str = sig['time']
        direction = sig['dir']

        lookup = cache.get(api)
        if not lookup:
            continue

        win_count = 0
        total_count = 0
        for date_ymd in date_list:
            entry = lookup.get((date_ymd, time_str))
            if not entry:
                continue
            total_count += 1
            found = False
            candle_dir = entry.get('direction', '').lower()
            if direction == "CALL" and candle_dir == "up":
                found = True
            elif direction == "PUT" and candle_dir == "down":
                found = True
            if not found and martingale_level > 0:
                h, m = map(int, time_str.split(':'))
                for step in range(1, martingale_level + 1):
                    next_dt = datetime(2000, 1, 1, h, m) + timedelta(minutes=step)
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
        # Append '-OTC' to pair name for display
        display_otc = display + "-OTC"
        if win_ratio >= THRESHOLD:
            win_signals.append(f"M1 {display_otc} {time_str} {direction}")
        else:
            loss_signals.append(f"M1 {display_otc} {time_str} {direction}")

    if not win_signals and not loss_signals:
        sender.send_message(uid, "⚠️ No signals could be processed.")
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
    summary = f"📊 𝚂𝚄𝙼𝙼𝙰𝚁𝚈\n━━━━━━━━━━━━━━━━━\n✅ Accepted: {len(win_signals)}\n❌ Rejected: {len(loss_signals)}\n📈 Acceptance Rate: {acceptance_rate:.1f}% (threshold 75%)\n🎯 Martingale Level: {martingale_level}\n📅 Days tested: {days}"
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
        # Use the 2000-candle endpoint (new mrbeaxt.site API)
        data = fetch_new_api_candles(api, count=2000)
        try:
            candles = data.get('candles', []) if data else []
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
    "FB_OTC", "MCD_OTC", "INTC_OTC",
    "USDCOP_OTC", "USDPHP_OTC", "USDZAR_OTC", "ATOUSD_OTC", "AVAUSD_OTC",
    "AXSUSD_OTC", "BCHUSD_OTC", "BNBUSD_OTC", "ETCUSD_OTC", "ETHUSD_OTC",
    "LINUSD_OTC", "LTCUSD_OTC", "TONUSD_OTC", "TRUUSD_OTC", "XRPUSD_OTC",
    "ZECUSD_OTC", "XAGUSD_OTC"
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
        data = fetch_new_api_candles(pair, count=2000)
        try:
            candles = data.get('candles', []) if data else []
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

    data = fetch_new_api_candles(pair, count=1)
    if data:
        payout = data.get("payout", "!")
        if isinstance(payout, str):
            payout = payout.replace('%', '')
        try:
            return int(payout)
        except BaseException:
            return payout
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

    data = fetch_new_api_candles(pair, count=180)
    if data:
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

    data = fetch_new_api_candles(pair, count=limit)
    if data:
        return data.get('candles', [])
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

def is_pair_tradeable(pair: str) -> bool:
    """
    Check if the pair is currently tradeable.
    - Live pairs: always tradeable (except weekends if you want, but we'll allow all).
    - OTC pairs: if they are in LIMITED_HOURS_OTC_PAIRS, only tradeable weekdays 2‑8 AM.
    - Other OTC pairs: always tradeable.
    """
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    is_weekend = now_pk.weekday() >= 5  # Saturday = 5, Sunday = 6
    current_time = now_pk.time()

    # Normalize pair name to match our list (replace _ with -)
    norm_pair = pair.replace("_", "-").upper()
    if not norm_pair.endswith("-OTC"):
        norm_pair += "-OTC"

    # If it's a Live pair (no OTC), always tradeable
    if "OTC" not in pair.upper():
        return True

    # If it's an OTC pair that has limited hours, check the window
    if norm_pair in LIMITED_HOURS_OTC_PAIRS:
        if is_weekend:
            return True  # weekends full open
        # Weekday: only between 02:00 and 08:00
        return current_time >= datetime.strptime("02:00", "%H:%M").time() and \
               current_time <= datetime.strptime("08:00", "%H:%M").time()

    # All other OTC pairs are always tradeable
    return True

# ─── MRBEAXT API ──────────────────────────────────────────
def fetch_candles_mrbeaxt(pair: str, count: int = 300, timeframe: str = "1m"):
    """
    Fetch candles using the mrbeaxt.site candle API.
    timeframe: '1m'/'M1', '5m'/'M5', etc. (default '1m')
    Returns: (candles_ascending, current_price, payout)
    """
    print(f"🔍 FETCH: {pair} → API: {to_new_api_pair(pair)} (tf={timeframe})")

    data = fetch_new_api_candles(pair, count=count, timeframe=timeframe)
    if not data or not data.get('candles'):
        print(f"   No candles in response")
        return [], 0.0, 0

    candles = sorted(data['candles'], key=lambda x: x['time'])
    for c in candles:
        if 'volume' not in c:
            c['volume'] = 1
    current_price = candles[-1]['close']
    payout_raw = data.get('payout', '92')

    # ─── Parse payout correctly ──────────────────
    if isinstance(payout_raw, str):
        payout_raw = payout_raw.replace('%', '').strip()
        if payout_raw == '!':
            payout_raw = '92'
    try:
        payout = int(payout_raw)
    except (ValueError, TypeError):
        payout = 92

    print(f"   ✅ {len(candles)} candles, payout={payout}")
    return candles, current_price, payout


def fetch_candle_at_epoch_with_retry(pair, target_epoch, count=50, timeframe="1m",
                                      tolerance=45, attempts=20, delay=3):
    """Retry-aware lookup for a specific candle by target UTC epoch.
    mrbeaxt.site can take a couple of seconds to publish a just-closed
    candle, so a single immediate fetch can miss it — this retries a few
    times before giving up, used by the Multi Engine / manual-signal
    result-checkers."""
    candles = None
    for attempt in range(attempts):
        candles, _, _ = fetch_candles_mrbeaxt(pair, count=count, timeframe=timeframe)
        if candles:
            match = next(
                (c for c in candles if abs(c['time'] - target_epoch) < tolerance), None)
            if match:
                return candles, match
            # Debug: show how close the nearest candle actually was
            nearest = min(candles, key=lambda c: abs(c['time'] - target_epoch))
            gap = nearest['time'] - target_epoch
            print(f"[CANDLE MATCH MISS] {pair} attempt {attempt+1}/{attempts} "
                  f"target={target_epoch} nearest={nearest['time']} gap={gap}s")
        time.sleep(delay)
    return candles, None

# ─── SUPERTREND REVERSAL STRATEGY (from newst.py) ──────
SUPERTREND_PERIOD = 10
SUPERTREND_MULTIPLIER = 3.0
CONSECUTIVE_CANDLES = 3
STRONG_BODY_MIN_PCT = 60
CLOSING_WICK_MAX_PCT = 7

def calc_supertrend(candles_asc, period=SUPERTREND_PERIOD, multiplier=SUPERTREND_MULTIPLIER):
    n = len(candles_asc)
    if n < period + 2:
        return [None]*n, [None]*n

    highs = [float(c['high']) for c in candles_asc]
    lows = [float(c['low']) for c in candles_asc]
    closes = [float(c['close']) for c in candles_asc]

    trs = [max(highs[i]-lows[i],
               abs(highs[i]-closes[i-1]),
               abs(lows[i]-closes[i-1]))
           for i in range(1, n)]

    if len(trs) < period:
        return [None]*n, [None]*n

    atr = [None]*n
    atr[period] = sum(trs[:period]) / period
    for i in range(period+1, n):
        tr = trs[i-1]
        atr[i] = (atr[i-1]*(period-1) + tr) / period

    basic_upper = [None]*n
    basic_lower = [None]*n
    for i in range(period, n):
        hl2 = (highs[i]+lows[i]) / 2
        basic_upper[i] = hl2 + multiplier*atr[i]
        basic_lower[i] = hl2 - multiplier*atr[i]

    final_upper = [None]*n
    final_lower = [None]*n
    trend = [None]*n
    st = [None]*n

    start = period
    final_upper[start] = basic_upper[start]
    final_lower[start] = basic_lower[start]
    trend[start] = 'up' if closes[start] >= final_lower[start] else 'down'
    st[start] = final_lower[start] if trend[start] == 'up' else final_upper[start]

    for i in range(start+1, n):
        if basic_upper[i] < final_upper[i-1] or closes[i-1] > final_upper[i-1]:
            final_upper[i] = basic_upper[i]
        else:
            final_upper[i] = final_upper[i-1]

        if basic_lower[i] > final_lower[i-1] or closes[i-1] < final_lower[i-1]:
            final_lower[i] = basic_lower[i]
        else:
            final_lower[i] = final_lower[i-1]

        if st[i-1] == final_upper[i-1]:
            if closes[i] <= final_upper[i]:
                st[i] = final_upper[i]
                trend[i] = 'down'
            else:
                st[i] = final_lower[i]
                trend[i] = 'up'
        else:
            if closes[i] >= final_lower[i]:
                st[i] = final_lower[i]
                trend[i] = 'up'
            else:
                st[i] = final_upper[i]
                trend[i] = 'down'

    return trend, st

def analyze_candle(c):
    o = float(c['open']); cl = float(c['close'])
    h = float(c['high']); l = float(c['low'])
    rng = (h - l) if (h - l) > 0 else 0.000001
    body = abs(cl - o)

    if cl > o:
        color = 'GREEN'
        closing_wick = h - cl
    elif cl < o:
        color = 'RED'
        closing_wick = cl - l
    else:
        color = 'DOJI'
        closing_wick = rng

    return {
        'color': color,
        'body_pct': round(body / rng * 100, 1),
        'closing_wick_pct': round(closing_wick / rng * 100, 1),
    }

def detect_supertrend_reversal(candles_asc):
    trend, st_line = calc_supertrend(candles_asc)

    if len(candles_asc) < CONSECUTIVE_CANDLES + 1 or trend[-1] is None:
        return None, {'reject': 'INSUFFICIENT_DATA', 'trend': None}

    current_trend = trend[-1]
    trend_label = 'GREEN (UPTREND)' if current_trend == 'up' else 'RED (DOWNTREND)'

    last_n = candles_asc[-CONSECUTIVE_CANDLES:]
    infos = [analyze_candle(c) for c in last_n]
    colors = [i['color'] for i in infos]
    third = infos[-1]

    details = {
        'trend': trend_label,
        'supertrend_value': st_line[-1],
        'candles': infos,
        'colors': colors,
    }

    if current_trend == 'up':
        if not all(c == 'RED' for c in colors):
            details['reject'] = 'NO_3_CONSECUTIVE_RED'
            return None, details
        if third['body_pct'] < STRONG_BODY_MIN_PCT:
            details['reject'] = f"3RD_CANDLE_BODY_WEAK ({third['body_pct']}% < {STRONG_BODY_MIN_PCT}%)"
            return None, details
        if third['closing_wick_pct'] > CLOSING_WICK_MAX_PCT:
            details['reject'] = f"3RD_CANDLE_WICK_TOO_BIG ({third['closing_wick_pct']}% > {CLOSING_WICK_MAX_PCT}%)"
            return None, details
        details['setup'] = f"{CONSECUTIVE_CANDLES}_RED_IN_UPTREND"
        return 'CALL', details

    else:  # downtrend
        if not all(c == 'GREEN' for c in colors):
            details['reject'] = 'NO_3_CONSECUTIVE_GREEN'
            return None, details
        if third['body_pct'] < STRONG_BODY_MIN_PCT:
            details['reject'] = f"3RD_CANDLE_BODY_WEAK ({third['body_pct']}% < {STRONG_BODY_MIN_PCT}%)"
            return None, details
        if third['closing_wick_pct'] > CLOSING_WICK_MAX_PCT:
            details['reject'] = f"3RD_CANDLE_WICK_TOO_BIG ({third['closing_wick_pct']}% > {CLOSING_WICK_MAX_PCT}%)"
            return None, details
        details['setup'] = f"{CONSECUTIVE_CANDLES}_GREEN_IN_DOWNTREND"
        return 'PUT', details

def calc_setup_strength(details):
    third = details['candles'][-1]
    body_score = min(third['body_pct'], 100)
    wick_score = max(0, 100 - (third['closing_wick_pct'] / CLOSING_WICK_MAX_PCT * 100)) if CLOSING_WICK_MAX_PCT else 0
    return round(body_score * 0.6 + wick_score * 0.4, 1)

# ─── MULTI ENGINE PAIR LIST DISPLAY ─────────────────────
async def show_multi_pair_list(update: Update, context: ContextTypes.DEFAULT_TYPE, pairs: list, manual: bool, pair_data: list = None):
    """
    Display pairs with payout % on buttons.
    If pair_data is provided, use it (no refetch).
    Otherwise, fetch in parallel and cache in context.
    """
    query = update.callback_query
    await query.answer()

    # ─── Load or fetch pair_data ──────────────────────
    if pair_data is None:
        loading = await query.message.reply_text("⏳ Fetching payouts...")
        async def fetch_payout(pair):
            _, _, payout = await asyncio.to_thread(fetch_candles_mrbeaxt, pair, 1)
            return pair, payout
        tasks = [fetch_payout(p) for p in pairs]
        results = await asyncio.gather(*tasks)
        pair_data = [(pair, payout) for pair, payout in results]
        # Cache it
        context.user_data['multi_pair_data'] = pair_data
        await loading.delete()
    else:
        # Use cached data
        pass

    # Sort by payout descending
    pair_data.sort(key=lambda x: x[1], reverse=True)

    selected = context.user_data.get('multi_selected_pairs', set()) if not manual else set()

    buttons = []
    row = []
    for pair, payout in pair_data:
        display = pair.upper().replace("_OTC", "").replace("-OTC", "").replace("_otc", "")
        label = f"{display} {payout}%"

        if payout >= 80:
            style = KeyboardButtonStyle.SUCCESS
        elif payout >= 70:
            style = KeyboardButtonStyle.PRIMARY
        else:
            style = KeyboardButtonStyle.DANGER

        if manual:
            cb = f"multi_manual_select_{pair}"
        else:
            # Toggle mode: use premium checkmark emoji when selected
            if pair in selected:
                label = f"✅ {display} {payout}%"
            cb = f"multi_toggle_pair_{pair}"

        # Use colored_button to support premium emoji (optional)
        row.append(InlineKeyboardButton(label, callback_data=cb, style=style))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    if manual:
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="multi_back_to_main", style=KeyboardButtonStyle.DANGER)])
    else:
        count = len(selected)
        buttons.append([InlineKeyboardButton(f"✅ Done ({count} selected)", callback_data="multi_pair_done", style=KeyboardButtonStyle.SUCCESS)])

    msg = "✍️ Manual Signal — Select a pair:" if manual else "Select pairs (tap to toggle):"
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

async def send_pair_list_new(update: Update, context: ContextTypes.DEFAULT_TYPE, pairs: list, manual: bool):
    """
    Send a fresh message with the pair list (no editing).
    Used for the "Change Pair" button.
    """
    query = update.callback_query
    uid = query.from_user.id

    # ─── Send loading message ──────────────────────────
    loading = await context.bot.send_message(chat_id=uid, text="⏳ Fetching payouts...")

    # ─── Fetch payouts in parallel ──────────────────────
    async def fetch_payout(pair):
        _, _, payout = await asyncio.to_thread(fetch_candles_mrbeaxt, pair, 1)
        return pair, payout
    tasks = [fetch_payout(p) for p in pairs]
    results = await asyncio.gather(*tasks)
    pair_data = [(pair, payout) for pair, payout in results]

    # ─── Delete loading message (ignore errors) ────────
    try:
        await loading.delete()
    except Exception:
        pass  # If it fails, we just continue – no harm

    pair_data.sort(key=lambda x: x[1], reverse=True)

    selected = context.user_data.get('multi_selected_pairs', set()) if not manual else set()

    buttons = []
    row = []
    for pair, payout in pair_data:
        display = pair.upper().replace("_OTC", "").replace("-OTC", "").replace("_otc", "")
        label = f"{display} {payout}%"

        if payout >= 80:
            style = KeyboardButtonStyle.SUCCESS
        elif payout >= 70:
            style = KeyboardButtonStyle.PRIMARY
        else:
            style = KeyboardButtonStyle.DANGER

        if manual:
            cb = f"multi_manual_select_{pair}"
        else:
            if pair in selected:
                label = f"✅ {display} {payout}%"
            cb = f"multi_toggle_pair_{pair}"

        row.append(InlineKeyboardButton(label, callback_data=cb, style=style))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    if manual:
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="multi_back_to_main", style=KeyboardButtonStyle.DANGER)])
    else:
        count = len(selected)
        buttons.append([InlineKeyboardButton(f"✅ Done ({count} selected)", callback_data="multi_pair_done", style=KeyboardButtonStyle.SUCCESS)])

    msg = "✍️ Manual Signal — Select a pair:" if manual else "Select pairs (tap to toggle):"
    await context.bot.send_message(
        chat_id=uid,
        text=msg,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── MANUAL S
def run_multi_manual_analysis(uid: int, pair: str, context: ContextTypes.DEFAULT_TYPE):
    """
    Manual signal: tries multiple strategies silently (no names shown).
    Order: Strategy 3 → Strategy 4 (75%) → Strategy 5 (80%) → Strategy 2 (SuperTrend, 80%).
    """
    market = context.user_data.get('multi_manual_market', 'otc')
    market_display = "OTC" if market == "otc" else "Live"

    status_msg = sender.send_message(
        uid,
        f"🔍 Analyzing {pair} ({market_display})..."
    )

    candles, price, payout = fetch_candles_mrbeaxt(pair, count=300)
    if not candles:
        sender.edit_message(uid, status_msg.id, f"❌ No data for {pair}.")
        return

    st = get_state(uid)

    # ─── 1. Strategy 3 (user settings) ──────────────────
    min_acc3 = st.strategy3_min_accuracy if st.strategy3_min_accuracy else 75
    lookback = st.strategy3_lookback if st.strategy3_lookback else 20
    direction, entry_dt, confidence = analyze_strategy3(candles, min_acc3, lookback)

    if direction and confidence is not None:
        sender.edit_message(uid, status_msg.id, "✅ Signal found! Sending...")
        _send_manual_signal_and_result(uid, pair, candles, direction, entry_dt, confidence, payout, context)
        return

    # ─── 2. Strategy 4 (ADX + Stochastic, 75%) ──────────
    sender.edit_message(uid, status_msg.id, "🔄 Trying another approach (2/4)...")
    direction, entry_dt, confidence = analyze_strategy4(candles, min_accuracy=75)

    if direction and confidence is not None:
        sender.edit_message(uid, status_msg.id, "✅ Signal found! Sending...")
        _send_manual_signal_and_result(uid, pair, candles, direction, entry_dt, confidence, payout, context)
        return

    # ─── 3. Strategy 5 (Confluence, 80%) ────────────────
    sender.edit_message(uid, status_msg.id, "🔄 Trying another approach (3/4)...")
    direction, entry_dt, confidence = analyze_strategy5(candles, min_accuracy=80)

    if direction and confidence is not None:
        sender.edit_message(uid, status_msg.id, "✅ Signal found! Sending...")
        _send_manual_signal_and_result(uid, pair, candles, direction, entry_dt, confidence, payout, context)
        return

    # ─── 4. Strategy 2 (SuperTrend only, 80%) ───────────
    sender.edit_message(uid, status_msg.id, "🔄 Trying another approach (4/4)...")
    filters = Strategy2Filters()
    filters.use_supertrend = True
    filters.use_trend = False
    filters.use_bollinger = False
    filters.use_support_resistance = False
    filters.use_price_action = False
    filters.use_fvg = False
    filters.use_trend_reverse = False
    filters.min_accuracy = 80

    direction, entry_dt, confidence = analyze_strategy2(candles, filters)

    if direction and confidence is not None:
        sender.edit_message(uid, status_msg.id, "✅ Signal found! Sending...")
        _send_manual_signal_and_result(uid, pair, candles, direction, entry_dt, confidence, payout, context)
        return

    # ─── No signal from any method ──────────────────────
    sender.edit_message(uid, status_msg.id, f"❌ No signal found for {pair}.")

# ─── RESULT CHECK (FORMAT 3) ─────────────────────
def handle_multi_result(uid: int, pair: str, entry_dt: datetime, direction: str, payout: int,
                        context: ContextTypes.DEFAULT_TYPE = None, is_manual: bool = False):
    wins = losses = 0

    # ─── Wait 1 minute ──────────────────────────────────
    t1 = entry_dt + timedelta(minutes=1)
    while datetime.now(timezone.utc) + timedelta(hours=5) < t1:
        time.sleep(1)

    entry_epoch = int((entry_dt - timedelta(hours=5)).timestamp())
    candles, c1 = fetch_candle_at_epoch_with_retry(pair, entry_epoch)
    if not candles:
        sender.send_message(uid, f"⚠️ {pair}: couldn't fetch candles to verify result (API issue). Skipped.")
        return
    if not c1:
        sender.send_message(uid, f"⚠️ {pair}: couldn't find the entry candle to verify result. Skipped.")
        return

    win1 = (c1['close'] > c1['open']) if direction == "CALL" else (c1['close'] < c1['open'])
    c1_color = "green" if c1['close'] > c1['open'] else "red"

    chart_path = None
    result_type = None
    owner = context.user_data.get('multi_owner_name', '@Rohailtrader') if context else '@Rohailtrader'

    if win1:
        wins += 1
        result_type = "WIN"
        result_text = build_result_format3(pair, entry_dt.strftime("%H:%M"), direction, "WIN",
                                           c1['open'], c1['close'], c1_color, wins, losses)
        chart_path = safe_draw_pro_chart(candles, pair, entry_dt.strftime("%H:%M"), direction, payout,
                                         result="WIN", confidence=80)
    else:
        # ─── Martingale ──────────────────────────────────
        t2 = entry_dt + timedelta(minutes=2)
        while datetime.now(timezone.utc) + timedelta(hours=5) < t2:
            time.sleep(1)

        mtg_epoch = int((entry_dt + timedelta(minutes=1) - timedelta(hours=5)).timestamp())
        candles2, c2 = fetch_candle_at_epoch_with_retry(pair, mtg_epoch)
        if not candles2:
            sender.send_message(uid, f"⚠️ {pair} MTG: couldn't fetch candles to verify result (API issue). Skipped.")
            return
        if not c2:
            sender.send_message(uid, f"⚠️ {pair} MTG: couldn't find the MTG candle to verify result. Skipped.")
            return

        win2 = (c2['close'] > c2['open']) if direction == "CALL" else (c2['close'] < c2['open'])
        c2_color = "green" if c2['close'] > c2['open'] else "red"

        if win2:
            wins += 1
            result_type = "MTG WIN"
            result_text = build_result_format3(pair, entry_dt.strftime("%H:%M"), direction, "MTG WIN",
                                               c2['open'], c2['close'], c2_color, wins, losses)
            chart_path = safe_draw_pro_chart(candles2, pair, entry_dt.strftime("%H:%M"), direction, payout,
                                             result="MTG WIN", confidence=80)
        else:
            losses += 1
            result_type = "LOSS"
            result_text = build_result_format3(pair, entry_dt.strftime("%H:%M"), direction, "LOSS",
                                               c2['open'], c2['close'], c2_color, wins, losses)
            chart_path = safe_draw_pro_chart(candles2, pair, entry_dt.strftime("%H:%M"), direction, payout,
                                             result="LOSS", confidence=80)

    # ─── Send result with buttons (if manual) ──────────
    if is_manual and context:
        # ─── Build inline keyboard with colored buttons ──
        keyboard = [
            [
                colored_button("Analyse Again", f"multi_manual_reanalyse_{pair}",
                               KeyboardButtonStyle.SUCCESS, "6147654280112248427"),
                colored_button("Change Pair", "multi_manual_changepair",
                               KeyboardButtonStyle.PRIMARY, "6145248943807667330")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # ─── Build entities for premium emojis & bold ──
        entities = build_custom_emoji_entities(result_text)
        # Add bold on header (first line) and footer (last line)
        lines = result_text.split('\n')
        if lines:
            header = lines[0]
            footer = lines[-1]
            header_len = len(header.encode('utf-16-le')) // 2
            footer_start = len((result_text[:result_text.rfind(footer)]).encode('utf-16-le')) // 2
            footer_len = len(footer.encode('utf-16-le')) // 2
            entities.append(MessageEntity(type='bold', offset=0, length=header_len))
            entities.append(MessageEntity(type='bold', offset=footer_start, length=footer_len))

        async def send_result():
            if chart_path and os.path.exists(chart_path):
                with open(chart_path, 'rb') as f:
                    await context.bot.send_photo(
                        chat_id=uid,
                        photo=f,
                        caption=result_text,
                        caption_entities=entities,
                        reply_markup=reply_markup
                    )
                os.remove(chart_path)
            else:
                await context.bot.send_message(
                    chat_id=uid,
                    text=result_text,
                    entities=entities,
                    reply_markup=reply_markup
                )
        asyncio.run_coroutine_threadsafe(send_result(), MAIN_LOOP)
    else:
        # Non‑manual: use old sender
        if chart_path and os.path.exists(chart_path):
            sender.send_file(uid, chart_path, result_text)
            try: os.remove(chart_path)
            except: pass
        else:
            sender.send_message(uid, result_text)

# ─── NON‑STOP SIGNAL LOOP ────────────────────────────────
async def start_multi_nonstop(update: Update, context: ContextTypes.DEFAULT_TYPE, pairs: list):
    query = update.callback_query
    uid = query.from_user.id

    if context.user_data.get('multi_running'):
        sender.send_message(uid, "⚠️ Multi Engine already running. Use Stop first.")
        return
    context.user_data['multi_pairs'] = pairs
    context.user_data['multi_running'] = True
    context.user_data['multi_paused'] = False
    context.user_data['multi_stats'] = {'wins': 0, 'losses': 0, 'total': 0}
    context.user_data['multi_index'] = 0

    # ─── Control panel with premium emojis ──────────────
    msg_text = (
        "🔁 Non‑Stop Signal Running\n\n"
        f"📊 Pairs: {len(pairs)}\n"
        "🍏 Status: RUNNING\n"
        "🗽 Wins: 0  |  Losses: 0\n"
        "─────────────────────\n"
        "Use buttons below."
    )
    entities = build_custom_emoji_entities(msg_text)
    buttons = [
        [colored_button(" Pause", "multi_pause", KeyboardButtonStyle.PRIMARY, "5348125953090403204")],
        [colored_button(" Resume", "multi_resume", KeyboardButtonStyle.SUCCESS, "5116447063432758252")],
        [colored_button(" Stop", "multi_stop", KeyboardButtonStyle.DANGER, "6084515769780013003")],
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    sent = await context.bot.send_message(
        chat_id=uid,
        text=msg_text,
        entities=entities,
        reply_markup=reply_markup
    )
    context.user_data['multi_control_msg_id'] = sent.message_id

    # ─── Initial status message ──────────────────────────
    status_msg = await context.bot.send_message(chat_id=uid, text="⏳ 🔄 Starting scan...")
    context.user_data['multi_status_msg_id'] = status_msg.message_id

    await query.edit_message_text("⏳ Non‑Stop started. Check control panel.")

    # ─── Start background thread ──────────────────────────
    thread = threading.Thread(target=multi_nonstop_loop, args=(uid, context), daemon=True)
    thread.start()

def _send_manual_signal_and_result(uid: int, pair: str, candles: list,
                                   direction: str, entry_dt: datetime,
                                   confidence: float, payout: int,
                                   context: ContextTypes.DEFAULT_TYPE):
    """Send signal (Format 3) and then handle result with manual buttons."""
    owner = context.user_data.get('multi_owner_name', '@Rohailtrader')  # ← NEW
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    entry_time = (now_pk + timedelta(minutes=1)).replace(second=0, microsecond=0).strftime("%H:%M")
    confidence = int(confidence)

    signal_text = build_signal_format3(pair, entry_time, direction, payout, owner_name=owner)  # ← PASS OWNER
    chart_path = safe_draw_pro_chart(candles, pair, entry_time, direction, payout,
                                     result=None, confidence=confidence)
    if chart_path and os.path.exists(chart_path):
        sender.send_file(uid, chart_path, signal_text)
        try: os.remove(chart_path)
        except: pass
    else:
        sender.send_message(uid, signal_text)

    entry_dt = (now_pk + timedelta(minutes=1)).replace(second=0, microsecond=0)
    handle_multi_result(uid, pair, entry_dt, direction, payout, context, is_manual=True)


def _update_status(uid: int, context: ContextTypes.DEFAULT_TYPE, status_text: str):
    """
    Show the current scanning status as a FRESH message that always sits below
    the most recent signal/result — instead of editing an old message in place
    (which used to leave the status stuck above newer signal/result messages).

    IMPORTANT: this used to swallow every failure with a bare `print()`, so if
    the Telethon `sender` client dropped its connection or hit a timeout, the
    loop kept running in the background but NOTHING reached the chat anymore
    (looked "stuck" on the last message forever). Now it logs the real error
    and falls back to the PTB bot so the user actually sees updates/errors.
    """
    # Premium emoji prefix
    if "SIGNAL" in status_text:
        emoji = "🍏"
    elif "scanning" in status_text.lower():
        emoji = "🔍"
    elif "market closed" in status_text.lower():
        emoji = "⏸️"
    elif "complete" in status_text.lower():
        emoji = "✅"
    elif "error" in status_text.lower() or "⚠️" in status_text:
        emoji = "⚠️"
    else:
        emoji = "⏳"

    full_text = f"{emoji} {status_text}"

    # Delete the previous status message so it doesn't linger above newer content
    old_msg_id = context.user_data.get('multi_status_msg_id')
    if old_msg_id:
        try:
            sender.delete_message(uid, old_msg_id)
        except Exception as _del_err:
            print(f"[MULTI STATUS DELETE ERROR] {_del_err}")

    # Always send a brand-new message so it lands after the latest signal/result
    sent = None
    try:
        sent = sender.send_message(uid, full_text)
    except Exception as _status_err:
        print(f"[MULTI STATUS ERROR - Telethon send failed] {_status_err}")

    if sent:
        context.user_data['multi_status_msg_id'] = sent.id
    else:
        # Telethon failed or returned nothing -> try the PTB bot so the user
        # still gets the update instead of total silence.
        fallback_sent = _send_via_ptb_fallback(uid, context, full_text)
        if fallback_sent:
            context.user_data['multi_status_msg_id'] = fallback_sent.message_id

def update_multi_control_panel(uid: int, context: ContextTypes.DEFAULT_TYPE):
    """Edit the control panel with premium emojis and colored buttons."""
    stats = context.user_data.get('multi_stats', {'wins': 0, 'losses': 0, 'total': 0})
    paused = context.user_data.get('multi_paused', False)
    status = " PAUSED" if paused else "🍋 RUNNING"
    msg = (
        f"🔁 Non‑Stop Signal Running\n\n"
        f"📊 Pairs: {len(context.user_data.get('multi_pairs', []))}\n"
        f"Status: {status}\n"
        f"🗽 Wins: {stats['wins']}  |  Losses: {stats['losses']}\n"
        f"Total: {stats['total']}\n"
        f"─────────────────────\n"
        f"Use buttons below."
    )
    entities = build_custom_emoji_entities(msg)
    buttons = [
        [colored_button(" Pause", "multi_pause", KeyboardButtonStyle.PRIMARY, "5348125953090403204")],
        [colored_button(" Resume", "multi_resume", KeyboardButtonStyle.SUCCESS, "5116447063432758252")],
        [colored_button(" Stop", "multi_stop", KeyboardButtonStyle.DANGER, "6084515769780013003")],
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    msg_id = context.user_data.get('multi_control_msg_id')
    if msg_id:
        async def edit():
            return await context.bot.edit_message_text(
                chat_id=uid,
                message_id=msg_id,
                text=msg,
                entities=entities,
                reply_markup=reply_markup
            )
        # Routed through the same throttled send-queue as everything else so
        # frequent panel refreshes across many concurrent users don't add to
        # flood risk.
        _enqueue_send(uid, edit)

def multi_nonstop_loop(uid: int, context: ContextTypes.DEFAULT_TYPE):
    """Background loop for non‑stop signals with infinite cycling and error recovery."""
    pairs = context.user_data.get('multi_pairs', [])
    if not pairs:
        sender.send_message(uid, "❌ No pairs in loop.")
        return

    idx = 0
    total = len(pairs)

    while context.user_data.get('multi_running', False):
        # ─── Top‑level try/except prevents the loop from dying ──
        try:
            if context.user_data.get('multi_paused', False):
                time.sleep(1)
                continue

            pair = pairs[idx % total]
            idx += 1
            context.user_data['multi_index'] = idx

            owner = context.user_data.get('multi_owner_name', '@Rohailtrader')

            # ─── Check market hours ──────────────────────
            if not is_pair_tradeable(pair):
                _update_status(uid, context, f"{pair} – market closed, skipping")
                time.sleep(0.5)
                continue

            _update_status(uid, context, f"Scanning {pair} ({idx}/{total})")

            candles, price, payout = fetch_candles_mrbeaxt(pair, count=300)
            time.sleep(0.2)

            if not candles:
                _update_status(uid, context, f"No data for {pair} – skipping")
                time.sleep(0.5)
                continue

            asc = sorted(candles, key=lambda x: x['time'])
            direction, details = detect_supertrend_reversal(asc)

            if not direction:
                reject = details.get('reject', 'NO_SETUP')
                trend = details.get('trend', '?')
                _update_status(uid, context, f"{pair} – {reject} (trend: {trend})")
                time.sleep(0.3)
                continue

            # ─── Signal Found ──────────────────────────────
            score = calc_setup_strength(details)
            now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
            entry_dt = (now_pk + timedelta(minutes=1)).replace(second=0, microsecond=0)
            entry_time = entry_dt.strftime("%H:%M")
            confidence = int(score)

            _update_status(uid, context, f" SIGNAL on {pair} – {direction} @ {entry_time} (confidence {confidence}%)")

            # Send signal
            signal_text = build_signal_format3(pair, entry_time, direction, payout)
            chart_path = safe_draw_pro_chart_timeout(candles, pair, entry_time, direction, payout,
                                             result=None, confidence=confidence)
            try:
                if chart_path and os.path.exists(chart_path):
                    sent_ok = sender.send_file(uid, chart_path, signal_text)
                    try: os.remove(chart_path)
                    except: pass
                else:
                    sent_ok = sender.send_message(uid, signal_text)
                if not sent_ok:
                    # Telethon returned nothing (likely a dead/expired session) —
                    # fall back to PTB so the signal text still reaches the chat.
                    _send_via_ptb_fallback(uid, context, signal_text)
            except Exception as _send_err:
                print(f"[MULTI SIGNAL SEND ERROR] {_send_err}")
                _send_via_ptb_fallback(uid, context, signal_text)

            # ─── Wait 1st candle ──────────────────────────
            t1 = entry_dt + timedelta(minutes=1)
            while datetime.now(timezone.utc) + timedelta(hours=5) < t1:
                time.sleep(1)

            entry_epoch = int((entry_dt - timedelta(hours=5)).timestamp())
            candles1, c1 = fetch_candle_at_epoch_with_retry(pair, entry_epoch)
            wins = context.user_data['multi_stats']['wins']
            losses = context.user_data['multi_stats']['losses']

            if candles1:
                if c1:
                    win1 = (c1['close'] > c1['open']) if direction == "CALL" else (c1['close'] < c1['open'])
                    c1_color = "green" if c1['close'] > c1['open'] else "red"

                    if win1:
                        result_text = build_result_format3(pair, entry_time, direction, "WIN",
                                                           c1['open'], c1['close'], c1_color, wins+1, losses)
                        chart_path = safe_draw_pro_chart_timeout(candles1, pair, entry_time, direction, payout,
                                                         result="WIN", confidence=confidence)
                        if chart_path:
                            sender.send_file(uid, chart_path, result_text)
                            try: os.remove(chart_path)
                            except: pass
                        else:
                            sender.send_message(uid, result_text)
                        context.user_data['multi_stats']['wins'] += 1
                        context.user_data['multi_stats']['total'] += 1
                        update_multi_control_panel(uid, context)
                        time.sleep(2)
                        continue
                else:
                    print(f"[MULTI C1 MISS] {pair} entry_epoch match not found, "
                          f"falling through to martingale")
            else:
                print(f"[MULTI C1 EMPTY] {pair} no candles returned for entry check, "
                      f"falling through to martingale")

            # ─── Martingale ──────────────────────────────────
            t2 = entry_dt + timedelta(minutes=2)
            while datetime.now(timezone.utc) + timedelta(hours=5) < t2:
                time.sleep(1)

            mtg_epoch = int((entry_dt + timedelta(minutes=1) - timedelta(hours=5)).timestamp())
            candles2, c2 = fetch_candle_at_epoch_with_retry(pair, mtg_epoch)
            wins = context.user_data['multi_stats']['wins']
            losses = context.user_data['multi_stats']['losses']

            if candles2:
                if c2:
                    win2 = (c2['close'] > c2['open']) if direction == "CALL" else (c2['close'] < c2['open'])
                    c2_color = "green" if c2['close'] > c2['open'] else "red"

                    if win2:
                        result_text = build_result_format3(pair, entry_time, direction, "MTG WIN",
                                                           c2['open'], c2['close'], c2_color, wins+1, losses)
                        chart_path = safe_draw_pro_chart_timeout(candles2, pair, entry_time, direction, payout,
                                                         result="MTG WIN", confidence=confidence)
                        if chart_path:
                            sender.send_file(uid, chart_path, result_text)
                            try: os.remove(chart_path)
                            except: pass
                        else:
                            sender.send_message(uid, result_text)
                        context.user_data['multi_stats']['wins'] += 1
                    else:
                        result_text = build_result_format3(pair, entry_time, direction, "LOSS",
                                                           c2['open'], c2['close'], c2_color, wins, losses+1)
                        chart_path = safe_draw_pro_chart_timeout(candles2, pair, entry_time, direction, payout,
                                                         result="LOSS", confidence=confidence)
                        if chart_path:
                            sender.send_file(uid, chart_path, result_text)
                            try: os.remove(chart_path)
                            except: pass
                        else:
                            sender.send_message(uid, result_text)
                        context.user_data['multi_stats']['losses'] += 1

                    context.user_data['multi_stats']['total'] += 1
                    update_multi_control_panel(uid, context)
                else:
                    sender.send_message(uid, f"⚠️ {pair} MTG: couldn't find the MTG candle to verify result. Skipped.")
            else:
                sender.send_message(uid, f"⚠️ {pair} MTG: couldn't fetch candles to verify result (API issue). Skipped.")

            _update_status(uid, context, f"Cycle complete for {pair} – waiting...")
            time.sleep(2)

        except Exception as e:
            # ─── Catch any error, log, and continue (never let this block itself crash the thread) ──
            import traceback
            print(f"Non‑stop loop error: {e}")
            traceback.print_exc()
            try:
                _update_status(uid, context, f"⚠️ Error: {str(e)[:50]} – continuing...")
            except Exception as _status_err:
                print(f"[MULTI STATUS ERROR in except] {_status_err}")
            time.sleep(2)

    # Loop ended
    sender.send_message(uid, "🔴 Non‑Stop Signal stopped.")

def run_strategy_finder(uid: int, context: ContextTypes.DEFAULT_TYPE):
    """
    Ultra‑fast Strategy Finder – samples 10 pairs, 300 candles, step=5.
    Uses PTB for final report with entities and Back button.
    """
    import time as t
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    market = context.user_data.get('finder_market', 'otc')
    tf = context.user_data.get('finder_tf', '1m')
    tf_display = 'M1' if tf == '1m' else 'M5'

    # ─── Select first 10 pairs for speed ────────────────
    if market == 'otc':
        pairs = MULTI_ENGINE_OTC[:10]
    elif market == 'live':
        pairs = MULTI_ENGINE_LIVE[:10]
    else:
        pairs = (MULTI_ENGINE_OTC + MULTI_ENGINE_LIVE)[:10]

    if not pairs:
        sender.send_message(uid, "❌ No pairs found.")
        return

    count = 300  # fixed candles per pair
    total_pairs = len(pairs)

    # ─── Show initial progress with pair count ──────────
    progress_msg = sender.send_message(
        uid,
        f"⏳ Scanning {total_pairs} pairs with {count} candles each ({tf_display})..."
    )
    msg_id = progress_msg.id

    # ─── Stage 1: Fetch candles (parallel) ──────────────
    def fetch_pair(pair):
        try:
            candles, _, _ = fetch_candles_mrbeaxt(pair, count=count, timeframe=tf)
            return pair, candles
        except:
            return pair, None

    pair_candles = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_pair, p): p for p in pairs}
        fetched = 0
        for future in as_completed(futures):
            pair, candles = future.result()
            fetched += 1
            if candles and len(candles) >= 50:
                pair_candles[pair] = candles
            pct = int((fetched / total_pairs) * 100)
            try:
                sender.edit_message(
                    uid, msg_id,
                    f"⏳ Fetching candles: {pct}% ({fetched}/{total_pairs} pairs done)"
                )
            except:
                pass

    if not pair_candles:
        sender.edit_message(uid, msg_id, "❌ No valid candles fetched.")
        return

    # ─── Stage 2: Simulate (parallel) ──────────────────
    stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'confidences': []})
    stats_lock = threading.Lock()

    def process_pair(pair, candles):
        n = len(candles)
        local_stats = {i: {'wins': 0, 'losses': 0, 'confidences': []} for i in range(1, 8)}
        step = 5

        for strat_id in range(1, 8):
            wins = losses = 0
            confidences = []
            for i in range(50, n - 1, step):
                window = candles[:i+1]
                try:
                    if strat_id == 1:
                        dir, _, conf = analyze_strategy1(window, 75)
                    elif strat_id == 2:
                        filters = Strategy2Filters()
                        dir, _, conf = analyze_strategy2(window, filters)
                    elif strat_id == 3:
                        dir, _, conf = analyze_strategy3(window, 75, 20)
                    elif strat_id == 4:
                        dir, _, conf = analyze_strategy4(window, 60)
                    elif strat_id == 5:
                        dir, _, conf = analyze_strategy5(window, 72)
                    elif strat_id == 6:
                        dir, _, conf = analyze_strategy6(window, 20, 10)
                    elif strat_id == 7:
                        dir, _, conf = analyze_strategy7(window, 60)
                    else:
                        continue
                except:
                    continue

                if dir is None or conf is None:
                    continue

                next_candle = candles[i+1]
                win = (next_candle['close'] > next_candle['open']) if dir == "CALL" else (next_candle['close'] < next_candle['open'])
                if win:
                    wins += 1
                else:
                    losses += 1
                confidences.append(conf)

            local_stats[strat_id]['wins'] = wins
            local_stats[strat_id]['losses'] = losses
            local_stats[strat_id]['confidences'] = confidences

        with stats_lock:
            for sid in range(1, 8):
                stats[sid]['wins'] += local_stats[sid]['wins']
                stats[sid]['losses'] += local_stats[sid]['losses']
                stats[sid]['confidences'].extend(local_stats[sid]['confidences'])

    processed = 0
    sender.edit_message(
        uid, msg_id,
        f"⏳ Simulating strategies on {len(pair_candles)} pairs (step=5)..."
    )
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_pair, pair, candles): pair for pair, candles in pair_candles.items()}
        for future in as_completed(futures):
            processed += 1
            pct = int((processed / len(pair_candles)) * 100)
            try:
                sender.edit_message(
                    uid, msg_id,
                    f"⏳ Simulating: {pct}% ({processed}/{len(pair_candles)} pairs done)"
                )
            except:
                pass

    # ─── Build report with better table ──────────────────
    results = []
    for sid in range(1, 8):
        total = stats[sid]['wins'] + stats[sid]['losses']
        win_rate = (stats[sid]['wins'] / total * 100) if total > 0 else 0
        avg_conf = sum(stats[sid]['confidences']) / len(stats[sid]['confidences']) if stats[sid]['confidences'] else 0
        results.append({
            'strategy': sid,
            'wins': stats[sid]['wins'],
            'losses': stats[sid]['losses'],
            'win_rate': round(win_rate, 1),
            'avg_conf': round(avg_conf, 1),
            'total_signals': total
        })

    results.sort(key=lambda x: (x['win_rate'], x['total_signals']), reverse=True)

    lines = []
    lines.append("🔍 STRATEGY FINDER – RESULTS")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    market_label = "OTC" if market == "otc" else "LIVE" if market == "live" else "OTC + LIVE"
    lines.append(f"📅 Market: {market_label}  |  TF: {tf_display}  |  Pairs: {len(pair_candles)}")
    lines.append("")

    best = results[0]
    lines.append("🏆 BEST STRATEGY: STRATEGY " + str(best['strategy']))
    lines.append(f"   Win Rate: {best['win_rate']}%  ({best['wins']}/{best['total_signals']})")
    lines.append(f"   Avg Confidence: {best['avg_conf']}%")
    lines.append("")

    # ─── Table with fixed widths ──────────────────────
    lines.append("📈 STRATEGY RANKINGS")
    lines.append("┌─────────┬──────────┬─────────┬────────────┐")
    lines.append("│ Strategy │ Win Rate │ Signals │ Avg Conf   │")
    lines.append("├─────────┼──────────┼─────────┼────────────┤")
    for r in results:
        lines.append(
            f"│ {r['strategy']:^7}  │ {r['win_rate']:>7} % │ {r['total_signals']:^7} │ {r['avg_conf']:>9} % │"
        )
    lines.append("└─────────┴──────────┴─────────┴────────────┘")
    lines.append("")
    lines.append("💡 SUGGESTION: Use Strategy " + str(best['strategy']) + " for best results.")
    lines.append("")
    lines.append("🔙 [Back to Menu]")

    final_text = "\n".join(lines)

    # ─── Send final report via PTB ──────────────────────
    entities = build_custom_emoji_entities(final_text)
    best_line = f"🏆 BEST STRATEGY: STRATEGY {best['strategy']}"
    offset_best = final_text.find(best_line)
    if offset_best != -1:
        entities.append(MessageEntity(type='bold', offset=len(final_text[:offset_best].encode('utf-16-le'))//2, length=len(best_line.encode('utf-16-le'))//2))

    async def send_report():
        try:
            await context.bot.delete_message(chat_id=uid, message_id=msg_id)
        except:
            pass
        buttons = [[colored_button("🔙 Back to Menu", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")]]
        await context.bot.send_message(
            chat_id=uid,
            text=final_text,
            entities=entities,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    asyncio.run_coroutine_threadsafe(send_report(), MAIN_LOOP)

async def s6_score_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def s6_candles_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 10 <= val <= 200:
            get_state(uid).strategy6_min_candles = val
            trading_mode = context.user_data.get('trading_mode', 'otc')
            bot = SMZXBot(uid)
            if trading_mode == 'forex':
                bot.pairs = FOREX_PAIRS
            await update.message.reply_text(
                f"✅ Confluence score ≥ {get_state(uid).strategy6_min_score}, "
                f"min candles = {val}. Scanning..."
            )
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 10‑200:")
            return S6_MIN_CANDLES
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S6_MIN_CANDLES

async def s7_accuracy_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data['uid']
    raw = update.message.text
    cleaned = clean_int_input(raw)
    try:
        val = int(cleaned)
        if 50 <= val <= 100:
            get_state(uid).strategy7_min_score = val
            trading_mode = context.user_data.get('trading_mode', 'otc')
            bot = SMZXBot(uid)
            if trading_mode == 'forex':
                bot.pairs = FOREX_PAIRS
            await update.message.reply_text(f"✅ Min confidence set to {val}%. Starting analysis...")
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 50-100:")
            return STATE_STRATEGY7_ACCURACY
    except:
        await update.message.reply_text("❌ Invalid number. Enter a number between 50-100:")
        return STATE_STRATEGY7_ACCURACY

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

STATE_LIVEFS_DAYS = 300
STATE_LIVEFS_CUSTOM_DAYS = 301
STATE_LIVEFS_PAIR_MODE = 302
STATE_LIVEFS_CUSTOM_PAIR_SELECT = 303
STATE_LIVEFS_START_TIME = 304
STATE_LIVEFS_END_TIME = 305

# Live FS Checker States
STATE_LIVEFS_CHECKER_DATE = 400
STATE_LIVEFS_CHECKER_MTG = 401
STATE_LIVEFS_CHECKER_SIGNALS = 402

# Backtest 2.0 States
STATE_BACKTEST2_UTC = 220
STATE_BACKTEST2_DAYS = 221      # instead of STATE_BACKTEST2_DATE
STATE_BACKTEST2_MTG = 222
STATE_BACKTEST2_SIGNALS = 223

STATE_FUT4_DAYS = 310
STATE_FUT4_CONFIDENCE = 311
STATE_FUT4_MARKET = 312
STATE_FUT4_PAIR_MODE = 313
STATE_FUT4_CUSTOM_PAIR_SELECT = 314
STATE_FUT4_START_TIME = 315
STATE_FUT4_END_TIME = 316

# Strategy 5 – NIGHTYY ST States
STATE_FUT5_PAIR_MODE = 520
STATE_FUT5_CUSTOM_PAIR_SELECT = 521
STATE_FUT5_DAYS = 522
STATE_FUT5_ACCURACY = 523
STATE_FUT5_START_TIME = 524
STATE_FUT5_END_TIME = 525
STATE_FUT5_GENERATING = 526
STATE_PAIR_LIST = 1000

STATE_SWAP_CP = 600

STATE_AUTO_SIGNAL_MARKET = 100
STATE_AUTO_SIGNAL_CHANNEL = 101
STATE_AUTO_SIGNAL_STRATEGY = 102
STATE_AUTO_SIGNAL_FORMAT = 103

STATE_STRATEGY7_ACCURACY = 105
STATE_AUTO_SIGNAL_S7_ACC = 106

STATE_CHECKER_SETTINGS = 700
STATE_CHECKER_UTC_SELECT = 701
STATE_CHECKER_MTG_SELECT = 702
STATE_CHECKER_DATE_SELECT = 703
STATE_CHECKER_PAYOUT_FILTER = 704
STATE_CHECKER_RUNNING = 705

STATE_BROADCAST_WAIT_FOR_MESSAGE = 900   # 🔵 NEW

# Multi Engine States
STATE_MULTI_ENGINE_MAIN = 1000
STATE_MULTI_MANUAL_MARKET = 1001
STATE_MULTI_MANUAL_PAIR_SELECT = 1002
STATE_MULTI_NONSTOP_FILTER = 1003
STATE_MULTI_NONSTOP_MARKET = 1004
STATE_MULTI_NONSTOP_MANUAL_SELECT = 1005
STATE_MULTI_NONSTOP_RUNNING = 1006
STATE_MULTI_SETTINGS = 1007
STATE_MULTI_SETTINGS_OWNER_NAME = 1008

STATE_AUTO_SIGNAL_TIMEFRAME = 1009

STATE_FINDER_MARKET = 1200
STATE_FINDER_TF = 1201
STATE_FINDER_RUNNING = 1203   # not used directly but for state tracking

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

def _build_livefs_pair_page(page=0, per_page=15, selected=None):
    if selected is None:
        selected = set()
    total = len(LIVE_PAIRS_FS)
    start = page * per_page
    end = min(start + per_page, total)
    page_pairs = LIVE_PAIRS_FS[start:end]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        label = f"✅ {pair}" if pair in selected else pair
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"livefs_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"livefs_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"livefs_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "livefs_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

def _build_fut5_pair_page(page=0, per_page=15, selected=None):
    if selected is None:
        selected = set()
    total = len(FUT4_OTC_PAIRS)   # Strategy 4 ki OTC pairs list
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total)
    page_pairs = FUT4_OTC_PAIRS[start_idx:end_idx]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        # Display name: remove "_otc" and add "-OTC" for showing
        display = pair.replace('_otc', '-OTC').upper()
        label = f"✅ {display}" if pair in selected else display
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"fut5_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"fut5_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"fut5_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "fut5_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

async def show_fut5_pair_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = context.user_data.get('fut5_pair_page', 0)
    selected = context.user_data.get('fut5_selected_pairs', set())
    buttons, page, total_pages = _build_fut5_pair_page(page, selected=selected)
    selected_count = len(selected)
    msg = f"📊 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛𝚜 (𝙿𝚊𝚐𝚎 {page+1}/{total_pages})\n\n💎 𝚃𝚊𝚙 𝚝𝚘 𝚝𝚘𝚐𝚐𝚕𝚎, 𝚝𝚑𝚎𝚗 𝙳𝚘𝚗𝚎\n📌 𝚂𝚎𝚕𝚎𝚌𝚝𝚎𝚍: {selected_count}"
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))

async def _show_livefs_pair_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = context.user_data.get('livefs_pair_page', 0)
    selected = context.user_data.get('livefs_selected_pairs', set())
    buttons, page, total_pages = _build_livefs_pair_page(page, selected=selected)
    msg = f"SELECT PAIRS (Page {page+1}/{total_pages})\n\nTap to toggle, then Done"
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))

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
    Parse signals in any flexible format using a robust regex-based parser.
    Returns list of dicts with keys: display_line, api_line, api_date, original_time, etc.
    """
    from datetime import datetime, timedelta, timezone
    import re

    lines = [l.strip() for l in signals_text.strip().split('\n') if l.strip()]
    tz_user = timezone(timedelta(hours=from_offset))
    tz_sio = timezone(timedelta(hours=SIO_ORIGINAL_TZ))
    base_date_obj = datetime.strptime(base_date, "%Y-%m-%d").date()
    parsed = []

    # Regex patterns
    # Time: HH:MM
    time_pattern = re.compile(r'\b(\d{2}:\d{2})\b')
    # Direction: CALL, PUT, BUY, SELL, UP, DOWN (case-insensitive)
    dir_pattern = re.compile(r'\b(CALL|PUT|BUY|SELL|UP|DOWN)\b', re.IGNORECASE)
    # Pair: alphanumeric, underscores, hyphens, dots, maybe ends with _OTC, _otc, -OTC
    # We'll extract the first token that looks like a pair (contains letters and maybe numbers, underscores, hyphens)
    # but we need to avoid capturing time or direction.
    # Strategy: remove time and direction from line, then clean the remaining to get pair.

    for line in lines:
        # Normalize fancy Unicode characters
        line = normalize_fancy(line)
        # Remove extra spaces
        line = re.sub(r'\s+', ' ', line).strip()

        # Extract time
        time_match = time_pattern.search(line)
        if not time_match:
            continue
        time_str = time_match.group(1)

        # Extract direction
        dir_match = dir_pattern.search(line)
        if not dir_match:
            continue
        dir_raw = dir_match.group(1).upper()
        if dir_raw in ("BUY", "UP"):
            direction = "CALL"
        elif dir_raw in ("SELL", "DOWN"):
            direction = "PUT"
        else:
            direction = dir_raw

        # Remove time and direction from line to get pair
        rest = line
        rest = re.sub(time_pattern, '', rest)
        rest = re.sub(dir_pattern, '', rest)
        # Remove common separators: ×, ;, ,, |, etc.
        rest = re.sub(r'[×;,|]', ' ', rest)
        # Remove extra spaces and split
        rest = re.sub(r'\s+', ' ', rest).strip()
        # Now rest should contain the pair, possibly with underscores/hyphens.
        # Take the first token that contains letters
        tokens = rest.split()
        pair_raw = None
        for tok in tokens:
            if re.search(r'[A-Za-z]', tok):
                pair_raw = tok
                break
        if not pair_raw:
            continue

        # Normalize pair: ensure -OTC format
        pair = pair_raw.upper()
        # Remove any trailing "Q" or "q" if present (sometimes from API)
        pair = re.sub(r'[Qq]$', '', pair)
        # Convert underscores to hyphen, and if it has _OTC or _otc, convert to -OTC
        pair = pair.replace('_', '-')
        if not pair.endswith('-OTC'):
            # If it ends with -OTC already, fine; else add -OTC
            if pair.endswith('OTC'):
                # e.g., USDPKR-OTC already?
                if not pair.endswith('-OTC'):
                    pair = pair.replace('OTC', '-OTC')  # but careful: if it's "USDPKROTC"?
            else:
                pair = pair + '-OTC'

        # Default timeframe to M1 (Checker 2.0 only works with M1 for now)
        tf = "M1"

        # Build display line (for user output)
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

async def _ask_livefs_start_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['state'] = STATE_LIVEFS_START_TIME
    msg = "🕐 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
    await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg))

async def _ask_livefs_end_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Call after start time is received
    context.user_data['state'] = STATE_LIVEFS_END_TIME
    msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:30"
    await update.message.reply_text(msg, entities=build_custom_emoji_entities(msg))

def _run_livefs_worker(uid: int, context):
    import time as t
    from datetime import datetime, timedelta, timezone
    import math
    import re

    days = context.user_data.get('livefs_days', 2)
    pairs = context.user_data.get('livefs_pairs', LIVE_PAIRS_FS)
    start_time = context.user_data.get('livefs_start_time', '00:00')
    end_time = context.user_data.get('livefs_end_time', '23:59')

    progress_msg = sender.send_message(
        uid,
        "⏳ 𝙻𝙸𝚅𝙴 𝙵𝚂 (Strategy 2)...\n🔄 0%  [░░░░░░░░░░]  0/0 pairs\n⏳ Waiting..."
    )
    if not progress_msg:
        return
    msg_id = progress_msg.id

    def update_progress(current, total, pair, status="🔍"):
        pct = int((current / total) * 100) if total else 0
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        text = (
            f"⏳ 𝙻𝙸𝚅𝙴 𝙵𝚂 ...\n"
            f"🔄 {pct}%  [{bar}]  {current}/{total} pairs\n"
            f"{status} {pair}\n"
            f"⏰ {datetime.now(timezone(timedelta(hours=5))).strftime('%H:%M:%S')}"
        )
        try:
            sender.edit_message(uid, msg_id, text)
        except Exception:
            pass

    # ---------- Time window ----------
    sh, sm = map(int, start_time.split(':'))
    eh, em = map(int, end_time.split(':'))
    start_min = sh*60 + sm
    end_min = eh*60 + em

    def time_in_window(hh, mm):
        tm = hh*60 + mm
        return start_min <= tm <= end_min

    # ---------- Helper to parse candle time ----------
    def parse_candle_time(c):
        raw = c.get('time')
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        if isinstance(raw, str):
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            try:
                return datetime.fromtimestamp(float(raw), tz=timezone.utc)
            except:
                pass
        return None

    # ---------- Strategy 2 helpers ----------
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
            low = min(c['l'] for c in window)
            high = max(c['h'] for c in window)
            if high - low != 0:
                k[i] = 100 * (candles[i]['c'] - low) / (high - low)
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
            window = [c['c'] for c in candles[i-period+1:i+1]]
            mean = sum(window)/period
            var = sum((x-mean)**2 for x in window)/period
            std = math.sqrt(var)
            mid[i] = mean
            upper[i] = mean + std_dev * std
            lower[i] = mean - std_dev * std
        return mid, upper, lower

    # ---------- Collect signals ----------
    raw_signals = []
    total_pairs = len(pairs)

    for idx, pair in enumerate(pairs, 1):
        update_progress(idx, total_pairs, pair, "🔍 Scanning")
        url = f"https://a39605-e545.a.jrnm.app/{pair}"
        success = False
        candles = []

        for attempt in range(1, 4):
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    success = True
                    break
                elif resp.status_code == 502:
                    update_progress(idx, total_pairs, pair, f"⚠️ 502 retry {attempt}/3")
                    t.sleep(1.5)
                    continue
                else:
                    update_progress(idx, total_pairs, pair, f"⚠️ HTTP {resp.status_code}")
                    break
            except Exception as e:
                update_progress(idx, total_pairs, pair, f"⚠️ Error retry {attempt}/3")
                t.sleep(1.5)
                continue

        if not success:
            update_progress(idx, total_pairs, pair, "❌ Skipped")
            t.sleep(0.3)
            continue

        try:
            data = resp.json()
            candles = data.get('candles', [])
            if not candles:
                update_progress(idx, total_pairs, pair, "⚠️ No candles")
                t.sleep(0.3)
                continue

            limit = days * 1440
            if len(candles) > limit:
                candles = candles[-limit:]

            candles.sort(key=lambda x: parse_candle_time(x) or datetime.min)

            closes = []
            for c in candles:
                if 'c' in c:
                    closes.append(c['c'])
                elif 'close' in c:
                    closes.append(c['close'])
                else:
                    closes.append(c.get('close', c.get('c', 0)))

            rsi = _calc_rsi(closes, 14)
            ma50 = _calc_sma(closes, 50)
            macd, sig, _ = _calc_macd(closes)
            k, d = _calc_stochastic(candles)
            bb_mid, bb_upper, bb_lower = _calc_bollinger(candles)

            for i in range(50, len(candles)-1):
                c = candles[i]
                dt_utc = parse_candle_time(c)
                if dt_utc is None:
                    continue
                dt_pk = dt_utc + timedelta(hours=5)
                time_str = dt_pk.strftime("%H:%M")
                if not time_in_window(dt_pk.hour, dt_pk.minute):
                    continue

                open_price = c.get('o', c.get('open', 0))
                close_price = c.get('c', c.get('close', 0))
                if open_price == 0 or close_price == 0:
                    continue
                dir_pred = 'CALL' if close_price > open_price else 'PUT'

                filters = 0

                # Filter 1: RSI + SMA50
                if dir_pred == 'CALL':
                    if rsi[i] is not None and ma50[i] is not None and 30 <= rsi[i] <= 50 and close_price > ma50[i]:
                        filters += 1
                else:
                    if rsi[i] is not None and ma50[i] is not None and 50 <= rsi[i] <= 70 and close_price < ma50[i]:
                        filters += 1

                # Filter 2: MACD crossover
                if dir_pred == 'CALL':
                    if macd[i] is not None and sig[i] is not None and macd[i-1] is not None and sig[i-1] is not None:
                        if macd[i] > sig[i] and macd[i-1] <= sig[i-1]:
                            filters += 1
                else:
                    if macd[i] is not None and sig[i] is not None and macd[i-1] is not None and sig[i-1] is not None:
                        if macd[i] < sig[i] and macd[i-1] >= sig[i-1]:
                            filters += 1

                # Filter 3: Bollinger Bands
                if dir_pred == 'CALL':
                    if bb_lower[i] is not None and close_price <= bb_lower[i] * 1.01:
                        filters += 1
                else:
                    if bb_upper[i] is not None and close_price >= bb_upper[i] * 0.99:
                        filters += 1

                # Filter 4: Stochastic
                if dir_pred == 'CALL':
                    if k[i] is not None and d[i] is not None and k[i] < 30 and k[i] > d[i]:
                        filters += 1
                else:
                    if k[i] is not None and d[i] is not None and k[i] > 70 and k[i] < d[i]:
                        filters += 1

                acc = (filters / 4) * 100
                if acc >= 75:
                    raw_signals.append({
                        'pair': pair,
                        'time': time_str,
                        'dir': dir_pred,
                        'acc': acc,
                        'ts': dt_utc.timestamp()
                    })

            update_progress(idx, total_pairs, pair, f"✅ Found {len(raw_signals)} so far")

        except Exception as e:
            update_progress(idx, total_pairs, pair, f"❌ Error: {str(e)[:30]}")
            t.sleep(0.3)
            continue

        t.sleep(0.5)

    # ---------- Group by (pair, time) with 5-min tolerance ----------
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
        best = max(sigs, key=lambda x: x['acc'])
        final_signals.append(best)

    unique = {}
    for s in final_signals:
        uk = (s['pair'], s['time'])
        if uk not in unique or s['acc'] > unique[uk]['acc']:
            unique[uk] = s
    final_signals = list(unique.values())
    final_signals.sort(key=lambda x: x['time'])

    # ---------- Build final message with bold header/footer only ----------
    if not final_signals:
        sender.edit_message(uid, msg_id, "❌ NO SIGNALS FOUND ")
        return

    now_pk = datetime.now(timezone(timedelta(hours=5)))
    day = now_pk.day
    suffix = "TH" if 4 <= day <= 20 or 24 <= day <= 30 else {1: "ST", 2: "ND", 3: "RD"}.get(day % 10, "TH")
    date_formatted = f"{day}{suffix} {now_pk.strftime('%B %Y').upper()}"

    # Header – bold
    header = (
        f"🐶{date_formatted} 🐶\n"
        f"🐱UTC/ GMT: ( +05:00 )\n"
        f"🐭1STEP MARTINGAL\n"
        f"\n"
        f"🐹ALL PLATFORM SIGNALS\n"
        f"🦊01 MINUTES"
    )

    # ----- Body (plain signal lines with 3 spaces) -----
    signal_lines = []
    for s in final_signals:
        # 3 spaces between pair and ×
        line = f"{s['pair']}   × {s['time']} > {s['dir']}"
        signal_lines.append(line)
    body = "\n".join(signal_lines)

    # ----- Footer (with emoji) -----
    footer = f"\n\n🐻DM- @Rohailtrader"

    full_text = header + "\n\n" + body + footer

    # Build custom emoji entities (for premium emoji IDs if any)
    entities = build_custom_emoji_entities(full_text)

    # Apply bold to header and footer only
    header_utf16_len = len(header.encode('utf-16-le')) // 2
    footer_start_utf16 = len((header + "\n\n" + body).encode('utf-16-le')) // 2
    footer_utf16_len = len(footer.encode('utf-16-le')) // 2

    entities.append(MessageEntity(type='bold', offset=0, length=header_utf16_len))
    entities.append(MessageEntity(type='bold', offset=footer_start_utf16, length=footer_utf16_len))

    # Send final message
    sender.edit_message(uid, msg_id, f"✅ 𝚂𝙲𝙰𝙽 𝙲𝙾𝙼𝙿𝙻𝙴𝚃𝙴! {len(final_signals)} signals found.\n📤 Sending final result...")

    asyncio.run_coroutine_threadsafe(
        context.bot.send_message(chat_id=uid, text=full_text, entities=entities),
        MAIN_LOOP
    )

def run_livefs_checker(uid: int, date_str: str, mtg_level: int, signals_text: str, context):
    import time as t
    from datetime import datetime, timedelta, timezone
    import re
    from collections import defaultdict

    # ---------- Parser: returns base pair (no OTC, no underscore) ----------
    def parse_signal_line(line):
        line = normalize_fancy(line)
        line = re.sub(r'^M\d+\s*', '', line)
        pair_match = re.search(r'([A-Z0-9]+[_-]?[A-Z0-9]*(?:[_-]OTC)?)', line)
        if not pair_match:
            return None, None, None
        pair_raw = pair_match.group(1).upper()
        # Remove OTC/underscores to get base symbol (e.g., EURUSD)
        pair = pair_raw.replace('_OTC', '').replace('-OTC', '').replace('_', '')
        time_match = re.search(r'(\d{2}:\d{2})', line)
        if not time_match:
            return None, None, None
        time_str = time_match.group(1)
        try:
            h, m = map(int, time_str.split(':'))
            time_str = f"{h:02d}:{m:02d}"
        except:
            pass
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
        return pair, time_str, direction

    # Parse signals
    raw_lines = [l.strip() for l in signals_text.split('\n') if l.strip()]
    if not raw_lines:
        sender.send_message(uid, "❌ No signals provided.")
        return

    signals = []
    for line in raw_lines:
        pair, time_str, direction = parse_signal_line(line)
        if not pair:
            continue
        signals.append({
            'pair': pair,
            'time': time_str,
            'dir': direction,
        })
    if not signals:
        sender.send_message(uid, "❌ No valid signals found.")
        return

    # ---------- Check if signal time is in the future (UTC+5) ----------
    now_pk = datetime.now(timezone(timedelta(hours=5)))
    for sig in signals:
        dt_str = f"{date_str} {sig['time']}"
        sig_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        sig_dt = sig_dt.replace(tzinfo=timezone(timedelta(hours=5)))
        sig['future'] = sig_dt > now_pk

    # Progress message
    progress_msg = sender.send_message(uid, "🔍 Checking signals...\n🔄 0% [░░░░░░░░░░]  0/0 signals")
    if not progress_msg:
        return
    msg_id = progress_msg.id

    def update_progress(current, total, status="⏳"):
        pct = int((current / total) * 100) if total else 0
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        text = f"🔍 Checking signals...\n🔄 {pct}% [{bar}]  {current}/{total}\n{status}"
        try:
            sender.edit_message(uid, msg_id, text)
        except Exception:
            pass

    # ---------- Timezone conversion: UTC+5 to UTC-3 (only for non-future signals) ----------
    USER_TZ = 5
    SIO_TZ = -3
    tz_user = timezone(timedelta(hours=USER_TZ))
    tz_sio = timezone(timedelta(hours=SIO_TZ))
    user_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz_user)

    groups = defaultdict(list)  # sio_date -> list of api_lines
    future_signals = {}         # index -> api_line for future signals (to assign ⏳)

    print(f"\n🔍 USER DATE: {date_str} (UTC+{USER_TZ})")

    for idx, sig in enumerate(signals):
        pair = sig['pair']
        time_str = sig['time']
        direction = sig['dir']

        if sig['future']:
            # Mark as future (⏳) and skip SIO
            future_signals[idx] = f"M1;{pair};{time_str};{direction}"
            print(f"   ⏳ {pair} {time_str} (future, not sent to API)")
            continue

        naive_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        user_dt = naive_dt.replace(tzinfo=tz_user)

        sio_dt = user_dt.astimezone(tz_sio)
        sio_date = sio_dt.strftime("%Y-%m-%d")
        sio_time = sio_dt.strftime("%H:%M")

        print(f"   {pair} {time_str} (UTC+5) → {sio_date} {sio_time} (UTC-3)")

        api_line = f"M1;{pair};{sio_time};{direction}"
        groups[sio_date].append((idx, api_line))  # store index for mapping

    # ---------- Send each group to SIO (but errors are masked) ----------
    all_results = {}  # api_line -> (icon, status)
    total_groups = len(groups)

    for gidx, (sio_date, api_lines_with_idx) in enumerate(groups.items(), 1):
        # Extract just api_lines for request
        api_signals = [line for _, line in api_lines_with_idx]
        update_progress(gidx, total_groups, f"📤 Sending {len(api_signals)} signals...")
        print(f"\n📤 Sending group {gidx}: date={sio_date}, signals={api_signals}")

        check_id = _sio_checker_request(api_signals, sio_date, mtg_level)
        if not check_id:
            print("❌ API request failed (check_id is None)")
            for idx, line in api_lines_with_idx:
                all_results[line] = ("❌", "LOSS")
            continue

        result = _sio_checker_poll(check_id, max_retries=30, delay=2)
        if not result:
            print("❌ API polling failed")
            for idx, line in api_lines_with_idx:
                all_results[line] = ("❌", "LOSS")
            continue

        print(f"✅ API returned: {result.get('signals', [])}")

        raw_signals = result.get("signals", [])
        for s in raw_signals:
            parts = s.split(";")
            if len(parts) < 5:
                continue
            tf, pair, tm, direc, status = parts[:5]
            api_line = f"{tf};{pair};{tm};{direc}"
            if status == "WIN":
                all_results[api_line] = ("✅", "WIN")
            elif status == "G1":
                all_results[api_line] = ("✅¹", "G1")
            elif status == "G2":
                all_results[api_line] = ("✅²", "G2")
            else:
                all_results[api_line] = ("❌", "LOSS")

    # ---------- Build final output with ⏳ for future signals ----------
    print("\n📊 MAPPING RESULTS:")
    final_display = []
    wins = losses = pending = 0

    for idx, sig in enumerate(signals):
        pair = sig['pair']
        time_str = sig['time']
        direction = sig['dir']

        if sig['future']:
            icon = "⏳"
            pending += 1
            final_display.append((f"M1 {pair} {time_str} {direction}", icon))
            print(f"   ⏳ {pair} {time_str} (future)")
            continue

        # Non-future: get result from API
        naive_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        user_dt = naive_dt.replace(tzinfo=tz_user)
        sio_dt = user_dt.astimezone(tz_sio)
        sio_time = sio_dt.strftime("%H:%M")
        api_line = f"M1;{pair};{sio_time};{direction}"

        icon, status = all_results.get(api_line, ("❌", "LOSS"))
        print(f"   {pair} {time_str} → {icon} {status}")

        if icon.startswith("✅"):
            wins += 1
        elif icon == "❌":
            losses += 1
        else:
            pending += 1

        final_display.append((f"M1 {pair} {time_str} {direction}", icon))

    total = wins + losses + pending

    # ---------- Build header (bold) ----------
    header = (
        f"{fancy_font('▰▱▱ 𝙻𝙸𝚅𝙴 𝙵𝚂 𝙲𝙷𝙴𝙲𝙺𝙴𝚁 ▱▱▰')}\n"
        f"{fancy_font('              ┏━━━━━━━━━━━┓')}\n"
        f"{fancy_font('                 🗓 - ')}{fancy_font(date_str)}{fancy_font('          ')}\n"
        f"{fancy_font('              ┗━━━━━━━━━━━┛')}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
    )

    # Body (plain)
    body_lines = []
    for sig, icon in final_display:
        body_lines.append(f"{sig} {icon}")
    body = "\n".join(body_lines)

    # Summary (bold footer)
    summary = (
        f"\n{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
        f"{fancy_font('🦊 Total : ')}{fancy_font(str(total))}\n"
        f"{fancy_font('✅ Win: ')}{fancy_font(str(wins))}\n"
        f"{fancy_font('✖ Loss: ')}{fancy_font(str(losses))}\n"
        f"{fancy_font('⏳ Pending: ')}{fancy_font(str(pending))}\n"
        f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}"
    )

    # Combine: header + newline + body + newline + summary
    full_text = header + "\n" + body + "\n" + summary

    # Build entities for bold: header and summary only
    entities = build_custom_emoji_entities(full_text)

    # Header length (UTF-16 code units)
    header_utf16_len = len(header.encode('utf-16-le')) // 2
    # Body length (to calculate summary start)
    header_and_body = header + "\n" + body
    summary_start_utf16 = len(header_and_body.encode('utf-16-le')) // 2
    summary_utf16_len = len(summary.encode('utf-16-le')) // 2

    entities.append(MessageEntity(type='bold', offset=0, length=header_utf16_len))
    entities.append(MessageEntity(type='bold', offset=summary_start_utf16, length=summary_utf16_len))

    # Delete progress message
    try:
        sender.delete_message(uid, msg_id)
    except:
        pass

    # Send final message with Home button
    buttons = [[colored_button(" Home", "back_to_main", KeyboardButtonStyle.SUCCESS, "5416041192905265756")]]
    reply_markup = InlineKeyboardMarkup(buttons)

    asyncio.run_coroutine_threadsafe(
        context.bot.send_message(
            chat_id=uid,
            text=full_text,
            entities=entities,
            reply_markup=reply_markup
        ),
        MAIN_LOOP
    )

async def show_lfc_loss_candles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    loss_data_list = context.user_data.get('lfc_loss_signals', [])
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

            # Convert candle dicts to format draw_loss_chart expects
            # Ensure each candle has keys: time, open, high, low, close
            chart_candles = []
            for c in candles:
                chart_candles.append({
                    'time': c.get('time', 0),
                    'open': c.get('open', 0),
                    'high': c.get('high', 0),
                    'low': c.get('low', 0),
                    'close': c.get('close', 0)
                })
            img_path = draw_loss_chart(pair, entry_time, direction, chart_candles)
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

def build_utc_keyboard_with_pagination(prefix, page=0, per_page=12):
    offsets = list(range(-12, 15))
    total = len(offsets)
    start = page * per_page
    end = min(start + per_page, total)
    page_offsets = offsets[start:end]
    total_pages = (total + per_page - 1) // per_page
    buttons = []
    row = []
    for off in page_offsets:
        sign = "+" if off >= 0 else ""
        label = f"UTC{sign}{off}"
        cb = f"{prefix}{off}"
        row.append(colored_button(label, cb, KeyboardButtonStyle.PRIMARY, None))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    nav_row = []
    if page > 0:
        nav_row.append(colored_button("⬅️", f"{prefix}page_{page-1}", KeyboardButtonStyle.PRIMARY, "5260651934720740549"))
    if page < total_pages - 1:
        nav_row.append(colored_button("➡️", f"{prefix}page_{page+1}", KeyboardButtonStyle.PRIMARY, "5416117059207572332"))
    if nav_row:
        buttons.append(nav_row)
    buttons.append([colored_button("🔙 Back", "checker_back_to_settings", KeyboardButtonStyle.DANGER, "6145317070578916456")])
    return buttons, page, total_pages

async def show_utc_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, prefix="checker_utc_"):
    query = update.callback_query
    if query:
        await query.answer()
    page = context.user_data.get('utc_page', 0)
    buttons, page, total_pages = build_utc_keyboard_with_pagination(prefix, page)
    context.user_data['utc_page'] = page
    msg = f"🌐 Select UTC offset (Page {page+1}/{total_pages}):"
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    if query:
        await query.edit_message_text(msg, entities=entities, reply_markup=markup)
    else:
        await update.message.reply_text(msg, entities=entities, reply_markup=markup)

# ══════════════ BOT HANDLERS ══════════════
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    # ─── Clear stale flow flags so a fresh /start never gets falsely blocked ───
    context.user_data.pop('auto_trade_mode', None)

    # ─── Log user to all_users (for broadcast) ───
    log_user_to_all_users(update)

    text = (
        f"🥸Assalamu Alaikum @{username} 👋\n\n"
        f"Welcome to our most advanced binary market tool🫠\n\n"
        f"🦶 Your all-in-one Binary Trading Tools Bot is here to help you trade smarter, faster, and more efficiently.\n\n"
        f"😷 Get accurate signals, powerful tools, and real-time insights to boost your trading experience.\n\n"
        f"🤢Tap below to get started and explore the features! 👇\n\n"
    )

    entities = build_custom_emoji_entities(text)
    text_utf16_len = len(text.encode('utf-16-le')) // 2
    entities.append(MessageEntity(type='bold', offset=0, length=text_utf16_len))

    # ─── All button rows (two per row) ────────────────
    buttons = [
        # Row 1: OTC LIVE MODE + FOREX LIVE MODE
        [colored_button("     OTC LIVE MODE     ", "menu_analysis", KeyboardButtonStyle.SUCCESS, "6145248943807667330"),
         colored_button("     FOREX LIVE MODE   ", "menu_forex_live", KeyboardButtonStyle.SUCCESS, "6102524232644698570")],

        # Row 2: AI OTC + Live FS Checker
        [colored_button("        AI OTC        ", "menu_ai_mode", KeyboardButtonStyle.SUCCESS, "5314391089514291948"),
         colored_button(" Live FS Checker ", "menu_livefs_checker", KeyboardButtonStyle.SUCCESS, "6231262273864736290")],

        # Row 3: Signal Checker + Backtest
        [colored_button("     Signal Checker     ", "menu_checker", KeyboardButtonStyle.SUCCESS, "6145553439809084250"),
         colored_button("        Backtest        ", "menu_backtest", KeyboardButtonStyle.SUCCESS, "6147840110462245787")],

        # Row 4: Auto Trade + Future Signals
        [colored_button("       Auto Trade       ", "auto_trade_start", KeyboardButtonStyle.SUCCESS, "5316681209026191987"),
         colored_button("    Future Signals     ", "menu_futuresignal", KeyboardButtonStyle.SUCCESS, "6062153953833917531")],

        # Row 5: Live FS + CHECKER 2.0
        [colored_button("     Live FS     ", "menu_live_fs", KeyboardButtonStyle.SUCCESS, "4956492465465984073"),
         colored_button("    CHECKER 2.0        ", "menu_checker2", KeyboardButtonStyle.SUCCESS, "6147440218942218700")],

        # Row 6: BACKTEST 2.0 + BLACKOUT FS
        [colored_button("   BACKTEST 2.0        ", "menu_backtest2", KeyboardButtonStyle.SUCCESS, "6145546134069714639"),
         colored_button("     BLACKOUT FS         ", "menu_blackout_fs", KeyboardButtonStyle.SUCCESS, "6282889047778007721")],

        # Row 7: BLACKOUT CHECKER + UTC Converter
        [colored_button(" BLACKOUT CHECKER ", "menu_blackout_checker", KeyboardButtonStyle.SUCCESS, "6282889047778007721"),
         colored_button("     UTC Converter      ", "menu_utc_converter", KeyboardButtonStyle.SUCCESS, "5413879192267805083")],

        # Row 8: Pair Payout% + Market Trend
        [colored_button("     Pair Payout%      ", "menu_pair_payout", KeyboardButtonStyle.SUCCESS, "6145449239607515472"),
         colored_button("     Market Trend      ", "menu_market_trend", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],

        # Row 9: Candle Colors + Text Formatter
        [colored_button("     Candle Colors     ", "menu_candle_colors", KeyboardButtonStyle.SUCCESS, "5217911744495624141"),
         colored_button("    Text Formatter     ", "menu_text_formatter", KeyboardButtonStyle.SUCCESS, "5282843764451195532")],

        # Row 10: Font Changer + Trend Filter
        [colored_button("     Font Changer      ", "menu_font_changer", KeyboardButtonStyle.SUCCESS, "6282685788450721937"),
         colored_button("     Trend Filter      ", "menu_trend_filter", KeyboardButtonStyle.SUCCESS, "6086858751749396920")],

        # Row 11: Auto Signal + AI Filter FS
        [colored_button("      Auto Signal      ", "menu_auto_signal", KeyboardButtonStyle.SUCCESS, "5965469803299738005"),
         colored_button("       AI Filter FS     ", "menu_ai_filter", KeyboardButtonStyle.SUCCESS, "6217370240800527004")],

        # Row 12: AI Chart Analyzer + WHITEOUT FS
        [colored_button("   AI Chart Analyzer   ", "menu_chart_analyzer", KeyboardButtonStyle.SUCCESS, "5854710508065658472"),
         colored_button("   WHITEOUT FS      ", "menu_whiteout", KeyboardButtonStyle.SUCCESS, "5278411976677014753")],

        # Row 13: NEWS SIGNAL + SWAP C/P
        [colored_button("     NEWS SIGNAL    ", "menu_news_filter", KeyboardButtonStyle.SUCCESS, "6154657967317717391"),
         colored_button("    SWAP C/P   ", "menu_swap_cp", KeyboardButtonStyle.SUCCESS, "6275878213746955453")],

        # Row 14: Pair List + About
        [colored_button("     Pair List     ", "menu_pair_list", KeyboardButtonStyle.SUCCESS, "6104927314091447883"),
         colored_button("         About          ", "menu_admin", KeyboardButtonStyle.DANGER, "6035189951581129197")],
    ]

    # ─── Row 15: Multi Engine + Strategy Finder ──────
    buttons.append([
        colored_button("  Multi Engine  ", "menu_multi_engine", KeyboardButtonStyle.SUCCESS, "5361612816119768210"),
        colored_button("  Strategy Finder  ", "menu_strategy_finder", KeyboardButtonStyle.SUCCESS, "6017163295234987326"),
    ])

    # ─── Broadcast button (owner only) ────────────────
    if uid == OWNER_ID:
        buttons.append([
            colored_button(" Broadcast", "admin_broadcast", KeyboardButtonStyle.DANGER, "6129805886383723340")
        ])

    reply_markup = InlineKeyboardMarkup(buttons)
    await context.bot.send_message(chat_id=uid, text=text, entities=entities, reply_markup=reply_markup)
    context.user_data['strategy_active'] = False
    context.user_data['state'] = None
    context.user_data.pop('trading_mode', None)


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
        context.user_data.pop('auto_trade_mode', None)
        context.user_data.pop('state', None)
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
       context.user_data['trading_mode'] = 'otc'
       text = "🤖 𝚂𝙴𝙻𝙴𝙲𝚃 𝚂𝚃𝚁𝙰𝚃𝙴𝙶𝚈 (1-7):"
       buttons = []
       for i in range(1, 8):
          style = KeyboardButtonStyle.SUCCESS if i % 2 else KeyboardButtonStyle.PRIMARY
          buttons.append([InlineKeyboardButton(f"Strategy {i}", callback_data=f"strat_{i}", style=style)])
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(text)
       await query.message.reply_text(text, entities=entities, reply_markup=markup)
       return

    elif data == "admin_broadcast":
       if uid != OWNER_ID:
          await query.answer("⛔ Access denied.", show_alert=True)
          return
       context.user_data['state'] = STATE_BROADCAST_WAIT_FOR_MESSAGE
       await query.message.reply_text(
         "📢 Send the broadcast message.\n"
         "You can use premium emojis and formatting.\n\n"
         "Type /cancel to abort."
       )
       await query.answer()

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

    elif data == "menu_live_fs":
        context.user_data.pop('fut4_start_time', None)
        context.user_data.pop('fut4_end_time', None)
        context.user_data['uid'] = uid
        context.user_data['state'] = STATE_LIVEFS_DAYS
        msg = "🔥 𝙻𝙸𝚅𝙴 𝙵𝚂 — 𝙳𝙰𝚈𝚂 𝚂𝙴𝙻𝙴𝙲𝚃𝙸𝙾𝙽\n\n"
        msg += "🗓 𝚂𝚎𝚕𝚎𝚌𝚝 𝚗𝚞𝚖𝚋𝚎𝚛 𝚘𝚏 𝚍𝚊𝚢𝚜 𝚝𝚘 𝚊𝚗𝚊𝚕𝚢𝚣𝚎:"
        buttons = [
        [colored_button(" 2 Days ", "livefs_days_2", KeyboardButtonStyle.SUCCESS, "6145553439809084250"),
         colored_button(" 3 Days ", "livefs_days_3", KeyboardButtonStyle.PRIMARY, "6147654280112248427")],
        [colored_button(" 6 Days ", "livefs_days_6", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
         colored_button(" Custom ", "livefs_days_custom", KeyboardButtonStyle.PRIMARY, "5217822164362739968")],
        [colored_button(" Cancel ", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg)
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "menu_livefs_checker":
        context.user_data['state'] = STATE_LIVEFS_CHECKER_DATE
        msg = "🗓 Select Date:"
        buttons = [
        [colored_button(" Today ", "lfc_date_today", KeyboardButtonStyle.SUCCESS, "6145553439809084250")],
        [colored_button(" Yesterday ", "lfc_date_yesterday", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        await query.message.reply_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=markup)

    elif data.startswith("lfc_date_"):
        query = update.callback_query
        await query.answer()
        if data == "lfc_date_today":
            date_str = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d")
        else:  # yesterday
           date_str = (datetime.now(timezone(timedelta(hours=5))) - timedelta(days=1)).strftime("%Y-%m-%d")
        context.user_data['lfc_date'] = date_str
        context.user_data['state'] = STATE_LIVEFS_CHECKER_MTG
        msg = "🎯 Select Martingale Level:"
        buttons = [
        [colored_button(" 0 (entry only) ", "lfc_mtg_0", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
        [colored_button(" 1 (entry+1) ", "lfc_mtg_1", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
        [colored_button(" 2 (entry+2) ", "lfc_mtg_2", KeyboardButtonStyle.DANGER, "6145248943807667330")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=markup)

    elif data.startswith("lfc_mtg_"):
        query = update.callback_query
        await query.answer()
        mtg = int(data.split("_")[-1])
        context.user_data['lfc_mtg'] = mtg
        context.user_data['state'] = STATE_LIVEFS_CHECKER_SIGNALS
        msg = "📝 Paste your signals (one per line):\nFormat: M1;PAIR;HH:MM;DIRECTION\n(e.g., M1;EURUSD;09:00;CALL)"
        await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg))

    # -------- FIX: livefs_pair_done handled BEFORE startswith --------
    elif data == "livefs_pair_done":
        selected = context.user_data.get('livefs_selected_pairs', set())
        if not selected:
            await query.answer("❌ Select at least one pair!", show_alert=True)
            return
        context.user_data['livefs_pairs'] = list(selected)
        await _ask_livefs_start_time(update, context)
        return

    elif data.startswith("livefs_pairpage_"):
        page = int(data.split("_")[-1])
        context.user_data['livefs_pair_page'] = page
        await _show_livefs_pair_page(update, context)
        return

    elif data.startswith("livefs_pickpair_"):
        pair = data.replace("livefs_pickpair_", "")
        selected = context.user_data.get('livefs_selected_pairs', set())
        if pair in selected:
            selected.discard(pair)
        else:
            selected.add(pair)
        context.user_data['livefs_selected_pairs'] = selected
        page = context.user_data.get('livefs_pair_page', 0)
        buttons, page, total_pages = _build_livefs_pair_page(page, selected=selected)
        msg = f"🎯 𝚂𝚎𝚕𝚎𝚌𝚝 𝚙𝚊𝚒𝚛𝚜 (𝙿𝚊𝚐𝚎 {page+1}/{total_pages})\n\n💎 𝚃𝚊𝚙 𝚝𝚘 𝚝𝚘𝚐𝚐𝚕𝚎, 𝚝𝚑𝚎𝚗 𝙳𝚘𝚗𝚎"
        await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=InlineKeyboardMarkup(buttons))
        return

    elif data.startswith("livefs_pair_"):
        await query.answer()
        if data == "livefs_pair_all":
            context.user_data['livefs_pairs'] = LIVE_PAIRS_FS  # list defined below
            await _ask_livefs_start_time(update, context)
        elif data == "livefs_pair_custom":
            # Start paginated selection
            context.user_data['livefs_selected_pairs'] = set()
            context.user_data['livefs_pair_page'] = 0
            await _show_livefs_pair_page(update, context)
            return

    elif data == "fut_strategy_4":
       # Strategy 4 – Pattern Replay
       context.user_data['uid'] = query.from_user.id
       context.user_data['state'] = STATE_FUT4_DAYS
       msg = "🧠 SMZX PRIV CORE 𝚂𝚃𝚁𝙰𝚃𝙴𝙶𝚈\n━━━━━━━━━━━━━━━━━━\n𝚂𝚎𝚕𝚎𝚌𝚝 𝙻𝚘𝚘𝚔𝚋𝚊𝚌𝚔 𝙳𝚊𝚢𝚜 (𝟸-𝟷𝟶):"
       buttons = [
        [colored_button(" 3 Days ", "fut4_days_3", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
         colored_button(" 5 Days ", "fut4_days_5", KeyboardButtonStyle.PRIMARY, "6145553439809084250")],
        [colored_button(" 7 Days ", "fut4_days_7", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
         colored_button(" Custom ", "fut4_days_custom", KeyboardButtonStyle.SUCCESS, "5217822164362739968")],
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg)
       await context.bot.send_message(chat_id=uid, text=msg, entities=entities, reply_markup=markup)

    elif data == "fut_strategy_5":
       context.user_data['uid'] = query.from_user.id
       context.user_data['fut5_selected_pairs'] = set()
       context.user_data['fut5_pair_page'] = 0
       context.user_data['state'] = STATE_FUT5_PAIR_MODE
       # directly show pair selection (no market choice, only OTC)
       await show_fut5_pair_page(update, context)
       return

    elif data == "menu_swap_cp":
       uid = query.from_user.id
       context.user_data['state'] = STATE_SWAP_CP
       msg = (
        "🐹 **SWAP CALL/PUT**\n\n"
        "🐶 Send me your signal list (any format, one per line).\n"
        "I will swap **CALL ↔ PUT** for every signal.\n\n"
        "📌 Examples:\n"
        "   M1;EURUSD-OTC;09:00;CALL\n"
        "   EURUSD 09:00 PUT\n"
        "   09:00 EURUSD CALL\n\n"
        "🚀 Paste your list now..."
       )
       entities = build_custom_emoji_entities(msg)
       await query.message.reply_text(msg, entities=entities)

    elif data == "menu_strategy_finder":
       if not is_authorized(uid):
          await query.answer("⛔ Access denied.", show_alert=True)
          return
       context.user_data['state'] = STATE_FINDER_MARKET
       msg = "🔍 STRATEGY FINDER\n\nSelect market to scan:"
       buttons = [
        [colored_button("OTC", "finder_market_otc", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
        [colored_button("LIVE", "finder_market_live", KeyboardButtonStyle.PRIMARY, "6062085844242537125")],
        [colored_button("BOTH", "finder_market_both", KeyboardButtonStyle.PRIMARY, "5314391089514291948")],
        [colored_button("Back", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data.startswith("finder_market_"):
       market = data.replace("finder_market_", "")
       context.user_data['finder_market'] = market
       context.user_data['state'] = STATE_FINDER_TF
       msg = "📊 Select timeframe:"
       buttons = [
        [colored_button("1M", "finder_tf_1m", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
        [colored_button("5M", "finder_tf_5m", KeyboardButtonStyle.PRIMARY, "6062085844242537125")],
        [colored_button("Back", "finder_back_to_market", KeyboardButtonStyle.DANGER, "6145317070578916456")],
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data.startswith("finder_tf_"):
       tf = data.replace("finder_tf_", "")
       context.user_data['finder_tf'] = tf  # '1m' or '5m'
       # Start the scan
       await query.edit_message_text("⏳ Starting Strategy Finder scan...\nThis may take 30–60 seconds.")
       # Launch background thread
       threading.Thread(target=run_strategy_finder, args=(uid, context), daemon=True).start()
       return

    elif data == "finder_back_to_market":
       # Go back to market selection
       context.user_data['state'] = STATE_FINDER_MARKET
       msg = "🔍 STRATEGY FINDER\n\nSelect market to scan:"
       buttons = [
        [colored_button("OTC", "finder_market_otc", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
        [colored_button("LIVE", "finder_market_live", KeyboardButtonStyle.PRIMARY, "6062085844242537125")],
        [colored_button("BOTH", "finder_market_both", KeyboardButtonStyle.PRIMARY, "5314391089514291948")],
        [colored_button("Back", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    # ==================== MULTI ENGINE ====================
    elif data == "menu_multi_engine":
       if not is_authorized(uid):
          await query.answer("⛔ Access denied.", show_alert=True)
          return
       context.user_data['state'] = STATE_MULTI_ENGINE_MAIN
       msg = "🔫 MULTI ENGINE MODE\n\nSelect your desired mode:"
       entities = build_custom_emoji_entities(msg)
       buttons = [
        [colored_button(" Manual Signal", "multi_manual", KeyboardButtonStyle.SUCCESS, "5215327832040811010")],
        [colored_button(" Non-Stop Signal", "multi_nonstop", KeyboardButtonStyle.PRIMARY, "6255693594032605539")],
        [colored_button(" Settings", "multi_settings", KeyboardButtonStyle.PRIMARY, "5341715473882955310")],
        [colored_button(" Back", "back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")],
       ]
       await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data == "multi_manual":
       context.user_data['state'] = STATE_MULTI_MANUAL_MARKET
       msg = "✍️ Manual Signal\n\nChoose market type:"
       buttons = [
        [colored_button(" OTC MARKET", "multi_market_otc", KeyboardButtonStyle.SUCCESS, "6264904718334563984")],
        [colored_button(" LIVE MARKET", "multi_market_live", KeyboardButtonStyle.PRIMARY, "6264620236880745132")],
        [colored_button(" Back", "multi_back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")],
       ]
       await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data.startswith("multi_market_"):
       market = data.replace("multi_market_", "")
       context.user_data['multi_manual_market'] = market   # ← STORE MARKET
       pairs = MULTI_ENGINE_OTC if market == "otc" else MULTI_ENGINE_LIVE
       await show_multi_pair_list(update, context, pairs, manual=True)
       return

    elif data.startswith("multi_manual_select_"):
       pair = data.replace("multi_manual_select_", "")
       await query.answer(f"Analyzing {pair}...")
       await query.edit_message_text(f"⏳ Analyzing {pair}...")
       threading.Thread(target=run_multi_manual_analysis, args=(uid, pair, context), daemon=True).start()
       return

    elif data.startswith("multi_manual_reanalyse_"):
       pair = data.replace("multi_manual_reanalyse_", "")
       await query.answer(f"Re‑analysing {pair}...")
       # Send a new status message instead of editing the photo
       await context.bot.send_message(
          chat_id=uid,
          text=f"🔄 Re‑analysing {pair}..."
       )
       threading.Thread(target=run_multi_manual_analysis, args=(uid, pair, context), daemon=True).start()
       return

    elif data == "multi_nonstop":
       context.user_data['state'] = STATE_MULTI_NONSTOP_FILTER
       msg = "🔁 Non‑Stop Signal\n\nChoose mode:"
       buttons = [
        [colored_button("OTC Market", "multi_ns_otc", KeyboardButtonStyle.SUCCESS, "6264904718334563984")],
        [colored_button("Live Market", "multi_ns_live", KeyboardButtonStyle.PRIMARY, "6264620236880745132")],
        [colored_button("All Pairs", "multi_ns_all", KeyboardButtonStyle.PRIMARY, "6102690370569642675")],
        [colored_button("Avoid under 80%", "multi_ns_avoid", KeyboardButtonStyle.DANGER, "6298686186600798589")],
        [colored_button("Manual Selection", "multi_ns_manual", KeyboardButtonStyle.PRIMARY, "5192825506239616944")],
        [colored_button("Back", "multi_back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")],
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data == "multi_ns_otc":
       await query.edit_message_text(f"⏳ Starting Non‑Stop with {len(MULTI_ENGINE_OTC)} OTC pairs...")
       await start_multi_nonstop(update, context, MULTI_ENGINE_OTC)
       return

    elif data == "multi_ns_live":
       await query.edit_message_text(f"⏳ Starting Non‑Stop with {len(MULTI_ENGINE_LIVE)} Live pairs...")
       await start_multi_nonstop(update, context, MULTI_ENGINE_LIVE)
       return

    elif data == "multi_ns_all":
       all_pairs = MULTI_ENGINE_OTC + MULTI_ENGINE_LIVE
       await query.edit_message_text(f"⏳ Starting Non‑Stop with {len(all_pairs)} pairs (OTC+Live)...")
       await start_multi_nonstop(update, context, all_pairs)
       return

    elif data == "multi_ns_avoid":
       context.user_data['state'] = STATE_MULTI_NONSTOP_MARKET
       msg = "Select market to scan (payout ≥ 80%):"
       buttons = [
        [colored_button("OTC", "multi_avoid_otc", KeyboardButtonStyle.SUCCESS, "6264904718334563984")],
        [colored_button("LIVE", "multi_avoid_live", KeyboardButtonStyle.PRIMARY, "6264620236880745132")],
        [colored_button("Back", "multi_back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")],
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data == "multi_ns_manual":
       context.user_data['multi_selected_pairs'] = set()
       context.user_data['state'] = STATE_MULTI_NONSTOP_MANUAL_SELECT
       # Force fresh fetch (pair_data = None)
       await show_multi_pair_list(update, context, MULTI_ENGINE_OTC, manual=False, pair_data=None)
       return

    elif data == "multi_filter_all":
       all_pairs = [p.replace("_OTC", "").replace("_otc", "") for p in MULTI_ENGINE_OTC] + MULTI_ENGINE_LIVE
       await query.edit_message_text("⏳ Starting Non‑Stop with all pairs...")
       await start_multi_nonstop(update, context, all_pairs)
       return

    elif data == "multi_filter_avoid":
       context.user_data['state'] = STATE_MULTI_NONSTOP_MARKET
       msg = "Select market to scan (payout >= 80%):"
       buttons = [
        [colored_button(" OTC", "multi_avoid_otc", KeyboardButtonStyle.SUCCESS, "6264904718334563984")],
        [colored_button(" LIVE", "multi_avoid_live", KeyboardButtonStyle.PRIMARY, "6264620236880745132")],
        [colored_button(" Back", "multi_back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")],
       ]
       await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data.startswith("multi_avoid_"):
       market = data.replace("multi_avoid_", "")
       base_pairs = MULTI_ENGINE_OTC if market == "otc" else MULTI_ENGINE_LIVE
       filtered = []
       progress = await query.message.reply_text("⏳ Filtering pairs...")
       for p in base_pairs:
          _, _, payout = fetch_candles_mrbeaxt(p, count=1)
          if payout >= 80:
              filtered.append(p)
          await asyncio.sleep(0.1)
       await progress.delete()
       if not filtered:
          await query.message.reply_text("❌ No pairs with payout >= 80% in this market.")
          return
       await query.edit_message_text(f"✅ Found {len(filtered)} pairs. Starting Non‑Stop...")
       await start_multi_nonstop(update, context, all_pairs)
       return

    elif data == "multi_manual_changepair":
       market = context.user_data.get('multi_manual_market', 'otc')
       pairs = MULTI_ENGINE_OTC if market == "otc" else MULTI_ENGINE_LIVE
       await send_pair_list_new(update, context, pairs, manual=True)
       await query.answer("Here are the pairs again.")
       return

    elif data == "multi_filter_manual":
       # Show OTC pair list with toggles (for manual selection)
       context.user_data['multi_selected_pairs'] = set()
       context.user_data['state'] = STATE_MULTI_NONSTOP_MANUAL_SELECT
       # We'll reuse show_multi_pair_list but with a toggle flag; we need a different display
       # For simplicity, we'll just show all OTC pairs with toggle buttons
       pairs = MULTI_ENGINE_OTC
       # Build toggle keyboard
       buttons = []
       row = []
       for p in pairs:
          label = p.upper().replace("_OTC", "").replace("_otc", "")
          cb = f"multi_toggle_pair_{p}"
          row.append(colored_button(label, cb, KeyboardButtonStyle.PRIMARY))
          if len(row) == 2:
              buttons.append(row)
              row = []
       if row:
          buttons.append(row)
       buttons.append([colored_button("✅ Done", "multi_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])
       markup = InlineKeyboardMarkup(buttons)
       msg = "Select pairs (tap to toggle):"
       await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=markup)
       return

    elif data.startswith("multi_toggle_pair_"):
       pair = data.replace("multi_toggle_pair_", "")
       selected = context.user_data.get('multi_selected_pairs', set())
       if pair in selected:
          selected.discard(pair)
       else:
          selected.add(pair)
       context.user_data['multi_selected_pairs'] = selected

       # Re‑render using cached pair_data
       pair_data = context.user_data.get('multi_pair_data')
       if pair_data is None:
          # Fallback: fetch if not cached (should not happen)
          pairs = MULTI_ENGINE_OTC
          loading = await query.message.reply_text("⏳ Fetching payouts...")
          async def fetch_payout(p):
              _, _, payout = await asyncio.to_thread(fetch_candles_mrbeaxt, p, 1)
              return p, payout
          tasks = [fetch_payout(p) for p in pairs]
          results = await asyncio.gather(*tasks)
          pair_data = [(pair, payout) for pair, payout in results]
          context.user_data['multi_pair_data'] = pair_data
          await loading.delete()
       await show_multi_pair_list(update, context, MULTI_ENGINE_OTC, manual=False, pair_data=pair_data)
       return

    elif data == "multi_pair_done":
       selected = context.user_data.get('multi_selected_pairs', set())
       if not selected:
          await query.answer("❌ Select at least one pair!", show_alert=True)
          return
       await query.edit_message_text(f"✅ {len(selected)} pairs selected. Starting Non‑Stop...")
       await start_multi_nonstop(update, context, list(selected))
       return

    elif data in ["multi_pause", "multi_resume", "multi_stop"]:
       if data == "multi_pause":
          context.user_data['multi_paused'] = True
          await query.answer("Paused")
       elif data == "multi_resume":
          context.user_data['multi_paused'] = False
          await query.answer("Resumed")
       else:  # stop
          context.user_data['multi_running'] = False
          await query.answer("Stopping...")
          await query.edit_message_text("🔴 Non‑Stop stopped.")
          return
       update_multi_control_panel(uid, context)
       return

    elif data == "multi_settings":
       owner = context.user_data.get('multi_owner_name', '@Rohailtrader')
       msg = (
        f"⚙️ NON STOP Signal Settings\n\n"

        f"👤 Owner Name: {owner}\n\n"
        f"Customize the owner name shown at the bottom of each signal."
       )
       buttons = [
        [colored_button("Change owner name", "multi_settings_owner", KeyboardButtonStyle.PRIMARY, "5787604998435115723")],
        [colored_button("Back", "multi_back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")],
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data == "multi_settings_owner":
       context.user_data['state'] = STATE_MULTI_SETTINGS_OWNER_NAME
       msg = (
        "👤 Change Owner Name\n\n"

        "Type the name you want to show as the owner on each live signal.\n\n"
        "Example:  @MyChannel  or  SMZxBOT\n\n"

        "⬇️ Send your owner name now:"
       )
       await query.edit_message_text(msg)
       await query.answer()
       return

    elif data == "multi_back_to_main":
       context.user_data['state'] = STATE_MULTI_ENGINE_MAIN
       msg = "🔫 MULTI ENGINE MODE\n\nSelect your desired mode:"
       entities = build_custom_emoji_entities(msg)
       buttons = [
        [colored_button(" Manual Signal", "multi_manual", KeyboardButtonStyle.SUCCESS, "5215327832040811010")],   # 🚀
        [colored_button(" Non-Stop Signal", "multi_nonstop", KeyboardButtonStyle.PRIMARY, "6255693594032605539")],  # 📊
        [colored_button(" Settings", "multi_settings", KeyboardButtonStyle.PRIMARY, "5341715473882955310")],       # ⚙️
        [colored_button(" Back to Main Menu", "back_to_main", KeyboardButtonStyle.DANGER, "5152368980190560982")], # ❌
       ]
       await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       await query.answer()
       return

    elif data == "menu_ai_filter":
       uid = query.from_user.id
       context.user_data['uid'] = uid
       context.user_data['state'] = STATE_AI_FILTER_SIGNALS
       msg = (
        "🤖 **AI FILTER FS**\n\n"
        "Send me your signal list (one per line).\n"
        "Any flexible format is accepted.\n\n"
        "I will filter signals using lastest Ai.\n\n"
        "📌 Paste your signals now:"
       )
       entities = build_custom_emoji_entities(msg)
       await query.message.reply_text(msg, entities=entities)

    elif data == "menu_forex_live":
        context.user_data['trading_mode'] = 'forex'
        text = "🤖 𝚂𝙴𝙻𝙴𝙲𝚃 𝚂𝚃𝚁𝙰𝚃𝙴𝙶𝚈 (1-7):"
        buttons = []
        for i in range(1, 8):
           style = KeyboardButtonStyle.SUCCESS if i % 2 else KeyboardButtonStyle.PRIMARY
           buttons.append([InlineKeyboardButton(f"Strategy {i}", callback_data=f"strat_{i}", style=style)])
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(text)
        await query.message.reply_text(text, entities=entities, reply_markup=markup)
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

    elif data.startswith("livefs_days_"):
        await query.answer()
        if data == "livefs_days_custom":
            context.user_data['state'] = STATE_LIVEFS_CUSTOM_DAYS
            msg = "🔢 𝙴𝚗𝚝𝚎𝚛 𝚗𝚞𝚖𝚋𝚎𝚛 𝚘𝚏 𝚍𝚊𝚢𝚜 (𝟷-𝟷𝟶):"
            await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg))
            return
        else:
            days = int(data.split("_")[-1])
            context.user_data['livefs_days'] = days
            await _show_livefs_pair_mode(update, context)
            return

    elif data.startswith("fut5_pairpage_"):
       page = int(data.split("_")[-1])
       context.user_data['fut5_pair_page'] = page
       await show_fut5_pair_page(update, context)
       return

    elif data.startswith("fut5_pickpair_"):
       pair = data.replace("fut5_pickpair_", "")
       selected = context.user_data.get('fut5_selected_pairs', set())
       if pair in selected:
          selected.discard(pair)
       else:
          selected.add(pair)
       context.user_data['fut5_selected_pairs'] = selected
       await show_fut5_pair_page(update, context)
       return

    elif data == "fut5_pair_done":
       selected = context.user_data.get('fut5_selected_pairs', set())
       if not selected:
          await query.answer("❌ 𝙰𝚝 𝚕𝚎𝚊𝚜𝚝 𝚘𝚗𝚎 𝚙𝚊𝚒𝚛 𝚜𝚎𝚕𝚎𝚌𝚝 𝚔𝚊𝚛𝚎𝚒𝚗!", show_alert=True)
          return
       context.user_data['fut5_selected_pairs'] = selected
       context.user_data['state'] = STATE_FUT5_DAYS
       # Show days buttons
       msg = "🗓 𝚂𝙴𝙻𝙴𝙲𝚃 𝙳𝙰𝚈𝚂 (𝟷-𝟽):"
       buttons = []
       row = []
       for d in range(1, 8):
         row.append(colored_button(f" {d} Day{'s' if d>1 else ''} ", f"fut5_days_{d}", KeyboardButtonStyle.PRIMARY, "6145553439809084250"))
         if len(row) == 4:
             buttons.append(row)
             row = []
       if row:
         buttons.append(row)
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg)
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data.startswith("fut5_days_"):
       days = int(data.split("_")[-1])
       context.user_data['fut5_days'] = days
       context.user_data['state'] = STATE_FUT5_ACCURACY
       # Show accuracy buttons: 65,70,75,80,85,90,95,100
       msg = f"✅ 𝙳𝚊𝚢𝚜: {days}\n\n🎯 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙸𝙽𝙸𝙼𝚄𝙼 𝙰𝙲𝙲𝚄𝚁𝙰𝙲𝚈 (65-100%):"
       acc_vals = [65, 70, 75, 80, 85, 90, 95, 100]
       buttons = []
       row = []
       for acc in acc_vals:
         row.append(colored_button(f" {acc}% ", f"fut5_acc_{acc}", KeyboardButtonStyle.SUCCESS if acc>=80 else KeyboardButtonStyle.PRIMARY, "6145248943807667330"))
         if len(row) == 4:
             buttons.append(row)
             row = []
       if row:
         buttons.append(row)
       buttons.append([colored_button(" Cancel", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")])
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg)
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data.startswith("fut5_acc_"):
       accuracy = int(data.split("_")[-1])
       context.user_data['fut5_accuracy'] = accuracy
       context.user_data['state'] = STATE_FUT5_START_TIME
       msg = f"✅ 𝙰𝚌𝚌𝚞𝚛𝚊𝚌𝚢: {accuracy}%\n\n⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
       entities = build_custom_emoji_entities(msg)
       await query.edit_message_text(msg, entities=entities)
       return

    elif data.startswith("auto_signal_market_"):
       market = data.replace("auto_signal_market_", "")
       context.user_data['auto_signal_market'] = market
       context.user_data['state'] = STATE_AUTO_SIGNAL_STRATEGY
    
       buttons = [[colored_button(f" Strategy {i}", f"autostrat_{i}", KeyboardButtonStyle.PRIMARY if i%2 else KeyboardButtonStyle.SUCCESS)] for i in range(1, 8)]
       markup = InlineKeyboardMarkup(buttons)
       msg = f"✅ Market: {market.upper()}\n\n👑 𝚂𝚎𝚕𝚎𝚌𝚝 𝚜𝚝𝚛𝚊𝚝𝚎𝚐𝚢:"
       entities = build_custom_emoji_entities(msg)
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data == "menu_pair_list":
       pair_list_text = generate_pair_list()
       entities = build_custom_emoji_entities(pair_list_text)
       # Find header end and footer start positions (in characters, not bytes)
       # Header ends before "🟢 OTC PAIRS"
       header_end_char = pair_list_text.find("\n\n🟢")
       if header_end_char == -1:
          header_end_char = 200  # fallback
       # Footer starts at "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" near the end
       footer_start_char = pair_list_text.find("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
       if footer_start_char == -1:
          footer_start_char = len(pair_list_text) - 100
       # Convert character positions to UTF-16 code unit offsets
       header_utf16_len = len(pair_list_text[:header_end_char].encode('utf-16-le')) // 2
       footer_start_utf16 = len(pair_list_text[:footer_start_char].encode('utf-16-le')) // 2
       footer_utf16_len = len(pair_list_text[footer_start_char:].encode('utf-16-le')) // 2
    
       # Add bold entities for header and footer
       entities.append(MessageEntity(type='bold', offset=0, length=header_utf16_len))
       entities.append(MessageEntity(type='bold', offset=footer_start_utf16, length=footer_utf16_len))
    
       # Send as new message using context.bot.send_message (more reliable)
       await context.bot.send_message(chat_id=uid, text=pair_list_text, entities=entities)
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
        msg = "🗓 𝙱𝙻𝙰𝙲𝙺𝙾𝚄𝚃 𝙲𝙷𝙴𝙲𝙺𝙴𝚁\n\n𝚂𝚎𝚕𝚎𝚌𝚝 𝚍𝚊𝚝𝚎:"
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
        msg = "🗓 𝙴𝚗𝚝𝚎𝚛 𝚍𝚊𝚝𝚎 (𝚈𝚈𝚈𝚈-𝙼𝙼-𝙳𝙳):"
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
        uid = query.from_user.id
        if 'checker_utc' not in context.user_data:
          context.user_data['checker_utc'] = 5
        context.user_data['state'] = STATE_CHECKER_SETTINGS
        msg = (
        f"🔍 SMZX OTC CHECKER\n"
        f"🎬 Settings:  UTC +{context.user_data['checker_utc']:02d}:00  (To change UTC click the UTC Change button)\n\n"
        f"🪇 Send your list now (All valid formats are supported):\n"
        f"M1;EURUSD-OTC;14:26;CALL\n"
        f"M1 EURUSD-OTC 14:42 PUT\n\n"
        f"Press Home button for main menu"
        )
        buttons = [
        [colored_button(" CHANGE UTC", "checker_change_utc", KeyboardButtonStyle.PRIMARY, "5447410659077661506")],
        [colored_button(" Home", "back_to_main", KeyboardButtonStyle.SUCCESS, "5416041192905265756")]
        ]
        markup = InlineKeyboardMarkup(buttons)
        entities = build_custom_emoji_entities(msg) + build_bold_entities(msg, [
            "SMZX OTC CHECKER",
            f"UTC +{context.user_data['checker_utc']:02d}:00",
            "Send your list now (All valid formats are supported):",
            "Press Home button for main menu"
        ])
        await query.message.reply_text(msg, entities=entities, reply_markup=markup)
        return

    elif data == "checker_change_utc":
       context.user_data['state'] = STATE_CHECKER_UTC_SELECT
       await show_utc_selection(update, context)
       return

    elif data.startswith("checker_utc_page_"):
       page = int(data.split("_")[-1])
       context.user_data['utc_page'] = page
       await show_utc_selection(update, context)
       return

    elif data.startswith("checker_utc_"):
       offset_str = data.replace("checker_utc_", "")
       try:
          offset = int(offset_str)
       except ValueError:
          return
       context.user_data['checker_utc'] = offset
       context.user_data['state'] = STATE_CHECKER_SETTINGS
       msg = (
        f"🔍 SMZX OTC CHECKER\n"
        f"🎬 Settings:  UTC +{offset:02d}:00  (To change UTC click the UTC Change button)\n\n"
        f"🪇 Send your list now (All valid formats are supported):\n"
        f"M1;EURUSD-OTC;14:26;CALL\n"
        f"M1 EURUSD-OTC 14:42 PUT\n\n"
        f"Press Home button for main menu"
       )
       buttons = [
        [colored_button(" CHANGE UTC", "checker_change_utc", KeyboardButtonStyle.PRIMARY, "5447410659077661506")],
        [colored_button(" Home", "back_to_main", KeyboardButtonStyle.SUCCESS, "5416041192905265756")]
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg) + build_bold_entities(msg, [
           "SMZX OTC CHECKER",
           f"UTC +{offset:02d}:00",
           "Send your list now (All valid formats are supported):",
           "Press Home button for main menu"
       ])
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data == "checker_back_to_settings":
       context.user_data['state'] = STATE_CHECKER_SETTINGS
       uid = query.from_user.id
       offset = context.user_data.get('checker_utc', 5)
       msg = (
        f"🔍 SMZX OTC CHECKER\n"
        f"🎬 Settings:  UTC +{offset:02d}:00  (To change UTC click the UTC Change button)\n\n"
        f"🪇 Send your list now (All valid formats are supported):\n"
        f"M1;EURUSD-OTC;14:26;CALL\n"
        f"M1 EURUSD-OTC 14:42 PUT\n\n"
        f"Press Home button for main menu"
       )
       buttons = [
        [colored_button(" CHANGE UTC", "checker_change_utc", KeyboardButtonStyle.PRIMARY, "5447410659077661506")],
        [colored_button(" Home", "back_to_main", KeyboardButtonStyle.SUCCESS, "5416041192905265756")]
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg) + build_bold_entities(msg, [
           "SMZX OTC CHECKER",
           f"UTC +{offset:02d}:00",
           "Send your list now (All valid formats are supported):",
           "Press Home button for main menu"
       ])
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data.startswith("checker_mtg_"):
       mtg = int(data.split("_")[-1])
       context.user_data['checker_mtg'] = mtg
       context.user_data['state'] = STATE_CHECKER_DATE_SELECT
       mode_label = 'MTG ' + str(mtg) if mtg > 0 else 'NON MTG'
       msg = f"✅ Mode: {mode_label}\n\n🗓 Select Date\n\nChoose which date to check signals for:"
       mode_entities = build_bold_entities(msg, [f"Mode: {mode_label}"])
       buttons = [
        [
            colored_button(" Today", "checker_date_today", KeyboardButtonStyle.SUCCESS, "6102750517291654328"),
            colored_button(" Yesterday", "checker_date_yesterday", KeyboardButtonStyle.PRIMARY, "6102689176568732825")
        ]
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg) + mode_entities
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data.startswith("checker_date_"):
       now_pk = datetime.now(timezone(timedelta(hours=5)))
       if data == "checker_date_today":
          date_str = now_pk.strftime("%Y-%m-%d")
       else:
          date_str = (now_pk - timedelta(days=1)).strftime("%Y-%m-%d")
       context.user_data['checker_date'] = date_str
       context.user_data['state'] = STATE_CHECKER_PAYOUT_FILTER
       msg = f"🗓 Selected date: {date_str}\n\n💰 Payout Filter\n\n✅ Enable Payout Filter\nSignals with payout below 80% will be shown with their payout% and will not be counted as WIN or LOSS.\n\nWould you like to enable the payout filter?"
       buttons = [
        [
            colored_button(" Yes", "checker_payout_yes", KeyboardButtonStyle.SUCCESS, "6147440218942218700"),
            colored_button(" No", "checker_payout_no", KeyboardButtonStyle.DANGER, "6145317070578916456")
        ]
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg) + build_bold_entities(msg, [
           f"Selected date: {date_str}",
           "Payout Filter",
           "Would you like to enable the payout filter?"
       ])
       await query.edit_message_text(msg, entities=entities, reply_markup=markup)
       return

    elif data.startswith("checker_payout_"):
       enable_filter = data == "checker_payout_yes"
       context.user_data['checker_payout_filter'] = enable_filter
       context.user_data['state'] = STATE_CHECKER_RUNNING
       signals = context.user_data.get('checker_signals_raw', [])
       mtg = context.user_data.get('checker_mtg', 1)
       date_str = context.user_data.get('checker_date')
       user_utc = context.user_data.get('checker_utc', 5)
       await query.edit_message_text(":)")
       await run_advanced_checker(update, context, signals, mtg, date_str, user_utc, enable_filter)
       return

    elif data == "checker_recheck_payout":
       context.user_data['checker_payout_filter'] = True
       context.user_data['state'] = STATE_CHECKER_RUNNING
       signals = context.user_data.get('checker_signals_raw', [])
       mtg = context.user_data.get('checker_mtg', 1)
       date_str = context.user_data.get('checker_date')
       user_utc = context.user_data.get('checker_utc', 5)
       await query.answer(".:)..")
       await context.bot.send_message(chat_id=query.from_user.id, text=" :(")
       await run_advanced_checker(update, context, signals, mtg, date_str, user_utc, True)
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
            [colored_button(" Strategy 4 – SMZ PRIV CORE", "fut_strategy_4", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
            [colored_button(" Strategy 5 – SMZ NIGHTYY", "fut_strategy_5", KeyboardButtonStyle.SUCCESS, "5283055978785285857")],

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
                # 🔥 FIX: use display_time (user's local UTC-offset time) if present,
                # otherwise fall back to entry_time (raw API/UTC+5 time).
                entry_time = loss_data.get('display_time') or loss_data['entry_time']
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
        return

    elif data == "menu_whiteout":
       uid = query.from_user.id
       context.user_data['uid'] = uid
       context.user_data['state'] = STATE_WHITEOUT_DAYS
       msg = "📅 𝚂𝙴𝙻𝙴𝙲𝚃 𝙳𝙰𝚈𝚂\n\nChoose number of past days (excluding today):"
       buttons = [
        [colored_button(" 2 Days ", "white_days_2", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
         colored_button(" 3 Days ", "white_days_3", KeyboardButtonStyle.PRIMARY, "6147654280112248427")],
        [colored_button(" 5 Days ", "white_days_5", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
         colored_button(" 7 Days ", "white_days_7", KeyboardButtonStyle.PRIMARY, "5316681209026191987")],
        [colored_button(" Custom (1-7) ", "white_days_custom", KeyboardButtonStyle.SUCCESS, "5217822164362739968")],
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg)
       await query.message.reply_text(msg, entities=entities, reply_markup=markup)

    elif data == "menu_news_filter":
       uid = query.from_user.id
       await query.message.reply_text("🗞 Fetching today's news...\n⏳ Please wait.")
       threading.Thread(target=run_news_filter, args=(uid, context), daemon=True).start()

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
        context.user_data.pop('auto_trade_mode', None)  # clear stale flag so Auto Signal isn't falsely blocked
        context.user_data['uid'] = uid
        context.user_data['state'] = STATE_AUTO_SIGNAL_FORMAT
        msg = "📝 𝚂𝙴𝙻𝙴𝙲𝚃 𝚂𝙸𝙶𝙽𝙰𝙻 𝙵𝙾𝚁𝙼𝙰𝚃\n\nChoose how you want signals to be sent:"
        buttons = [
            [colored_button(" Format 1 ", "auto_signal_fmt1", KeyboardButtonStyle.SUCCESS, "5283055978785285857")],
            [colored_button(" Format 2 ", "auto_signal_fmt2", KeyboardButtonStyle.PRIMARY, "5267419403019886452")],
            [colored_button(" Format 3 ", "auto_signal_fmt3", KeyboardButtonStyle.PRIMARY, "6233426881547345061")],
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

async def _show_livefs_pair_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['state'] = STATE_LIVEFS_PAIR_MODE
    msg = "📊 𝚂𝙴𝙻𝙴𝙲𝚃 𝙿𝙰𝙸𝚁 𝙼𝙾𝙳𝙴"
    buttons = [
        [colored_button(" All Pairs (20) ", "livefs_pair_all", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
        [colored_button(" Custom Pairs ", "livefs_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    await query.edit_message_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=markup)

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


async def strategy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    """Proceed with selected strategy after MM setup."""
    
    # 🔥 Trading mode check
    trading_mode = context.user_data.get('trading_mode', 'otc')
    
    if strat == 1:
        text = "✅ Strategy 1 selected. Scanning..."
        await query.message.reply_text(text)
        bot = SMZXBot(uid)
        if trading_mode == 'forex':
            bot.pairs = FOREX_PAIRS
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
        
    elif strat == 7:
        text = "✅ Strategy 7 selected. Enter min confidence % (50-100):"
        await query.message.reply_text(text)
        return STATE_STRATEGY7_ACCURACY
        
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


async def s2_accuracy_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            trading_mode = context.user_data.get('trading_mode', 'otc')
            bot = SMZXBot(uid)
            if trading_mode == 'forex':
                bot.pairs = FOREX_PAIRS
            sender.send_message(uid, f"✅ Min accuracy set to {val}%.\nStarting analysis...")
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 50-100:")
            return S2_ACCURACY
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S2_ACCURACY

async def s3_accuracy_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def s3_lookback_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            trading_mode = context.user_data.get('trading_mode', 'otc')
            bot = SMZXBot(uid)
            if trading_mode == 'forex':
                bot.pairs = FOREX_PAIRS
            sender.send_message(uid, f"✅ Lookback set to {val}. Starting analysis...")
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter between 10-30:")
            return S3_LOOKBACK
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Enter a number.")
        return S3_LOOKBACK

async def s4_accuracy_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            trading_mode = context.user_data.get('trading_mode', 'otc')
            bot = SMZXBot(uid)
            if trading_mode == 'forex':
                bot.pairs = FOREX_PAIRS
            sender.send_message(uid, f"✅ Accuracy set. Starting analysis...")
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter 50-100:")
            return S4_ACCURACY
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S4_ACCURACY

async def s5_score_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            trading_mode = context.user_data.get('trading_mode', 'otc')
            bot = SMZXBot(uid)
            if trading_mode == 'forex':
                bot.pairs = FOREX_PAIRS
            sender.send_message(uid, f"✅ Score set. Starting analysis...")
            threading.Thread(target=bot.run_single_signal, daemon=True).start()
            context.user_data['strategy_active'] = False
            return ConversationHandler.END
        else:
            await update.message.reply_text("❌ Enter 50-100:")
            return S5_SCORE
    except ValueError:
        await update.message.reply_text(f"❌ Invalid number: '{cleaned}'. Please enter a number.")
        return S5_SCORE

# ----- Checker date callback ----


# ----- Continue and Stop commands -----

async def continue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        sender.send_message(uid, "🤖 AI Mode — Scanning for next best signal...")
    else:
        bot = SMZXBot(uid)
        bot.strategy = st.strategy
        # 🔥 YAHAN FIX HAI – trading_mode check karein
        trading_mode = context.user_data.get('trading_mode', 'otc')
        if trading_mode == 'forex':
            bot.pairs = FOREX_PAIRS
        # else OTC pairs (default)
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
    logger.info(f"[DEBUG] Classifying result: {res}")
    rc = res.get("result")
    if isinstance(rc, (int, float)):
        if rc == 1:
            logger.info("[DEBUG] -> WIN")
            return "win"
        if rc == 2:
            logger.info("[DEBUG] -> LOSS")
            return "loss"
        if rc == 3:
            logger.info("[DEBUG] -> TIE")
            return "tie"
    # fallback
    profit = res.get("profit")
    if profit is not None:
        if profit > 0:
            logger.info("[DEBUG] -> WIN (profit)")
            return "win"
        if profit < 0:
            logger.info("[DEBUG] -> LOSS (profit)")
            return "loss"
        if profit == 0:
            logger.info("[DEBUG] -> TIE (profit)")
            return "tie"
    logger.warning(f"[DEBUG] Unknown result, returning TIE")
    return "tie"


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
    """High-accuracy auto trade loop — TradoWix WebSocket only."""
    import time as _t
    import asyncio as _aio
    import uuid
    import threading

    loop = _aio.new_event_loop()
    _aio.set_event_loop(loop)
    client = trader.client

    # ---- trade-result correlation via WS callbacks ----
    trader._results = []          # list to hold incoming results
    trader._last_trade_id = None
    trader._opened_evt = threading.Event()

    def _on_open(data):
        trader._last_opened = data
        trade_id = (
            data.get("data", {}).get("id") or
            data.get("id") or
            data.get("tradeId") or
            data.get("requestId")
        )
        if trade_id:
            trader._last_trade_id = trade_id
            logger.info(f"[DEBUG] tradeOpened -> tradeId: {trade_id}")
        else:
            logger.warning(f"[DEBUG] Could not extract trade id from: {data}")
        trader._opened_evt.set()

    def _on_res(data):
        # Handle batch results
        if data.get("type") == "tradeResultsBatch":
            batch = data.get("data", [])
            # Log the entire batch for debugging
            logger.info(f"[DEBUG] Full batch: {data}")
            for item in batch:
                trader._results.append(item)
                logger.info(f"[DEBUG] tradeResult: {item}")
            return
        # Single result
        if data.get("tradeId") or data.get("id"):
            trader._results.append(data)
            logger.info(f"[DEBUG] tradeResult (single): {data}")

    client.on_trade_opened(_on_open)
    client.on_trade_result(_on_res)

    def fire(pair, side, amount):
        request_id = f"trade-{int(_t.time()*1000)}-{uuid.uuid4().hex[:6]}"
        try:
            client.place_trade(
                pair,
                side,
                amount,
                duration_minutes=1,
                is_demo=trader.is_demo
            )
            return request_id
        except Exception as e:
            return f"place failed: {e}"

    def confirm_opened():
        if not trader._opened_evt.wait(timeout=10):
            return {"error": "no tradeOpened"}
        trade_id = trader._last_trade_id
        if not trade_id:
            return {"error": "no trade id in opened data"}
        return {"tradeId": trade_id}

    def wait_result():
        """Wait for the first result after trade placement."""
        deadline = _t.time() + 240
        while _t.time() < deadline:
            if trader._results:
                # Take the first result
                res = trader._results.pop(0)
                logger.info(f"[DEBUG] Popped result: {res}")
                return res
            _t.sleep(0.1)
        return {"error": "result timeout"}

    # ---- Telethon sender helpers ----
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
        while True:
            d = ts - _t.time()
            if d <= 0.03:
                break
            await _aio.sleep(min(d - 0.02, 0.2))
        while _t.time() < ts:
            pass

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

    async def ensure_connected():
        nonlocal client
        if not client._authenticated:
            await send("🔌 Reconnecting to broker...")
            try:
                client.disconnect()
            except:
                pass
            new_client = TradoWixClient()
            new_client.login(trader.email, trader.password)
            new_client.connect()
            new_client.on_trade_opened(_on_open)
            new_client.on_trade_result(_on_res)
            for p in trader.pairs:
                try:
                    new_client.subscribe(p, lookback_minutes=240, timeframe=60)
                except:
                    pass
            client = new_client
            trader.client = new_client
            await send("✅ Reconnected successfully.")

    async def run():
        st = get_state(trader.uid)
        st.strategy = trader.strategy
        trader._loss_cooldown = {}
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
        await _aio.sleep(6)

        trader.balance = _auto_acct_balance(trader)
        trader.starting_balance = trader.balance

        while trader.running:
            try:
                if getattr(trader, "paused", False):
                    reset_status()
                    await send(f"⏳ {fancy_font('PAUSED')}\n🚀 Tap Resume to continue scanning.")
                    while trader.running and getattr(trader, "paused", False):
                        await _aio.sleep(1)
                    if not trader.running:
                        break
                    await send(f"🚀 {fancy_font('RESUMED')}\nScanning again...")
                    reset_status()

                if not getattr(client, "_authenticated", True):
                    reset_status()
                    await send(f"🚨 {fancy_font('RECONNECTING')}\n🔍 Lost broker link — restoring connection...")
                    for _ in range(60):
                        if not trader.running or getattr(client, "_authenticated", False):
                            break
                        await _aio.sleep(1)
                    if not trader.running:
                        break
                    if getattr(client, "_authenticated", False):
                        await send(f"✅ {fancy_font('RECONNECTED')}\nResuming scan...")
                    else:
                        continue

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

                # ---- pre-boundary SCAN ----
                now = _t.time()
                boundary = (int(now // 60) + 1) * 60
                await sleep_until(boundary - 3)
                if not trader.running:
                    break

                entry_label = datetime.fromtimestamp(boundary, tz=_UTC5).strftime("%H:%M")
                await status(
                    f"🔍 {fancy_font('SCANNING')}\n"
                    f"🤖 {trader.strategy}. {trader.strategy_name}\n"
                    f"📊 {len(trader.pairs)} OTC pairs\n"
                    f"⏰ Next entry : {entry_label}"
                )

                chosen = None
                for pair in trader.pairs:
                    if not trader.running:
                        break
                    if trader._loss_cooldown.get(pair, 0) > _t.time():
                        continue
                    try:
                        candles = client.get_candles(pair, timeframe=60, count=210, lookback_minutes=240, timeout=1.0)
                    except Exception:
                        candles = []
                    if not candles or len(candles) < 30:
                        continue
                    direction, _edt, conf = _auto_run_strategy(trader.strategy, candles, st)
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
                trader._loss_cooldown[pair] = _t.time() + 240
                side = "call" if direction == "CALL" else "put"
                entry_label = datetime.fromtimestamp(boundary, tz=_UTC5).strftime("%H:%M:%S")
                expiry_label = datetime.fromtimestamp(boundary + 60, tz=_UTC5).strftime("%H:%M:%S")
                base_amt = round(max(1.0, trader.balance * trader.risk_percent / 100.0), 2)

                reset_status()
                await sleep_until(boundary)
                await ensure_connected()

                # ---- Place the trade ----
                # CRITICAL: Clear any stale results before placing
                trader._results.clear()
                logger.info("[DEBUG] Cleared results before trade")
                req_id = fire(pair, side, base_amt)
                trader.trade_count += 1
                await send(_auto_signal_card(trader, pair, direction, conf, entry_label, expiry_label, base_amt))

                if isinstance(req_id, str) and req_id.startswith("place failed"):
                    await send(f"⚠️ Trade error: {req_id}")
                    await episode_pause()
                    continue

                # ---- Wait for tradeOpened ----
                opened = await loop.run_in_executor(None, confirm_opened)
                if opened.get("error"):
                    await send(f"⚠️ Trade error: {opened['error']}")
                    await episode_pause()
                    continue

                await send(
                    f"✅ {fancy_font('TRADE OPENED')}\n"
                    f"📊 {pair}   {'UP/CALL' if side == 'call' else 'DOWN/PUT'}\n"
                    f"💲 ${base_amt:.2f}   ⏰ {entry_label} → {expiry_label}\n"
                    f"⏳ Waiting for candle to close..."
                )

                # ---- Wait for result ----
                res = await loop.run_in_executor(None, wait_result)
                if res.get("error"):
                    await send(f"⚠️ Result error: {res['error']}")
                    await episode_pause()
                    continue

                # Log the raw result for debugging
                logger.info(f"[DEBUG] Raw result for classification: {res}")

                outcome = _auto_classify(res, side)
                logger.info(f"[DEBUG] Classified outcome: {outcome}")

                # ---- Handle win/tie/loss ----
                if outcome != "loss":
                    await _aio.sleep(0.8)
                    trader.balance = _auto_acct_balance(trader)
                    pnl = trader.balance - trader.starting_balance
                    if outcome == "win":
                        trader.win_count += 1
                        trader.win_streak += 1
                        trader.loss_streak = 0
                        profit = float(res.get("profit") or round(base_amt * 0.9, 2))
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

                # ---- LOSS ----
                trader.balance = _auto_acct_balance(trader)
                pnl = trader.balance - trader.starting_balance
                logger.info(f"[DEBUG] Processing LOSS, mtg_enabled={trader.mtg_enabled}")

                if not trader.mtg_enabled:
                    trader.loss_count += 1
                    trader.loss_streak += 1
                    trader.win_streak = 0
                    await send(
                        f"❌ {fancy_font('LOSS')}  -${base_amt:.2f}\n"
                        f"💎 Balance : ${trader.balance:.2f}\n"
                        f"📊 Session : {pnl:+.2f}   |   {trader.win_count}W / {trader.loss_count}L"
                    )
                    trader._loss_cooldown[pair] = _t.time() + 180
                    await episode_pause()
                    continue

                # ---- MARTINGALE ----
                mtg_amt = round(base_amt * 2, 2)
                mtg_candle = int(_t.time() // 60) * 60
                if mtg_candle < boundary + 60:
                    mtg_candle = boundary + 60
                await sleep_until(mtg_candle)
                mtg_entry = datetime.fromtimestamp(mtg_candle, tz=_UTC5).strftime("%H:%M:%S")
                mtg_expiry = datetime.fromtimestamp(mtg_candle + 60, tz=_UTC5).strftime("%H:%M:%S")

                # Clear results again for MTG
                trader._results.clear()
                logger.info("[DEBUG] Cleared results before MTG")
                req_id_mtg = fire(pair, side, mtg_amt)
                trader.trade_count += 1
                await send(
                    f"🔥 {fancy_font('MARTINGALE')}  (Step 1)\n"
                    f"📊 {pair}   {'UP/CALL' if side == 'call' else 'DOWN/PUT'}\n"
                    f"💲 ${mtg_amt:.2f}  (2x)    ⏰ {mtg_entry} → {mtg_expiry}\n"
                    f"🔥 Zero-delay placed."
                )
                if isinstance(req_id_mtg, str) and req_id_mtg.startswith("place failed"):
                    await send(f"⚠️ Martingale error: {req_id_mtg}")
                    await episode_pause()
                    continue

                opened2 = await loop.run_in_executor(None, confirm_opened)
                if opened2.get("error"):
                    await send(f"⚠️ Martingale error: {opened2['error']}")
                    await episode_pause()
                    continue

                await send(
                    f"✅ {fancy_font('MTG OPENED')}\n"
                    f"📊 {pair}   {'UP/CALL' if side == 'call' else 'DOWN/PUT'}\n"
                    f"💲 ${mtg_amt:.2f}   ⏰ {mtg_entry} → {mtg_expiry}\n"
                    f"⏳ Waiting for candle to close..."
                )

                res2 = await loop.run_in_executor(None, wait_result)
                if res2.get("error"):
                    await send(f"⚠️ Martingale result error: {res2['error']}")
                    await episode_pause()
                    continue

                logger.info(f"[DEBUG] Raw MTG result: {res2}")
                out2 = _auto_classify(res2, side)
                logger.info(f"[DEBUG] MTG outcome: {out2}")

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

                await episode_pause()

            except Exception as _e:
                try:
                    await send(f"⚠️ {fancy_font('SKIPPED')} a cycle\n🔍 {str(_e)[:120]}")
                except Exception:
                    pass
                await _aio.sleep(2)
                continue

        # final report
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

        # ---------- CLEAR ALL STALE DATA ----------
        context.user_data.pop('auto_target_id', None)
        context.user_data.pop('state', None)
        context.user_data['auto_trade_mode'] = True
        print("[DEBUG] auto_trade_start: cleared auto_target_id, state, set auto_trade_mode=True")

        # ---------- FORCE FRESH LOGIN ----------
        if trader.client is not None:
            try:
                trader.client.disconnect()
            except Exception:
                pass
            trader.client = None
        trader.email = None
        trader.password = None
        trader.is_demo = True
        trader.balance = 0.0
        trader.starting_balance = 0.0

        context.user_data['strategy_active'] = False
        context.user_data['state'] = STATE_AUTO_LOGIN_EMAIL
        msg = (
            f"🚀 {fancy_font('AUTO TRADE SETUP')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📧 Enter your TradoWix email:\n"
            f"(Fresh login required every session)"
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
    elif data == "auto_signal_fmt2":
        context.user_data['auto_signal_format'] = 2
    elif data == "auto_signal_fmt3":
        context.user_data['auto_signal_format'] = 3
    else:
        return
    
    # ─── Ask for timeframe ──────────────────────────────
    context.user_data['state'] = STATE_AUTO_SIGNAL_TIMEFRAME
    msg = (
        "⏰ Select Timeframe\n\n"
        "Send 1M for 1‑minute candles\n"
        "Send 5M for 5‑minute candles\n\n"
        "Example: 1M  or  5M"
    )
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities)
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
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    print(f"[DEBUG] auto_strategy_cb (Auto Trade) called with data: {data}")
    trader = get_auto_trader(uid)
    s = int(data.split("_")[-1])
    trader.strategy = s
    trader.strategy_name = STRATEGY_NAMES_AUTO.get(s, "Unknown")
    st = get_state(uid)
    st.strategy = s
    head = (
        f"🤖 {fancy_font('STRATEGY')} : {s}. {trader.strategy_name}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    if s == 1:
        context.user_data['state'] = STATE_AUTO_TP
        msg = head + _auto_tp_prompt_body()
        await query.message.edit_text(msg, entities=build_custom_emoji_entities(msg))
        return
    if s == 2:
        # Use dedicated Auto Trade filter choice
        msg = head + "🔍 Enable additional filters?"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes", callback_data="atx_s2_filters_yes"),
             InlineKeyboardButton("❌ No", callback_data="atx_s2_filters_no")]
        ])
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

async def auto_s2_filter_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    print(f"[DEBUG] auto_s2_filter_choice called with data: {data}")

    # Check if it's Auto Trade (atx_) or Auto Signal (auto_)
    is_auto_trade = data.startswith("atx_")

    if is_auto_trade:
        # Auto Trade branch
        if data == "atx_s2_filters_no":
            get_state(uid).strategy2_filters = Strategy2Filters()
            await query.edit_message_text("✅ Filters disabled. Enter min accuracy (50-100):")
            context.user_data['state'] = STATE_AUTO_S2_ACC   # 80
            print("[DEBUG] Auto Trade: filters disabled, state = STATE_AUTO_S2_ACC")
            return
        else:  # Yes
            filters = Strategy2Filters()
            context.user_data['auto_filters'] = filters
            text, markup = _build_auto_s2_filters(filters)   # Auto Trade filter keyboard
            await query.edit_message_text(text, reply_markup=markup)
            context.user_data['state'] = STATE_AUTO_S2_FILTER_TOGGLE   # 79
            print("[DEBUG] Auto Trade: filters enabled, state = STATE_AUTO_S2_FILTER_TOGGLE")
            return
    else:
        # Auto Signal branch (original)
        if data == "auto_s2_filters_no":
            get_state(uid).strategy2_filters = Strategy2Filters()
            await query.edit_message_text("✅ Filters disabled. Enter min accuracy (50-100):")
            context.user_data['state'] = STATE_AUTO_SIGNAL_S2_ACC   # 93
            print("[DEBUG] Auto Signal: filters disabled, state = STATE_AUTO_SIGNAL_S2_ACC")
            return
        else:  # Yes
            filters = Strategy2Filters()
            context.user_data['auto_filters'] = filters
            text, markup = build_auto_s2_filter_message(filters)   # Auto Signal filter keyboard
            await query.edit_message_text(text, reply_markup=markup)
            context.user_data['state'] = STATE_AUTO_SIGNAL_S2_FILTER_TOGGLE   # 92
            print("[DEBUG] Auto Signal: filters enabled, state = STATE_AUTO_SIGNAL_S2_FILTER_TOGGLE")
            return

async def auto_s2_filter_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    print(f"[DEBUG] auto_s2_filter_toggle called with data: {data}")
    filters = context.user_data.get('auto_filters', Strategy2Filters())

    # Toggle maps for both prefixes
    toggle_map = {
        "auto_s2_trend": "use_trend",
        "auto_s2_bb": "use_bollinger",
        "auto_s2_sr": "use_support_resistance",
        "auto_s2_pa": "use_price_action",
        "auto_s2_st": "use_supertrend",
        "auto_s2_fvg": "use_fvg",
        "auto_s2_tr": "use_trend_reverse",
        "atx_s2_trend": "use_trend",
        "atx_s2_bb": "use_bollinger",
        "atx_s2_sr": "use_support_resistance",
        "atx_s2_pa": "use_price_action",
        "atx_s2_st": "use_supertrend",
        "atx_s2_fvg": "use_fvg",
        "atx_s2_tr": "use_trend_reverse",
    }

    if data in toggle_map:
        attr = toggle_map[data]
        setattr(filters, attr, not getattr(filters, attr))
        # Rebuild message based on prefix
        if data.startswith("atx_"):
            text, markup = _build_auto_s2_filters(filters)   # Auto Trade
            print("[DEBUG] Auto Trade filter toggled, rebuilt keyboard")
        else:
            text, markup = build_auto_s2_filter_message(filters)   # Auto Signal
            print("[DEBUG] Auto Signal filter toggled, rebuilt keyboard")
        await query.edit_message_text(text, reply_markup=markup)
        return

    elif data == "auto_s2_done":
        # Auto Signal done
        get_state(uid).strategy2_filters = filters
        await query.edit_message_text("✅ Filters saved. Enter min accuracy (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S2_ACC   # 93
        print("[DEBUG] Auto Signal: filters done, state = STATE_AUTO_SIGNAL_S2_ACC")
        return

    elif data == "atx_s2_done":
        # Auto Trade done
        get_state(uid).strategy2_filters = filters
        await query.edit_message_text("✅ Filters saved. Enter min accuracy (50-100):")
        context.user_data['state'] = STATE_AUTO_S2_ACC   # 80
        print("[DEBUG] Auto Trade: filters done, state = STATE_AUTO_S2_ACC")
        return

async def auto_trade_s2_filter_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    print(f"[DEBUG] auto_trade_s2_filter_choice called with data: {data}")
    if data == "atx_s2_filters_no":
        get_state(uid).strategy2_filters = Strategy2Filters()
        await query.edit_message_text("✅ Filters disabled. Enter min accuracy (50-100):")
        context.user_data['state'] = STATE_AUTO_S2_ACC
        print("[DEBUG] Auto Trade: filters disabled, state = STATE_AUTO_S2_ACC")
        return
    else:  # yes
        filters = Strategy2Filters()
        context.user_data['auto_filters'] = filters
        text, markup = _build_auto_s2_filters(filters)
        await query.edit_message_text(text, reply_markup=markup)
        context.user_data['state'] = STATE_AUTO_S2_FILTER_TOGGLE
        print("[DEBUG] Auto Trade: filters enabled, state = STATE_AUTO_S2_FILTER_TOGGLE")
        return

async def auto_trade_s2_filter_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    print(f"[DEBUG] auto_trade_s2_filter_toggle called with data: {data}")
    filters = context.user_data.get('auto_filters', Strategy2Filters())
    toggle_map = {
        "atx_s2_trend": "use_trend",
        "atx_s2_bb": "use_bollinger",
        "atx_s2_sr": "use_support_resistance",
        "atx_s2_pa": "use_price_action",
        "atx_s2_st": "use_supertrend",
        "atx_s2_fvg": "use_fvg",
        "atx_s2_tr": "use_trend_reverse",
    }
    if data in toggle_map:
        attr = toggle_map[data]
        setattr(filters, attr, not getattr(filters, attr))
        text, markup = _build_auto_s2_filters(filters)
        await query.edit_message_text(text, reply_markup=markup)
        return
    elif data == "atx_s2_done":
        get_state(uid).strategy2_filters = filters
        await query.edit_message_text("✅ Filters saved. Enter min accuracy (50-100):")
        context.user_data['state'] = STATE_AUTO_S2_ACC
        print("[DEBUG] Auto Trade: filters done, state = STATE_AUTO_S2_ACC")
        return


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
    import re
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
    print(f"User {uid} - State: {state}")
    print(
        f"[DEBUG 2] User {uid} - State: {state}, Has photo: {bool(update.message.photo)}")

    # ---- INTERCEPT WRONG STATE FROM AUTO SIGNAL ----
    if state == STATE_AUTO_SIGNAL_S2_ACC and context.user_data.get('auto_trade_mode'):
        print("[DEBUG] global_text_handler: intercepted STATE_AUTO_SIGNAL_S2_ACC in Auto Trade mode, resetting to STATE_AUTO_S2_ACC")
        context.user_data['state'] = STATE_AUTO_S2_ACC
        state = STATE_AUTO_S2_ACC
        await update.message.reply_text("⚠️ State reset. Please enter min accuracy (50-100) for Auto Trade Strategy 2:")
        return
    elif state == STATE_AUTO_SIGNAL_S2_FILTER_TOGGLE and context.user_data.get('auto_trade_mode'):
        print("[DEBUG] global_text_handler: intercepted STATE_AUTO_SIGNAL_S2_FILTER_TOGGLE in Auto Trade mode, resetting to STATE_AUTO_S2_FILTER_TOGGLE")
        context.user_data['state'] = STATE_AUTO_S2_FILTER_TOGGLE
        state = STATE_AUTO_S2_FILTER_TOGGLE
        await update.message.reply_text("⚠️ State reset. Please toggle filters and press Done:")
        return

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

    # ===== LIVE FS STATES (NEW) =====
    elif state == STATE_LIVEFS_CUSTOM_DAYS:
        try:
            days = int(clean_int_input(text))
            if 1 <= days <= 10:
                context.user_data['livefs_days'] = days
                msg = "📊 𝚂𝙴𝙻𝙴𝙲𝚃 𝙿𝙰𝙸𝚁 𝙼𝙾𝙳𝙴"
                buttons = [
                    [colored_button(" All Pairs (20) ", "livefs_pair_all", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
                    [colored_button(" Custom Pairs ", "livefs_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
                ]
                markup = InlineKeyboardMarkup(buttons)
                await update.message.reply_text(msg, entities=build_custom_emoji_entities(msg), reply_markup=markup)
                context.user_data['state'] = STATE_LIVEFS_PAIR_MODE
            else:
                await update.message.reply_text("❌ Enter between 1-10:")
        except:
            await update.message.reply_text("❌ Invalid number. Enter days (1-10):")
        return

    elif state == STATE_LIVEFS_START_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['livefs_start_time'] = text
            context.user_data['state'] = STATE_LIVEFS_END_TIME
            msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:30"
            await update.message.reply_text(msg, entities=build_custom_emoji_entities(msg))
        else:
            await update.message.reply_text("❌ Invalid format. Use HH:MM")
        return

    elif state == STATE_LIVEFS_END_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['livefs_end_time'] = text
            await update.message.reply_text("⏳ 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚒𝚗𝚐 𝙻𝚒𝚟𝚎 𝙵𝚂 𝚜𝚒𝚐𝚗𝚊𝚕𝚜...\n⏰ 𝚃𝚑𝚒𝚜 𝚖𝚊𝚢 𝚝𝚊𝚔𝚎 𝚊 𝚏𝚎𝚠 𝚜𝚎𝚌𝚘𝚗𝚍𝚜.")
            threading.Thread(target=_run_livefs_worker, args=(update.effective_user.id, context), daemon=True).start()
            context.user_data['state'] = None
        else:
            await update.message.reply_text("❌ Invalid format. Use HH:MM")
        return

    elif state == STATE_LIVEFS_CHECKER_SIGNALS:
        signals_text = text
        date_str = context.user_data.get('lfc_date')
        mtg = context.user_data.get('lfc_mtg', 0)
        if not date_str:
            await update.message.reply_text("❌ Date missing. Please start again.")
            return
        context.user_data['state'] = None
        threading.Thread(target=run_livefs_checker, args=(uid, date_str, mtg, signals_text, context), daemon=True).start()
        await update.message.reply_text("⏳ Processing signals...\n🔄 Progress will show below.")

    # ---- PATTERN REPLAY (Strategy 4) states ----
    elif state == STATE_FUT4_DAYS:
        try:
            days = int(clean_int_input(text))
            if 2 <= days <= 10:
                context.user_data['fut4_days'] = days
                context.user_data['state'] = STATE_FUT4_CONFIDENCE
                msg = "🎯 𝚂𝚎𝚕𝚎𝚌𝚝 𝙲𝚘𝚗𝚏𝚒𝚍𝚎𝚗𝚌𝚎 𝚃𝚑𝚛𝚎𝚜𝚑𝚘𝚕𝚍:"
                buttons = [
                    [colored_button(" 65% ", "fut4_conf_65", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
                     colored_button(" 75% ", "fut4_conf_75", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
                    [colored_button(" 85% ", "fut4_conf_85", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
                     colored_button(" 90% ", "fut4_conf_90", KeyboardButtonStyle.DANGER, "6145317070578916456")],
                ]
                markup = InlineKeyboardMarkup(buttons)
                entities = build_custom_emoji_entities(msg)
                await update.message.reply_text(msg, entities=entities, reply_markup=markup)
            else:
                await update.message.reply_text("❌ Enter a number between 2 and 10.")
                return STATE_FUT4_DAYS
        except ValueError:
            await update.message.reply_text("❌ Invalid number. Enter a number between 2 and 10.")
            return STATE_FUT4_DAYS
        return

    elif state == STATE_FUT4_START_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['fut4_start_time'] = text
            context.user_data['state'] = STATE_FUT4_END_TIME
            msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝙸𝙽 𝚈𝙾𝚄𝚁 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:00"
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
        else:
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 𝙷𝙷:𝙼𝙼 (𝚎.𝚐., 09:00)")
        return

    elif state == STATE_FUT4_END_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['fut4_end_time'] = text
            await update.message.reply_text("⏳ Generating Pattern Replay signals... This may take 30-60 seconds.")
            threading.Thread(target=generate_pattern_replay_signals, args=(uid, context), daemon=True).start()
            context.user_data['state'] = None
        else:
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 𝙷𝙷:𝙼𝙼")
        return

    elif state == STATE_FUT5_START_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
            context.user_data['fut5_start_time'] = text
            context.user_data['state'] = STATE_FUT5_END_TIME
            msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:30"
            entities = build_custom_emoji_entities(msg)
            await update.message.reply_text(msg, entities=entities)
        else:
            await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 HH:MM")
        return

    elif state == STATE_FUT5_END_TIME:
        if re.match(r'^\d{2}:\d{2}$', text):
           context.user_data['fut5_end_time'] = text
           # Start generating signals in background
           await update.message.reply_text("⏳ 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚒𝚗𝚐 𝙽𝙸𝙶𝙷𝚃𝚈𝚈 𝚂𝚃 𝚜𝚒𝚐𝚗𝚊𝚕𝚜...\n⏰ 𝚃𝚑𝚒𝚜 𝚖𝚊𝚢 𝚝𝚊𝚔𝚎 20-30 𝚜𝚎𝚌𝚘𝚗𝚍𝚜.")
           threading.Thread(target=generate_nightyy_signals, args=(update.effective_user.id, context), daemon=True).start()
           context.user_data['state'] = None
        else:
           await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎 HH:MM")
        return

    elif state == STATE_BROADCAST_WAIT_FOR_MESSAGE:
       if uid != OWNER_ID:
          await update.message.reply_text("⛔ Access denied.")
          context.user_data['state'] = None
          return

       msg_text = update.message.text
       msg_entities = update.message.entities

       # Get all user IDs from all_users
       user_ids = get_all_telegram_ids()
       if not user_ids:
          await update.message.reply_text("❌ No users found in database.")
          context.user_data['state'] = None
          return

       progress_msg = await update.message.reply_text(f"📡 Broadcasting to {len(user_ids)} users...")

       success = 0
       fail = 0
       for target_id in user_ids:
          try:
             await context.bot.send_message(
                 chat_id=target_id,
                 text=msg_text,
                 entities=msg_entities
             )
             success += 1
          except Exception:
             fail += 1
          await asyncio.sleep(0.05)   # avoid rate limits

       await progress_msg.edit_text(
          f"✅ Broadcast complete!\n"
          f"📤 Sent to: {success} users\n"
          f"❌ Failed: {fail} users"
       )
       context.user_data['state'] = None
       return

    elif state == STATE_AUTO_SIGNAL_TIMEFRAME:
       tf = text.strip().upper()
       if tf in ("1M", "5M"):
          context.user_data['auto_signal_timeframe'] = tf.lower()  # '1m' or '5m'
          context.user_data['state'] = STATE_AUTO_SIGNAL_CHANNEL
          msg = "📢 Enter the channel ID or username:\n(e.g., @my_channel or -100123456789)"
          entities = build_custom_emoji_entities(msg)
          await update.message.reply_text(msg, entities=entities)
       else:
          msg = "❌ Invalid timeframe. Please send 1M or 5M."
          entities = build_custom_emoji_entities(msg)
          await update.message.reply_text(msg, entities=entities)
          return

    elif state == STATE_SWAP_CP:
       if text.strip().lower() == "done":
        # Finalize and send
          lines = context.user_data.get('swap_cp_lines', [])
          if not lines:
              await update.message.reply_text("❌ No signals to swap.")
              context.user_data['state'] = None
              return

          swapped_lines = []
          import re
          for line in lines:
             line = line.strip()
             if not line:
                 continue
             match = re.search(r'\b(CALL|PUT)\b', line, re.IGNORECASE)
             if match:
                original = match.group(0)
                new_dir = "PUT" if original.upper() == "CALL" else "CALL"
                new_line = re.sub(r'\b' + re.escape(original) + r'\b', new_dir, line, flags=re.IGNORECASE)
                swapped_lines.append(new_line)
             else:
                swapped_lines.append(line)

          if not swapped_lines:
             await update.message.reply_text("❌ No valid signals found.")
             context.user_data['state'] = None
             return

             # Build output
          now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
          date_str = now_pk.strftime("%d-%b-%Y")
          header = (
            f"🔄 SWAP C/P COMPLETED 🔄\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 {date_str} (UTC+5)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
          )
          body = "\n".join(swapped_lines)
          footer = (
            f"\n\n✅ All CALL ↔ PUT swapped.\n"
            f"✨ DM @Rohailtrader"
          )

          full_msg = header + body + footer
          entities = build_custom_emoji_entities(full_msg)
          header_len = len(header.encode('utf-16-le')) // 2
          footer_start = len((header + body).encode('utf-16-le')) // 2
          footer_len = len(footer.encode('utf-16-le')) // 2
          entities.append(MessageEntity(type='bold', offset=0, length=header_len))
          entities.append(MessageEntity(type='bold', offset=footer_start, length=footer_len))

          await context.bot.send_message(chat_id=uid, text=full_msg, entities=entities)
          context.user_data['state'] = None
          context.user_data.pop('swap_cp_lines', None)
          return

       else:
        # Accumulate lines
         if 'swap_cp_lines' not in context.user_data:
             context.user_data['swap_cp_lines'] = []
         context.user_data['swap_cp_lines'].append(text)
         await update.message.reply_text(f"✅ Added {len(text.splitlines())} lines. Send more or type `done` to finish.")
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
        print("[DEBUG] global_text_handler: processing AUTO TRADE Strategy 2 accuracy")
        try:
            val = int(clean_int_input(text))
            if 50 <= val <= 100:
                st = get_state(uid)
                if st.strategy2_filters is None:
                    st.strategy2_filters = Strategy2Filters()
                st.strategy2_filters.min_accuracy = val
                print(f"[DEBUG] AUTO TRADE S2 accuracy set to {val}, moving to STATE_AUTO_TP")
                context.user_data['state'] = STATE_AUTO_TP
                await update.message.reply_text(
                    f"✅ Min accuracy set to {val}%.\n\n" + _auto_tp_prompt_body()
                )
            else:
                await update.message.reply_text("❌ Enter between 50-100:")
        except:
            await update.message.reply_text("❌ Invalid number.")
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
       try:
          if channel_input.startswith('@'):
              channel_input = channel_input[1:]
          chat = await context.bot.get_chat(chat_id=channel_input)
          chat_id = chat.id
       except Exception as e:
          await update.message.reply_text(f"❌ Failed to resolve channel.\nError: {e}")
          return

       try:
          test_msg = f"✅ Auto Signal mode activated.\nChannel connected."
          await context.bot.send_message(chat_id=chat_id, text=test_msg)
       except Exception as e:
          await update.message.reply_text(f"❌ Failed to send test message.\nError: {e}")
          return

       # Store the chat ID as integer
       context.user_data['auto_target_id'] = chat_id
       context.user_data['state'] = STATE_AUTO_SIGNAL_MARKET

       msg = "🌐 𝚂𝙴𝙻𝙴𝙲𝚃 𝙼𝙰𝚁𝙺𝙴𝚃 𝚃𝚈𝙿𝙴"
       buttons = [
        [colored_button("  OTC MARKET ", "auto_signal_market_otc", KeyboardButtonStyle.SUCCESS, "6280484433027931563")],
        [colored_button("  FOREX MARKET ", "auto_signal_market_forex", KeyboardButtonStyle.PRIMARY, "5229094735527814890")],
        [colored_button("  BOTH ", "auto_signal_market_both", KeyboardButtonStyle.PRIMARY, "6269322397141177131")],
        [colored_button("  Cancel ", "back_to_main", KeyboardButtonStyle.DANGER, "6145317070578916456")],
       ]
       markup = InlineKeyboardMarkup(buttons)
       entities = build_custom_emoji_entities(msg)
       await update.message.reply_text(msg, entities=entities, reply_markup=markup)
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

    elif state == STATE_WHITEOUT_DAYS:
       try:
          days = int(clean_int_input(text))
          if 1 <= days <= 7:
             context.user_data['white_days'] = days
             context.user_data['state'] = STATE_WHITEOUT_PAIR_MODE
             msg = "📊 𝚂𝙴𝙻𝙴𝙲𝚃 𝙿𝙰𝙸𝚁 𝙼𝙾𝙳𝙴"
             buttons = [
                [colored_button(" All OTC Pairs ", "white_pair_all", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
                [colored_button(" Custom Pairs ", "white_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
             ]
             markup = InlineKeyboardMarkup(buttons)
             entities = build_custom_emoji_entities(msg)
             await update.message.reply_text(msg, entities=entities, reply_markup=markup)
          else:
             await update.message.reply_text("❌ Enter between 1-7:")
       except:
          await update.message.reply_text("❌ Invalid number. Enter days (1-7):")
       return

    elif state == STATE_WHITEOUT_START_TIME:
       if re.match(r'^\d{2}:\d{2}$', text):
           context.user_data['white_start_time'] = text
           context.user_data['state'] = STATE_WHITEOUT_END_TIME
           msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚎𝚗𝚍 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝚄𝚃𝙲+𝟻):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 16:30"
           entities = build_custom_emoji_entities(msg)
           await update.message.reply_text(msg, entities=entities)
       else:
           await update.message.reply_text("❌ Invalid format. Use HH:MM")
       return

    elif state == STATE_WHITEOUT_END_TIME:
       if re.match(r'^\d{2}:\d{2}$', text):
          context.user_data['white_end_time'] = text
          await update.message.reply_text("⏳ Generating Whiteout signals...\n⏰ This may take a few seconds.")
          threading.Thread(target=run_whiteout_signals, args=(uid, context), daemon=True).start()
          context.user_data['state'] = None
       else:
          await update.message.reply_text("❌ Invalid format. Use HH:MM")
       return

    # ---- Other existing states ----

    elif state == STATE_CHECKER_SETTINGS:
       raw_lines = [l.strip() for l in text.split('\n') if l.strip()]
       if not raw_lines:
          await update.message.reply_text("❌ No signals found. Send again or /cancel")
          return
       parsed = []
       skipped = 0
       for line in raw_lines:
          pair, tm, direction = parse_signal_line(line)
          if pair and tm and direction:
              parsed.append((pair, tm, direction))
          else:
              skipped += 1
       if not parsed:
          await update.message.reply_text("❌ No valid signals found. Check format and try again.")
          return
       context.user_data['checker_signals_raw'] = parsed
       context.user_data['checker_skipped'] = skipped
       preview_lines = []
       for i, (p, t, d) in enumerate(parsed[:5]):
          preview_lines.append(f"M1;{p.replace('_', '-')};{t};{d}")
       if len(parsed) > 5:
          preview_lines.append(f"... +{len(parsed)-5} more")
       msg = f"✅ {len(parsed)} signal(s) loaded\n"
       if skipped:
          msg += f"⚠️ {skipped} line(s) skipped (wrong format)\n\n"
       else:
          msg += "\n"
       msg += "\n".join(preview_lines)
       msg += "\n\nSelect Martingale Step:\n\n 🥸 1 Step — check entry + 1 recovery candle\n 😷  2 Steps — check entry + up to 2 recovery candles"
       buttons = [
        [
            colored_button(" MTG 1", "checker_mtg_1", KeyboardButtonStyle.SUCCESS, "6230982053018475627"),
            colored_button(" MTG 2", "checker_mtg_2", KeyboardButtonStyle.PRIMARY, "6231187897916072711")
        ],
        [
            colored_button(" NON MTG", "checker_mtg_0", KeyboardButtonStyle.DANGER, "6230840525256136996")
        ]
       ]
       markup = InlineKeyboardMarkup(buttons)
       bold_phrases = [f"{len(parsed)} signal(s) loaded"]
       if len(parsed) > 5:
          bold_phrases.append(f"... +{len(parsed)-5} more")
       bold_phrases.append("Select Martingale Step:")
       entities = build_custom_emoji_entities(msg) + build_bold_entities(msg, bold_phrases)
       await update.message.reply_text(msg, entities=entities, reply_markup=markup)
       context.user_data['state'] = STATE_CHECKER_MTG_SELECT
       return

    elif state == STATE_MULTI_SETTINGS_OWNER_NAME:
       new_owner = text.strip()
       if not new_owner:
          await update.message.reply_text("❌ Owner name cannot be empty. Please try again.")
          return
       context.user_data['multi_owner_name'] = new_owner
       msg = (
        f"✅ Owner name updated!\n\n"
        f"👤 New Owner Name: {new_owner}\n\n"
        f"This name will now appear on all your live signals."
       )
       buttons = [[colored_button("Back", "multi_back_to_main", KeyboardButtonStyle.PRIMARY, "6145317070578916456")]]
       await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
       context.user_data['state'] = None
       return

    elif state == STATE_STRATEGY7_ACCURACY:
        try:
           val = int(clean_int_input(text))
           if 50 <= val <= 100:
              get_state(uid).strategy7_min_score = val
              trading_mode = context.user_data.get('trading_mode', 'otc')
              bot = SMZXBot(uid)
              if trading_mode == 'forex':
                  bot.pairs = FOREX_PAIRS
              sender.send_message(uid, f"✅ Min confidence set to {val}%.\nStarting analysis...")
              threading.Thread(target=bot.run_single_signal, daemon=True).start()
              context.user_data['strategy_active'] = False
              return ConversationHandler.END
           else:
              await update.message.reply_text("❌ Enter between 50-100:")
              return STATE_STRATEGY7_ACCURACY
        except:
              await update.message.reply_text("❌ Invalid number. Enter a number between 50-100:")
              return STATE_STRATEGY7_ACCURACY

    elif state == STATE_AUTO_SIGNAL_S7_ACC:
        try:
           val = int(clean_int_input(text))
           if 50 <= val <= 100:
              get_state(uid).strategy7_min_score = val
              await update.message.reply_text(f"✅ Min confidence set to {val}%. Starting Auto Signal...")
              start_auto_signal_session(uid, 7, context)
              context.user_data['state'] = None
           else:
              await update.message.reply_text("❌ Enter between 50-100:")
              return STATE_AUTO_SIGNAL_S7_ACC
        except:
              await update.message.reply_text("❌ Invalid number. Enter a number between 50-100:")
              return STATE_AUTO_SIGNAL_S7_ACC

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

def _build_fut4_pair_page(page=0, per_page=15, selected=None, market="otc"):
    if selected is None:
        selected = set()
    pairs = FUT4_OTC_PAIRS if market == "otc" else FUT4_LIVE_PAIRS
    total = len(pairs)
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, total)
    page_pairs = pairs[start_idx:end_idx]
    total_pages = (total + per_page - 1) // per_page

    buttons = []
    row = []
    for pair in page_pairs:
        display = pair.replace("_otc", "-OTC") if market == "otc" else pair
        label = f"✅ {display}" if pair in selected else display
        style = KeyboardButtonStyle.SUCCESS if pair in selected else KeyboardButtonStyle.PRIMARY
        row.append(InlineKeyboardButton(text=label, callback_data=f"fut4_pickpair_{pair}", style=style))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"fut4_pairpage_{page-1}", style=KeyboardButtonStyle.PRIMARY))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"fut4_pairpage_{page+1}", style=KeyboardButtonStyle.PRIMARY))
    if nav_row:
        buttons.append(nav_row)

    if selected:
        buttons.append([colored_button(f" Done ({len(selected)} selected)", "fut4_pair_done", KeyboardButtonStyle.SUCCESS, "6145553439809084250")])

    return buttons, page, total_pages

async def fut4_days_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid = query.from_user.id
    if data == "fut4_days_custom":
        context.user_data['state'] = STATE_FUT4_DAYS
        msg = "🔢 Enter a number between 2 and 10:"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return
    days = int(data.split("_")[-1])
    context.user_data['fut4_days'] = days
    # Ask for confidence
    context.user_data['state'] = STATE_FUT4_CONFIDENCE
    msg = "🎯 𝚂𝚎𝚕𝚎𝚌𝚝 𝙲𝚘𝚗𝚏𝚒𝚍𝚎𝚗𝚌𝚎 𝚃𝚑𝚛𝚎𝚜𝚑𝚘𝚕𝚍:"
    buttons = [
        [colored_button(" 65% ", "fut4_conf_65", KeyboardButtonStyle.PRIMARY, "6145553439809084250"),
         colored_button(" 75% ", "fut4_conf_75", KeyboardButtonStyle.SUCCESS, "6147654280112248427")],
        [colored_button(" 85% ", "fut4_conf_85", KeyboardButtonStyle.PRIMARY, "6145248943807667330"),
         colored_button(" 90% ", "fut4_conf_90", KeyboardButtonStyle.PRIMARY, "6145317070578916456")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def fut4_conf_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    confidence = int(data.split("_")[-1])
    context.user_data['fut4_confidence'] = confidence
    # Ask for Market type
    context.user_data['state'] = STATE_FUT4_MARKET
    msg = "📊 𝚂𝚎𝚕𝚎𝚌𝚝 𝙼𝚊𝚛𝚔𝚎𝚝 𝚃𝚢𝚙𝚎:"
    buttons = [
        [colored_button(" OTC Market ", "fut4_market_otc", KeyboardButtonStyle.SUCCESS, "6064347140228912866"),
         colored_button(" Live Market ", "fut4_market_live", KeyboardButtonStyle.PRIMARY, "6062085844242537125")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def fut4_market_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    market = data.split("_")[-1]  # "otc" or "live"
    context.user_data['fut4_market'] = market
    # Ask for Pair Mode
    context.user_data['state'] = STATE_FUT4_PAIR_MODE
    msg = "🌐 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛 𝙼𝚘𝚍𝚎:"
    buttons = [
        [colored_button(" All Pairs ", "fut4_pair_all", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
        [colored_button(" Custom Pairs ", "fut4_pair_custom", KeyboardButtonStyle.PRIMARY, "6217370240800527004")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=markup)

async def show_fut4_pair_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "fut4_pair_all":
        market = context.user_data.get('fut4_market', 'otc')
        pairs = FUT4_OTC_PAIRS if market == "otc" else FUT4_LIVE_PAIRS
        context.user_data['fut4_selected_pairs'] = set(pairs)
        context.user_data['state'] = STATE_FUT4_START_TIME
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝙸𝙽 𝚈𝙾𝚄𝚁 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return

    elif data == "fut4_pair_custom":
        context.user_data['fut4_selected_pairs'] = set()
        context.user_data['fut4_pair_page'] = 0
        # fall through to display page

    elif data.startswith("fut4_pairpage_"):
        page = int(data.split("_")[-1])
        context.user_data['fut4_pair_page'] = page
        # fall through

    elif data.startswith("fut4_pickpair_"):
        pair = data.replace("fut4_pickpair_", "")
        selected = context.user_data.get('fut4_selected_pairs', set())
        if pair in selected:
            selected.discard(pair)
        else:
            selected.add(pair)
        context.user_data['fut4_selected_pairs'] = selected
        # fall through

    elif data == "fut4_pair_done":
        selected = context.user_data.get('fut4_selected_pairs', set())
        if not selected:
            await query.answer("❌ Select at least one pair!", show_alert=True)
            return
        context.user_data['state'] = STATE_FUT4_START_TIME
        msg = "⏰ 𝙴𝚗𝚝𝚎𝚛 𝚜𝚝𝚊𝚛𝚝 𝚝𝚒𝚖𝚎 (𝙷𝙷:𝙼𝙼, 𝙸𝙽 𝚈𝙾𝚄𝚁 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴):\n📝 𝙴𝚡𝚊𝚖𝚙𝚕𝚎: 09:00"
        entities = build_custom_emoji_entities(msg)
        await query.edit_message_text(msg, entities=entities)
        return

    # --- Display the current pair selection page ---
    page = context.user_data.get('fut4_pair_page', 0)
    market = context.user_data.get('fut4_market', 'otc')
    selected = context.user_data.get('fut4_selected_pairs', set())
    buttons, page, total_pages = _build_fut4_pair_page(page, selected=selected, market=market)
    selected_count = len(selected)
    msg = f"📊 𝚂𝚎𝚕𝚎𝚌𝚝 𝙿𝚊𝚒𝚛𝚜 (𝙿𝚊𝚐𝚎 {page+1}/{total_pages})\n\n💎 𝚃𝚊𝚙 𝚝𝚘 𝚝𝚘𝚐𝚐𝚕𝚎, 𝚝𝚑𝚎𝚗 𝙳𝚘𝚗𝚎\n📌 Selected: {selected_count}"
    entities = build_custom_emoji_entities(msg)
    await query.edit_message_text(msg, entities=entities, reply_markup=InlineKeyboardMarkup(buttons))

def generate_pattern_replay_signals(uid, context):
    """
    Strategy 4: Pattern Replay
    Output: Custom header + signal lines with CALL/PUT + footer
    """
    try:
        import math
        import heapq
        from datetime import datetime, timedelta, timezone
        import time as ttime
        import asyncio
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Speed boost: use numpy for vectorized distance calc if available,
        # otherwise fall back to the pure-Python method automatically.
        try:
            import numpy as np
            _HAS_NUMPY = True
        except ImportError:
            _HAS_NUMPY = False

        # Retrieve user settings
        days = context.user_data.get('fut4_days', 5)
        confidence = context.user_data.get('fut4_confidence', 75)
        market = context.user_data.get('fut4_market', 'otc')
        selected_pairs = list(context.user_data.get('fut4_selected_pairs', []))
        start_time = context.user_data.get('fut4_start_time', '00:00')
        end_time = context.user_data.get('fut4_end_time', '23:59')

        if not selected_pairs:
            sender.send_message(uid, "❌ No pairs selected.")
            return

        total_pairs = len(selected_pairs)
        start_time_total = ttime.time()

        progress_msg = sender.send_message(uid, "⏳ Initializing...")
        if not progress_msg:
            return
        progress_id = progress_msg.id

        def update_progress(stage, current, total, pair="", signals_found=0, elapsed=0):
            pct = int((current / total) * 100) if total > 0 else 0
            bar = progress_bar_text(pct)
            elapsed_str = f"{elapsed // 60}:{elapsed % 60:02d}"
            emoji = "🔍" if stage == "Fetching" else "🧠" if stage == "Analyzing" else "✅"
            text = (
                f"{emoji} {fancy_font('PATTERN REPLAY')}\n"
                f"{fancy_font('━━━━━━━━━━━ • ━━━━━━━━━━━')}\n"
                f"⏱ {elapsed_str}  |  {fancy_font(stage)}\n"
                f"{bar} {pct}%\n"
                f"📊 {pair}  |  {current}/{total}\n"
                f"💎 Signals found: {signals_found}"
            )
            try:
                sender.edit_message(uid, progress_id, text)
            except Exception:
                pass

        # ---- Fetch candles with retry (parallelized across pairs for speed) ----
        all_candles = {}
        pair_index = 0
        fetch_lock = threading.Lock()

        def fetch_one_pair(pair):
            """Same fetch + retry logic as before, just runs concurrently per pair."""
            url = f"https://a39605-e545.a.jrnm.app/{pair}"
            print(f"[DEBUG] Fetching {pair} from {url}")

            success = False
            resp = None
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = requests.get(url, timeout=15)
                    if resp.status_code == 200:
                        success = True
                        break
                    elif resp.status_code in [502, 503, 504]:
                        wait = 2 ** attempt
                        print(f"⚠️ {pair} returned {resp.status_code}, retrying in {wait}s (attempt {attempt+1}/{max_retries})")
                        ttime.sleep(wait)
                        continue
                    else:
                        print(f"❌ Failed to fetch {pair}: HTTP {resp.status_code}")
                        break
                except Exception as e:
                    print(f"⚠️ Network error for {pair}: {e}, retrying (attempt {attempt+1}/{max_retries})")
                    ttime.sleep(2 ** attempt)
                    continue

            if not success:
                print(f"❌ Giving up on {pair} after {max_retries} attempts")
                return pair, None

            try:
                data = resp.json()
                candles = data.get("candles", [])
                if not candles:
                    print(f"⚠️ No candles for {pair}")
                    return pair, None
                parsed = []
                for c in candles:
                    parsed.append({
                        "time_str": c.get("time", ""),
                        "open": float(c.get("o", 0)),
                        "high": float(c.get("h", 0)),
                        "low": float(c.get("l", 0)),
                        "close": float(c.get("c", 0)),
                        "direction": c.get("d", "").lower(),
                        "timestamp": c.get("t", 0)
                    })
                print(f"✅ {pair}: fetched {len(parsed)} candles")
                return pair, parsed
            except Exception as e:
                print(f"❌ Error parsing data for {pair}: {e}")
                return pair, None

        with ThreadPoolExecutor(max_workers=min(10, max(1, total_pairs))) as executor:
            futures = {executor.submit(fetch_one_pair, pair): pair for pair in selected_pairs}
            for future in as_completed(futures):
                pair_index += 1
                pair, parsed = future.result()
                if parsed:
                    with fetch_lock:
                        all_candles[pair] = parsed
                elapsed = int(ttime.time() - start_time_total)
                update_progress("Fetching", pair_index, total_pairs, pair, len(all_candles), elapsed)

        if not all_candles:
            sender.edit_message(uid, progress_id, "❌ No data fetched. Check console.")
            return

        # ---- Analyze ----
        now_utc5 = datetime.now(timezone(timedelta(hours=5)))
        yesterday_str = (now_utc5 - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"📅 Yesterday (UTC+5) = {yesterday_str}")

        def normalize_pattern(candles):
            if len(candles) < 20:
                return None
            ref_open = candles[0]['open']
            if ref_open == 0:
                return None
            pattern = []
            for c in candles:
                pattern.append((c['open'] / ref_open) - 1)
                pattern.append((c['high'] / ref_open) - 1)
                pattern.append((c['low'] / ref_open) - 1)
                pattern.append((c['close'] / ref_open) - 1)
            return pattern

        def euclidean_dist(p1, p2):
            return math.sqrt(sum((a - b) ** 2 for a, b in zip(p1, p2)))

        all_signals = []
        pair_count = 0
        total_pairs_with_data = len(all_candles)

        for pair, candles in all_candles.items():
            pair_count += 1
            elapsed = int(ttime.time() - start_time_total)
            update_progress("Analyzing", pair_count, total_pairs_with_data, pair, len(all_signals), elapsed)

            by_date = {}
            for c in candles:
                time_str = c.get("time_str", "")
                if time_str:
                    date_part = time_str.split()[0]
                    by_date.setdefault(date_part, []).append(c)
            for date in by_date:
                by_date[date].sort(key=lambda x: x.get("timestamp", 0))

            if yesterday_str not in by_date:
                print(f"⚠️ {pair}: no candles for {yesterday_str}")
                continue

            historical_candles = []
            date_keys = sorted(by_date.keys(), reverse=True)
            count = 0
            for d in date_keys:
                if d == yesterday_str:
                    continue
                if count >= days:
                    break
                historical_candles.extend(by_date[d])
                count += 1
            historical_candles.sort(key=lambda x: x.get("timestamp", 0))

            if len(historical_candles) < 20:
                print(f"⚠️ {pair}: not enough historical candles ({len(historical_candles)})")
                continue

            hist_patterns = []
            for i in range(20, len(historical_candles)):
                window = historical_candles[i-20:i]
                pat = normalize_pattern(window)
                if pat is None:
                    continue
                next_candle = historical_candles[i]
                next_dir = next_candle.get("direction", "")
                hist_patterns.append((pat, next_dir))

            if not hist_patterns:
                print(f"⚠️ {pair}: no historical patterns")
                continue

            print(f"📊 {pair}: {len(hist_patterns)} historical patterns")

            # ---- Speed boost: pre-build lookup structures once per pair ----
            # (same data as hist_patterns, just laid out for fast comparison)
            hist_dirs = [d for _, d in hist_patterns]
            if _HAS_NUMPY:
                hist_matrix = np.array([p for p, _ in hist_patterns], dtype=np.float64)
            else:
                hist_pats_only = [p for p, _ in hist_patterns]

            sh, sm = map(int, start_time.split(':'))
            eh, em = map(int, end_time.split(':'))
            start_min = sh * 60 + sm
            end_min = eh * 60 + em

            yest_candles = by_date[yesterday_str]

            # ---- Speed boost: build hhmm -> index lookup once instead of
            # re-scanning yest_candles for every target minute ----
            hhmm_to_idx = {}
            for i, c in enumerate(yest_candles):
                time_parts = c.get("time_str", "").split()
                if len(time_parts) >= 2:
                    hhmm = time_parts[1][:5]
                    if hhmm not in hhmm_to_idx:
                        hhmm_to_idx[hhmm] = i

            for minute in range(start_min, end_min + 1):
                target_h = minute // 60
                target_m = minute % 60
                target_time_str = f"{target_h:02d}:{target_m:02d}"

                idx = hhmm_to_idx.get(target_time_str)
                if idx is None or idx < 19:
                    continue

                pattern_window = yest_candles[idx-19:idx+1]
                current_pat = normalize_pattern(pattern_window)
                if current_pat is None:
                    continue

                # ---- Speed boost: vectorized distance calc (numpy) when
                # available, otherwise fast pure-Python fallback. Same
                # euclidean-distance / top-10-nearest logic either way. ----
                if _HAS_NUMPY:
                    current_vec = np.array(current_pat, dtype=np.float64)
                    dists = np.linalg.norm(hist_matrix - current_vec, axis=1)
                    k = min(10, len(dists))
                    nearest_idx = np.argpartition(dists, k - 1)[:k]
                    top_k = [(dists[i], hist_dirs[i]) for i in nearest_idx]
                else:
                    matches = [(math.dist(current_pat, hist_pat), next_dir)
                               for hist_pat, next_dir in zip(hist_pats_only, hist_dirs)]
                    top_k = heapq.nsmallest(10, matches, key=lambda x: x[0])

                if not top_k:
                    continue

                call_count = sum(1 for _, d in top_k if d == "up")
                put_count = sum(1 for _, d in top_k if d == "down")
                total = len(top_k)

                call_ratio = (call_count / total) * 100
                put_ratio = (put_count / total) * 100

                print(f"🔍 {pair} {target_time_str}: CALL={call_ratio:.1f}% PUT={put_ratio:.1f}% (threshold={confidence}%)")

                if call_ratio >= confidence:
                    direction = "CALL"
                elif put_ratio >= confidence:
                    direction = "PUT"
                else:
                    continue

                all_signals.append({
                    'pair': pair,
                    'time': target_time_str,
                    'dir': direction,
                })

        # ---- Final output ----
        elapsed = int(ttime.time() - start_time_total)
        if all_signals:
            all_signals.sort(key=lambda x: x['time'])

            # --- Header (no conversion, keep CALL/PUT) ---
            now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
            day = now_pk.day
            suffix = "TH" if 4 <= day <= 20 or 24 <= day <= 30 else {1: "ST", 2: "ND", 3: "RD"}.get(day % 10, "TH")
            date_str = f"{day}{suffix} {now_pk.strftime('%B %Y').upper()}"
            header = (
                f"👑PRIV ST SMZ SIGNALS👑\n"
                f"🐱{date_str}\n"
                f"🚨UTC/ GMT: (+5:00)\n\n"
                f"🦁QUOTEX BROKER\n"
                f"🦊1 STEP MARTINGALE \n"
                f"🪱TF : 01 M\n\n"
            )

            # --- Body (signal lines with CALL/PUT) ---
            body_lines = []
            for sig in all_signals:
                # direction is already "CALL" or "PUT"
                body_lines.append(f"{sig['pair']} × {sig['time']} × {sig['dir']}")
            body = "\n".join(body_lines)

            # --- Footer ---
            footer = (
                f"\n👙AVOID BELOW % 77 & OPPOSITE TREND\n"
                f"🐻DM @Rohailtrader"
            )

            full_text = header + body + footer

            # Build custom emoji entities (premium emojis)
            entities = build_custom_emoji_entities(full_text)

            # Compute UTF-16 lengths for bold header and footer only
            header_len = len(header.encode('utf-16-le')) // 2
            footer_start = len((header + body).encode('utf-16-le')) // 2
            footer_len = len(footer.encode('utf-16-le')) // 2

            entities.append(MessageEntity(type='bold', offset=0, length=header_len))
            entities.append(MessageEntity(type='bold', offset=footer_start, length=footer_len))

            # Send the final message via the main loop
            async def send_final():
                await context.bot.send_message(chat_id=uid, text=full_text, entities=entities)

            asyncio.run_coroutine_threadsafe(send_final(), MAIN_LOOP)

            # Update progress message to COMPLETED
            sender.edit_message(uid, progress_id,
                f"✅ {fancy_font('COMPLETED')}\n"
                f"⏱ {elapsed // 60}:{elapsed % 60:02d}\n"
                f"📊 {len(all_signals)} signals found"
            )
        else:
            sender.edit_message(uid, progress_id, "❌ No signals met the confidence threshold.")

    except Exception as e:
        import traceback
        error_msg = f"❌ Failed to generate signals.\n🔍 Error: {str(e)[:200]}"
        try:
            sender.edit_message(uid, progress_id, error_msg)
        except Exception:
            sender.send_message(uid, error_msg)
        print(f"Strategy 4 error: {traceback.format_exc()}")

def generate_nightyy_signals(uid, context):
    import time as ttime
    from datetime import datetime, timedelta, timezone
    import re
    import asyncio

    selected_pairs = list(context.user_data.get('fut5_selected_pairs', []))
    days = context.user_data.get('fut5_days', 2)
    accuracy = context.user_data.get('fut5_accuracy', 80)
    start_time_user = context.user_data.get('fut5_start_time', '00:00')
    end_time_user = context.user_data.get('fut5_end_time', '23:59')

    if not selected_pairs:
        sender.send_message(uid, "❌ 𝙽𝚘 𝚙𝚊𝚒𝚛𝚜 𝚜𝚎𝚕𝚎𝚌𝚝𝚎𝚍.")
        return

    # Convert user times (UTC+5) to API times (UTC+6)
    def convert_to_utc6(hhmm):
        h, m = map(int, hhmm.split(':'))
        total = h*60 + m + 60   # +1 hour
        total %= 1440
        return f"{total//60:02d}:{total%60:02d}"

    def convert_to_utc5(hhmm):
        h, m = map(int, hhmm.split(':'))
        total = h*60 + m - 60
        total %= 1440
        return f"{total//60:02d}:{total%60:02d}"

    start_api = convert_to_utc6(start_time_user)
    end_api = convert_to_utc6(end_time_user)

    all_signals = []
    total_pairs = len(selected_pairs)

    for idx, pair in enumerate(selected_pairs):
        # pair already in "_otc" format (e.g., "EURUSD_otc")
        api_pair = pair
        url = f"https://futuretopquotex-apisignal.poghen-dx.workers.dev/pairs={api_pair}?start_time={start_api}&end_time={end_api}&min_percentage={accuracy}&separate=1&days={days}"
        try:
            resp = requests.get(url, timeout=20)
            if resp.status_code != 200:
                print(f"NIGHTYY ST: {api_pair} failed with {resp.status_code}")
                continue
            data = resp.json()
            if data.get('status') != 'success':
                continue
            signals = data.get('signals', [])
            for sig in signals:
                asset = sig.get('asset', '').upper()  # e.g., "USDINR-OTC"
                time_utc6 = sig.get('time', '')
                direction_raw = sig.get('direction', '').upper()
                # Convert direction to BUY/SELL as per format
                if direction_raw == "CALL":
                    direction = "BUY"
                elif direction_raw == "PUT":
                    direction = "PUT"
                else:
                    direction = direction_raw
                accuracy_str = sig.get('accuracy', '0%')
                try:
                    acc_num = float(accuracy_str.replace('%', ''))
                except:
                    acc_num = 0
                time_utc5 = convert_to_utc5(time_utc6)
                all_signals.append({
                    'pair': asset,
                    'time': time_utc5,
                    'direction': direction,
                    'accuracy': acc_num,
                    'accuracy_display': accuracy_str
                })
        except Exception as e:
            print(f"NIGHTYY ST error for {api_pair}: {e}")
            continue

    if not all_signals:
        sender.send_message(uid, "❌ 𝙽𝚘 𝚜𝚒𝚐𝚗𝚊𝚕𝚜 𝚏𝚘𝚞𝚗𝚍.")
        return

    # Sort by time
    all_signals.sort(key=lambda x: x['time'])

    # --- Build output in EXACT requested format ---
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    date_str = now_pk.strftime("%B-%d-%Y").upper()  # e.g., JULY-23-2026

    header = (
        f"💟 SMZ X NIGHTMARE 💟\n\n"
        f"🗽 {date_str}\n"
        f"🐭 1 STEP MTG IF NEEDED\n"
        f"🗼 TF : 1 MINUTE \n"
        f"🏰 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴: 𝚄𝚃𝙲+𝟻 (𝙿𝙺)\n\n"
        f"🏯JUST AVOID AGAINST TREND AND BELOW 78%\n\n"
    )

    # Body: PAIR;M1:HH:MM;DIRECTION
    body_lines = []
    for sig in all_signals:
        line = f"{sig['pair']};M1:{sig['time']};{sig['direction']}"
        body_lines.append(line)
    body = "\n".join(body_lines)

    footer = f"\n\n🐙𝙳𝙴𝚅𝙴𝙻𝙾𝙿𝙴𝚁: @Rohailtrader"

    full_text = header + body + footer

    # Build entities: premium emojis + bold for specific parts
    entities = build_custom_emoji_entities(full_text)

    # Bold on header lines: "💟 SMZ X NIGHTMARE 💟" and "🏰 𝚃𝙸𝙼𝙴𝚉𝙾𝙽𝙴: 𝚄𝚃𝙲+𝟻 (𝙿𝙺)"
    # We'll apply bold to the entire header + footer (as per your request)
    header_len = len(header.encode('utf-16-le')) // 2
    footer_start = len((header + body).encode('utf-16-le')) // 2
    footer_len = len(footer.encode('utf-16-le')) // 2

    entities.append(MessageEntity(type='bold', offset=0, length=header_len))
    entities.append(MessageEntity(type='bold', offset=footer_start, length=footer_len))

    # Send via main loop
    async def send():
        await context.bot.send_message(chat_id=uid, text=full_text, entities=entities)
    asyncio.run_coroutine_threadsafe(send(), MAIN_LOOP)

    sender.send_message(uid, f"✅ NIGHTYY ST signals sent! ({len(all_signals)} found)")

def get_pairs_by_market(market_type):
    """
    Return pairs based on market selection.
    """
    otc_pairs = DEFAULT_OTC_PAIRS.copy()
    forex_pairs = FOREX_PAIRS.copy()
    
    if market_type == "otc":
        return otc_pairs
    elif market_type == "forex":
        return forex_pairs
    else:  # both
        return otc_pairs + forex_pairs

def generate_pair_list() -> str:
    """
    Generate a formatted list of all supported pairs with display names.
    """
    # OTC Pairs with display names (from FUT4_OTC_PAIRS)
    otc_pairs = [
        ("ATOUSD_otc", "Cosmos (OTC)"),
        ("AUDCAD_otc", "AUD/CAD (OTC)"),
        ("AUDCHF_otc", "AUD/CHF (OTC)"),
        ("AUDJPY_otc", "AUD/JPY (OTC)"),
        ("AUDNZD_otc", "AUD/NZD (OTC)"),
        ("AUDUSD_otc", "AUD/USD (OTC)"),
        ("AVAUSD_otc", "Avalanche (OTC)"),
        ("AXSUSD_otc", "Axie Infinity (OTC)"),
        ("BCHUSD_otc", "Bitcoin Cash (OTC)"),
        ("BNBUSD_otc", "Binance Coin (OTC)"),
        ("BRLUSD_otc", "USD/BRL (OTC)"),
        ("BTCUSD_otc", "Bitcoin (OTC)"),
        ("CADCHF_otc", "CAD/CHF (OTC)"),
        ("CADJPY_otc", "CAD/JPY (OTC)"),
        ("CHFJPY_otc", "CHF/JPY (OTC)"),
        ("DASUSD_otc", "Dash (OTC)"),
        ("DOTUSD_otc", "Polkadot (OTC)"),
        ("ETCUSD_otc", "Ethereum Classic (OTC)"),
        ("ETHUSD_otc", "Ethereum (OTC)"),
        ("EURAUD_otc", "EUR/AUD (OTC)"),
        ("EURCAD_otc", "EUR/CAD (OTC)"),
        ("EURCHF_otc", "EUR/CHF (OTC)"),
        ("EURGBP_otc", "EUR/GBP (OTC)"),
        ("EURJPY_otc", "EUR/JPY (OTC)"),
        ("EURNZD_otc", "EUR/NZD (OTC)"),
        ("EURUSD_otc", "EUR/USD (OTC)"),
        ("GBPAUD_otc", "GBP/AUD (OTC)"),
        ("GBPCAD_otc", "GBP/CAD (OTC)"),
        ("GBPCHF_otc", "GBP/CHF (OTC)"),
        ("GBPJPY_otc", "GBP/JPY (OTC)"),
        ("GBPNZD_otc", "GBP/NZD (OTC)"),
        ("GBPUSD_otc", "GBP/USD (OTC)"),
        ("LINUSD_otc", "Chainlink (OTC)"),
        ("LTCUSD_otc", "Litecoin (OTC)"),
        ("NZDCAD_otc", "NZD/CAD (OTC)"),
        ("NZDCHF_otc", "NZD/CHF (OTC)"),
        ("NZDJPY_otc", "NZD/JPY (OTC)"),
        ("NZDUSD_otc", "NZD/USD (OTC)"),
        ("SOLUSD_otc", "Solana (OTC)"),
        ("TONUSD_otc", "Toncoin (OTC)"),
        ("TRUUSD_otc", "Trump (OTC)"),
        ("UKBrent_otc", "UKBrent (OTC)"),
        ("USCrude_otc", "USCrude (OTC)"),
        ("USDARS_otc", "USD/ARS (OTC)"),
        ("USDBDT_otc", "USD/BDT (OTC)"),
        ("USDCAD_otc", "USD/CAD (OTC)"),
        ("USDCHF_otc", "USD/CHF (OTC)"),
        ("USDCOP_otc", "USD/COP (OTC)"),
        ("USDDZD_otc", "USD/DZD (OTC)"),
        ("USDEGP_otc", "USD/EGP (OTC)"),
        ("USDIDR_otc", "USD/IDR (OTC)"),
        ("USDINR_otc", "USD/INR (OTC)"),
        ("USDJPY_otc", "USD/JPY (OTC)"),
        ("USDMXN_otc", "USD/MXN (OTC)"),
        ("USDNGN_otc", "USD/NGN (OTC)"),
        ("USDPHP_otc", "USD/PHP (OTC)"),
        ("USDPKR_otc", "USD/PKR (OTC)"),
        ("USDZAR_otc", "USD/ZAR (OTC)"),
        ("XAGUSD_otc", "Silver (OTC)"),
        ("XAUUSD_otc", "Gold (OTC)"),
        ("XRPUSD_otc", "Ripple (OTC)"),
        ("ZECUSD_otc", "Zcash (OTC)"),
    ]

    # Live Pairs with display names (from FUT4_LIVE_PAIRS)
    live_pairs = [
        ("AUDCAD", "AUD/CAD"),
        ("AUDCHF", "AUD/CHF"),
        ("AUDJPY", "AUD/JPY"),
        ("AUDUSD", "AUD/USD"),
        ("AXJAUD", "S&P/ASX 200"),
        ("CADJPY", "CAD/JPY"),
        ("CHFJPY", "CHF/JPY"),
        ("CHIA50", "FTSE China A50 Index"),
        ("EURAUD", "EUR/AUD"),
        ("EURCAD", "EUR/CAD"),
        ("EURCHF", "EUR/CHF"),
        ("EURGBP", "EUR/GBP"),
        ("EURJPY", "EUR/JPY"),
        ("EURUSD", "EUR/USD"),
        ("F40EUR", "CAC 40"),
        ("FTSGBP", "FTSE 100"),
        ("GBPAUD", "GBP/AUD"),
        ("GBPCAD", "GBP/CAD"),
        ("GBPCHF", "GBP/CHF"),
        ("GBPJPY", "GBP/JPY"),
        ("GBPUSD", "GBP/USD"),
        ("HSIHKD", "Hong Kong 50"),
        ("IBXEUR", "IBEX 35"),
        ("JPXJPY", "Nikkei 225"),
        ("STXEUR", "EURO STOXX 50"),
        ("USDCAD", "USD/CAD"),
        ("USDCHF", "USD/CHF"),
        ("USDJPY", "USD/JPY"),
        ("XAGUSD", "Silver"),
        ("XAUUSD", "Gold"),
    ]

    # Build output
    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
    date_str = now_pk.strftime("%B %d, %Y").upper()

    header = (
        f"📊 𝙿𝙰𝙸𝚁 𝙻𝙸𝚂𝚃 𝙵𝙾𝚁 𝚂𝙼𝚉𝚇 𝙽𝙸𝙶𝙷𝚃𝙼𝙰𝚁𝙴\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {date_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f" 𝙾𝚃𝙲 𝙿𝙰𝙸𝚁𝚂 ({len(otc_pairs)})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    otc_lines = []
    for idx, (pair, display) in enumerate(otc_pairs, 1):
        otc_lines.append(f"  {idx:2d}) {pair:<12}     {display}")

    live_header = (
        f"\n\n 𝙻𝙸𝚅𝙴 𝙿𝙰𝙸𝚁𝚂 ({len(live_pairs)})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    live_lines = []
    start_idx = len(otc_pairs) + 1
    for idx, (pair, display) in enumerate(live_pairs, start_idx):
        live_lines.append(f"  {idx:2d}) {pair:<12}     {display}")

    footer = (
        f"\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 𝚃𝙾𝚃𝙰𝙻: {len(otc_pairs) + len(live_pairs)} 𝙿𝙰𝙸𝚁𝚂\n"
        f"💎 𝙳𝙴𝚅𝙴𝙻𝙾𝙿𝙴𝚁: @Rohailtrader\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

    full_text = header + "\n".join(otc_lines) + live_header + "\n".join(live_lines) + footer

    return full_text

async def auto_strategy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    data = query.data
    print(f"[DEBUG] auto_strategy_callback (Auto Signal) called with data: {data}")
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
    elif strat == 7:
        await query.message.reply_text("✅ Strategy 7 selected. Enter min confidence % (50-100):")
        context.user_data['state'] = STATE_AUTO_SIGNAL_S7_ACC

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
    import traceback
    print("[DEBUG] start_auto_signal_session called. Stack trace:")
    traceback.print_stack()

    if context.user_data.get('auto_trade_mode'):
        sender.send_message(uid, "❌ Internal error: Auto Trade mode is active, but Auto Signal session started. Please restart with /start and select Auto Trade again.")
        print("[DEBUG] GUARD triggered: auto_trade_mode is True, aborting.")
        return

    st = get_state(uid)
    if st.running:
        sender.send_message(uid, "⚠️ Auto Signal already running. Use /stop first.")
        print("[DEBUG] GUARD triggered: auto_signal already running, aborting duplicate start.")
        return
    st.running = True
    st.stop_requested = False
    st.paused = False
    st.session_stats = {'wins': 0, 'losses': 0}
    st.signal_history = []
    target_id = context.user_data.get('auto_target_id')
    if not target_id:
        sender.send_message(uid, "❌ Target channel not set. Use /start again.")
        print("[DEBUG] start_auto_signal_session: target_id missing, error sent.")
        return
    st.target_chat = target_id

    # ─── Store timeframe ──────────────────────────────
    tf = context.user_data.get('auto_signal_timeframe', '1m')
    st.auto_timeframe = tf

    # ─── REMOVED: user_sender check (no longer needed) ───

    # ─── Control buttons (Telethon style replaced with InlineKeyboardButton) ──
    # Note: We use PTB buttons here, but auto_control_callback uses them fine.
    buttons = [
        [InlineKeyboardButton("📊 Partial Results", callback_data="autopartial")],
        [InlineKeyboardButton("⏸️ Pause", callback_data="autopause"), InlineKeyboardButton("▶️ Continue", callback_data="autocontinue")],
        [InlineKeyboardButton("🔴 Stop", callback_data="autostop")],
    ]
    control_msg = (
        f"💎 𝚂𝙼𝚉𝚇 𝙰𝚄𝚃𝙾 𝚂𝙸𝙶𝙽𝙰𝙻\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Strategy: {strategy_id}\n"
        f"📊 Status: ✅ RUNNING\n"
        f"📢 Channel connected (premium emojis active)\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    sender.send_message(uid, control_msg, buttons=InlineKeyboardMarkup(buttons))

    threading.Thread(target=auto_signal_loop, args=(uid, strategy_id, context), daemon=True).start()

def send_result_message(st, sender, fmt, pair, entry_t, direction, payout, result_type,
                        open_price, close_price, candle_color, wins, losses,
                        tf_display, owner, candles, confidence):
    """Send result using the selected format."""
    if fmt == 3:
        result_text = build_result_format3(pair, entry_t, direction, result_type,
                                           open_price, close_price, candle_color,
                                           wins, losses, owner_name=owner)
        chart_path = safe_draw_pro_chart(candles, pair, entry_t, direction, payout,
                                         result=result_type, confidence=confidence, tf=tf_display)
        if chart_path and os.path.exists(chart_path):
            sender.send_file(st.target_chat, chart_path, result_text, add_bold=True)
            try: os.remove(chart_path)
            except: pass
        else:
            sender.send_message(st.target_chat, result_text, entities=build_bold_entities(result_text, [result_text]))
    elif fmt == 2:
        if result_type == "WIN":
            result_text = build_result_first_win_format2(pair, entry_t)
        elif result_type == "MTG WIN":
            result_text = build_result_second_win_format2(pair, entry_t)
        else:
            result_text = build_result_loss_format2(pair, entry_t)
        sender.send_message(st.target_chat, result_text, entities=build_bold_entities(result_text, [result_text]))
    else:
        # Format 1
        if result_type == "WIN":
            result_text = build_result_message_first_win(pair, entry_t, payout, wins, losses)
        elif result_type == "MTG WIN":
            result_text = build_result_message_second_win(pair, entry_t, payout, wins, losses)
        else:
            result_text = build_result_message_loss(pair, entry_t, payout, wins, losses)
        # Also generate a neon result chart? Original format 1 did not have a result chart, only text.
        # We'll keep it as text only, or we could add a chart if desired.
        sender.send_message(st.target_chat, result_text, entities=build_bold_entities(result_text, [result_text]))

def auto_signal_loop(uid, strategy_id, context):
    import time as t
    from datetime import timedelta

    st = get_state(uid)
    bot = SMZXBot(uid)
    bot.strategy = strategy_id
    
    market = context.user_data.get('auto_signal_market', 'otc')
    pairs = get_pairs_by_market(market)
    st.session_stats = {'wins': 0, 'losses': 0}
    st.signal_history = []
    st.running = True
    st.paused = False
    st.stop_requested = False

    # ─── Get timeframe ──────────────────────────────────
    tf = context.user_data.get('auto_signal_timeframe', '1m')
    tf_display = 'M1' if tf == '1m' else 'M5'
    tf_minutes = 1 if tf == '1m' else 5

    owner = context.user_data.get('multi_owner_name', '@Rohailtrader')
    fmt = context.user_data.get('auto_signal_format', 1)

    pair_cooldown = {}
    last_signal_pair = None

    while st.running:
        try:
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

                if pair in pair_cooldown and now < pair_cooldown[pair]:
                    continue

                candles, price, payout = fetch_candles_mrbeaxt(pair, count=300, timeframe=tf)
                if not candles:
                    continue

                try:
                    payout_num = int(payout) if payout != "!" else 0
                except:
                    payout_num = 0
                if payout_num < 77:
                    continue

                try:
                    bias, entry_dt, score = bot.analyze(candles)
                except Exception as _analyze_err:
                    print(f"[AUTO ANALYZE ERROR] {pair}: {_analyze_err}")
                    continue

                if bias:
                    signal_found = True
                    now_pk = datetime.now(timezone.utc) + timedelta(hours=5)
                    if tf == '5m':
                        minute = now_pk.minute
                        next_5 = ((minute // 5) + 1) * 5
                        if next_5 >= 60:
                            next_5 = 0
                            entry_dt = (now_pk + timedelta(hours=1)).replace(minute=next_5, second=0, microsecond=0)
                        else:
                            entry_dt = now_pk.replace(minute=next_5, second=0, microsecond=0)
                    else:
                        entry_dt = (now_pk + timedelta(minutes=1)).replace(second=0, microsecond=0)
                    entry_t = entry_dt.strftime("%H:%M")
                    direction = "CALL" if bias == "CALL" else "PUT"
                    confidence = int(score) if score else 80

                    if last_signal_pair == pair:
                        pair_cooldown[pair] = t.time() + 120
                    last_signal_pair = pair

                    # ─── Send Signal ──────────────────────────────
                    if fmt == 2:
                        signal_text = build_signal_format2(pair, entry_t, direction, tf=tf_display)
                        # OLD: user_sender.send_bold_message(...)
                        sender.send_message(st.target_chat, signal_text, entities=build_bold_entities(signal_text, [signal_text]))
                    elif fmt == 3:
                        chart_path = safe_draw_pro_chart(candles, pair, entry_t, direction, payout,
                                                         result=None, confidence=confidence, tf=tf_display)
                        signal_text = build_signal_format3(pair, entry_t, direction, payout,
                                                           owner_name=owner, tf=tf_display)
                        if chart_path and os.path.exists(chart_path):
                            # OLD: user_sender.send_file(..., add_bold=True)
                            sender.send_file(st.target_chat, chart_path, signal_text, add_bold=True)
                            try: os.remove(chart_path)
                            except: pass
                        else:
                            sender.send_message(st.target_chat, signal_text, entities=build_bold_entities(signal_text, [signal_text]))
                    else:
                        # Format 1
                        signal_text = build_signal_message(pair, entry_t, direction, payout,
                                                           bot.get_trend_text(candles, direction), tf=tf_display)
                        chart_path = draw_neon_chart(candles, pair, entry_t, direction, payout,
                                                     confidence=confidence,
                                                     wins=st.session_stats.get('wins', 0),
                                                     losses=st.session_stats.get('losses', 0),
                                                     strategy=strategy_id,
                                                     martingale_steps=1,
                                                     signal_history=st.signal_history)
                        if chart_path and os.path.exists(chart_path):
                            sender.send_file(st.target_chat, chart_path, signal_text, add_bold=True)
                            try: os.remove(chart_path)
                            except: pass
                        else:
                            sender.send_message(st.target_chat, signal_text, entities=build_bold_entities(signal_text, [signal_text]))

                    # ─── Result Check ──────────────────────────────
                    bot.sleep_until(entry_dt + timedelta(minutes=tf_minutes))
                    if st.stop_requested:
                        break
                    candles2, first = bot.fetch_candle_at_time_with_retry(
                        pair, entry_dt, limit=50, attempts=15, delay=3, timeframe=tf)
                    if not candles2:
                        sender.send_message(
                            st.target_chat,
                            f"⚠️ {pair} {entry_t}: couldn't fetch candles to verify result (API issue). Skipped.")
                        continue
                    if not first:
                        sender.send_message(
                            st.target_chat,
                            f"⚠️ {pair} {entry_t}: couldn't find the entry candle to verify result. Skipped.")
                        continue
                    win1 = (first['close'] > first['open']) if direction == "CALL" else (first['close'] < first['open'])
                    c1_color = "green" if first['close'] > first['open'] else "red"

                    wins = st.session_stats['wins']
                    losses = st.session_stats['losses']

                    if win1:
                        st.session_stats['wins'] += 1
                        st.signal_history.append({'pair': pair, 'direction': direction, 'time': entry_t, 'result': 'WIN', 'type': 'NON-MTG'})
                        send_result_message(st, sender, fmt, pair, entry_t, direction, payout, "WIN",
                                            first['open'], first['close'], c1_color,
                                            st.session_stats['wins'], st.session_stats['losses'],
                                            tf_display, owner, candles2, confidence)
                        pair_cooldown[pair] = t.time() + 240
                    else:
                        mtg_time = entry_dt + timedelta(minutes=tf_minutes * 2)
                        bot.sleep_until(mtg_time)
                        if st.stop_requested:
                            break
                        candles3, second = bot.fetch_candle_at_time_with_retry(
                            pair, entry_dt + timedelta(minutes=tf_minutes), limit=50, attempts=15, delay=3, timeframe=tf)
                        if not candles3:
                            sender.send_message(
                                st.target_chat,
                                f"⚠️ {pair} {entry_t} MTG: couldn't fetch candles to verify result (API issue). Skipped.")
                            continue
                        if not second:
                            sender.send_message(
                                st.target_chat,
                                f"⚠️ {pair} {entry_t} MTG: couldn't find the MTG candle to verify result. Skipped.")
                            continue
                        win2 = (second['close'] > second['open']) if direction == "CALL" else (second['close'] < second['open'])
                        c2_color = "green" if second['close'] > second['open'] else "red"

                        if win2:
                            st.session_stats['wins'] += 1
                            st.signal_history.append({'pair': pair, 'direction': direction, 'time': entry_t, 'result': 'WIN', 'type': 'MTG'})
                            send_result_message(st, sender, fmt, pair, entry_t, direction, payout, "MTG WIN",
                                                second['open'], second['close'], c2_color,
                                                st.session_stats['wins'], st.session_stats['losses'],
                                                tf_display, owner, candles3, confidence)
                        else:
                            st.session_stats['losses'] += 1
                            st.signal_history.append({'pair': pair, 'direction': direction, 'time': entry_t, 'result': 'LOSS', 'type': 'NON-MTG'})
                            send_result_message(st, sender, fmt, pair, entry_t, direction, payout, "LOSS",
                                                second['open'], second['close'], c2_color,
                                                st.session_stats['wins'], st.session_stats['losses'],
                                                tf_display, owner, candles3, confidence)
                            pair_cooldown[pair] = t.time() + 240

                    for _ in range(15):
                        if st.stop_requested or st.paused:
                            break
                        t.sleep(1)
                    break

            if not signal_found and not st.stop_requested and not st.paused:
                t.sleep(10)

        except Exception as _loop_err:
            print(f"[AUTO SIGNAL LOOP ERROR] {_loop_err}")
            import traceback as _tb
            _tb.print_exc()
            # Notify via PTB fallback (optional, but sender.send_message is already the fallback)
            try:
                sender.send_message(uid, f"⚠️ Auto signal error: {str(_loop_err)[:80]} – retrying...")
            except:
                pass
            t.sleep(3)
            continue

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
    sender.send_message(st.target_chat, final_msg)
    sender.send_message(uid, "🔴 Auto Signal session stopped. Use /start to begin again.")
    st.running = False
    st.target_chat = None
    # NOTE: reset to empty containers instead of None — if a duplicate/late
    # thread for this uid is still mid-iteration, `.append()` on these must
    # not crash with 'NoneType' object has no attribute 'append'.
    st.session_stats = {'wins': 0, 'losses': 0}
    st.signal_history = []

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
        fresh_buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Partial Results", callback_data="autopartial")],
            [InlineKeyboardButton("▶️ Continue", callback_data="autocontinue")],
            [InlineKeyboardButton("🔴 Stop", callback_data="autostop")],
        ])
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
        fresh_buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Partial Results", callback_data="autopartial")],
            [InlineKeyboardButton("⏸️ Pause", callback_data="autopause")],
            [InlineKeyboardButton("🔴 Stop", callback_data="autostop")],
        ])
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
        # Send to target channel
        if st.target_chat:
            sender.send_message(st.target_chat, partial_msg)
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


async def future_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
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
        [colored_button(" Strategy 4 – SMZ PRIV CORE", "fut_strategy_4", KeyboardButtonStyle.SUCCESS, "6145248943807667330")],
        [colored_button(" Strategy 5 – SMZ NIGHTYY", "fut_strategy_5", KeyboardButtonStyle.SUCCESS, "5283055978785285857")],
    ]
    entities = build_custom_emoji_entities(fut_text)
    await update.message.reply_text(fut_text, entities=entities, reply_markup=InlineKeyboardMarkup(fut_buttons))


async def testemoji_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_authorized(uid):
        await update.message.reply_text("⛔ Access denied.")
        return
    test_text = "✅ 𝚃𝚎𝚜𝚝 𝙴𝚖𝚘𝚓𝚒 𝙼𝚎𝚜𝚜𝚊𝚐𝚎"
    entities = build_custom_emoji_entities(test_text)
    await update.message.reply_text(test_text, entities=entities)


def main():
    global MAIN_LOOP, BOT_INSTANCE
    init(autoreset=True)
    print(f"{Fore.CYAN}{'█' * 100}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}✅ Access Granted!{Style.RESET_ALL}")

    if not BOT_TOKEN or not USER_API_ID or not USER_API_HASH:
        raise ValueError("Missing required environment variables: BOT_TOKEN, USER_API_ID, USER_API_HASH")

    API_ID = int(USER_API_ID)
    API_HASH = USER_API_HASH
    BOT_TOKEN_SENDER = BOT_TOKEN

    # ---- CREATE/STORE THE MAIN EVENT LOOP ----
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    MAIN_LOOP = loop   # store for background threads

    app = Application.builder().token(BOT_TOKEN) \
        .connect_timeout(30) \
        .read_timeout(30) \
        .write_timeout(30) \
        .pool_timeout(30) \
        .get_updates_connect_timeout(30) \
        .get_updates_read_timeout(30) \
        .build()
    BOT_INSTANCE = app.bot   # <-- NEW: store bot instance for sender

    # ---- Background janitor thread (unchanged) ----
    def _unauthorized_userdata_janitor():
        while True:
            time.sleep(1800)
            try:
                removed = 0
                for uid in list(app.user_data.keys()):
                    # Never purge a user who has an active Multi Engine or
                    # Auto Signal session running — even a real deauthorization
                    # should not silently kill their in-flight loop; wiping
                    # user_data mid-run was causing "stops on its own" for
                    # random users whenever this janitor ran.
                    ud = app.user_data.get(uid) or {}
                    if ud.get('multi_running', False):
                        continue
                    try:
                        _ust = get_state(uid)
                        if getattr(_ust, 'running', False):
                            continue
                    except Exception:
                        pass

                    if not is_authorized(uid):
                        # app.user_data is a read-only mappingproxy in PTB v20+,
                        # so .pop() on it always raises. Use the dedicated API
                        # to actually drop a user's data; fall back to
                        # clearing the inner dict in place if that's missing
                        # on an older PTB version.
                        try:
                            app.drop_user_data(uid)
                        except AttributeError:
                            try:
                                app.user_data[uid].clear()
                            except Exception:
                                pass
                        removed += 1
                    # Small delay between checks so we never burst-hit
                    # Supabase with 50+ rapid requests and trip its rate
                    # limit (which used to be misread as "unauthorized").
                    time.sleep(0.3)
                if removed:
                    print(f"[JANITOR] Freed user_data for {removed} unauthorized users.")
                    _release_chart_memory()
            except Exception as _jan_err:
                print(f"[JANITOR ERROR] {_jan_err}")
    threading.Thread(target=_unauthorized_userdata_janitor, daemon=True).start()

    # ---- HANDLERS (unchanged) ----
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_|^fut5_|^fut5$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^blk_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^bl_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_live_fs$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^livefs_"))
    app.add_handler(CallbackQueryHandler(show_lfc_loss_candles, pattern="^show_lfc_loss_candles$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^lfc_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^checker_"))

    # Conversation handler for strategy selection
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
            STATE_STRATEGY7_ACCURACY: [MessageHandler(filters.TEXT & ~filters.COMMAND, s7_accuracy_received)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )
    app.add_handler(conv_handler)

    app.add_handler(CommandHandler("futuresignal", future_cmd))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^auto_signal_market_"))
    app.add_handler(CallbackQueryHandler(auto_signal_format_callback, pattern="^auto_signal_fmt"))
    app.add_handler(CallbackQueryHandler(auto_trade_start, pattern="^auto_trade_start$"))
    app.add_handler(MessageHandler(filters.PHOTO, chart_analyzer_photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, global_text_handler))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^fut_strategy_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^alc_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^fut_strategy_3$"))
    app.add_handler(CallbackQueryHandler(fut_pair_callback, pattern="^pair_"))
    app.add_handler(CallbackQueryHandler(smz_tf_callback, pattern="^smz_tf_"))
    app.add_handler(CallbackQueryHandler(smz_pair_callback, pattern="^smz_pair_"))
    app.add_handler(CallbackQueryHandler(auto_strategy_callback, pattern="^autostrat_"))
    app.add_handler(CallbackQueryHandler(smz_pair_callback, pattern="^smz_pickpair_"))
    app.add_handler(CallbackQueryHandler(auto_strategy_callback, pattern="^autostrat_"))
    app.add_handler(CallbackQueryHandler(auto_s2_filter_choice, pattern="^auto_s2_filters_"))
    app.add_handler(CallbackQueryHandler(auto_s2_filter_toggle, pattern="^auto_s2_"))
    app.add_handler(CallbackQueryHandler(auto_control_callback, pattern="^auto"))
    app.add_handler(CallbackQueryHandler(smz_pair_callback, pattern="^smz_pairpage_"))
    app.add_handler(CallbackQueryHandler(aifilter_conf_callback, pattern="^aifilter_conf_"))
    app.add_handler(CallbackQueryHandler(font_style_callback, pattern="^font_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^show_loss_candles$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^back_to_main$"))
    app.add_handler(CommandHandler("continue", continue_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("testemoji", testemoji_cmd))
    app.add_handler(CallbackQueryHandler(backtest_mtg_callback, pattern="^backtest_mtg_"))
    app.add_handler(CallbackQueryHandler(backtest_days_callback, pattern="^backtest_days_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_forex_live$"))

    # Future Signals – Strategy 4
    app.add_handler(CallbackQueryHandler(fut4_days_callback, pattern="^fut4_days_"))
    app.add_handler(CallbackQueryHandler(fut4_conf_callback, pattern="^fut4_conf_"))
    app.add_handler(CallbackQueryHandler(fut4_market_callback, pattern="^fut4_market_"))
    app.add_handler(CallbackQueryHandler(show_fut4_pair_page, pattern="^fut4_pairpage_"))
    app.add_handler(CallbackQueryHandler(show_fut4_pair_page, pattern="^fut4_pair_done$"))
    app.add_handler(CallbackQueryHandler(show_fut4_pair_page, pattern="^fut4_pickpair_"))
    app.add_handler(CallbackQueryHandler(show_fut4_pair_page, pattern="^fut4_pair_all$"))
    app.add_handler(CallbackQueryHandler(show_fut4_pair_page, pattern="^fut4_pair_custom$"))
    app.add_handler(CallbackQueryHandler(show_fut5_pair_page, pattern="^fut5_pairpage_"))
    app.add_handler(CallbackQueryHandler(show_fut5_pair_page, pattern="^fut5_pickpair_"))
    app.add_handler(CallbackQueryHandler(show_fut5_pair_page, pattern="^fut5_pair_done$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^fut5_days_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^fut5_acc_"))
    app.add_handler(CallbackQueryHandler(restricted_main_menu_callback, pattern="^restricted_main_menu$"))
    app.add_handler(CallbackQueryHandler(whiteout_days_callback, pattern="^white_days_"))
    app.add_handler(CallbackQueryHandler(white_pair_callback, pattern="^white_pair_"))
    app.add_handler(CallbackQueryHandler(white_pair_callback, pattern="^white_pairpage_"))
    app.add_handler(CallbackQueryHandler(white_pair_callback, pattern="^white_pickpair_"))
    app.add_handler(CallbackQueryHandler(white_pair_callback, pattern="^white_pair_done$"))
    app.add_handler(CallbackQueryHandler(aifilter_conf_callback, pattern="^aifilter_conf_"))

    # Auto Trade Handlers
    app.add_handler(CallbackQueryHandler(auto_account_cb, pattern=r"^atx_acc_(demo|real)$"))
    app.add_handler(CallbackQueryHandler(auto_strategy_cb, pattern=r"^atx_strat_\d+$"))
    app.add_handler(CallbackQueryHandler(auto_s2_filter_choice, pattern=r"^atx_s2_filters_(yes|no)$"))
    app.add_handler(CallbackQueryHandler(auto_s2_filter_toggle, pattern=r"^atx_s2_(trend|bb|sr|pa|st|fvg|tr|done)$"))
    app.add_handler(CallbackQueryHandler(auto_trade_s2_filter_choice, pattern="^atx_s2_filters_"))
    app.add_handler(CallbackQueryHandler(auto_trade_s2_filter_toggle, pattern="^atx_s2_"))
    app.add_handler(CallbackQueryHandler(auto_mtg_cb, pattern=r"^atx_mtg_(on|off)$"))
    app.add_handler(CallbackQueryHandler(auto_start_cb, pattern="^atx_start$"))
    app.add_handler(CallbackQueryHandler(auto_cancel_cb, pattern="^atx_cancel$"))
    app.add_handler(CallbackQueryHandler(auto_pause_cb, pattern="^atx_(pause|resume)$"))
    app.add_handler(CallbackQueryHandler(auto_status_cb, pattern="^atx_status$"))
    app.add_handler(CallbackQueryHandler(auto_stop_cb, pattern="^atx_stop$"))

    # Checker 2.0 & Backtest 2.0
    app.add_handler(CallbackQueryHandler(chk2_utc_callback, pattern="^chk2_utc_"))
    app.add_handler(CallbackQueryHandler(chk2_date_callback, pattern="^chk2_date_"))
    app.add_handler(CallbackQueryHandler(chk2_mtg_callback, pattern="^chk2_mtg_"))
    app.add_handler(CallbackQueryHandler(bt2_utc_callback, pattern="^bt2_utc_"))
    app.add_handler(CallbackQueryHandler(bt2_days_callback, pattern="^bt2_days_"))
    app.add_handler(CallbackQueryHandler(bt2_mtg_callback, pattern="^bt2_mtg_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^multi_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_multi_engine$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^admin_broadcast$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^finder_"))

    print(f"{Fore.GREEN}[✓] Bot polling...{Style.RESET_ALL}")
    app.run_polling()


if __name__ == "__main__":
    main()
