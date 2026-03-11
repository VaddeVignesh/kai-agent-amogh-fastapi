# app/adapters/redis_store.py
from __future__ import annotations
import os
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional
import redis

# -----------------------------
# Config
# -----------------------------
@dataclass(frozen=True)
class RedisConfig:
   host: str = "localhost"
   port: int = 6379
   db: int = 0
   decode_responses: bool = True
   socket_connect_timeout_sec: float = 0.8
   socket_timeout_sec: float = 0.8
   session_ttl_sec: int = 1800          # 30 minutes
   lock_ttl_ms: int = 8000
   lock_wait_ms: int = 2500
   lock_retry_ms: int = 50
   idem_ttl_sec: int = 300

class RedisStore:
   """
   RedisStore provides:
     - session memory (slots + anchor + last_intent + last_user_input)
     - optional distributed lock (per session)
     - optional idempotency cache (per request)
   For your current graph_router.py you mainly need:
     - load_session(session_id)
     - save_session(session_id, session_patch)
   """
   def __init__(self, cfg: RedisConfig):
       self.cfg = cfg
       connect_timeout = float(os.getenv("REDIS_CONNECT_TIMEOUT_SEC", str(cfg.socket_connect_timeout_sec)))
       socket_timeout = float(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", str(cfg.socket_timeout_sec)))
       self.client = redis.Redis(
           host=cfg.host,
           port=cfg.port,
           db=cfg.db,
           decode_responses=cfg.decode_responses,
           socket_connect_timeout=connect_timeout,
           socket_timeout=socket_timeout,
       )
       # Fallback: if Redis is down, keep sessions in-memory so the chatbot can still run.
       self._fallback_sessions: Dict[str, Dict[str, Any]] = {}
       self._redis_available: Optional[bool] = None

   def _default_session(self) -> Dict[str, Any]:
       return {
           "slots": {},
           "last_intent": None,
           "anchor_type": None,
           "anchor_id": None,
           "last_user_input": None,
           "turn": 0,
           "updated_at": int(time.time()),
       }

   def _redis_disabled(self) -> bool:
       return (os.getenv("REDIS_DISABLED") or "").strip().lower() in ("1", "true", "yes", "y", "on")

   def _safe_get(self, key: str) -> Optional[str]:
       if self._redis_disabled():
           return None
       if self._redis_available is False:
           return None
       try:
           raw = self.client.get(key)
           self._redis_available = True
           return raw
       except redis.exceptions.RedisError:
           self._redis_available = False
           return None

   def _safe_setex(self, key: str, ttl_sec: int, value: str) -> bool:
       if self._redis_disabled():
           return False
       if self._redis_available is False:
           return False
       try:
           self.client.setex(key, ttl_sec, value)
           self._redis_available = True
           return True
       except redis.exceptions.RedisError:
           self._redis_available = False
           return False
   # =========================================================
   # Session Memory
   # =========================================================
   def _session_key(self, session_id: str) -> str:
       return f"session:{session_id}"
   def load_session(self, session_id: str) -> Dict[str, Any]:
       sk = self._session_key(session_id)
       raw = self._safe_get(sk)
       if not raw:
           # In-memory fallback if Redis is down / disabled.
           cached = self._fallback_sessions.get(session_id)
           return dict(cached) if isinstance(cached, dict) else self._default_session()
       try:
           return json.loads(raw)
       except Exception:
           # if corrupted, reset
           return self._default_session()
   def save_session(self, session_id: str, session_patch: Dict[str, Any]) -> None:
       """
       Merge patch into existing session and refresh TTL.
       """
       session = self.load_session(session_id)
       # merge patch (shallow merge)
       for k, v in (session_patch or {}).items():
           session[k] = v
       # increment turn
       session["turn"] = int(session.get("turn", 0)) + 1
       session["updated_at"] = int(time.time())

       payload = json.dumps(session)
       ok = self._safe_setex(self._session_key(session_id), self.cfg.session_ttl_sec, payload)
       if not ok:
           # In-memory fallback if Redis is down / disabled.
           self._fallback_sessions[session_id] = dict(session)
   # =========================================================
   # Optional: Idempotency Cache (per request)
   # =========================================================
   def _idem_key(self, session_id: str, request_id: str) -> str:
       return f"idem:{session_id}:{request_id}"
   def idem_get(self, session_id: str, request_id: str) -> Optional[Dict[str, Any]]:
       raw = self.client.get(self._idem_key(session_id, request_id))
       return json.loads(raw) if raw else None
   def idem_set(self, session_id: str, request_id: str, response: Dict[str, Any]) -> None:
       self.client.setex(
           self._idem_key(session_id, request_id),
           self.cfg.idem_ttl_sec,
           json.dumps(response),
       )
   # =========================================================
   # Optional: Distributed Lock (per session)
   # =========================================================
   def _lock_key(self, session_id: str) -> str:
       return f"lock:{session_id}"
   def acquire_lock(self, session_id: str) -> Optional[str]:
       """
       Acquire a lock for a session. Returns token if acquired else None.
       """
       token = str(uuid.uuid4())
       start_ms = int(time.time() * 1000)
       while int(time.time() * 1000) - start_ms < self.cfg.lock_wait_ms:
           ok = self.client.set(
               self._lock_key(session_id),
               token,
               nx=True,
               px=self.cfg.lock_ttl_ms,
           )
           if ok:
               return token
           time.sleep(self.cfg.lock_retry_ms / 1000)
       return None
   def release_lock(self, session_id: str, token: str) -> None:
       """
       Safe lock release using Lua compare-and-delete.
       """
       lua = """
       if redis.call("get", KEYS[1]) == ARGV[1]
       then return redis.call("del", KEYS[1])
       else return 0 end
       """
       self.client.eval(lua, 1, self._lock_key(session_id), token)