# app/adapters/redis_store.py
from __future__ import annotations
import os
import json
import time
import uuid
from decimal import Decimal
from dataclasses import dataclass
from typing import Any, Dict, Optional
import redis

# Slots that are safe to persist across different intents
STICKY_SLOTS = {"scenario", "limit", "vessel_name", "vessel_imo"}

# Slots that must be cleared when the intent changes
VOLATILE_SLOTS = {
    "voyage_id", "voyage_ids", "voyage_number", "voyage_numbers",
    "cargo_grades", "port_name", "filter_port",
}

SESSION_MAX_AGE_SECONDS = 1800  # 30 minutes
TURN_HISTORY_MAX_ENTRIES = 5


def _json_safe(value: Any) -> Any:
   if isinstance(value, Decimal):
       try:
           return float(value)
       except Exception:
           return str(value)
   if isinstance(value, dict):
       return {str(k): _json_safe(v) for k, v in value.items()}
   if isinstance(value, (list, tuple)):
       return [_json_safe(v) for v in value]
   return value


def _intent_family(intent_key: Any) -> Optional[str]:
   text = str(intent_key or "").strip().lower()
   if not text:
       return None
   if text.startswith("voyage."):
       return "voyage"
   if text.startswith("vessel.") or text == "ranking.vessel_metadata":
       return "vessel"
   if text.startswith("port.") or text.startswith("ops.port_"):
       return "port"
   if text.startswith("ranking.") or text.startswith("analysis.") or text.startswith("aggregation.") or text.startswith("comparison."):
       return "fleet"
   return None


def _same_turn(session: Dict[str, Any], session_patch: Dict[str, Any]) -> bool:
   marker = (session_patch or {}).get("_turn_marker")
   if marker and marker == session.get("_last_turn_marker"):
       return True
   return False


def _compact_turn_history_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
   if not isinstance(entry, dict):
       return {}
   out: Dict[str, Any] = {}
   turn = entry.get("turn")
   if isinstance(turn, int):
       out["turn"] = turn
   query = entry.get("query")
   if isinstance(query, str) and query.strip():
       out["query"] = query.strip()[:300]
   raw_user_input = entry.get("raw_user_input")
   if isinstance(raw_user_input, str) and raw_user_input.strip():
       out["raw_user_input"] = raw_user_input.strip()[:160]
   intent_key = entry.get("intent_key")
   if isinstance(intent_key, str) and intent_key.strip():
       out["intent_key"] = intent_key.strip()
   plan_type = entry.get("plan_type")
   if isinstance(plan_type, str) and plan_type.strip():
       out["plan_type"] = plan_type.strip()
   slots = entry.get("slots")
   if isinstance(slots, dict):
       compact_slots = {
           str(k): _json_safe(v)
           for k, v in slots.items()
           if v not in (None, "", [], {})
       }
       if compact_slots:
           out["slots"] = compact_slots
   answer_headline = entry.get("answer_headline")
   if isinstance(answer_headline, str) and answer_headline.strip():
       out["answer_headline"] = answer_headline.strip()[:240]
   return out

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
     - session memory (slots + anchor + last_intent_key + last_user_input)
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
          "last_intent_key": None,
           "last_intent": None,
           "anchor_type": None,
           "anchor_id": None,
           "last_user_input": None,
           "turn_history": [],
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
           session = dict(cached) if isinstance(cached, dict) else self._default_session()
           if not isinstance(session.get("turn_history"), list):
               session["turn_history"] = []
           last_updated = session.get("last_updated_ts")
           if last_updated:
               if time.time() - float(last_updated) > SESSION_MAX_AGE_SECONDS:
                   return {}
           return session
       try:
           session = json.loads(raw)
           if not isinstance(session.get("turn_history"), list):
               session["turn_history"] = []
           if "last_intent" not in session and session.get("last_intent_key"):
               session["last_intent"] = session.get("last_intent_key")
           last_updated = session.get("last_updated_ts")
           if last_updated:
               if time.time() - float(last_updated) > SESSION_MAX_AGE_SECONDS:
                   return {}
           return session
       except Exception:
           # if corrupted, reset
           return self._default_session()
   def save_session(self, session_id: str, session_patch: Dict[str, Any]) -> None:
       """
       Merge patch into existing session and refresh TTL.
       """
       session = self.load_session(session_id)
       patch = dict(session_patch or {})
       slots = session.get("slots")
       if not isinstance(slots, dict):
           slots = {}
           session["slots"] = slots

       intent_key = (
           patch.get("intent_key")
           or patch.get("last_intent_key")
           or patch.get("last_intent")
       )
       prev_intent_key = session.get("last_intent") or session.get("last_intent_key")
       prev_family = _intent_family(prev_intent_key)
       next_family = _intent_family(intent_key)
       # If the entity family changed, clear volatile slots to prevent context poisoning.
       if prev_family and next_family and prev_family != next_family:
           for k in VOLATILE_SLOTS:
               slots.pop(k, None)
           session.pop("last_result_set", None)
           session.pop("voyage_ids", None)
           session.pop("last_focus_slots", None)

       record_turn = patch.pop("_record_turn", None)
       turn_marker = patch.pop("_turn_marker", None)

       # merge patch (shallow merge)
       for k, v in patch.items():
           session[k] = v
       if intent_key:
           session["last_intent_key"] = intent_key
           session["last_intent"] = intent_key

       if record_turn and not _same_turn(session, {"_turn_marker": turn_marker}):
           next_turn = int(session.get("turn", 0)) + 1
           session["turn"] = next_turn
           session["_last_turn_marker"] = turn_marker or f"turn-{next_turn}"
           history = session.get("turn_history")
           if not isinstance(history, list):
               history = []
           entry = _compact_turn_history_entry({**record_turn, "turn": next_turn})
           if entry:
               history.append(entry)
               session["turn_history"] = history[-TURN_HISTORY_MAX_ENTRIES:]

       session["updated_at"] = int(time.time())
       session["last_updated_ts"] = time.time()

       payload = json.dumps(_json_safe(session))
       ok = self._safe_setex(self._session_key(session_id), self.cfg.session_ttl_sec, payload)
       if not ok:
           # In-memory fallback if Redis is down / disabled.
           self._fallback_sessions[session_id] = dict(session)

   def clear_session(self, session_id: str, *, include_idem: bool = True, include_lock: bool = True) -> Dict[str, Any]:
       """
       Clear backend cache for a single session id.
       Removes session memory key (+ optional idem/lock keys) without touching other sessions.
       """
       sid = str(session_id or "").strip()
       if not sid:
           return {"ok": False, "reason": "missing_session_id", "deleted": 0, "deleted_keys": []}

       deleted_keys = []

       # Clear in-memory fallback copy if present.
       if sid in self._fallback_sessions:
           self._fallback_sessions.pop(sid, None)
           deleted_keys.append(self._session_key(sid) + " (fallback)")

       if self._redis_disabled() or self._redis_available is False:
           return {"ok": True, "deleted": len(deleted_keys), "deleted_keys": deleted_keys}

       keys = [self._session_key(sid)]
       if include_lock:
           keys.append(self._lock_key(sid))

       try:
           if include_idem:
               for k in self.client.scan_iter(match=f"idem:{sid}:*", count=200):
                   keys.append(k)

           # de-duplicate preserving order
           seen = set()
           uniq = []
           for k in keys:
               if k not in seen:
                   seen.add(k)
                   uniq.append(k)

           if uniq:
               self.client.delete(*uniq)
               deleted_keys.extend(uniq)
               self._redis_available = True

           return {"ok": True, "deleted": len(deleted_keys), "deleted_keys": deleted_keys}
       except redis.exceptions.RedisError as e:
           self._redis_available = False
           return {"ok": False, "reason": f"redis_error: {e}", "deleted": len(deleted_keys), "deleted_keys": deleted_keys}
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