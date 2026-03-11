from __future__ import annotations
 
import os
import re
import uuid
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
 
import streamlit as st
from dotenv import load_dotenv
 
import requests
 
API_URL = os.getenv("KAI_API_URL", "http://127.0.0.1:8000/query")
 
def call_api(query: str, session_id: str) -> Dict[str, Any]:
    payload = {
        "query": query,
        "session_id": session_id
    }
 
    r = requests.post(API_URL, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()
 
APP_TITLE    = "Digital Sales Agent"
APP_SUBTITLE = "Maritime finance + operations analytics chatbot"
 
# Ensure repo root is importable (so `import app.*` works even when this file lives under app/UI/UX/).
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
 
# Always load environment from repo root.
load_dotenv(dotenv_path=_PROJECT_ROOT / ".env")
 
 
def _inject_global_css(*, theme: str) -> None:
    is_light = "Light" in (theme or "")
 
    if is_light:
        root = """
:root {
  --kai-app-glow:          rgba(34,211,238,0.18);
  --kai-app-glow2:         rgba(99,102,241,0.12);
  --kai-app-bg-top:        #f7faff;
  --kai-app-bg-bottom:     #e9f4ff;
  --kai-float-bg:          #e9f4ff;
  --kai-chat-pill-bg:      #ffffff;
  --kai-chat-pill-border:  rgba(15,23,42,0.13);
  --kai-sidebar-bg:        #eef6ff;
  --kai-border:            rgba(15,23,42,0.10);
  --kai-text:              #0f172a;
  --kai-text-muted:        rgba(15,23,42,0.50);
  --kai-text-caption:      rgba(15,23,42,0.60);
  --kai-shadow:            0 8px 32px rgba(15,23,42,0.09);
  --kai-primary-a:         #22d3ee;
  --kai-primary-b:         #6366f1;
  --kai-primary-text:      #f8fafc;
  --kai-btn-bg:            rgba(15,23,42,0.06);
  --kai-btn-bg-hover:      rgba(15,23,42,0.11);
  --kai-btn-border:        rgba(15,23,42,0.18);
  --kai-btn-border-hover:  rgba(15,23,42,0.28);
  --kai-card-bg:           #ffffff;
  --kai-card-border:       rgba(15,23,42,0.10);
  --kai-chip-bg:           #ffffff;
  --kai-chip-border:       rgba(15,23,42,0.12);
  --kai-ok:                #0ea5e9;
  --kai-warn:              #b45309;
  --kai-bad:               #b91c1c;
  --kai-table-border:      rgba(15,23,42,0.22);
  --kai-table-header-bg:   rgba(15,23,42,0.06);
  --kai-table-stripe-bg:   rgba(15,23,42,0.025);
  --kai-send-icon:         #0f172a;
}
 
/* Light theme: Deploy strip = same white as the KAI-Agent card */
header[data-testid="stHeader"],
div[data-testid="stHeader"] {
  background:              #ffffff !important;
  border-bottom:           1px solid rgba(15,23,42,0.08) !important;
  box-shadow:              0 1px 4px rgba(15,23,42,0.06) !important;
  backdrop-filter:         none !important;
  -webkit-backdrop-filter: none !important;
}
"""
    else:
        root = """
:root {
  --kai-app-glow:          rgba(147,164,184,0.09);
  --kai-app-glow2:         rgba(255,255,255,0.04);
  --kai-app-bg-top:        #0e0f11;
  --kai-app-bg-bottom:     #0a0a0b;
  --kai-float-bg:          #0a0a0b;
  --kai-chat-pill-bg:      #1c1d20;
  --kai-chat-pill-border:  rgba(255,255,255,0.09);
  --kai-sidebar-bg:        #111214;
  --kai-border:            rgba(255,255,255,0.07);
  --kai-text:              #e8e8e8;
  --kai-text-muted:        #888888;
  --kai-text-caption:      #aaaaaa;
  --kai-shadow:            0 8px 32px rgba(0,0,0,0.35);
  --kai-primary-a:         #93a4b8;
  --kai-primary-b:         #bcc8d8;
  --kai-primary-text:      #0a0a0b;
  --kai-btn-bg:            rgba(255,255,255,0.07);
  --kai-btn-bg-hover:      rgba(255,255,255,0.12);
  --kai-btn-border:        rgba(255,255,255,0.15);
  --kai-btn-border-hover:  rgba(255,255,255,0.25);
  --kai-card-bg:           #1c1d20;
  --kai-card-border:       rgba(255,255,255,0.09);
  --kai-chip-bg:           #1c1d20;
  --kai-chip-border:       rgba(255,255,255,0.10);
  --kai-ok:                #22c55e;
  --kai-warn:              #f59e0b;
  --kai-bad:               #ef4444;
  --kai-table-border:      rgba(255,255,255,0.14);
  --kai-table-header-bg:   rgba(255,255,255,0.07);
  --kai-table-stripe-bg:   rgba(255,255,255,0.025);
  --kai-send-icon:         #b0b8c8;
}
 
/* Dark theme: frosted glass header */
header[data-testid="stHeader"],
div[data-testid="stHeader"] {
  background:              rgba(14,15,17,0.88) !important;
  backdrop-filter:         blur(14px) !important;
  -webkit-backdrop-filter: blur(14px) !important;
  border-bottom:           1px solid rgba(255,255,255,0.06) !important;
  box-shadow:              none !important;
}
"""
 
    shared = """
/* ── App background ─────────────────────────── */
.stApp {
  color: var(--kai-text) !important;
  background:
    radial-gradient(1000px 500px at 20% 0%, var(--kai-app-glow), transparent 55%),
    radial-gradient(800px  400px at 80% 0%, var(--kai-app-glow2), transparent 55%),
    linear-gradient(180deg, var(--kai-app-bg-top) 0%, var(--kai-app-bg-bottom) 100%);
}
html, body,
div[data-testid="stAppViewContainer"],
div[data-testid="stAppViewContainer"] > .main {
  background:
    radial-gradient(1000px 500px at 20% 0%, var(--kai-app-glow), transparent 55%),
    radial-gradient(800px  400px at 80% 0%, var(--kai-app-glow2), transparent 55%),
    linear-gradient(180deg, var(--kai-app-bg-top) 0%, var(--kai-app-bg-bottom) 100%) !important;
}
 
/* ── Layout ──────────────────────────────────── */
main .block-container { max-width:1100px; padding-top:.6rem; padding-bottom:6rem; }
 
/* ── Typography ──────────────────────────────── */
body, .stApp, .stMarkdown, .stMarkdown p, .stMarkdown li,
.stText, .stAlert, label, p, span, div { color:var(--kai-text) !important; }
.stCaption, small,
div[data-testid="stCaptionContainer"] p,
div[data-testid="stCaptionContainer"] span { color:var(--kai-text-caption) !important; }
hr { border-color:var(--kai-border) !important; opacity:.6; }
 
/* ── Decoration ──────────────────────────────── */
div[data-testid="stDecoration"] { display:none !important; }
div[data-testid="stToolbar"]    { background:transparent !important; }
 
/* ── Sidebar ─────────────────────────────────── */
section[data-testid="stSidebar"] {
  background:var(--kai-sidebar-bg) !important;
  border-right:1px solid var(--kai-border) !important;
}
section[data-testid="stSidebar"] .block-container { padding-top:1rem; }
section[data-testid="stSidebar"] *,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] div { color:var(--kai-text) !important; }
section[data-testid="stSidebar"] .stCaption p,
section[data-testid="stSidebar"] small { color:var(--kai-text-caption) !important; }
 
/* ── Theme toggle button ─────────────────────── */
.kai-theme-btn .stButton > button {
  width:        100% !important;
  border-radius:12px !important;
  padding:      .55rem .85rem !important;
  border:       1px solid var(--kai-btn-border) !important;
  background:   var(--kai-btn-bg) !important;
  color:        var(--kai-text) !important;
  font-size:    .95rem !important;
  font-weight:  600 !important;
  text-align:   left !important;
  transition:   background .2s, border-color .2s !important;
}
.kai-theme-btn .stButton > button:hover {
  background:   var(--kai-btn-bg-hover) !important;
  border-color: var(--kai-btn-border-hover) !important;
}
 
/* ══════════════════════════════════════════════════════
   EXPANDERS
   ══════════════════════════════════════════════════════ */
div[data-testid="stExpander"] {
  background:var(--kai-card-bg) !important; background-color:var(--kai-card-bg) !important;
  border:1px solid var(--kai-card-border) !important;
  border-radius:14px !important; overflow:hidden !important;
  box-shadow:none !important; padding:0 !important; margin:0 !important;
}
div[data-testid="stExpander"] > *,
div[data-testid="stExpander"] > * > * {
  background:var(--kai-card-bg) !important; background-color:var(--kai-card-bg) !important;
  border:none !important; border-radius:0 !important;
  box-shadow:none !important; margin:0 !important; padding:0 !important;
}
div[data-testid="stExpander"] details {
  background:var(--kai-card-bg) !important; background-color:var(--kai-card-bg) !important;
  border:none !important; margin:0 !important; padding:0 !important;
}
div[data-testid="stExpander"] details > summary {
  background:var(--kai-card-bg) !important; background-color:var(--kai-card-bg) !important;
  border:none !important; border-radius:0 !important;
  padding:.65rem 1rem !important; color:var(--kai-text) !important;
  list-style:none !important; margin:0 !important;
}
div[data-testid="stExpander"] details > summary:hover { filter:brightness(1.07) !important; }
div[data-testid="stExpander"] details > summary *,
div[data-testid="stExpander"] details > summary p,
div[data-testid="stExpander"] details > summary span,
div[data-testid="stExpander"] details > summary div,
div[data-testid="stExpander"] details > summary svg {
  background:var(--kai-card-bg) !important; background-color:var(--kai-card-bg) !important;
  color:var(--kai-text) !important; fill:var(--kai-text) !important; border:none !important;
}
div[data-testid="stExpanderDetails"],
div[data-testid="stExpander"] div[data-testid="stExpanderDetails"] {
  background:var(--kai-card-bg) !important; background-color:var(--kai-card-bg) !important;
  border-top:1px solid var(--kai-card-border) !important;
  border-bottom:none !important; border-left:none !important; border-right:none !important;
  border-radius:0 !important; padding:.75rem 1rem !important; margin:0 !important;
}
div[data-testid="stExpanderDetails"] p,
div[data-testid="stExpanderDetails"] span,
div[data-testid="stExpanderDetails"] label,
div[data-testid="stExpanderDetails"] div { color:var(--kai-text) !important; }
div[data-testid="stExpanderDetails"] .stCaption p,
div[data-testid="stExpanderDetails"] small { color:var(--kai-text-caption) !important; }
div[data-testid="stExpanderDetails"] code  { color:var(--kai-text) !important; }
 
/* ══════════════════════════════════════════════
   MARKDOWN TABLES
   ══════════════════════════════════════════════ */
.stMarkdown table {
  border-collapse: collapse !important;
  width:           100% !important;
  margin:          .6rem 0 !important;
  border:          1px solid var(--kai-table-border) !important;
}
.stMarkdown table th,
.stMarkdown table td {
  border:     1px solid var(--kai-table-border) !important;
  padding:    9px 14px !important;
  text-align: left !important;
  color:      var(--kai-text) !important;
}
.stMarkdown table thead th {
  background:    var(--kai-table-header-bg) !important;
  font-weight:   700 !important;
  border-bottom: 2px solid var(--kai-table-border) !important;
}
.stMarkdown table tbody tr:nth-child(even) td {
  background: var(--kai-table-stripe-bg) !important;
}
.stMarkdown table tbody tr:hover td {
  background: var(--kai-table-header-bg) !important;
}
 
/* ══════════════════════════════════════════════
   CHAT INPUT
   ══════════════════════════════════════════════ */
.stChatFloatingInputContainer,
.stChatFloatingInputContainer > div,
div[data-testid="stChatFloatingInputContainer"],
div[data-testid="stChatFloatingInputContainer"] > div,
div[data-testid="stBottom"],
div[data-testid="stBottom"] > div,
div[data-testid="stBottomBlockContainer"],
div[data-testid="stBottomBlockContainer"] > div,
footer {
  background:var(--kai-float-bg) !important; background-color:var(--kai-float-bg) !important;
  background-image:none !important; border:none !important; box-shadow:none !important;
}
.stChatFloatingInputContainer { padding:1rem 0 !important; }
.stChatInput {
  background:var(--kai-chat-pill-bg) !important;
  border:1px solid var(--kai-chat-pill-border) !important;
  border-radius:25px !important; box-shadow:0 4px 14px rgba(0,0,0,0.2) !important;
  padding:.5rem 1rem !important; max-width:700px !important; margin:0 auto !important;
}
.stChatInput:hover,.stChatInput:focus,.stChatInput:focus-within,.stChatInput:active {
  border:1px solid var(--kai-chat-pill-border) !important; outline:none !important;
}
.stChatInput > div,.stChatInput > div > div {
  background:var(--kai-chat-pill-bg) !important; background-color:var(--kai-chat-pill-bg) !important;
  border:none !important;
}
.stChatInput input,.stChatInput textarea {
  color:var(--kai-text) !important; background:var(--kai-chat-pill-bg) !important;
  background-color:var(--kai-chat-pill-bg) !important; border:none !important;
  font-size:.9rem !important; padding:.5rem 0 !important;
}
.stChatInput input::placeholder,.stChatInput textarea::placeholder {
  color:var(--kai-text-muted) !important;
}
 
/* ── Send arrow: ONLY tint via currentColor, touch nothing else ── */
/* Streamlit's SVG uses fill="currentColor" — just set color on the button */
div[data-testid="stChatInputSubmitButton"] button {
  color: var(--kai-send-icon) !important;
}
 
/* ── Chat messages ───────────────────────────── */
div[data-testid="stChatMessage"] { margin:.3rem 0; }
div[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]),
.stChatMessage[data-testid="user-message"] {
  background:var(--kai-card-bg) !important; border:1px solid var(--kai-card-border) !important;
  border-radius:18px !important; padding:.75rem 1rem !important; box-shadow:var(--kai-shadow) !important;
}
 
/* ── Buttons ─────────────────────────────────── */
.stButton > button {
  border-radius:14px !important; padding:.55rem .85rem !important;
  border:1px solid var(--kai-btn-border) !important; background:var(--kai-btn-bg) !important;
  color:var(--kai-text) !important; transition:background .2s,border-color .2s !important;
}
.stButton > button:hover {
  border-color:var(--kai-btn-border-hover) !important; background:var(--kai-btn-bg-hover) !important;
}
.stButton > button:focus { outline:none !important; }
.stButton > button[kind="primary"] {
  background:linear-gradient(90deg,var(--kai-primary-a) 0%,var(--kai-primary-b) 100%) !important;
  color:var(--kai-primary-text) !important; border:none !important;
  font-weight:700 !important; border-radius:999px !important;
}
 
/* ── Widget inputs ───────────────────────────── */
div[data-testid="stTextInput"] input,
div[data-testid="stTextArea"] textarea {
  background:var(--kai-card-bg) !important; border:1px solid var(--kai-card-border) !important;
  color:var(--kai-text) !important; border-radius:14px !important;
}
div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
div[data-testid="stSelectbox"] div[data-baseweb="select"] > div > div {
  background:var(--kai-card-bg) !important; border:1px solid var(--kai-card-border) !important;
  color:var(--kai-text) !important; border-radius:14px !important;
}
 
/* ── Scrollbar ───────────────────────────────── */
::-webkit-scrollbar { width:6px; }
::-webkit-scrollbar-track { background:transparent; }
::-webkit-scrollbar-thumb { background:var(--kai-border); border-radius:10px; }
 
/* ── Utilities ───────────────────────────────── */
.kai-card {
  border:1px solid var(--kai-card-border); background:var(--kai-card-bg);
  border-radius:18px; padding:16px; box-sizing:border-box;
  color:var(--kai-text); box-shadow:var(--kai-shadow);
}
.kai-muted  { color:var(--kai-text-muted) !important; }
.kai-hero   { margin-bottom:1rem; }
.kai-chips  { display:flex; gap:.4rem; flex-wrap:wrap; margin:.35rem 0 .75rem 0; }
.kai-chip   {
  display:inline-flex; align-items:center; gap:.35rem;
  padding:.18rem .55rem; border-radius:999px; font-size:.82rem;
  border:1px solid var(--kai-chip-border); background:var(--kai-chip-bg);
}
.kai-dot      { width:8px; height:8px; border-radius:99px; display:inline-block; }
.kai-dot-ok   { background:var(--kai-ok); }
.kai-dot-warn { background:var(--kai-warn); }
.kai-dot-bad  { background:var(--kai-bad); }
"""
    st.markdown(f"<style>{root}{shared}</style>", unsafe_allow_html=True)
 
 
def _redact_dsn(dsn: str) -> str:
    return re.sub(r"://([^:/?#]+):([^@/?#]+)@", r"://\1:***@", dsn or "")
 
def _env_fingerprint() -> str:
    return "|".join([
        f"GROQ={'set' if (os.getenv('GROQ_API_KEY') or '').strip() else 'unset'}",
        f"PG={_redact_dsn(os.getenv('POSTGRES_DSN',''))}",
        f"MONGO={'set' if os.getenv('MONGO_URI') else 'unset'}",
        f"REDIS={os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}",
    ])
 
def _router_code_fingerprint() -> str:
    """
    Cache-buster for st.cache_resource().
    Ensures backend router rebuilds when core orchestration code changes.
    """
    try:
        p = _PROJECT_ROOT / "app" / "orchestration" / "graph_router.py"
        return f"graph_router_mtime={os.path.getmtime(str(p))}"
    except Exception:
        return "graph_router_mtime=na"
 
@dataclass(frozen=True)
class UiSettings:
    groq_model: str
    groq_temperature: float
 
def _apply_runtime_env(s: UiSettings) -> None:
    os.environ["GROQ_MODEL"]       = s.groq_model
    os.environ["GROQ_TEMPERATURE"] = str(s.groq_temperature)
 
@st.cache_resource(show_spinner=False)
def _build_router_cached(*, groq_model: str, groq_temperature: float, env_fingerprint: str, code_fingerprint: str) -> Any:
    from app.adapters.mongo_adapter    import MongoAdapter
    from app.adapters.postgres_adapter import PostgresAdapter, PostgresConfig
    from app.adapters.redis_store      import RedisConfig, RedisStore
    from app.agents.finance_agent      import FinanceAgent
    from app.agents.mongo_agent        import MongoAgent
    from app.agents.ops_agent          import OpsAgent
    from app.config.database           import get_mongo_db
    from app.llm.llm_client            import LLMClient, LLMConfig
    from app.orchestration.graph_router import GraphRouter
 
    llm = LLMClient(LLMConfig(api_key=os.getenv("GROQ_API_KEY",""), model=groq_model, temperature=float(groq_temperature)))
    db  = get_mongo_db()
    pg  = PostgresAdapter(PostgresConfig.from_env())
    rs  = RedisStore(RedisConfig(host=os.getenv("REDIS_HOST","localhost"),
                                  port=int(os.getenv("REDIS_PORT","6379")),
                                  db=int(os.getenv("REDIS_DB","0"))))
    return GraphRouter(
        llm=llm, redis_store=rs,
        mongo_agent=MongoAgent(MongoAdapter(db.client, db_name=db.name), llm_client=llm),
        finance_agent=FinanceAgent(pg, llm_client=llm),
        ops_agent=OpsAgent(pg, llm_client=llm),
    )
 
def _parse_suggestions(md: str) -> List[Tuple[int, str]]:
    out = []
    for line in (md or "").splitlines():
        m = re.match(r"^\s*-\s*(\d{1,2})\.\s+(.*?)\s*$", line)
        if m:
            try: out.append((int(m.group(1)), m.group(2).strip()))
            except Exception: pass
    return out
 
def _init_session() -> None:
    for k, v in {"session_id": f"ui-{uuid.uuid4().hex[:10]}", "messages": [],
                 "last_result": None, "ui_theme": "Dark (Charcoal)"}.items():
        if k not in st.session_state:
            st.session_state[k] = v
 
 
def _render_header() -> None:
    st.markdown(f"""
<div class="kai-hero"><div class="kai-card">
  <div style="display:flex;justify-content:space-between;gap:1rem;align-items:flex-start;flex-wrap:wrap">
    <div>
      <div style="font-size:1.75rem;font-weight:800;letter-spacing:-0.02em;line-height:1.1">{APP_TITLE}</div>
      <div class="kai-muted" style="margin-top:.25rem;font-size:1rem">{APP_SUBTITLE}</div>
    </div>
    <div class="kai-muted" style="font-size:.88rem;line-height:1.3;text-align:right;max-width:17rem">
      Ask about voyages, vessels, ports, cargo, delays/offhire, and finance KPIs.
    </div>
  </div>
</div></div>""", unsafe_allow_html=True)
 
 
def _sidebar_settings() -> UiSettings:
    sid = st.session_state.get("session_id", "")
    st.sidebar.markdown(f"""
<div style="margin:.2rem 0 .5rem">
  <div style="font-size:.9rem;font-weight:700;opacity:.7">Session</div>
  <div style="font-size:.95rem;font-weight:900;font-family:ui-monospace,monospace">{sid}</div>
</div>""", unsafe_allow_html=True)
 
    if not (os.getenv("GROQ_API_KEY") or "").strip():
        st.sidebar.error("Set `GROQ_API_KEY` in `.env`")
 
    # ── Connections ───────────────────────────────
    st.sidebar.markdown("---")
    st.sidebar.markdown("## Connections")
    pg_set    = bool(os.getenv("POSTGRES_DSN") or os.getenv("POSTGRES_DB"))
    mongo_set = bool(os.getenv("MONGO_URI") or os.getenv("MONGO_DB_NAME"))
    st.sidebar.markdown(f"""
<div class="kai-chips">
  <span class="kai-chip"><span class="kai-dot {'kai-dot-ok' if pg_set else 'kai-dot-warn'}"></span>Postgres</span>
  <span class="kai-chip"><span class="kai-dot {'kai-dot-ok' if mongo_set else 'kai-dot-warn'}"></span>Mongo</span>
  <span class="kai-chip"><span class="kai-dot kai-dot-ok"></span>Redis</span>
</div>""", unsafe_allow_html=True)
 
    with st.sidebar.expander("Details", expanded=False):
        pg_dsn = _redact_dsn(os.getenv("POSTGRES_DSN","")) or os.getenv("POSTGRES_DB","(not set)")
        st.caption(f"Postgres: `{pg_dsn}`")
        st.caption(f"Mongo DB: `{os.getenv('MONGO_DB_NAME','(not set)')}`")
        st.caption(f"Redis: `{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}`")
 
    # ── Model ─────────────────────────────────────
    st.sidebar.markdown("---")
    st.sidebar.markdown("## Model")
    with st.sidebar.expander("Settings", expanded=False):
        groq_model       = st.text_input("Model ID", value=os.getenv("GROQ_MODEL","llama-3.3-70b-versatile"))
        groq_temperature = st.slider("Temperature", 0.0, 1.0, float(os.getenv("GROQ_TEMPERATURE","0.0")), 0.1)
 
    # ── Bottom actions ─────────────────────────────
    st.sidebar.markdown("---")
    if st.sidebar.button("Clear backend cache", use_container_width=True):
        st.cache_resource.clear(); st.rerun()
    if st.sidebar.button("New chat", use_container_width=True):
        st.session_state.update({"session_id":f"ui-{uuid.uuid4().hex[:10]}","messages":[],"last_result":None})
        st.rerun()
 
    # ── Theme toggle — very last in sidebar ───────
    st.sidebar.markdown("---")
    is_dark = "Dark" in st.session_state.get("ui_theme", "Dark (Charcoal)")
    icon    = "☀️  Switch to Light" if is_dark else "🌙  Switch to Dark"
    st.sidebar.markdown('<div class="kai-theme-btn">', unsafe_allow_html=True)
    if st.sidebar.button(icon, key="kai_theme_toggle", use_container_width=True):
        st.session_state.ui_theme = "Light (Tech Blue)" if is_dark else "Dark (Charcoal)"
        st.rerun()
    st.sidebar.markdown('</div>', unsafe_allow_html=True)
 
    return UiSettings(groq_model=groq_model.strip() or "llama-3.3-70b-versatile",
                      groq_temperature=float(groq_temperature))
 
 
def _append(role: str, content: str, *, meta: Optional[Dict]=None) -> None:
    msg: Dict[str,Any] = {"role":role,"content":content}
    if meta: msg["meta"] = meta
    st.session_state.messages.append(msg)
 
def _render_messages() -> None:
    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            if m["role"]=="assistant" and m.get("meta"):
                _render_trace(m["meta"])
            st.markdown(m["content"])
 
def _render_trace(result: Dict[str,Any]) -> None:
    trace = result.get("trace")
    if not isinstance(trace,list) or not trace:
        return
    with st.expander("Execution Trace", expanded=False):
        starts  = [e for e in trace if isinstance(e,dict) and e.get("phase")=="composite_step_start"]
        results = {(e.get("step_index"),e.get("agent"),e.get("operation")):e
                   for e in trace if isinstance(e,dict) and e.get("phase")=="composite_step_result"}

        if starts:
            for idx, s in enumerate(starts):
                r   = results.get((s.get("step_index"),s.get("agent"),s.get("operation"))) or {}
                status = "OK" if r.get("ok") is True else ("Failed" if r.get("ok") is False else "Pending")
                step_title = f"Step {s.get('step_index')}: {s.get('agent')}.{s.get('operation')} — {status}"
                with st.expander(step_title, expanded=False):
                    if s.get("goal"):
                        st.caption(s["goal"])
                    st.markdown("**Inputs**")
                    st.json(s.get("inputs") or {})
                    if r:
                        st.markdown("**Results**")
                        if r.get("summary"):
                            st.info(r["summary"])
                        st.json({k:v for k,v in r.items() if k not in ("phase","summary","sql","mongo_query")})
                    if r and isinstance(r.get("sql"), str) and r["sql"].strip():
                        st.markdown("**Generated SQL**")
                        st.code(r["sql"], language="sql")
                    if r and isinstance(r.get("mongo_query"), dict) and any(v is not None for v in (r.get("mongo_query") or {}).values()):
                        st.markdown("**MongoDB Query**")
                        st.json(r["mongo_query"])

        with st.expander("Raw JSON (Debug)", expanded=False):
            st.json(trace)
 
def _run_turn(*, router: Any, user_text: str) -> None:
    user_text = (user_text or "").strip()
    if not user_text:
        return
 
    _append("user", user_text)
 
    try:
        api_result = call_api(
            query=user_text,
            session_id=st.session_state.session_id
        )
 
        answer = (api_result.get("answer") or "").strip() or "No response."
        if api_result.get("clarification"):
            answer = (api_result.get("clarification") or "").strip() or answer
 
        st.session_state.last_result = api_result
 
        # Pass trace/intent/slots so Execution trace expander is shown (same shape as _render_trace expects)
        meta = {
            k: api_result.get(k)
            for k in ("trace", "intent_key", "slots", "dynamic_sql_used", "dynamic_sql_agents")
        }
        _append("assistant", answer, meta=meta if any(meta.get(k) for k in ("trace", "intent_key")) else None)
 
    except Exception as e:
        _append("assistant", f"❌ API Error: {str(e)}")
 
    st.rerun()
 
 
def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="🧭", layout="centered")
    _init_session()
    _inject_global_css(theme=str(st.session_state.get("ui_theme","Dark (Charcoal)")))
    settings = _sidebar_settings()
    _apply_runtime_env(settings)
    _render_header()
 
    if not (os.getenv("GROQ_API_KEY") or "").strip():
        st.info("Set `GROQ_API_KEY` in `.env` to start chatting.")
        _render_messages()
        return
 
    router = None # Router now lives in FastAPI backend and is not directly used in the Streamlit app, but we keep the caching logic around for potential future use.
    _render_messages()
 
    last = st.session_state.messages[-1] if st.session_state.messages else None
    if last and last.get("role")=="assistant" and "Quick question" in (last.get("content") or ""):
        suggestions = _parse_suggestions(last["content"])
        if suggestions:
            cols = st.columns(min(4,len(suggestions)))
            for i,(idx,label) in enumerate(suggestions):
                if cols[i%4].button(f"{idx}. {label}", use_container_width=True):
                    _run_turn(router=router, user_text=str(idx))
 
    user_text = st.chat_input("Ask about a voyage, vessel, port, cargo grades, delays, PnL\u2026")
    if user_text:
        _run_turn(router=router, user_text=user_text)
 
if __name__ == "__main__":
    main()