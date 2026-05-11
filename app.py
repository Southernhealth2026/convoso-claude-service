"""
Convoso Claude Operations Center

Phase 1: Receives Convoso disposition webhooks, generates Claude summaries,
         posts back to lead Notes field.
Phase 2: Monitors Convoso operations every 30 seconds, surfaces anomalies on
         a live dashboard at /dashboard.

Stage 1 monitors:
  - Campaign continuity   (via /campaigns/search)
  - Non-connect rate      (via /agent-performance/search)
  - List depth (hopper)   (via /lists/search + /leads/search per list)

Stage 2 monitors:
  - Idle agents           (via /agent-monitor/search)
  - Connect rate by list  (via /agent-performance/search with list_ids)
  - AM ratio drift        (via /agent-performance/search + history)
  - Productivity outliers (via /agent-performance/search)
"""

import json
import logging
import os
import statistics
import threading
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
from collections import deque

import anthropic
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("convoso-claude")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
CONVOSO_AUTH_TOKEN = os.environ.get("CONVOSO_AUTH_TOKEN")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")
CONVOSO_API_BASE = "https://api.convoso.com/v1"

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))

# --- thresholds (all tunable via env vars) ---
NON_CONNECT_WARN_THRESHOLD = float(os.environ.get("NON_CONNECT_WARN_THRESHOLD", "0.65"))
NON_CONNECT_CRIT_THRESHOLD = float(os.environ.get("NON_CONNECT_CRIT_THRESHOLD", "0.80"))

HOPPER_WARN_LEADS = int(os.environ.get("HOPPER_WARN_LEADS", "1000"))
HOPPER_CRIT_LEADS = int(os.environ.get("HOPPER_CRIT_LEADS", "200"))

# Idle = logged in with too-high wait_sec_pt (% of time spent waiting)
IDLE_WAIT_PCT_WARN = float(os.environ.get("IDLE_WAIT_PCT_WARN", "90"))   # >90% time waiting
IDLE_WAIT_PCT_CRIT = float(os.environ.get("IDLE_WAIT_PCT_CRIT", "95"))   # >95% time waiting

# Per-list connect rate thresholds
CONNECT_RATE_WARN = float(os.environ.get("CONNECT_RATE_WARN", "0.15"))   # below 15% = warning
CONNECT_RATE_CRIT = float(os.environ.get("CONNECT_RATE_CRIT", "0.08"))   # below 8% = critical
CONNECT_RATE_MIN_CALLS = int(os.environ.get("CONNECT_RATE_MIN_CALLS", "20"))  # ignore lists with <20 calls
CONNECT_RATE_MAX_LISTS = int(os.environ.get("CONNECT_RATE_MAX_LISTS", "20"))  # cap per poll

# AM ratio drift - compare current to rolling avg
AM_DRIFT_RATIO_WARN = float(os.environ.get("AM_DRIFT_RATIO_WARN", "1.30"))  # 30% above avg
AM_DRIFT_RATIO_CRIT = float(os.environ.get("AM_DRIFT_RATIO_CRIT", "1.50"))  # 50% above avg
AM_DRIFT_MIN_HISTORY = int(os.environ.get("AM_DRIFT_MIN_HISTORY", "10"))    # need 10 samples

# Productivity outlier thresholds
OUTLIER_MIN_TEAM_SIZE = int(os.environ.get("OUTLIER_MIN_TEAM_SIZE", "5"))
OUTLIER_STDDEV_THRESHOLD = float(os.environ.get("OUTLIER_STDDEV_THRESHOLD", "2.0"))

# History buffer size (= 30 minutes at 30s polls)
HISTORY_MAX_POINTS = int(os.environ.get("HISTORY_MAX_POINTS", "60"))

if not ANTHROPIC_API_KEY:
    log.warning("ANTHROPIC_API_KEY is not set!")
if not CONVOSO_AUTH_TOKEN:
    log.warning("CONVOSO_AUTH_TOKEN is not set!")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ---------------------------------------------------------------------------
# State store with time-series history
# ---------------------------------------------------------------------------

MONITOR_NAMES = [
    "campaign_pauses",
    "non_connect_rate",
    "hopper",
    "idle_agents",
    "connect_rate_by_list",
    "am_ratio_drift",
    "productivity_outliers",
]


class StateStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.last_poll_at = None
        self.last_poll_status = "pending"
        self.monitors = {
            name: {"status": "pending", "data": None, "error": None}
            for name in MONITOR_NAMES
        }
        self.anomalies = deque(maxlen=200)
        self.previous = {}
        # Time-series history per metric
        self.history = {
            "non_connect_rate": deque(maxlen=HISTORY_MAX_POINTS),
            "am_ratio":         deque(maxlen=HISTORY_MAX_POINTS),
            "active_campaigns": deque(maxlen=HISTORY_MAX_POINTS),
            "logged_in_agents": deque(maxlen=HISTORY_MAX_POINTS),
        }

    def update_monitor(self, name, status, data=None, error=None):
        with self.lock:
            self.monitors[name] = {"status": status, "data": data, "error": error}

    def add_anomaly(self, severity, monitor, title, detail, fingerprint=None):
        with self.lock:
            now = datetime.now(timezone.utc).isoformat()
            if fingerprint:
                cutoff = time.time() - 300
                for a in list(self.anomalies)[:30]:
                    if a.get("fingerprint") == fingerprint and a.get("ts_epoch", 0) > cutoff:
                        return
            self.anomalies.appendleft({
                "ts": now, "ts_epoch": time.time(),
                "severity": severity, "monitor": monitor,
                "title": title, "detail": detail, "fingerprint": fingerprint,
            })

    def record_metric(self, name, value):
        """Append a metric point to history."""
        if value is None:
            return
        with self.lock:
            if name in self.history:
                self.history[name].append({
                    "ts_epoch": time.time(),
                    "value": value,
                })

    def get_history_values(self, name, n=None):
        with self.lock:
            arr = list(self.history.get(name, []))
        if n is not None:
            arr = arr[-n:]
        return arr

    def snapshot(self):
        with self.lock:
            history_out = {}
            for k, v in self.history.items():
                history_out[k] = list(v)
            return {
                "last_poll_at": self.last_poll_at,
                "last_poll_status": self.last_poll_status,
                "monitors": dict(self.monitors),
                "anomalies": list(self.anomalies)[:50],
                "history": history_out,
            }


state = StateStore()


# ---------------------------------------------------------------------------
# Convoso API client
# ---------------------------------------------------------------------------

class ConvosoAPIError(Exception):
    pass


def convoso_post(path, payload=None, timeout=20, raise_on_html=True):
    if not CONVOSO_AUTH_TOKEN:
        raise ConvosoAPIError("CONVOSO_AUTH_TOKEN is not configured")
    url = f"{CONVOSO_API_BASE}{path}"
    body = {"auth_token": CONVOSO_AUTH_TOKEN}
    if payload:
        body.update(payload)
    try:
        resp = requests.post(url, data=body, timeout=timeout)
    except requests.RequestException as e:
        raise ConvosoAPIError(f"Network error to {path}: {e}")
    text = resp.text
    if raise_on_html and (text.lstrip().startswith("<") or "<!DOCTYPE" in text[:200]):
        raise ConvosoAPIError(f"Convoso returned HTML (probably 404) for {path}")
    try:
        return resp.json()
    except ValueError:
        raise ConvosoAPIError(f"Convoso returned non-JSON for {path}: {text[:200]}")


# ---------------------------------------------------------------------------
# Phase 1: webhook handler (unchanged)
# ---------------------------------------------------------------------------

def parse_convoso_payload(req):
    content_type = (req.content_type or "").lower()
    raw_body = req.get_data(as_text=True)
    if "application/json" in content_type:
        try:
            return req.get_json(force=True, silent=False)
        except Exception:
            pass
    if "application/x-www-form-urlencoded" in content_type or "=" in raw_body:
        decoded = urllib.parse.unquote_plus(raw_body)
        candidate = decoded.rstrip("=").strip()
        if candidate.startswith("{") and candidate.endswith("}"):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
        form_data = req.form.to_dict()
        if form_data:
            if len(form_data) == 1:
                only_key = next(iter(form_data.keys()))
                if only_key.startswith("{"):
                    try:
                        return json.loads(only_key)
                    except json.JSONDecodeError:
                        pass
            return form_data
    stripped = raw_body.strip().rstrip("=")
    if stripped.startswith("{"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass
    raise ValueError(f"Could not parse payload. Body: {raw_body[:300]}")


def normalize_phone(phone):
    if not phone:
        return ""
    return "".join(c for c in str(phone) if c.isdigit())


def lookup_lead_id_by_phone(phone_number):
    phone_digits = normalize_phone(phone_number)
    if not phone_digits:
        raise ValueError("Cannot lookup - phone number is empty")
    if len(phone_digits) == 11 and phone_digits.startswith("1"):
        phone_digits = phone_digits[1:]
    body = convoso_post("/leads/search", {
        "phone_number": phone_digits, "limit": 5, "offset": 0,
    })
    if not body.get("success"):
        raise RuntimeError(f"Convoso lookup failed: {body}")
    data = body.get("data", {})
    entries = data.get("entries", []) if isinstance(data, dict) else []
    if not entries:
        raise RuntimeError(f"No lead found for phone {phone_digits}")
    def sort_key(lead):
        return (
            lead.get("modified_at") or "",
            int(lead.get("id", 0)) if str(lead.get("id", "0")).isdigit() else 0,
        )
    entries.sort(key=sort_key, reverse=True)
    return entries[0].get("id")


def resolve_lead_id(call_data):
    lead_id = call_data.get("lead_id")
    if lead_id:
        return lead_id
    phone = (call_data.get("phone_number_call")
             or call_data.get("phone_number")
             or call_data.get("phone"))
    if not phone:
        raise ValueError("Payload has no lead_id and no phone number")
    return lookup_lead_id_by_phone(phone)


SUMMARY_SYSTEM_PROMPT = """You are an expert call center analyst writing CRM notes.

Generate a concise, professional 2-3 sentence summary of a call based on the
data provided. Focus on:
  - The outcome (what disposition, what happened)
  - The agent's action
  - Any next steps or follow-up needed

Rules:
  - No preamble. Start directly with the summary.
  - No "**CRM Interaction Note:**" headers or markdown.
  - No disclaimers about missing data - work with what you have.
  - If data is sparse, write a brief factual note. Don't pad with filler.
  - Professional tone. No emojis."""


def generate_summary(call_data):
    if not claude:
        raise RuntimeError("Anthropic client is not configured")
    relevant_fields = {
        "Lead first name": call_data.get("first_name"),
        "Lead last name": call_data.get("last_name"),
        "Phone": call_data.get("phone_number") or call_data.get("phone_number_call"),
        "Disposition": call_data.get("disposition") or call_data.get("status"),
        "Call duration (seconds)": call_data.get("length_in_sec"),
        "Agent": call_data.get("agent_full_name"),
        "Campaign": call_data.get("campaign_name"),
        "Call date": call_data.get("call_date"),
        "Call type": call_data.get("call_type"),
        "Termination reason": call_data.get("term_reason"),
        "Agent comment": call_data.get("agent_comment"),
        "Agent notes": call_data.get("agent_notes"),
    }
    lines = [f"{label}: {value}" for label, value in relevant_fields.items()
             if value not in (None, "", 0)]
    if not lines:
        return "Call completed. No additional details captured."
    response = claude.messages.create(
        model=CLAUDE_MODEL, max_tokens=300,
        system=SUMMARY_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": "Call data:\n" + "\n".join(lines)}],
    )
    return response.content[0].text.strip()


def update_convoso_lead_notes(lead_id, notes):
    body = convoso_post("/leads/update", {"lead_id": str(lead_id), "notes": notes})
    if not body.get("success", False):
        raise RuntimeError(f"Convoso update failed: {body}")
    return body


# ---------------------------------------------------------------------------
# Shared fetchers (cached within a single poll cycle)
# ---------------------------------------------------------------------------

class PollCache:
    """Cache shared between monitors in a single poll cycle to avoid
    duplicate API calls."""
    def __init__(self):
        self.agent_performance = None
        self.lists = None
        self.campaigns = None
        self.agent_monitor = None


def _to_number(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def is_active_status(status_val):
    if status_val is True:
        return True
    if status_val is False:
        return False
    if status_val is None:
        return False
    s = str(status_val).strip().lower()
    return s in ("y", "yes", "1", "true", "active", "running")


def fetch_agent_performance(cache, list_ids=None):
    """Fetch /agent-performance/search. If list_ids given, fetches scoped to
    those lists (not cached because list_ids change). Otherwise uses cache."""
    if list_ids is not None:
        payload = {"list_ids": ",".join(str(i) for i in list_ids)}
        body = convoso_post("/agent-performance/search", payload)
        if not body.get("success"):
            raise ConvosoAPIError(
                f"agent-performance failed for list_ids={list_ids}: "
                f"code={body.get('code')} text={body.get('text')}"
            )
        return body.get("data") or {}
    if cache.agent_performance is None:
        body = convoso_post("/agent-performance/search", {})
        if not body.get("success"):
            raise ConvosoAPIError(
                f"agent-performance/search failed: code={body.get('code')} text={body.get('text')}"
            )
        cache.agent_performance = body.get("data") or {}
    return cache.agent_performance


def fetch_lists_cached(cache):
    if cache.lists is None:
        body = convoso_post("/lists/search", {"limit": 500, "offset": 0})
        if not body.get("success"):
            raise ConvosoAPIError(
                f"lists/search failed: code={body.get('code')} text={body.get('text')}"
            )
        data = body.get("data") or []
        if isinstance(data, dict):
            data = data.get("entries") or list(data.values())
        cache.lists = data
    return cache.lists


def fetch_campaigns_cached(cache):
    if cache.campaigns is None:
        body = convoso_post("/campaigns/search", {"limit": 200, "offset": 0})
        if not body.get("success"):
            raise ConvosoAPIError(
                f"campaigns/search failed: code={body.get('code')} text={body.get('text')}"
            )
        data = body.get("data") or {}
        if isinstance(data, dict):
            entries = data.get("entries")
            if entries is None:
                entries = list(data.values())
        elif isinstance(data, list):
            entries = data
        else:
            entries = []
        cache.campaigns = entries
    return cache.campaigns


def fetch_agent_monitor_cached(cache):
    if cache.agent_monitor is None:
        body = convoso_post("/agent-monitor/search", {})
        if not body.get("success"):
            raise ConvosoAPIError(
                f"agent-monitor/search failed: code={body.get('code')} text={body.get('text')}"
            )
        cache.agent_monitor = body.get("data") or {}
    return cache.agent_monitor


# ---------------------------------------------------------------------------
# Monitor 01: Campaign continuity
# ---------------------------------------------------------------------------

def monitor_campaign_pauses(cache):
    try:
        campaigns = fetch_campaigns_cached(cache)
    except ConvosoAPIError as e:
        state.update_monitor("campaign_pauses", "error", error=str(e))
        return

    summary = []
    for c in campaigns:
        cid = c.get("id") or c.get("campaign_id")
        name = c.get("name") or c.get("campaign_name") or f"Campaign {cid}"
        active = c.get("active")
        if active is None:
            active = c.get("is_active")
        if active is None:
            active = is_active_status(c.get("status"))
        else:
            active = bool(active)
        summary.append({"id": cid, "name": name, "active": active})

    prev = state.previous.get("campaign_pauses") or {}
    prev_by_id = {c["id"]: c for c in prev.get("campaigns", [])}
    for c in summary:
        prev_state = prev_by_id.get(c["id"])
        if prev_state is None:
            continue
        if prev_state["active"] and not c["active"]:
            state.add_anomaly(
                severity="warning", monitor="campaign_pauses",
                title=f"Campaign paused: {c['name']}",
                detail=f"Campaign id {c['id']} transitioned from active to paused.",
                fingerprint=f"campaign_paused:{c['id']}",
            )
        elif not prev_state["active"] and c["active"]:
            state.add_anomaly(
                severity="info", monitor="campaign_pauses",
                title=f"Campaign resumed: {c['name']}",
                detail=f"Campaign id {c['id']} transitioned from paused to active.",
                fingerprint=f"campaign_resumed:{c['id']}",
            )

    active_count = sum(1 for c in summary if c["active"])
    state.previous["campaign_pauses"] = {"campaigns": summary}
    state.record_metric("active_campaigns", active_count)
    state.update_monitor("campaign_pauses", "ok", data={
        "total": len(summary),
        "active": active_count,
        "paused": sum(1 for c in summary if not c["active"]),
        "campaigns": summary,
    })


# ---------------------------------------------------------------------------
# Monitor 02: Non-connect rate
# ---------------------------------------------------------------------------

def monitor_non_connect_rate(cache):
    try:
        agents = fetch_agent_performance(cache)
    except ConvosoAPIError as e:
        state.update_monitor("non_connect_rate", "error", error=str(e))
        return

    if not agents:
        state.update_monitor("non_connect_rate", "ok", data={
            "rate": 0.0, "non_connects": 0, "total_calls": 0,
            "human_answered": 0, "am": 0,
            "warn_threshold": NON_CONNECT_WARN_THRESHOLD,
            "crit_threshold": NON_CONNECT_CRIT_THRESHOLD,
            "severity": "ok",
            "note": "No agents in the response.",
        })
        return

    total_calls = 0.0
    total_human = 0.0
    total_am = 0.0
    agents_counted = 0

    for agent_id, stats in agents.items():
        if not isinstance(stats, dict):
            continue
        calls = _to_number(stats.get("calls"))
        human = _to_number(stats.get("human_answered"))
        am = _to_number(stats.get("am"))
        if calls is None:
            continue
        total_calls += calls
        if human is not None:
            total_human += human
        if am is not None:
            total_am += am
        agents_counted += 1

    if total_calls == 0:
        state.update_monitor("non_connect_rate", "ok", data={
            "rate": 0.0, "non_connects": 0, "total_calls": 0,
            "human_answered": 0, "am": 0,
            "warn_threshold": NON_CONNECT_WARN_THRESHOLD,
            "crit_threshold": NON_CONNECT_CRIT_THRESHOLD,
            "severity": "ok", "agents_counted": agents_counted,
            "note": "No calls reported by any agent today.",
        })
        state.record_metric("non_connect_rate", 0.0)
        state.record_metric("am_ratio", 0.0)
        return

    non_connects = max(0.0, total_calls - total_human - total_am)
    rate = non_connects / total_calls
    am_ratio = total_am / total_calls

    severity = "ok"
    if rate >= NON_CONNECT_CRIT_THRESHOLD:
        severity = "critical"
    elif rate >= NON_CONNECT_WARN_THRESHOLD:
        severity = "warning"

    if severity in ("warning", "critical"):
        state.add_anomaly(
            severity=severity, monitor="non_connect_rate",
            title=f"Non-connect rate at {rate*100:.1f}%",
            detail=(f"{int(non_connects)} non-connects out of {int(total_calls)} dialed "
                    f"(human: {int(total_human)}, AM: {int(total_am)})."),
            fingerprint=f"non_connect_rate:{severity}",
        )

    state.record_metric("non_connect_rate", rate)
    state.record_metric("am_ratio", am_ratio)
    state.update_monitor("non_connect_rate", "ok", data={
        "rate": rate,
        "non_connects": int(non_connects),
        "total_calls": int(total_calls),
        "human_answered": int(total_human),
        "am": int(total_am),
        "warn_threshold": NON_CONNECT_WARN_THRESHOLD,
        "crit_threshold": NON_CONNECT_CRIT_THRESHOLD,
        "severity": severity,
        "agents_counted": agents_counted,
    })


# ---------------------------------------------------------------------------
# Monitor 03: Hopper depth (lists)
# ---------------------------------------------------------------------------

def count_leads_in_list(list_id):
    body = convoso_post("/leads/search", {
        "list_id": list_id, "limit": 1, "offset": 0,
    })
    if not body.get("success"):
        raise ConvosoAPIError(
            f"leads/search failed for list {list_id}: "
            f"code={body.get('code')} text={body.get('text')}"
        )
    total = body.get("total")
    if total is not None:
        try:
            return int(total)
        except (TypeError, ValueError):
            pass
    data = body.get("data") or {}
    if isinstance(data, dict):
        for key in ("total", "total_count", "count"):
            if key in data:
                try:
                    return int(data[key])
                except (TypeError, ValueError):
                    pass
        entries = data.get("entries") or []
        return len(entries)
    if isinstance(data, list):
        return len(data)
    return 0


def monitor_hopper_depletion(cache):
    try:
        lists = fetch_lists_cached(cache)
    except ConvosoAPIError as e:
        state.update_monitor("hopper", "error", error=str(e))
        return

    active_lists = [
        L for L in lists
        if isinstance(L, dict) and is_active_status(L.get("status"))
    ]

    if not active_lists:
        state.update_monitor("hopper", "ok", data={
            "lists": [],
            "warn_threshold": HOPPER_WARN_LEADS,
            "crit_threshold": HOPPER_CRIT_LEADS,
            "note": "No active lists.",
        })
        return

    summaries = []
    errors_per_list = []
    max_lists_per_poll = int(os.environ.get("HOPPER_MAX_LISTS_PER_POLL", "20"))
    checked = active_lists[:max_lists_per_poll]

    for L in checked:
        list_id = L.get("id")
        list_name = L.get("name") or f"List {list_id}"
        try:
            total = count_leads_in_list(list_id)
        except ConvosoAPIError as e:
            errors_per_list.append((list_name, str(e)))
            continue

        severity = "ok"
        if total <= HOPPER_CRIT_LEADS:
            severity = "critical"
        elif total <= HOPPER_WARN_LEADS:
            severity = "warning"

        if severity in ("warning", "critical"):
            state.add_anomaly(
                severity=severity, monitor="hopper",
                title=f"List depleting: {list_name}",
                detail=f"{total} leads remain in active list (id {list_id}).",
                fingerprint=f"hopper:{list_id}:{severity}",
            )

        summaries.append({
            "list_id": list_id,
            "list_name": list_name,
            "remaining": total,
            "severity": severity,
        })

    if not summaries and errors_per_list:
        state.update_monitor("hopper", "error", error=(
            f"Could not count leads. First error: {errors_per_list[0][1]}"
        ))
        return

    state.update_monitor("hopper", "ok", data={
        "lists": summaries,
        "warn_threshold": HOPPER_WARN_LEADS,
        "crit_threshold": HOPPER_CRIT_LEADS,
        "active_total": len(active_lists),
        "checked": len(checked),
    })


# ---------------------------------------------------------------------------
# Monitor 04: Idle agents
#
# Strategy: use agent-performance's wait_sec_pt field, which is the percentage
# of each agent's logged-in time spent waiting. High wait% with low calls
# means they're online but not productive. This is a much cleaner signal than
# trying to combine /agent-monitor/search with call counts.
# ---------------------------------------------------------------------------

def monitor_idle_agents(cache):
    try:
        agents = fetch_agent_performance(cache)
    except ConvosoAPIError as e:
        state.update_monitor("idle_agents", "error", error=str(e))
        return

    if not agents:
        state.update_monitor("idle_agents", "ok", data={
            "logged_in": 0, "idle": 0, "active": 0, "idle_list": [],
            "note": "No agent records.",
        })
        return

    idle_list = []
    active_count = 0
    logged_in_count = 0

    for agent_id, stats in agents.items():
        if not isinstance(stats, dict):
            continue
        name = stats.get("name") or f"Agent {agent_id}"
        wait_pct = _to_number(stats.get("wait_sec_pt"))
        calls = _to_number(stats.get("calls")) or 0
        total_time = stats.get("total_time")    # "HH:MM:SS" string
        if wait_pct is None:
            continue
        logged_in_count += 1

        severity = None
        if wait_pct >= IDLE_WAIT_PCT_CRIT:
            severity = "critical"
        elif wait_pct >= IDLE_WAIT_PCT_WARN:
            severity = "warning"

        if severity:
            idle_list.append({
                "user_id": agent_id,
                "name": name,
                "wait_pct": wait_pct,
                "calls": int(calls),
                "total_time": total_time,
                "severity": severity,
            })
            state.add_anomaly(
                severity=severity, monitor="idle_agents",
                title=f"Agent waiting: {name}",
                detail=f"{wait_pct:.0f}% of logged-in time waiting ({int(calls)} calls).",
                fingerprint=f"idle:{agent_id}:{severity}",
            )
        else:
            active_count += 1

    state.update_monitor("idle_agents", "ok", data={
        "logged_in": logged_in_count,
        "idle": len(idle_list),
        "active": active_count,
        "idle_list": sorted(idle_list, key=lambda x: -x["wait_pct"])[:20],
        "warn_threshold": IDLE_WAIT_PCT_WARN,
        "crit_threshold": IDLE_WAIT_PCT_CRIT,
    })
    state.record_metric("logged_in_agents", logged_in_count)


# ---------------------------------------------------------------------------
# Monitor 05: Connect rate by list
#
# For each active list, call /agent-performance/search with list_ids=<id>
# and compute human_answered / calls. Spread across multiple poll cycles to
# limit API load - we cycle through active lists round-robin.
# ---------------------------------------------------------------------------

_connect_rate_results = {}    # list_id -> {result dict, ts_epoch}
_connect_rate_lock = threading.Lock()
_connect_rate_round_robin_index = 0


def monitor_connect_rate_by_list(cache):
    global _connect_rate_round_robin_index

    try:
        lists = fetch_lists_cached(cache)
    except ConvosoAPIError as e:
        state.update_monitor("connect_rate_by_list", "error", error=str(e))
        return

    active_lists = [
        L for L in lists
        if isinstance(L, dict) and is_active_status(L.get("status"))
    ]
    if not active_lists:
        state.update_monitor("connect_rate_by_list", "ok", data={
            "lists": [],
            "note": "No active lists.",
        })
        return

    # Round-robin: each poll cycle, check the next N lists
    per_cycle = int(os.environ.get("CONNECT_RATE_PER_CYCLE", "4"))
    start = _connect_rate_round_robin_index % len(active_lists)
    batch = []
    for i in range(per_cycle):
        idx = (start + i) % len(active_lists)
        batch.append(active_lists[idx])
    _connect_rate_round_robin_index = (start + per_cycle) % len(active_lists)

    now = time.time()
    for L in batch:
        list_id = L.get("id")
        list_name = L.get("name") or f"List {list_id}"
        try:
            data = fetch_agent_performance(cache, list_ids=[list_id])
        except ConvosoAPIError as e:
            with _connect_rate_lock:
                _connect_rate_results[list_id] = {
                    "list_id": list_id, "list_name": list_name,
                    "error": str(e), "ts_epoch": now,
                }
            continue

        if not isinstance(data, dict) or not data:
            with _connect_rate_lock:
                _connect_rate_results[list_id] = {
                    "list_id": list_id, "list_name": list_name,
                    "calls": 0, "human_answered": 0, "rate": None,
                    "ts_epoch": now, "note": "No agent activity on this list.",
                }
            continue

        calls_sum = 0.0
        human_sum = 0.0
        for stats in data.values():
            if not isinstance(stats, dict):
                continue
            c = _to_number(stats.get("calls")) or 0
            h = _to_number(stats.get("human_answered")) or 0
            calls_sum += c
            human_sum += h

        rate = (human_sum / calls_sum) if calls_sum else None
        severity = "ok"
        if calls_sum >= CONNECT_RATE_MIN_CALLS and rate is not None:
            if rate <= CONNECT_RATE_CRIT:
                severity = "critical"
            elif rate <= CONNECT_RATE_WARN:
                severity = "warning"

        if severity in ("warning", "critical"):
            state.add_anomaly(
                severity=severity, monitor="connect_rate_by_list",
                title=f"Low connect rate: {list_name}",
                detail=f"{(rate or 0)*100:.1f}% connect on {int(calls_sum)} calls.",
                fingerprint=f"connect_rate:{list_id}:{severity}",
            )

        with _connect_rate_lock:
            _connect_rate_results[list_id] = {
                "list_id": list_id, "list_name": list_name,
                "calls": int(calls_sum), "human_answered": int(human_sum),
                "rate": rate, "severity": severity, "ts_epoch": now,
            }

    # Build summary from cached results, filtering out stale entries
    with _connect_rate_lock:
        results = list(_connect_rate_results.values())

    # Drop entries for lists no longer active
    active_ids = {L.get("id") for L in active_lists}
    results = [r for r in results if r.get("list_id") in active_ids]

    state.update_monitor("connect_rate_by_list", "ok", data={
        "lists": sorted(results, key=lambda r: r.get("rate") or 0),
        "active_total": len(active_lists),
        "checked_so_far": len(results),
        "warn_threshold": CONNECT_RATE_WARN,
        "crit_threshold": CONNECT_RATE_CRIT,
        "min_calls_to_evaluate": CONNECT_RATE_MIN_CALLS,
    })


# ---------------------------------------------------------------------------
# Monitor 06: AM ratio drift
#
# Compares current AM% to the rolling average over recent history. Sudden
# spikes indicate list quality issues, timezone problems, or DID issues.
# ---------------------------------------------------------------------------

def monitor_am_ratio_drift(cache):
    history = state.get_history_values("am_ratio")
    current = history[-1]["value"] if history else None

    if current is None:
        state.update_monitor("am_ratio_drift", "ok", data={
            "current": None,
            "baseline": None,
            "ratio_to_baseline": None,
            "severity": "ok",
            "history_points": 0,
            "note": "Waiting for AM ratio data to accumulate.",
        })
        return

    # Need enough history before we trust the baseline
    if len(history) < AM_DRIFT_MIN_HISTORY:
        state.update_monitor("am_ratio_drift", "ok", data={
            "current": current,
            "baseline": None,
            "ratio_to_baseline": None,
            "severity": "ok",
            "history_points": len(history),
            "warn_ratio": AM_DRIFT_RATIO_WARN,
            "crit_ratio": AM_DRIFT_RATIO_CRIT,
            "note": f"Building baseline ({len(history)}/{AM_DRIFT_MIN_HISTORY} samples).",
        })
        return

    # Use median of older history (excludes most recent point) as baseline
    older = [p["value"] for p in history[:-1]]
    baseline = statistics.median(older)

    severity = "ok"
    ratio_to_baseline = None
    if baseline > 0.0001:
        ratio_to_baseline = current / baseline
        if ratio_to_baseline >= AM_DRIFT_RATIO_CRIT:
            severity = "critical"
        elif ratio_to_baseline >= AM_DRIFT_RATIO_WARN:
            severity = "warning"

    if severity in ("warning", "critical"):
        state.add_anomaly(
            severity=severity, monitor="am_ratio_drift",
            title=f"AM ratio spike: {current*100:.1f}% (baseline {baseline*100:.1f}%)",
            detail=f"Answering-machine ratio is {ratio_to_baseline:.2f}x the recent baseline.",
            fingerprint=f"am_drift:{severity}",
        )

    state.update_monitor("am_ratio_drift", "ok", data={
        "current": current,
        "baseline": baseline,
        "ratio_to_baseline": ratio_to_baseline,
        "severity": severity,
        "history_points": len(history),
        "warn_ratio": AM_DRIFT_RATIO_WARN,
        "crit_ratio": AM_DRIFT_RATIO_CRIT,
    })


# ---------------------------------------------------------------------------
# Monitor 07: Productivity outliers
#
# Identify agents who are >N std deviations above or below team mean for
# 'calls'. Excludes obvious non-dialers (zero calls or very short total_time).
# ---------------------------------------------------------------------------

def parse_hms(s):
    """Parse 'HH:MM:SS' into total seconds. Returns None on failure."""
    if not s:
        return None
    try:
        parts = str(s).split(":")
        if len(parts) != 3:
            return None
        h, m, sec = (int(p) for p in parts)
        return h * 3600 + m * 60 + sec
    except (TypeError, ValueError):
        return None


def monitor_productivity_outliers(cache):
    try:
        agents = fetch_agent_performance(cache)
    except ConvosoAPIError as e:
        state.update_monitor("productivity_outliers", "error", error=str(e))
        return

    eligible = []
    for agent_id, stats in agents.items():
        if not isinstance(stats, dict):
            continue
        calls = _to_number(stats.get("calls"))
        total_sec = parse_hms(stats.get("total_time"))
        # Only consider agents who've been logged in long enough to be comparable
        if calls is None or total_sec is None:
            continue
        if total_sec < 1800:    # less than 30 min logged in - skip
            continue
        eligible.append({
            "user_id": agent_id,
            "name": stats.get("name") or f"Agent {agent_id}",
            "calls": int(calls),
            "total_time": stats.get("total_time"),
            "total_sec": total_sec,
            "human_answered": int(_to_number(stats.get("human_answered")) or 0),
        })

    if len(eligible) < OUTLIER_MIN_TEAM_SIZE:
        state.update_monitor("productivity_outliers", "ok", data={
            "team_size": len(eligible),
            "team_mean_calls": None,
            "team_stddev_calls": None,
            "top": [], "bottom": [],
            "note": (f"Not enough agents to evaluate "
                     f"({len(eligible)}/{OUTLIER_MIN_TEAM_SIZE}+).")
        })
        return

    calls_per_hour = [
        (a, a["calls"] / (a["total_sec"] / 3600.0))
        for a in eligible
    ]
    rates = [r for _, r in calls_per_hour]
    mean = statistics.mean(rates)
    stddev = statistics.stdev(rates) if len(rates) > 1 else 0.0

    if stddev > 0:
        for a, rate in calls_per_hour:
            a["calls_per_hour"] = round(rate, 1)
            a["z_score"] = (rate - mean) / stddev
    else:
        for a, rate in calls_per_hour:
            a["calls_per_hour"] = round(rate, 1)
            a["z_score"] = 0.0

    sorted_by_z = sorted(eligible, key=lambda a: a["z_score"])
    bottom = [a for a in sorted_by_z if a["z_score"] <= -OUTLIER_STDDEV_THRESHOLD][:5]
    top = [a for a in reversed(sorted_by_z) if a["z_score"] >= OUTLIER_STDDEV_THRESHOLD][:5]

    for a in bottom:
        state.add_anomaly(
            severity="warning", monitor="productivity_outliers",
            title=f"Underperformer: {a['name']}",
            detail=(f"{a['calls_per_hour']} calls/hr vs team avg {mean:.1f} "
                    f"(z={a['z_score']:.1f})."),
            fingerprint=f"under:{a['user_id']}",
        )
    for a in top:
        state.add_anomaly(
            severity="info", monitor="productivity_outliers",
            title=f"Top performer: {a['name']}",
            detail=(f"{a['calls_per_hour']} calls/hr vs team avg {mean:.1f} "
                    f"(z={a['z_score']:.1f})."),
            fingerprint=f"top:{a['user_id']}",
        )

    state.update_monitor("productivity_outliers", "ok", data={
        "team_size": len(eligible),
        "team_mean_calls_per_hour": round(mean, 1),
        "team_stddev": round(stddev, 2),
        "top": top, "bottom": bottom,
        "threshold_stddev": OUTLIER_STDDEV_THRESHOLD,
    })


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

MONITORS_ORDERED = [
    ("campaign_pauses",        monitor_campaign_pauses),
    ("non_connect_rate",       monitor_non_connect_rate),
    ("hopper",                 monitor_hopper_depletion),
    ("idle_agents",            monitor_idle_agents),
    ("connect_rate_by_list",   monitor_connect_rate_by_list),
    ("am_ratio_drift",         monitor_am_ratio_drift),
    ("productivity_outliers",  monitor_productivity_outliers),
]


def run_all_monitors():
    started = time.time()
    log.info("Monitor poll cycle starting")
    cache = PollCache()
    statuses = []
    for name, fn in MONITORS_ORDERED:
        try:
            fn(cache)
            statuses.append(state.monitors[name]["status"])
        except Exception as e:
            log.exception("Monitor %s crashed", name)
            state.update_monitor(name, "error", error=f"Crash: {e}")
            statuses.append("error")
    if all(s == "ok" for s in statuses):
        overall = "ok"
    elif any(s == "ok" for s in statuses):
        overall = "partial"
    else:
        overall = "error"
    state.last_poll_at = datetime.now(timezone.utc).isoformat()
    state.last_poll_status = overall
    log.info("Poll finished in %.2fs status=%s", time.time() - started, overall)


def start_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(run_all_monitors, "interval",
                  seconds=POLL_INTERVAL_SECONDS,
                  next_run_time=datetime.now(),
                  max_instances=1, coalesce=True)
    sched.start()
    log.info("Scheduler started: polling every %ss", POLL_INTERVAL_SECONDS)


_scheduler_started = False
_scheduler_lock = threading.Lock()


@app.before_request
def ensure_scheduler():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            start_scheduler()
            _scheduler_started = True


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "service": "convoso-claude-summary",
        "status": "running",
        "endpoints": {
            "POST /webhook":     "Receives Convoso disposition webhooks (Phase 1)",
            "GET /dashboard":    "Live operations dashboard (Phase 2)",
            "GET /api/state":    "JSON state snapshot used by dashboard",
            "GET /api/diagnose": "Probes Convoso endpoints to verify availability",
            "GET /api/probe":    "Inspect a single Convoso endpoint response shape",
            "POST /api/poll":    "Force an immediate monitor poll",
            "GET /":             "This health check",
        },
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        call_data = parse_convoso_payload(request)
    except ValueError as e:
        log.error("Payload parse failed: %s", e)
        return jsonify({"ok": False, "error": "bad_payload", "detail": str(e)}), 400
    try:
        lead_id = resolve_lead_id(call_data)
    except Exception as e:
        log.exception("Could not resolve lead_id")
        return jsonify({"ok": False, "error": "lead_id_unresolvable", "detail": str(e)}), 400
    try:
        summary = generate_summary(call_data)
    except Exception as e:
        log.exception("Claude call failed")
        return jsonify({"ok": False, "error": "claude_failed", "detail": str(e)}), 500
    try:
        result = update_convoso_lead_notes(lead_id, summary)
    except Exception as e:
        log.exception("Convoso update failed")
        return jsonify({
            "ok": False, "error": "convoso_update_failed", "detail": str(e),
            "summary_that_would_have_been_saved": summary,
        }), 500
    return jsonify({"ok": True, "lead_id": lead_id, "summary": summary,
                    "convoso_response": result})


@app.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/state", methods=["GET"])
def api_state():
    return jsonify(state.snapshot())


@app.route("/api/diagnose", methods=["GET"])
def api_diagnose():
    paths_to_test = [
        ("/campaigns/search",          {"limit": 1, "offset": 0}),
        ("/lists/search",              {"limit": 1, "offset": 0}),
        ("/leads/search",              {"limit": 1, "offset": 0}),
        ("/users/search",              {"limit": 1, "offset": 0}),
        ("/agent-performance/search",  {}),
        ("/agent-productivity/search", {"limit": 1, "offset": 0}),
        ("/agent-monitor/search",      {}),
        ("/callbacks/search",          {"limit": 1, "offset": 0}),
        ("/dnc/search",                {"limit": 1, "offset": 0}),
    ]
    results = {}
    for path, payload in paths_to_test:
        try:
            body = convoso_post(path, payload)
            results[path] = {
                "success": body.get("success"),
                "code": body.get("code"),
                "text": body.get("text"),
                "total": body.get("total"),
                "data_keys": (list(body.get("data", {}).keys())
                              if isinstance(body.get("data"), dict) else None),
            }
        except ConvosoAPIError as e:
            results[path] = {"error": str(e)}
        except Exception as e:
            results[path] = {"error": f"unexpected: {e}",
                             "trace": traceback.format_exc()[:300]}
    return jsonify(results)


@app.route("/api/probe", methods=["GET"])
def api_probe():
    path = request.args.get("path")
    if not path:
        return jsonify({"error": "Pass ?path=/some/endpoint"}), 400
    if not path.startswith("/"):
        path = "/" + path
    extra = {}
    for k in ("list_id", "list_ids", "campaign_id", "limit", "offset",
              "status", "start_time", "end_time", "agent_emails"):
        v = request.args.get(k)
        if v is not None:
            extra[k] = v
    if "limit" not in extra:
        extra["limit"] = 3
    if "offset" not in extra:
        extra["offset"] = 0
    try:
        body = convoso_post(path, extra)
        return jsonify(body)
    except ConvosoAPIError as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/poll", methods=["POST", "GET"])
def api_poll():
    run_all_monitors()
    return jsonify({"ok": True, "snapshot": state.snapshot()})


if __name__ == "__main__":
    start_scheduler()
    _scheduler_started = True
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
