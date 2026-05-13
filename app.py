"""
Convoso Claude Operations Center

Phase 1: Receives Convoso disposition webhooks, generates Claude summaries.
Phase 2: Live ops dashboard with 9 monitors + AI briefing at /dashboard.

Monitor inventory:
  01 Campaign continuity   /campaigns/search
  02 Non-connect rate      /agent-performance/search aggregated
  03 Hopper depth          /lists/search + /leads/search per list
  04 Idle agents           /agent-performance/search wait_sec_pt
  05 Connect rate by list  /agent-performance/search with list_ids
  06 AM ratio drift        /agent-performance/search + history
  07 Productivity outliers /agent-performance/search (inbound-filtered)
  08 Talk-time anomalies   /agent-performance/search talk_sec_pt
  09 Pause-time anomalies  /agent-performance/search pause_sec_pt
  10 Claude AI briefing    every N minutes; analyst-style summary

v1.0 changes from v0.6:
  - Two new monitors (talk time, pause time)
  - Background scheduler for Claude briefing
  - State endpoint exposes briefing
  - POST /api/briefing/refresh for manual regeneration
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
BRIEFING_INTERVAL_MINUTES = int(os.environ.get("BRIEFING_INTERVAL_MINUTES", "10"))

# --- thresholds ---
NON_CONNECT_WARN_THRESHOLD = float(os.environ.get("NON_CONNECT_WARN_THRESHOLD", "0.65"))
NON_CONNECT_CRIT_THRESHOLD = float(os.environ.get("NON_CONNECT_CRIT_THRESHOLD", "0.80"))

HOPPER_WARN_LEADS = int(os.environ.get("HOPPER_WARN_LEADS", "1000"))
HOPPER_CRIT_LEADS = int(os.environ.get("HOPPER_CRIT_LEADS", "200"))

IDLE_WAIT_PCT_WARN = float(os.environ.get("IDLE_WAIT_PCT_WARN", "90"))
IDLE_WAIT_PCT_CRIT = float(os.environ.get("IDLE_WAIT_PCT_CRIT", "95"))

CONNECT_RATE_WARN = float(os.environ.get("CONNECT_RATE_WARN", "0.15"))
CONNECT_RATE_CRIT = float(os.environ.get("CONNECT_RATE_CRIT", "0.08"))
CONNECT_RATE_MIN_CALLS = int(os.environ.get("CONNECT_RATE_MIN_CALLS", "20"))

AM_DRIFT_RATIO_WARN = float(os.environ.get("AM_DRIFT_RATIO_WARN", "1.30"))
AM_DRIFT_RATIO_CRIT = float(os.environ.get("AM_DRIFT_RATIO_CRIT", "1.50"))
AM_DRIFT_MIN_HISTORY = int(os.environ.get("AM_DRIFT_MIN_HISTORY", "10"))

OUTLIER_MIN_TEAM_SIZE = int(os.environ.get("OUTLIER_MIN_TEAM_SIZE", "5"))
OUTLIER_STDDEV_THRESHOLD = float(os.environ.get("OUTLIER_STDDEV_THRESHOLD", "2.0"))

# Talk-time: agents stuck on a long call or barely talking
TALK_TIME_HIGH_WARN = float(os.environ.get("TALK_TIME_HIGH_WARN", "85"))   # >85% talk = possible stuck call
TALK_TIME_HIGH_CRIT = float(os.environ.get("TALK_TIME_HIGH_CRIT", "92"))
TALK_TIME_LOW_WARN  = float(os.environ.get("TALK_TIME_LOW_WARN",  "3"))    # <3% talk for >30min = quiet
TALK_TIME_LOW_CRIT  = float(os.environ.get("TALK_TIME_LOW_CRIT",  "1"))
TALK_TIME_MIN_LOGGED_SECONDS = 1800    # only evaluate after 30 min logged in

# Pause-time: extended breaks / AFK
PAUSE_TIME_WARN = float(os.environ.get("PAUSE_TIME_WARN", "15"))
PAUSE_TIME_CRIT = float(os.environ.get("PAUSE_TIME_CRIT", "25"))
PAUSE_TIME_MIN_LOGGED_SECONDS = 1800

DEFAULT_EXCLUDE_PATTERNS = "custo,customer,inbound,manager,sup,supervisor,trainer,training,test,admin,qa"
OUTLIER_EXCLUDE_PATTERNS = [
    p.strip().lower()
    for p in os.environ.get("OUTLIER_EXCLUDE_PATTERNS", DEFAULT_EXCLUDE_PATTERNS).split(",")
    if p.strip()
]

HISTORY_MAX_POINTS = int(os.environ.get("HISTORY_MAX_POINTS", "60"))

if not ANTHROPIC_API_KEY:
    log.warning("ANTHROPIC_API_KEY is not set!")
if not CONVOSO_AUTH_TOKEN:
    log.warning("CONVOSO_AUTH_TOKEN is not set!")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ---------------------------------------------------------------------------
# State store
# ---------------------------------------------------------------------------

MONITOR_NAMES = [
    "campaign_pauses",
    "non_connect_rate",
    "hopper",
    "idle_agents",
    "connect_rate_by_list",
    "am_ratio_drift",
    "productivity_outliers",
    "talk_time_anomalies",
    "pause_time_anomalies",
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
        self.history = {
            "non_connect_rate": deque(maxlen=HISTORY_MAX_POINTS),
            "am_ratio":         deque(maxlen=HISTORY_MAX_POINTS),
            "active_campaigns": deque(maxlen=HISTORY_MAX_POINTS),
            "logged_in_agents": deque(maxlen=HISTORY_MAX_POINTS),
        }
        self.entity_severity = {}
        self.briefing = {
            "text": None,
            "generated_at": None,
            "generating": False,
            "error": None,
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

    def get_entity_severity(self, monitor, entity_id):
        with self.lock:
            return self.entity_severity.get(monitor, {}).get(str(entity_id), "ok")

    def set_entity_severity(self, monitor, entity_id, severity):
        with self.lock:
            if monitor not in self.entity_severity:
                self.entity_severity[monitor] = {}
            self.entity_severity[monitor][str(entity_id)] = severity

    def all_entity_severities(self, monitor):
        with self.lock:
            return dict(self.entity_severity.get(monitor, {}))

    def emit_on_transition(self, monitor, entity_id, new_severity,
                           entity_name, detail_for_new_severity):
        prev_severity = self.get_entity_severity(monitor, entity_id)
        if prev_severity == new_severity:
            return False
        self.set_entity_severity(monitor, entity_id, new_severity)

        if new_severity in ("warning", "critical"):
            self.add_anomaly(
                severity=new_severity, monitor=monitor,
                title=f"{entity_name}",
                detail=detail_for_new_severity,
                fingerprint=f"{monitor}:{entity_id}:{new_severity}",
            )
            return True

        if new_severity == "ok" and prev_severity in ("warning", "critical"):
            self.add_anomaly(
                severity="info", monitor=monitor,
                title=f"Resolved: {entity_name}",
                detail=f"Returned to normal from {prev_severity}.",
                fingerprint=f"{monitor}:{entity_id}:resolved",
            )
            return True
        return False

    def set_briefing(self, text=None, error=None, generating=None):
        with self.lock:
            if generating is not None:
                self.briefing["generating"] = generating
            if text is not None:
                self.briefing["text"] = text
                self.briefing["generated_at"] = datetime.now(timezone.utc).isoformat()
                self.briefing["error"] = None
            if error is not None:
                self.briefing["error"] = error

    def get_briefing(self):
        with self.lock:
            return dict(self.briefing)

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
                "briefing": dict(self.briefing),
            }


state = StateStore()


# ---------------------------------------------------------------------------
# Convoso API
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
# Shared helpers
# ---------------------------------------------------------------------------

class PollCache:
    def __init__(self):
        self.agent_performance = None
        self.lists = None
        self.campaigns = None


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


def is_excluded_agent(name):
    if not name:
        return False
    name_lower = name.lower()
    return any(p in name_lower for p in OUTLIER_EXCLUDE_PATTERNS)


def parse_hms(s):
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


def fetch_agent_performance(cache, list_ids=None):
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

    for c in summary:
        new_severity = "ok" if c["active"] else "warning"
        state.emit_on_transition(
            monitor="campaign_pauses",
            entity_id=c["id"],
            new_severity=new_severity,
            entity_name=("Campaign resumed: " if c["active"] else "Campaign paused: ") + c["name"],
            detail_for_new_severity=(f"Campaign id {c['id']} is now paused."
                                     if not c["active"]
                                     else f"Campaign id {c['id']} is active again."),
        )

    active_count = sum(1 for c in summary if c["active"])
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

    state.emit_on_transition(
        monitor="non_connect_rate",
        entity_id="team",
        new_severity=severity,
        entity_name=f"Non-connect rate at {rate*100:.1f}%",
        detail_for_new_severity=(
            f"{int(non_connects)} non-connects out of {int(total_calls)} dialed "
            f"(human: {int(total_human)}, AM: {int(total_am)})."
        ),
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
# Monitor 03: Hopper depth
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

        state.emit_on_transition(
            monitor="hopper",
            entity_id=list_id,
            new_severity=severity,
            entity_name=f"List depleting: {list_name}",
            detail_for_new_severity=f"{total} leads remain in active list (id {list_id}).",
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
        total_time = stats.get("total_time")
        if wait_pct is None:
            continue
        logged_in_count += 1

        severity = "ok"
        if wait_pct >= IDLE_WAIT_PCT_CRIT:
            severity = "critical"
        elif wait_pct >= IDLE_WAIT_PCT_WARN:
            severity = "warning"

        state.emit_on_transition(
            monitor="idle_agents",
            entity_id=agent_id,
            new_severity=severity,
            entity_name=f"Agent waiting: {name}",
            detail_for_new_severity=(
                f"{wait_pct:.0f}% of logged-in time waiting ({int(calls)} calls)."
            ),
        )

        if severity == "ok":
            active_count += 1
        else:
            idle_list.append({
                "user_id": agent_id,
                "name": name,
                "wait_pct": wait_pct,
                "calls": int(calls),
                "total_time": total_time,
                "severity": severity,
            })

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
# ---------------------------------------------------------------------------

_connect_rate_results = {}
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
            state.emit_on_transition(
                monitor="connect_rate_by_list", entity_id=list_id,
                new_severity="ok",
                entity_name=f"Low connect rate: {list_name}",
                detail_for_new_severity="",
            )
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

        state.emit_on_transition(
            monitor="connect_rate_by_list",
            entity_id=list_id,
            new_severity=severity,
            entity_name=f"Low connect rate: {list_name}",
            detail_for_new_severity=f"{(rate or 0)*100:.1f}% connect on {int(calls_sum)} calls.",
        )

        with _connect_rate_lock:
            _connect_rate_results[list_id] = {
                "list_id": list_id, "list_name": list_name,
                "calls": int(calls_sum), "human_answered": int(human_sum),
                "rate": rate, "severity": severity, "ts_epoch": now,
            }

    with _connect_rate_lock:
        results = list(_connect_rate_results.values())

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
# ---------------------------------------------------------------------------

def monitor_am_ratio_drift(cache):
    history = state.get_history_values("am_ratio")
    current = history[-1]["value"] if history else None

    if current is None:
        state.update_monitor("am_ratio_drift", "ok", data={
            "current": None, "baseline": None,
            "ratio_to_baseline": None, "severity": "ok",
            "history_points": 0,
            "note": "Waiting for AM ratio data to accumulate.",
        })
        return

    if len(history) < AM_DRIFT_MIN_HISTORY:
        state.update_monitor("am_ratio_drift", "ok", data={
            "current": current, "baseline": None,
            "ratio_to_baseline": None, "severity": "ok",
            "history_points": len(history),
            "warn_ratio": AM_DRIFT_RATIO_WARN,
            "crit_ratio": AM_DRIFT_RATIO_CRIT,
            "note": f"Building baseline ({len(history)}/{AM_DRIFT_MIN_HISTORY} samples).",
        })
        return

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

    state.emit_on_transition(
        monitor="am_ratio_drift",
        entity_id="team",
        new_severity=severity,
        entity_name=f"AM ratio spike: {current*100:.1f}% (baseline {baseline*100:.1f}%)",
        detail_for_new_severity=(
            f"Answering-machine ratio is {ratio_to_baseline:.2f}x the recent baseline."
            if ratio_to_baseline is not None else ""
        ),
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
# ---------------------------------------------------------------------------

def monitor_productivity_outliers(cache):
    try:
        agents = fetch_agent_performance(cache)
    except ConvosoAPIError as e:
        state.update_monitor("productivity_outliers", "error", error=str(e))
        return

    eligible = []
    excluded = []
    for agent_id, stats in agents.items():
        if not isinstance(stats, dict):
            continue
        name = stats.get("name") or f"Agent {agent_id}"
        calls = _to_number(stats.get("calls"))
        total_sec = parse_hms(stats.get("total_time"))
        if calls is None or total_sec is None:
            continue
        if total_sec < 1800:
            continue
        if is_excluded_agent(name):
            excluded.append({"user_id": agent_id, "name": name})
            continue
        eligible.append({
            "user_id": agent_id,
            "name": name,
            "calls": int(calls),
            "total_time": stats.get("total_time"),
            "total_sec": total_sec,
            "human_answered": int(_to_number(stats.get("human_answered")) or 0),
        })

    previous_severities = state.all_entity_severities("productivity_outliers")
    eligible_ids = {str(a["user_id"]) for a in eligible}
    for prev_id, prev_sev in previous_severities.items():
        if prev_id not in eligible_ids and prev_sev in ("warning", "critical"):
            state.emit_on_transition(
                monitor="productivity_outliers",
                entity_id=prev_id,
                new_severity="ok",
                entity_name=f"Agent {prev_id}",
                detail_for_new_severity="",
            )

    if len(eligible) < OUTLIER_MIN_TEAM_SIZE:
        state.update_monitor("productivity_outliers", "ok", data={
            "team_size": len(eligible),
            "team_mean_calls_per_hour": None,
            "team_stddev": None,
            "top": [], "bottom": [],
            "excluded_count": len(excluded),
            "excluded_names": [e["name"] for e in excluded][:10],
            "exclude_patterns": OUTLIER_EXCLUDE_PATTERNS,
            "note": (f"Not enough agents to evaluate "
                     f"({len(eligible)}/{OUTLIER_MIN_TEAM_SIZE}+)."),
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

    bottom_ids = {str(a["user_id"]) for a in bottom}
    top_ids    = {str(a["user_id"]) for a in top}

    for a in eligible:
        aid = str(a["user_id"])
        if aid in bottom_ids:
            new_sev = "warning"
            entity_name = f"Underperformer: {a['name']}"
            detail = (f"{a['calls_per_hour']} calls/hr vs team avg "
                      f"{mean:.1f} (z={a['z_score']:.1f}).")
        elif aid in top_ids:
            prev = state.get_entity_severity("productivity_outliers", aid)
            if prev != "top":
                state.set_entity_severity("productivity_outliers", aid, "top")
                state.add_anomaly(
                    severity="info", monitor="productivity_outliers",
                    title=f"Top performer: {a['name']}",
                    detail=(f"{a['calls_per_hour']} calls/hr vs team avg "
                            f"{mean:.1f} (z={a['z_score']:.1f})."),
                    fingerprint=f"productivity_outliers:{aid}:top",
                )
            continue
        else:
            new_sev = "ok"
            entity_name = a["name"]
            detail = ""

        state.emit_on_transition(
            monitor="productivity_outliers",
            entity_id=aid,
            new_severity=new_sev,
            entity_name=entity_name,
            detail_for_new_severity=detail,
        )

    state.update_monitor("productivity_outliers", "ok", data={
        "team_size": len(eligible),
        "team_mean_calls_per_hour": round(mean, 1),
        "team_stddev": round(stddev, 2),
        "top": top, "bottom": bottom,
        "threshold_stddev": OUTLIER_STDDEV_THRESHOLD,
        "excluded_count": len(excluded),
        "excluded_names": [e["name"] for e in excluded][:10],
        "exclude_patterns": OUTLIER_EXCLUDE_PATTERNS,
    })


# ---------------------------------------------------------------------------
# Monitor 08: Talk time anomalies
# Detects agents whose talk_sec_pt is unusually high (stuck on a call) or
# unusually low (barely talking despite being logged in).
# ---------------------------------------------------------------------------

def monitor_talk_time_anomalies(cache):
    try:
        agents = fetch_agent_performance(cache)
    except ConvosoAPIError as e:
        state.update_monitor("talk_time_anomalies", "error", error=str(e))
        return

    high_talkers = []    # potentially stuck on call
    quiet_talkers = []   # below floor of acceptable
    excluded_count = 0

    for agent_id, stats in agents.items():
        if not isinstance(stats, dict):
            continue
        name = stats.get("name") or f"Agent {agent_id}"
        talk_pct = _to_number(stats.get("talk_sec_pt"))
        total_sec = parse_hms(stats.get("total_time"))
        calls = _to_number(stats.get("calls")) or 0
        if talk_pct is None or total_sec is None:
            continue
        if total_sec < TALK_TIME_MIN_LOGGED_SECONDS:
            continue
        if is_excluded_agent(name):
            excluded_count += 1
            continue

        severity = "ok"
        category = None
        if talk_pct >= TALK_TIME_HIGH_CRIT:
            severity, category = "critical", "high"
        elif talk_pct >= TALK_TIME_HIGH_WARN:
            severity, category = "warning", "high"
        elif talk_pct <= TALK_TIME_LOW_CRIT:
            severity, category = "critical", "low"
        elif talk_pct <= TALK_TIME_LOW_WARN:
            severity, category = "warning", "low"

        state.emit_on_transition(
            monitor="talk_time_anomalies",
            entity_id=agent_id,
            new_severity=severity,
            entity_name=(f"Stuck on call: {name}" if category == "high"
                         else f"Barely talking: {name}" if category == "low"
                         else name),
            detail_for_new_severity=(
                f"{talk_pct:.1f}% of logged-in time on call ({int(calls)} calls)."
                if category == "high"
                else f"Only {talk_pct:.1f}% talk time across {int(calls)} calls."
                if category == "low"
                else ""
            ),
        )

        if severity in ("warning", "critical"):
            entry = {
                "user_id": agent_id,
                "name": name,
                "talk_pct": talk_pct,
                "calls": int(calls),
                "total_time": stats.get("total_time"),
                "severity": severity,
            }
            if category == "high":
                high_talkers.append(entry)
            else:
                quiet_talkers.append(entry)

    state.update_monitor("talk_time_anomalies", "ok", data={
        "high_talkers": sorted(high_talkers, key=lambda x: -x["talk_pct"])[:10],
        "quiet_talkers": sorted(quiet_talkers, key=lambda x: x["talk_pct"])[:10],
        "high_warn_threshold": TALK_TIME_HIGH_WARN,
        "high_crit_threshold": TALK_TIME_HIGH_CRIT,
        "low_warn_threshold": TALK_TIME_LOW_WARN,
        "low_crit_threshold": TALK_TIME_LOW_CRIT,
        "excluded_count": excluded_count,
        "total_flagged": len(high_talkers) + len(quiet_talkers),
    })


# ---------------------------------------------------------------------------
# Monitor 09: Pause time anomalies
# Detects agents whose pause_sec_pt is excessive (long breaks / AFK).
# ---------------------------------------------------------------------------

def monitor_pause_time_anomalies(cache):
    try:
        agents = fetch_agent_performance(cache)
    except ConvosoAPIError as e:
        state.update_monitor("pause_time_anomalies", "error", error=str(e))
        return

    flagged = []
    excluded_count = 0

    for agent_id, stats in agents.items():
        if not isinstance(stats, dict):
            continue
        name = stats.get("name") or f"Agent {agent_id}"
        pause_pct = _to_number(stats.get("pause_sec_pt"))
        total_sec = parse_hms(stats.get("total_time"))
        if pause_pct is None or total_sec is None:
            continue
        if total_sec < PAUSE_TIME_MIN_LOGGED_SECONDS:
            continue
        if is_excluded_agent(name):
            excluded_count += 1
            continue

        severity = "ok"
        if pause_pct >= PAUSE_TIME_CRIT:
            severity = "critical"
        elif pause_pct >= PAUSE_TIME_WARN:
            severity = "warning"

        state.emit_on_transition(
            monitor="pause_time_anomalies",
            entity_id=agent_id,
            new_severity=severity,
            entity_name=f"Extended break: {name}",
            detail_for_new_severity=(
                f"{pause_pct:.1f}% of logged-in time in pause state."
            ),
        )

        if severity in ("warning", "critical"):
            flagged.append({
                "user_id": agent_id,
                "name": name,
                "pause_pct": pause_pct,
                "total_time": stats.get("total_time"),
                "severity": severity,
            })

    state.update_monitor("pause_time_anomalies", "ok", data={
        "flagged": sorted(flagged, key=lambda x: -x["pause_pct"])[:10],
        "total_flagged": len(flagged),
        "warn_threshold": PAUSE_TIME_WARN,
        "crit_threshold": PAUSE_TIME_CRIT,
        "excluded_count": excluded_count,
    })


# ---------------------------------------------------------------------------
# Monitor 10: Claude AI briefing
# ---------------------------------------------------------------------------

BRIEFING_SYSTEM_PROMPT = """You are a senior call center operations analyst writing a real-time briefing for an operations manager at a health insurance call center.

You will be given a snapshot of the current dialer state plus recent history. Write a concise briefing (2-3 short paragraphs, total ~120-180 words) in flowing editorial prose.

Cover, in order:
1. One sentence on overall operational health
2. The most urgent issue(s) requiring attention, with context for why they matter
3. Notable trends or patterns in the recent data, with brief interpretation
4. ONE specific recommended action

Style:
- Professional, calm, like a daily news brief
- Flowing prose only - no bullets, headers, lists, or markdown
- Be specific with numbers when relevant ("4,400 calls dialed", not "many calls")
- Apply call center domain knowledge to interpret what numbers MEAN. For example: rising AM ratio + steady dialing volume often signals stale list data or DID flagging. High wait time across multiple agents usually means dialer pacing issue, not agent behavior.
- Look for relationships BETWEEN metrics
- Don't fabricate facts not in the data
- If the situation is genuinely fine, say so plainly - don't manufacture concerns
- Avoid clichés ("firing on all cylinders", "running smoothly", "as expected")
- Don't preface with "based on the data" or similar - just write the analysis

Output ONLY the briefing prose. No greeting, no signature, no preamble."""


def build_briefing_context(snapshot):
    """Format the dashboard state into a structured prompt for Claude."""
    monitors = snapshot.get("monitors", {})
    history = snapshot.get("history", {})

    parts = []

    # Time
    ts = snapshot.get("last_poll_at") or datetime.now(timezone.utc).isoformat()
    parts.append(f"Time: {ts}")

    # Campaigns
    c = monitors.get("campaign_pauses", {}).get("data") or {}
    if c:
        names_active = [x["name"] for x in c.get("campaigns", []) if x.get("active")]
        names_paused = [x["name"] for x in c.get("campaigns", []) if not x.get("active")]
        parts.append(
            f"\nCAMPAIGNS: {c.get('active', 0)} active of {c.get('total', 0)} total."
            + (f"\n  Active: {', '.join(names_active)}." if names_active else "")
            + (f"\n  Paused: {', '.join(names_paused)}." if names_paused else "")
        )

    # Dialing
    n = monitors.get("non_connect_rate", {}).get("data") or {}
    if n and n.get("total_calls"):
        parts.append(
            f"\nDIALING TODAY: {n['total_calls']:,} calls."
            f"\n  Live answers: {n.get('human_answered', 0):,}"
            f" ({(n.get('human_answered', 0)/n['total_calls']*100):.1f}%)"
            f"\n  Answering machine: {n.get('am', 0):,}"
            f" ({(n.get('am', 0)/n['total_calls']*100):.1f}%)"
            f"\n  Non-connect: {n.get('non_connects', 0):,}"
            f" ({n.get('rate', 0)*100:.1f}%)"
            f"\n  Agents reporting: {n.get('agents_counted', 0)}"
        )

    # Agents
    i = monitors.get("idle_agents", {}).get("data") or {}
    if i:
        parts.append(
            f"\nAGENTS ON FLOOR: {i.get('logged_in', 0)} logged in,"
            f" {i.get('active', 0)} active, {i.get('idle', 0)} idle (>90% wait time)."
        )
        if i.get("idle_list"):
            idle_names = [f"{x['name']} ({x['wait_pct']:.0f}%)" for x in i["idle_list"][:5]]
            parts.append(f"  Idle: {', '.join(idle_names)}")

    # Hopper
    h = monitors.get("hopper", {}).get("data") or {}
    if h and isinstance(h.get("lists"), list):
        critical = [L for L in h["lists"] if L.get("severity") == "critical"]
        warning = [L for L in h["lists"] if L.get("severity") == "warning"]
        if critical or warning:
            parts.append(
                f"\nLIST HEALTH: {h.get('active_total', len(h['lists']))} active lists."
                f"\n  Critically depleted ({len(critical)}): "
                + (", ".join(f"{L['list_name']} ({L['remaining']} leads)"
                             for L in critical[:5])
                   if critical else "none")
                + (f"\n  Warning ({len(warning)}): "
                   + ", ".join(f"{L['list_name']} ({L['remaining']} leads)"
                               for L in warning[:5])
                   if warning else "")
            )

    # Connect rate by list
    cr = monitors.get("connect_rate_by_list", {}).get("data") or {}
    if cr.get("lists"):
        evaluated = [L for L in cr["lists"]
                     if L.get("rate") is not None and L.get("calls", 0) >= cr.get("min_calls_to_evaluate", 0)]
        if evaluated:
            worst = evaluated[0]
            parts.append(
                f"\nCONNECT RATE: lowest is {worst['list_name']} at "
                f"{worst['rate']*100:.1f}% ({worst.get('calls', 0)} calls)."
                f" Lists checked: {cr.get('checked_so_far', 0)}/{cr.get('active_total', 0)}."
            )

    # AM drift
    a = monitors.get("am_ratio_drift", {}).get("data") or {}
    if a and a.get("current") is not None:
        if a.get("baseline") is not None:
            parts.append(
                f"\nAM RATIO: {a['current']*100:.1f}% currently,"
                f" {a['baseline']*100:.1f}% rolling baseline"
                f" (ratio {a.get('ratio_to_baseline', 1):.2f}x)."
            )
        else:
            parts.append(f"\nAM RATIO: {a['current']*100:.1f}% (baseline still building).")

    # Outliers
    o = monitors.get("productivity_outliers", {}).get("data") or {}
    if o.get("team_mean_calls_per_hour"):
        underperformers = o.get("bottom", []) or []
        top = o.get("top", []) or []
        parts.append(
            f"\nPRODUCTIVITY: team avg {o['team_mean_calls_per_hour']} calls/hr"
            f" across {o.get('team_size', 0)} dialers."
            + (f"\n  Underperformers: " + ", ".join(
                f"{a['name']} ({a['calls_per_hour']} c/hr)"
                for a in underperformers
            ) if underperformers else "")
            + (f"\n  Top performers: " + ", ".join(
                f"{a['name']} ({a['calls_per_hour']} c/hr)"
                for a in top
            ) if top else "")
        )

    # Talk time anomalies
    t = monitors.get("talk_time_anomalies", {}).get("data") or {}
    if t.get("total_flagged"):
        stuck = t.get("high_talkers", [])
        quiet = t.get("quiet_talkers", [])
        if stuck:
            parts.append(
                "\nPOSSIBLY STUCK ON CALL: "
                + ", ".join(f"{a['name']} ({a['talk_pct']:.0f}% talk)" for a in stuck[:5])
            )
        if quiet:
            parts.append(
                "\nBARELY TALKING: "
                + ", ".join(f"{a['name']} ({a['talk_pct']:.1f}% talk)" for a in quiet[:5])
            )

    # Pause time
    p = monitors.get("pause_time_anomalies", {}).get("data") or {}
    if p.get("total_flagged"):
        flagged = p.get("flagged", [])
        parts.append(
            "\nEXTENDED BREAKS: "
            + ", ".join(f"{a['name']} ({a['pause_pct']:.0f}% pause)" for a in flagged[:5])
        )

    # Trends - describe direction over the available history
    def describe_series(name, label, fmt=lambda v: f"{v:.2f}"):
        series = history.get(name, [])
        if len(series) < 3:
            return None
        values = [p.get("value") for p in series if p.get("value") is not None]
        if len(values) < 3:
            return None
        first_third = statistics.mean(values[:len(values) // 3]) if len(values) >= 6 else values[0]
        last_third = statistics.mean(values[-(len(values) // 3):]) if len(values) >= 6 else values[-1]
        change = last_third - first_third
        latest = values[-1]
        direction = "stable"
        if first_third > 0.0001 and abs(change / first_third) > 0.10:
            direction = "rising" if change > 0 else "falling"
        return (f"{label}: latest {fmt(latest)}, "
                f"{direction} over last ~30min "
                f"(early avg {fmt(first_third)}, recent avg {fmt(last_third)}).")

    trend_lines = []
    nc = describe_series("non_connect_rate", "Non-connect rate", lambda v: f"{v*100:.1f}%")
    if nc: trend_lines.append(nc)
    am = describe_series("am_ratio", "AM ratio", lambda v: f"{v*100:.1f}%")
    if am: trend_lines.append(am)
    ag = describe_series("logged_in_agents", "Logged-in agents", lambda v: f"{int(v)}")
    if ag: trend_lines.append(ag)
    if trend_lines:
        parts.append("\nTRENDS (last 30 min):\n  " + "\n  ".join(trend_lines))

    return "\n".join(parts)


def generate_claude_briefing():
    """Background job: snapshot state, call Claude, store the briefing."""
    if not claude:
        state.set_briefing(error="Anthropic API key not configured.")
        return
    try:
        snapshot = state.snapshot()

        # Skip if monitors haven't completed a successful poll yet
        if not snapshot.get("last_poll_at"):
            log.info("Briefing skipped - no monitor poll yet")
            return

        # Skip if all monitors are still pending/errored
        ok_count = sum(
            1 for m in snapshot.get("monitors", {}).values()
            if m.get("status") == "ok"
        )
        if ok_count == 0:
            log.info("Briefing skipped - no monitors in ok state")
            return

        state.set_briefing(generating=True)
        context = build_briefing_context(snapshot)
        log.info("Generating Claude briefing (context: %d chars)", len(context))

        response = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=600,
            system=BRIEFING_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": context}],
        )
        text = response.content[0].text.strip()
        state.set_briefing(text=text, generating=False)
        log.info("Briefing generated (%d chars)", len(text))
    except Exception as e:
        log.exception("Briefing generation failed")
        state.set_briefing(error=f"Briefing generation failed: {e}", generating=False)


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
    ("talk_time_anomalies",    monitor_talk_time_anomalies),
    ("pause_time_anomalies",   monitor_pause_time_anomalies),
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
                  max_instances=1, coalesce=True,
                  id="monitors")
    sched.add_job(generate_claude_briefing, "interval",
                  minutes=BRIEFING_INTERVAL_MINUTES,
                  next_run_time=datetime.now() + timedelta(seconds=90),
                  max_instances=1, coalesce=True,
                  id="briefing")
    sched.start()
    log.info("Scheduler started: monitors=%ss briefing=%smin",
             POLL_INTERVAL_SECONDS, BRIEFING_INTERVAL_MINUTES)


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
            "POST /webhook":              "Receives Convoso disposition webhooks (Phase 1)",
            "GET /dashboard":             "Live operations dashboard (Phase 2)",
            "GET /api/state":             "JSON state snapshot used by dashboard",
            "GET /api/diagnose":          "Probes Convoso endpoints",
            "GET /api/probe":             "Inspect a single Convoso endpoint",
            "POST /api/poll":             "Force an immediate monitor poll",
            "POST /api/briefing/refresh": "Force an immediate Claude briefing regeneration",
            "GET /":                      "This health check",
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


@app.route("/api/briefing/refresh", methods=["POST", "GET"])
def api_briefing_refresh():
    """Trigger a Claude briefing regeneration on demand. Runs in a thread so
    the request returns immediately."""
    t = threading.Thread(target=generate_claude_briefing, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Briefing regeneration started",
                    "current": state.get_briefing()})


@app.route("/api/briefing/preview-context", methods=["GET"])
def api_briefing_preview():
    """Returns the structured context that would be sent to Claude (for
    debugging the prompt)."""
    return jsonify({"context": build_briefing_context(state.snapshot())})


if __name__ == "__main__":
    start_scheduler()
    _scheduler_started = True
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
