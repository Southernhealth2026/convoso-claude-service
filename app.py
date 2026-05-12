"""
Convoso Claude Operations Center

Phase 1: Receives Convoso disposition webhooks, generates Claude summaries.
Phase 2: Live ops dashboard with 7 monitors at /dashboard.

Stage 1 monitors:
  - Campaign continuity   (via /campaigns/search)
  - Non-connect rate      (via /agent-performance/search)
  - List depth (hopper)   (via /lists/search + /leads/search per list)

Stage 2 monitors:
  - Idle agents           (via /agent-performance/search wait_sec_pt)
  - Connect rate by list  (via /agent-performance/search with list_ids)
  - AM ratio drift        (via /agent-performance/search + history)
  - Productivity outliers (via /agent-performance/search; excludes inbound)

Polish (v0.6):
  - Inbound agents (Custo / Manager / Sup) excluded from outliers
  - Anomalies fire only on state transitions, not every poll
  - Resolved transitions emit "info" anomalies for visibility
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

# Inbound / non-dialer name patterns. Case-insensitive substring match.
# Override with comma-separated patterns in OUTLIER_EXCLUDE_PATTERNS env var.
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
# State store with transition tracking
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
        self.history = {
            "non_connect_rate": deque(maxlen=HISTORY_MAX_POINTS),
            "am_ratio":         deque(maxlen=HISTORY_MAX_POINTS),
            "active_campaigns": deque(maxlen=HISTORY_MAX_POINTS),
            "logged_in_agents": deque(maxlen=HISTORY_MAX_POINTS),
        }
        # Per-monitor entity severity tracking for state-transition anomalies.
        # Shape: {monitor_name: {entity_id: severity_string}}
        self.entity_severity = {}

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
        """Emit anomaly only when an entity's severity changes. Resolutions
        (warning/critical -> ok) emit an 'info' anomaly so the operator sees
        the closure event. Returns True if an anomaly was emitted."""
        prev_severity = self.get_entity_severity(monitor, entity_id)
        if prev_severity == new_severity:
            return False
        self.set_entity_severity(monitor, entity_id, new_severity)

        # Going to a worse state - emit an alert at that severity
        if new_severity in ("warning", "critical"):
            self.add_anomaly(
                severity=new_severity, monitor=monitor,
                title=f"{entity_name}",
                detail=detail_for_new_severity,
                fingerprint=f"{monitor}:{entity_id}:{new_severity}",
            )
            return True

        # Returning to ok from a non-ok state - emit a resolution event
        if new_severity == "ok" and prev_severity in ("warning", "critical"):
            self.add_anomaly(
                severity="info", monitor=monitor,
                title=f"Resolved: {entity_name}",
                detail=f"Returned to normal from {prev_severity}.",
                fingerprint=f"{monitor}:{entity_id}:resolved",
            )
            return True

        return False

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
    """True if the agent name matches any of the exclude patterns
    (inbound, custo, manager, etc.)."""
    if not name:
        return False
    name_lower = name.lower()
    return any(p in name_lower for p in OUTLIER_EXCLUDE_PATTERNS)


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

    # Use transition tracking for pause/resume events
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

    # Single-entity transition: the whole team's non-connect rate.
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
    checked_ids = set()

    for L in checked:
        list_id = L.get("id")
        list_name = L.get("name") or f"List {list_id}"
        checked_ids.add(str(list_id))
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
            # No data == treat as ok severity
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
            f"Answering-machine ratio is "
            f"{ratio_to_baseline:.2f}x the recent baseline."
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
# Monitor 07: Productivity outliers (with inbound filter)
# ---------------------------------------------------------------------------

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

    # Clear previous outlier transitions for agents that are no longer eligible
    # so resolved events fire when they leave the cohort.
    previous_severities = state.all_entity_severities("productivity_outliers")
    eligible_ids = {str(a["user_id"]) for a in eligible}
    for prev_id, prev_sev in previous_severities.items():
        if prev_id not in eligible_ids and prev_sev in ("warning", "critical"):
            # Agent no longer in cohort - resolve
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
            # Top performers are "info" - we track them with a special severity
            # but emit only on entry, not as warning/critical.
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
