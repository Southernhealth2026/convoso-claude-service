"""
Convoso Claude Operations Center

Phase 1: Receives Convoso disposition webhooks, generates Claude summaries,
         posts back to lead Notes field.
Phase 2: Monitors Convoso operations every 30 seconds, surfaces anomalies on
         a live dashboard at /dashboard.

Stage 1 monitors:
  - Campaign pauses     (via /campaigns/search)
  - Drop rate spikes    (via /call-logs/search, counting status=DROP)
  - Hopper / list depth (via /lists/search)
"""

import json
import logging
import os
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

DROP_RATE_WARN_THRESHOLD = float(os.environ.get("DROP_RATE_WARN_THRESHOLD", "0.04"))
DROP_RATE_CRIT_THRESHOLD = float(os.environ.get("DROP_RATE_CRIT_THRESHOLD", "0.06"))

HOPPER_WARN_PCT = float(os.environ.get("HOPPER_WARN_PCT", "0.20"))
HOPPER_CRIT_PCT = float(os.environ.get("HOPPER_CRIT_PCT", "0.05"))

# Drop rate look-back window in minutes
DROP_RATE_WINDOW_MINUTES = int(os.environ.get("DROP_RATE_WINDOW_MINUTES", "60"))

if not ANTHROPIC_API_KEY:
    log.warning("ANTHROPIC_API_KEY is not set!")
if not CONVOSO_AUTH_TOKEN:
    log.warning("CONVOSO_AUTH_TOKEN is not set!")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ---------------------------------------------------------------------------
# In-memory state store
# ---------------------------------------------------------------------------

class StateStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.last_poll_at = None
        self.last_poll_status = "pending"
        self.monitors = {
            "campaign_pauses": {"status": "pending", "data": None, "error": None},
            "drop_rate":       {"status": "pending", "data": None, "error": None},
            "hopper":          {"status": "pending", "data": None, "error": None},
        }
        self.anomalies = deque(maxlen=200)
        self.previous = {}

    def update_monitor(self, name, status, data=None, error=None):
        with self.lock:
            self.monitors[name] = {"status": status, "data": data, "error": error}

    def add_anomaly(self, severity, monitor, title, detail, fingerprint=None):
        with self.lock:
            now = datetime.now(timezone.utc).isoformat()
            if fingerprint:
                cutoff = time.time() - 300
                for a in list(self.anomalies)[:20]:
                    if a.get("fingerprint") == fingerprint and a.get("ts_epoch", 0) > cutoff:
                        return
            self.anomalies.appendleft({
                "ts": now,
                "ts_epoch": time.time(),
                "severity": severity,
                "monitor": monitor,
                "title": title,
                "detail": detail,
                "fingerprint": fingerprint,
            })

    def snapshot(self):
        with self.lock:
            return {
                "last_poll_at": self.last_poll_at,
                "last_poll_status": self.last_poll_status,
                "monitors": dict(self.monitors),
                "anomalies": list(self.anomalies)[:50],
            }


state = StateStore()


# ---------------------------------------------------------------------------
# Convoso API client
# ---------------------------------------------------------------------------

class ConvosoAPIError(Exception):
    pass


def convoso_post(path, payload=None, timeout=20):
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
    try:
        return resp.json()
    except ValueError:
        raise ConvosoAPIError(f"Convoso returned non-JSON for {path}: {resp.text[:200]}")


# ---------------------------------------------------------------------------
# Phase 1: webhook handler
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
# Phase 2: monitors
# ---------------------------------------------------------------------------

def fetch_campaigns():
    """Fetch the campaign list. Verified to work via /campaigns/search."""
    body = convoso_post("/campaigns/search", {"limit": 200, "offset": 0})
    if not body.get("success"):
        raise ConvosoAPIError(
            f"campaigns/search failed: code={body.get('code')} text={body.get('text')}"
        )
    data = body.get("data") or {}
    if isinstance(data, dict):
        # Convoso usually wraps in {"entries": [...]} but we tolerate flat dict too
        entries = data.get("entries")
        if entries is None:
            # data is a dict keyed by id - take its values
            entries = list(data.values())
    elif isinstance(data, list):
        entries = data
    else:
        entries = []
    return entries


def monitor_campaign_pauses():
    try:
        campaigns = fetch_campaigns()
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
            status_val = str(c.get("status") or "").lower()
            active = status_val in ("active", "1", "true", "running")
        summary.append({"id": cid, "name": name, "active": bool(active)})

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

    state.previous["campaign_pauses"] = {"campaigns": summary}
    state.update_monitor("campaign_pauses", "ok", data={
        "total": len(summary),
        "active": sum(1 for c in summary if c["active"]),
        "paused": sum(1 for c in summary if not c["active"]),
        "campaigns": summary,
    })


def fetch_call_logs_count(start_time, end_time, status=None):
    """Query /call-logs/search and return (count_in_window, raw_response_total).

    Convoso may return a 'total' field in the envelope. If it does we use that.
    Otherwise we count returned entries (capped at limit). For accurate counts
    we set limit=500 (the max Convoso allows).
    """
    payload = {
        "start_time": start_time,
        "end_time": end_time,
        "limit": 500,
        "offset": 0,
        "order": "DESC",
    }
    if status:
        payload["status"] = status
    body = convoso_post("/call-logs/search", payload)
    if not body.get("success"):
        raise ConvosoAPIError(
            f"call-logs/search failed: code={body.get('code')} text={body.get('text')}"
        )
    # Try a top-level 'total' first
    total = body.get("total")
    if total is not None:
        try:
            return int(total)
        except (TypeError, ValueError):
            pass
    # Fall back to counting entries in data
    data = body.get("data") or {}
    if isinstance(data, dict):
        entries = data.get("entries") or list(data.values())
    elif isinstance(data, list):
        entries = data
    else:
        entries = []
    return len(entries)


def monitor_drop_rate():
    """Compute drop rate for the last DROP_RATE_WINDOW_MINUTES.

    Strategy: query call-logs twice -
      1. with status=DROP -> drop count
      2. without status filter -> total count
    Convoso uses 24h server time format 'YYYY-MM-DD HH:MM:SS'.
    """
    now = datetime.utcnow()    # Convoso assumes server-local; UTC is safe enough for ratios
    start = now - timedelta(minutes=DROP_RATE_WINDOW_MINUTES)
    fmt = "%Y-%m-%d %H:%M:%S"
    start_s = start.strftime(fmt)
    end_s = now.strftime(fmt)

    try:
        drops = fetch_call_logs_count(start_s, end_s, status="DROP")
        total = fetch_call_logs_count(start_s, end_s, status=None)
    except ConvosoAPIError as e:
        state.update_monitor("drop_rate", "error", error=str(e))
        return

    if total == 0:
        state.update_monitor("drop_rate", "ok", data={
            "rate": 0.0,
            "drops": 0,
            "total_calls": 0,
            "warn_threshold": DROP_RATE_WARN_THRESHOLD,
            "crit_threshold": DROP_RATE_CRIT_THRESHOLD,
            "severity": "ok",
            "window_minutes": DROP_RATE_WINDOW_MINUTES,
            "note": "No calls in the look-back window.",
        })
        return

    rate = float(drops) / float(total)
    severity = "ok"
    if rate >= DROP_RATE_CRIT_THRESHOLD:
        severity = "critical"
    elif rate >= DROP_RATE_WARN_THRESHOLD:
        severity = "warning"

    if severity in ("warning", "critical"):
        state.add_anomaly(
            severity=severity, monitor="drop_rate",
            title=f"Drop rate at {rate*100:.2f}%",
            detail=f"{drops} drops out of {total} calls in last {DROP_RATE_WINDOW_MINUTES}m.",
            fingerprint=f"drop_rate:{severity}",
        )

    state.update_monitor("drop_rate", "ok", data={
        "rate": rate,
        "drops": drops,
        "total_calls": total,
        "warn_threshold": DROP_RATE_WARN_THRESHOLD,
        "crit_threshold": DROP_RATE_CRIT_THRESHOLD,
        "severity": severity,
        "window_minutes": DROP_RATE_WINDOW_MINUTES,
    })


def fetch_lists():
    body = convoso_post("/lists/search", {"limit": 200, "offset": 0})
    if not body.get("success"):
        raise ConvosoAPIError(
            f"lists/search failed: code={body.get('code')} text={body.get('text')}"
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
    return entries


# Field names Convoso may use for "leads available to dial" and "total leads"
REMAINING_FIELD_NAMES = [
    "leads_remaining", "remaining", "available", "available_leads",
    "leads_available", "hopper_count", "active_leads", "callable_leads",
]
TOTAL_FIELD_NAMES = [
    "leads_total", "total_leads", "total", "list_size", "lead_count",
    "size", "count",
]


def first_present(d, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k], k
    return None, None


def monitor_hopper_depletion():
    try:
        lists = fetch_lists()
    except ConvosoAPIError as e:
        state.update_monitor("hopper", "error", error=str(e))
        return

    if not lists:
        state.update_monitor("hopper", "ok", data={
            "lists": [],
            "note": "No lists returned.",
        })
        return

    hopper_summary = []
    used_remaining_field = None
    used_total_field = None

    for L in lists:
        if not isinstance(L, dict):
            continue

        # Skip inactive lists if the field is present and false
        active = L.get("active") if "active" in L else L.get("is_active", True)
        if active is False:
            continue

        list_id = L.get("id") or L.get("list_id")
        list_name = L.get("name") or L.get("list_name") or f"List {list_id}"

        remaining_val, remaining_field = first_present(L, REMAINING_FIELD_NAMES)
        total_val, total_field = first_present(L, TOTAL_FIELD_NAMES)

        if remaining_val is None or total_val is None:
            continue

        used_remaining_field = used_remaining_field or remaining_field
        used_total_field = used_total_field or total_field

        try:
            remaining = float(remaining_val)
            total = float(total_val)
        except (TypeError, ValueError):
            continue

        pct = (remaining / total) if total > 0 else 0.0
        severity = "ok"
        if pct <= HOPPER_CRIT_PCT:
            severity = "critical"
        elif pct <= HOPPER_WARN_PCT:
            severity = "warning"

        if severity in ("warning", "critical"):
            state.add_anomaly(
                severity=severity, monitor="hopper",
                title=f"List depleting: {list_name}",
                detail=f"{int(remaining)} of {int(total)} leads remaining ({pct*100:.1f}%).",
                fingerprint=f"hopper:{list_id}:{severity}",
            )

        hopper_summary.append({
            "list_id": list_id,
            "list_name": list_name,
            "remaining": int(remaining),
            "total": int(total),
            "pct": pct,
            "severity": severity,
        })

    if not hopper_summary:
        # We got lists back but none had recognizable count fields.
        # Surface a few sample keys so we can patch the field-name list.
        sample_keys = list(lists[0].keys()) if isinstance(lists[0], dict) else []
        state.update_monitor("hopper", "needs_setup", error=(
            "Lists returned but no recognizable lead-count fields. "
            f"Sample list keys: {sample_keys[:30]}"
        ))
        return

    state.update_monitor("hopper", "ok", data={
        "lists": hopper_summary,
        "remaining_field": used_remaining_field,
        "total_field": used_total_field,
    })


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

def run_all_monitors():
    started = time.time()
    log.info("Monitor poll cycle starting")
    statuses = []
    for name, fn in [
        ("campaign_pauses", monitor_campaign_pauses),
        ("drop_rate",       monitor_drop_rate),
        ("hopper",          monitor_hopper_depletion),
    ]:
        try:
            fn()
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
            "POST /webhook":      "Receives Convoso disposition webhooks (Phase 1)",
            "GET /dashboard":     "Live operations dashboard (Phase 2)",
            "GET /api/state":     "JSON state snapshot used by dashboard",
            "GET /api/diagnose":  "Probes Convoso endpoints to verify availability",
            "GET /api/probe":     "Inspect a single Convoso endpoint response shape",
            "POST /api/poll":     "Force an immediate monitor poll",
            "GET /":              "This health check",
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
        ("/users/search",              {"limit": 1, "offset": 0}),
        ("/call-logs/search",          {"limit": 1, "offset": 0}),
        ("/agent-performance/search",  {}),
        ("/agent-productivity/search", {"limit": 1, "offset": 0}),
        ("/agent-monitor/search",      {}),
        ("/queues/search",             {"limit": 1, "offset": 0}),
        ("/dnc/search",                {"limit": 1, "offset": 0}),
        ("/statuses/search",           {"limit": 1, "offset": 0}),
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
    """Probe a single Convoso endpoint and return the raw JSON.

    Use ?path=/lists/search to inspect what fields come back.
    Useful when monitors say 'needs_setup' so we can map field names.
    """
    path = request.args.get("path")
    if not path:
        return jsonify({"error": "Pass ?path=/some/endpoint"}), 400
    if not path.startswith("/"):
        path = "/" + path
    try:
        body = convoso_post(path, {"limit": 3, "offset": 0})
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
