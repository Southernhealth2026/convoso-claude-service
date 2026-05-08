"""
Convoso Claude Operations Center

Phase 1: Receives Convoso disposition webhooks, generates Claude summaries,
         posts back to lead Notes field.
Phase 2: Monitors Convoso operations every 30 seconds, surfaces anomalies on
         a live dashboard at /dashboard.

Stage 1 monitors:
  - Campaign pauses
  - Drop rate spikes
  - Hopper depletion
"""

import json
import logging
import os
import threading
import time
import traceback
import urllib.parse
from datetime import datetime, timezone
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

# How often the background poller runs
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))

# Drop rate threshold - if a campaign exceeds this, it's flagged
DROP_RATE_WARN_THRESHOLD = float(os.environ.get("DROP_RATE_WARN_THRESHOLD", "0.04"))   # 4%
DROP_RATE_CRIT_THRESHOLD = float(os.environ.get("DROP_RATE_CRIT_THRESHOLD", "0.06"))   # 6%

# Hopper depletion thresholds (percentage of leads remaining)
HOPPER_WARN_PCT = float(os.environ.get("HOPPER_WARN_PCT", "0.20"))  # under 20% remaining
HOPPER_CRIT_PCT = float(os.environ.get("HOPPER_CRIT_PCT", "0.05"))  # under 5% remaining

if not ANTHROPIC_API_KEY:
    log.warning("ANTHROPIC_API_KEY is not set!")
if not CONVOSO_AUTH_TOKEN:
    log.warning("CONVOSO_AUTH_TOKEN is not set!")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ---------------------------------------------------------------------------
# In-memory state store
#
# Holds the latest snapshot for each monitor plus a rolling anomaly feed.
# Render free tier has ephemeral disk, so we keep state in memory. State
# is rebuilt on each poll cycle, which is fine - we only need the last
# value to detect changes.
# ---------------------------------------------------------------------------

class StateStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.last_poll_at = None
        self.last_poll_status = "pending"   # pending | ok | partial | error
        self.monitors = {
            "campaign_pauses": {"status": "pending", "data": None, "error": None},
            "drop_rate":       {"status": "pending", "data": None, "error": None},
            "hopper":          {"status": "pending", "data": None, "error": None},
        }
        # Rolling feed of detected anomalies, newest first
        self.anomalies = deque(maxlen=200)
        # Previous snapshots for diff detection
        self.previous = {}

    def update_monitor(self, name, status, data=None, error=None):
        with self.lock:
            self.monitors[name] = {"status": status, "data": data, "error": error}

    def add_anomaly(self, severity, monitor, title, detail, fingerprint=None):
        """Add an anomaly to the feed. fingerprint dedupes recent identical
        events so we don't spam the feed every 30 seconds."""
        with self.lock:
            now = datetime.now(timezone.utc).isoformat()
            if fingerprint:
                # Suppress if same fingerprint added in last 5 minutes
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
# Convoso API client (lightweight)
# ---------------------------------------------------------------------------

class ConvosoAPIError(Exception):
    pass


def convoso_post(path, payload=None, timeout=20):
    """POST to a Convoso endpoint with auth_token and return the parsed body.
    Raises ConvosoAPIError on auth/transport failure, but returns the body
    even when success=false so callers can inspect Convoso's error code."""
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
        parsed = resp.json()
    except ValueError:
        raise ConvosoAPIError(f"Convoso returned non-JSON for {path}: {resp.text[:200]}")

    return parsed


# ---------------------------------------------------------------------------
# Phase 1: webhook handler (unchanged from working version)
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
        model=CLAUDE_MODEL,
        max_tokens=300,
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
#
# Each monitor is a function that:
#   1. Fetches state from Convoso
#   2. Updates the StateStore monitor entry
#   3. Compares to previous snapshot and emits anomalies on change
#
# We try several known Convoso endpoint paths and use whichever works.
# Convoso's API surface varies by account; if we don't recognize the
# response shape we surface that gracefully on the dashboard.
# ---------------------------------------------------------------------------

def fetch_campaigns():
    """Try a few likely endpoints to list campaigns. Returns a list of
    campaign dicts or raises if all attempts fail."""
    last_err = None
    for path in ["/campaigns/search", "/campaigns/list", "/campaigns"]:
        try:
            body = convoso_post(path, {"limit": 200, "offset": 0})
        except ConvosoAPIError as e:
            last_err = str(e)
            continue
        if not body.get("success"):
            last_err = f"{path}: {body.get('text') or body.get('code')}"
            continue
        data = body.get("data") or {}
        entries = data.get("entries") if isinstance(data, dict) else None
        if entries is None and isinstance(data, list):
            entries = data
        if entries is None:
            last_err = f"{path}: unexpected response shape"
            continue
        return entries
    raise ConvosoAPIError(f"Could not fetch campaigns. Last error: {last_err}")


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
        # Convoso can use 'status', 'active', or 'is_active' depending on response
        active = c.get("active")
        if active is None:
            active = c.get("is_active")
        if active is None:
            status_val = (c.get("status") or "").lower()
            active = status_val in ("active", "1", "true", "running")
        summary.append({
            "id": cid,
            "name": name,
            "active": bool(active),
        })

    # Diff against previous to detect newly paused campaigns
    prev = state.previous.get("campaign_pauses") or {}
    prev_by_id = {c["id"]: c for c in prev.get("campaigns", [])}
    for c in summary:
        prev_state = prev_by_id.get(c["id"])
        if prev_state is None:
            continue
        if prev_state["active"] and not c["active"]:
            state.add_anomaly(
                severity="warning",
                monitor="campaign_pauses",
                title=f"Campaign paused: {c['name']}",
                detail=f"Campaign id {c['id']} transitioned from active to paused.",
                fingerprint=f"campaign_paused:{c['id']}",
            )
        elif not prev_state["active"] and c["active"]:
            state.add_anomaly(
                severity="info",
                monitor="campaign_pauses",
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


def fetch_drop_rate_stats():
    """Try to fetch drop-rate-relevant stats. We look at recent call
    dispositions and calculate dropped/total."""
    last_err = None
    for path in ["/stats/dispositions", "/reports/dispositions", "/stats/calls"]:
        try:
            body = convoso_post(path, {})
        except ConvosoAPIError as e:
            last_err = str(e)
            continue
        if body.get("success"):
            return body.get("data")
        last_err = f"{path}: {body.get('text') or body.get('code')}"
    raise ConvosoAPIError(f"Could not fetch drop rate stats. Last error: {last_err}")


def monitor_drop_rate():
    """For Stage 1 we attempt to compute a fleet-wide drop rate. If the
    available endpoint doesn't expose what we need, we surface 'pending
    endpoint verification' so the user can tell us which Convoso report
    endpoint to use."""
    try:
        data = fetch_drop_rate_stats()
    except ConvosoAPIError as e:
        state.update_monitor("drop_rate", "error", error=str(e))
        return

    # Try to find total calls and drops in whatever shape was returned
    total_calls = None
    drops = None
    if isinstance(data, dict):
        total_calls = (data.get("total_calls") or data.get("total")
                       or data.get("call_count"))
        drops = (data.get("drops") or data.get("dropped")
                 or data.get("dropped_calls") or data.get("drop_count"))

    if not total_calls or drops is None:
        state.update_monitor("drop_rate", "needs_setup", error=(
            "Drop rate endpoint returned data but the field names did not "
            "match expected shape. Send the raw response for mapping."
        ))
        return

    rate = float(drops) / float(total_calls) if total_calls else 0.0
    severity = "ok"
    if rate >= DROP_RATE_CRIT_THRESHOLD:
        severity = "critical"
    elif rate >= DROP_RATE_WARN_THRESHOLD:
        severity = "warning"

    if severity in ("warning", "critical"):
        state.add_anomaly(
            severity=severity,
            monitor="drop_rate",
            title=f"Drop rate at {rate*100:.2f}%",
            detail=f"{drops} drops out of {total_calls} calls.",
            fingerprint=f"drop_rate:{severity}",
        )

    state.update_monitor("drop_rate", "ok", data={
        "rate": rate,
        "drops": drops,
        "total_calls": total_calls,
        "warn_threshold": DROP_RATE_WARN_THRESHOLD,
        "crit_threshold": DROP_RATE_CRIT_THRESHOLD,
        "severity": severity,
    })


def fetch_hopper_for_campaign(campaign_id):
    last_err = None
    for path in ["/hopper/list", "/hopper", "/campaigns/hopper"]:
        try:
            body = convoso_post(path, {"campaign_id": campaign_id})
        except ConvosoAPIError as e:
            last_err = str(e)
            continue
        if body.get("success"):
            return body.get("data")
        last_err = f"{path}: {body.get('text') or body.get('code')}"
    raise ConvosoAPIError(f"Could not fetch hopper. Last error: {last_err}")


def monitor_hopper_depletion():
    # We need the campaign list first. Reuse previous if available.
    cached = state.previous.get("campaign_pauses") or {}
    campaigns = cached.get("campaigns") or []
    if not campaigns:
        try:
            campaigns_raw = fetch_campaigns()
            campaigns = [{
                "id": c.get("id") or c.get("campaign_id"),
                "name": c.get("name") or c.get("campaign_name"),
                "active": True,
            } for c in campaigns_raw]
        except ConvosoAPIError as e:
            state.update_monitor("hopper", "error", error=str(e))
            return

    hopper_summary = []
    seen_endpoint_error = None

    for c in campaigns:
        if not c.get("active"):
            continue
        try:
            data = fetch_hopper_for_campaign(c["id"])
        except ConvosoAPIError as e:
            seen_endpoint_error = str(e)
            continue
        if not isinstance(data, dict):
            continue
        # We don't know the exact shape; try the common ones
        leads_remaining = (data.get("leads_remaining")
                           or data.get("remaining")
                           or data.get("hopper_count"))
        leads_total = (data.get("leads_total")
                       or data.get("total")
                       or data.get("list_size"))
        if leads_remaining is None or leads_total is None:
            continue

        pct = (float(leads_remaining) / float(leads_total)) if leads_total else 0.0
        severity = "ok"
        if pct <= HOPPER_CRIT_PCT:
            severity = "critical"
        elif pct <= HOPPER_WARN_PCT:
            severity = "warning"

        if severity in ("warning", "critical"):
            state.add_anomaly(
                severity=severity,
                monitor="hopper",
                title=f"Hopper depleting: {c['name']}",
                detail=f"{leads_remaining} of {leads_total} leads remaining ({pct*100:.1f}%).",
                fingerprint=f"hopper:{c['id']}:{severity}",
            )

        hopper_summary.append({
            "campaign_id": c["id"],
            "campaign_name": c["name"],
            "remaining": leads_remaining,
            "total": leads_total,
            "pct": pct,
            "severity": severity,
        })

    if not hopper_summary and seen_endpoint_error:
        state.update_monitor("hopper", "needs_setup", error=(
            f"Hopper endpoint not yet identified. Last error: {seen_endpoint_error}"
        ))
        return

    state.update_monitor("hopper", "ok", data={"campaigns": hopper_summary})


# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

def run_all_monitors():
    """Called by APScheduler every POLL_INTERVAL_SECONDS."""
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

    # Determine overall poll status
    if all(s == "ok" for s in statuses):
        overall = "ok"
    elif any(s == "ok" for s in statuses):
        overall = "partial"
    else:
        overall = "error"

    state.last_poll_at = datetime.now(timezone.utc).isoformat()
    state.last_poll_status = overall
    log.info("Monitor poll cycle finished in %.2fs status=%s",
             time.time() - started, overall)


def start_scheduler():
    """Start the background poller. Called once at startup."""
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(run_all_monitors, "interval",
                  seconds=POLL_INTERVAL_SECONDS,
                  next_run_time=datetime.now(),    # run once immediately
                  max_instances=1,
                  coalesce=True)
    sched.start()
    log.info("Scheduler started: polling every %ss", POLL_INTERVAL_SECONDS)


# Run the first poll on startup so the dashboard isn't empty
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
            "POST /webhook":       "Receives Convoso disposition webhooks (Phase 1)",
            "GET /dashboard":      "Live operations dashboard (Phase 2)",
            "GET /api/state":      "JSON state snapshot used by dashboard",
            "GET /api/diagnose":   "Probes Convoso endpoints to verify availability",
            "GET /":               "This health check",
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
    """Probe known Convoso endpoints to figure out which work for this
    account. Useful when monitors say 'needs_setup'."""
    results = {}
    paths_to_test = [
        ("/campaigns/search", {"limit": 1, "offset": 0}),
        ("/campaigns/list",   {"limit": 1, "offset": 0}),
        ("/campaigns",        {"limit": 1, "offset": 0}),
        ("/stats/dispositions", {}),
        ("/reports/dispositions", {}),
        ("/stats/calls", {}),
        ("/hopper/list", {}),
        ("/hopper", {}),
        ("/users/search", {"limit": 1, "offset": 0}),
        ("/lists/search", {"limit": 1, "offset": 0}),
    ]
    for path, payload in paths_to_test:
        try:
            body = convoso_post(path, payload)
            results[path] = {
                "success": body.get("success"),
                "code": body.get("code"),
                "text": body.get("text"),
                "data_keys": list(body.get("data", {}).keys()) if isinstance(body.get("data"), dict) else None,
            }
        except ConvosoAPIError as e:
            results[path] = {"error": str(e)}
        except Exception as e:
            results[path] = {"error": f"unexpected: {e}", "trace": traceback.format_exc()[:300]}
    return jsonify(results)


@app.route("/api/poll", methods=["POST"])
def api_poll():
    """Force an immediate poll - useful for testing."""
    run_all_monitors()
    return jsonify({"ok": True, "snapshot": state.snapshot()})


if __name__ == "__main__":
    start_scheduler()
    _scheduler_started = True
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
