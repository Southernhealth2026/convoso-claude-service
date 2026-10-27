"""
Live floor stats for the top of the /insights page:
agents logged in / in call / waiting / paused, dial level,
dialable leads and calls waiting.

Convoso's agent-monitor response shape isn't documented in detail, so the
parsing here is deliberately tolerant: it looks for an agent list and for
known-looking keys anywhere in the response. Anything it can't find comes
back as None and the page shows "—".

/api/live-shape (see app.py) returns only key names and status values, never
lead data, so the field mapping can be checked safely.
"""

import logging
import threading
import time

log = logging.getLogger("convoso-claude.live")

CACHE_SECONDS = 15
_cache = {"at": 0, "key": None, "data": None}
_lock = threading.Lock()
_campaign_map = {}   # campaign name -> id, learned from agent-monitor


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _val(v):
    """Number from 12, "12", "56.51%", or "hh:mm:ss"/"mm:ss" (-> seconds)."""
    if v is None or isinstance(v, (dict, list)):
        return None
    t = str(v).strip().rstrip("%")
    if ":" in t:
        try:
            parts = [int(float(p)) for p in t.split(":")]
        except ValueError:
            return None
        sec = 0
        for p in parts:
            sec = sec * 60 + p
        return float(sec)
    return _num(t)


def _summary(mon):
    """Convoso's own Agent Monitor totals live at data.entries.data."""
    if isinstance(mon, dict):
        e = mon.get("entries")
        if isinstance(e, dict) and isinstance(e.get("data"), dict):
            return e["data"]
    return {}


def _campaign_ids(mon):
    """name -> id from data.entries.campaigns ([{name, value}])."""
    out = {}
    if isinstance(mon, dict):
        e = mon.get("entries")
        if isinstance(e, dict):
            for c in e.get("campaigns") or []:
                if isinstance(c, dict) and c.get("name") is not None:
                    out[str(c.get("name"))] = c.get("value")
    return out


def _walk(obj, depth=0):
    """Yield (key, value) pairs from nested dicts/lists."""
    if depth > 5:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k).lower(), v
            yield from _walk(v, depth + 1)
    elif isinstance(obj, list):
        for v in obj[:500]:
            yield from _walk(v, depth + 1)


def _find_number(obj, names):
    """First numeric value whose key matches one of `names` exactly."""
    names = [n.lower() for n in names]
    for k, v in _walk(obj):
        if k in names and not isinstance(v, (dict, list)):
            n = _num(v)
            if n is not None:
                return n
    return None


def _sum_numbers(rows, names):
    names = [n.lower() for n in names]
    total, found = 0.0, False
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k, v in r.items():
            if str(k).lower() in names:
                n = _num(v)
                if n is not None:
                    total += n
                    found = True
                break
    return total if found else None


def _agent_rows(data):
    """Find the list of agent records in an agent-monitor response."""
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("agents", "entries", "results", "users", "agent_list", "data"):
        v = data.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
        if isinstance(v, dict):
            inner = _agent_rows(v)
            if inner:
                return inner
    vals = list(data.values())
    if vals and all(isinstance(v, dict) for v in vals) and any(
            any(s in v for s in ("status", "agent_status", "state")) for v in vals):
        return vals
    return []


def _status_of(row):
    for k in ("status", "agent_status", "state", "status_name", "agent_state"):
        if row.get(k) not in (None, ""):
            return str(row.get(k))
    return ""


def classify_status(s):
    s = s.lower()
    if any(w in s for w in ("dead",)):
        return "dead"
    if any(w in s for w in ("incall", "in call", "in_call", "talk", "queue", "oncall", "on call")):
        return "in_call"
    if any(w in s for w in ("dispo", "wrap")):
        return "dispo"
    if any(w in s for w in ("pause", "break", "lunch", "meeting", "training", "not ready", "notready")):
        return "paused"
    if any(w in s for w in ("ready", "wait", "available", "idle")):
        return "waiting"
    return "other"


def _campaign_of(row):
    for k in ("campaign_name", "campaign", "campaign_id"):
        if row.get(k) not in (None, ""):
            return str(row.get(k))
    return ""


def fetch_live(convoso_post, campaign=None):
    key = campaign or ""
    with _lock:
        if _cache["key"] == key and time.time() - _cache["at"] < CACHE_SECONDS:
            return _cache["data"]

    out = {
        "logged_in": None, "in_call": None, "waiting": None, "paused": None,
        "dispo": None, "dial_level": None, "dialable_leads": None,
        "calls_waiting": None, "errors": [],
    }

    # --- agents --------------------------------------------------------
    mon = None
    try:
        payload = {}
        if campaign and _campaign_map.get(campaign) is not None:
            payload["campaign_id"] = _campaign_map[campaign]
        body = convoso_post("/agent-monitor/search", payload)
        if body.get("success") is False:
            out["errors"].append(f"agent-monitor: {body.get('text') or body.get('code')}")
        mon = body.get("data") if isinstance(body, dict) else None
    except Exception as e:
        out["errors"].append(f"agent-monitor: {e}")

    if mon is not None:
        rows = _agent_rows(mon)
        if campaign:
            rows = [r for r in rows if _campaign_of(r) in ("", campaign)] or rows
        if rows:
            counts = {"in_call": 0, "waiting": 0, "paused": 0, "dispo": 0, "dead": 0, "other": 0}
            for r in rows:
                counts[classify_status(_status_of(r))] += 1
            out["logged_in"] = len(rows)
            out["in_call"] = counts["in_call"]
            out["waiting"] = counts["waiting"]
            out["paused"] = counts["paused"]
            out["dispo"] = counts["dispo"]
        else:
            out["logged_in"] = _find_number(mon, ["logged_in", "logged_in_agents", "agents_logged_in", "total_agents", "total_logged_in"])
            out["waiting"] = _find_number(mon, ["available_agents", "agents_available", "ready_agents", "waiting_agents"])
            out["in_call"] = _find_number(mon, ["agents_in_call", "in_call_agents", "incall_agents"])
            out["paused"] = _find_number(mon, ["paused_agents", "agents_paused"])
        out["calls_waiting"] = _find_number(mon, ["calls_waiting", "waiting_calls", "live_calls_waiting", "calls_in_queue", "queue_calls"])

        summ = _summary(mon)
        if summ:
            g = lambda k: _val(summ.get(k))
            if g("agent_total") is not None:
                out["logged_in"] = g("agent_total")
                out["in_call"] = g("agent_incall")
                out["waiting"] = g("agent_ready")
                out["paused"] = g("agent_paused")
            cw_in, cw_out = g("calls_waiting_in"), g("calls_waiting_out")
            if cw_in is not None or cw_out is not None:
                out["calls_waiting"] = (cw_in or 0) + (cw_out or 0)
            out["dialable_leads"] = g("dialable_leads")
            out["hopper"] = g("hopper")
            out["cv_dropped"] = g("dropped")
            out["cv_drop_pct"] = g("dropped_percent")
            out["cv_answered"] = g("answered")
            out["cv_calls_today"] = g("calls_today")
            out["cv_wait_avg"] = g("outbound_total_wait_avg")
            out["cv_talk_avg"] = g("outbound_total_talk_avg")
            dl = g("dial_level")
            if dl is not None:
                out["dial_level"] = dl
        out["dial_level"] = _find_number(mon, ["dial_level", "auto_dial_level", "dial_ratio"])
        out["dialable_leads"] = _find_number(mon, ["dialable_leads", "dialable", "dialable_count"])

    if mon is not None:
        _campaign_map.update(_campaign_ids(mon))

    # Convoso only reports dial level for a single campaign. With "All
    # campaigns" selected, ask each campaign and show the busiest one.
    if out["dial_level"] is None and not campaign and _campaign_map:
        best = None
        for name, cid in list(_campaign_map.items())[:6]:
            try:
                b = convoso_post("/agent-monitor/search", {"campaign_id": cid})
                sm = _summary(b.get("data"))
                dl, ag = _val(sm.get("dial_level")), _val(sm.get("agent_total")) or 0
                if dl is not None and (best is None or ag > best[2]):
                    best = (name, dl, ag)
            except Exception as e:
                out["errors"].append(f"dial level {name}: {e}")
        if best:
            out["dial_level"], out["dial_level_campaign"] = best[1], best[0]

    # --- campaign settings (dial level / dialable leads fallback) ------
    if out["dial_level"] is None or out["dialable_leads"] is None:
        try:
            body = convoso_post("/campaigns/search", {"limit": 200, "offset": 0})
            data = body.get("data") or {}
            entries = data.get("entries") if isinstance(data, dict) else data
            if isinstance(data, dict) and entries is None:
                entries = list(data.values())
            entries = [c for c in (entries or []) if isinstance(c, dict)]
            if campaign:
                entries = [c for c in entries if str(c.get("name")) == campaign or str(c.get("id")) == campaign] or entries
            active = [c for c in entries if str(c.get("status", "")).strip().lower() in ("y", "yes", "1", "true", "active", "running")] or entries
            if out["dial_level"] is None:
                levels = []
                for c in active:
                    v = None
                    for k in ("dial_level", "auto_dial_level", "dial_ratio", "dialing_ratio"):
                        v = _num(c.get(k))
                        if v is not None:
                            break
                    if v:
                        levels.append(v)
                if levels:
                    out["dial_level"] = round(sum(levels) / len(levels), 1)
            if out["dialable_leads"] is None:
                out["dialable_leads"] = _sum_numbers(active, ["dialable_leads", "dialable", "dialable_count"])
        except Exception as e:
            out["errors"].append(f"campaigns: {e}")

    with _lock:
        _cache.update(at=time.time(), key=key, data=out)
    return out


def describe_shape(obj, depth=0, max_depth=4):
    """Key names and value types only, so the structure can be inspected
    without exposing lead data. Status-like fields show their values."""
    if depth > max_depth:
        return "…"
    if isinstance(obj, dict):
        out = {}
        for i, (k, v) in enumerate(obj.items()):
            if i >= 80:
                out["…"] = f"{len(obj) - 80} more keys"
                break
            kl = str(k).lower()
            if kl in ("status", "agent_status", "state", "status_name", "agent_state", "call_type") and not isinstance(v, (dict, list)):
                out[k] = f"str = {v!r}"
            elif kl in ("dial_level", "auto_dial_level", "dialable_leads", "calls_waiting", "available_agents", "logged_in", "total_agents",
                        "dropped", "dropped_percent", "answered", "calls_today", "hopper", "outbound_total_wait_avg",
                        "outbound_total_talk_avg", "calls_waiting_in", "calls_waiting_out"):
                out[k] = f"{type(v).__name__} = {v!r}"
            else:
                out[k] = describe_shape(v, depth + 1, max_depth)
        return out
    if isinstance(obj, list):
        return [f"list of {len(obj)}", describe_shape(obj[0], depth + 1, max_depth) if obj else None]
    return type(obj).__name__
