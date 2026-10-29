"""
Telesero (CXDialer) source for the Dialing Insights page — the marketing
company's dialer, shown as its own tab next to Convoso.

Uses Telesero's read-only API key (env TELESERO_API_KEY, sent as the
X-AUTH-TOKEN header) and its /api/post-conversations-filter call log.
Rows are turned into the same record shape the Convoso side uses, so the
existing InsightsStore.summary() and lead-attempt views work unchanged.
Lead names / phone numbers are dropped on the way in.

The filter endpoint has a `limit` but no offset, so a time window that
returns a full page is split in half until each piece fits.

Env vars (optional unless noted):
  TELESERO_API_KEY   (required) read-only key from Telesero → User Management → Api Keys
  TELESERO_BASE      default https://portal.teleserosuite.com
  TELESERO_TZ        time zone Telesero's timestamps are in (default America/New_York)
  TELESERO_PAGE      rows per request (default 2000)
"""

import logging
import os
import threading
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from insights import TZ, InsightsStore

log = logging.getLogger("convoso-claude.telesero")

API_KEY = os.environ.get("TELESERO_API_KEY")
BASE = os.environ.get("TELESERO_BASE", "https://portal.teleserosuite.com").rstrip("/")
SRC_TZ = ZoneInfo(os.environ.get("TELESERO_TZ", "America/New_York"))
PAGE = int(os.environ.get("TELESERO_PAGE", "2000"))
REFRESH_SECONDS = int(os.environ.get("TELESERO_REFRESH_SECONDS", "120"))
MIN_SPLIT_SECONDS = 120
CACHE_DAYS = 7

# Global disposition statuses from Telesero (Campaigns → Disposition Statuses)
# code: (name, category)
STATUS = {
    "A": ("Answering Machine", "amd"), "AA": ("Answering Machine Auto", "amd"),
    "AL": ("Answering Machine Msg Played", "amd"), "AM": ("Answering Machine SentToMesg", "amd"),
    "AB": ("Busy Auto", "no_answer"), "B": ("Busy", "no_answer"),
    "N": ("No Answer", "no_answer"), "NA": ("No Answer AutoDial", "no_answer"),
    "ADC": ("Disconnected Number Auto", "bad_number"), "ADCT": ("Disconnected Number Temporary", "bad_number"),
    "AFAX": ("Fax Machine Auto", "bad_number"), "DC": ("Disconnected Number", "bad_number"),
    "AFTHRS": ("Inbound After Hours Drop", "dropped"), "DROP": ("Agent Not Available", "dropped"),
    "XDROP": ("Agent Not Available IN", "dropped"), "NANQUE": ("Inbound No Agent No Queue Drop", "dropped"),
    "TIMEOT": ("Inbound Queue Timeout Drop", "dropped"), "MAXCAL": ("Inbound Max Calls Drop", "dropped"),
    "QVMAIL": ("Queue Abandon Voicemail Left", "dropped"),
    "CALLBK": ("Call Back", "contact"), "CBHOLD": ("Call Back Hold", "contact"),
    "DEC": ("Declined Sale", "contact"), "NI": ("Not Interested", "contact"),
    "NP": ("No Pitch No Price", "contact"), "XFER": ("Call Transferred", "contact"),
    "SALE": ("Sale Made", "sale"),
    "DNC": ("DO NOT CALL", "dnc"), "DNCC": ("DO NOT CALL Hopper Camp Match", "dnc"),
    "DNCL": ("DO NOT CALL Hopper Sys Match", "dnc"),
    "CELL": ("Cell Phone", "other"), "VOIP": ("Voip Phone", "other"),
    "ERI": ("Agent Error", "other"), "INCALL": ("Lead Being Called", "other"),
    "IVRXFR": ("Outbound drop to Call Menu", "other"), "LRERR": ("Outbound Local Channel Res Err", "other"),
    "LSMERG": ("Agent lead search old lead mrg", "other"), "NEW": ("New Lead", "other"),
    "PM": ("Played Message", "other"), "PU": ("Call Picked Up", "other"),
    "QUEUE": ("Lead To Be Called", "other"),
    "SVYCLM": ("Survey sent to Call Menu", "other"), "SVYEXT": ("Survey sent to Extension", "other"),
    "SVYHU": ("Survey Hungup", "other"), "SVYREC": ("Survey sent to Record", "other"),
    "SVYVM": ("Survey sent to Voicemail", "other"),
}
SYSTEM_USERS = {"", "vdad", "vdcl", "vdcl0", "system", "auto"}


def configured():
    return bool(API_KEY)


def _post(path, body, timeout=45):
    resp = requests.post(f"{BASE}{path}", json=body, timeout=timeout,
                         headers={"X-AUTH-TOKEN": API_KEY, "Accept": "application/json"})
    if resp.status_code >= 400:
        raise RuntimeError(f"Telesero {path} HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _get(path, timeout=30):
    resp = requests.get(f"{BASE}{path}", timeout=timeout,
                        headers={"X-AUTH-TOKEN": API_KEY, "Accept": "application/json"})
    if resp.status_code >= 400:
        raise RuntimeError(f"Telesero {path} HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _rows(body):
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for k in ("data", "items", "results", "conversations", "entries"):
            v = body.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                inner = _rows(v)
                if inner:
                    return inner
    return []


def _fmt(dt):
    """Telesero's DateTime object: {"date": "mm-dd-yyyy", "time": "hh:mm"}."""
    d = dt.astimezone(SRC_TZ)
    return {"date": d.strftime("%m-%d-%Y"), "time": d.strftime("%H:%M")}


def _floor_min(dt):
    return dt.replace(second=0, microsecond=0)


def fetch_conversations(start, end, extra=None):
    """All conversations between start and end, splitting busy windows."""
    body = {"date_start": _fmt(start), "date_end": _fmt(end), "limit": PAGE}
    if extra:
        body.update(extra)
    rows = _rows(_post("/api/post-conversations-filter", body))
    if len(rows) >= PAGE:
        span = (end - start).total_seconds()
        if span > MIN_SPLIT_SECONDS:
            mid = _floor_min(start + (end - start) / 2)
            if mid <= start:
                mid = start + timedelta(minutes=1)
            return fetch_conversations(start, mid, extra) + fetch_conversations(mid, end, extra)
        log.warning("Telesero: %s-%s returned a full page (%d); some calls may be missing", start, end, PAGE)
    return rows


def _parse_ts(v):
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
        n = float(v)
        if n > 1e12:
            n /= 1000.0
        return datetime.fromtimestamp(n, tz=timezone.utc).astimezone(TZ)
    s = str(v).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=SRC_TZ).astimezone(TZ)
        except ValueError:
            continue
    try:
        d = datetime.fromisoformat(str(v))
        return (d if d.tzinfo else d.replace(tzinfo=SRC_TZ)).astimezone(TZ)
    except ValueError:
        return None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


class Names:
    """Campaign key -> name and list id -> name, refreshed occasionally."""

    def __init__(self):
        self.campaigns = {}
        self.lists = {}
        self.at = 0
        self.lock = threading.Lock()

    def refresh(self, force=False):
        if not configured() or (not force and time.time() - self.at < 1800):
            return
        camps, lists = {}, {}
        try:
            for c in _rows(_get("/api/get-campaigns")):
                if not isinstance(c, dict):
                    continue
                key = str(c.get("key") or c.get("id") or "")
                camps[key] = f"{key} - {c.get('name')}" if c.get("name") else key
                try:
                    for L in _rows(_get(f"/api/get-lead-lists/{c.get('id')}")):
                        if isinstance(L, dict):
                            rid = str(L.get("remote_id") or L.get("remote_list_id") or L.get("list_id") or L.get("id") or "")
                            if rid:
                                lists[rid] = L.get("name") or L.get("description") or f"List {rid}"
                except Exception as e:
                    log.warning("Telesero lists for campaign %s: %s", key, e)
        except Exception as e:
            log.warning("Telesero campaigns: %s", e)
        with self.lock:
            if camps:
                self.campaigns = camps
            if lists:
                self.lists = lists
            self.at = time.time()


names = Names()


def normalize(row):
    code = str(row.get("status") or "").strip().upper()
    name, cat = STATUS.get(code, (code or "Unknown", None))
    user = str(row.get("user") or "").strip()
    agent = user if user.lower() not in SYSTEM_USERS else ""
    length = _num(row.get("length"))
    if cat is None:   # campaign-specific status we don't know: judge by who handled it
        cat = "contact" if agent and length > 0 else "other"
    if cat == "contact" and not agent:
        cat = "other"
    ck = str(row.get("campaign_key") or "")
    attempt = row.get("called_count")
    try:
        attempt = int(float(attempt)) if attempt not in (None, "") else None
    except (TypeError, ValueError):
        attempt = None
    outbound = row.get("is_outbound")
    inbound = not (outbound in (True, 1, "1", "true", "True", "Y", "y"))
    return {
        "id": str(row.get("id") or row.get("unique_id") or row.get("log_id") or ""),
        "when": _parse_ts(row.get("call_timestamp")),
        "status": name if name != code else code,
        "cat": cat,
        "agent_id": agent,
        "agent": agent,
        "list_id": str(row.get("remote_list_id") or ""),
        "attempt": attempt,
        "cid": "",
        "campaign": names.campaigns.get(ck, ck),
        "inbound": inbound,
        "length": length,
    }


def _store_add(store, rows, day=None):
    added = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        rec = normalize(raw)
        if day and rec["when"] and rec["when"].date() != day:
            continue
        key = rec["id"] or f"{rec['when']}|{rec['agent_id']}|{rec['list_id']}|{rec['status']}"
        if key not in store.calls:
            added += 1
        store.calls[key] = rec
        if rec["when"] and (store.last_seen is None or rec["when"] > store.last_seen):
            store.last_seen = rec["when"]
    return added


class TeleseroToday(InsightsStore):
    """Today's Telesero calls, pulled incrementally like the Convoso store."""

    def __init__(self):
        super().__init__(convoso_post=None)

    def refresh(self, list_names=None):
        if not configured() or self.refreshing:
            return
        self.refreshing = True
        try:
            names.refresh()
            now = datetime.now(TZ)
            today = now.date()
            with self.lock:
                if self.day != today:
                    self.day, self.calls, self.last_seen = today, {}, None
                start = (self.last_seen - timedelta(minutes=5)) if self.last_seen \
                    else datetime(today.year, today.month, today.day, tzinfo=TZ)
            rows = fetch_conversations(start, now + timedelta(minutes=1))
            with self.lock:
                added = _store_add(self, rows, day=today)
                self.list_names = dict(names.lists)
                self.last_refresh = now.isoformat()
                self.last_error = None
            log.info("Telesero refresh: %d rows, %d new, %d today", len(rows), added, len(self.calls))
        except Exception as e:
            log.exception("Telesero refresh failed")
            with self.lock:
                self.last_error = str(e)
        finally:
            self.refreshing = False


class TeleseroPast:
    """Full past days, loaded once and cached (same idea as dayview.PastDays)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.stores = OrderedDict()
        self.status = {}

    def _load(self, day):
        st = self.status[day]
        try:
            names.refresh()
            store = InsightsStore(None)
            store.day = day
            start = datetime(day.year, day.month, day.day, tzinfo=TZ)
            rows = []
            # pull the day in 2-hour chunks so progress is visible
            for h in range(0, 24, 2):
                rows += fetch_conversations(start + timedelta(hours=h), start + timedelta(hours=h + 2))
                st["rows"] = len(rows)
            _store_add(store, rows, day=day)
            store.list_names = dict(names.lists)
            store.last_refresh = datetime.now(TZ).isoformat()
            with self.lock:
                self.stores[day] = store
                self.stores.move_to_end(day)
                while len(self.stores) > CACHE_DAYS:
                    old, _ = self.stores.popitem(last=False)
                    self.status.pop(old, None)
            st["loading"] = False
        except Exception as e:
            log.exception("Telesero day %s failed", day)
            st["loading"], st["error"] = False, str(e)

    def get(self, day):
        with self.lock:
            store = self.stores.get(day)
            if store is not None:
                return store, {"loading": False}
            st = self.status.get(day)
            if st is None or (not st.get("loading") and st.get("error")):
                st = {"loading": True, "rows": 0, "page": 0, "error": None}
                self.status[day] = st
                threading.Thread(target=self._load, args=(day,), daemon=True).start()
            return None, dict(st)


def describe_sample(limit=3):
    """Field names / types of a few recent conversations (no lead data) —
    used once to confirm the API's shape after the key is set."""
    if not configured():
        return {"configured": False}
    end = datetime.now(TZ)
    start = end - timedelta(hours=2)
    body = _post("/api/post-conversations-filter", {"date_start": _fmt(start), "date_end": _fmt(end), "limit": limit})
    rows = _rows(body)
    safe = ("status", "campaign_key", "is_outbound", "call_timestamp", "called_count",
            "length", "term_reason", "user_group", "is_manual_dial", "remote_list_id")
    return {
        "configured": True,
        "top_level": (list(body.keys()) if isinstance(body, dict) else f"list[{len(body)}]"),
        "rows_returned": len(rows),
        "fields": {k: (repr(v) if k in safe else type(v).__name__) for k, v in (rows[0].items() if rows else [])},
        "now_eastern": end.strftime("%Y-%m-%d %H:%M:%S"),
    }
