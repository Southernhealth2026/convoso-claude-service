"""
Dialing Insights — call-log analytics for the /insights page.

Pulls today's call log from Convoso (/log/retrieve) into memory, then
rolls it up by disposition, agent, list and hour. Only aggregates and
agent names leave this module; no lead names or phone numbers are exposed.

Refresh model:
  - On first run (or after a Render restart / new day) it backfills the
    whole day, page by page.
  - After that it only pulls calls since the last one it saw (with a
    small overlap), de-duplicated by call id.

Env vars (all optional):
  CONVOSO_TZ               Time zone Convoso reports call times in (default America/Los_Angeles)
  DISPLAY_TZ               Time zone the page shows hours/days in (default America/New_York)
  INSIGHTS_REFRESH_SECONDS How often to pull new calls (default 120)
  INSIGHTS_PAGE_SIZE       Rows per /log/retrieve page (default 500)
  SALE_STATUSES            Comma list of words that mark a sale disposition
                           (default: sale,sold,enrolled,submitted,closed)
  CONTACT_EXCLUDE_STATUSES Extra comma list of words that mean "not a real
                           conversation" even if an agent touched the call
"""

import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

log = logging.getLogger("convoso-claude.insights")

SRC_TZ = ZoneInfo(os.environ.get("CONVOSO_TZ", "America/Los_Angeles"))
TZ = ZoneInfo(os.environ.get("DISPLAY_TZ", "America/New_York"))
REFRESH_SECONDS = int(os.environ.get("INSIGHTS_REFRESH_SECONDS", "120"))
PAGE_SIZE = int(os.environ.get("INSIGHTS_PAGE_SIZE", "500"))
MAX_PAGES = int(os.environ.get("INSIGHTS_MAX_PAGES", "60"))
OVERLAP_MINUTES = 5
DID_MIN_DIALS = int(os.environ.get("DID_MIN_DIALS", "40"))


def _words(env_name, default):
    raw = os.environ.get(env_name, default)
    return [w.strip().lower() for w in raw.split(",") if w.strip()]


SALE_WORDS = _words("SALE_STATUSES", "sale,sold,enrolled,submitted,closed")
CONTACT_EXCLUDE_WORDS = _words("CONTACT_EXCLUDE_STATUSES", "")
# Campaigns whose "Sale" dispositions are service work on existing clients,
# not new deals (e.g. Southern Custo). They still count as conversations.
SALE_EXCLUDE_CAMPAIGNS = _words("SALE_EXCLUDE_CAMPAIGNS", "custo")
# A disposition that mentions a sale but means "no sale" (e.g. "Pitched - No Sale")
NOT_SALE_WORDS = _words("NOT_SALE_STATUSES", "no sale,not sold,unsold,no-sale,nosale,not closed,not enrolled,not submitted")

# Order matters: first match wins.
CATEGORY_RULES = [
    ("amd",       ["answering machine", "machine detected", "amd", "voicemail", "voice mail"]),
    ("dropped",   ["drop", "abandon"]),
    ("dnc",       ["dnc", "do not call", "do not contact"]),
    ("bad_number",["disconnected", "bad number", "wrong number", "invalid", "fax", "not in service"]),
    ("no_answer", ["no answer", "busy", "not available", "ring no", "unanswered", "dead air", "hangup", "hang up", "congestion", "network", "failed"]),
]

CATEGORY_LABELS = {
    "sale": "Sale",
    "contact": "Contact (no sale)",
    "amd": "Answering machine",
    "dropped": "Dropped / abandoned",
    "no_answer": "No answer / busy",
    "bad_number": "Bad number",
    "dnc": "DNC",
    "other": "Other",
}


def _first(d, *keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _parse_dt(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=SRC_TZ).astimezone(TZ)
        except ValueError:
            continue
    return None


ATTEMPT_BUCKETS = [(1, 1, "1st call"), (2, 3, "2–3"), (4, 6, "4–6"), (7, 10, "7–10"), (11, 10**9, "11+")]


def attempt_bucket(n):
    for lo, hi, label in ATTEMPT_BUCKETS:
        if lo <= n <= hi:
            return label
    return "1st call" if n <= 0 else "11+"


def _digits(v):
    d = "".join(ch for ch in str(v or "") if ch.isdigit())
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return d


def classify(status_name, agent_handled):
    s = (status_name or "").lower()
    if any(w in s for w in SALE_WORDS) and not any(w in s for w in NOT_SALE_WORDS):
        return "sale"
    for cat, words in CATEGORY_RULES:
        if any(w in s for w in words):
            return cat
    if any(w in s for w in CONTACT_EXCLUDE_WORDS):
        return "other"
    if agent_handled:
        return "contact"
    return "other"


def normalize(row):
    """Turn one raw /log/retrieve row into the small record we keep.
    Lead names and phone numbers are deliberately dropped here."""
    call_id = _first(row, "id", "call_log_id", "uniqueid")
    status_name = _first(row, "status_name", "status_desc", "status") or "Unknown"
    user_id = str(_first(row, "user_id", "agent_id") or "").strip()
    user = str(_first(row, "user", "agent_name", "user_name", "agent_full_name") or "").strip()
    agent_handled = (bool(user_id) and user_id not in ("0", "-1")
                     and user.lower() not in ("", "vdad", "auto")
                     and not user.lower().startswith("system"))
    call_type = str(_first(row, "call_type", "type", "direction") or "").upper()
    when = _parse_dt(_first(row, "call_date", "start_time", "date"))
    length = _num(_first(row, "call_length", "length_in_sec", "talk_time", "duration"))
    cat = classify(status_name, agent_handled)
    campaign = str(_first(row, "campaign", "campaign_name", "campaign_id") or "")
    if cat == "sale" and any(w in campaign.lower() for w in SALE_EXCLUDE_CAMPAIGNS):
        cat = "contact"
    return {
        "id": str(call_id) if call_id is not None else None,
        "when": when,
        "status": str(status_name),
        "cat": cat,
        "agent_id": user_id if agent_handled else "",
        "agent": user if agent_handled else "",
        "list_id": str(_first(row, "list_id") or ""),
        "attempt": int(_num(_first(row, "called_count"))) if _first(row, "called_count") is not None else None,
        "cid": _digits(_first(row, "caller_id_displayed", "callerid", "caller_id", "outbound_cid",
                              "cid", "did", "dialed_from", "from_number", "ani")),
        "campaign": str(_first(row, "campaign", "campaign_name", "campaign_id") or ""),
        "inbound": "IN" in call_type and "OUT" not in call_type,
        "length": length,
    }


class InsightsStore:
    def __init__(self, convoso_post):
        self.convoso_post = convoso_post
        self.lock = threading.Lock()
        self.day = None
        self.calls = {}           # id -> record
        self.last_seen = None     # newest call datetime we have
        self.last_refresh = None
        self.last_error = None
        self.refreshing = False
        self.list_names = {}

    # -- fetching ----------------------------------------------------------

    def _fetch_window(self, start_dt, end_dt):
        rows = []
        offset = 0
        for _ in range(MAX_PAGES):
            body = self.convoso_post("/log/retrieve", {
                "start_time": start_dt.astimezone(SRC_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_dt.astimezone(SRC_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "limit": PAGE_SIZE,
                "offset": offset,
            }, timeout=45)
            if not body.get("success", True) and not body.get("data"):
                raise RuntimeError(f"log/retrieve failed: code={body.get('code')} text={body.get('text')}")
            data = body.get("data") or {}
            if isinstance(data, list):
                page = data
            else:
                page = data.get("results") or data.get("entries") or []
            rows.extend(r for r in page if isinstance(r, dict))
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return rows

    def refresh(self, list_names=None):
        if self.refreshing:
            return
        self.refreshing = True
        try:
            now = datetime.now(TZ)
            today = now.date()
            with self.lock:
                if self.day != today:
                    self.day = today
                    self.calls = {}
                    self.last_seen = None
                start = (self.last_seen - timedelta(minutes=OVERLAP_MINUTES)) if self.last_seen \
                    else datetime(today.year, today.month, today.day, tzinfo=TZ)
            rows = self._fetch_window(start, now + timedelta(minutes=1))
            added = 0
            with self.lock:
                for raw in rows:
                    rec = normalize(raw)
                    if rec["when"] and rec["when"].date() != today:
                        continue
                    key = rec["id"] or f"{rec['when']}|{rec['agent_id']}|{rec['list_id']}|{rec['status']}"
                    if key not in self.calls:
                        added += 1
                    self.calls[key] = rec
                    if rec["when"] and (self.last_seen is None or rec["when"] > self.last_seen):
                        self.last_seen = rec["when"]
                if list_names:
                    self.list_names.update(list_names)
                self.last_refresh = now.isoformat()
                self.last_error = None
            log.info("Insights refresh: %d rows fetched, %d new, %d total today", len(rows), added, len(self.calls))
        except Exception as e:  # keep the last good data on screen
            log.exception("Insights refresh failed")
            with self.lock:
                self.last_error = str(e)
        finally:
            self.refreshing = False

    # -- rollups -----------------------------------------------------------

    def summary(self, agent_perf=None, campaign=None, direction="all"):
        with self.lock:
            calls = list(self.calls.values())
            list_names = dict(self.list_names)
            meta = {
                "day": self.day.isoformat() if self.day else None,
                "last_refresh": self.last_refresh,
                "last_error": self.last_error,
                "tz": str(TZ),
                "sale_words": SALE_WORDS,
            }

        campaigns = sorted({c["campaign"] for c in calls if c["campaign"]})
        if campaign:
            calls = [c for c in calls if c["campaign"] == campaign]
        if direction == "outbound":
            calls = [c for c in calls if not c["inbound"]]
        elif direction == "inbound":
            calls = [c for c in calls if c["inbound"]]

        def blank():
            return defaultdict(float)

        totals = blank()
        by_status = defaultdict(lambda: {"count": 0, "cat": None, "talk": 0.0})
        by_agent = defaultdict(lambda: blank())
        agent_names = {}
        agent_status = defaultdict(lambda: defaultdict(int))
        by_list = defaultdict(lambda: blank())
        by_cid = defaultdict(lambda: blank())
        by_attempt = defaultdict(lambda: blank())
        by_exact = defaultdict(lambda: blank())          # attempt 1..20 (20 = 20+)
        by_list_att = defaultdict(lambda: defaultdict(lambda: blank()))
        list_att_sum = defaultdict(float)
        by_hour = defaultdict(lambda: blank())

        for c in calls:
            cat = c["cat"]
            buckets = [totals, by_list[c["list_id"] or "—"],
                       by_hour[c["when"].hour if c["when"] else -1]]
            if c.get("cid") and not c["inbound"]:
                buckets.append(by_cid[c["cid"]])
            if c.get("attempt") is not None and not c["inbound"]:
                ab = attempt_bucket(c["attempt"])
                buckets.append(by_attempt[ab])
                buckets.append(by_exact[max(1, min(c["attempt"], 20))])
                buckets.append(by_list_att[c["list_id"] or "—"][ab])
                list_att_sum[c["list_id"] or "—"] += c["attempt"]
            for bucket in buckets:
                bucket["dials"] += 1
                bucket[cat] += 1
                if cat in ("contact", "sale"):
                    bucket["contacts"] += 1
                    bucket["talk"] += c["length"]
            s = by_status[c["status"]]
            s["count"] += 1
            s["cat"] = cat
            if cat in ("contact", "sale"):
                s["talk"] += c["length"]
            if c["agent_id"]:
                a = by_agent[c["agent_id"]]
                agent_names[c["agent_id"]] = c["agent"]
                a["handled"] += 1
                a[cat] += 1
                if cat in ("contact", "sale"):
                    a["contacts"] += 1
                    a["talk"] += c["length"]
                    if c["length"] < 30:
                        a["short"] += 1
                agent_status[c["agent_id"]][c["status"]] += 1

        def rate(n, d):
            return (n / d) if d else None

        def live_answers(b):
            return b["contacts"] + b["dropped"]

        dials = totals["dials"]
        kpis = {
            "dials": int(dials),
            "contacts": int(totals["contacts"]),
            "sales": int(totals["sale"]),
            "contact_rate": rate(totals["contacts"], dials),
            "amd_rate": rate(totals["amd"], dials),
            "drop_rate": rate(totals["dropped"], live_answers(totals)),
            "close_rate": rate(totals["sale"], totals["contacts"]),
            "avg_talk": rate(totals["talk"], totals["contacts"]),
            "sales_per_100": rate(totals["sale"] * 100, dials),
            "dropped": int(totals["dropped"]),
        }

        cats = []
        for key, label in CATEGORY_LABELS.items():
            n = totals.get(key, 0)
            if n:
                cats.append({"cat": key, "label": label, "count": int(n), "pct": rate(n, dials)})

        statuses = sorted(
            ({"status": k, "cat": v["cat"], "count": v["count"], "pct": rate(v["count"], dials),
              "avg_talk": rate(v["talk"], v["count"]) if v["cat"] in ("contact", "sale") else None}
             for k, v in by_status.items()),
            key=lambda r: -r["count"])

        # Agent performance (time split) keyed by user id from /agent-performance
        perf = {}
        for uid, st in (agent_perf or {}).items():
            if isinstance(st, dict):
                perf[str(uid)] = st
                if st.get("name") and str(uid) not in agent_names:
                    agent_names[str(uid)] = st.get("name")

        def hms_to_sec(v):
            try:
                h, m, s = (int(x) for x in str(v).split(":"))
                return h * 3600 + m * 60 + s
            except Exception:
                return None

        agents = []
        for uid in set(by_agent) | set(perf):
            a = by_agent.get(uid, blank())
            p = perf.get(uid, {})
            logged = hms_to_sec(p.get("total_time"))
            hours = (logged / 3600) if logged else None
            top = sorted(agent_status.get(uid, {}).items(), key=lambda kv: -kv[1])[:3]
            agents.append({
                "id": uid,
                "name": agent_names.get(uid) or f"Agent {uid}",
                "handled": int(a["handled"]),
                "contacts": int(a["contacts"]),
                "sales": int(a["sale"]),
                "close_rate": rate(a["sale"], a["contacts"]),
                "avg_talk": rate(a["talk"], a["contacts"]),
                "short_calls": int(a["short"]),
                "logged_sec": logged,
                "talk_pct": _num(p.get("talk_sec_pt")) if p else None,
                "wait_pct": _num(p.get("wait_sec_pt")) if p else None,
                "pause_pct": _num(p.get("pause_sec_pt")) if p else None,
                "dispo_pct": _num(_first(p, "wrapup_sec_pt", "dispo_sec_pt")) if p and _first(p, "wrapup_sec_pt", "dispo_sec_pt") is not None else None,
                "sales_per_hour": (a["sale"] / hours) if hours else None,
                # idle time between calls: logged-in seconds x wait % / calls taken
                "avg_wait": ((logged * _num(p.get("wait_sec_pt")) / 100.0) / _num(p.get("calls")))
                            if (logged and p and _num(p.get("calls")) > 0 and p.get("wait_sec_pt") not in (None, "")) else None,
                "top_dispos": [{"status": k, "count": v} for k, v in top],
            })
        agents = [a for a in agents
                  if (a["handled"] or (a["logged_sec"] or 0) > 0)
                  and not str(a["name"]).lower().startswith("system")]
        agents.sort(key=lambda a: (-(a["sales"]), -(a["contacts"])))

        lists = []
        for lid, b in by_list.items():
            lists.append({
                "list_id": lid,
                "name": list_names.get(lid) or (f"List {lid}" if lid != "—" else "No list"),
                "dials": int(b["dials"]),
                "contacts": int(b["contacts"]),
                "sales": int(b["sale"]),
                "contact_rate": rate(b["contacts"], b["dials"]),
                "amd_rate": rate(b["amd"], b["dials"]),
                "drop_rate": rate(b["dropped"], live_answers(b)),
                "sales_per_100": rate(b["sale"] * 100, b["dials"]),
                "share": rate(b["dials"], dials),
            })
        lists.sort(key=lambda r: -r["dials"])

        # Caller ID health: a number flagged as spam gets far fewer live
        # answers than the team average on the same lists.
        out_dials = sum(b["dials"] for b in by_cid.values())
        out_live = sum(b["contacts"] + b["dropped"] for b in by_cid.values())
        avg_live = rate(out_live, out_dials)
        dids = []
        for cid, b in by_cid.items():
            live = b["contacts"] + b["dropped"]
            lr = rate(live, b["dials"])
            health = "low data"
            if b["dials"] >= DID_MIN_DIALS and avg_live:
                ratio = (lr or 0) / avg_live
                health = "flagged" if ratio < 0.4 else "weak" if ratio < 0.7 else "ok"
            dids.append({
                "cid": cid,
                "dials": int(b["dials"]),
                "live": int(live),
                "live_rate": lr,
                "contacts": int(b["contacts"]),
                "amd_rate": rate(b["amd"], b["dials"]),
                "no_answer_rate": rate(b["no_answer"], b["dials"]),
                "sales": int(b["sale"]),
                "vs_avg": (lr / avg_live) if (lr is not None and avg_live) else None,
                "health": health,
            })
        dids.sort(key=lambda r: -r["dials"])

        attempts = []
        for _, _, label in ATTEMPT_BUCKETS:
            b = by_attempt.get(label)
            if not b:
                continue
            attempts.append({
                "bucket": label,
                "dials": int(b["dials"]),
                "share": rate(b["dials"], out_dials) if out_dials else None,
                "contacts": int(b["contacts"]),
                "live_rate": rate(b["contacts"] + b["dropped"], b["dials"]),
                "amd_rate": rate(b["amd"], b["dials"]),
                "sales": int(b["sale"]),
            })

        # Exact attempt curve (1..20+)
        curve = []
        for a in range(1, 21):
            b = by_exact.get(a)
            if not b:
                continue
            curve.append({
                "attempt": a,
                "label": f"{a}+" if a == 20 else str(a),
                "dials": int(b["dials"]),
                "live_rate": rate(b["contacts"] + b["dropped"], b["dials"]),
                "amd_rate": rate(b["amd"], b["dials"]),
                "contacts": int(b["contacts"]),
                "sales": int(b["sale"]),
            })

        # Per list, split by attempt bucket
        list_attempts = []
        for lid, bks in by_list_att.items():
            tot = sum(b["dials"] for b in bks.values())
            if not tot:
                continue
            row = {
                "list_id": lid,
                "name": list_names.get(lid) or (f"List {lid}" if lid != "—" else "No list"),
                "dials": int(tot),
                "avg_attempt": list_att_sum[lid] / tot,
                "sales": int(sum(b["sale"] for b in bks.values())),
                "contacts": int(sum(b["contacts"] for b in bks.values())),
                "buckets": {},
            }
            for _, _, label in ATTEMPT_BUCKETS:
                b = bks.get(label)
                if b and b["dials"]:
                    row["buckets"][label] = {
                        "dials": int(b["dials"]),
                        "share": b["dials"] / tot,
                        "live_rate": rate(b["contacts"] + b["dropped"], b["dials"]),
                        "amd_rate": rate(b["amd"], b["dials"]),
                        "sales": int(b["sale"]),
                    }
            old = row["buckets"].get("11+", {})
            row["share_11"] = old.get("share", 0.0)
            list_attempts.append(row)
        list_attempts.sort(key=lambda r: -r["dials"])

        # Headline numbers
        att_dials = sum(x["dials"] for x in curve)
        att_sum = sum(list_att_sum.values())
        sales_total = sum(x["sales"] for x in curve)
        sales_early = sum(x["sales"] for x in curve if x["attempt"] <= 3)
        d11 = by_attempt.get("11+")
        # Suggested cutoff: first attempt after which every later attempt
        # (with enough calls) converts to a live person at under half the
        # 1st-call rate and has produced no sales today.
        first = curve[0]["live_rate"] if curve and curve[0]["attempt"] == 1 else None
        cutoff = None
        if first:
            for i, x in enumerate(curve):
                rest = [y for y in curve[i:] if y["dials"] >= 30]
                if rest and all((y["live_rate"] or 0) < first * 0.5 and y["sales"] == 0 for y in rest):
                    cutoff = x["attempt"] - 1
                    break
        attempt_summary = {
            "avg_attempt": (att_sum / att_dials) if att_dials else None,
            "share_11": rate(d11["dials"], att_dials) if d11 else 0.0,
            "dials_11": int(d11["dials"]) if d11 else 0,
            "sales_11": int(d11["sale"]) if d11 else 0,
            "sales_early_share": rate(sales_early, sales_total),
            "sales_total": int(sales_total),
            "suggested_cutoff": cutoff,
            "dials_past_cutoff": int(sum(x["dials"] for x in curve if cutoff and x["attempt"] > cutoff)),
            "share_past_cutoff": rate(sum(x["dials"] for x in curve if cutoff and x["attempt"] > cutoff), att_dials) if cutoff else None,
        }

        hours = []
        for h in sorted(k for k in by_hour if k >= 0):
            b = by_hour[h]
            hours.append({
                "hour": h,
                "dials": int(b["dials"]),
                "contacts": int(b["contacts"]),
                "sales": int(b["sale"]),
                "contact_rate": rate(b["contacts"], b["dials"]),
                "amd_rate": rate(b["amd"], b["dials"]),
                "drop_rate": rate(b["dropped"], live_answers(b)),
            })

        meta["calls_in_view"] = len(calls)
        return {
            "meta": meta,
            "campaigns": campaigns,
            "kpis": kpis,
            "categories": cats,
            "statuses": statuses,
            "agents": agents,
            "lists": lists,
            "hours": hours,
            "dids": dids,
            "did_count": len(dids),
            "attempts": attempts,
            "attempt_curve": curve,
            "list_attempts": list_attempts,
            "attempt_summary": attempt_summary,
            "did_avg_live_rate": avg_live,
        }
