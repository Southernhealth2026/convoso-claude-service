"""
Lead-attempt history: the "Times this lead has been called" view over
7 / 30 / 90 days instead of just today.

Past days never change, so each day is pulled from Convoso's call log once,
reduced to small counts (by attempt #, by list x attempt range, by campaign)
and kept in memory. Raw call rows are not kept. Today comes from the live
InsightsStore so it's always current.

A Render restart clears the cache; the next request for a range re-pulls it
in the background, newest day first, and the page shows partial results
while it fills in.
"""

import logging
import threading
from collections import defaultdict
from datetime import datetime, timedelta

from insights import ATTEMPT_BUCKETS, SRC_TZ, TZ, attempt_bucket, normalize

log = logging.getLogger("convoso-claude.history")

PAGE_SIZE = 1000
MAX_PAGES_PER_DAY = 200
MAX_DAYS = 90


def _counts():
    return defaultdict(float)


def new_agg():
    return {
        "exact": defaultdict(_counts),                              # 1..20 -> counts
        "list_bucket": defaultdict(lambda: defaultdict(_counts)),   # list -> bucket -> counts
        "list_att_sum": defaultdict(float),
        "rows": 0,
    }


def add_record(agg, c):
    if c.get("inbound") or c.get("attempt") is None:
        return
    cat = c["cat"]
    lid = c["list_id"] or "—"
    for b in (agg["exact"][max(1, min(c["attempt"], 20))],
              agg["list_bucket"][lid][attempt_bucket(c["attempt"])]):
        b["dials"] += 1
        b[cat] += 1
        if cat in ("contact", "sale"):
            b["contacts"] += 1
    agg["list_att_sum"][lid] += c["attempt"]
    agg["rows"] += 1


def merge_into(dst, src):
    for a, cnt in src["exact"].items():
        for k, v in cnt.items():
            dst["exact"][a][k] += v
    for lid, bks in src["list_bucket"].items():
        for b, cnt in bks.items():
            for k, v in cnt.items():
                dst["list_bucket"][lid][b][k] += v
    for lid, v in src["list_att_sum"].items():
        dst["list_att_sum"][lid] += v
    dst["rows"] += src["rows"]


def _rate(n, d):
    return (n / d) if d else None


def build_views(agg, list_names):
    """Same shape the page already renders for today."""
    exact = agg["exact"]
    curve = []
    for a in range(1, 21):
        b = exact.get(a)
        if not b or not b["dials"]:
            continue
        curve.append({
            "attempt": a,
            "label": f"{a}+" if a == 20 else str(a),
            "dials": int(b["dials"]),
            "live_rate": _rate(b["contacts"] + b["dropped"], b["dials"]),
            "amd_rate": _rate(b["amd"], b["dials"]),
            "contacts": int(b["contacts"]),
            "sales": int(b["sale"]),
        })
    total_dials = sum(x["dials"] for x in curve)

    # grouped
    grouped = []
    for lo, hi, label in ATTEMPT_BUCKETS:
        rows = [exact[a] for a in exact if lo <= a <= hi]
        d = sum(r["dials"] for r in rows)
        if not d:
            continue
        c = sum(r["contacts"] for r in rows)
        grouped.append({
            "bucket": label,
            "dials": int(d),
            "share": _rate(d, total_dials),
            "contacts": int(c),
            "live_rate": _rate(c + sum(r["dropped"] for r in rows), d),
            "amd_rate": _rate(sum(r["amd"] for r in rows), d),
            "sales": int(sum(r["sale"] for r in rows)),
        })

    # per list
    list_attempts = []
    for lid, bks in agg["list_bucket"].items():
        tot = sum(b["dials"] for b in bks.values())
        if not tot:
            continue
        row = {
            "list_id": lid,
            "name": list_names.get(lid) or (f"List {lid}" if lid != "—" else "No list"),
            "dials": int(tot),
            "avg_attempt": agg["list_att_sum"][lid] / tot,
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
                    "live_rate": _rate(b["contacts"] + b["dropped"], b["dials"]),
                    "amd_rate": _rate(b["amd"], b["dials"]),
                    "sales": int(b["sale"]),
                }
        row["share_11"] = row["buckets"].get("11+", {}).get("share", 0.0)
        list_attempts.append(row)
    list_attempts.sort(key=lambda r: -r["dials"])

    # cutoff: smallest k where everything after attempt k, taken together,
    # converts to sales at under 25% of the 1st-3rd-call rate and reaches a
    # live person at under half the 1st-call rate.
    sales_total = sum(x["sales"] for x in curve)
    early = [x for x in curve if x["attempt"] <= 3]
    early_dials = sum(x["dials"] for x in early)
    early_sales = sum(x["sales"] for x in early)
    early_rate = _rate(early_sales, early_dials) or 0
    first_live = curve[0]["live_rate"] if curve and curve[0]["attempt"] == 1 else None
    cutoff = None
    if first_live and early_dials >= 100:
        for k in range(3, 20):
            rest = [x for x in curve if x["attempt"] > k]
            rd = sum(x["dials"] for x in rest)
            if rd < 100:
                break
            rs = sum(x["sales"] for x in rest)
            rl = sum((x["live_rate"] or 0) * x["dials"] for x in rest) / rd
            sales_ok = (rs == 0) if early_sales < 10 else (rs / rd < 0.25 * early_rate)
            if sales_ok and rl < 0.5 * first_live:
                cutoff = k
                break

    past = [x for x in curve if cutoff and x["attempt"] > cutoff]
    d11 = next((g for g in grouped if g["bucket"] == "11+"), None)
    summary = {
        "avg_attempt": (sum(agg["list_att_sum"].values()) / total_dials) if total_dials else None,
        "share_11": d11["share"] if d11 else 0.0,
        "dials_11": d11["dials"] if d11 else 0,
        "sales_11": d11["sales"] if d11 else 0,
        "sales_early_share": _rate(early_sales, sales_total),
        "sales_total": int(sales_total),
        "suggested_cutoff": cutoff,
        "dials_past_cutoff": int(sum(x["dials"] for x in past)),
        "share_past_cutoff": _rate(sum(x["dials"] for x in past), total_dials) if cutoff else None,
        "sales_past_cutoff": int(sum(x["sales"] for x in past)),
        "total_dials": int(total_dials),
    }
    return {
        "attempts": grouped,
        "attempt_curve": curve,
        "list_attempts": list_attempts,
        "attempt_summary": summary,
    }


class HistoryStore:
    def __init__(self, convoso_post, today_store):
        self.convoso_post = convoso_post
        self.today = today_store
        self.lock = threading.Lock()
        self.days = {}          # date -> {campaign -> agg}
        self.failed = {}        # date -> error text
        self.job = None
        self.wanted = 0
        self.current_day = None

    def _fetch_day(self, day):
        start = datetime(day.year, day.month, day.day, tzinfo=TZ)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        by_camp = defaultdict(new_agg)
        offset = 0
        for _ in range(MAX_PAGES_PER_DAY):
            body = self.convoso_post("/log/retrieve", {
                "start_time": start.astimezone(SRC_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.astimezone(SRC_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "limit": PAGE_SIZE,
                "offset": offset,
            }, timeout=60)
            data = body.get("data") or {}
            page = data if isinstance(data, list) else (data.get("results") or data.get("entries") or [])
            if not isinstance(page, list):
                page = []
            for raw in page:
                if isinstance(raw, dict):
                    rec = normalize(raw)
                    add_record(by_camp[rec["campaign"] or ""], rec)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return dict(by_camp)

    def _run(self):
        try:
            while True:
                today = datetime.now(TZ).date()
                with self.lock:
                    todo = [today - timedelta(days=i) for i in range(1, self.wanted)
                            if (today - timedelta(days=i)) not in self.days
                            and (today - timedelta(days=i)) not in self.failed]
                if not todo:
                    return
                day = todo[0]   # newest first
                with self.lock:
                    self.current_day = day
                try:
                    agg = self._fetch_day(day)
                    with self.lock:
                        self.days[day] = agg
                    log.info("History: %s loaded (%d rows)", day, sum(a["rows"] for a in agg.values()))
                except Exception as e:
                    log.exception("History: %s failed", day)
                    with self.lock:
                        self.failed[day] = str(e)
        finally:
            with self.lock:
                self.job = None
                self.current_day = None

    def ensure(self, days):
        days = max(1, min(int(days), MAX_DAYS))
        with self.lock:
            self.wanted = max(self.wanted, days)
            # retry failures when asked again
            self.failed = {}
            if self.job is None and days > 1:
                self.job = threading.Thread(target=self._run, daemon=True)
                self.job.start()

    def view(self, days, campaign=None):
        days = max(1, min(int(days), MAX_DAYS))
        self.ensure(days)
        today = datetime.now(TZ).date()
        total = new_agg()

        # today, from the live store
        with self.today.lock:
            today_calls = list(self.today.calls.values())
            list_names = dict(self.today.list_names)
        for c in today_calls:
            if campaign and c["campaign"] != campaign:
                continue
            add_record(total, c)

        loaded = 1
        with self.lock:
            for i in range(1, days):
                d = today - timedelta(days=i)
                per = self.days.get(d)
                if per is None:
                    continue
                loaded += 1
                for camp, agg in per.items():
                    if campaign and camp != campaign:
                        continue
                    merge_into(total, agg)
            running = self.job is not None
            current = self.current_day.isoformat() if self.current_day else None
            failed = len([d for d in self.failed if (today - d).days < days])

        out = build_views(total, list_names)
        out["range"] = {
            "days": days,
            "loaded_days": loaded,
            "loading": running and loaded < days,
            "loading_day": current,
            "failed_days": failed,
            "from": (today - timedelta(days=days - 1)).isoformat(),
            "to": today.isoformat(),
        }
        return out
