"""
Past-day view for /insights: load one full day's call log so the whole page
(dispositions, agents, lists, caller IDs, hours, lead attempts) can show
any earlier day, not just today.

Each loaded day is an InsightsStore with its `day` fixed and its calls filled
once (past days don't change). Rows are normalized on the way in, so lead
names and phone numbers are never kept. The last few days stay cached.
"""

import logging
import threading
from collections import OrderedDict
from datetime import date, datetime, timedelta

from insights import SRC_TZ, TZ, InsightsStore, normalize

log = logging.getLogger("convoso-claude.dayview")

PAGE_SIZE = 1000
MAX_PAGES = 200
CACHE_DAYS = 7


class PastDays:
    def __init__(self, convoso_post, today_store):
        self.convoso_post = convoso_post
        self.today = today_store
        self.lock = threading.Lock()
        self.stores = OrderedDict()     # date -> InsightsStore
        self.status = {}                # date -> {"loading", "rows", "page", "error"}

    def _load(self, day):
        st = self.status[day]
        store = InsightsStore(self.convoso_post)
        store.day = day
        start = datetime(day.year, day.month, day.day, tzinfo=TZ)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        offset, rows, seen = 0, 0, set()
        try:
            for page_no in range(1, MAX_PAGES + 1):
                st["page"] = page_no
                body = None
                for attempt in range(3):
                    try:
                        body = self.convoso_post("/log/retrieve", {
                            "start_time": start.astimezone(SRC_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                            "end_time": end.astimezone(SRC_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                            "limit": PAGE_SIZE,
                            "offset": offset,
                        }, timeout=30)
                        break
                    except Exception as e:
                        log.warning("Day %s page %s attempt %s failed: %s", day, page_no, attempt + 1, e)
                if body is None:
                    raise RuntimeError(f"Convoso didn't answer for page {page_no}")
                data = body.get("data") or {}
                page = data if isinstance(data, list) else (data.get("results") or data.get("entries") or [])
                if not isinstance(page, list):
                    page = []
                first_id = str(page[0].get("id")) if page and isinstance(page[0], dict) else None
                if first_id and first_id in seen:
                    break
                if first_id:
                    seen.add(first_id)
                for raw in page:
                    if not isinstance(raw, dict):
                        continue
                    rec = normalize(raw)
                    if rec["when"] and rec["when"].date() != day:
                        continue
                    key = rec["id"] or f"{rec['when']}|{rec['agent_id']}|{rec['list_id']}|{rec['status']}"
                    store.calls[key] = rec
                    rows += 1
                st["rows"] = rows
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
            with self.today.lock:
                store.list_names = dict(self.today.list_names)
            store.last_refresh = datetime.now(TZ).isoformat()
            with self.lock:
                self.stores[day] = store
                self.stores.move_to_end(day)
                while len(self.stores) > CACHE_DAYS:
                    old, _ = self.stores.popitem(last=False)
                    self.status.pop(old, None)
            st["loading"] = False
            log.info("Day %s loaded: %d calls", day, rows)
        except Exception as e:
            log.exception("Day %s failed", day)
            st["loading"] = False
            st["error"] = str(e)

    def get(self, day):
        """Returns (store or None, status dict). Starts a load if needed."""
        with self.lock:
            store = self.stores.get(day)
            if store is not None:
                self.stores.move_to_end(day)
                return store, {"loading": False}
            st = self.status.get(day)
            if st is None or (not st.get("loading") and st.get("error")):
                st = {"loading": True, "rows": 0, "page": 0, "error": None}
                self.status[day] = st
                threading.Thread(target=self._load, args=(day,), daemon=True).start()
            return None, dict(st)


def parse_day(s):
    """'2026-10-27' -> date, or None for missing / today / future."""
    if not s:
        return None
    try:
        d = date.fromisoformat(s)
    except ValueError:
        return None
    if d >= datetime.now(TZ).date():
        return None
    if d < datetime.now(TZ).date() - timedelta(days=120):
        return None
    return d
