"""Background citation-chain search: shortest directed path between two
registered URLs of one crawl job.

Design constraints and how they are met:

* Snapshot semantics -- each search runs on a read-only SQLite transaction
  (WAL mode) opened when execution starts.  It sees exactly the corpus
  committed at that moment; later crawls, backfills and retries are invisible
  to it, and the read snapshot never takes a write transaction on the
  business database, so crawling, retries and queries keep working.
* Bounded memory -- the BFS frontier and the visited set (with parent
  pointers) live in a per-search scratch SQLite file, processed in chunks.
  Neither the whole graph nor the search state is held in memory.  The
  scratch file is deleted when the search settles.
* Cancellation -- a per-search event is checked at every expanded node and
  between chunks, so a cancel stops the actual traversal promptly instead of
  waiting for it to finish.  Status transitions are compare-and-set on the
  searches row: if the cancel lands first the worker's result publish fails
  and is discarded (no late results); if completion lands first the cancel
  request is rejected.
* Bounded concurrency -- a fixed pool of worker threads executes searches;
  the rest stay queued.
* Restart -- completed results persist in the business database; searches
  that were queued/running are marked interrupted (see db.recover_searches)
  and their scratch files are removed.  No partial chains are served.
"""

import json
import logging
import os
import queue
import sqlite3
import threading
from urllib.parse import quote

from .db import (
    SEARCH_COMPLETED,
    SEARCH_FAILED,
    SEARCH_NOT_FOUND,
    SEARCH_QUEUED,
    SEARCH_RUNNING,
    Database,
    utcnow,
)

log = logging.getLogger("crawler.search")

STATE_SCHEMA = """
CREATE TABLE visited (
    url    TEXT PRIMARY KEY,
    depth  INTEGER NOT NULL,
    parent TEXT                     -- first-discovered-by URL (NULL for start)
);
CREATE TABLE frontier (
    url   TEXT PRIMARY KEY,
    depth INTEGER NOT NULL
);
"""

TITLE_LOOKUP_CHUNK = 500


class _Abort(Exception):
    """Raised inside the BFS when the search must stop right now."""


class _Runtime:
    __slots__ = ("cancel", "progress")

    def __init__(self):
        self.cancel = threading.Event()
        self.progress = {"depth": 0, "expanded": 0, "visited": 0, "frontier": 0}


class SearchManager:
    def __init__(self, db: Database, db_path: str, state_dir: str,
                 workers: int = 2, chunk_size: int = 200, chunk_delay_ms: int = 0):
        self.db = db
        self._db_path = db_path
        self._state_dir = state_dir
        self._workers = max(1, workers)
        self._chunk = max(1, chunk_size)
        # optional pacing between frontier chunks so cancellation is
        # demonstrable on small corpora; 0 disables it
        self._delay = max(0, chunk_delay_ms) / 1000.0
        self._queue: queue.Queue = queue.Queue()
        self._runtimes: dict[int, _Runtime] = {}
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------ lifecycle

    def start(self):
        os.makedirs(self._state_dir, exist_ok=True)
        # scratch files from before a restart belong to searches that are now
        # marked interrupted and never resume: drop them
        for name in os.listdir(self._state_dir):
            if name.startswith("search_"):
                try:
                    os.unlink(os.path.join(self._state_dir, name))
                except OSError:
                    log.warning("could not remove stale search state %s", name)
        self._threads = [
            threading.Thread(target=self._worker, args=(i,),
                             name=f"search-worker-{i}", daemon=True)
            for i in range(self._workers)
        ]
        for t in self._threads:
            t.start()
        log.info("search manager started: %d worker(s), chunk=%d, delay=%.3fs",
                 self._workers, self._chunk, self._delay)

    def stop(self):
        self._shutdown.set()
        with self._lock:
            runtimes = list(self._runtimes.values())
        for rt in runtimes:  # wake chunk-delay sleeps and BFS loops
            rt.cancel.set()
        for _ in self._threads:
            self._queue.put(None)
        for t in self._threads:
            t.join(timeout=10)

    # ------------------------------------------------------------------ API

    def submit(self, job_id: int, start_url: str, end_url: str) -> int:
        search_id = self.db.create_search(job_id, start_url, end_url)
        with self._lock:
            self._runtimes[search_id] = _Runtime()
        self._queue.put(search_id)
        return search_id

    def cancel(self, search_id: int) -> bool:
        """Accept the cancel only if the search is still queued/running.

        The database compare-and-set is the arbiter of the race with result
        publication: whoever commits first wins.
        """
        if not self.db.cancel_search(search_id):
            return False
        with self._lock:
            rt = self._runtimes.get(search_id)
        if rt is not None:
            rt.cancel.set()  # the worker notices within one node expansion
        return True

    def describe(self, search_id: int):
        row = self.db.get_search(search_id)
        if row is None:
            return None
        return self._payload(row)

    def describe_all(self, job_id: int | None = None):
        return [self._payload(row) for row in self.db.list_searches(job_id)]

    # ------------------------------------------------------------- payloads

    def _payload(self, row: dict) -> dict:
        payload = {
            "id": row["id"],
            "job_id": row["job_id"],
            "start_url": row["start_url"],
            "end_url": row["end_url"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "snapshot_at": row["snapshot_at"],
            "progress": None,
            "result": None,
            "note": row["note"],
        }
        if row["status"] == SEARCH_RUNNING:
            with self._lock:
                rt = self._runtimes.get(row["id"])
            if rt is not None:
                payload["progress"] = dict(rt.progress)
        elif row["status"] != SEARCH_QUEUED and row["expanded"] is not None:
            payload["progress"] = {"expanded": row["expanded"]}
        if row["status"] == SEARCH_COMPLETED and row["chain"]:
            payload["result"] = {"edges": row["edges"], "chain": json.loads(row["chain"])}
        return payload

    # -------------------------------------------------------------- workers

    def _worker(self, idx: int):
        while not self._shutdown.is_set():
            try:
                search_id = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if search_id is None:  # shutdown sentinel
                return
            try:
                self._run_one(search_id)
            except Exception:
                log.exception("search worker %s: unexpected error on search %s",
                              idx, search_id)
            finally:
                with self._lock:
                    self._runtimes.pop(search_id, None)
                self._discard_state(search_id)

    def _run_one(self, search_id: int):
        with self._lock:
            rt = self._runtimes.get(search_id)
        if rt is None or rt.cancel.is_set():
            return  # cancelled while still queued
        if not self.db.claim_search_start(search_id, utcnow()):
            return  # lost the race with a cancel while queued
        row = self.db.get_search(search_id)
        try:
            status, chain, expanded, snapshot_at = self._bfs(row, rt)
        except _Abort:
            log.info("search %s aborted (cancelled or shutdown)", search_id)
            return  # the cancel endpoint already committed 'cancelled';
                    # on shutdown the row stays 'running' and is marked
                    # 'interrupted' at the next startup
        except Exception as exc:
            log.exception("search %s failed", search_id)
            self.db.finish_search(search_id, SEARCH_FAILED,
                                  note=f"internal search error: {type(exc).__name__}: {exc}")
            return

        if status == SEARCH_COMPLETED:
            published = self.db.finish_search(
                search_id, SEARCH_COMPLETED,
                edges=len(chain) - 1, chain=json.dumps(chain), expanded=expanded,
            )
            if not published:
                log.info("search %s: result discarded, cancel landed first", search_id)
        else:
            note = (
                f"no directed citation chain from {row['start_url']} to "
                f"{row['end_url']} in the corpus snapshot of job {row['job_id']} "
                f"committed at {snapshot_at}; the search covers only citations "
                f"fully committed when it started - later crawls, backfills and "
                f"retries are not included"
            )
            self.db.finish_search(search_id, SEARCH_NOT_FOUND,
                                  expanded=expanded, note=note)

    # ------------------------------------------------------------------ BFS

    def _bfs(self, row: dict, rt: _Runtime):
        """Breadth-first search over the stored citation edges.

        Returns (status, chain|None, expanded, snapshot_at).  Raises _Abort.
        """
        search_id, job_id = row["id"], row["job_id"]
        start, end = row["start_url"], row["end_url"]

        state = sqlite3.connect(self._state_path(search_id))
        state.execute("PRAGMA journal_mode=OFF")   # scratch state: no recovery
        state.execute("PRAGMA synchronous=OFF")    # needed, file is deleted after
        state.executescript(STATE_SCHEMA)

        # read-only snapshot of the business db: exactly the committed corpus
        # at execution start, and never a write transaction on it
        snap = sqlite3.connect(f"file:{quote(self._db_path)}?mode=ro",
                               uri=True, isolation_level=None)
        snap.execute("BEGIN")
        snap.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
        snapshot_at = utcnow()
        self.db.set_search_snapshot(search_id, snapshot_at)

        try:
            state.execute("INSERT INTO visited VALUES (?,0,NULL)", (start,))
            state.execute("INSERT INTO frontier VALUES (?,0)", (start,))
            state.commit()
            depth, expanded, visited_n, frontier_n = 0, 0, 1, 1
            rt.progress.update(depth=0, expanded=0, visited=1, frontier=1)

            if start == end:
                # the edge-minimal chain from a page to itself is the page
                return (SEARCH_COMPLETED,
                        self._chain(state, snap, job_id, start, end),
                        expanded, snapshot_at)

            while frontier_n:
                # expand one BFS level, in bounded chunks
                while True:
                    self._check_abort(rt)
                    rows = state.execute(
                        "SELECT url FROM frontier WHERE depth=? ORDER BY rowid LIMIT ?",
                        (depth, self._chunk),
                    ).fetchall()
                    if not rows:
                        break
                    state.executemany("DELETE FROM frontier WHERE url=?",
                                      [(u,) for (u,) in rows])
                    new, found = [], False
                    for (url,) in rows:
                        self._check_abort(rt)
                        expanded += 1
                        for (dst,) in snap.execute(
                            "SELECT dst_url FROM citations WHERE job_id=? AND src_url=?",
                            (job_id, url),
                        ):
                            # cycles are never re-expanded: only the first
                            # discovery of a URL enters visited/frontier
                            cur = state.execute(
                                "INSERT OR IGNORE INTO visited VALUES (?,?,?)",
                                (dst, depth + 1, url),
                            )
                            if cur.rowcount:
                                new.append((dst, depth + 1))
                                if dst == end:
                                    found = True  # first discovery = shortest
                        if found:
                            break
                    if new:
                        state.executemany(
                            "INSERT OR IGNORE INTO frontier VALUES (?,?)", new)
                    state.commit()
                    frontier_n += len(new) - len(rows)
                    visited_n += len(new)
                    rt.progress.update(depth=depth, expanded=expanded,
                                       visited=visited_n, frontier=frontier_n)
                    if self._delay:
                        # interruptible pacing (demo knob for cancellation)
                        if rt.cancel.wait(self._delay) or self._shutdown.is_set():
                            raise _Abort()
                    if found:
                        return (SEARCH_COMPLETED,
                                self._chain(state, snap, job_id, start, end),
                                expanded, snapshot_at)
                depth += 1
            return SEARCH_NOT_FOUND, None, expanded, snapshot_at
        finally:
            state.close()
            snap.close()

    def _chain(self, state, snap, job_id: int, start: str, end: str):
        """Walk parent pointers from end back to start, then attach titles
        read from the same corpus snapshot."""
        urls = [end]
        while urls[-1] != start:
            parent = state.execute(
                "SELECT parent FROM visited WHERE url=?", (urls[-1],)
            ).fetchone()[0]
            urls.append(parent)
        urls.reverse()
        titles = {}
        for i in range(0, len(urls), TITLE_LOOKUP_CHUNK):
            part = urls[i:i + TITLE_LOOKUP_CHUNK]
            marks = ",".join("?" * len(part))
            for u, t in snap.execute(
                f"SELECT url, title FROM papers WHERE job_id=? AND url IN ({marks})",
                [job_id, *part],
            ):
                titles[u] = t
        return [{"url": u, "title": titles.get(u)} for u in urls]

    # ------------------------------------------------------------- helpers

    def _check_abort(self, rt: _Runtime):
        if rt.cancel.is_set() or self._shutdown.is_set():
            raise _Abort()

    def _state_path(self, search_id: int) -> str:
        return os.path.join(self._state_dir, f"search_{search_id}.db")

    def _discard_state(self, search_id: int):
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.unlink(self._state_path(search_id) + suffix)
            except FileNotFoundError:
                pass
            except OSError:
                log.warning("could not remove state of search %s", search_id)
