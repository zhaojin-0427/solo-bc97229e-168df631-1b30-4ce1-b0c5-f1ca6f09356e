#!/usr/bin/env python3
"""Static paper site for local citation-crawl demos.

Serves plain static HTML files from ./site, plus three behaviours needed by
the demo corpus:

* /papers/slow.html    -- served only after a delay, so its (short) path to
                          paper X arrives *after* the long chain registered X
                          at a deeper depth ("late short path").
* /papers/broken.html  -- the FIRST request fails with HTTP 500; later
                          requests succeed ("one request failure", retryable).
* /papers/big/...      -- a procedurally generated big citation graph
                          (hub -> mids -> leaves, leaves link back to the
                          hub) used to demonstrate background chain searches
                          and their cancellation on a larger corpus.
* everything else      -- ordinary static files (text/html for .html).

State is in-memory only; restarting the container resets the flaky page.
"""

import http.server
import os
import re
import socketserver
import threading
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "site")
PORT = int(os.environ.get("PORT", "8080"))

DELAY_SECONDS = {"/papers/slow.html": 2.5}
FLAKY_PATHS = {"/papers/broken.html"}

# procedural big graph: 1 hub + BIG_MIDS mids + BIG_MIDS*BIG_LEAVES leaves
BIG_MIDS = int(os.environ.get("BIG_MIDS", "1500"))
BIG_LEAVES = int(os.environ.get("BIG_LEAVES", "2"))
BIG_HUB = "/papers/big/index.html"
BIG_EXTERNAL = "http://example.org/big-external-reference"

_lock = threading.Lock()
_already_failed: set[str] = set()


def _page(title: str, links: list[str]) -> bytes:
    items = "\n".join(f'    <li><a class="citation" href="{href}">{href}</a></li>'
                      for href in links)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
</head>
<body>
  <h1>{title}</h1>
  <p>Procedurally generated demo paper of the big citation graph.</p>
  <h2>References</h2>
  <ul>
{items}
  </ul>
</body>
</html>
""".encode()


def big_page(path: str):
    """Return the generated HTML for /papers/big/* paths, else None."""
    if path == BIG_HUB:
        links = [f"/papers/big/m/{i:02d}.html" for i in range(BIG_MIDS)]
        links.append(BIG_EXTERNAL)  # registered as external, never fetched
        return _page("BigHub: Procedural Citation Graph Root", links)
    m = re.fullmatch(r"/papers/big/m/(\d+)\.html", path)
    if m and int(m.group(1)) < BIG_MIDS:
        i = int(m.group(1))
        links = [f"/papers/big/l/{i:02d}_{j:02d}.html" for j in range(BIG_LEAVES)]
        return _page(f"BigMid {i:02d}: Fan-out Node", links)
    m = re.fullmatch(r"/papers/big/l/(\d+)_(\d+)\.html", path)
    if m and int(m.group(1)) < BIG_MIDS and int(m.group(2)) < BIG_LEAVES:
        # leaves cite back to the hub: a giant cycle the search must not
        # expand more than once
        return _page(f"BigLeaf {m.group(1)}_{m.group(2)}: End Node", [BIG_HUB])
    return None


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def do_GET(self):
        path = self.path.split("?", 1)[0].split("#", 1)[0]
        if path in FLAKY_PATHS:
            with _lock:
                if path not in _already_failed:
                    _already_failed.add(path)
                    print(f"[papersite] {path} -> 500 (simulated transient failure)", flush=True)
                    self.send_error(500, "Simulated transient failure; a retry will succeed")
                    return
        delay = DELAY_SECONDS.get(path)
        if delay:
            print(f"[papersite] {path} -> delayed {delay}s", flush=True)
            time.sleep(delay)
        body = big_page(path)
        if body is not None:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def log_message(self, fmt, *args):
        print("[papersite] " + (fmt % args), flush=True)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with Server(("", PORT), Handler) as httpd:
        print(f"[papersite] serving {ROOT} on :{PORT}", flush=True)
        httpd.serve_forever()
