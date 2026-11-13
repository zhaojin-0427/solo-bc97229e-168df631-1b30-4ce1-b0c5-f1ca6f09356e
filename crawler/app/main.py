"""FastAPI surface: submit jobs, watch status, inspect failures, query the
collected papers and citations, retry failed pages, and run background
citation-chain searches between two registered URLs.

Interactive API docs (Swagger UI) are served at /docs.
"""

import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import urldefrag, urlparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .crawler import CrawlManager
from .db import JOB_RUNNING, Database
from .search import SearchManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

DB_PATH = os.environ.get("DATABASE_PATH", "/data/crawler.db")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

db = Database(DB_PATH)
manager = CrawlManager(db)
search_manager = SearchManager(
    db,
    DB_PATH,
    state_dir=os.environ.get(
        "SEARCH_STATE_DIR", os.path.join(os.path.dirname(DB_PATH) or ".", "search_state")
    ),
    workers=int(os.environ.get("SEARCH_WORKERS", "2")),
    chunk_size=int(os.environ.get("SEARCH_CHUNK_SIZE", "100")),
    chunk_delay_ms=int(os.environ.get("SEARCH_CHUNK_DELAY_MS", "200")),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.start()
    search_manager.start()
    interrupted = db.recover_searches()
    if interrupted:
        logging.getLogger("crawler").info(
            "marked %s unfinished search(es) as interrupted", interrupted
        )
    # auto-resume every job that was interrupted mid-crawl
    for job_id in db.recover():
        logging.getLogger("crawler").info("resuming interrupted job %s", job_id)
        manager.ensure_workers(job_id)
    yield
    search_manager.stop()
    await manager.stop()


app = FastAPI(
    title="Citation Corpus Collector",
    version="1.1.0",
    description=(
        "Collects a citation corpus from a static paper site. Submit a seed URL "
        "and a max depth (0-4, seed = depth 0); the crawler fetches same-origin, "
        "redirect-free static HTML pages with two slots per job, persists pages, "
        "citations and pending progress in SQLite, resumes after restarts and "
        "never reports completion while failures remain. On top of a collected "
        "corpus, background searches find a shortest directed citation chain "
        "between two registered URLs: each search runs on a consistent snapshot "
        "of the corpus committed when it starts, keeps its traversal state on "
        "disk (never in memory, never in a business-db write transaction), "
        "reports progress, and supports prompt cancellation."
    ),
    lifespan=lifespan,
)


class JobCreate(BaseModel):
    seed_url: str = Field(..., examples=["http://papersite:8080/papers/p0.html"])
    max_depth: int = Field(..., ge=0, le=4, examples=[4])


class PathSearchCreate(BaseModel):
    start_url: str = Field(..., examples=["http://papersite:8080/papers/p0.html"])
    end_url: str = Field(..., examples=["http://papersite:8080/papers/Z.html"])


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.post("/jobs", status_code=201, summary="Create a crawl job")
async def create_job(body: JobCreate):
    seed_url = urldefrag(body.seed_url.strip())[0]
    parsed = urlparse(seed_url)
    if parsed.scheme.lower() not in ("http", "https") or not parsed.netloc:
        raise HTTPException(422, "seed_url must be an absolute http(s) URL")
    origin = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    job_id = db.create_job(seed_url, origin, body.max_depth)
    manager.ensure_workers(job_id)
    return _job_payload(job_id)


@app.get("/jobs", summary="List all jobs")
async def list_jobs():
    return [_job_payload(j["id"]) for j in db.list_jobs()]


@app.get("/jobs/{job_id}", summary="Job status and progress")
async def job_detail(job_id: int):
    return _job_payload(job_id)


@app.post("/jobs/{job_id}/retry", summary="Re-queue failed pages and resume the job")
async def retry_job(job_id: int):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["status"] == JOB_RUNNING:
        raise HTTPException(409, "job is still running")
    result = db.retry_job(job_id)
    if result["requeued"]:
        manager.ensure_workers(job_id)
    return {"job_id": job_id, **result, "job": _job_payload(job_id)}


@app.get("/jobs/{job_id}/failures", summary="Failed URLs of a job")
async def job_failures(job_id: int):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "failures": db.list_failures(job_id)}


@app.get("/jobs/{job_id}/papers", summary="Papers registered by a job")
async def job_papers(job_id: int, status: str | None = None, depth: int | None = None):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "papers": db.list_papers(job_id, status=status, depth=depth)}


@app.get("/jobs/{job_id}/citations", summary="Citation edges collected by a job")
async def job_citations(job_id: int, src: str | None = None):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return {"job_id": job_id, "citations": db.list_citations(job_id, src=src)}


# ---------------------------------------------------------- chain searches


@app.post(
    "/jobs/{job_id}/paths",
    status_code=201,
    summary="Start a background search for a shortest citation chain",
)
async def create_path_search(job_id: int, body: PathSearchCreate):
    """Both URLs must already be registered in this job's corpus.  The search
    runs in the background on the corpus snapshot committed when execution
    starts; poll `GET /paths/{id}` for progress and the result."""
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    start_url = urldefrag(body.start_url.strip())[0]
    end_url = urldefrag(body.end_url.strip())[0]
    for label, url in (("start_url", start_url), ("end_url", end_url)):
        if not db.paper_registered(job_id, url):
            raise HTTPException(422, f"{label} is not registered in job {job_id}: {url}")
    search_id = search_manager.submit(job_id, start_url, end_url)
    return search_manager.describe(search_id)


@app.get("/paths", summary="List citation-chain searches")
async def list_path_searches(job_id: int | None = None):
    return search_manager.describe_all(job_id)


@app.get("/paths/{search_id}", summary="Search status, live progress and result")
async def path_search_detail(search_id: int):
    payload = search_manager.describe(search_id)
    if payload is None:
        raise HTTPException(404, "search not found")
    return payload


@app.post("/paths/{search_id}/cancel", summary="Cancel a queued or running search")
async def cancel_path_search(search_id: int):
    """Cancellation is accepted immediately and stops the actual traversal
    (within one node expansion), it does not wait for the search to finish.
    If the search already reached a final state, the cancel is rejected."""
    if db.get_search(search_id) is None:
        raise HTTPException(404, "search not found")
    if not search_manager.cancel(search_id):
        status = db.get_search(search_id)["status"]
        raise HTTPException(
            409, f"search is already {status}; only queued/running searches can be cancelled"
        )
    return search_manager.describe(search_id)


def _job_payload(job_id: int) -> dict:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    counts = db.job_counts(job_id)
    return {**job, "counts": counts["by_status"], "papers_by_depth": counts["by_depth"]}
