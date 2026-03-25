"""
RAFT Replica Node - Real-time Drawing Board
Implements: leader election, log replication, commit + push to gateway
"""
import asyncio
import os
import logging
import random
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s"
)
log = logging.getLogger(os.environ.get("REPLICA_ID", "replica"))

# ── Config from environment ────────────────────────────────────────────────────
REPLICA_ID: str = os.environ.get("REPLICA_ID", "replica1")
PORT: int = int(os.environ.get("PORT", 4001))
PEERS: list[str] = [p for p in os.environ.get("PEERS", "").split(",") if p]
GATEWAY_URL: str = os.environ.get("GATEWAY_URL", "http://gateway:3000")

# Timing constants (all in seconds)
HEARTBEAT_INTERVAL = 0.15          # leader sends heartbeats every 150ms
ELECTION_TIMEOUT_MIN = 0.5
ELECTION_TIMEOUT_MAX = 0.9

# ── RAFT State ─────────────────────────────────────────────────────────────────
role: str = "follower"
current_term: int = 0
voted_for: Optional[str] = None
leader_id: Optional[str] = None

log_entries: list[dict] = []       # [{term, data}, ...]
commit_index: int = -1             # highest committed entry index
last_applied: int = -1             # highest entry pushed to gateway

# Leader-only state
next_index: dict[str, int] = {}
match_index: dict[str, int] = {}

# Election timer
last_heartbeat: float = time.time()
election_timeout: float = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

state_lock = asyncio.Lock()
commit_event = asyncio.Event()

# ── Helpers ───────────────────────────────────────────────────────────────────
def reset_election_timer():
    global last_heartbeat, election_timeout
    last_heartbeat = time.time()
    election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

def become_follower(term: int, new_leader: Optional[str] = None):
    global role, current_term, voted_for, leader_id
    role = "follower"
    current_term = term
    voted_for = None
    if new_leader:
        leader_id = new_leader
    reset_election_timer()
    log.info(f"→ FOLLOWER  term={current_term}  leader={leader_id}")

async def become_leader():
    global role, leader_id, next_index, match_index
    role = "leader"
    leader_id = REPLICA_ID
    next_index = {p: len(log_entries) for p in PEERS}
    match_index = {p: -1 for p in PEERS}
    log.info(f"★ LEADER  term={current_term}")
    asyncio.create_task(notify_gateway_leader())
    asyncio.create_task(send_heartbeats())

# ── Gateway notifications ──────────────────────────────────────────────────────
async def notify_gateway_leader():
    payload = {
        "leader_id": REPLICA_ID,
        "leader_url": f"http://{REPLICA_ID}:{PORT}",
        "term": current_term,
    }
    for attempt in range(5):
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                r = await client.post(f"{GATEWAY_URL}/notify-leader", json=payload)
                if r.status_code == 200:
                    log.info("Notified gateway of leadership")
                    return
        except Exception as e:
            log.warning(f"notify_gateway attempt {attempt+1}: {e}")
        await asyncio.sleep(0.5)

async def push_commits():
    """
    Push committed entries to gateway CONCURRENTLY (batched per wakeup).
    Fixes the high-latency bug: no more sequential one-by-one pushes.
    """
    global last_applied
    while True:
        await commit_event.wait()
        commit_event.clear()

        # Snapshot what needs pushing under the lock
        async with state_lock:
            to_push = []
            while last_applied < commit_index:
                last_applied += 1
                entry = log_entries[last_applied]
                to_push.append((last_applied, dict(entry)))

        if not to_push:
            continue

        async def push_one(idx: int, entry: dict):
            payload = {"index": idx, "term": entry["term"], "data": entry["data"]}
            for attempt in range(3):
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        r = await client.post(f"{GATEWAY_URL}/commit", json=payload)
                        if r.status_code == 200:
                            return
                except Exception as e:
                    log.warning(f"commit push idx={idx} attempt {attempt+1}: {e}")
                await asyncio.sleep(0.05)
            log.error(f"Failed to push commit idx={idx} after 3 attempts")

        # All concurrent — no sequential waiting
        await asyncio.gather(*[push_one(idx, entry) for idx, entry in to_push])
        log.info(f"Pushed {len(to_push)} commits (up to index={to_push[-1][0]})")

# ── Election ───────────────────────────────────────────────────────────────────
async def run_election():
    global role, current_term, voted_for

    async with state_lock:
        if role == "leader":
            return
        role = "candidate"
        current_term += 1
        voted_for = REPLICA_ID
        term_for_election = current_term
        last_log_index = len(log_entries) - 1
        last_log_term = log_entries[-1]["term"] if log_entries else -1
        reset_election_timer()

    log.info(f"⚡ ELECTION  term={term_for_election}")
    votes = 1
    total = len(PEERS) + 1
    majority = total // 2 + 1

    async def request_vote(peer_url: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.post(f"{peer_url}/request-vote", json={
                    "term": term_for_election,
                    "candidate_id": REPLICA_ID,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                })
                if r.status_code == 200:
                    body = r.json()
                    if body.get("vote_granted"):
                        return True
                    if body.get("term", 0) > term_for_election:
                        async with state_lock:
                            become_follower(body["term"])
        except Exception as e:
            log.debug(f"vote request to {peer_url}: {e}")
        return False

    results = await asyncio.gather(*[request_vote(p) for p in PEERS])
    votes += sum(results)

    async with state_lock:
        if role == "candidate" and current_term == term_for_election:
            if votes >= majority:
                await become_leader()
            else:
                log.info(f"Election lost ({votes}/{total} votes)")
                role = "follower"

# ── Replication ────────────────────────────────────────────────────────────────
async def send_heartbeats():
    """Send AppendEntries to all peers concurrently."""
    async def replicate_to(peer_url: str):
        global commit_index
        ni = next_index.get(peer_url, len(log_entries))
        prev_index = ni - 1
        prev_term = log_entries[prev_index]["term"] if 0 <= prev_index < len(log_entries) else -1
        entries = log_entries[ni:]

        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.post(f"{peer_url}/append-entries", json={
                    "term": current_term,
                    "leader_id": REPLICA_ID,
                    "prev_log_index": prev_index,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": commit_index,
                })
                if r.status_code == 200:
                    body = r.json()
                    if body.get("success"):
                        new_match = ni + len(entries) - 1
                        if entries:
                            async with state_lock:
                                match_index[peer_url] = new_match
                                next_index[peer_url] = new_match + 1
                    else:
                        if body.get("term", 0) > current_term:
                            async with state_lock:
                                become_follower(body["term"])
                        else:
                            async with state_lock:
                                conflict_index = body.get("conflict_index", ni - 1)
                                next_index[peer_url] = max(0, conflict_index)
        except Exception as e:
            log.debug(f"replicate to {peer_url}: {e}")

    await asyncio.gather(*[replicate_to(p) for p in PEERS])
    await advance_commit()

async def advance_commit():
    global commit_index
    if role != "leader" or not log_entries:
        return
    async with state_lock:
        majority = (len(PEERS) + 1) // 2 + 1
        # Scan from highest down; find the highest index that belongs to the
        # current term and is replicated on a majority. Lower indices are
        # implicitly committed by RAFT's log-matching property.
        new_commit = commit_index
        for n in range(len(log_entries) - 1, commit_index, -1):
            if log_entries[n]["term"] != current_term:
                continue  # cannot directly commit entries from old terms
            replicated = 1 + sum(1 for mi in match_index.values() if mi >= n)
            if replicated >= majority:
                new_commit = n
                break  # highest safe index found
        if new_commit > commit_index:
            commit_index = new_commit
            log.info(f"Committed index={commit_index}")
            commit_event.set()

# ── Background loops ───────────────────────────────────────────────────────────
_election_running = False  # guard against concurrent elections

async def election_timer_loop():
    global _election_running
    while True:
        await asyncio.sleep(0.05)
        if role == "leader":
            continue
        if _election_running:
            continue
        if time.time() - last_heartbeat >= election_timeout:
            _election_running = True
            try:
                await run_election()
            finally:
                _election_running = False

async def heartbeat_loop():
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        if role == "leader":
            asyncio.create_task(send_heartbeats())

@asynccontextmanager
async def lifespan(app: FastAPI):
    reset_election_timer()
    asyncio.create_task(election_timer_loop())
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(push_commits())
    log.info(f"Replica {REPLICA_ID} started on port {PORT}  peers={PEERS}")
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Pydantic schemas ───────────────────────────────────────────────────────────
class StrokePayload(BaseModel):
    model_config = {"extra": "allow"}   # pass clientId, strokeId etc. through unchanged
    x0: float
    y0: float
    x1: float
    y1: float
    color: str = "#000000"
    width: float = 3

class AppendEntriesPayload(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict]
    leader_commit: int

class RequestVotePayload(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/status")
async def status():
    return {
        "id": REPLICA_ID,
        "role": role,
        "term": current_term,
        "leader_id": leader_id,
        "log_length": len(log_entries),
        "commit_index": commit_index,
        "last_applied": last_applied,
        "peers": PEERS,
    }

@app.get("/log")
async def get_log():
    # Return committed strokes as plain objects — gateway sends these as msg.log
    # Frontend iterates: (msg.log || []).forEach(s => drawStroke(s))
    return {"log": [e["data"] for e in log_entries[:commit_index + 1]]}

@app.post("/stroke")
async def receive_stroke(payload: StrokePayload):
    if role != "leader":
        raise HTTPException(
            status_code=307,
            detail={"error": "not_leader", "leader_id": leader_id,
                    "leader_url": f"http://{leader_id}:{PORT}" if leader_id else None}
        )
    async with state_lock:
        entry = {"term": current_term, "data": payload.model_dump()}
        log_entries.append(entry)
        new_index = len(log_entries) - 1
        log.info(f"Appended stroke index={new_index}")
    asyncio.create_task(send_heartbeats())
    return {"ok": True, "index": new_index}

@app.post("/append-entries")
async def append_entries(payload: AppendEntriesPayload):
    global current_term, commit_index, leader_id, role

    async with state_lock:
        if payload.term < current_term:
            return {"term": current_term, "success": False}

        if payload.term > current_term:
            become_follower(payload.term, payload.leader_id)
        else:
            reset_election_timer()
            leader_id = payload.leader_id
            if role == "candidate":
                role = "follower"
                log.info(f"→ FOLLOWER (leader seen)  term={current_term}")

        # Log consistency check
        if payload.prev_log_index >= 0:
            if payload.prev_log_index >= len(log_entries):
                return {"term": current_term, "success": False,
                        "conflict_index": len(log_entries)}
            if log_entries[payload.prev_log_index]["term"] != payload.prev_log_term:
                ct = log_entries[payload.prev_log_index]["term"]
                ci = payload.prev_log_index
                while ci > 0 and log_entries[ci - 1]["term"] == ct:
                    ci -= 1
                return {"term": current_term, "success": False, "conflict_index": ci}

        # Append entries
        insert_at = payload.prev_log_index + 1
        for i, entry in enumerate(payload.entries):
            idx = insert_at + i
            if idx < len(log_entries):
                if log_entries[idx]["term"] != entry["term"]:
                    del log_entries[idx:]
                    log_entries.append(entry)
            else:
                log_entries.append(entry)

        # Advance commit
        if payload.leader_commit > commit_index:
            commit_index = min(payload.leader_commit, len(log_entries) - 1)
            if commit_index >= 0:
                commit_event.set()

    return {"term": current_term, "success": True}

@app.post("/request-vote")
async def request_vote(payload: RequestVotePayload):
    global current_term, voted_for

    async with state_lock:
        if payload.term < current_term:
            return {"term": current_term, "vote_granted": False}

        if payload.term > current_term:
            become_follower(payload.term)

        my_last_index = len(log_entries) - 1
        my_last_term = log_entries[-1]["term"] if log_entries else -1

        log_ok = (
            payload.last_log_term > my_last_term or
            (payload.last_log_term == my_last_term and payload.last_log_index >= my_last_index)
        )
        can_vote = (voted_for is None or voted_for == payload.candidate_id)

        if can_vote and log_ok:
            voted_for = payload.candidate_id
            reset_election_timer()
            log.info(f"Voted for {payload.candidate_id}  term={payload.term}")
            return {"term": current_term, "vote_granted": True}

        return {"term": current_term, "vote_granted": False}

@app.post("/clear")
async def clear_log():
    if role != "leader":
        raise HTTPException(status_code=307, detail={"error": "not_leader"})
    async with state_lock:
        log_entries.clear()
        global commit_index, last_applied
        commit_index = -1
        last_applied = -1
    asyncio.create_task(send_heartbeats())
    return {"ok": True}
