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
from typing import Optional, Any

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
ELECTION_TIMEOUT_MAX = 0.8
APPEND_RPC_TIMEOUT = 0.35
ELECTION_BUCKETS = 3
ELECTION_BUCKET_SPAN = (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) / ELECTION_BUCKETS

def election_bucket_for_replica(replica_id: str) -> int:
    """Assign each replica a deterministic timeout bucket to avoid split-vote lockstep."""
    digits = "".join(ch for ch in replica_id if ch.isdigit())
    if digits:
        return (int(digits) - 1) % ELECTION_BUCKETS
    return sum(ord(ch) for ch in replica_id) % ELECTION_BUCKETS

ELECTION_TIMEOUT_BUCKET = election_bucket_for_replica(REPLICA_ID)

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
def next_election_timeout() -> float:
    low = ELECTION_TIMEOUT_MIN + (ELECTION_TIMEOUT_BUCKET * ELECTION_BUCKET_SPAN)
    high = low + ELECTION_BUCKET_SPAN
    if ELECTION_TIMEOUT_BUCKET < ELECTION_BUCKETS - 1:
        high -= 0.005
    high = min(high, ELECTION_TIMEOUT_MAX)
    if low >= high:
        low, high = ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX
    return random.uniform(low, high)

election_timeout: float = next_election_timeout()

state_lock = asyncio.Lock()
commit_event = asyncio.Event()
replication_event = asyncio.Event()
replication_lock = asyncio.Lock()
sync_lock = asyncio.Lock()

# ── Helpers ───────────────────────────────────────────────────────────────────
def reset_election_timer():
    global last_heartbeat, election_timeout
    last_heartbeat = time.time()
    election_timeout = next_election_timeout()

def become_follower(term: int, new_leader: Optional[str] = None):
    global role, current_term, voted_for, leader_id
    role = "follower"
    current_term = term
    voted_for = None
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
    replication_event.set()

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

def leader_url_from_id(node_id: Optional[str]) -> Optional[str]:
    if not node_id:
        return None
    for peer in PEERS:
        host = peer.split("//", 1)[-1].split(":", 1)[0]
        if host == node_id:
            return peer
    return None

async def sync_from_leader(leader_node_id: Optional[str], from_index: int):
    """Follower catch-up endpoint caller required by the assignment spec."""
    leader_url = leader_url_from_id(leader_node_id)
    if not leader_url:
        return

    if sync_lock.locked():
        return

    async with sync_lock:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                r = await client.post(f"{leader_url}/sync-log", json={"from_index": from_index})
            if r.status_code != 200:
                return

            body = r.json()
            missing_entries = body.get("entries", [])
            leader_commit = int(body.get("commit_index", -1))
            leader_term = int(body.get("term", current_term))

            async with state_lock:
                if leader_term > current_term:
                    become_follower(leader_term, leader_node_id)

                # Replace tail from from_index onward with leader-provided committed entries.
                safe_from = max(0, from_index)
                if safe_from < len(log_entries):
                    del log_entries[safe_from:]
                log_entries.extend(missing_entries)

                global commit_index
                commit_index = min(leader_commit, len(log_entries) - 1)
                if commit_index >= 0:
                    commit_event.set()
                reset_election_timer()

            if missing_entries:
                log.info(
                    "Sync-log catch-up applied %s entries from %s",
                    len(missing_entries),
                    leader_node_id,
                )
        except Exception as e:
            log.warning(f"sync_from_leader failed: {e}")

async def push_commits():
    """Push committed entries to gateway in batches to reduce HTTP overhead."""
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

        batch_payload = {
            "entries": [
                {"index": idx, "term": entry["term"], "data": entry["data"]}
                for idx, entry in to_push
            ]
        }

        sent = False
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    r = await client.post(f"{GATEWAY_URL}/commit-batch", json=batch_payload)
                    if r.status_code == 200:
                        sent = True
                        break
                    if r.status_code == 404:
                        # Backward compatibility with old gateway versions.
                        for idx, entry in to_push:
                            payload = {"index": idx, "term": entry["term"], "data": entry["data"]}
                            await client.post(f"{GATEWAY_URL}/commit", json=payload)
                        sent = True
                        break
            except Exception as e:
                log.warning(f"commit batch attempt {attempt+1}: {e}")
            await asyncio.sleep(0.05)

        if not sent:
            log.error("Failed to push commit batch to gateway")
            continue

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
    """Send AppendEntries to all peers concurrently, with overlap protection."""
    if role != "leader":
        return
    if replication_lock.locked():
        return

    async with replication_lock:
        if role != "leader":
            return

        async def replicate_to(peer_url: str):
            async with state_lock:
                if role != "leader":
                    return
                ni = next_index.get(peer_url, len(log_entries))
                prev_index = ni - 1
                prev_term = (
                    log_entries[prev_index]["term"]
                    if 0 <= prev_index < len(log_entries)
                    else -1
                )
                entries = [dict(e) for e in log_entries[ni:]]
                term = current_term
                leader_commit = commit_index

            try:
                async with httpx.AsyncClient(timeout=APPEND_RPC_TIMEOUT) as client:
                    r = await client.post(
                        f"{peer_url}/append-entries",
                        json={
                            "term": term,
                            "leader_id": REPLICA_ID,
                            "prev_log_index": prev_index,
                            "prev_log_term": prev_term,
                            "entries": entries,
                            "leader_commit": leader_commit,
                        },
                    )
                if r.status_code != 200:
                    return

                body = r.json()
                if body.get("success"):
                    if entries:
                        new_match = ni + len(entries) - 1
                        async with state_lock:
                            if role == "leader" and current_term == term:
                                match_index[peer_url] = new_match
                                next_index[peer_url] = new_match + 1
                    return

                remote_term = int(body.get("term", 0))
                if remote_term > term:
                    async with state_lock:
                        if remote_term > current_term:
                            become_follower(remote_term)
                    return

                conflict_index = int(body.get("conflict_index", ni - 1))
                async with state_lock:
                    if role == "leader" and current_term == term:
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

async def replication_loop():
    while True:
        try:
            await asyncio.wait_for(replication_event.wait(), timeout=HEARTBEAT_INTERVAL)
        except asyncio.TimeoutError:
            pass
        replication_event.clear()
        if role == "leader":
            await send_heartbeats()

@asynccontextmanager
async def lifespan(app: FastAPI):
    reset_election_timer()
    asyncio.create_task(election_timer_loop())
    asyncio.create_task(replication_loop())
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
    tool: str = "draw"

class StrokeBatchPayload(BaseModel):
    strokes: list[StrokePayload]

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

class HeartbeatPayload(BaseModel):
    term: int
    leader_id: str
    leader_commit: int = -1

class SyncLogRequestPayload(BaseModel):
    from_index: int

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
    # Build visible canvas state from committed log entries after the last clear marker.
    committed_data = [e["data"] for e in log_entries[:commit_index + 1]]
    last_clear_idx = -1
    for i, data in enumerate(committed_data):
        if isinstance(data, dict) and data.get("type") == "clear":
            last_clear_idx = i

    visible = [
        data
        for data in committed_data[last_clear_idx + 1 :]
        if not (isinstance(data, dict) and data.get("type") == "clear")
    ]
    return {"log": visible}

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
    replication_event.set()
    return {"ok": True, "index": new_index}

@app.post("/strokes")
async def receive_strokes(payload: StrokeBatchPayload):
    if role != "leader":
        raise HTTPException(
            status_code=307,
            detail={"error": "not_leader", "leader_id": leader_id,
                    "leader_url": f"http://{leader_id}:{PORT}" if leader_id else None}
        )

    if not payload.strokes:
        return {"ok": True, "count": 0}

    async with state_lock:
        from_index = len(log_entries)
        for stroke in payload.strokes:
            log_entries.append({"term": current_term, "data": stroke.model_dump()})
        to_index = len(log_entries) - 1
        count = len(payload.strokes)
        log.info(f"Appended stroke batch size={count} range={from_index}..{to_index}")

    replication_event.set()
    return {"ok": True, "from_index": from_index, "to_index": to_index, "count": count}

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
                asyncio.create_task(sync_from_leader(payload.leader_id, len(log_entries)))
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

@app.post("/heartbeat")
async def heartbeat(payload: HeartbeatPayload):
    global current_term, leader_id, role, commit_index

    async with state_lock:
        if payload.term < current_term:
            return {"term": current_term, "success": False}

        if payload.term > current_term:
            become_follower(payload.term, payload.leader_id)
        else:
            leader_id = payload.leader_id
            if role == "candidate":
                role = "follower"

        reset_election_timer()
        if payload.leader_commit > commit_index:
            commit_index = min(payload.leader_commit, len(log_entries) - 1)
            if commit_index >= 0:
                commit_event.set()

    return {"term": current_term, "success": True}

@app.post("/sync-log")
async def sync_log(payload: SyncLogRequestPayload):
    if role != "leader":
        raise HTTPException(
            status_code=307,
            detail={"error": "not_leader", "leader_id": leader_id,
                    "leader_url": f"http://{leader_id}:{PORT}" if leader_id else None}
        )

    async with state_lock:
        start = max(0, payload.from_index)
        end = commit_index + 1
        missing = [dict(e) for e in log_entries[start:end]] if end > start else []
        return {
            "term": current_term,
            "leader_id": REPLICA_ID,
            "from_index": start,
            "commit_index": commit_index,
            "entries": missing,
        }

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
        log_entries.append({"term": current_term, "data": {"type": "clear"}})
        new_index = len(log_entries) - 1
        log.info(f"Appended clear marker index={new_index}")
    replication_event.set()
    return {"ok": True, "index": new_index}
