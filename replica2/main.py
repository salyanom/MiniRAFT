"""
RAFT Replica Node
Member 3 — Replication Specialist
Owns: stroke log, /append-entries RPC, commit push, sync on restart
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
    format="%(asctime)s [replica-%(name)s] %(message)s"
)

# ── Environment (injected by docker-compose) ──────────────────────────────────
REPLICA_ID: str     = os.environ.get("ID", "1")          # "1", "2", or "3"
GATEWAY_URL: str    = os.environ.get("GATEWAY_URL", "http://gateway:8000")

# Each replica knows the URLs of its two peers
PEER_MAP = {
    "1": ["http://replica2:8002", "http://replica3:8003"],
    "2": ["http://replica1:8001", "http://replica3:8003"],
    "3": ["http://replica1:8001", "http://replica2:8002"],
}
PEERS: list[str] = PEER_MAP.get(REPLICA_ID, [])

log = logging.getLogger(REPLICA_ID)

# ── Timing ────────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL   = 0.15   # 150ms
ELECTION_TIMEOUT_MIN = 0.5    # 500ms
ELECTION_TIMEOUT_MAX = 0.9    # 900ms

# ── RAFT State ────────────────────────────────────────────────────────────────
state:        str           = "follower"    # "follower" | "candidate" | "leader"
current_term: int           = 0
voted_for:    Optional[str] = None
leader_id:    Optional[str] = None

# ── Stroke Log (Member 3 owns this) ──────────────────────────────────────────
# Each entry: {"term": int, "data": {x0,y0,x1,y1,color,width,...}}
log_entries:  list[dict]    = []
commit_index: int           = -1   # highest index committed by majority
last_applied: int           = -1   # highest index already pushed to gateway

# ── Leader replication tracking ───────────────────────────────────────────────
next_index:  dict[str, int] = {}   # next log index to send to each peer
match_index: dict[str, int] = {}   # highest confirmed match per peer

# ── Concurrency ───────────────────────────────────────────────────────────────
last_heartbeat:   float = time.time()
election_timeout: float = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
state_lock              = asyncio.Lock()
commit_event            = asyncio.Event()

# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def reset_election_timer():
    global last_heartbeat, election_timeout
    last_heartbeat   = time.time()
    election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)


def become_follower(term: int, new_leader: Optional[str] = None):
    global state, current_term, voted_for, leader_id
    state        = "follower"
    current_term = term
    voted_for    = None
    if new_leader:
        leader_id = new_leader
    reset_election_timer()
    log.info(f"→ FOLLOWER  term={current_term}  leader={leader_id}")


async def become_leader():
    global state, leader_id, next_index, match_index
    state     = "leader"
    leader_id = REPLICA_ID
    next_index  = {p: len(log_entries) for p in PEERS}
    match_index = {p: -1 for p in PEERS}
    log.info(f"★ LEADER  term={current_term}")
    # Immediately send heartbeats so followers stop their election timers
    asyncio.create_task(send_heartbeats())


# ═════════════════════════════════════════════════════════════════════════════
# COMMIT PUSH TO GATEWAY  (Member 3 owns this)
# ═════════════════════════════════════════════════════════════════════════════

async def push_commits():
    """
    Runs forever in the background.
    Wakes up when commit_event is set (by /append-entries or advance_commit).
    Pushes every newly committed stroke to the gateway's /committed-stroke endpoint.
    All pushes in one batch are sent CONCURRENTLY.
    """
    global last_applied
    while True:
        await commit_event.wait()
        commit_event.clear()

        async with state_lock:
            to_push = []
            while last_applied < commit_index:
                last_applied += 1
                entry = log_entries[last_applied]
                to_push.append(dict(entry))   # snapshot

        if not to_push:
            continue

        async def push_one(entry: dict):
            # Gateway /committed-stroke expects: {"stroke": {stroke data}}
            payload = {"stroke": entry["data"]}
            for attempt in range(3):
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        r = await client.post(
                            f"{GATEWAY_URL}/committed-stroke",
                            json=payload
                        )
                        if r.status_code == 200:
                            return
                except Exception as e:
                    log.warning(f"push_commit attempt {attempt+1}: {e}")
                await asyncio.sleep(0.05)
            log.error(f"Failed to push commit after 3 attempts")

        await asyncio.gather(*[push_one(e) for e in to_push])
        log.info(f"Pushed {len(to_push)} committed stroke(s) to gateway")


# ═════════════════════════════════════════════════════════════════════════════
# ELECTION
# ═════════════════════════════════════════════════════════════════════════════

async def run_election():
    global state, current_term, voted_for

    async with state_lock:
        if state == "leader":
            return
        state         = "candidate"
        current_term += 1
        voted_for     = REPLICA_ID
        term_snap     = current_term
        last_log_idx  = len(log_entries) - 1
        last_log_term = log_entries[-1]["term"] if log_entries else -1
        reset_election_timer()

    log.info(f"⚡ ELECTION  term={term_snap}")
    votes    = 1
    majority = (len(PEERS) + 1) // 2 + 1

    async def request_vote(peer_url: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.post(f"{peer_url}/request-vote", json={
                    "term":           term_snap,
                    "candidate_id":   REPLICA_ID,
                    "last_log_index": last_log_idx,
                    "last_log_term":  last_log_term,
                })
                if r.status_code == 200:
                    body = r.json()
                    if body.get("vote_granted"):
                        return True
                    if body.get("term", 0) > term_snap:
                        async with state_lock:
                            become_follower(body["term"])
        except Exception as e:
            log.debug(f"vote req to {peer_url}: {e}")
        return False

    results = await asyncio.gather(*[request_vote(p) for p in PEERS])
    votes  += sum(results)

    async with state_lock:
        if state == "candidate" and current_term == term_snap:
            if votes >= majority:
                await become_leader()
            else:
                log.info(f"Election lost ({votes}/{len(PEERS)+1})")
                state = "follower"


# ═════════════════════════════════════════════════════════════════════════════
# REPLICATION  (Member 3 owns everything in this section)
# ═════════════════════════════════════════════════════════════════════════════

async def replicate_to(peer_url: str):
    """
    Send AppendEntries to one peer.
    - If entries=[] it is a pure heartbeat.
    - On success: update match_index and next_index.
    - On failure: use conflict_index hint to jump back (fast log backup).
    """
    ni         = next_index.get(peer_url, len(log_entries))
    prev_index = ni - 1
    prev_term  = log_entries[prev_index]["term"] if 0 <= prev_index < len(log_entries) else -1
    entries    = log_entries[ni:]

    try:
        async with httpx.AsyncClient(timeout=1.0) as client:
            r = await client.post(f"{peer_url}/append-entries", json={
                "term":           current_term,
                "leader_id":      REPLICA_ID,
                "prev_log_index": prev_index,
                "prev_log_term":  prev_term,
                "entries":        entries,
                "leader_commit":  commit_index,
            })

        if r.status_code != 200:
            return

        body = r.json()
        if body.get("success"):
            if entries:
                async with state_lock:
                    new_match             = ni + len(entries) - 1
                    match_index[peer_url] = new_match
                    next_index[peer_url]  = new_match + 1
        else:
            if body.get("term", 0) > current_term:
                async with state_lock:
                    become_follower(body["term"])
            else:
                # Fast log backup — jump directly to conflict_index
                async with state_lock:
                    ci = body.get("conflict_index", ni - 1)
                    next_index[peer_url] = max(0, ci)

    except Exception as e:
        log.debug(f"replicate_to {peer_url}: {e}")


async def advance_commit():
    """
    After replication round, check if any new index has majority coverage.
    Only commit entries from current term (RAFT safety rule).
    """
    global commit_index
    if state != "leader" or not log_entries:
        return

    async with state_lock:
        majority   = (len(PEERS) + 1) // 2 + 1
        new_commit = commit_index

        for n in range(len(log_entries) - 1, commit_index, -1):
            if log_entries[n]["term"] != current_term:
                continue
            replicated = 1 + sum(1 for mi in match_index.values() if mi >= n)
            if replicated >= majority:
                new_commit = n
                break

        if new_commit > commit_index:
            commit_index = new_commit
            log.info(f"Committed up to index={commit_index}")
            commit_event.set()


async def send_heartbeats():
    """Replicate to all peers concurrently, then check if commit can advance."""
    await asyncio.gather(*[replicate_to(p) for p in PEERS])
    await advance_commit()


# ═════════════════════════════════════════════════════════════════════════════
# BACKGROUND LOOPS
# ═════════════════════════════════════════════════════════════════════════════

_election_running = False

async def election_timer_loop():
    global _election_running
    while True:
        await asyncio.sleep(0.05)
        if state == "leader" or _election_running:
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
        if state == "leader":
            asyncio.create_task(send_heartbeats())


@asynccontextmanager
async def lifespan(app: FastAPI):
    reset_election_timer()
    asyncio.create_task(election_timer_loop())
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(push_commits())
    log.info(f"Replica {REPLICA_ID} started — peers={PEERS}  gateway={GATEWAY_URL}")
    yield


# ═════════════════════════════════════════════════════════════════════════════
# FASTAPI APP + ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class StrokePayload(BaseModel):
    model_config = {"extra": "allow"}
    x0: float
    y0: float
    x1: float
    y1: float
    color: str  = "#000000"
    width: float = 3


class AppendEntriesPayload(BaseModel):
    term:           int
    leader_id:      str
    prev_log_index: int
    prev_log_term:  int
    entries:        list[dict]
    leader_commit:  int


class RequestVotePayload(BaseModel):
    term:           int
    candidate_id:   str
    last_log_index: int
    last_log_term:  int


# ── GET /status ───────────────────────────────────────────────────────────────
# IMPORTANT: gateway checks data["state"] == "leader" — must use "state" not "role"

@app.get("/status")
async def status():
    return {
        "id":           REPLICA_ID,
        "state":        state,          # "follower" | "candidate" | "leader"
        "term":         current_term,
        "leader_id":    leader_id,
        "log_length":   len(log_entries),
        "commit_index": commit_index,
        "last_applied": last_applied,
    }


# ── GET /log ──────────────────────────────────────────────────────────────────
# Returns all committed strokes as plain data — for canvas replay on new clients

@app.get("/log")
async def get_log():
    return {
        "log": [e["data"] for e in log_entries[:commit_index + 1]]
    }


# ── POST /stroke ──────────────────────────────────────────────────────────────
# Gateway sends drawing strokes here. Leader only — returns 307 if not leader.

@app.post("/stroke")
async def receive_stroke(payload: StrokePayload):
    if state != "leader":
        raise HTTPException(
            status_code=307,
            detail={
                "error":      "not_leader",
                "leader_id":  leader_id,
                "leader_url": f"http://replica{leader_id}:{8000 + int(leader_id)}" if leader_id else None,
            }
        )
    async with state_lock:
        entry     = {"term": current_term, "data": payload.model_dump()}
        log_entries.append(entry)
        new_index = len(log_entries) - 1
        log.info(f"Stroke appended at index={new_index}  term={current_term}")

    # Replicate immediately — don't wait for next heartbeat tick
    asyncio.create_task(send_heartbeats())
    return {"ok": True, "index": new_index}


# ── POST /append-entries ──────────────────────────────────────────────────────
# THE core Member 3 endpoint.
# Leader calls this on followers for heartbeats, replication, and commit advance.
# Sync-on-restart is handled automatically via the conflict_index hint.

@app.post("/append-entries")
async def append_entries(payload: AppendEntriesPayload):
    global current_term, commit_index, leader_id, state

    async with state_lock:

        # 1. Reject stale leader
        if payload.term < current_term:
            return {"term": current_term, "success": False}

        # 2. Step down if higher term seen
        if payload.term > current_term:
            become_follower(payload.term, payload.leader_id)
        else:
            reset_election_timer()
            leader_id = payload.leader_id
            if state == "candidate":
                state = "follower"
                log.info(f"→ FOLLOWER (leader seen)  term={current_term}")

        # 3. Log consistency check — also handles sync-on-restart
        #
        #    When a node restarts with empty log:
        #      prev_log_index will be > 0  →  Case A fires
        #      returns conflict_index = 0
        #      leader retries from index 0 sending the full log
        #      follower catches up in one AppendEntries call
        if payload.prev_log_index >= 0:

            # Case A: our log is shorter than leader expects
            if payload.prev_log_index >= len(log_entries):
                return {
                    "term":           current_term,
                    "success":        False,
                    "conflict_index": len(log_entries),
                }

            # Case B: term mismatch at that position
            if log_entries[payload.prev_log_index]["term"] != payload.prev_log_term:
                conflict_term = log_entries[payload.prev_log_index]["term"]
                ci = payload.prev_log_index
                while ci > 0 and log_entries[ci - 1]["term"] == conflict_term:
                    ci -= 1
                return {
                    "term":           current_term,
                    "success":        False,
                    "conflict_index": ci,
                }

        # 4. Append new entries (truncate on conflict)
        insert_at = payload.prev_log_index + 1
        for i, entry in enumerate(payload.entries):
            idx = insert_at + i
            if idx < len(log_entries):
                if log_entries[idx]["term"] != entry["term"]:
                    log.info(f"Truncating log at index={idx} (term conflict)")
                    del log_entries[idx:]
                    log_entries.append(entry)
            else:
                log_entries.append(entry)

        # 5. Advance commit_index and wake push_commits()
        if payload.leader_commit > commit_index:
            commit_index = min(payload.leader_commit, len(log_entries) - 1)
            if commit_index >= 0:
                log.info(f"Commit index → {commit_index}")
                commit_event.set()

    return {"term": current_term, "success": True}


# ── POST /request-vote ────────────────────────────────────────────────────────

@app.post("/request-vote")
async def request_vote(payload: RequestVotePayload):
    global current_term, voted_for

    async with state_lock:
        if payload.term < current_term:
            return {"term": current_term, "vote_granted": False}

        if payload.term > current_term:
            become_follower(payload.term)

        my_last_idx  = len(log_entries) - 1
        my_last_term = log_entries[-1]["term"] if log_entries else -1

        log_ok   = (
            payload.last_log_term > my_last_term or
            (payload.last_log_term == my_last_term and
             payload.last_log_index >= my_last_idx)
        )
        can_vote = (voted_for is None or voted_for == payload.candidate_id)

        if can_vote and log_ok:
            voted_for = payload.candidate_id
            reset_election_timer()
            log.info(f"Voted for {payload.candidate_id}  term={payload.term}")
            return {"term": current_term, "vote_granted": True}

        return {"term": current_term, "vote_granted": False}


# ── POST /clear ───────────────────────────────────────────────────────────────

@app.post("/clear")
async def clear_log():
    if state != "leader":
        raise HTTPException(status_code=307, detail={"error": "not_leader"})
    async with state_lock:
        global commit_index, last_applied
        log_entries.clear()
        commit_index = -1
        last_applied = -1
    asyncio.create_task(send_heartbeats())
    return {"ok": True}