"""
Gateway Service - simplified and fixed
"""
import asyncio
import os
import logging
from contextlib import asynccontextmanager
from typing import Optional, Any

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GATEWAY] %(message)s")
log = logging.getLogger("gateway")

REPLICA_URLS: list[str] = os.environ.get(
    "REPLICA_URLS", "http://replica1:4001,http://replica2:4002,http://replica3:4003"
).split(",")

# ── State ─────────────────────────────────────────────────────────────────────
connected_clients: list[WebSocket] = []
current_leader: Optional[str] = None

# ── Leader discovery ──────────────────────────────────────────────────────────
async def find_leader() -> Optional[str]:
    """Poll all replicas once and return the leader URL, or None."""
    async with httpx.AsyncClient(timeout=1.0) as client:
        for url in REPLICA_URLS:
            try:
                r = await client.get(f"{url}/status")
                if r.status_code == 200:
                    data = r.json()
                    if data.get("role") == "leader":
                        log.info(f"Leader found: {url}  term={data.get('term')}")
                        return url
                    # This node knows who the leader is — find their URL
                    lid = data.get("leader_id")
                    if lid:
                        for candidate in REPLICA_URLS:
                            if lid in candidate:
                                log.info(f"Leader from redirect: {candidate}")
                                return candidate
            except Exception as e:
                log.debug(f"Status check {url}: {e}")
    return None

async def get_leader() -> Optional[str]:
    """Return current leader, discovering if needed. Retries up to 10 times."""
    global current_leader
    if current_leader:
        return current_leader
    for attempt in range(10):
        leader = await find_leader()
        if leader:
            current_leader = leader
            return leader
        log.warning(f"No leader yet (attempt {attempt+1}/10), retrying in 500ms...")
        await asyncio.sleep(0.5)
    return None

# ── Stroke forwarding ─────────────────────────────────────────────────────────
async def forward_stroke(stroke: dict):
    """Forward a stroke to the leader. Retries on failure with rediscovery."""
    global current_leader

    for attempt in range(5):
        leader = await get_leader()
        if not leader:
            log.error("No leader available — stroke dropped")
            return

        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.post(f"{leader}/stroke", json=stroke)

            if r.status_code == 200:
                return  # success

            if r.status_code == 307:
                body = r.json()
                log.warning(f"Leader {leader} redirected stroke: {body}")
                redirect = body.get("detail", {})
                if isinstance(redirect, dict) and redirect.get("leader_url"):
                    hint = redirect["leader_url"]
                    for url in REPLICA_URLS:
                        rid = url.split("//")[1].split(":")[0]
                        if rid in hint:
                            current_leader = url
                            log.info(f"Redirected to: {current_leader}")
                            break
                    else:
                        current_leader = None
                else:
                    current_leader = None

            elif r.status_code == 422:
                # Validation error — log it and give up (retrying won't help)
                log.error(f"Stroke validation error 422: {r.text[:200]}")
                return

            else:
                log.warning(f"Stroke POST returned {r.status_code}, rediscovering")
                current_leader = None

        except Exception as e:
            log.warning(f"Stroke forward failed (attempt {attempt+1}): {e}")
            current_leader = None

        await asyncio.sleep(0.1)

    log.error("Stroke forwarding gave up after 5 attempts")

# ── Broadcast ─────────────────────────────────────────────────────────────────
async def broadcast(message: dict):
    dead = []
    for ws in connected_clients:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in connected_clients:
            connected_clients.remove(ws)

# ── Background health check ───────────────────────────────────────────────────
async def health_loop():
    global current_leader
    while True:
        await asyncio.sleep(2)
        if not current_leader:
            continue
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.get(f"{current_leader}/status")
            if r.status_code != 200 or r.json().get("role") != "leader":
                log.warning("Leader health check failed — clearing")
                current_leader = None
        except Exception:
            log.warning("Leader unreachable — clearing")
            current_leader = None

# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(health_loop())
    asyncio.create_task(get_leader())   # warm up leader discovery on start
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Pydantic schemas ──────────────────────────────────────────────────────────
class CommitPayload(BaseModel):
    index: int
    term: int
    data: Any

class NotifyLeaderPayload(BaseModel):
    leader_id: str
    leader_url: str
    term: int

# ── REST endpoints ────────────────────────────────────────────────────────────
@app.post("/commit")
async def receive_commit(payload: CommitPayload):
    """Called by the leader when a stroke is committed — broadcast to all clients."""
    log.info(f"Committed stroke index={payload.index}")
    await broadcast({"type": "stroke", "stroke": payload.data})
    return {"ok": True}

@app.post("/notify-leader")
async def notify_leader(payload: NotifyLeaderPayload):
    """Called by newly elected leader."""
    global current_leader
    log.info(f"New leader: {payload.leader_id}  term={payload.term}  url={payload.leader_url}")
    # Map the internal URL to one in our REPLICA_URLS list
    for url in REPLICA_URLS:
        rid = url.split("//")[1].split(":")[0]
        if rid == payload.leader_id:
            current_leader = url
            log.info(f"Leader URL set to: {current_leader}")
            break
    else:
        current_leader = payload.leader_url  # fallback to what replica sent
    await broadcast({
        "type": "leader_change",
        "leader_id": payload.leader_id,
        "term": payload.term,
    })
    return {"ok": True}

@app.post("/clear")
async def receive_clear():
    await broadcast({"type": "clear"})
    return {"ok": True}

@app.get("/status")
async def gateway_status():
    leader_term = None
    if current_leader:
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.get(f"{current_leader}/status")
                if r.status_code == 200:
                    leader_term = r.json().get("term")
        except Exception:
            pass
    return {
        "current_leader": current_leader,
        "connected_clients": len(connected_clients),
        "replicas": REPLICA_URLS,
        "term": leader_term,
    }

# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.append(ws)
    log.info(f"Client connected  total={len(connected_clients)}")

    # Tell new client who the leader is (with term)
    leader = await get_leader()
    if leader:
        for url in REPLICA_URLS:
            if url == leader:
                rid = url.split("//")[1].split(":")[0]
                # Fetch term from replica status
                leader_term = None
                try:
                    async with httpx.AsyncClient(timeout=1.0) as client:
                        sr = await client.get(f"{leader}/status")
                        if sr.status_code == 200:
                            leader_term = sr.json().get("term")
                except Exception:
                    pass
                await ws.send_json({
                    "type": "leader",
                    "leader_id": rid,
                    "term": leader_term,
                })
                break

    # Replay full canvas state from leader log
    if leader:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                r = await client.get(f"{leader}/log")
            if r.status_code == 200:
                data = r.json()
                await ws.send_json({"type": "full_log", "log": data.get("log", [])})
        except Exception as e:
            log.warning(f"Could not fetch log for new client: {e}")

    try:
        while True:
            msg = await ws.receive_json()
            mtype = msg.get("type")

            if mtype == "stroke":
                # Fire and forget — don't block the receive loop
                asyncio.create_task(forward_stroke(msg["stroke"]))

            elif mtype == "clear":
                if leader:
                    try:
                        async with httpx.AsyncClient(timeout=1.0) as client:
                            await client.post(f"{leader}/clear")
                    except Exception:
                        pass
                await broadcast({"type": "clear"})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.error(f"WS error: {e}")
    finally:
        if ws in connected_clients:
            connected_clients.remove(ws)
        log.info(f"Client disconnected  total={len(connected_clients)}")