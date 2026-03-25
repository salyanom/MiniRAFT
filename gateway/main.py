import asyncio
import os
import httpx
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────
REPLICA_URLS = os.environ.get(
    "REPLICA_URLS",
    "http://replica1:8001,http://replica2:8002,http://replica3:8003"
).split(",")

# ── State ─────────────────────────────────────────────────
connected_clients: list[WebSocket] = []
current_leader: str | None = None


# ── Leader Discovery ──────────────────────────────────────
async def find_leader() -> str | None:
    """Poll all replicas to find who is the current leader."""
    async with httpx.AsyncClient(timeout=2.0) as client:
        for url in REPLICA_URLS:
            try:
                r = await client.get(f"{url}/status")
                data = r.json()
                if data.get("state") == "leader":
                    print(f"[GATEWAY] Leader found: {url}")
                    return url
            except Exception:
                continue
    print("[GATEWAY] No leader found across all replicas")
    return None


async def get_leader() -> str | None:
    """Return cached leader if still valid, otherwise rediscover."""
    global current_leader
    if current_leader:
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.get(f"{current_leader}/status")
                if r.json().get("state") == "leader":
                    return current_leader
        except Exception:
            pass
        # Cached leader is stale
        print(f"[GATEWAY] Cached leader {current_leader} is gone, rediscovering...")
        current_leader = None

    current_leader = await find_leader()
    return current_leader


# ── WebSocket: Browser Clients ────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"[GATEWAY] Client connected. Total: {len(connected_clients)}")

    try:
        while True:
            data = await websocket.receive_text()
            stroke = json.loads(data)

            # Find leader and forward stroke
            leader = await get_leader()
            if leader is None:
                print("[GATEWAY] No leader available, dropping stroke")
                continue

            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    await client.post(f"{leader}/stroke", json=stroke)
            except Exception as e:
                print(f"[GATEWAY] Failed to reach leader {leader}: {e}")
                global current_leader
                current_leader = None  # Force rediscovery next time

    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"[GATEWAY] Client disconnected. Total: {len(connected_clients)}")


# ── Called by Leader after Commit ─────────────────────────
@app.post("/committed-stroke")
async def committed_stroke(payload: dict):
    """
    Leader calls this once a stroke is committed by majority.
    Gateway broadcasts it to ALL connected browser clients.
    """
    stroke = payload.get("stroke")
    if not stroke:
        return JSONResponse({"error": "no stroke in payload"}, status_code=400)

    message = json.dumps({"type": "stroke", **stroke})
    disconnected = []

    for client in connected_clients:
        try:
            await client.send_text(message)
        except Exception:
            disconnected.append(client)

    # Cleanup dead connections
    for c in disconnected:
        connected_clients.remove(c)

    print(f"[GATEWAY] Broadcasted committed stroke to {len(connected_clients)} clients")
    return {"status": "broadcasted"}


# ── Health Check ──────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "clients_connected": len(connected_clients)}


# ── Startup ───────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global current_leader
    print("[GATEWAY] Starting up, waiting for replicas...")
    await asyncio.sleep(3)  # Give replicas time to boot and elect a leader
    current_leader = await find_leader()
    print(f"[GATEWAY] Initial leader: {current_leader}")