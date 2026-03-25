# Distributed Real-Time Drawing Board with Mini-RAFT Consensus

> Cloud Computing Assignment — Team 01
> Python 3.11 · FastAPI · Docker · WebSocket · Mini-RAFT

---

## What is this?

A fault-tolerant collaborative drawing board where multiple users draw on a shared HTML5 canvas in real time. The backend is a cluster of **3 replica nodes** that maintain a shared stroke log using a **Mini-RAFT consensus protocol**. Even if a node crashes or restarts, the system stays live and the canvas stays consistent.

---

## Architecture

```
Browser A ──┐
            ├──── WebSocket ──── nginx (frontend) ──── Gateway (FastAPI)
Browser B ──┘                                              │
                                               ┌───────────┴────────────┐
                                          POST /stroke             POST /commit
                                               │                        │
                                          ┌────▼────┐                   │
                                          │Replica 1│ ◄─── LEADER ──────┘
                                          │(LEADER) │
                                          └────┬────┘
                                   AppendEntries│
                                    ┌───────────┴───────────┐
                                    ▼                       ▼
                               ┌─────────┐           ┌─────────┐
                               │Replica 2│           │Replica 3│
                               │follower │           │follower │
                               └─────────┘           └─────────┘
```

### Services

| Service | Port | Description |
|---|---|---|
| `frontend` | 8080 | nginx — serves canvas UI, proxies `/ws` and `/api/` to gateway |
| `gateway` | 3000 | FastAPI WebSocket server — routes strokes to leader, broadcasts commits |
| `replica1` | 4001 | RAFT node — leader or follower |
| `replica2` | 4002 | RAFT node — leader or follower |
| `replica3` | 4003 | RAFT node — leader or follower |

> All three replicas run the **same Docker image** (`./replica`), differentiated only by environment variables (`REPLICA_ID`, `PORT`, `PEERS`).

---

## Mini-RAFT Protocol

### Node States

```
 ┌──────────┐  timeout(500-900ms)  ┌───────────┐  majority votes  ┌────────┐
 │ Follower │ ──────────────────► │ Candidate │ ───────────────► │ Leader │
 └──────────┘                     └───────────┘                  └────────┘
      ▲                                 │                             │
      │                  split vote     │                             │
      │                 (retry election)│                             │
      │◄────────────────────────────────┘                             │
      │                                                               │
      └───────────────── higher term seen ◄──────────────────────────┘
```

### Timing

| Parameter | Value |
|---|---|
| Election timeout | Random 500–900 ms |
| Heartbeat interval | 150 ms |
| RPC timeout | 1000 ms |
| Majority (3 nodes) | 2 of 3 |

### Stroke Commit Flow

1. Browser sends stroke over WebSocket → nginx → Gateway
2. Gateway forwards stroke to Leader via `POST /stroke`
3. Leader appends entry to its local log as `{term, data}`
4. Leader immediately sends `AppendEntries` to both followers concurrently
5. Once the leader + at least 1 follower have the entry (majority = 2 of 3), the leader advances `commit_index`
6. Leader's `push_commits()` loop fires `POST /commit` to Gateway (all pending commits sent concurrently)
7. Gateway broadcasts each committed stroke to all connected WebSocket clients

### Catch-up Sync (Restarted Node)

Catch-up uses RAFT's built-in **fast log backup** — no separate sync endpoint is needed:

1. Node restarts as Follower with empty log
2. First `AppendEntries` from leader fails the `prev_log_index` consistency check
3. Follower replies with `success: false` and a `conflict_index` hint pointing to the first inconsistent entry
4. Leader decrements `next_index[follower]` to `conflict_index` and retries with the missing entries
5. This repeats (converging quickly) until the follower's log matches the leader's
6. Normal replication resumes — the follower is fully caught up

---

## Project Structure

```
project-root/
├── docker-compose.yml
├── frontend/
│   ├── Dockerfile          # nginx image
│   ├── nginx.conf          # proxy /ws and /api/ → gateway
│   └── index.html          # canvas UI + cluster state debug panel
├── gateway/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py             # WebSocket manager + leader routing + commit broadcast
└── replica/
    ├── Dockerfile
    ├── requirements.txt
    └── main.py             # Full RAFT state machine (used by all 3 replicas)
```

---

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Run

```bash
git clone <your-repo-url>
cd project-root
docker-compose up --build
```

Open **http://localhost:8080** in multiple browser tabs and start drawing.

### Stop

```bash
docker-compose down
```

---

## API Reference

### Replica Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/request-vote` | RequestVote RPC — grants vote if candidate log is up-to-date |
| `POST` | `/append-entries` | AppendEntries RPC — heartbeat + log replication + commit advance |
| `POST` | `/stroke` | Accept stroke from gateway (leader only; returns 307 if not leader) |
| `POST` | `/clear` | Clear canvas log (leader only) |
| `GET` | `/log` | Returns all committed stroke data (used for canvas replay on new client connect) |
| `GET` | `/status` | Returns `id`, `role`, `term`, `leader_id`, `log_length`, `commit_index` |

### Gateway Endpoints

| Method | Path | Purpose |
|---|---|---|
| `WS` | `/ws` | Browser WebSocket connection — receives strokes, broadcasts commits |
| `POST` | `/commit` | Receive committed stroke from leader — broadcast to all clients |
| `POST` | `/notify-leader` | Newly elected leader registers itself with gateway |
| `POST` | `/clear` | Broadcast canvas clear to all clients |
| `GET` | `/status` | Returns current leader URL, connected client count, current term |

---

## Failure Testing

```bash
# Kill the leader — watch automatic failover
docker-compose stop replica1

# Restart it — watch catch-up sync via AppendEntries fast-backup
docker-compose start replica1

# Watch RAFT logs live
docker-compose logs -f replica1 replica2 replica3

# Check cluster state from each node
curl localhost:4001/status
curl localhost:4002/status
curl localhost:4003/status

# Check gateway state
curl localhost:3000/status
```

### Sample log output during failover

```
[replica1] Leader unreachable — election timeout (0.743s)
[replica1] ⚡ ELECTION  term=4
[replica1] Voted for replica1  term=4
[replica2] Voted for replica1  term=4
[replica1] ★ LEADER  term=4
[replica1] Notified gateway of leadership
[replica2] → FOLLOWER  term=4  leader=replica1
[replica3] → FOLLOWER  term=4  leader=replica1
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | HTML5 Canvas + Vanilla JS + WebSocket API |
| Frontend server | nginx |
| Gateway | Python 3.11 + FastAPI + WebSockets |
| Replica nodes | Python 3.11 + FastAPI + asyncio |
| Inter-service HTTP | httpx (async) |
| Hot reload | uvicorn --reload + watchfiles |
| Containerisation | Docker + docker-compose |

---

## Cloud Concepts Demonstrated

| Concept | Implementation |
|---|---|
| Consensus protocol | Mini-RAFT — leader election + log replication + majority commit |
| Fault tolerance | Survives any single node failure; automatic leader re-election |
| Log catch-up | Fast-backup via `conflict_index` in AppendEntries — no extra sync endpoint needed |
| State replication | Append-only stroke log, majority commit before broadcast |
| Service discovery | Gateway polls `/status` to find leader; notified via `/notify-leader` on election |
| Real-time collaboration | WebSocket broadcast to all clients on every committed stroke |
| Containerisation | 5-service docker-compose stack (1 shared replica image, 3 instances) |

---

## Team

Cloud Computing — Team 01