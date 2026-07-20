# Manager Rewrite Plan

## Overview

The legacy manager (`workforce_1/manager/`) is a solid, complete implementation. The
primary work here is adapting it to the new netargv-based protocol that the new worker
already uses, and cleaning up a handful of things we changed in the worker rewrite.
The vast majority of the manager is **protocol-agnostic** and can be copied nearly
verbatim.

---

## 1. Contract additions (`workforce/contract/`)

Before writing any manager code, three missing pieces need to land in the contract
package. All are direct copies from `workforce_1/contract/` with only import-path
corrections.

| File | Source | Notes |
|---|---|---|
| `types.go` | `workforce_1/contract/types.go` | `Job`, `JobStatus`, `Limitations`, `RawResult`, `ChildJobResult`, `ConsolidatePayload`, `WebhookEntry`, `ArtifactInfo`, `RetryPolicy`. `RawResult` is nearly identical to `raw_value.go` already in the package — merge or deduplicate. |
| `store.go` | `workforce_1/contract/store.go` | `JobStore`, `TaskStore`, `LogStore`, `WebhookStore` interfaces. Exact copy. |
| `task.go` | `workforce_1/contract/task.go` | `TaskDefinition` struct. Exact copy. |

---

## 2. Files to create in `workforce/manger/`

### 2a. Direct copies (no protocol changes needed)

These files are entirely internal logic with no direct dependency on the wire
protocol. Copy verbatim, fix import paths.

| File | Reason for direct copy |
|---|---|
| `worker_pool.go` | Rename `hub` → `WorkerPool` (see §2c). |
| `pipeline.go` | Operates on `JobStore`/`TaskStore` only; dispatches jobs by calling `dispatcher.enqueue`. Zero protocol coupling. |
| `selector.go` | Pure worker-selection strategies (`LeastLoaded`, `RoundRobin`, `Random`, `FirstFound`). No protocol dependency. |
| `webhook.go` | HTTP webhook delivery queue and retry loop. No protocol dependency. |
| `ids.go` | `newID()` helper. Exact copy. |
| `artifact/registry.go` | `ArtifactRegistry` interface. Exact copy. |
| `artifact/fsregistry.go` | Filesystem artifact storage. Exact copy. |
| `artifact/seed.go` | Local binary seeding utility. Exact copy. |
| `store/memstore.go` | In-memory store implementations. Exact copy. |
| `store/fsstore.go` | Filesystem-backed store. Exact copy. |
| `store/fsstore_journal.go` | Journal replay. Exact copy. |
| `store/fsstore_snapshot.go` | Snapshot compaction. Exact copy. |

---

### 2b. `config.go` — Copy with minor changes

The `Config` struct is nearly identical. Changes:

- Remove `urlsign` import from `workforce_1/urlsign` → use `lowbit.dev/urlsign` (already
  a workspace module).
- Remove the `artifact` sub-package import path correction.
- The `DefaultRetryPolicy`, `WorkerSelector`, `OnResourceShortage`, `OnIdleWorker`
  fields all survive unchanged.

---

### 2c. `worker_pool.go` — Copy with protocol adaptation

Rename `hub` → `WorkerPool` (and `newHub` → `newWorkerPool`) throughout. The type name
better reflects its purpose as the registry of all connected workers.

`WorkerConn` changes:

- **Remove** `codec contract.Codec` field. The new protocol is plaintext netargv; the
  connection is a `net.Conn` written to directly.
- **Replace** `send(pkt contract.Packet) error` with two helpers matching the worker's
  sender pair:
  ```go
  func (w *WorkerConn) send(line string) error
  func (w *WorkerConn) sendWithPayload(header string, payload []byte) error
  ```
- **Add** the netargv message-sending helpers the dispatcher will call (see §2d).

Everything else on `WorkerPool` — `register`, `unregister`, `eligibleWorkers`, capacity
accounting, idle tracking, heartbeat monitoring, platform indexing — is unchanged.

---

### 2d. `connection.go` — Rewrite (biggest change)

This file changes the most because the connection handshake and read loop are protocol-specific.

**Legacy flow:**
1. Read 1-byte codec selector
2. Decode `TYPE_HANDSHAKE` packet for worker identity
3. Register in hub
4. `codec.Decode` read loop → switch on `PacketType`

**New flow:**
1. Worker identity comes from HTTP request headers (`X-Worker-ID`, `X-Worker-Capacity`,
   `X-Worker-OS`, `X-Worker-Arch`), parsed in the connection handler *before* the
   connection is hijacked by `cooper`.
2. Register in hub immediately after hijack.
3. Read loop uses `netargv.NewReader(conn).Itterate(ctx)` and resolves message type via
   `verreg.Registry` (same pattern as the worker's `ConnectAndWork`).

**Message handlers to port (worker → manager direction):**

| Message verb | Legacy packet type | Handler method |
|---|---|---|
| `accept` | `TypeAccept` | `onJobAccepted` — copy unchanged |
| `reject` | `TypeNack` | `onJobNacked` — copy unchanged |
| `staged` | *(new in new protocol)* | `onJobStaged` — transition status to `provisioning` |
| `starting` | `TypeStarting` | `onJobStarting` — copy unchanged |
| `log` | `TypeLogStream` | `onLogLine` — append payload bytes to LogStore |
| `result` | `TypeResult` / `TypeSubjobsEmitted` / `TypeJobError` | `onResult` — merge the three legacy handlers into one switch on `msg.Type` |
| `heartbeat` | `TypeHeartbeat` | echo back `"heartbeat"`, update `lastHeartbeat` |
| `capacity` | `TypeCapacityUpdate` | `WorkerPool.overrideCapacity` — copy unchanged |

**New `staged` event:**

The new protocol introduces a two-step propose/dispatch handshake not present in the
legacy:

```
manager → worker:  propose --job-id=X --task=Y --cost=N ...
worker  → manager: accept  --job-id=X
manager → worker:  dispatch --job-id=X --phase=... --attempt=N -- <n>\n<payload>
worker  → manager: staged  --job-id=X
worker  → manager: starting --job-id=X
worker  → manager: result ...
```

`onJobAccepted` should, after updating job status, send the `dispatch` message.
`onJobStaged` transitions the job to `provisioning` (equivalent to the legacy's
`TypeAccept` path, which moved to `provisioning` there).

**Manager → worker senders to add to `WorkerConn`:**

```go
// propose --job-id=X --task=Y --cost=N --artifact-hash=Z --artifact-url=U [--dep=A ...] [--no-result]
func (w *WorkerConn) sendPropose(p contract.ProposeMessage) error

// dispatch --job-id=X --phase=Y --attempt=N [--max-exec-time=...] [--max-memory=...] [--max-cpu-cores=...] -- N\n<payload>
func (w *WorkerConn) sendDispatch(p contract.DispatchMessage) error

// heartbeat
func (w *WorkerConn) sendHeartbeat() error

// system --command=drain|shutdown
func (w *WorkerConn) sendSystem(command string) error

// cancel --job-id=X
func (w *WorkerConn) sendCancel(jobID string) error
```

---

### 2e. `dispatcher.go` — Copy with sender substitution

The dispatch loop logic (priority heap, `processHeap`, `tryPropose`, `checkResourceShortage`,
starvation aging, `requeueJob`, `cancelJob`) is entirely copy-paste.

The only changes are in how jobs are **sent** to a worker:

- Replace `wc.send(mustEncodeJSON(TypeProposeJob, ...))` with `wc.sendPropose(...)`.
- Remove the combined propose+dispatch in one packet. After `onJobAccepted` triggers
  `sendDispatch`, the dispatcher's job for a given proposal is complete — the dispatch
  happens in `connection.go`'s `onJobAccepted` handler, not in the dispatcher loop.

This keeps the dispatcher stateless with respect to the per-job proposal lifecycle.

---

### 2f. `manager.go` — Copy with lifecycle changes

`New()` is largely the same. Changes:

- Add `verreg.Registry[contract.MessageFactory]` field (built once in `New()` via
  `contract.RegisterMessages`, same as in the worker).
- Replace the bare `dispatchSignal chan struct{}` with a `rungroup`-managed service
  pattern (matching the worker's `Run()` style) for the dispatcher loop, `WorkerPool` heartbeat
  monitor, webhook dispatcher, and pipeline.
- `Start(ctx)` becomes `Run(ctx context.Context) error` returning on context
  cancellation.
- `ServeHTTP` delegates to `m.mux` unchanged.

---

### 2g. `routes.go` — Copy with three targeted changes

1. **Worker connect handler:** The `cooper.Hijack` callback now receives a worker
   whose identity is already in the request headers. Parse `X-Worker-ID`,
   `X-Worker-Capacity`, `X-Worker-OS`, `X-Worker-Arch` from `r.Header` inside the
   hijack callback. Pass these to `serveWorkerConn`. Also validate `X-Worker-Accept`
   using `contract.ValidateWorkforceAccept` (the HMAC challenge from `proto_magic.go`).

2. **Error responses:** The legacy imports `github.com/cornejong/goproblem`. Replace
   with `lowbit.dev/problemjson` (already in the workspace).

3. **Protocol version in `cooper.Protocols(...)`:** Change from `"workforce/1"` to
   `fmt.Sprintf("workforce/%d", verreg.Version(0))` to match the worker's connect
   request.

All route handlers, middleware, cluster/job/task/artifact/log/webhook API endpoints are
copy-paste unchanged.

---

## 3. New state: `stateProvisioning`

The new two-step propose/dispatch flow introduces an intermediate state between
`accepted` and `starting`. The legacy mapped `TypeAccept` directly to
`JobStatusProvisioning`. In the new protocol:

- `accept` → `JobStatusProposing` (accepted, dispatch not yet sent)
- `staged` → `JobStatusProvisioning` (dispatch sent, artifact being fetched)
- `starting` → `JobStatusRunning`

The job status constants in `contract/types.go` already include `JobStatusProposing`
and `JobStatusProvisioning` from the legacy; they map cleanly.

---

## 4. Work order

1. Contract additions (`types.go`, `store.go`, `task.go`)
2. `config.go`, `ids.go`, `selector.go`
3. `hub.go` (WorkerConn send helpers)
4. `connection.go` (read loop + message handlers + WorkerConn senders)
5. `dispatcher.go`
6. `pipeline.go`
7. `webhook.go`
8. `manager.go` (wires everything together)
9. `routes.go`
10. `artifact/` and `store/` copies
11. Integration test port from `workforce_1/manager/integration_test.go`

Steps 1–3 are prerequisites; steps 4–9 can be written in parallel once step 3 is done;
steps 10–11 can happen at any point after step 1.
