# Vault — fault-tolerant distributed object storage

Vault is a hackathon-ready prototype of a distributed object store. It demonstrates the core ideas behind services such as Amazon S3 and Ceph without needing Docker, cloud accounts, or any third-party packages.

## What it demonstrates

| Challenge requirement | Vault implementation |
|---|---|
| Large objects | Breaks uploads into 1 MiB content-addressed chunks. |
| Replication and durability policy | Configurable replication factor and `W`-of-`N` write quorum. |
| Concurrent reads and writes | Immutable versions; a key-specific write lock; atomic version publication. |
| Node failures / partitions | Health checks have timeouts; unreachable nodes are removed from write placement. |
| Data corruption | Every node verifies incoming SHA-256 hashes. Every read verifies hashes again. |
| Replica inconsistency | Reads retry another replica if one has missing/corrupt data. |
| Automatic repair | Background scrubbing copies a verified replica to a healthy replacement. |
| Rebalancing | Rendezvous hashing chooses balanced placements; rebalance moves chunks and removes extras after safe copy. |
| Metadata consistency | A single metadata leader uses atomic on-disk JSON commits and only publishes a complete object version after quorum. |

## Architecture

```text
                    Browser / REST client
                           |
                           v
              Coordinator (port 3100)
   metadata, placement, quorum, integrity, repair
                 /             |            \
                v              v             v
       node-a :3001    node-b :3002   node-c :3003
          immutable SHA-256 addressed chunk storage

       node-d :3004 starts as a replacement spare during the failure demo
```

The coordinator is intentionally a single metadata leader to keep the prototype understandable and demoable. A production design should run several metadata leaders using a consensus protocol such as Raft. The object data path is already decentralized: each replica is stored on an independent node.

## Run it — exact beginner steps

1. Open **PowerShell**.

2. Copy and run this command:

   ```powershell
   cd "C:\Users\rushd\OneDrive\Desktop\hackathon\problem1\vault-distributed-storage"
   npm run start
   ```

3. Open [http://127.0.0.1:3100](http://127.0.0.1:3100) in a browser.

4. Click **Choose file**, select any small file, and click **Upload with quorum**.

5. You should see the object with `3/3` replicas and a green `healthy` label.

6. To stop all Vault processes when you are done:

   ```powershell
   npm run stop
   ```

No `npm install` command is required. This project uses Node.js built-in modules only.

## Best two-minute live demonstration

Before judges arrive, start Vault and upload a file that they can recognize—an image, PDF, or your presentation.

1. Show the dashboard: nodes `a`, `b`, and `c` are healthy, and your file has `3/3` replicas.
2. In the PowerShell window, run:

   ```powershell
   npm run demo
   ```

   This stops `node-a` and starts the fresh replacement `node-d`.

3. Return to the browser. Within a few seconds, `node-a` becomes `unavailable` and `node-d` becomes `healthy`.
4. Click **Download**. The file downloads correctly despite node-a being dead, because Vault retries an intact replica.
5. Click **Run integrity scan**. The object returns to `3/3`: Vault copied verified chunks from node-b/node-c to node-d.
6. Click **Inspect** to show the actual chunk hashes and node locations.

### Optional corruption demonstration

The coordinator offers a local demo-only API to deliberately corrupt one replica. First inspect an object and identify a chunk hash and a node that hosts it, then run this in PowerShell (replace the three values):

```powershell
$body = @{ key = 'your-file-name.txt'; chunkIndex = 0; nodeId = 'node-b' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:3100/api/demo/corrupt -ContentType 'application/json' -Body $body
```

Then download the object. Vault rejects the corrupt checksum, reads a healthy replica, and queues repair. Press **Run integrity scan** to make the repair immediate and visible.

## How the important operations work

### Upload

1. The client sends `PUT /objects/<object-key>` with file bytes.
2. The coordinator splits the bytes into 1 MiB chunks and calculates a SHA-256 hash for each chunk.
3. Rendezvous hashing chooses independent nodes for each chunk.
4. The coordinator sends a chunk to all chosen nodes in parallel.
5. After the configured write quorum acknowledges, the coordinator atomically publishes a new immutable object version. A partially uploaded version is never visible to readers.
6. If fewer than the write quorum respond, the upload returns a clear error and the old version stays untouched.

### Download

1. The coordinator reads the latest complete object version from metadata.
2. For every chunk it tries a known replica.
3. It re-hashes returned bytes. A bad hash is treated exactly like an unavailable replica.
4. It retries another replica and reassembles only verified chunks.
5. Any bad/missing copy triggers background repair, without delaying the successful read.

### Repair and rebalance

Every 15 seconds, and whenever you click **Run integrity scan**, the coordinator checks known replicas. A chunk with fewer verified copies than its policy requires is copied from a good replica to a healthy node. **Rebalance** uses rendezvous hashing to move data toward its preferred balanced locations, deleting old copies only after a new verified copy exists.

## API quick reference

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/objects/{key}` | Upload / create a new immutable version. |
| `GET` | `/objects/{key}` | Download verified data. |
| `GET` | `/api/objects` | List objects and their health. |
| `GET` | `/api/objects/{key}` | Inspect version, chunk hashes, and replica locations. |
| `GET` | `/api/cluster` | Show node health and policy. |
| `PATCH` | `/api/config` | Change replication factor / write quorum for new uploads. |
| `POST` | `/api/repair` | Run repair immediately. |
| `POST` | `/api/rebalance` | Move replicas to their calculated preferred nodes. |

## What to say to judges

> “Vault stores objects as immutable, SHA-256-addressed chunks. A configurable write quorum prevents partially written objects from becoming visible. Every download verifies integrity at the chunk level and retries replicas transparently. A health monitor detects failed nodes, and a scrubber automatically rebuilds missing or corrupt replicas. Our metadata leader commits object versions atomically; in production we would replace that leader with a Raft metadata quorum.”

## Project map

| File | Why it exists |
|---|---|
| `src/storage-node.js` | One independent storage server. It never accepts a chunk with an incorrect hash. |
| `src/coordinator.js` | The brain: metadata, placement, quorum, reads, node monitoring, repair, and rebalance. |
| `src/dashboard.html` | The visual dashboard for your demo. |
| `scripts/start-cluster.ps1` | Starts the three-node cluster. |
| `scripts/demo.ps1` | Performs a convincing fail-node / start-spare simulation. |
| `scripts/stop-cluster.ps1` | Stops only Vault processes listed in its own PID file. |

## Honest limitations and next steps

This is a local prototype, not production storage. The central metadata coordinator is still a single point of failure; a next iteration would use a three-member Raft cluster. Uploads are buffered in memory up to 100 MiB; production code would stream chunks. Nodes communicate over localhost HTTP; production requires TLS, authentication, authorization, rate limits, audit logs, multi-rack placement rules, and erasure coding to reduce replication overhead.
