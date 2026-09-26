/*
 * Vault coordinator: durable metadata, chunk placement, quorum writes, verified
 * reads, node health monitoring, integrity scrubbing, automatic repair, and rebalance.
 * This hackathon prototype deliberately has one metadata leader. See README for how
 * a production deployment would replicate that small metadata service with Raft.
 */
const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const {
  atomicWriteJson, ensureDirectory, json, now, readBody, readJson,
  safeObjectKey, sha256, text,
} = require('./utils');

const projectRoot = path.resolve(__dirname, '..');
const coordinatorDirectory = path.resolve(process.argv[2] || path.join(projectRoot, 'data', 'coordinator'));
const port = Number(process.argv[3] || 3100);
const metadataFile = path.join(coordinatorDirectory, 'metadata.json');
const dashboardFile = path.join(__dirname, 'dashboard.html');
const MAX_OBJECT_BYTES = 100 * 1024 * 1024;
const NETWORK_TIMEOUT_MS = 2500;

const configuredNodes = [
  { id: 'node-a', url: 'http://127.0.0.1:3001' },
  { id: 'node-b', url: 'http://127.0.0.1:3002' },
  { id: 'node-c', url: 'http://127.0.0.1:3003' },
  { id: 'node-d', url: 'http://127.0.0.1:3004' },
];

function initialMetadata() {
  return {
    schemaVersion: 1,
    cluster: { replicationFactor: 3, writeQuorum: 2, chunkSizeBytes: 1024 * 1024 },
    nodes: configuredNodes.map((node) => ({ ...node, status: 'unknown', lastSeen: null, lastError: null })),
    objects: {},
    updatedAt: now(),
  };
}

let metadata;
let metadataQueue = Promise.resolve();
const keyLocks = new Map();
let repairRunning = false;

function mutate(change) {
  const operation = metadataQueue.then(async () => {
    const result = await change(metadata);
    metadata.updatedAt = now();
    await atomicWriteJson(metadataFile, metadata);
    return result;
  });
  metadataQueue = operation.catch(() => {});
  return operation;
}

async function withKeyLock(key, action) {
  const previous = keyLocks.get(key) || Promise.resolve();
  let release;
  const current = new Promise((resolve) => { release = resolve; });
  keyLocks.set(key, current);
  await previous;
  try {
    return await action();
  } finally {
    release();
    if (keyLocks.get(key) === current) keyLocks.delete(key);
  }
}

function errorWithStatus(message, statusCode = 500) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

async function fetchNode(node, route, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), NETWORK_TIMEOUT_MS);
  try {
    return await fetch(`${node.url}${route}`, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

function allLiveNodes() {
  return metadata.nodes.filter((node) => node.status === 'healthy');
}

function nodeForId(id) {
  return metadata.nodes.find((node) => node.id === id);
}

// Rendezvous hashing gives stable, balanced placement without a large placement table.
function rankedNodes(chunkHash, nodes) {
  return [...nodes].sort((left, right) => (
    sha256(`${chunkHash}:${right.id}`).localeCompare(sha256(`${chunkHash}:${left.id}`))
  ));
}

function placement(chunkHash, count, nodes = allLiveNodes()) {
  return rankedNodes(chunkHash, nodes).slice(0, count);
}

async function probeNode(node) {
  let status = 'unavailable';
  let lastError = null;
  try {
    const response = await fetchNode(node, '/health');
    if (!response.ok) throw new Error(`health endpoint returned ${response.status}`);
    const value = await response.json();
    if (value.id !== node.id) throw new Error(`expected ${node.id}, received ${value.id}`);
    status = 'healthy';
  } catch (error) {
    lastError = error.name === 'AbortError' ? 'health check timed out' : error.message;
  }
  await mutate((state) => {
    const found = state.nodes.find((candidate) => candidate.id === node.id);
    if (!found) return;
    found.status = status;
    found.lastSeen = status === 'healthy' ? now() : found.lastSeen;
    found.lastError = lastError;
  });
  return status;
}

async function probeNodes() {
  await Promise.all(metadata.nodes.map((node) => probeNode(node)));
}

async function getVerifiedChunk(node, hash) {
  const response = await fetchNode(node, `/chunks/${hash}`);
  if (!response.ok) throw new Error(`node ${node.id} returned ${response.status}`);
  const data = Buffer.from(await response.arrayBuffer());
  if (sha256(data) !== hash) {
    const error = new Error(`checksum mismatch from ${node.id}`);
    error.code = 'CORRUPT';
    throw error;
  }
  return data;
}

async function putChunk(node, hash, data) {
  const response = await fetchNode(node, `/chunks/${hash}`, {
    method: 'PUT', body: data, headers: { 'content-type': 'application/octet-stream' },
  });
  if (!response.ok) throw new Error(`node ${node.id} rejected replica: ${response.status}`);
}

function findVersion(key, versionId) {
  const object = metadata.objects[key];
  if (!object) return null;
  const resolvedId = versionId || object.latestVersion;
  return { object, version: object.versions[resolvedId], versionId: resolvedId };
}

function objectSummary(key, object) {
  const version = object.versions[object.latestVersion];
  const desired = version.replicationFactor;
  const replicaCounts = version.chunks.map((chunk) => chunk.replicas.filter((replica) => replica.status === 'verified').length);
  const minimumReplicaCount = replicaCounts.length ? Math.min(...replicaCounts) : 0;
  return {
    key,
    latestVersion: object.latestVersion,
    updatedAt: object.updatedAt,
    bytes: version.size,
    contentType: version.contentType,
    chunks: version.chunks.length,
    desiredReplicas: desired,
    minimumVerifiedReplicas: minimumReplicaCount,
    health: minimumReplicaCount >= desired ? 'healthy' : 'degraded',
  };
}

async function uploadObject(key, request) {
  return withKeyLock(key, async () => {
    const body = await readBody(request, MAX_OBJECT_BYTES);
    const factor = metadata.cluster.replicationFactor;
    const quorum = metadata.cluster.writeQuorum;
    const candidates = allLiveNodes();
    if (candidates.length < quorum) {
      throw errorWithStatus(`Write quorum is ${quorum}, but only ${candidates.length} storage nodes are reachable.`, 503);
    }

    const chunks = [];
    for (let offset = 0; offset < body.length || (body.length === 0 && offset === 0); offset += metadata.cluster.chunkSizeBytes) {
      const data = body.subarray(offset, Math.min(offset + metadata.cluster.chunkSizeBytes, body.length));
      const hash = sha256(data);
      const targets = placement(hash, factor, candidates);
      const results = await Promise.allSettled(targets.map(async (node) => {
        await putChunk(node, hash, data);
        return node.id;
      }));
      const replicas = results
        .filter((result) => result.status === 'fulfilled')
        .map((result) => ({ nodeId: result.value, status: 'verified', lastChecked: now() }));
      if (replicas.length < quorum) {
        throw errorWithStatus(`Chunk ${chunks.length + 1} did not reach write quorum (${replicas.length}/${quorum}).`, 503);
      }
      chunks.push({ hash, bytes: data.length, desiredReplicaCount: factor, replicas });
      if (body.length === 0) break;
    }

    const versionId = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
    const version = {
      id: versionId,
      createdAt: now(),
      size: body.length,
      contentType: request.headers['content-type'] || 'application/octet-stream',
      originalFilename: request.headers['x-vault-filename'] || key,
      replicationFactor: factor,
      writeQuorum: quorum,
      chunks,
    };
    await mutate((state) => {
      const object = state.objects[key] || { key, versions: {}, latestVersion: null, createdAt: now() };
      object.versions[versionId] = version;
      object.latestVersion = versionId; // Atomic publish: readers see either the old version or this complete version.
      object.updatedAt = now();
      state.objects[key] = object;
    });
    return { key, versionId, chunks: chunks.length, bytes: body.length, replicationFactor: factor, writeQuorum: quorum };
  });
}

async function retrieveObject(key, versionId) {
  const found = findVersion(key, versionId);
  if (!found || !found.version) throw errorWithStatus('Object or version not found.', 404);
  const output = [];
  const problems = [];
  for (const chunk of found.version.chunks) {
    let recovered = null;
    for (const replica of chunk.replicas) {
      const node = nodeForId(replica.nodeId);
      if (!node) continue;
      try {
        recovered = await getVerifiedChunk(node, chunk.hash);
        break;
      } catch (error) {
        problems.push({ chunk: chunk.hash, nodeId: replica.nodeId, reason: error.message, status: error.code === 'CORRUPT' ? 'corrupt' : 'unavailable' });
      }
    }
    if (!recovered) throw errorWithStatus(`No readable verified replica remains for chunk ${chunk.hash.slice(0, 12)}.`, 503);
    output.push(recovered);
  }
  if (problems.length) {
    // The caller receives valid data now; repairs run in the background instead of delaying the read.
    setImmediate(() => repairAll(false).catch((error) => console.error('read-triggered repair failed:', error.message)));
  }
  return { data: Buffer.concat(output), version: found.version, problems };
}

async function repairChunk(key, versionId, chunkIndex, rebalance) {
  const found = findVersion(key, versionId);
  const chunk = found?.version?.chunks[chunkIndex];
  if (!chunk) return { skipped: true };
  const statuses = new Map();
  let source = null;

  for (const replica of chunk.replicas) {
    const node = nodeForId(replica.nodeId);
    if (!node) continue;
    try {
      const data = await getVerifiedChunk(node, chunk.hash);
      statuses.set(node.id, { nodeId: node.id, status: 'verified', lastChecked: now() });
      source ||= data;
    } catch (error) {
      statuses.set(node.id, { nodeId: node.id, status: error.code === 'CORRUPT' ? 'corrupt' : 'unavailable', lastChecked: now(), error: error.message });
    }
  }

  if (!source) {
    await mutate(() => {});
    return { unrecoverable: true, hash: chunk.hash };
  }

  const desired = chunk.desiredReplicaCount || found.version.replicationFactor;
  const validIds = () => [...statuses.values()].filter((replica) => replica.status === 'verified').map((replica) => replica.nodeId);
  const healthy = allLiveNodes();
  const preferred = placement(chunk.hash, desired, healthy);
  let targets;
  if (rebalance) {
    targets = preferred.filter((node) => !validIds().includes(node.id));
  } else {
    targets = rankedNodes(chunk.hash, healthy.filter((node) => !validIds().includes(node.id)))
      .slice(0, Math.max(0, desired - validIds().length));
  }

  let repaired = 0;
  for (const target of targets) {
    try {
      await putChunk(target, chunk.hash, source);
      statuses.set(target.id, { nodeId: target.id, status: 'verified', lastChecked: now() });
      repaired += 1;
    } catch (error) {
      statuses.set(target.id, { nodeId: target.id, status: 'unavailable', lastChecked: now(), error: error.message });
    }
  }

  // A rebalancing pass first establishes preferred placements, then removes extras.
  if (rebalance && preferred.every((node) => validIds().includes(node.id))) {
    for (const replica of [...statuses.values()]) {
      if (replica.status !== 'verified' || preferred.some((node) => node.id === replica.nodeId)) continue;
      const node = nodeForId(replica.nodeId);
      try {
        const response = await fetchNode(node, `/chunks/${chunk.hash}`, { method: 'DELETE' });
        if (response.ok) statuses.delete(replica.nodeId);
      } catch { /* Keep the metadata record; it will be cleaned up later. */ }
    }
  }

  await mutate(() => {
    const current = findVersion(key, versionId)?.version?.chunks[chunkIndex];
    if (!current || current.hash !== chunk.hash) return;
    current.replicas = [...statuses.values()];
    current.lastRepairAt = now();
  });
  return { repaired, verified: validIds().length, desired, hash: chunk.hash };
}

async function repairAll(rebalance) {
  if (repairRunning) return { alreadyRunning: true };
  repairRunning = true;
  try {
    const tasks = [];
    for (const [key, object] of Object.entries(metadata.objects)) {
      for (const versionId of Object.keys(object.versions)) {
        object.versions[versionId].chunks.forEach((_, chunkIndex) => tasks.push({ key, versionId, chunkIndex }));
      }
    }
    const report = { scanned: tasks.length, repaired: 0, unrecoverable: 0, rebalance };
    for (const task of tasks) {
      const result = await repairChunk(task.key, task.versionId, task.chunkIndex, rebalance);
      report.repaired += result.repaired || 0;
      report.unrecoverable += result.unrecoverable ? 1 : 0;
    }
    return report;
  } finally {
    repairRunning = false;
  }
}

async function parseJson(request) {
  const body = await readBody(request, 1024 * 1024);
  try { return JSON.parse(body.toString('utf8')); } catch { throw errorWithStatus('Request body must be valid JSON.', 400); }
}

async function handle(request, response) {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const pathname = url.pathname;

  if (request.method === 'GET' && pathname === '/') {
    return text(response, 200, await fs.readFile(dashboardFile), 'text/html; charset=utf-8');
  }
  if (request.method === 'GET' && pathname === '/api/cluster') {
    return json(response, 200, { cluster: metadata.cluster, nodes: metadata.nodes, repairRunning });
  }
  if (request.method === 'GET' && pathname === '/api/objects') {
    return json(response, 200, { objects: Object.entries(metadata.objects).map(([key, object]) => objectSummary(key, object)) });
  }
  if (request.method === 'PATCH' && pathname === '/api/config') {
    const value = await parseJson(request);
    const replicationFactor = Number(value.replicationFactor);
    const writeQuorum = Number(value.writeQuorum);
    if (!Number.isInteger(replicationFactor) || !Number.isInteger(writeQuorum) || replicationFactor < 1 || writeQuorum < 1 || writeQuorum > replicationFactor || replicationFactor > metadata.nodes.length) {
      throw errorWithStatus(`Use integers where 1 ≤ write quorum ≤ replication factor ≤ ${metadata.nodes.length}.`, 400);
    }
    await mutate((state) => { state.cluster.replicationFactor = replicationFactor; state.cluster.writeQuorum = writeQuorum; });
    return json(response, 200, { cluster: metadata.cluster });
  }
  if (request.method === 'POST' && pathname === '/api/nodes') {
    const value = await parseJson(request);
    if (!/^[a-z0-9-]{2,40}$/i.test(value.id || '') || !/^http:\/\/127\.0\.0\.1:\d+$/.test(value.url || '')) {
      throw errorWithStatus('Node requires an id and a localhost URL, for example node-e / http://127.0.0.1:3005.', 400);
    }
    if (nodeForId(value.id)) throw errorWithStatus('A node with that id already exists.', 409);
    await mutate((state) => state.nodes.push({ id: value.id, url: value.url, status: 'unknown', lastSeen: null, lastError: null }));
    await probeNode(nodeForId(value.id));
    return json(response, 201, { added: value.id });
  }
  if (request.method === 'POST' && pathname === '/api/repair') {
    return json(response, 200, await repairAll(false));
  }
  if (request.method === 'POST' && pathname === '/api/rebalance') {
    return json(response, 200, await repairAll(true));
  }
  if (request.method === 'POST' && pathname === '/api/demo/corrupt') {
    const { key, chunkIndex = 0, nodeId } = await parseJson(request);
    const found = findVersion(key);
    const chunk = found?.version?.chunks[chunkIndex];
    const node = nodeForId(nodeId);
    if (!chunk || !node) throw errorWithStatus('Unknown object/chunk/node.', 404);
    const result = await fetchNode(node, `/chunks/${chunk.hash}/corrupt`, { method: 'POST' });
    if (!result.ok) throw errorWithStatus('Could not corrupt that replica.', 409);
    return json(response, 200, { corrupted: { key, chunkIndex, nodeId, hash: chunk.hash }, message: 'Now download the file or press Run integrity scan; Vault will repair it.' });
  }

  if (pathname.startsWith('/api/objects/')) {
    const key = decodeURIComponent(pathname.slice('/api/objects/'.length));
    if (!safeObjectKey(key)) throw errorWithStatus('Invalid object key.', 400);
    if (request.method === 'GET') {
      const found = findVersion(key, url.searchParams.get('version'));
      if (!found || !found.version) throw errorWithStatus('Object or version not found.', 404);
      const replicaDetails = found.version.chunks.map((chunk, index) => ({
        index, hash: chunk.hash, bytes: chunk.bytes, desiredReplicas: chunk.desiredReplicaCount,
        replicas: chunk.replicas,
      }));
      return json(response, 200, { ...objectSummary(key, found.object), version: found.version.id, replicas: replicaDetails });
    }
  }

  if (pathname.startsWith('/objects/')) {
    const key = decodeURIComponent(pathname.slice('/objects/'.length));
    if (!safeObjectKey(key)) throw errorWithStatus('Invalid object key.', 400);
    if (request.method === 'PUT') {
      const result = await uploadObject(key, request);
      return json(response, 201, { ...result, message: 'Version published only after every chunk reached its write quorum.' });
    }
    if (request.method === 'GET') {
      const result = await retrieveObject(key, url.searchParams.get('version'));
      response.writeHead(200, {
        'content-type': result.version.contentType,
        'content-length': result.data.length,
        'content-disposition': `attachment; filename="${String(result.version.originalFilename).replaceAll('"', '')}"`,
        'x-vault-version': result.version.id,
        'x-vault-read-repairs': result.problems.length,
      });
      return response.end(result.data);
    }
  }

  return json(response, 404, { error: 'Route not found.' });
}

async function start() {
  await ensureDirectory(coordinatorDirectory);
  metadata = await readJson(metadataFile, initialMetadata());
  // Future runs can safely gain the default nodes without destroying existing metadata.
  for (const node of configuredNodes) {
    if (!metadata.nodes.some((candidate) => candidate.id === node.id)) metadata.nodes.push({ ...node, status: 'unknown', lastSeen: null, lastError: null });
  }
  await atomicWriteJson(metadataFile, metadata);
  http.createServer((request, response) => {
    handle(request, response).catch((error) => {
      console.error('Coordinator request failed:', error.message);
      if (!response.headersSent) json(response, error.statusCode || 500, { error: error.message });
      else response.destroy(error);
    });
  }).listen(port, '127.0.0.1', async () => {
    console.log(`Vault coordinator listening at http://127.0.0.1:${port}`);
    await probeNodes();
    await repairAll(false);
  });
  setInterval(() => probeNodes().catch((error) => console.error('health check failed:', error.message)), 3000);
  setInterval(() => repairAll(false).catch((error) => console.error('background repair failed:', error.message)), 15000);
}

start().catch((error) => { console.error(error); process.exit(1); });
