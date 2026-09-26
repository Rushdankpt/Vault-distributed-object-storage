/*
 * One independent Vault storage node. It knows nothing about objects or replicas:
 * it safely stores immutable content-addressed chunks and exposes a very small HTTP API.
 */
const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const { ensureDirectory, json, now, readBody, sha256, text } = require('./utils');

const [id, portArgument, dataDirectory] = process.argv.slice(2);
const port = Number(portArgument);
if (!id || !Number.isInteger(port) || !dataDirectory) {
  console.error('Usage: node src/storage-node.js <node-id> <port> <data-directory>');
  process.exit(1);
}

const chunksDirectory = path.resolve(dataDirectory, 'chunks');
const startedAt = now();

function chunkPath(hash) {
  return path.join(chunksDirectory, hash.slice(0, 2), hash);
}

function validHash(hash) {
  return /^[a-f0-9]{64}$/.test(hash);
}

async function exists(file) {
  try { await fs.access(file); return true; } catch { return false; }
}

async function handle(request, response) {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const parts = url.pathname.split('/').filter(Boolean);

  if (request.method === 'GET' && url.pathname === '/health') {
    return json(response, 200, { id, status: 'healthy', startedAt, timestamp: now() });
  }

  if (parts[0] !== 'chunks' || !validHash(parts[1] || '')) {
    return json(response, 404, { error: 'Unknown route.' });
  }

  const hash = parts[1];
  const destination = chunkPath(hash);

  if (request.method === 'PUT' && parts.length === 2) {
    const body = await readBody(request, 20 * 1024 * 1024);
    if (sha256(body) !== hash) return json(response, 400, { error: 'Chunk checksum does not match its path.' });
    await ensureDirectory(path.dirname(destination));
    // Chunks are normally immutable. If a previous copy is corrupted, safely replace it
    // with this verified copy; that is what lets the coordinator repair corruption.
    let needsWrite = true;
    if (await exists(destination)) {
      needsWrite = sha256(await fs.readFile(destination)) !== hash;
    }
    if (needsWrite) {
      const temporary = `${destination}.${process.pid}.${Date.now()}.tmp`;
      await fs.writeFile(temporary, body);
      await fs.rename(temporary, destination);
    }
    return json(response, 201, { id, hash, bytes: body.length, storedAt: now() });
  }

  if (request.method === 'GET' && parts.length === 2) {
    try {
      const data = await fs.readFile(destination);
      return text(response, 200, data, 'application/octet-stream');
    } catch (error) {
      if (error.code === 'ENOENT') return json(response, 404, { error: 'Chunk not found.' });
      throw error;
    }
  }

  if (request.method === 'DELETE' && parts.length === 2) {
    try { await fs.unlink(destination); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    return json(response, 200, { id, hash, deleted: true });
  }

  // Development-only endpoint used in the live failure/corruption demonstration.
  if (request.method === 'POST' && parts[2] === 'corrupt') {
    try {
      await fs.writeFile(destination, Buffer.from('INTENTIONALLY CORRUPTED FOR VAULT DEMO'));
      return json(response, 200, { id, hash, corrupted: true });
    } catch (error) {
      if (error.code === 'ENOENT') return json(response, 404, { error: 'Chunk not found.' });
      throw error;
    }
  }

  return json(response, 405, { error: 'Method not allowed.' });
}

ensureDirectory(chunksDirectory).then(() => {
  http.createServer((request, response) => {
    handle(request, response).catch((error) => {
      console.error(`[${id}]`, error);
      if (!response.headersSent) json(response, error.statusCode || 500, { error: error.message });
      else response.destroy(error);
    });
  }).listen(port, '127.0.0.1', () => {
    console.log(`Vault storage node ${id} listening at http://127.0.0.1:${port}`);
  });
});
