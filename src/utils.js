const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const path = require('node:path');

function sha256(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}

function json(res, statusCode, value) {
  const body = Buffer.from(JSON.stringify(value, null, 2));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': body.length,
    'cache-control': 'no-store',
  });
  res.end(body);
}

function text(res, statusCode, value, contentType = 'text/plain; charset=utf-8') {
  const body = Buffer.isBuffer(value) ? value : Buffer.from(value);
  res.writeHead(statusCode, { 'content-type': contentType, 'content-length': body.length });
  res.end(body);
}

async function readBody(req, maximumBytes = 100 * 1024 * 1024) {
  const pieces = [];
  let length = 0;
  for await (const piece of req) {
    length += piece.length;
    if (length > maximumBytes) {
      const error = new Error(`Request body is larger than ${maximumBytes} bytes.`);
      error.statusCode = 413;
      throw error;
    }
    pieces.push(piece);
  }
  return Buffer.concat(pieces);
}

async function ensureDirectory(directory) {
  await fs.mkdir(directory, { recursive: true });
}

async function readJson(file, fallback) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return fallback;
    throw error;
  }
}

// Write to a temporary file, then rename it. A restart cannot leave half a JSON file behind.
async function atomicWriteJson(file, data) {
  await ensureDirectory(path.dirname(file));
  const temporary = `${file}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(data, null, 2));
  await fs.rename(temporary, file);
}

function safeObjectKey(value) {
  if (!value || value.length > 240 || value.includes('..')) return null;
  return value;
}

function encodeKey(key) {
  return Buffer.from(key).toString('base64url');
}

function now() {
  return new Date().toISOString();
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

module.exports = { atomicWriteJson, encodeKey, ensureDirectory, json, now, readBody, readJson, safeObjectKey, sha256, sleep, text };
