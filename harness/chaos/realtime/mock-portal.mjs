// mock-portal.mjs - scripted local SQD Portal + minimal JSON-RPC for the realtime chaos harness.
//
// The public Portal surface under test is /stream plus /finalized-head. This fork's historical Portal
// client also uses /finalized-stream, so the mock implements that route too; otherwise the stream-mode
// app could not reach the realtime cutover against a local-only upstream.

import { appendFileSync, mkdirSync, readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { keccak256, stringToHex } from 'viem';

const DEFAULT_PORT = 8701;
const DEFAULT_HOST = '127.0.0.1';
const ZERO_HASH = `0x${'00'.repeat(32)}`;
const ZERO_BLOOM = `0x${'00'.repeat(256)}`;
const FACTORY =
  process.env.EULER_FACTORY ?? '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e';

export const DEFAULT_SCENARIO = {
  chainId: 1,
  genesis: {
    number: 100,
    timestamp: 1_700_000_000,
  },
  finalizedHeadSeq: [{ number: 100 }],
  steps: [{ type: 'blocks', count: 12, emitPhase: 'K1-append' }],
};

const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && Array.isArray(value) === false;

export function mergeScenario(base, override) {
  if (!isPlainObject(override)) return structuredClone(base);

  const out = structuredClone(base);
  for (const [key, value] of Object.entries(override)) {
    if (isPlainObject(value) && isPlainObject(out[key])) {
      out[key] = mergeScenario(out[key], value);
      continue;
    }
    out[key] = structuredClone(value);
  }

  return out;
}

export function hashBlock(number, branchId = 'main') {
  return keccak256(stringToHex(`block:${number}:${branchId}`));
}

export function advanceScenarioCursor(cursor, step) {
  const count = Number(step?.count ?? 0);
  if (step?.type === 'blocks' || step?.type === 'fork') {
    return cursor + Math.max(0, count);
  }
  if (step?.type === 'childDiscovery') return Number(step.block ?? cursor + 1);
  if (step?.type === 'gapTrigger') return Number(step.block ?? cursor + 1);
  if (step?.type === 'wrongForkFinalize') return cursor;
  if (step?.type === 'idle204' || step?.type === 'status409') return cursor;
  if (step?.type === 'awaitRedelivery') return cursor;

  return cursor;
}

export function normalizeScenario(input) {
  const scenario = mergeScenario(DEFAULT_SCENARIO, input ?? {});
  const genesis = scenario.genesis;
  genesis.hash ??= hashBlock(genesis.number, 'main');
  genesis.parentHash ??= hashBlock(genesis.number - 1, 'main');
  genesis.timestamp ??= 1_700_000_000 + genesis.number * 12;
  if (!Array.isArray(scenario.finalizedHeadSeq)) {
    scenario.finalizedHeadSeq = [
      { number: genesis.number, hash: genesis.hash },
    ];
  }
  scenario.finalizedHeadSeq = scenario.finalizedHeadSeq.map((head) => ({
    number: Number(head.number),
    hash: head.hash ?? hashBlock(Number(head.number), 'main'),
  }));
  scenario.steps = Array.isArray(scenario.steps) ? scenario.steps : [];

  return scenario;
}

function json(res, code, body) {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(body));
}

function notFound(res) {
  json(res, 404, { error: 'not found' });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8');
      if (text.length === 0) {
        resolve(undefined);
        return;
      }
      try {
        resolve(JSON.parse(text));
      } catch (error) {
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

function hexQuantity(number) {
  return `0x${BigInt(number).toString(16)}`;
}

function paddedAddressTopic(address) {
  return `0x${address.toLowerCase().replace(/^0x/, '').padStart(64, '0')}`;
}

function deterministicAddress(label) {
  return `0x${hashBlock(0, label).slice(-40)}`;
}

function headerFor(number, branchId = 'main', parentBranchId = branchId) {
  return {
    number,
    hash: hashBlock(number, branchId),
    parentHash:
      number === 0 ? ZERO_HASH : hashBlock(number - 1, parentBranchId),
    timestamp: 1_700_000_000 + number * 12,
    logsBloom: ZERO_BLOOM,
    miner: deterministicAddress(`miner:${number}`),
    gasUsed: '0x0',
    gasLimit: '0x1c9c380',
    baseFeePerGas: '0x3b9aca00',
    nonce: '0x0000000000000000',
    mixHash: hashBlock(number, `mix:${branchId}`),
    stateRoot: hashBlock(number, `state:${branchId}`),
    receiptsRoot: hashBlock(number, `receipts:${branchId}`),
    transactionsRoot: hashBlock(number, `txroot:${branchId}`),
    sha3Uncles: hashBlock(number, `uncles:${branchId}`),
    size: '0x1',
    difficulty: '0x0',
    totalDifficulty: '0x0',
    extraData: '0x',
  };
}

function rpcBlock(number, branchId = 'main') {
  const header = headerFor(number, branchId);

  return {
    ...header,
    number: hexQuantity(number),
    timestamp: hexQuantity(header.timestamp),
    gasUsed: hexQuantity(0),
    gasLimit: hexQuantity(30_000_000),
    baseFeePerGas: hexQuantity(1_000_000_000),
    size: hexQuantity(1),
    difficulty: hexQuantity(0),
    totalDifficulty: hexQuantity(0),
    transactions: [],
    uncles: [],
  };
}

function requestMatchesLog(log, request) {
  if (request.address !== undefined) {
    const addresses = request.address.map((a) => a.toLowerCase());
    if (addresses.includes(log.address.toLowerCase()) === false) return false;
  }
  for (const [idx, key] of ['topic0', 'topic1', 'topic2', 'topic3'].entries()) {
    const expected = request[key];
    if (expected === undefined) continue;

    const actual = log.topics[idx]?.toLowerCase();
    const allowed = expected.map((t) => t.toLowerCase());
    if (allowed.includes(actual) === false) return false;
  }

  return true;
}

function filterLogs(logs, requests) {
  if (!Array.isArray(requests) || requests.length === 0) return [];

  const out = [];
  const seen = new Set();
  for (const log of logs) {
    for (const request of requests) {
      if (requestMatchesLog(log, request) === false) continue;

      const key = `${log.transactionHash}:${log.logIndex}`;
      if (seen.has(key) === false) {
        seen.add(key);
        out.push(log);
      }
      break;
    }
  }

  return out;
}

function transactionsForLogs(logs, blockNumber) {
  const out = [];
  const seen = new Set();
  for (const log of logs) {
    if (seen.has(log.transactionHash)) continue;

    seen.add(log.transactionHash);
    out.push({
      transactionIndex: log.transactionIndex ?? 0,
      hash: log.transactionHash,
      from: deterministicAddress(`from:${blockNumber}:${log.logIndex}`),
      to: log.address,
      input: '0x',
      value: '0x0',
      nonce: 0,
      gas: '0x5208',
      gasPrice: '0x3b9aca00',
      maxFeePerGas: '0x3b9aca00',
      maxPriorityFeePerGas: '0x0',
      type: 0,
      r: `0x${'01'.repeat(32)}`,
      s: `0x${'02'.repeat(32)}`,
      v: '0x1b',
      yParity: 0,
    });
  }

  return out;
}

function factoryLog(number, child, topic0) {
  return {
    address: FACTORY.toLowerCase(),
    topics: [topic0, paddedAddressTopic(child)],
    data: '0x',
    transactionHash: hashBlock(number, `factory-tx:${child}`),
    transactionIndex: 0,
    logIndex: 0,
  };
}

function matchingFactoryTopic(requests) {
  const factory = FACTORY.toLowerCase();
  for (const request of requests ?? []) {
    const addresses = request.address?.map((a) => a.toLowerCase());
    if (addresses !== undefined && addresses.includes(factory) === false) {
      continue;
    }
    if (request.topic0?.[0] !== undefined) return request.topic0[0];
  }

  return hashBlock(0, 'ProxyCreated').slice(0, 66);
}

function blockBatch(number, request, opts = {}) {
  const header = opts.header ?? headerFor(number, opts.branchId ?? 'main');
  const rawLogs = opts.logs ?? [];
  const logs = filterLogs(rawLogs, request.logs);
  const wantsTx = (request.logs ?? []).some((r) => r.transaction === true);

  return {
    header,
    logs,
    transactions: wantsTx ? transactionsForLogs(logs, number) : [],
  };
}

function writeNdjson(res, batch) {
  res.write(`${JSON.stringify(batch)}\n`);
}

function createRuntime(initialScenario, options = {}) {
  let scenario = normalizeScenario(initialScenario);
  let stepIndex = 0;
  let streamCursor = scenario.genesis.number;
  let finalizedIndex = 0;
  let seq = 0;
  let currentPhase = {
    name: 'idle',
    phase: 'idle',
    seq,
    blocked: false,
    details: {},
  };
  const releaseWaiters = new Map();
  const stats = {
    requests: 0,
    stream: 0,
    finalizedStream: 0,
    finalizedHead: 0,
    rpc: 0,
    r200: 0,
    r204: 0,
    r409: 0,
    phases: {},
    redeliveryReopens: 0,
    requestLog: [],
  };
  const phaseLog = options.phaseLog;
  const autoRelease = options.autoRelease ?? false;

  const appendPhaseLog = (phase) => {
    if (phaseLog === undefined) return;

    mkdirSync(dirname(phaseLog), { recursive: true });
    appendFileSync(phaseLog, `${JSON.stringify(phase)}\n`);
  };

  const setPhase = (name, blocked, details) => {
    seq += 1;
    currentPhase = { name, phase: name, seq, blocked, details: details ?? {} };
    stats.phases[name] = (stats.phases[name] ?? 0) + 1;
    appendPhaseLog({
      at: new Date().toISOString(),
      ...currentPhase,
    });
  };

  const gate = async (name, details, res) => {
    setPhase(name, true, details);
    if (autoRelease) {
      setImmediate(() => release(name));
    }

    await new Promise((resolveGate) => {
      const waiters = releaseWaiters.get(name) ?? [];
      waiters.push(resolveGate);
      releaseWaiters.set(name, waiters);
      res.once('close', resolveGate);
    });
    setPhase(name, false, { ...details, released: true });

    return res.writableEnded === false && res.destroyed === false;
  };

  const release = (name) => {
    const waiters = releaseWaiters.get(name) ?? [];
    releaseWaiters.delete(name);
    for (const resolveGate of waiters) resolveGate();
  };

  const reset = () => {
    stepIndex = 0;
    streamCursor = scenario.genesis.number;
    finalizedIndex = 0;
    for (const name of releaseWaiters.keys()) release(name);
    currentPhase = {
      name: 'idle',
      phase: 'idle',
      seq,
      blocked: false,
      details: {},
    };
    stats.requests = 0;
    stats.stream = 0;
    stats.finalizedStream = 0;
    stats.finalizedHead = 0;
    stats.rpc = 0;
    stats.r200 = 0;
    stats.r204 = 0;
    stats.r409 = 0;
    stats.phases = {};
    stats.redeliveryReopens = 0;
    stats.requestLog = [];
  };

  const load = (override) => {
    scenario = normalizeScenario(mergeScenario(scenario, override));
    reset();
  };

  const recordRequest = (kind, body) => {
    stats.requests += 1;
    stats[kind] += 1;
    stats.requestLog.push({
      seq: stats.requests,
      kind,
      fromBlock: body?.fromBlock,
      toBlock: body?.toBlock,
      parentBlockHash: body?.parentBlockHash,
      includeAllBlocks: body?.includeAllBlocks,
      logs: Array.isArray(body?.logs) ? body.logs.length : undefined,
    });
  };

  const finalizedHead = async (res) => {
    recordRequest('finalizedHead');
    const head =
      scenario.finalizedHeadSeq[
        Math.min(finalizedIndex, scenario.finalizedHeadSeq.length - 1)
      ] ?? scenario.finalizedHeadSeq[0];
    if (finalizedIndex < scenario.finalizedHeadSeq.length - 1) {
      finalizedIndex += 1;
    }
    stats.r200 += 1;
    json(res, 200, head);
  };

  const finalizedStream = async (body, res) => {
    recordRequest('finalizedStream', body);
    const from = Number(body?.fromBlock ?? scenario.genesis.number);
    const head = scenario.finalizedHeadSeq[0] ?? {
      number: scenario.genesis.number,
    };
    const to = Math.min(Number(body?.toBlock ?? head.number), head.number);
    if (from > to) {
      stats.r204 += 1;
      res.writeHead(204);
      res.end();
      return;
    }

    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    for (let number = from; number <= to; number++) {
      writeNdjson(res, blockBatch(number, body));
    }
    stats.r200 += 1;
    res.end();
  };

  const handleStatus409 = async (step, body, res) => {
    const phase = step.emitPhase;
    if (phase !== undefined) {
      const open = await gate(
        phase,
        {
          stepIndex,
          fromBlock: body.fromBlock,
          parentBlockHash: body.parentBlockHash,
        },
        res,
      );
      if (open === false) return;
    }
    stats.r409 += 1;
    res.writeHead(409, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ previousBlocks: step.previousBlocks ?? [] }));
    stepIndex += 1;
  };

  const handleIdle204 = async (step, body, res) => {
    if (step.emitPhase !== undefined) {
      const open = await gate(
        step.emitPhase,
        {
          stepIndex,
          fromBlock: body.fromBlock,
          parentBlockHash: body.parentBlockHash,
        },
        res,
      );
      if (open === false) return;
    }
    stats.r204 += 1;
    res.writeHead(204);
    res.end();
    stepIndex += 1;
  };

  const handleBlocks = async (step, body, res) => {
    const requestFrom = Number(body?.fromBlock ?? streamCursor + 1);
    streamCursor = requestFrom - 1;

    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    const count = Math.max(0, Number(step.count ?? 0));
    for (let i = 0; i < count; i++) {
      const number = streamCursor + 1;
      writeNdjson(res, blockBatch(number, body));
      streamCursor = number;

      if (step.emitPhase !== undefined && i === 0 && i < count - 1) {
        const open = await gate(
          step.emitPhase,
          {
            stepIndex,
            afterBlock: number,
            nextBlock: number + 1,
            fromBlock: body.fromBlock,
            parentBlockHash: body.parentBlockHash,
          },
          res,
        );
        if (open === false) return;
      }
    }
    stepIndex += 1;
    stats.r200 += 1;
    res.end();
  };

  const handleChildDiscovery = async (step, body, res) => {
    const number = Number(step.block);
    const child = step.child ?? deterministicAddress(`child:${number}`);
    const topic0 = matchingFactoryTopic(body.logs);
    const log = factoryLog(number, child, topic0);
    const header = headerFor(number, 'main');
    streamCursor = number;
    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    writeNdjson(res, blockBatch(number, body, { header, logs: [log] }));
    stats.r200 += 1;
    stepIndex += 1;
    res.end();
  };

  const handleAwaitRedelivery = async (step, body, res) => {
    const block = Number(step.block);
    const parent = hashBlock(block - 1, 'main');
    const isRedelivery =
      Number(body.fromBlock) === block && body.parentBlockHash === parent;
    if (isRedelivery) stats.redeliveryReopens += 1;

    if (isRedelivery === false) {
      stats.r204 += 1;
      res.writeHead(204);
      res.end();
      return;
    }

    if (step.emitPhase !== undefined) {
      const open = await gate(
        step.emitPhase,
        {
          stepIndex,
          fromBlock: body.fromBlock,
          parentBlockHash: body.parentBlockHash,
          heldFinalizePending: true,
        },
        res,
      );
      if (open === false) return;
    }
    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    writeNdjson(res, blockBatch(block, body));
    stats.r200 += 1;
    stepIndex += 1;
    res.end();
  };

  const handleGapTrigger = async (step, body, res) => {
    const number = Number(step.block ?? streamCursor + 1);
    const header = {
      ...headerFor(number, 'main'),
      parentHash: step.parentHash ?? hashBlock(number - 10, 'unknown-parent'),
    };
    if (step.emitPhase !== undefined) {
      setPhase(step.emitPhase, false, { stepIndex, block: number });
    }
    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    writeNdjson(res, blockBatch(number, body, { header }));
    stats.r200 += 1;
    stepIndex += 1;
    res.end();
  };

  const handleWrongForkFinalize = async (step, body, res) => {
    const number = Number(step.block);
    scenario.finalizedHeadSeq.splice(finalizedIndex, 0, {
      number,
      hash: step.canonicalHash ?? hashBlock(number, 'wrong-fork'),
    });
    if (step.emitPhase !== undefined) {
      setPhase(step.emitPhase, false, { stepIndex, block: number });
    }
    stepIndex += 1;
    await stream(body, res);
  };

  const stream = async (body, res) => {
    recordRequest('stream', body);
    const step = scenario.steps[stepIndex];
    if (step === undefined) {
      stats.r204 += 1;
      res.writeHead(204);
      res.end();
      return;
    }

    switch (step.type) {
      case 'blocks':
      case 'fork':
        await handleBlocks(step, body, res);
        return;
      case 'status409':
        await handleStatus409(step, body, res);
        return;
      case 'idle204':
        await handleIdle204(step, body, res);
        return;
      case 'childDiscovery':
        await handleChildDiscovery(step, body, res);
        return;
      case 'awaitRedelivery':
        await handleAwaitRedelivery(step, body, res);
        return;
      case 'gapTrigger':
        await handleGapTrigger(step, body, res);
        return;
      case 'wrongForkFinalize':
        await handleWrongForkFinalize(step, body, res);
        return;
      default:
        json(res, 500, { error: `unknown scenario step: ${step.type}` });
    }
  };

  const rpc = async (body, res) => {
    stats.rpc += 1;
    const method = body?.method;
    const params = Array.isArray(body?.params) ? body.params : [];
    const id = body?.id ?? null;
    const scenarioEnd =
      Number(process.env.PONDER_END ?? scenario.genesis.number + 12) ||
      scenario.genesis.number + 12;
    const end =
      Number(process.env.MOCK_RPC_HEAD ?? scenarioEnd + 65) || scenarioEnd + 65;
    const head = scenario.finalizedHeadSeq[0] ?? {
      number: scenario.genesis.number,
    };
    let result;
    if (method === 'eth_chainId') result = hexQuantity(scenario.chainId ?? 1);
    else if (method === 'net_version') result = String(scenario.chainId ?? 1);
    else if (method === 'web3_clientVersion') result = 'rg3-mock-portal/phaseA';
    else if (method === 'eth_blockNumber') result = hexQuantity(end);
    else if (method === 'eth_getBlockByNumber') {
      const tag = params[0];
      let number;
      if (tag === 'latest') number = end;
      else if (tag === 'safe' || tag === 'finalized') number = head.number;
      else number = Number.parseInt(String(tag), 16);
      result = rpcBlock(number);
    } else if (method === 'eth_getBlockByHash') {
      const hash = String(params[0]).toLowerCase();
      let found;
      for (let number = 0; number <= end; number++) {
        if (hashBlock(number).toLowerCase() === hash) {
          found = number;
          break;
        }
      }
      result = found === undefined ? null : rpcBlock(found);
    } else if (method === 'eth_getLogs') result = [];
    else if (method === 'eth_call') result = '0x';
    else if (method === 'eth_getTransactionReceipt') result = null;
    else if (method === 'eth_getTransactionByHash') result = null;
    else if (method === 'eth_getTransactionCount') result = '0x0';
    else {
      json(res, 200, {
        jsonrpc: '2.0',
        id,
        error: {
          code: -32601,
          message: `unsupported mock RPC method ${method}`,
        },
      });
      return;
    }

    json(res, 200, { jsonrpc: '2.0', id, result });
  };

  return {
    phase: () => currentPhase,
    stats: () => structuredClone(stats),
    reset,
    load,
    release,
    finalizedHead,
    finalizedStream,
    stream,
    rpc,
  };
}

function scenarioFromEnv() {
  if (process.env.MOCK_SCENARIO === undefined) return DEFAULT_SCENARIO;

  return JSON.parse(readFileSync(resolve(process.env.MOCK_SCENARIO), 'utf8'));
}

export async function main() {
  const runtime = createRuntime(scenarioFromEnv(), {
    phaseLog: process.env.MOCK_PHASE_LOG,
    autoRelease: process.env.MOCK_AUTO_RELEASE === '1',
  });
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', `http://${req.headers.host}`);
      if (req.method === 'GET' && url.pathname === '/__phase') {
        json(res, 200, runtime.phase());
        return;
      }
      if (req.method === 'GET' && url.pathname === '/__stats') {
        json(res, 200, runtime.stats());
        return;
      }
      if (req.method === 'GET' && url.pathname === '/finalized-head') {
        await runtime.finalizedHead(res);
        return;
      }
      if (req.method === 'POST' && url.pathname === '/__release') {
        const body = await readBody(req);
        runtime.release(body?.phase);
        json(res, 200, { ok: true });
        return;
      }
      if (req.method === 'POST' && url.pathname === '/__reset') {
        runtime.reset();
        json(res, 200, { ok: true });
        return;
      }
      if (req.method === 'POST' && url.pathname === '/__load') {
        const body = await readBody(req);
        runtime.load(body);
        json(res, 200, { ok: true });
        return;
      }
      if (req.method === 'POST' && url.pathname === '/stream') {
        await runtime.stream(await readBody(req), res);
        return;
      }
      if (req.method === 'POST' && url.pathname === '/finalized-stream') {
        await runtime.finalizedStream(await readBody(req), res);
        return;
      }
      if (req.method === 'POST' && url.pathname === '/rpc') {
        await runtime.rpc(await readBody(req), res);
        return;
      }
      notFound(res);
    } catch (error) {
      json(res, 500, { error: error.message });
    }
  });

  const port = Number(process.env.MOCK_PORT ?? DEFAULT_PORT);
  const host = process.env.MOCK_HOST ?? DEFAULT_HOST;
  server.listen(port, host, () => {
    console.log(`mock-portal listening on http://${host}:${port}`);
  });
}

const isMain = process.argv[1]
  ? pathToFileURL(fileURLToPath(import.meta.url)).href ===
    pathToFileURL(resolve(process.argv[1])).href
  : false;

if (isMain) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
