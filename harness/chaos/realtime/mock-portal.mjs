// mock-portal.mjs - scripted local SQD Portal + minimal JSON-RPC for the realtime chaos harness.
//
// The public Portal surface under test is /stream plus /finalized-head. This fork's historical Portal
// client also uses /finalized-stream, so the mock implements that route too; otherwise the stream-mode
// app could not reach the realtime cutover against a local-only upstream.

import { appendFileSync, mkdirSync, readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { encodeAbiParameters, keccak256, stringToHex } from 'viem';

const DEFAULT_PORT = 8701;
const DEFAULT_HOST = '127.0.0.1';
const ZERO_HASH = `0x${'00'.repeat(32)}`;
const ZERO_BLOOM = `0x${'00'.repeat(256)}`;
const FACTORY =
  process.env.EULER_FACTORY ?? '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e';
const EVENT_SIGNATURES = {
  ProxyCreated: 'ProxyCreated(address,bool,address,bytes)',
  Deposit: 'Deposit(address,address,uint256,uint256)',
};
const DEFAULT_CHILD = '0x1111111111111111111111111111111111111111';
const DEFAULT_SENDER = '0x2222222222222222222222222222222222222222';
const DEFAULT_OWNER = '0x3333333333333333333333333333333333333333';
const DEFAULT_IMPLEMENTATION = '0x4444444444444444444444444444444444444444';

export const DEFAULT_SCENARIO = {
  chainId: 1,
  genesis: {
    number: 100,
    timestamp: 1_700_000_000,
  },
  finalizedHeadSeq: [{ number: 100 }],
  steps: [
    { type: 'blocks', count: 12, killAt: { block: 102, phase: 'K1-append' } },
  ],
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
  if (step?.type === 'rollbackApply') {
    return cursor + Math.max(0, count || 1);
  }
  if (step?.type === 'childDiscovery') return Number(step.block ?? cursor + 1);
  if (step?.type === 'gapTrigger') return Number(step.block ?? cursor + 1);
  if (step?.type === 'wrongForkFinalize') return cursor;
  if (step?.type === 'cutoverGate') {
    return cursor;
  }
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

export function eventTopic(name) {
  const signature = EVENT_SIGNATURES[name];
  if (signature === undefined) {
    throw new Error(`unknown mock event: ${name}`);
  }

  return keccak256(stringToHex(signature));
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

function factoryLog(number, child, topic0, opts = {}) {
  const log = encodeLog(
    {
      event: 'ProxyCreated',
      proxy: child,
      transactionHash:
        opts.transactionHash ?? hashBlock(number, `factory-tx:${child}`),
      logIndex: opts.logIndex ?? 0,
    },
    number,
  );

  return { ...log, topics: [topic0, ...log.topics.slice(1)] };
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

  return eventTopic('ProxyCreated');
}

function logBlockNumber(log) {
  const value = log.block ?? log.blockNumber;
  if (value === undefined) return undefined;
  if (typeof value === 'string' && value.startsWith('0x')) {
    return Number.parseInt(value, 16);
  }

  return Number(value);
}

function bigintValue(value, fallback) {
  if (value === undefined || value === null) return BigInt(fallback);
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number') return BigInt(value);
  return BigInt(value);
}

function logTransactionHash(log, number) {
  return (
    log.transactionHash ??
    hashBlock(
      number,
      `${log.event ?? 'log'}:${log.address ?? ''}:${log.logIndex ?? 0}`,
    )
  );
}

export function encodeLog(log, number) {
  if (log?.event === undefined) return log;

  const logIndex = Number(log.logIndex ?? 0);
  const transactionIndex = Number(log.transactionIndex ?? 0);
  if (log.event === 'ProxyCreated') {
    const proxy = (log.proxy ?? log.child ?? DEFAULT_CHILD).toLowerCase();
    return {
      address: (log.address ?? FACTORY).toLowerCase(),
      topics: [eventTopic('ProxyCreated'), paddedAddressTopic(proxy)],
      data: encodeAbiParameters(
        [{ type: 'bool' }, { type: 'address' }, { type: 'bytes' }],
        [
          Boolean(log.upgradeable ?? false),
          log.implementation ?? DEFAULT_IMPLEMENTATION,
          log.trailingData ?? '0x',
        ],
      ),
      transactionHash: logTransactionHash(log, number),
      transactionIndex,
      logIndex,
    };
  }
  if (log.event === 'Deposit') {
    const vault = (log.vault ?? log.address ?? DEFAULT_CHILD).toLowerCase();
    return {
      address: vault,
      topics: [
        eventTopic('Deposit'),
        paddedAddressTopic(log.sender ?? DEFAULT_SENDER),
        paddedAddressTopic(log.owner ?? DEFAULT_OWNER),
      ],
      data: encodeAbiParameters(
        [{ type: 'uint256' }, { type: 'uint256' }],
        [
          bigintValue(log.assets, 123_456_789n),
          bigintValue(log.shares, 987_654_321n),
        ],
      ),
      transactionHash: logTransactionHash(log, number),
      transactionIndex,
      logIndex,
    };
  }

  throw new Error(`unknown mock event: ${log.event}`);
}

export function logsForBlock(step, number) {
  if (Array.isArray(step?.logs) === false) return [];

  return step.logs
    .filter((log) => logBlockNumber(log) === number)
    .map((log) => {
      const emitted = { ...log };
      delete emitted.block;
      delete emitted.blockNumber;
      return encodeLog(emitted, number);
    });
}

function scenarioLogsForBlock(scenario, number) {
  return scenario.steps.flatMap((step) => logsForBlock(step, number));
}

export function gatePhaseForBlock(step, number) {
  if (step?.killAt?.phase === undefined) return undefined;
  if (logBlockNumber({ block: step.killAt.block }) !== number) {
    return undefined;
  }

  return step.killAt.phase;
}

function phaseForStep(step) {
  return step?.emitPhase ?? step?.killAt?.phase;
}

function stepMatchParentHash(step) {
  const match = step?.match ?? {};
  if (match.parentBlockHash !== undefined) return match.parentBlockHash;
  if (step?.parentBlockHash !== undefined) return step.parentBlockHash;

  const parentBlock = match.parentBlock ?? step?.parentBlock;
  if (parentBlock !== undefined) {
    return hashBlock(Number(parentBlock), match.parentBranch ?? 'main');
  }
  if (step?.type === 'awaitRedelivery') {
    return hashBlock(Number(step.block) - 1, step.branch ?? 'main');
  }

  return undefined;
}

function isCursorStep(step) {
  if (step === undefined) return false;
  return (
    new Set([
      'awaitRedelivery',
      'status409',
      'idle204',
      'wrongForkFinalize',
      'cutoverGate',
      'rollbackApply',
    ]).has(step.type) || step.match !== undefined
  );
}

export function cursorMatchesStep(step, body) {
  if (step === undefined || body === undefined) return false;
  if (isCursorStep(step) === false) return false;

  const match = step.match ?? {};
  const wantFrom =
    match.fromBlock ??
    step.fromBlock ??
    (step.type === 'awaitRedelivery'
      ? step.block
      : step.type === 'status409'
        ? Number(step.block ?? 0) + 1
        : undefined);
  if (wantFrom !== undefined && Number(body.fromBlock) !== Number(wantFrom)) {
    return false;
  }

  const wantParent = stepMatchParentHash(step);
  if (wantParent !== undefined && body.parentBlockHash !== wantParent) {
    return false;
  }

  return true;
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

function streamTurnDelay(ms = 10) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createRuntime(initialScenario, options = {}) {
  let scenario = normalizeScenario(initialScenario);
  let stepIndex = 0;
  let streamCursor = scenario.genesis.number;
  let finalizedIndex = 0;
  let resumeMode = false;
  let seq = 0;
  let pendingWrongForkHead;
  let skipWrongForkRejection = false;
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
    finalizedHeadGates: 0,
    wrongForkFinalizes: 0,
    wrongForkFinalizeConsumed: 0,
    wrongForkFinalizeRejected: 0,
    reorgApplied: 0,
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

  const reset = (opts = {}) => {
    stepIndex = 0;
    streamCursor = scenario.genesis.number;
    resumeMode = opts.resume === true;
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
    stats.finalizedHeadGates = 0;
    stats.wrongForkFinalizes = 0;
    stats.wrongForkFinalizeConsumed = 0;
    stats.wrongForkFinalizeRejected = 0;
    stats.reorgApplied = 0;
    stats.requestLog = [];
    pendingWrongForkHead = undefined;
    skipWrongForkRejection = false;
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

  const turnDelayForStep = (step) =>
    Number(
      resumeMode
        ? (step.resumeTurnDelayMs ?? step.turnDelayMs ?? 10)
        : (step.turnDelayMs ?? 10),
    );

  const observeWrongForkConsumed = (head) => {
    if (pendingWrongForkHead === undefined) {
      return;
    }
    if (head === undefined) {
      return;
    }
    if (Number(head.number) !== pendingWrongForkHead.number) {
      return;
    }
    if (head.hash !== pendingWrongForkHead.hash) {
      return;
    }

    stats.wrongForkFinalizeConsumed += 1;
    pendingWrongForkHead = undefined;
    skipWrongForkRejection = false;
  };

  const observeWrongForkStreamRequest = (body) => {
    if (pendingWrongForkHead === undefined) {
      return;
    }
    if (skipWrongForkRejection) {
      skipWrongForkRejection = false;
      return;
    }

    if (body?.parentBlockHash !== pendingWrongForkHead.hash) {
      stats.wrongForkFinalizeRejected += 1;
      pendingWrongForkHead = undefined;
    }
  };

  const finalizedHead = async (res) => {
    recordRequest('finalizedHead');
    const head =
      scenario.finalizedHeadSeq[
        Math.min(finalizedIndex, scenario.finalizedHeadSeq.length - 1)
      ] ?? scenario.finalizedHeadSeq[0];
    const phase = gatePhaseForBlock(scenario, head.number);
    if (phase !== undefined) {
      stats.finalizedHeadGates += 1;
      const open = await gate(
        phase,
        {
          route: 'finalized-head',
          head,
          finalizedIndex,
        },
        res,
      );
      if (open === false) return;
    }
    observeWrongForkConsumed(head);
    if (finalizedIndex < scenario.finalizedHeadSeq.length - 1) {
      finalizedIndex += 1;
    }
    stats.r200 += 1;
    json(res, 200, head);
  };

  const finalizedStream = async (body, res) => {
    recordRequest('finalizedStream', body);
    const from = Number(body?.fromBlock ?? scenario.genesis.number);
    const head = scenario.finalizedHeadSeq[
      Math.max(
        0,
        Math.min(finalizedIndex - 1, scenario.finalizedHeadSeq.length - 1),
      )
    ] ??
      scenario.finalizedHeadSeq[0] ?? {
        number: scenario.genesis.number,
      };
    observeWrongForkConsumed(head);
    const to = Math.min(Number(body?.toBlock ?? head.number), head.number);
    if (from > to) {
      stats.r204 += 1;
      res.writeHead(204);
      res.end();
      return;
    }

    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    for (let number = from; number <= to; number++) {
      writeNdjson(
        res,
        blockBatch(number, body, {
          logs: scenarioLogsForBlock(scenario, number),
        }),
      );
    }
    stats.r200 += 1;
    res.end();
  };

  const previousBlocksForStep = (step, body) => {
    if (Array.isArray(step.previousBlocks)) {
      return step.previousBlocks.map((block) => ({
        number: Number(block.number),
        hash: block.hash ?? hashBlock(Number(block.number), 'main'),
      }));
    }

    const last = Number(step.block ?? Number(body.fromBlock ?? 1) - 1);
    const first = Number(step.floor ?? scenario.genesis.number);
    const out = [];
    for (let number = last; number >= first; number--) {
      out.push({ number, hash: hashBlock(number, step.branch ?? 'main') });
    }

    return out;
  };

  const handleStatus409 = async (step, body, res) => {
    const phase = phaseForStep(step);
    if (phase !== undefined) {
      const open = await gate(
        phase,
        {
          stepIndex,
          block: step.block,
          fromBlock: body.fromBlock,
          parentBlockHash: body.parentBlockHash,
        },
        res,
      );
      if (open === false) return;
    }
    stats.r409 += 1;
    res.writeHead(409, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({ previousBlocks: previousBlocksForStep(step, body) }),
    );
    stepIndex += 1;
  };

  const handleCutoverGate = async (step, body, res) => {
    const number = Number(step.fromBlock);
    const phase = phaseForStep(step);
    if (phase !== undefined) {
      const open = await gate(
        phase,
        {
          stepIndex,
          block: number,
          fromBlock: body.fromBlock,
          parentBlockHash: body.parentBlockHash,
          cutoverBlock: number,
        },
        res,
      );
      if (open === false) return;
    }

    const branchId = step.branch ?? step.branchId ?? 'main';
    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    writeNdjson(
      res,
      blockBatch(number, body, {
        header: headerFor(number, branchId),
        logs: logsForBlock(step, number),
      }),
    );
    streamCursor = number;
    stepIndex += 1;
    stats.r200 += 1;
    res.end();
  };

  const handleIdle204 = async (step, body, res) => {
    if (phaseForStep(step) !== undefined) {
      const open = await gate(
        phaseForStep(step),
        {
          stepIndex,
          block: step.killAt?.block ?? step.block,
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
    const firstNumber = streamCursor + 1;

    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    const count = Math.max(0, Number(step.count ?? 0));
    for (let i = 0; i < count; i++) {
      const number = streamCursor + 1;
      const phase = gatePhaseForBlock(step, number);
      if (phase !== undefined) {
        const open = await gate(
          phase,
          {
            stepIndex,
            afterBlock: number - 1,
            nextBlock: number,
            fromBlock: body.fromBlock,
            parentBlockHash: body.parentBlockHash,
          },
          res,
        );
        if (open === false) return;
      }

      const branchId = step.branch ?? step.branchId ?? 'main';
      const parentBranchId =
        number === firstNumber && step.parentBranch !== undefined
          ? step.parentBranch
          : branchId;
      writeNdjson(
        res,
        blockBatch(number, body, {
          header: headerFor(number, branchId, parentBranchId),
          logs: logsForBlock(step, number),
        }),
      );
      streamCursor = number;
      await streamTurnDelay(turnDelayForStep(step));
      if (res.destroyed || res.writableEnded) return;
    }
    stepIndex += 1;
    stats.r200 += 1;
    res.end();
  };

  const handleChildDiscovery = async (step, body, res) => {
    const number = Number(step.block);
    const child = step.child ?? deterministicAddress(`child:${number}`);
    const topic0 = matchingFactoryTopic(body.logs);
    const log = factoryLog(number, child, topic0, step);
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

    if (phaseForStep(step) !== undefined) {
      const open = await gate(
        phaseForStep(step),
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
    writeNdjson(
      res,
      blockBatch(block, body, { logs: logsForBlock(step, block) }),
    );
    streamCursor = block;
    stats.r200 += 1;
    stepIndex += 1;
    res.end();
  };

  const handleRollbackApply = async (step, body, res) => {
    // Cursor discipline (parity with handleStatus409): only serve the reorg branch on the client's
    // natural resume cursor. `stream()` already 204s a mis-cursored request via isCursorStep, but a
    // defensive re-check here makes a stray request 204 rather than mis-deliver the rollback branch.
    if (step.match !== undefined && cursorMatchesStep(step, body) === false) {
      stats.r204 += 1;
      res.writeHead(204);
      res.end();

      return;
    }

    const reorgBlock = Number(step.reorgBlock ?? streamCursor - 1);
    const count = Math.max(0, Number(step.count ?? 1));
    const branchId = step.branch ?? step.branchId ?? 'rollback';
    const parentBranchId = step.parentBranch ?? 'main';
    res.writeHead(200, { 'content-type': 'application/x-ndjson' });
    for (let i = 0; i < count; i++) {
      const number = reorgBlock + i;
      // Count the reorg branch actually being served: the fork block (i === 0) carries a CROSS-branch
      // parentHash (parent on `parentBranchId`, self on `branchId`), which is what drives the product's
      // reconcile -> {kind:'reorg'} rollback-apply. A scenario that silently degraded to a plain
      // tip-append (fork point == branch) has parentBranchId === branchId and never increments this,
      // so a vacuous "append masquerading as rollback" cannot self-report a K7 pass.
      if (i === 0 && parentBranchId !== branchId) {
        stats.reorgApplied += 1;
      }

      const phase = gatePhaseForBlock(step, number);
      if (phase !== undefined) {
        const open = await gate(
          phase,
          {
            stepIndex,
            block: number,
            fromBlock: body.fromBlock,
            parentBlockHash: body.parentBlockHash,
            reorgBlock,
          },
          res,
        );
        if (open === false) return;
      }

      writeNdjson(
        res,
        blockBatch(number, body, {
          header: headerFor(
            number,
            branchId,
            number === reorgBlock ? parentBranchId : branchId,
          ),
          logs: logsForBlock(step, number),
        }),
      );
      streamCursor = number;
      await streamTurnDelay(turnDelayForStep(step));
      if (res.destroyed || res.writableEnded) return;
    }
    stepIndex += 1;
    stats.r200 += 1;
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
    stats.wrongForkFinalizes += 1;
    if (phaseForStep(step) !== undefined) {
      const open = await gate(
        phaseForStep(step),
        {
          stepIndex,
          block: number,
          fromBlock: body.fromBlock,
          parentBlockHash: body.parentBlockHash,
        },
        res,
      );
      if (open === false) return;
    }
    const hash =
      step.hash ?? step.canonicalHash ?? hashBlock(number, 'wrong-fork');
    scenario.finalizedHeadSeq.splice(finalizedIndex, 0, {
      number,
      hash,
    });
    pendingWrongForkHead = { number, hash };
    skipWrongForkRejection = true;
    stepIndex += 1;
    await stream(body, res);
  };

  const stream = async (body, res) => {
    recordRequest('stream', body);
    observeWrongForkStreamRequest(body);
    const step = scenario.steps[stepIndex];
    if (step === undefined) {
      stats.r204 += 1;
      res.writeHead(204);
      res.end();
      return;
    }
    if (isCursorStep(step) && cursorMatchesStep(step, body) === false) {
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
      case 'cutoverGate':
        await handleCutoverGate(step, body, res);
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
      case 'rollbackApply':
        await handleRollbackApply(step, body, res);
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

export function retargetKillAtBlock(scenario, block) {
  if (block === undefined || block === null) return scenario;
  // Empty/whitespace-only string ⇒ no override (orchestrate passes MOCK_KILLAT_BLOCK="" for classes
  // that keep the scenario's own killAt.block). Guard before Number(), since Number("") === 0.
  if (typeof block === 'string' && block.trim() === '') return scenario;

  const number = Number(block);
  if (Number.isNaN(number)) return scenario;

  if (scenario.killAt?.phase !== undefined) {
    scenario.killAt.block = number;
  }
  for (const step of Array.isArray(scenario.steps) ? scenario.steps : []) {
    if (step?.killAt?.phase !== undefined) {
      step.killAt.block = number;
    }
  }

  return scenario;
}

function scenarioFromEnv() {
  if (process.env.MOCK_SCENARIO === undefined) return DEFAULT_SCENARIO;

  const scenario = JSON.parse(
    readFileSync(resolve(process.env.MOCK_SCENARIO), 'utf8'),
  );

  return retargetKillAtBlock(scenario, process.env.MOCK_KILLAT_BLOCK);
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
        runtime.reset({ resume: true });
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
