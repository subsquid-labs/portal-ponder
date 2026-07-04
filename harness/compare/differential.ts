/**
 * Correctness: Portal-derived logs must equal ground-truth JSON-RPC logs.
 *
 * Fetches the same (address, topic, range) via (a) this fork's Portal path
 * → SyncLog, and (b) a real RPC node via viem eth_getLogs, then asserts the
 * sets are identical (blockNumber, logIndex, address, topics, data). This is
 * the property the full sync-store differential will assert per interval.
 *
 * Env: RPC_URL (required), DATASET, ADDRESS, TOPIC0, FROM, TO.
 */
import { createPublicClient, http, numberToHex } from 'viem';
import { PortalClient } from '../../packages/portal-sync/src/portal-client.ts';
import { buildPortalQuery } from '../../packages/portal-sync/src/query.ts';
import { toSyncLog } from '../../packages/portal-sync/src/transform.ts';

const RPC_URL = process.env.RPC_URL ?? 'https://eth.llamarpc.com';
const DATASET = process.env.DATASET ?? 'ethereum-mainnet';
// WETH, Transfer(address,address,uint256)
const ADDRESS = (
  process.env.ADDRESS ?? '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
).toLowerCase();
const TOPIC0 =
  process.env.TOPIC0 ??
  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const FROM = Number(process.env.FROM ?? 21_000_000);
const TO = Number(process.env.TO ?? 21_000_050);

const key = (l: { blockNumber: string; logIndex: string }) =>
  `${BigInt(l.blockNumber)}:${BigInt(l.logIndex)}`;

// --- Portal path ---
const portal = new PortalClient({ dataset: DATASET });
const portalLogs: ReturnType<typeof toSyncLog>[] = [];
for await (const batch of portal.streamFinalized(
  buildPortalQuery([FROM, TO], [{ address: [ADDRESS], topic0: [TOPIC0] }]),
)) {
  for (const b of batch.blocks)
    for (const log of b.logs ?? []) portalLogs.push(toSyncLog(log, b.header));
}

// --- RPC ground truth ---
const rpc = createPublicClient({ transport: http(RPC_URL) });
const rpcLogs = (await rpc.request({
  method: 'eth_getLogs',
  params: [
    {
      address: ADDRESS as `0x${string}`,
      topics: [TOPIC0 as `0x${string}`],
      fromBlock: numberToHex(FROM),
      toBlock: numberToHex(TO),
    },
  ],
})) as any[];

// --- compare ---
const portalMap = new Map(portalLogs.map((l) => [key(l), l]));
const rpcMap = new Map(
  rpcLogs.map((l) => [
    key({ blockNumber: l.blockNumber, logIndex: l.logIndex }),
    l,
  ]),
);

console.log(
  `\n=== DIFFERENTIAL: ${DATASET} ${ADDRESS} topic0=${TOPIC0.slice(0, 10)} [${FROM},${TO}] ===`,
);
console.log(`Portal logs: ${portalLogs.length} | RPC logs: ${rpcLogs.length}`);

let mismatches = 0;
const allKeys = new Set([...portalMap.keys(), ...rpcMap.keys()]);
for (const k of allKeys) {
  const p = portalMap.get(k),
    r = rpcMap.get(k);
  if (!p) {
    console.log(`  MISSING in Portal: ${k}`);
    mismatches++;
    continue;
  }
  if (!r) {
    console.log(`  EXTRA in Portal:   ${k}`);
    mismatches++;
    continue;
  }
  const fields: [string, unknown, unknown][] = [
    ['address', p.address, r.address.toLowerCase()],
    ['data', p.data, r.data],
    ['topics', JSON.stringify(p.topics), JSON.stringify(r.topics)],
    ['txHash', p.transactionHash, r.transactionHash],
    ['blockHash', p.blockHash, r.blockHash],
    [
      'blockNumber',
      BigInt(p.blockNumber).toString(),
      BigInt(r.blockNumber).toString(),
    ],
    ['logIndex', BigInt(p.logIndex).toString(), BigInt(r.logIndex).toString()],
    [
      'txIndex',
      BigInt(p.transactionIndex).toString(),
      BigInt(r.transactionIndex).toString(),
    ],
  ];
  for (const [name, a, b] of fields) {
    if (a !== b) {
      console.log(`  FIELD MISMATCH ${k} ${name}: portal=${a} rpc=${b}`);
      mismatches++;
    }
  }
}

const pass = mismatches === 0 && portalLogs.length === rpcLogs.length;
console.log(
  `\n  result: ${pass ? '✅ IDENTICAL' : `❌ ${mismatches} mismatches`} (counts ${portalLogs.length === rpcLogs.length ? 'match' : 'DIFFER'})`,
);
process.exit(pass ? 0 : 1);
