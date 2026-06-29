/**
 * SQD Portal EVM query/response shapes (subset we use).
 * Reference: docs.sqd.dev EVM API + OpenAPI; verified live against
 * portal.sqd.dev/datasets/{slug}/finalized-stream.
 */

export type Hex = `0x${string}`;

export type PortalLogRequest = {
  address?: string[];
  topic0?: string[];
  topic1?: string[];
  topic2?: string[];
  topic3?: string[];
  /** pull related items into the same stream */
  transaction?: boolean;
  transactionTraces?: boolean;
  transactionLogs?: boolean;
};

export type PortalTxRequest = {
  from?: string[];
  to?: string[];
  sighash?: string[];
  logs?: boolean;
  traces?: boolean;
};

export type PortalTraceRequest = {
  type?: string[];
  createFrom?: string[];
  callFrom?: string[];
  callTo?: string[];
  callSighash?: string[];
  transaction?: boolean;
  subtraces?: boolean;
};

export type PortalFieldSelection = {
  block?: Record<string, boolean>;
  transaction?: Record<string, boolean>;
  log?: Record<string, boolean>;
  trace?: Record<string, boolean>;
};

export type PortalEvmQuery = {
  type: "evm";
  fromBlock: number;
  toBlock?: number;
  parentBlockHash?: string;
  includeAllBlocks?: boolean;
  fields: PortalFieldSelection;
  logs?: PortalLogRequest[];
  transactions?: PortalTxRequest[];
  traces?: PortalTraceRequest[];
};

/** One NDJSON line. Only `header.number` is guaranteed present. */
export type PortalBlock = {
  header: { number: number; hash?: string; parentHash?: string; timestamp?: number; [k: string]: unknown };
  logs?: any[];
  transactions?: any[];
  traces?: any[];
};

export type BlockRef = { number: number; hash: string };
