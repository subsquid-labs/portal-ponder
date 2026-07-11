import { ponder } from 'ponder:registry';
import { counter, deposit, vault } from 'ponder:schema';
import { EVaultAbi } from '../abis/EVault';

const logId = (event: any) => `${event.transaction.hash}-${event.log.logIndex}`;

const base = (event: any) => ({
  id: logId(event),
  vault: event.log.address,
  blockNumber: event.block.number,
  logIndex: event.log.logIndex,
  txHash: event.transaction.hash,
});

async function readVaultMetadata(context: any, id: `0x${string}`) {
  const read = (functionName: string) =>
    context.client
      .readContract({ abi: EVaultAbi, address: id, functionName })
      .catch(() => null);

  const [asset, name, symbol, decimals, oracle, creator, evc] =
    await Promise.all([
      read('asset'),
      read('name'),
      read('symbol'),
      read('decimals'),
      read('oracle'),
      read('creator'),
      read('EVC'),
    ]);

  return {
    asset,
    name,
    symbol,
    decimals: decimals == null ? null : Number(decimals),
    oracle,
    creator,
    evc,
  };
}

async function ensureVault(event: any, context: any) {
  const id = event.log.address as `0x${string}`;
  if (await context.db.find(vault, { id })) return;

  await context.db
    .insert(vault)
    .values({
      id,
      asset: null,
      name: null,
      symbol: null,
      decimals: null,
      oracle: null,
      creator: null,
      evc: null,
      createdBlock: event.block.number,
    })
    .onConflictDoNothing();
}

async function bump(context: any, type: string) {
  for (const id of [type, 'global']) {
    await context.db
      .insert(counter)
      .values({ id, value: 1n })
      .onConflictDoUpdate((row: any) => ({ value: row.value + 1n }));
  }
}

ponder.on('EVaultFactory:ProxyCreated', async ({ event, context }) => {
  const id = event.args.proxy;
  const metadata = await readVaultMetadata(context, id);

  await context.db
    .insert(vault)
    .values({
      id,
      ...metadata,
      createdBlock: event.block.number,
    })
    .onConflictDoNothing();
});

ponder.on('EVault:Deposit', async ({ event, context }) => {
  await ensureVault(event, context);
  await context.db
    .insert(deposit)
    .values({
      ...base(event),
      sender: event.args.sender,
      owner: event.args.owner,
      assets: event.args.assets,
      shares: event.args.shares,
    })
    .onConflictDoNothing();
  await bump(context, 'deposit');
});
