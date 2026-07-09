import { ponder } from 'ponder:registry';
import { vault, vaultEvent } from 'ponder:schema';

const vaultCounts = new Map<string, number>();

async function record(context: any, event: any, type: string) {
  const chain: string = context.chain.name;
  const id = `${chain}:${event.log.address}`;
  await context.db
    .insert(vault)
    .values({ id, chain, address: event.log.address, eventCount: 1 })
    .onConflictDoUpdate((row: any) => ({ eventCount: row.eventCount + 1 }));
  await context.db.insert(vaultEvent).values({
    id: event.id,
    chain,
    vault: event.log.address,
    type,
    blockNumber: event.block.number,
  });
}

ponder.on('EVaultFactory:ProxyCreated', async ({ event, context }) => {
  const chain: string = context.chain.name;
  const address = event.args.proxy;
  const id = `${chain}:${address}`;
  const count = (vaultCounts.get(chain) ?? 0) + 1;
  vaultCounts.set(chain, count);
  await context.db
    .insert(vault)
    .values({ id, chain, address, eventCount: 0 })
    .onConflictDoNothing();
  console.log(`  ▸ ${chain} vault #${count} discovered: ${address}`);
});

ponder.on('EVault:Deposit', ({ event, context }) =>
  record(context, event, 'deposit'),
);
ponder.on('EVault:Withdraw', ({ event, context }) =>
  record(context, event, 'withdraw'),
);
ponder.on('EVault:Borrow', ({ event, context }) =>
  record(context, event, 'borrow'),
);
ponder.on('EVault:Repay', ({ event, context }) =>
  record(context, event, 'repay'),
);
ponder.on('EVault:Liquidate', ({ event, context }) =>
  record(context, event, 'liquidate'),
);
