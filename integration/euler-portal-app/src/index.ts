import { ponder } from "ponder:registry";
import { vault, vaultEvent } from "ponder:schema";

async function record(context: any, event: any, type: string) {
  const chain: string = context.chain.name;
  const id = `${chain}:${event.log.address}`;
  await context.db
    .insert(vault)
    .values({ id, chain, address: event.log.address, eventCount: 1 })
    .onConflictDoUpdate((row: any) => ({ eventCount: row.eventCount + 1 }));
  await context.db.insert(vaultEvent).values({
    id: event.id, chain, vault: event.log.address, type, blockNumber: event.block.number,
  });
}

ponder.on("EVault:Deposit", ({ event, context }) => record(context, event, "deposit"));
ponder.on("EVault:Withdraw", ({ event, context }) => record(context, event, "withdraw"));
ponder.on("EVault:Borrow", ({ event, context }) => record(context, event, "borrow"));
ponder.on("EVault:Repay", ({ event, context }) => record(context, event, "repay"));
ponder.on("EVault:Liquidate", ({ event, context }) => record(context, event, "liquidate"));
