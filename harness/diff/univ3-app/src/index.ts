import { ponder } from 'ponder:registry';

for (const ev of ['Swap', 'Mint', 'Burn'])
  ponder.on(`Pool:${ev}` as any, async () => {});
