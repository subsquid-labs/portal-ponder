import { onchainTable } from '@subsquid/ponder';
export const noop = onchainTable('noop', (t) => ({
  id: t.text().primaryKey(),
}));
