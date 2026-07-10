const endpoint =
  process.env.PONDER_GRAPHQL_URL ?? 'http://localhost:42069/graphql';

const query = `query Summary {
  vaults {
    totalCount
  }
  deposits {
    totalCount
  }
  withdraws {
    totalCount
  }
  borrows {
    totalCount
  }
  repays {
    totalCount
  }
  liquidates {
    totalCount
  }
  vaultStatuss {
    totalCount
  }
}`;

const response = await fetch(endpoint, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ query }),
});

if (!response.ok) {
  throw new Error(`GraphQL request failed: ${response.status}`);
}

const body = await response.json();

if (body.errors) {
  throw new Error(JSON.stringify(body.errors, null, 2));
}

const { data } = body;
const actionTotal =
  data.deposits.totalCount +
  data.withdraws.totalCount +
  data.borrows.totalCount +
  data.repays.totalCount +
  data.liquidates.totalCount;

console.log(
  `${data.vaults.totalCount} vaults · ${data.deposits.totalCount} deposits · ${data.withdraws.totalCount} withdraws · ${data.borrows.totalCount} borrows · ${data.repays.totalCount} repays · ${data.liquidates.totalCount} liquidations · ${data.vaultStatuss.totalCount} vault status updates · ${actionTotal} actions indexed from the SQD Portal`,
);
console.log(
  `\nExplore the data: open ${endpoint} in your browser to run your own queries.`,
);
