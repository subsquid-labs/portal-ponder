import { db } from 'ponder:api';
import schema from 'ponder:schema';
import { client, graphql } from '@subsquid/ponder';
import { Hono } from 'hono';

const app = new Hono();

app.use('/sql/*', client({ db, schema }));
app.use('/', graphql({ db, schema }));
app.use('/graphql', graphql({ db, schema }));

export default app;
