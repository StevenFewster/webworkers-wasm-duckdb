/// <reference lib="webworker" />

import * as duckdb from '@duckdb/duckdb-wasm';

// ---------------------------------------------------------------------------
// Message contracts shared with the main thread
// ---------------------------------------------------------------------------

export type ToWorker =
  | { type: 'INIT' }
  | { type: 'LOAD'; url: string; total: number }
  | { type: 'QUERY'; sql: string; id: string };

export type FromWorker =
  | { type: 'INIT_DONE' }
  | { type: 'CACHED'; count: number }
  | { type: 'PROGRESS'; loaded: number; total: number }
  | { type: 'LOAD_DONE'; count: number }
  | { type: 'QUERY_RESULT'; id: string; rows: Record<string, unknown>[] }
  | { type: 'ERROR'; message: string };

// ---------------------------------------------------------------------------
// DuckDB bundles – served from /assets/duckdb/ via angular.json asset config
// ---------------------------------------------------------------------------

const BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: `${self.location.origin}/assets/duckdb/duckdb-mvp.wasm`,
    mainWorker: `${self.location.origin}/assets/duckdb/duckdb-browser-mvp.worker.js`,
  },
  eh: {
    mainModule: `${self.location.origin}/assets/duckdb/duckdb-eh.wasm`,
    mainWorker: `${self.location.origin}/assets/duckdb/duckdb-browser-eh.worker.js`,
  },
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

async function init(): Promise<void> {
  const bundle = await duckdb.selectBundle(BUNDLES);

  // DuckDB needs its own inner worker; create via blob URL so no separate file
  // is required and CSP allows it.
  const innerWorkerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: 'text/javascript',
    })
  );
  const innerWorker = new Worker(innerWorkerUrl);
  URL.revokeObjectURL(innerWorkerUrl);

  db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), innerWorker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  // Use OPFS persistence when cross-origin isolation is active
  // (requires COOP + COEP headers on the dev/prod server).
  const isolated =
    typeof crossOriginIsolated !== 'undefined' && crossOriginIsolated;

  if (isolated) {
    try {
      await db.open({ path: 'opfs://marketdata.db' });
    } catch {
      await db.open({});
    }
  } else {
    await db.open({});
  }

  conn = await db.connect();
}

// ---------------------------------------------------------------------------
// Table helpers
// ---------------------------------------------------------------------------

async function tableHasData(): Promise<boolean> {
  try {
    const r = await conn!.query('SELECT COUNT(*) AS n FROM market_data');
    return Number(r.toArray()[0]['n']) > 0;
  } catch {
    return false;
  }
}

async function createTable(): Promise<void> {
  await conn!.query(`
    CREATE TABLE IF NOT EXISTS market_data (
      ts      TIMESTAMP,
      symbol  VARCHAR,
      price   DOUBLE,
      volume  BIGINT
    )
  `);
}

// ---------------------------------------------------------------------------
// Data loading  (streaming NDJSON → batched DuckDB inserts)
// ---------------------------------------------------------------------------

async function loadData(url: string, total: number): Promise<void> {
  const response = await fetch(url);
  if (!response.ok || !response.body) {
    throw new Error(`Fetch failed: ${response.status} ${response.statusText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let loaded = 0;
  let batchNum = 0;
  let batch: object[] = [];
  const BATCH_SIZE = 5_000;

  const flush = async (): Promise<void> => {
    if (batch.length === 0) return;
    const ndjson = batch.map((r) => JSON.stringify(r)).join('\n');
    const fname = `batch_${batchNum++}.ndjson`;
    await db!.registerFileText(fname, ndjson);
    await conn!.query(`
      INSERT INTO market_data
      SELECT
        CAST(ts AS TIMESTAMP) AS ts,
        symbol,
        price,
        volume
      FROM read_json_auto('${fname}')
    `);
    await db!.dropFile(fname);
    batch = [];
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop()!;

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        batch.push(JSON.parse(trimmed));
        loaded++;
        if (batch.length >= BATCH_SIZE) {
          await flush();
          post({ type: 'PROGRESS', loaded, total });
        }
      } catch {
        // skip malformed lines
      }
    }
  }

  await flush();
  post({ type: 'LOAD_DONE', count: loaded });
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

async function runQuery(sql: string, id: string): Promise<void> {
  const result = await conn!.query(sql);
  const rows = result.toArray().map((row) =>
    Object.fromEntries(
      Object.entries(row as Record<string, unknown>).map(([k, v]) => [
        k,
        typeof v === 'bigint' ? Number(v) : v,
      ])
    )
  );
  post({ type: 'QUERY_RESULT', id, rows });
}

// ---------------------------------------------------------------------------
// Message bus
// ---------------------------------------------------------------------------

function post(msg: FromWorker): void {
  self.postMessage(msg);
}

self.addEventListener('message', async (event: MessageEvent<ToWorker>) => {
  const msg = event.data;
  try {
    switch (msg.type) {
      case 'INIT': {
        await init();
        post({ type: 'INIT_DONE' });
        break;
      }

      case 'LOAD': {
        await createTable();
        if (await tableHasData()) {
          const r = await conn!.query('SELECT COUNT(*) AS n FROM market_data');
          post({ type: 'CACHED', count: Number(r.toArray()[0]['n']) });
        } else {
          await loadData(msg.url, msg.total);
        }
        break;
      }

      case 'QUERY': {
        await runQuery(msg.sql, msg.id);
        break;
      }
    }
  } catch (err) {
    post({ type: 'ERROR', message: String(err) });
  }
});
