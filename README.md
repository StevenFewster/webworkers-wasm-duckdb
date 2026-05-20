## Backend — backend/main.py

FastAPI app with:

GET /api/stream?n=100000 — streams 100k NDJSON records (timestamp, symbol, price, volume) using async generator
100 deterministic 4-letter symbols (seeded so they're consistent across restarts)

Cross-Origin-Resource-Policy: cross-origin header so the browser can fetch it when in cross-origin-isolated mode

Run it:

```
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

## Angular app — market-data-ui/

Run it:

```
cd market-data-ui
ng serve   # COOP/COEP headers already wired in angular.json
```

## Architecture

ng serve (COOP + COEP headers)
└─ Angular App (main thread)
├─ Service Worker → caches app shell + DuckDB WASM files
└─ Web Worker (data.worker.ts)
├─ DuckDB-WASM (inner worker, mvp/eh bundle auto-selected)
│ └─ OPFS persistence (opfs://marketdata.db) when cross-origin isolated
└─ Streams NDJSON → batches of 5k → INSERT INTO market_data

## Key files

| File | Purpose |
|---|---|
| `src/app/data.worker.ts` | DuckDB init, OPFS open, NDJSON streaming ingestion, query dispatch |
| `src/app/app.ts` | Component — Worker lifecycle, query orchestration, sort/filter logic |
| `src/app/app.html` | Template — progress bar, 4 stat cards, filterable/sortable table |
| `ngsw-config.json` | Service Worker caches app shell + DuckDB WASM assets |

OPFS persistence

On first load: streams 100k records from FastAPI → DuckDB in-memory → persisted to opfs://marketdata.db. On reload: worker detects data already in the table and skips the download — shown with a blue OPFS Cache badge. OPFS requires the COOP/COEP headers (already configured in angular.json for ng serve).
