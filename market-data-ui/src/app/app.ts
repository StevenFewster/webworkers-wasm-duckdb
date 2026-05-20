import { Component, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { FromWorker, ToWorker } from './data.worker';

interface SymbolRow {
  symbol: string;
  count: number;
  avg_price: number;
  total_volume: number;
  min_price: number;
  max_price: number;
}

interface OverallStats {
  total_records: number;
  unique_symbols: number;
  avg_price: number;
  total_volume: number;
}

type SortCol = keyof SymbolRow;
type AppStatus = 'idle' | 'initializing' | 'loading' | 'ready' | 'error';

const API_URL = 'http://localhost:8000/api/stream';
const TOTAL_RECORDS = 100_000;

@Component({
  selector: 'app-root',
  imports: [CommonModule, FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App implements OnInit, OnDestroy {
  private worker: Worker | null = null;
  private pending = new Map<string, (rows: Record<string, unknown>[]) => void>();
  private querySeq = 0;

  status: AppStatus = 'idle';
  progress = 0;
  loadedCount = 0;
  readonly totalCount = TOTAL_RECORDS;
  isCached = false;
  isIsolated = typeof crossOriginIsolated !== 'undefined' && crossOriginIsolated;
  errorMsg = '';

  stats: OverallStats | null = null;
  allRows: SymbolRow[] = [];
  visibleRows: SymbolRow[] = [];
  filterText = '';
  sortCol: SortCol = 'symbol';
  sortAsc = true;

  readonly fmt = new Intl.NumberFormat('en-GB');
  readonly fmtCcy = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
  });

  ngOnInit(): void {
    this.boot();
  }

  ngOnDestroy(): void {
    this.worker?.terminate();
  }

  // -------------------------------------------------------------------------
  // Worker lifecycle
  // -------------------------------------------------------------------------

  private boot(): void {
    this.status = 'initializing';
    this.worker = new Worker(new URL('./data.worker', import.meta.url), {
      type: 'module',
    });
    this.worker.onmessage = (e: MessageEvent<FromWorker>) =>
      this.onWorkerMessage(e.data);
    this.worker.onerror = (e) => {
      this.errorMsg = e.message;
      this.status = 'error';
    };
    this.send({ type: 'INIT' });
  }

  private send(msg: ToWorker): void {
    this.worker?.postMessage(msg);
  }

  private onWorkerMessage(msg: FromWorker): void {
    switch (msg.type) {
      case 'INIT_DONE':
        this.status = 'loading';
        this.send({ type: 'LOAD', url: API_URL, total: TOTAL_RECORDS });
        break;

      case 'CACHED':
        this.isCached = true;
        this.loadedCount = msg.count;
        this.progress = 100;
        this.fetchResults();
        break;

      case 'PROGRESS':
        this.loadedCount = msg.loaded;
        this.progress = Math.round((msg.loaded / msg.total) * 100);
        break;

      case 'LOAD_DONE':
        this.loadedCount = msg.count;
        this.progress = 100;
        this.fetchResults();
        break;

      case 'QUERY_RESULT': {
        const resolve = this.pending.get(msg.id);
        if (resolve) {
          resolve(msg.rows);
          this.pending.delete(msg.id);
        }
        break;
      }

      case 'ERROR':
        this.errorMsg = msg.message;
        this.status = 'error';
        break;
    }
  }

  private query(sql: string): Promise<Record<string, unknown>[]> {
    return new Promise((resolve) => {
      const id = String(this.querySeq++);
      this.pending.set(id, resolve);
      this.send({ type: 'QUERY', sql, id });
    });
  }

  // -------------------------------------------------------------------------
  // Data fetching
  // -------------------------------------------------------------------------

  private async fetchResults(): Promise<void> {
    const [statsRows, symbolRows] = await Promise.all([
      this.query(`
        SELECT
          COUNT(*)               AS total_records,
          COUNT(DISTINCT symbol) AS unique_symbols,
          ROUND(AVG(price), 2)   AS avg_price,
          SUM(volume)            AS total_volume
        FROM market_data
      `),
      this.query(`
        SELECT
          symbol,
          COUNT(*)             AS count,
          ROUND(AVG(price), 2) AS avg_price,
          SUM(volume)          AS total_volume,
          ROUND(MIN(price), 2) AS min_price,
          ROUND(MAX(price), 2) AS max_price
        FROM market_data
        GROUP BY symbol
        ORDER BY symbol
      `),
    ]);

    this.stats = statsRows[0] as unknown as OverallStats;
    this.allRows = symbolRows as unknown as SymbolRow[];
    this.applyFilter();
    this.status = 'ready';
  }

  // -------------------------------------------------------------------------
  // Table interaction
  // -------------------------------------------------------------------------

  applyFilter(): void {
    const needle = this.filterText.trim().toUpperCase();
    this.visibleRows = needle
      ? this.allRows.filter((r) => r.symbol.includes(needle))
      : [...this.allRows];
    this.doSort();
  }

  toggleSort(col: SortCol): void {
    if (this.sortCol === col) {
      this.sortAsc = !this.sortAsc;
    } else {
      this.sortCol = col;
      this.sortAsc = true;
    }
    this.doSort();
  }

  private doSort(): void {
    const col = this.sortCol;
    const dir = this.sortAsc ? 1 : -1;
    this.visibleRows.sort((a, b) => {
      const va = a[col];
      const vb = b[col];
      return (
        dir *
        (typeof va === 'string'
          ? va.localeCompare(vb as string)
          : (va as number) - (vb as number))
      );
    });
  }

  reload(): void {
    this.worker?.terminate();
    this.worker = null;
    this.pending.clear();
    this.status = 'idle';
    this.progress = 0;
    this.loadedCount = 0;
    this.isCached = false;
    this.stats = null;
    this.allRows = [];
    this.visibleRows = [];
    this.filterText = '';
    this.errorMsg = '';
    this.boot();
  }

  // -------------------------------------------------------------------------
  // Template helpers
  // -------------------------------------------------------------------------

  n(v: number): string {
    return this.fmt.format(v);
  }

  c(v: number): string {
    return this.fmtCcy.format(v);
  }

  sortIcon(col: SortCol): string {
    if (this.sortCol !== col) return '';
    return this.sortAsc ? '▲' : '▼';
  }
}
