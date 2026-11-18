// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Database } from "@stoolap/node";

export type Params = unknown[] | Record<string, unknown>;

export interface RowSet {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
}

export interface SessionOptions {
  readOnly: boolean;
  maxRows: number;
}

// The driver returns VECTOR as Float32Array and TIMESTAMP as Date. JSON has
// no NaN or Infinity, and JSON.stringify would print them as null.
function toJsonValue(v: unknown): unknown {
  if (typeof v === "number" && !Number.isFinite(v)) return String(v);
  if (v instanceof Float32Array) return Array.from(v, (x) => (Number.isFinite(x) ? x : String(x)));
  if (v instanceof Date) return v.toISOString();
  if (typeof v === "bigint") return v.toString();
  return v;
}

// One database handle shared by every tool. Stoolap keeps transaction state
// per handle, so a BEGIN stays open across tool calls until COMMIT or
// ROLLBACK. Calls run one at a time: MCP clients may send requests
// concurrently, and statements of one transaction must not interleave.
export class Session {
  readonly readOnly: boolean;
  readonly maxRows: number;
  private inTx = false;
  private queue: Promise<unknown> = Promise.resolve();

  private constructor(
    private readonly db: Database,
    opts: SessionOptions,
  ) {
    this.readOnly = opts.readOnly;
    this.maxRows = opts.maxRows;
  }

  static async open(path: string, opts: SessionOptions): Promise<Session> {
    return new Session(await Database.open(path), opts);
  }

  get txActive(): boolean {
    return this.inTx;
  }

  run<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.queue.then(fn, fn);
    this.queue = next.catch(() => undefined);
    return next;
  }

  async rows(sql: string, params?: Params, maxRows = this.maxRows): Promise<RowSet> {
    const raw = await this.db.queryRaw(sql, params as never);
    const limit = maxRows > 0 ? maxRows : Infinity;
    const kept = raw.rows.length > limit ? raw.rows.slice(0, limit) : raw.rows;
    return {
      columns: raw.columns,
      rows: kept.map((r) => r.map(toJsonValue)),
      row_count: kept.length,
      truncated: kept.length < raw.rows.length,
    };
  }

  // Rows as objects, for small internal metadata queries.
  async objects(sql: string): Promise<Record<string, unknown>[]> {
    const raw = await this.db.queryRaw(sql);
    return raw.rows.map((r) =>
      Object.fromEntries(raw.columns.map((c, i) => [c, toJsonValue(r[i])])),
    );
  }

  async execute(sql: string, params?: Params): Promise<number> {
    return (await this.db.execute(sql, params as never)).changes;
  }

  async exec(sql: string): Promise<void> {
    await this.db.exec(sql);
  }

  // Outside a transaction the driver wraps the batch in its own transaction,
  // so it applies atomically. Inside one, the rows join the open transaction.
  batch(sql: string, paramsArray: unknown[][]): number {
    if (!this.inTx) return this.db.executeBatchSync(sql, paramsArray).changes;
    const stmt = this.db.prepare(sql);
    try {
      let total = 0;
      for (const p of paramsArray) total += stmt.executeSync(p).changes;
      return total;
    } finally {
      stmt.finalize();
    }
  }

  async begin(isolation: "read_committed" | "snapshot"): Promise<void> {
    await this.db.exec(
      isolation === "snapshot" ? "BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT" : "BEGIN TRANSACTION",
    );
    this.inTx = true;
  }

  async end(commit: boolean): Promise<void> {
    try {
      await this.db.exec(commit ? "COMMIT" : "ROLLBACK");
    } finally {
      this.inTx = false;
    }
  }

  closeSync(): void {
    if (this.inTx) {
      try {
        this.db.execSync("ROLLBACK");
      } catch {
        // the handle is closed next either way
      }
      this.inTx = false;
    }
    this.db.closeSync();
  }
}
