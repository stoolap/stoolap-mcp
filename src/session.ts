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
export type Isolation = "read_committed" | "snapshot";

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
  private isolation: Isolation | null = null;
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

  get txIsolation(): Isolation | null {
    return this.isolation;
  }

  run<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.queue.then(fn, fn);
    this.queue = next.catch(() => undefined);
    return next;
  }

  // The server's --max-rows caps every call; a call may only ask for fewer.
  // Infinity is for the server's own reports. The whole result is read
  // first: the driver's cursor opens on the main thread (stoolap-node#12).
  async rows(sql: string, params?: Params, maxRows?: number): Promise<RowSet> {
    const raw = await this.db.queryRaw(sql, params as never);
    const cap = this.maxRows > 0 ? this.maxRows : Infinity;
    const limit = maxRows === Infinity ? Infinity : Math.min(cap, maxRows ?? cap);
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

  // All or nothing. Outside a transaction the driver wraps the batch in its
  // own; inside one, a savepoint undoes the sets applied before a failure.
  batch(sql: string, paramsArray: unknown[][]): number {
    if (!this.inTx) return this.db.executeBatchSync(sql, paramsArray).changes;
    const sp = "stoolap_mcp_batch";
    this.db.execSync(`SAVEPOINT ${sp}`);
    const stmt = this.db.prepare(sql);
    try {
      let total = 0;
      for (const p of paramsArray) total += stmt.executeSync(p).changes;
      this.db.execSync(`RELEASE SAVEPOINT ${sp}`);
      return total;
    } catch (e) {
      this.db.execSync(`ROLLBACK TO SAVEPOINT ${sp}`);
      this.db.execSync(`RELEASE SAVEPOINT ${sp}`);
      throw e;
    } finally {
      stmt.finalize();
    }
  }

  // The level is always explicit: SET isolation_level changes the default.
  async begin(isolation: Isolation): Promise<void> {
    await this.db.exec(
      `BEGIN TRANSACTION ISOLATION LEVEL ${isolation === "snapshot" ? "SNAPSHOT" : "READ COMMITTED"}`,
    );
    this.inTx = true;
    this.isolation = isolation;
  }

  async end(commit: boolean): Promise<void> {
    try {
      await this.db.exec(commit ? "COMMIT" : "ROLLBACK");
    } finally {
      this.inTx = false;
      this.isolation = null;
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
