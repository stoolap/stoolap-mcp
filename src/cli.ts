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

export const USAGE = `Usage: stoolap-mcp [--path <database>] [--read-only] [--max-rows <n>] [--query-timeout <ms>]

Options:
  --path <path>          Database path or DSN (default: :memory:). Engine options
                         go in the query string: ./mydata?sync_mode=full
  --read-only            Refuse every statement that writes data or schema
  --max-rows <n>         Rows returned per call at most (default 1000, 0 = no limit)
  --query-timeout <ms>   Stop a statement after this long (default 30000, or the
                         DSN's query_timeout when it has one; 0 = none)
  --version              Print the server version and exit
  -h, --help             Show this help and exit`;

export interface Options {
  path: string;
  readOnly: boolean;
  maxRows: number;
  // undefined: not given on the command line
  queryTimeoutMs?: number;
}

export type Parsed = { kind: "run"; options: Options } | { kind: "exit"; code: number; out?: string; err?: string };

function count(flag: string, v: string | undefined): number | string {
  if (v === undefined) return `${flag} requires a value`;
  if (!/^\d+$/.test(v)) return `${flag} takes a non-negative integer, got '${v}'`;
  return Number(v);
}

export function parseArgs(args: string[], version: string): Parsed {
  const o: Options = { path: ":memory:", readOnly: false, maxRows: 1000 };
  const fail = (msg: string): Parsed => ({ kind: "exit", code: 2, err: `error: ${msg}\n\n${USAGE}` });
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--path") {
      if (i + 1 >= args.length) return fail("--path requires a value");
      o.path = args[++i];
    } else if (a === "--read-only") {
      o.readOnly = true;
    } else if (a === "--max-rows" || a === "--query-timeout") {
      const n = count(a, args[++i]);
      if (typeof n === "string") return fail(n);
      if (a === "--max-rows") o.maxRows = n;
      else o.queryTimeoutMs = n;
    } else if (a === "--version") {
      return { kind: "exit", code: 0, out: version };
    } else if (a === "--help" || a === "-h") {
      return { kind: "exit", code: 0, err: USAGE };
    } else {
      return fail(`unknown argument: ${a}`);
    }
  }
  return { kind: "run", options: o };
}
