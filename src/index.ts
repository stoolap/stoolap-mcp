#!/usr/bin/env node

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

import { createRequire } from "node:module";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { parseArgs } from "./cli.js";
import { createServer } from "./server.js";
import { Session } from "./session.js";

const DEFAULT_QUERY_TIMEOUT_MS = 30000;

const require = createRequire(import.meta.url);
const { version } = require("../package.json") as { version: string };

const parsed = parseArgs(process.argv.slice(2), version);
if (parsed.kind === "exit") {
  if (parsed.out) console.log(parsed.out);
  if (parsed.err) console.error(parsed.err);
  process.exit(parsed.code);
}
const opts = parsed.options;

const session = await Session.open(opts.path, { readOnly: opts.readOnly, maxRows: opts.maxRows });

// A query_timeout in the DSN wins over the default, not over the flag.
const timeout =
  opts.queryTimeoutMs ?? (/[?&]query_timeout=/.test(opts.path) ? undefined : DEFAULT_QUERY_TIMEOUT_MS);
if (timeout !== undefined) await session.exec(`PRAGMA query_timeout = ${timeout}`);

const server = createServer(session, version);

let closing = false;
function shutdown(): void {
  if (closing) return;
  closing = true;
  try {
    session.closeSync();
  } catch {
    // exiting either way
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("SIGHUP", shutdown);
server.server.onclose = shutdown;

await server.connect(new StdioServerTransport());
console.error(
  `stoolap-mcp ${version}: connected (path=${opts.path}, readOnly=${opts.readOnly}, maxRows=${opts.maxRows}, queryTimeoutMs=${timeout ?? "dsn"})`,
);
