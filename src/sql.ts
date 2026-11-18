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

// Statement classification for tool routing and read-only mode. The engine
// has no read-only open mode, so these checks are the server's only guard.

export type StatementKind =
  | "read" // SELECT, WITH ... SELECT, SHOW, DESCRIBE, EXPLAIN of a read
  | "write" // INSERT, UPDATE, DELETE, WITH ... DML, COPY
  | "ddl" // CREATE, ALTER, DROP, TRUNCATE
  | "admin" // ANALYZE, VACUUM, SET, PRAGMA
  | "tx" // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
  | "other";

// Blanks out string literals, quoted identifiers and comments so keyword and
// semicolon checks never look inside them. Mirrors the engine lexer: '' and
// backslash escapes in literals, doubled quotes in identifiers, # and -- line
// comments, unnested block comments.
export function sanitizeSql(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    if (c === "'" || c === '"' || c === "`") {
      out += " ";
      i++;
      while (i < sql.length) {
        if (c === "'" && sql[i] === "\\") {
          i += 2;
        } else if (sql[i] === c && sql[i + 1] === c) {
          i += 2;
        } else if (sql[i] === c) {
          i++;
          break;
        } else {
          i++;
        }
      }
      continue;
    }
    if (c === "/" && sql[i + 1] === "*") {
      out += " ";
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 2;
      continue;
    }
    if (c === "#" || (c === "-" && sql[i + 1] === "-")) {
      out += " ";
      while (i < sql.length && sql[i] !== "\n") i++;
      continue;
    }
    out += c;
    i++;
  }
  return out;
}

// The engine runs every statement of a multi-statement string and reports
// only the last one, so each call carries exactly one statement.
export function singleStatementError(sql: string): string | null {
  // The engine lexer reads a NUL byte as the end of input.
  if (sql.includes("\0")) return "SQL must not contain a NUL character";
  const body = sanitizeSql(sql).replace(/[\s;]+$/, "");
  if (body.trim() === "") return "empty SQL statement";
  if (body.includes(";")) {
    return "multiple SQL statements are not allowed. Send one statement per call.";
  }
  return null;
}

function words(sql: string): string[] {
  return sanitizeSql(sql)
    .replace(/[(),]/g, " ")
    .trim()
    .split(/\s+/)
    .filter((w) => w !== "")
    .map((w) => w.toUpperCase());
}

const DDL = new Set(["CREATE", "ALTER", "DROP", "TRUNCATE"]);
const ADMIN = new Set(["ANALYZE", "VACUUM", "SET", "PRAGMA"]);
const TX = new Set(["BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"]);
const DML = new Set(["INSERT", "UPDATE", "DELETE", "COPY"]);
const READ = new Set(["SELECT", "SHOW", "DESCRIBE", "DESC"]);

// WITH ... INSERT/UPDATE/DELETE: the main statement follows the last CTE body,
// so a DML keyword right after a closing parenthesis marks a write.
function withIsWrite(sql: string): boolean {
  return /\)\s*(INSERT|UPDATE|DELETE)\b/i.test(sanitizeSql(sql));
}

export function classify(sql: string): StatementKind {
  const w = words(sql);
  let first = w[0] ?? "";
  if (first === "EXPLAIN") {
    // EXPLAIN ANALYZE executes the statement, so it takes that statement's kind.
    const analyze = w[1] === "ANALYZE";
    const inner = sanitizeSql(sql)
      .trimStart()
      .replace(/^\(*\s*EXPLAIN\s+(ANALYZE\s+)?/i, "");
    const kind = classify(inner);
    if (analyze) return kind;
    return kind === "tx" ? "tx" : "read";
  }
  if (first === "WITH") return withIsWrite(sql) ? "write" : "read";
  if (READ.has(first)) return "read";
  if (DML.has(first)) return "write";
  if (DDL.has(first)) return "ddl";
  if (ADMIN.has(first)) return "admin";
  if (TX.has(first)) return "tx";
  return "other";
}

export function firstKeyword(sql: string): string {
  return words(sql)[0] ?? "";
}

export function hasReturning(sql: string): boolean {
  return /\bRETURNING\b/i.test(sanitizeSql(sql));
}

// Strips a leading EXPLAIN [ANALYZE] so the explain tool accepts either form.
export function stripExplain(sql: string): string {
  return sql.trimStart().replace(/^EXPLAIN\s+(ANALYZE\s+)?/i, "");
}

export function quoteId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export function isBareId(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}
