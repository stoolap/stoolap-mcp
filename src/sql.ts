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

// Statement checks for tool routing and read-only mode. The engine has no
// read-only open mode, so these checks are the server's only guard. They
// read SQL the way the engine lexer does (src/parser/lexer.rs).

export type StatementKind =
  | "read" // SELECT, WITH ... SELECT, SHOW, DESCRIBE, EXPLAIN without ANALYZE
  | "write" // INSERT, UPDATE, DELETE, WITH ... DML, COPY
  | "ddl" // CREATE, ALTER, DROP, TRUNCATE
  | "admin" // ANALYZE, VACUUM, SET
  | "pragma"
  | "tx" // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
  | "other";

// A word token outside literals, quoted identifiers and comments, with the
// parenthesis depth it sits at.
interface Word {
  text: string;
  depth: number;
}

export interface Statement {
  kind: StatementKind;
  explain: boolean;
  // EXPLAIN ANALYZE: the statement runs, and kind is that statement's kind.
  analyzed: boolean;
  // Words of the statement EXPLAIN [ANALYZE] would run, upper-cased.
  words: Word[];
}

const IDENT = /[\p{L}\p{N}_$]/u;
// Rust char::is_whitespace, which the engine lexer skips, is Unicode
// White_Space; JavaScript's \s differs (it has U+FEFF, lacks U+0085).
const SPACE = /\p{White_Space}/u;

// Returns the words of the SQL, or an error when a second statement or a
// character the check cannot vouch for is present.
function scan(sql: string): { words: Word[]; error?: string } {
  const words: Word[] = [];
  let depth = 0;
  let i = 0;
  let ended = false; // a ';' was seen; only spaces and comments may follow
  while (i < sql.length) {
    const c = sql[i];
    if (c === "\0") return { words, error: "SQL must not contain a NUL character" };
    if (c === "﻿") return { words, error: "SQL must not contain a byte order mark" };
    if (SPACE.test(c)) {
      i++;
      continue;
    }
    if (c === "#" || (c === "-" && sql[i + 1] === "-")) {
      while (i < sql.length && sql[i] !== "\n") i++;
      continue;
    }
    if (c === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 2;
      continue;
    }
    if (ended) {
      return { words, error: "multiple SQL statements are not allowed. Send one statement per call." };
    }
    if (c === ";") {
      ended = true;
      i++;
      continue;
    }
    if (c === "'" || c === '"' || c === "`") {
      i++;
      while (i < sql.length) {
        if (c === "'" && sql[i] === "\\") i += 2;
        else if (sql[i] === c && sql[i + 1] === c) i += 2;
        else if (sql[i] === c) {
          i++;
          break;
        } else i++;
      }
      // A quoted name stands where a word would.
      words.push({ text: "", depth });
      continue;
    }
    if (c === "(") depth++;
    else if (c === ")") depth = Math.max(0, depth - 1);
    if (IDENT.test(c)) {
      let j = i;
      while (j < sql.length && IDENT.test(sql[j])) j++;
      words.push({ text: sql.slice(i, j).toUpperCase(), depth });
      i = j;
      continue;
    }
    words.push({ text: c, depth });
    i++;
  }
  if (words.length === 0) return { words, error: "empty SQL statement" };
  return { words };
}

const DDL = new Set(["CREATE", "ALTER", "DROP", "TRUNCATE"]);
const ADMIN = new Set(["ANALYZE", "VACUUM", "SET"]);
const TX = new Set(["BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"]);
const DML = new Set(["INSERT", "UPDATE", "DELETE", "COPY", "MERGE"]);
const READ = new Set(["SELECT", "SHOW", "DESCRIBE", "DESC"]);

function kindOf(words: Word[]): StatementKind {
  const lead = words.filter((w) => w.text !== "(");
  const first = lead[0]?.text ?? "";
  if (first === "WITH") {
    // The main statement is the first verb at the WITH's own depth after it.
    const d = lead[0].depth;
    const verb = lead.find((w, k) => k > 0 && w.depth === d && (READ.has(w.text) || DML.has(w.text) || w.text === "VALUES"));
    if (!verb) return "other";
    return DML.has(verb.text) ? "write" : READ.has(verb.text) ? "read" : "other";
  }
  if (READ.has(first)) return "read";
  if (DML.has(first)) return "write";
  if (DDL.has(first)) return "ddl";
  if (ADMIN.has(first)) return "admin";
  if (first === "PRAGMA") return "pragma";
  if (TX.has(first)) return "tx";
  return "other";
}

export function parse(sql: string): Statement | { error: string } {
  const { words, error } = scan(sql);
  if (error) return { error };
  if (words[0].text === "EXPLAIN") {
    const analyzed = words[1]?.text === "ANALYZE";
    const inner = words.slice(analyzed ? 2 : 1);
    if (inner.length === 0) return { error: "EXPLAIN needs a statement" };
    const kind = kindOf(inner);
    // Plain EXPLAIN only plans. EXPLAIN of transaction control is refused
    // so it can never be mistaken for the statement itself.
    return { kind: analyzed || kind === "tx" ? kind : "read", explain: true, analyzed, words: inner };
  }
  return { kind: kindOf(words), explain: false, analyzed: false, words };
}

// CREATE TABLE ... AS SELECT persists after ROLLBACK; a plain CREATE TABLE
// is undone with the transaction.
export function isPlainCreateTable(st: Statement): boolean {
  const w = st.words;
  if (w[0]?.text !== "CREATE" || w[1]?.text !== "TABLE") return false;
  return !w.some((x) => x.depth === 0 && x.text === "AS");
}

export function hasReturning(st: Statement): boolean {
  return st.words.some((w) => w.text === "RETURNING");
}

export function quoteId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export function isBareId(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}
