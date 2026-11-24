import {
  parseTimestampMs,
  tokenCount,
  totalTokens,
  type ProviderUsageReader,
  type ProviderUsageScan,
  type UsageRecord,
} from "@t3tools/provider-core/server/usage";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

import type { AcpRegistrySettings } from "../settings.ts";

interface DevinRow {
  session: string;
  node: number;
  written: number;
  generated: string | null;
  request: string | null;
  model: string | null;
  input: number | null;
  output: number | null;
  cache_read: number | null;
  cache_write: number | null;
}

const readRecords = Effect.fn("DevinUsage.readRecords")(function* (since: number) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<DevinRow>`
    WITH messages AS MATERIALIZED (
      SELECT n.session_id, n.node_id, n.created_at, s.model,
             jsonb_extract(n.chat_message, '$.metadata') AS meta
      FROM message_nodes n JOIN sessions s ON s.id = n.session_id
      WHERE n.created_at >= ${Math.floor(since / 1000)}
        AND json_valid(n.chat_message)
        AND json_extract(n.chat_message, '$.role') = 'assistant'
        AND json_type(n.chat_message, '$.metadata') = 'object'
    )
    SELECT session_id AS session, node_id AS node, created_at AS written,
           json_extract(meta, '$.created_at') AS generated,
           json_extract(meta, '$.request_id') AS request,
           coalesce(nullif(json_extract(meta, '$.generation_model'), ''), model) AS model,
           json_extract(meta, '$.metrics.input_tokens') AS input,
           json_extract(meta, '$.metrics.output_tokens') AS output,
           json_extract(meta, '$.metrics.cache_read_tokens') AS cache_read,
           json_extract(meta, '$.metrics.cache_creation_tokens') AS cache_write
    FROM messages WHERE json_type(meta, '$.metrics') = 'object'
    ORDER BY session_id, node_id
  `;
  const records = new Map<string, UsageRecord>();
  for (const row of rows) {
    // Devin copies the same generation into several nodes when a conversation branches.
    const generation = row.request ? `request:${row.request}` : `node:${row.node}`;
    const key = `${row.session.length}:${row.session}:${generation}`;
    if (records.has(key)) continue;
    const totals = {
      uncachedInputTokens: tokenCount(row.input),
      cachedInputTokens: tokenCount(row.cache_read),
      cacheCreationTokens: tokenCount(row.cache_write),
      outputTokens: tokenCount(row.output),
      reasoningTokens: 0,
    };
    if (totalTokens(totals) === 0) continue;
    const model =
      typeof row.model === "string" && row.model.trim() ? row.model.trim() : "devin-unknown";
    const rateModel = model
      .replace(/^(gpt-\d+)-(\d+)(?=-)/, "$1.$2")
      .replace(/^((?:gemini|gpt)-\d[\w.-]*)-(?:low|medium|high|xhigh)$/, "$1");
    records.set(key, {
      provider: "devin",
      timestampMs: parseTimestampMs(row.generated) ?? row.written * 1000,
      model,
      ...(rateModel !== model ? { rateModel } : {}),
      sessionId: row.session,
      totals,
      // Session credit and ACU totals are not dollar amounts.
      reportedCostUsd: null,
      speed: "standard",
      dedupeKey: `devin:${key}`,
    });
  }
  return [...records.values()];
});

// A window can reuse a wider scan until SQLite changes either the database or its WAL.
const history = new Map<
  string,
  { fingerprint: string; since: number; records: readonly UsageRecord[] }
>();
function stamp(info: FileSystem.File.Info) {
  return [Option.getOrNull(info.ino), info.size, Option.getOrNull(info.mtime)?.getTime()].join(":");
}

const scanDirectory = Effect.fn("DevinUsage.scanDirectory")(function* (dir: string, since: number) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const filename = path.join(dir, "sessions.db");
  return yield* Effect.gen(function* () {
    const database = yield* fs.stat(filename);
    const wal = yield* fs.stat(filename + "-wal").pipe(
      Effect.catchTags({
        PlatformError: (error) =>
          error.reason._tag === "NotFound" ? Effect.succeed(null) : Effect.fail(error),
      }),
    );
    const fingerprint = stamp(database) + "/" + (wal ? stamp(wal) : "-");
    const cached = history.get(filename);
    if (cached?.fingerprint === fingerprint && cached.since <= since) {
      return { dir, files: [{ path: filename, records: cached.records }] };
    }
    const records = yield* readRecords(since).pipe(
      NodeSqliteClient.readOnly,
      Effect.provide(NodeSqliteClient.layer({ filename, readonly: true, readerWorker: true })),
    );
    history.set(filename, { fingerprint, since, records });
    return { dir, files: [{ path: filename, records }] } satisfies ProviderUsageScan;
  }).pipe(
    Effect.catch((error) =>
      Effect.succeed<ProviderUsageScan>(
        error._tag === "PlatformError" && error.reason._tag === "NotFound"
          ? { dir, files: null }
          : { dir, status: "failed", files: [], message: "Devin usage history could not be read." },
      ),
    ),
  );
});

export const devinUsageReader = {
  kind: "scan",
  provider: "devin",
  scan: Effect.fn("DevinUsage.scan")(function* ({ instances, windowStartMs }) {
    const path = yield* Path.Path;
    const fs = yield* FileSystem.FileSystem;
    const home = yield* HostProcess.HomeDirectory;
    const dirs = new Set<string>();
    for (const { configured, config, environment } of instances) {
      if (
        configured &&
        config?.agentId !== "devin" &&
        !(
          config?.source === "local" &&
          /^devin(?:\.exe|\.cmd)?$/i.test(path.basename(config.commandPath))
        )
      )
        continue;
      const xdg = environment["XDG_DATA_HOME"];
      const root = xdg && path.isAbsolute(xdg) ? xdg : path.join(home, ".local", "share");
      const dir = path.join(root, "devin", "cli");
      dirs.add(yield* fs.realPath(dir).pipe(Effect.orElseSucceed(() => dir)));
    }
    return yield* Effect.forEach(dirs, (dir) => scanDirectory(dir, windowStartMs));
  }),
} satisfies ProviderUsageReader<AcpRegistrySettings, FileSystem.FileSystem | Path.Path>;
