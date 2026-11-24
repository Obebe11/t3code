import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProviderInstanceId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { devinUsageReader } from "./devinUsage.ts";

it.effect("counts a Devin generation once at its generation time and retains unpriced tokens", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped();
    const dir = path.join(root, "devin", "cli");
    yield* fs.makeDirectory(dir, { recursive: true });
    const scan = () =>
      devinUsageReader.scan({
        instances: [
          {
            instanceId: ProviderInstanceId.make("acpRegistry"),
            configured: false,
            config: undefined,
            environment: { XDG_DATA_HOME: root },
          },
        ],
        settings: DEFAULT_SERVER_SETTINGS,
        windowStartMs: 0,
        retentionCutoffMs: 0,
        awaitRefresh: true,
      });
    assert.isNull((yield* scan())[0]?.files);
    yield* Effect.gen(function* () {
      const db = yield* SqlClient.SqlClient;
      yield* db`CREATE TABLE sessions (id TEXT, model TEXT)`;
      yield* db`CREATE TABLE message_nodes (session_id TEXT, node_id INTEGER, created_at INTEGER, chat_message TEXT)`;
      yield* db`INSERT INTO sessions VALUES ('a', 'swe-2-max')`;
      const message = JSON.stringify({
        role: "assistant",
        metadata: {
          created_at: "2026-05-01T23:59:58Z",
          request_id: "first",
          generation_model: "gemini-4-neon-high",
          metrics: {
            input_tokens: 10,
            cache_read_tokens: 90,
            context_input_tokens: 100,
            output_tokens: 5,
          },
        },
      });
      yield* db`INSERT INTO message_nodes VALUES ('a', 1, 1777680000, ${message}), ('a', 2, 1777680001, ${message})`;
      const noRequest = JSON.stringify({
        role: "assistant",
        metadata: { metrics: { output_tokens: 2 } },
      });
      yield* db`INSERT INTO message_nodes VALUES ('a', 3, 1777680002, ${noRequest}), ('a', 4, 1777680003, 'malformed')`;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: path.join(dir, "sessions.db") })));
    const records = (yield* scan())[0]?.files?.[0]?.records;
    assert.lengthOf(records ?? [], 2);
    assert.deepInclude(records?.[0], {
      provider: "devin",
      model: "gemini-4-neon-high",
      sessionId: "a",
      rateModel: "gemini-4-neon",
      timestampMs: Date.parse("2026-05-01T23:59:58Z"),
    });
    assert.deepEqual(records?.[0]?.totals, {
      uncachedInputTokens: 10,
      cachedInputTokens: 90,
      cacheCreationTokens: 0,
      outputTokens: 5,
      reasoningTokens: 0,
    });
    assert.deepInclude(records?.[1], { model: "swe-2-max", reportedCostUsd: null });
  }).pipe(Effect.provide(NodeServices.layer)),
);
