import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import {
  LobbylensClient,
  callTool,
  createClient,
  DEFAULT_ENDPOINT_URL,
  isKnownTool,
  parseRpcBody,
  PROTOCOL_VERSION,
  TOOL_DEFS,
  TOOL_NAMES,
  VERSION,
} from "../src/forward.js";
import { handleCallTool } from "../src/mcp.js";
import type {
  FetchFn,
  FetchResponseLike,
  ToolDef,
  ToolResult,
} from "../src/types.js";

/**
 * Every test runs against a stubbed JSON-RPC upstream that
 * implements the streamable-HTTP session flow
 * (initialize -> notifications/initialized -> tools/call):
 * no live network, and the real Lobbyvo endpoint is
 * never contacted.
 */

const FIXTURE_URL = new URL(
  "../../../harness/fixtures/lobbyvo-tools-original.json",
  import.meta.url,
);
const FIXTURE_AVAILABLE = existsSync(FIXTURE_URL);
const fixture = (
  FIXTURE_AVAILABLE
    ? JSON.parse(readFileSync(FIXTURE_URL, "utf-8"))
    : { tools: [] }
) as {
  tools: ToolDef[];
};
/**
 * Publish-survival guard (Wave 25 preflight, proven Wave 24 pattern):
 * the parity fixture lives in the build harness, outside the published
 * repo tree, so fixture-dependent tests skip cleanly when it is absent;
 * with the fixture present they run in full, with no skips.
 */
function skipWithoutFixture(t: {
  skip: (message: string) => void;
}): boolean {
  if (FIXTURE_AVAILABLE) return false;
  t.skip("parity fixture lives in the build harness, not the repo tree");
  return true;
}

/** Recursively strip `description` fields (tool- and parameter-level).
 * Ruling 1 (extended fleet-wide 2026-11-15): the served tools carry
 * parameter descriptions added over the original capture; everything
 * else must match the fixture exactly. */
function stripDescriptions(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripDescriptions);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (k !== "description") out[k] = stripDescriptions(v);
    }
    return out;
  }
  return value;
}


const COLUMNS = "dataset_columns";
const SEARCH = "dataset_search";
const ROW = "dataset_row";
const COMPARE = "dataset_compare";
const STATS = "dataset_stats";
const TOP = "dataset_top";
const PROVENANCE = "dataset_provenance";
const ALL_TOOLS = [COLUMNS, SEARCH, ROW, COMPARE, STATS, TOP, PROVENANCE];

const SAMPLE_RESULT = {
  content: [{ type: "text", text: "{\"rows\":[{\"name\":\"Acme Lobbying LLC\"}]}" }],
  structuredContent: { rows: [{ name: "Acme Lobbying LLC" }] },
};

const SAMPLE_ARGS = { query: "smith" };

interface Captured {
  url: string;
  headers: Record<string, string>;
  body: Record<string, unknown>;
}

interface FakeOptions {
  callResult?: Record<string, unknown>;
  callBodyText?: string;
  callError?: Record<string, unknown>;
  callStatus?: number;
  initError?: Record<string, unknown>;
  throwOn?: "initialize" | "tools/call";
  sessionId?: string | null;
  hangOnCall?: boolean;
  sse?: boolean;
}

function makeResponse(
  status: number,
  bodyText: string,
  headers: Record<string, string> = {},
): FetchResponseLike {
  const lowered = new Map(
    Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]),
  );
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name: string) => lowered.get(name.toLowerCase()) ?? null },
    text: async () => bodyText,
  };
}

function jsonResponse(
  status: number,
  payload: unknown,
  headers: Record<string, string> = {},
) {
  return makeResponse(status, JSON.stringify(payload), headers);
}

function makeUpstream(options: FakeOptions = {}): {
  fetchFn: FetchFn;
  calls: Captured[];
} {
  const calls: Captured[] = [];
  const fetchFn: FetchFn = async (url, init) => {
    const body = JSON.parse(init?.body ?? "{}") as Record<string, unknown>;
    calls.push({ url, headers: init?.headers ?? {}, body });
    const method = body.method as string;
    if (options.throwOn === method) throw new Error("socket hang up");
    if (method === "initialize") {
      if (options.initError !== undefined) {
        return jsonResponse(200, {
          jsonrpc: "2.0",
          id: body.id,
          error: options.initError,
        });
      }
      const headers: Record<string, string> = {};
      const sessionId =
        options.sessionId === undefined ? "sess-abc" : options.sessionId;
      if (sessionId !== null) headers["mcp-session-id"] = sessionId;
      return jsonResponse(
        200,
        {
          jsonrpc: "2.0",
          id: body.id,
          result: {
            protocolVersion: PROTOCOL_VERSION,
            capabilities: {},
            serverInfo: { name: "lobbyvo", version: "1.0.1" },
          },
        },
        headers,
      );
    }
    if (method === "notifications/initialized") {
      return makeResponse(202, "");
    }
    if (method === "tools/call") {
      if (options.hangOnCall) {
        return new Promise<FetchResponseLike>((_resolve, reject) => {
          const signal = init?.signal;
          if (signal) {
            if (signal.aborted) {
              reject(new Error("aborted"));
              return;
            }
            signal.addEventListener("abort", () => reject(new Error("aborted")));
          }
        });
      }
      if (options.callBodyText !== undefined) {
        return makeResponse(options.callStatus ?? 200, options.callBodyText);
      }
      if (options.callError !== undefined) {
        return jsonResponse(200, {
          jsonrpc: "2.0",
          id: body.id,
          error: options.callError,
        });
      }
      const result = options.callResult ?? SAMPLE_RESULT;
      const payload = { jsonrpc: "2.0", id: body.id, result };
      if (options.sse) {
        return makeResponse(
          200,
          `event: message\ndata: ${JSON.stringify(payload)}\n\n`,
        );
      }
      return jsonResponse(options.callStatus ?? 200, payload);
    }
    return jsonResponse(200, {});
  };
  return { fetchFn, calls };
}

function stubClient(options: FakeOptions = {}, extra: Record<string, unknown> = {}) {
  const upstream = makeUpstream(options);
  const c = new LobbylensClient({
    fetchFn: upstream.fetchFn,
    env: {},
    ...extra,
  });
  return { client: c, calls: upstream.calls };
}

function textOf(result: ToolResult): string {
  const content = result.content as Array<{ type: string; text: string }>;
  return content[0].text;
}

function methods(calls: Captured[]): string[] {
  return calls.map((c) => c.body.method as string);
}

function schemaOf(tool: ToolDef): Record<string, any> {
  return tool.inputSchema as Record<string, any>;
}

function defOf(name: string): ToolDef {
  const def = TOOL_DEFS.find((t) => t.name === name);
  assert.ok(def !== undefined, `missing tool def ${name}`);
  return def;
}

/* ------------------------------------------------------------------ */

describe("registration integrity (captured surface)", () => {
  it("serves exactly the fixture's 7 tools, deep-equal, in order", (t) => {
    if (skipWithoutFixture(t)) return;
    assert.equal(fixture.tools.length, 7);
    assert.deepEqual(stripDescriptions(TOOL_DEFS), stripDescriptions(fixture.tools));
  });

  it("serves the 7 tool names in fixture order", () => {
    assert.deepEqual(TOOL_NAMES, ALL_TOOLS);
  });

  it("carries exactly the fixture's keys on every tool", () => {
    for (const tool of TOOL_DEFS) {
      assert.deepEqual(Object.keys(tool).sort(), [
        "description",
        "inputSchema",
        "name",
        "title",
      ]);
    }
  });

  it("carries the fixture's titles verbatim", () => {
    assert.deepEqual(
      TOOL_DEFS.map((t) => t.title),
      [
        "Dataset columns and shape",
        "Search the dataset",
        "Look a row up by an exact key",
        "Compare rows side by side",
        "Summary statistics for a numeric column",
        "Rank rows by a numeric column",
        "Where this data comes from, and how to cite it",
      ],
    );
  });

  it("carries the fixture's descriptions verbatim", (t) => {
    if (skipWithoutFixture(t)) return;
    for (let i = 0; i < TOOL_DEFS.length; i++) {
      assert.equal(TOOL_DEFS[i].description, fixture.tools[i].description);
    }
    assert.match(defOf(SEARCH).description, /Rows of the Lobbyvo dataset/);
    assert.match(defOf(COLUMNS).description, /provenance banner/);
  });

  it("carries the fixture's required fields per tool", () => {
    assert.equal(schemaOf(defOf(COLUMNS)).required, undefined);
    assert.deepEqual(schemaOf(defOf(SEARCH)).required, ["query"]);
    assert.deepEqual(schemaOf(defOf(ROW)).required, ["column", "value"]);
    assert.deepEqual(schemaOf(defOf(COMPARE)).required, ["column", "values"]);
    assert.deepEqual(schemaOf(defOf(STATS)).required, ["column"]);
    assert.deepEqual(schemaOf(defOf(TOP)).required, ["column"]);
    assert.equal(schemaOf(defOf(PROVENANCE)).required, undefined);
  });

  it("carries the $schema marker and object type on every inputSchema", () => {
    for (const tool of TOOL_DEFS) {
      assert.equal(schemaOf(tool).type, "object");
      assert.equal(
        schemaOf(tool)["$schema"],
        "http://json-schema.org/draft-07/schema#",
      );
    }
  });

  it("carries every fixture property, in order, for every tool", (t) => {
    if (skipWithoutFixture(t)) return;
    for (let i = 0; i < TOOL_DEFS.length; i++) {
      const served = Object.keys(schemaOf(TOOL_DEFS[i]).properties);
      const expected = Object.keys(schemaOf(fixture.tools[i]).properties);
      assert.deepEqual(served, expected);
    }
  });

  it("dataset_columns and dataset_provenance take no properties", () => {
    assert.deepEqual(Object.keys(schemaOf(defOf(COLUMNS)).properties), []);
    assert.deepEqual(Object.keys(schemaOf(defOf(PROVENANCE)).properties), []);
  });

  it("dataset_search carries query then limit", () => {
    assert.deepEqual(Object.keys(schemaOf(defOf(SEARCH)).properties), [
      "query",
      "limit",
    ]);
    const query = schemaOf(defOf(SEARCH)).properties.query;
    assert.equal(query.type, "string");
    assert.equal(query.minLength, 1);
    const limit = schemaOf(defOf(SEARCH)).properties.limit;
    assert.equal(limit.type, "integer");
    assert.equal(limit.minimum, 1);
    assert.equal(limit.maximum, 50);
  });

  it("dataset_compare values is a 2..10 array of non-empty strings", () => {
    const values = schemaOf(defOf(COMPARE)).properties.values;
    assert.equal(values.type, "array");
    assert.equal(values.minItems, 2);
    assert.equal(values.maxItems, 10);
    assert.equal(values.items.type, "string");
    assert.equal(values.items.minLength, 1);
  });

  it("dataset_top carries column, limit and ascending", () => {
    assert.deepEqual(Object.keys(schemaOf(defOf(TOP)).properties), [
      "column",
      "limit",
      "ascending",
    ]);
    assert.equal(schemaOf(defOf(TOP)).properties.ascending.type, "boolean");
  });

  it("additionalProperties is false on the five argument-taking tools and absent on the other two", () => {
    for (const name of [SEARCH, ROW, COMPARE, STATS, TOP]) {
      assert.equal(schemaOf(defOf(name)).additionalProperties, false);
    }
    assert.equal("additionalProperties" in schemaOf(defOf(COLUMNS)), false);
    assert.equal("additionalProperties" in schemaOf(defOf(PROVENANCE)), false);
  });

  it("isKnownTool recognises the captured tools and rejects others", () => {
    for (const name of ALL_TOOLS) assert.equal(isKnownTool(name), true);
    assert.equal(isKnownTool("dataset"), false);
    assert.equal(isKnownTool("dataset_search_all"), false);
    assert.equal(isKnownTool(""), false);
  });
});

/* ------------------------------------------------------------------ */

describe("session establishment and reuse", () => {
  it("runs initialize, notifications/initialized, then tools/call in order", async () => {
    const { client: c, calls } = stubClient();
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, undefined);
    assert.deepEqual(methods(calls), [
      "initialize",
      "notifications/initialized",
      "tools/call",
    ]);
    assert.ok(calls.every((call) => call.url === DEFAULT_ENDPOINT_URL));
    assert.equal(DEFAULT_ENDPOINT_URL, "https://lobbyvo.com/mcp");
    for (const call of calls) {
      assert.equal(call.headers["Content-Type"], "application/json");
      assert.equal(call.headers["Accept"], "application/json, text/event-stream");
    }
    assert.equal(calls[0].headers["Mcp-Session-Id"], undefined);
    assert.equal(calls[1].headers["Mcp-Session-Id"], "sess-abc");
    assert.equal(calls[2].headers["Mcp-Session-Id"], "sess-abc");
  });

  it("sends clientInfo name lobbylens and the pinned protocol version on initialize", async () => {
    const { client: c, calls } = stubClient();
    await c.callTool(SEARCH, SAMPLE_ARGS);
    const initParams = calls[0].body.params as Record<string, unknown>;
    assert.equal(initParams.protocolVersion, PROTOCOL_VERSION);
    assert.equal(PROTOCOL_VERSION, "2025-03-26");
    assert.deepEqual(initParams.capabilities, {});
    assert.deepEqual(initParams.clientInfo, { name: "lobbylens", version: VERSION });
  });

  it("establishes the session lazily once and reuses it on later calls", async () => {
    const { client: c, calls } = stubClient();
    await c.callTool(SEARCH, SAMPLE_ARGS);
    await c.callTool(STATS, { column: "employees" });
    assert.deepEqual(methods(calls), [
      "initialize",
      "notifications/initialized",
      "tools/call",
      "tools/call",
    ]);
    assert.equal(calls[3].headers["Mcp-Session-Id"], "sess-abc");
    assert.notEqual(calls[2].body.id, calls[3].body.id);
  });

  it("sends no session header when the upstream issues none", async () => {
    const { client: c, calls } = stubClient({ sessionId: null });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, undefined);
    assert.ok(calls.every((call) => call.headers["Mcp-Session-Id"] === undefined));
  });

  it("honours the LOBBYVO_MCP_URL env override", async () => {
    const upstream = makeUpstream();
    const c = new LobbylensClient({
      fetchFn: upstream.fetchFn,
      env: { LOBBYVO_MCP_URL: "https://staging.example.com/mcp" },
    });
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.ok(
      upstream.calls.every((call) => call.url === "https://staging.example.com/mcp"),
    );
  });

  it("honours an explicit url override over the default endpoint", async () => {
    const upstream = makeUpstream();
    const c = createClient({
      fetchFn: upstream.fetchFn,
      env: {},
      url: "https://alt.example.com/mcp",
    });
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.ok(upstream.calls.every((call) => call.url === "https://alt.example.com/mcp"));
  });

  it("prefers an explicit url override over the env override", async () => {
    const upstream = makeUpstream();
    const c = createClient({
      fetchFn: upstream.fetchFn,
      env: { LOBBYVO_MCP_URL: "https://env.example.com/mcp" },
      url: "https://explicit.example.com/mcp",
    });
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.ok(
      upstream.calls.every((call) => call.url === "https://explicit.example.com/mcp"),
    );
  });

  it("falls back to the pinned endpoint when the env override is blank", async () => {
    const upstream = makeUpstream();
    const c = new LobbylensClient({
      fetchFn: upstream.fetchFn,
      env: { LOBBYVO_MCP_URL: "   " },
    });
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.ok(upstream.calls.every((call) => call.url === DEFAULT_ENDPOINT_URL));
  });
});

/* ------------------------------------------------------------------ */

describe("no auth header is ever sent (record declares NO headers)", () => {
  it("sends no Authorization header when the environment is empty", async () => {
    const { client: c, calls } = stubClient();
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(calls.length, 3);
    assert.ok(calls.every((call) => call.headers["Authorization"] === undefined));
  });

  it("sends no Authorization header even when an LOBBYVO_API_KEY-style env var is present", async () => {
    const upstream = makeUpstream();
    const c = new LobbylensClient({
      fetchFn: upstream.fetchFn,
      env: { LOBBYVO_API_KEY: "some-key-42" },
    });
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.ok(
      upstream.calls.every((call) => call.headers["Authorization"] === undefined),
    );
  });

  it("sends no Authorization header even when AUTHORIZATION / API_KEY env vars are present", async () => {
    const upstream = makeUpstream();
    const c = new LobbylensClient({
      fetchFn: upstream.fetchFn,
      env: { AUTHORIZATION: "Bearer x", API_KEY: "y", LOBBYVO_TOKEN: "z" },
    });
    await c.callTool(ROW, { column: "state", value: "CA" });
    assert.ok(
      upstream.calls.every((call) => call.headers["Authorization"] === undefined),
    );
  });

  it("never sends an X-API-Key header either", async () => {
    const upstream = makeUpstream();
    const c = new LobbylensClient({
      fetchFn: upstream.fetchFn,
      env: { LOBBYVO_API_KEY: "some-key-42" },
    });
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.ok(upstream.calls.every((call) => call.headers["X-API-Key"] === undefined));
  });

  it("sends only Content-Type, Accept and (when issued) Mcp-Session-Id headers", async () => {
    const { client: c, calls } = stubClient();
    await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.deepEqual(Object.keys(calls[0].headers).sort(), [
      "Accept",
      "Content-Type",
    ]);
    assert.deepEqual(Object.keys(calls[2].headers).sort(), [
      "Accept",
      "Content-Type",
      "Mcp-Session-Id",
    ]);
  });
});

/* ------------------------------------------------------------------ */

describe("tools/call forwarding", () => {
  it("forwards a dataset_search call verbatim", async () => {
    const { client: c, calls } = stubClient();
    await c.callTool(SEARCH, SAMPLE_ARGS);
    const params = calls[2].body.params as Record<string, unknown>;
    assert.equal(params.name, SEARCH);
    assert.deepEqual(params.arguments, SAMPLE_ARGS);
  });

  it("forwards a dataset_compare call with every option verbatim", async () => {
    const args = { column: "state", values: ["CA", "TX", "NY"] };
    const { client: c, calls } = stubClient();
    await c.callTool(COMPARE, args);
    const params = calls[2].body.params as Record<string, unknown>;
    assert.equal(params.name, COMPARE);
    assert.deepEqual(params.arguments, args);
  });

  it("forwards dataset_top under its own name", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    await c.callTool(TOP, { column: "employees", limit: 5, ascending: true });
    const params = upstream.calls[2].body.params as Record<string, unknown>;
    assert.equal(params.name, TOP);
    assert.deepEqual(params.arguments, {
      column: "employees",
      limit: 5,
      ascending: true,
    });
  });

  it("forwards dataset_columns with empty arguments", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    await c.callTool(COLUMNS, {});
    const params = upstream.calls[2].body.params as Record<string, unknown>;
    assert.equal(params.name, COLUMNS);
    assert.deepEqual(params.arguments, {});
  });

  it("passes the upstream result through verbatim, including extra fields", async () => {
    const { client: c } = stubClient();
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.deepEqual(out, SAMPLE_RESULT);
  });

  it("passes an upstream isError result through untouched", async () => {
    const upstreamResult = {
      content: [{ type: "text", text: "no such column upstream" }],
      isError: true,
    };
    const { client: c } = stubClient({ callResult: upstreamResult });
    const out = await c.callTool(STATS, { column: "nope" });
    assert.deepEqual(out, upstreamResult);
  });

  it("reads an SSE-framed tools/call response", async () => {
    const { client: c } = stubClient({ sse: true });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, undefined);
    assert.deepEqual(out, SAMPLE_RESULT);
  });

  it("the one-shot callTool helper forwards under the right tool name", async () => {
    const upstream = makeUpstream();
    const out = await callTool(
      STATS,
      { column: "employees" },
      { fetchFn: upstream.fetchFn, env: {} },
    );
    assert.equal(out.isError, undefined);
    const params = upstream.calls[2].body.params as Record<string, unknown>;
    assert.equal(params.name, STATS);
    assert.deepEqual(params.arguments, { column: "employees" });
  });
});

/* ------------------------------------------------------------------ */

describe("upstream failures surface as lobbylens error text", () => {
  it("maps a JSON-RPC error to `lobbylens error: <message>`", async () => {
    const { client: c } = stubClient({
      callError: { code: -32000, message: "query failed upstream" },
    });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.equal(textOf(out), "lobbylens error: query failed upstream");
  });

  it("maps an initialize JSON-RPC error to lobbylens error text", async () => {
    const { client: c } = stubClient({
      initError: { code: -32600, message: "bad initialize" },
    });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.equal(textOf(out), "lobbylens error: bad initialize");
  });

  it("maps an unparseable upstream response to a malformed-response error", async () => {
    const { client: c } = stubClient({ callBodyText: "<html>oops</html>" });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.equal(textOf(out), "lobbylens error: malformed response");
  });

  it("maps a response with no result object to a malformed-response error", async () => {
    const { client: c } = stubClient({
      callBodyText: '{"jsonrpc":"2.0","id":3,"result":null}',
    });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.equal(textOf(out), "lobbylens error: malformed response");
  });

  it("maps a non-2xx upstream status to lobbylens error text", async () => {
    const { client: c } = stubClient({ callBodyText: "internal error", callStatus: 500 });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.equal(textOf(out), "lobbylens error: HTTP 500");
  });

  it("maps a throwing fetch to lobbylens error text", async () => {
    const { client: c } = stubClient({ throwOn: "tools/call" });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.match(textOf(out), /^lobbylens error: /);
  });

  it("maps a throwing initialize fetch to lobbylens error text", async () => {
    const { client: c } = stubClient({ throwOn: "initialize" });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.equal(textOf(out), "lobbylens error: socket hang up");
  });

  it("maps a timed-out call to lobbylens error text", async () => {
    const { client: c } = stubClient({ hangOnCall: true }, { timeoutMs: 20 });
    const out = await c.callTool(SEARCH, SAMPLE_ARGS);
    assert.equal(out.isError, true);
    assert.match(textOf(out), /^lobbylens error: /);
  });
});

/* ------------------------------------------------------------------ */

describe("parseRpcBody", () => {
  it("parses plain JSON and picks the message with the matching id", () => {
    const body = '{"jsonrpc":"2.0","id":7,"result":{"ok":true}}';
    assert.deepEqual(parseRpcBody(body, 7), {
      jsonrpc: "2.0",
      id: 7,
      result: { ok: true },
    });
  });

  it("parses SSE data lines", () => {
    const body =
      'event: message\ndata: {"jsonrpc":"2.0","id":2,"result":{"ok":true}}\n\n';
    assert.deepEqual(parseRpcBody(body, 2), {
      jsonrpc: "2.0",
      id: 2,
      result: { ok: true },
    });
  });

  it("picks the matching id among several SSE messages", () => {
    const body =
      'data: {"jsonrpc":"2.0","id":1,"result":{"n":1}}\n\n' +
      'data: {"jsonrpc":"2.0","id":2,"result":{"n":2}}\n\n';
    assert.deepEqual(parseRpcBody(body, 2), {
      jsonrpc: "2.0",
      id: 2,
      result: { n: 2 },
    });
  });

  it("ignores the SSE [DONE] sentinel", () => {
    const body =
      'data: {"jsonrpc":"2.0","id":4,"result":{"ok":true}}\ndata: [DONE]\n';
    assert.deepEqual(parseRpcBody(body, 4), {
      jsonrpc: "2.0",
      id: 4,
      result: { ok: true },
    });
  });

  it("returns null for empty and unparseable bodies", () => {
    assert.equal(parseRpcBody(""), null);
    assert.equal(parseRpcBody("not json at all"), null);
    assert.equal(parseRpcBody("[1,2,3]"), null);
  });
});

/* ------------------------------------------------------------------ */

describe("MCP layer validation", () => {
  it("rejects an unknown tool with MethodNotFound before any network hop", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    await assert.rejects(
      () => handleCallTool(c, "not_a_tool", {}),
      (err: unknown) =>
        err instanceof McpError && err.code === ErrorCode.MethodNotFound,
    );
    assert.equal(upstream.calls.length, 0);
  });

  it("rejects array arguments with InvalidParams before any network hop", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    await assert.rejects(
      () => handleCallTool(c, SEARCH, ["nope"]),
      (err: unknown) =>
        err instanceof McpError && err.code === ErrorCode.InvalidParams,
    );
    assert.equal(upstream.calls.length, 0);
  });

  it("rejects string arguments with InvalidParams before any network hop", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    await assert.rejects(
      () => handleCallTool(c, SEARCH, "query=smith"),
      (err: unknown) =>
        err instanceof McpError && err.code === ErrorCode.InvalidParams,
    );
    assert.equal(upstream.calls.length, 0);
  });

  it("forwards a valid dataset_search call through the MCP layer unchanged", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    const out = await handleCallTool(c, SEARCH, SAMPLE_ARGS);
    assert.deepEqual(out, SAMPLE_RESULT);
    const params = upstream.calls[2].body.params as Record<string, unknown>;
    assert.deepEqual(params.arguments, SAMPLE_ARGS);
  });

  it("forwards a valid dataset_compare call through the MCP layer unchanged", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    const out = await handleCallTool(c, COMPARE, {
      column: "state",
      values: ["CA", "TX"],
    });
    assert.deepEqual(out, SAMPLE_RESULT);
    const params = upstream.calls[2].body.params as Record<string, unknown>;
    assert.equal(params.name, COMPARE);
    assert.deepEqual(params.arguments, { column: "state", values: ["CA", "TX"] });
  });

  it("treats omitted arguments as an empty object", async () => {
    const upstream = makeUpstream();
    const c = createClient({ fetchFn: upstream.fetchFn, env: {} });
    const out = await handleCallTool(c, PROVENANCE, undefined);
    assert.deepEqual(out, SAMPLE_RESULT);
    const params = upstream.calls[2].body.params as Record<string, unknown>;
    assert.deepEqual(params.arguments, {});
  });
});

/* ------------------------------------------------------------------ */

describe("CLI", () => {
  const root = fileURLToPath(new URL("..", import.meta.url));
  const cliPath = fileURLToPath(new URL("../src/cli.ts", import.meta.url));

  function runCli(args: string[]) {
    return spawnSync(process.execPath, ["--import", "tsx", cliPath, ...args], {
      cwd: root,
      encoding: "utf8",
      timeout: 30000,
    });
  }

  it("--version prints 0.1.0 and exits 0", () => {
    const res = runCli(["--version"]);
    assert.equal(res.status, 0);
    assert.equal(res.stdout.trim(), "0.1.0");
    assert.equal(res.stdout.trim(), VERSION);
  });

  it("-v prints 0.1.0 and exits 0", () => {
    const res = runCli(["-v"]);
    assert.equal(res.status, 0);
    assert.equal(res.stdout.trim(), "0.1.0");
  });

  it("an unknown command prints usage on stderr and exits 2", () => {
    const res = runCli(["not-a-tool"]);
    assert.equal(res.status, 2);
    assert.match(res.stderr, /Usage:/);
  });

  it("no command at all prints usage on stderr and exits 2", () => {
    const res = runCli([]);
    assert.equal(res.status, 2);
    assert.match(res.stderr, /Usage:/);
  });

  it("a non-JSON argument exits 2", () => {
    const res = runCli([SEARCH, "not json"]);
    assert.equal(res.status, 2);
  });

  it("a JSON array argument exits 2", () => {
    const res = runCli([SEARCH, "[]"]);
    assert.equal(res.status, 2);
  });

  it("extra arguments after a tool exit 2", () => {
    const res = runCli([SEARCH, "{}", "extra"]);
    assert.equal(res.status, 2);
  });

  it("extra arguments after mcp exit 2", () => {
    const res = runCli(["mcp", "extra"]);
    assert.equal(res.status, 2);
  });
});
