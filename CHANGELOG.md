# Changelog

## 0.1.0 (2026-10-28)

- First release: a forwarding client for the hosted Lobbyvo MCP server. The seven captured tools are answered locally for tools/list, and each tools/call is relayed upstream over a streamable-HTTP JSON-RPC session (initialize, notifications/initialized, tools/call, carrying the Mcp-Session-Id header whenever the upstream issues one).
- Tools: `dataset_columns`, `dataset_search`, `dataset_row`, `dataset_compare`, `dataset_stats`, `dataset_top`, `dataset_provenance` — the full captured 7-tool surface, with no omissions.
- Upstream results return verbatim, isError flag included; upstream failures are surfaced as `lobbylens error:` text results.
- A single bin provides the CLI (one subcommand per tool, plus `--version`) and the stdio MCP server (`mcp` subcommand). The upstream endpoint may be overridden with `LOBBYVO_MCP_URL`. The registry record lists no headers on the remote, so no auth header is ever sent.
