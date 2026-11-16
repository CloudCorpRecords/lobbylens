# lobbylens

lobbylens puts the Lobbyvo dataset within reach of your MCP client without mirroring it locally. It is a thin forwarding client: the tool list is served from this package, built from a captured copy of the upstream definitions, while every actual query is relayed to the hosted Lobbyvo service and its reply is handed back unchanged.

All seven tools are read-only. You can learn the dataset's shape, hunt through its rows by text, fetch rows matching an exact key, set several rows next to each other, summarise a numeric column, rank rows by one, and read where the data came from so you can cite it properly.

## Tools

- **`dataset_columns`** — Lists the columns, flags which ones are numeric, gives the row count and shows the provenance banner. No arguments; a good first call.
- **`dataset_search`** — Finds rows where any cell contains the search text, ignoring case, returning at most 50. Requires `query`; optional `limit` between 1 and 50.
- **`dataset_row`** — Returns rows where one column matches a value exactly, ignoring case. Requires both `column` and `value`.
- **`dataset_compare`** — Collects the rows whose column equals any of several values, preserving the order you asked in, for head-to-head questions. Requires `column` and `values` (2 to 10 entries).
- **`dataset_stats`** — Computes count, minimum, maximum, mean, median and sum for a numeric column. Commas used as grouping separators and currency formatting are understood; rows that are not numeric are skipped and tallied. Requires `column`.
- **`dataset_top`** — Orders rows by a numeric column, highest first by default. Requires `column`; optional `limit` (1 to 50) and `ascending` to flip to lowest first.
- **`dataset_provenance`** — Reports the source, the date the dataset was computed, its licence and the preferred citation. No arguments.

## Running it

Through npm, as an MCP server:

```bash
npx -y @reneturcios/lobbylens mcp
```

Client configuration:

```json
{
  "mcpServers": {
    "lobbylens": {
      "command": "npx",
      "args": ["-y", "@reneturcios/lobbylens", "mcp"]
    }
  }
}
```

Through Docker:

```bash
docker run -i ghcr.io/cloudcorprecords/lobbylens
docker run --rm ghcr.io/cloudcorprecords/lobbylens dataset_provenance '{}'
```

## Command line

The same binary doubles as a CLI. Each tool becomes a subcommand whose single argument is a JSON object written as one string:

```bash
lobbylens dataset_columns '{}'
lobbylens dataset_search '{"query":"energy"}'
lobbylens dataset_row '{"column":"state","value":"CA"}'
lobbylens dataset_compare '{"column":"state","values":["CA","TX"]}'
lobbylens dataset_stats '{"column":"amount"}'
lobbylens dataset_top '{"column":"amount","limit":10,"ascending":true}'
lobbylens dataset_provenance '{}'
lobbylens --version
```

Exit status is `0` for a successful forwarded call, `1` when the upstream answers with an error result, and `2` for usage mistakes like an unknown subcommand or malformed JSON.

## Configuration and behaviour notes

- `LOBBYVO_MCP_URL` — Replaces the upstream address. When unset, calls go to the pinned endpoint `https://lobbyvo.com/mcp`. This override is an addition made by this rebuild.
- No credentials are involved: the upstream registry entry lists no headers for the remote, so lobbylens never attaches an `Authorization` header or any other authentication header.
- When something upstream goes wrong (a JSON-RPC error, a non-2xx status, an unreadable body, or a failed request), the caller receives an error result whose text begins with `lobbylens error:`.

## Session handling

Forwarding speaks streamable-HTTP JSON-RPC. The first call in a process opens a session with an `initialize` request (protocol version `2025-03-26`), sends `notifications/initialized`, and only then issues `tools/call`. If the service returns an `Mcp-Session-Id` header, lobbylens echoes it on subsequent requests and keeps reusing that one session. Replies in either plain JSON or server-sent-events framing are accepted. Successful results are passed through exactly as received, `isError` flag and any extra fields included.

## Development

```bash
npm install
npm run build   # tsc -> dist/ (dist/cli.js is the bin)
npm test        # tsx --test test/*.test.ts — stubbed upstream, no network
npm run dev     # run the CLI straight from source
```

Tests compare the served tool list against the captured surface, walk through the handshake and its reuse, confirm that no `Authorization` header leaves the process under any environment, and check that arguments and results cross the boundary untouched.

## License

MIT — see `LICENSE`. Copyright (c) 2026 Rene Turcios.
