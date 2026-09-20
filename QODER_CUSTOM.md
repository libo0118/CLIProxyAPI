# Qoder request Credits

`qoder-custom` extends the v7.3.5 baseline with optional per-request Qoder billing metadata. The default branch retains upstream code.

The release workflow is retained unchanged from this fork's `main` at `61fdfc341b96178a8dcb53f2efc46cbc341d267c`; the application baseline remains v7.3.5.

The plugin executor captures `usage.credits`, `usage.original_credits` and `usage.billable` before response translation. Streaming, non-streaming and failed attempts retain observed billing independently from token counts and request outcome. Decimal JSON numbers are preserved. The usage queue publishes this as `qoder_credits` only for the Qoder provider; an empty object indicates that billing was not supplied.

Use the matching [Keeper custom branch](https://github.com/libo0118/cpa-usage-keeper/tree/qoder-custom) to persist and display these fields. The [Qoder plugin custom branch](https://github.com/libo0118/cpa-multi-plugins/tree/qoder-custom) also preserves nested upstream errors and rejects empty streams.

## Updating and verification

Keep `origin` pointed at this fork and `upstream` at `router-for-me/CLIProxyAPI`. Merge selected stable tags into `qoder-custom`; do not replace the custom branch with upstream files.

```sh
git fetch upstream --tags
git switch qoder-custom
git merge <stable-tag>
go test ./internal/runtime/executor/helps ./internal/pluginhost ./internal/redisqueue ./sdk/cliproxy/usage
docker build --build-arg VERSION=v7.3.5-qoder-credits.1 -t local/cli-proxy-api:v7.3.5-qoder-credits.1 .
```

Before deployment, back up runtime configuration and verify that the existing native plugin loads in an isolated container. Preserve auth files, management assets and mounts. Repository synchronization alone does not deploy a server. Never commit credentials, runtime configuration, databases, auth files or build artifacts.

## Optional Responses WebSocket application keepalive

```yaml
streaming:
  keepalive-seconds: 20
  websocket-application-keepalive: true
```

The new option defaults to false, retaining protocol Ping frames. When enabled, active Responses WebSocket turns send the private text event `{"type":"cpa.keepalive"}` while waiting for upstream data. Enable it only for clients that ignore unknown events, as Codex does. This is a CPA extension, not an OpenAI response lifecycle event; it adds no model output, timeline entry or usage. SSE behavior is unchanged.

Heartbeats stop on completion, error or cancellation. They do not cover idle time between turns, remove upstream liveness limits, or recover a failed upstream. This does not guarantee that every intermediary or future client version accepts the extension.

## Codex reasoning catalog

OpenAI-compatible models at the exact native `api.deepseek.com` host receive documented `low/high/max` defaults for `deepseek-flash`, `deepseek-v4-pro`, and the accepted legacy Flash IDs when no explicit thinking configuration exists. Explicit model settings and other gateways are preserved. This prevents generic `low/medium/high` defaults from hiding `max` in the Codex catalog. The existing client-version gate for extended efforts remains intact. No new thinking-off option is introduced.

Verify with `go test ./internal/config ./internal/client/codex/models`, compile `./cmd/server`, and check `/v1/models?client_version=cpa` and the actual client's version. Model-list checks do not consume inference quota. The corresponding Qoder plugin publishes model-specific `thinking_config.enabled.efforts`; WorkBuddy retains its own upstream effort names rather than copying the native DeepSeek contract.
