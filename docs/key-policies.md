# Client-key resource permissions and spending budgets

Set `key-policy-file` to a durable JSON file and restart CPA. Persist its entire
directory, including the `.initialized` marker. Only one CPA process may own a
ledger. Back up this directory together with the configuration before upgrades.
An unreadable/corrupt ledger blocks access; do not remove the marker to recover.

On first initialization, existing `api-keys` keep all-resource, unlimited access.
Keys added afterward start with no grants. In CPAMC, open **Key permissions and
budgets** to label a masked key, select OAuth credentials and individual upstream
API-key resources, and set each resource's period and dollar limit. Unlimited is
explicit; zero prevents generation. Removing a grant takes effect on the next
request/WS turn, including pinned credentials and retry selection.

Standard and Codex model catalogs show only models from authorized resources.
Exhaustion does not hide model names. Shared-account health is independent of a
particular client key's budget. Existing Qoder/WorkBuddy Credits are separate from
these operator-defined USD limits.

Keeper synchronizes its saved model prices (including the multiplier) and actual
observed Codex quota windows. Calendar day/week/month periods use UTC; weeks start
Monday. Codex primary/weekly periods use observed reset times, never a guessed
seven-day rolling window. Configure the desired period before spending: a period
change with overlapping recorded spend/reservations is rejected to prevent a
balance reset. Changing a limit within the same period preserves spending.

This is a **soft spending cap**. CPA persists a reservation before each upstream
attempt, then settles synchronously from canonical token usage. Actual cost can
exceed the estimate, particularly for long output or multimodal inputs. Missing
usage retains the reservation; missing price/cycle data blocks finite budgets.
Unlimited requests still record usage and flag unpriced requests. USD accounting
starts when this feature is enabled; it does not backfill historical Keeper usage.
Unsupported conditional pricing is unavailable rather than treated as free.

Scoped keys currently support text Chat/Responses (including Responses WebSocket),
Claude Messages, and Gemini generation/Interactions. Auxiliary image/video/live/
realtime/alpha-search transports fail closed for scoped or finite-budget keys.
Home forwarding is unavailable while key policies are enabled. Legacy unscoped
unlimited keys retain their ordinary auxiliary transports.

## Management API

All endpoints require the existing management credential, under `/v0/management`:

- `GET /key-policies` and `/key-policies/report`: safe keys/resources/budget report.
- `PUT /key-policies/{key_id}`: `{revision,label,allow_all,rules}`; each rule is
  `{resource_id,period,limit_usd}`. Money is an unsigned decimal string with up to
  six fractional digits; `null` is unlimited. Stale revisions return 409.
- `PUT /key-policies/sync`: `{prices,cycles}`. A supplied prices array replaces
  the complete current catalog (empty clears it); omitted prices preserve it.
  Prices use `model`, `input_per_million`, `output_per_million`,
  `cache_read_per_million`, `cache_write_per_million`; `unavailable:true` blocks a
  model price. Cycles use `resource_id`, `period`, `cycle_id`, `starts_at`,
  `reset_at`, `observed_at`. Stable observed cycle IDs distinguish manual resets.

Frontend authorization failures return 403, exhausted budgets 429, and unavailable
budget data/storage 503. Reports never expose full client or upstream keys.
