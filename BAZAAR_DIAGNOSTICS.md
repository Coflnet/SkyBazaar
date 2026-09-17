# Bazaar rollout diagnostics

SkyBazaar logs owned-order transitions, Redis publication attempts/recovery, and ledger failures.
SkyApi logs price-upload outcomes and snapshot fallbacks. SkyUserState logs synchronization and
retries. SkyModCommands logs snapshot receipt, filtering and display updates. SkyEventBroker logs
processing, target outcomes and acknowledgement. Diagnostic changes do not change fill rules or
the `2.0.0-pre1` display gate.

## Following one order

1. Start with the user's ID and observation time in SkyUserState's `Bazaar sync` logs. These also
   include the operation (`register`, `observe`, `confirm`, `remove`), item and order creation time.
2. In SkyBazaar, find `Bazaar order {OrderId}`. The ID format is
   `UserId:ItemId:creation UTC ticks rounded to milliseconds:IsSell` (`True`/`False`). Transitions
   show old/new filled amounts and confidence, price, observation time, reason and trace ID.
3. Follow `Published Bazaar snapshot` by user and revision. Publication failures include the number
   of pending fill candidates, failure count and elapsed time. Successful recovery logs at
   Information; ordinary successful publication logs at Debug. `Enqueued ... confirmed Bazaar fills`
   reports the number actually inserted by Redis, excluding deduplicated events. `Prepared confirmed
   Bazaar fill` at Debug links each order ID, alert reference and originating revision.
4. For the HUD, match the user/revision in SkyModCommands. Receipt logs include active socket count;
   restoration logs identify cache versus authority; application logs explain version/owner/stale
   skips and the matching-player order count, hidden preference and clear flag.
5. For an alert, `Processing Bazaar fill` in EventBroker links Redis entry ID, order ID, user,
   original revision, stable reference and trace ID. Target outcome logs include the same reference.
   `Completed Bazaar fill` means processing and Redis acknowledgement/deletion succeeded; it does
   not imply every target received a notification (disabled targets and subscriptions can skip it).
   A failed entry stays pending and is reclaimed after a minute; retries retain the same reference.

Example read-only log lookup (replace the placeholder):

```sh
kubectl --context talos-eu -n sky logs deploy/sky-bazaar --since=30m | rg --fixed-strings '<order-id>'
```

Missing traces do not mean an event failed: sampling, legacy payloads, exporter availability and
trace retention all matter. The order ID, revision and reference remain usable independently.

## Log levels

Information includes registration/confirmation/removal, matching-ready timing, publication
recovery, new alert enqueue counts, and alert processing/completion. Partial market-fill changes
are Debug. Trace adds individual market-observation outcomes, changed price levels and FIFO
allocations. No raw inventory, complete book payload, webhook URL or API key is added to logs.

Merge the relevant entries into each service's `Logging:LogLevel` configuration for an investigation:

```json
{
  "Coflnet.Sky.SkyAuctionTracker.Services.OrderBookService": "Debug",
  "Coflnet.Sky.SkyAuctionTracker.Services.BazaarOrderPublisher": "Debug",
  "Coflnet.Sky.Api.Services.BazaarUserOrders": "Debug",
  "Coflnet.Sky.Api.Services.Description.BazaarPriceUpdater": "Trace",
  "Coflnet.Sky.PlayerState.Bazaar": "Debug",
  "Coflnet.Sky.ModCommands.Services.BazaarSignalSubscriptionService": "Debug",
  "Coflnet.Sky.EventBroker.Services.BazaarFillNotifications": "Debug"
}
```

For a suspected FIFO calculation error, temporarily set `OrderBookService` to Trace as well. That
records incoming quantity/delta, eligible queue count and allocation order, including anonymous
liquidity. Keep this time-limited: market updates touch many items. Normal hot-path Trace formatting
is guarded by the enabled level. Leave the global level at Information; per-category overrides can
be provided through Fleet-managed configuration without changing unrelated log categories.

## Traces

SkyBazaar enables the existing batched OTLP exporter when `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is
configured (already present in the Talos global environment). Its startup log states whether trace
export is enabled. `JAEGER_SERVICE_NAME` is `sky-bazaar`; no collector connection is awaited during
startup. Without an endpoint, logs/metrics remain available and startup still succeeds.

Sources and useful spans:

| Service/source | Spans |
| --- | --- |
| `Coflnet.Sky.Api.Bazaar` | `bazaar.price.upload`, `bazaar.orders.read` |
| `Coflnet.Sky.UserState.Bazaar` | `bazaar.order.sync` |
| `Coflnet.Sky.Bazaar` | `bazaar.observe.price`, `bazaar.observe.market`, `bazaar.observe.player`, `bazaar.persist`, `bazaar.snapshot`, `bazaar.publish.attempt` |
| `Coflnet.Sky.ModCommands.Bazaar` | `bazaar.hud.restore`, `bazaar.hud.apply` |
| `Coflnet.Sky.EventBroker.Bazaar` | `bazaar.fill.process` |

HTTP instrumentation propagates the current context between services. Redis snapshots and fill
messages add optional W3C `TraceParent`/`TraceState` fields. HUD and alert consumers continue that
context; API cache reads link their request span to the snapshot's producer. Missing/invalid trace
parents remain compatible and do not prevent processing. Failed ledger/publication/delivery spans
are marked Error. Sampled matching spans contain `bazaar.order.changed` events.

A pending alert retains its original trace/revision even if a later claim replaces the pending
snapshot. Publication attempts follow the latest snapshot context. An alert processed after a long
offline period can therefore refer to a trace that the collector has already expired; use its
stable reference/order ID in logs in that case.

`BAZAAR_TRACE_SAMPLE_RATE` controls SkyBazaar root sampling (default 0.01, valid 0–1). Parent sampling
is respected; this does not force unsampled SkyApi/SkyUserState requests to become sampled. Those
services retain their existing sampling configuration. Setting SkyBazaar's rate to 1 alone is not
a global full-tracing switch. Export remains asynchronous and sampled, without extra database
queries or blocking flushes on the matching path.

## Metrics to watch

Metrics use fixed operation/result/reason labels. User IDs, item tags, order IDs, revisions and
references are deliberately not metric labels. Counters/gauges are per process and reset on restart.

| Metric | Meaning |
| --- | --- |
| `sky_bazaar_matching_ready` | 0 while restoring the ledger, 1 when matching is available |
| `sky_bazaar_observations_total{source,result}` | Direct/Kafka observations accepted or dropped (`applied`, `loading`, `expired`, `future`, `out_of_order`); Kafka outcomes normally count product observations, with one outcome for an entire skipped loading/expired pull |
| `sky_bazaar_match_duration_seconds{source}` | Successful processing including lock waits, ledger writes and publication attempts; `direct` is one price upload, `kafka` is a full market pull, `personal` is a complete personal view |
| `sky_bazaar_matched_observation_age_seconds{source}` | Age of the source observation at processing completion, including upstream transport delay |
| `sky_bazaar_order_transitions_total{reason}` | `registered`, `personal_view`, `chat`, `market_decrease`, `price_passed`, `legacy_confirmed`, `removed` |
| `sky_bazaar_order_persistence_failures_total{operation}` | Failed ledger insert/update/delete |
| `sky_bazaar_publications_total{result}` | Successful/failed Redis publication attempts |
| `sky_bazaar_pending_publication_users` | User snapshots in the in-memory publication buffer |
| `sky_bazaar_pending_fill_events` | Confirmed alert candidates in that buffer; returns toward zero on recovery |
| `sky_api_bazaar_uploads_total{result}` | `accepted`, `rejected`, `expired`, `empty`, `failure` |
| `sky_api_bazaar_read_events_total{result}` | Cache hits/misses/errors, authority success/unavailability, history fallback/unavailability; counts stages, not unique requests |
| `sky_userstate_bazaar_sync_total{operation,result}` | Success, retry and permanent failure for each synchronization operation |
| `sky_userstate_bazaar_retry_waits` | Observations currently in the ten-second retry delay |
| `sky_mod_bazaar_snapshots_total{result}` | `applied`, `stale`, `version`, `owner` |
| `sky_eventbroker_bazaar_deliveries_total{result}` | Successful acknowledgement or failed worker processing |
| `sky_eventbroker_bazaar_reclaimed_total` | Pending stream entries reclaimed for retry |

During rollout, watch ready become 1, pending gauges drain after Redis recovery, and failures stop
increasing. Sustained history fallback indicates that the API is not receiving authoritative
snapshots. A growing `stale` HUD counter can be legitimate during restoration, so correlate it with
revisions before treating it as a fault. Existing process/scrape availability monitoring is still
needed: a stopped consumer cannot increment a failure counter.

## Validation and limits

Regression tests cover actual fill transitions and their diagnostic fields, expired-drop counters,
publication gauges returning to baseline, original alert context surviving retries, API trace links,
and legacy/invalid trace metadata in the HUD and broker. Redis integration checks publication/dedup
and recovery processing using an isolated server. Live collector ingestion and an in-game rollout
smoke test remain deployment checks; unit ActivityListener coverage does not prove those services
are reachable in production.

These additions do not remove the previously documented RDB save window or the in-memory pending
publication crash window. See [order update architecture](ORDER_UPDATES.md).

## Orders-menu refresh failure found on 2026-09-16

The client reported a ten-second description HTTP timeout followed by a null-array error.
SkyApi nevertheless logged an uploaded `Ekwav Co-op Bazaar Orders` view. UserState repeatedly
received HTTP 500 from `/OrderBook/player`: menu items without an item tag made the entire
observation invalid. Resolve missing tags using the existing item-name lookup before sending;
malformed observations return HTTP 400 so they do not block a player in an endless retry loop.
The BUY-name parser must strip formatting before removing the side prefix.

Independently, Redis reached its 32 MiB cap. The sampled dataset held roughly 17,400 keys,
17.9 MB of string payloads and 12,790 queued fill events; a single user snapshot was 1.3 MB.
Publisher logs contained `OOM command not allowed` and retained pending updates. Rebuildable
snapshots now expire after ten minutes instead of seven days, with the authoritative order
ledger unchanged. The chart raises the cap to 50 MiB and the volume to 200 MB, preserving space
for both RDB files. Deploy both changes; existing cache keys acquire the shorter TTL
when republished. Watch pending publications and stream lag drain after rollout.

## Slow rollout readiness found on 2026-09-16

The Talos overlay forced `readinessProbe.initialDelaySeconds: 90`, overriding the chart default.
The observed container started at 19:03:38 UTC and became Ready at 19:05:09 UTC (91 seconds).
Remove that override and set both startup/readiness initial delays to zero. Startup now checks
every three seconds, retaining its fifteen-minute failure budget (300 attempts). Readiness
continues to use `/ready`, which requires successful database activity within five minutes;
this change removes probe waiting time, not actual initialization or database recovery time.
The existing Longhorn storage class supports expansion from 90 MiB to the requested 200 MB.

## Ekwav menu/claim investigation on 2026-09-16

Loki confirmed menu uploads at 20:32:48, 20:33:55 and 20:35:00–44 UTC. UserState received
those views after roughly 0.1–2.8 seconds. The 20:35:10.876 claim chat reached the UserState update entry
at 20:35:17.485 (6.61 seconds). At 20:35:45, claim chat was processed before an older menu
upload: applying that menu afterwards could restore a just-claimed order. These timings
measure ingestion/handler arrival, not the full client-to-display latency.

The uploaded lore explicitly contained expired Agatha orders at 91/160 and 0/160, a fully
filled 2,000 order with 1,616 claimable, and Gill Membrane at 1,024 filled with 512 claimable.
The parser previously lost expiry when restoring the order timestamp, ignored claimable
amounts, and fell back to truncated vendor lists for abbreviated `1k`/`2k` fill text.

- Preserve `IsExpired` and nullable `Claimed` through UserState, the ledger, Redis and readers.
  Expired orders never rejoin matching or generate new fill/outbid alerts. Seven-day expiry
  also stops matching without another upload. Claiming does not announce a new completion.
- Personal reconciliation publishes once after applying the view and removals, and does not
  generate historical outbid alerts while registering already existing orders.
- Only a newer **fill change**, rather than any newer market tick, blocks a personal fill
  correction. Explicit expiry overrides the former matching estimate.
- A stored UserState observation timestamp prevents older menus from reversing claims and
  prevents older claim chat from subtracting quantities already reflected by a newer menu.
- Slow reconciliation (over one second) logs duration and observation age at Information;
  transitions include expiry and claimed count, also attached to trace events.

Deploy SkyBazaar first to enable the additive `is_expired`/`claimed` column migration, then
UserState for parsing and claim handling; API and ModCommands consume the optional metadata.
Mixed versions remain wire compatible, but expiry/claim accuracy requires both producer and
matcher updates. One fresh order-menu upload corrects already misclassified historical orders.
Existing delivered/queued notifications are not retrospectively withdrawn. Public SkyApi price
parsing still pushes directly to SkyBazaar, and the client version gate stays `2.0.0-pre1`.

Validation: 51 Bazaar matching/publisher tests (including isolated Redis), 73 UserState
Bazaar/persistence tests, 20 ModCommands display tests and 7 API reader tests passed. Local
validation used the existing temporary MSBuild package overrides described in ORDER_UPDATES.md.

## Matching latency and partial claims (2026-09-17)

The sampled deployment logged 113 price-passed transitions: median source age 6.03s,
p95 11.03s. This is observation age, not isolated matching CPU time. For example, the
22:42:41.052Z pull began history insertion at 22:42:42.064Z and confirmed orders at
22:42:49.864Z. Matching now runs before that batch's historical inserts. History still
shares the Kafka consumer: a slow previous batch can delay the next one. Direct SkyApi
uploads remain independent and retain their ten-second expiry.

Market matching persists final changes with at most eight concurrent writes per side,
then publishes once per affected user. Personal refreshes process independent item groups
concurrently (eight maximum), preserving per-item locks and publishing after reconciliation.
The matching tests cover both sources and sides, all 50 affected users with two orders each,
and an unaffected user. Session delivery tests cover a slow tutorial and a disconnected socket.
Matching covers registered users regardless of connection status; the HUD retains its existing
2.0.0-pre1 version gate and player filter.

Use a one-second processing target separately from source-to-publication age:

```promql
histogram_quantile(0.95, sum by (source, le) (rate(sky_bazaar_match_duration_seconds_bucket[5m])))
sum by (source) (rate(sky_bazaar_match_duration_seconds_bucket{le="1"}[5m]))
  / sum by (source) (rate(sky_bazaar_match_duration_seconds_count[5m]))
histogram_quantile(0.95, sum by (source, le) (rate(sky_bazaar_matched_observation_age_seconds_bucket[5m])))
```

Also check publication failures/pending users: a failed Redis attempt can return quickly
while delivery remains pending. These metrics do not measure client rendering or acknowledge
receipt by Minecraft. The Kafka market poll cadence also limits how quickly an otherwise
unobserved in-game event can be detected. Confirm live percentiles after deployment; local
I/O simulations do not establish a production latency guarantee.

Ekwav's Gill Membrane menu at 22:35:25.2460183Z reported 256 claimable items, followed
5.354ms later by the chat for that same 256-item withdrawal. UserState added it twice and
removed the order. UserState now carries newly observed withdrawal amounts forward until
matching claim chat consumes them, instead of relying on upload timestamp ordering alone.
That credit survives persistence, repeated menus do not increase it, and subsequent genuine
claims still reduce the remainder. Reopening the orders view restores an already removed
order from its current lore; no ledger migration or mod update is needed for this fix.

### Instant buys and canonical shard IDs

`SkyModCommands` forwards `[Bazaar] Bought ...` directly to `POST /OrderBook/instant-buy`
while the existing Kafka chat path continues recording transactions in `SkyUserState`.
The request carries the item ID, quantity, total coins (including decimals), and chat-receipt timestamp.
`SkyBazaar` consumes sell liquidity through the existing FIFO matcher only when its known book
explains both quantity and total price (within 0.11 coins for display rounding). These fills remain
estimates; chat does not identify the individual sellers. A later price or personal observation
confirms/corrects them. All affected users receive the usual snapshots.

Requests older than ten seconds, in the future, already covered by a newer observation, or received
while loading are dropped. No retry queue is used. An older Bazaar returns 404, which ModCommands
tolerates during rolling deployments. Watch `sky_bazaar_observations_total{source="instant_buy"}`
for `applied`, `book_mismatch`, `expired`, `loading`, and `out_of_order`; the matching duration and
observation-age histograms also include `source="instant_buy"`. A missing or mismatched book still
waits for ordinary observations, so the fast path does not guarantee an immediate fill in every case.

SkyApi now forwards canonical shard IDs from its existing description parser to player-state uploads.
Special shard resolution no longer depends on exactly one NBT metadata field. UserState chat lookups
use the shared shard-name mapping before cached search results. Reopening the orders menu repairs
previously registered generic shard IDs through the normal personal-state reconciliation.
