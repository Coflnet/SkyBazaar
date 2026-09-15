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
