## Bazaar History manager
This service stores the historic prices of all items on bazaar in cassandra.

It also owns Bazaar order matching and fill state. See [Bazaar order updates](ORDER_UPDATES.md)
for the direct SkyApi price path, offline tracking, seven-day order expiry, API/HUD snapshots,
and offline alerts, including shared Redis persistence, restart behavior and compatibility.

## Deploying
This project should be deployed within a container. 
### Configuration
There are currently no configuration options.
`CASSANDRA:HOSTS` can be a comma seperated list of ip/dns names

## Research exports (internal, read-only)

- `GET api/Bazaar/{itemId}/depth/export?start&end&levels=10` streams `depth-compact-v1`
  (`{"schema","fields","rows":[[ts,bid,ask,buyVolume,sellVolume,buyMovingWeek,sellMovingWeek,buyOrdersCount,sellOrdersCount,minAsk,maxAsk,minBid,maxBid,bids,asks]]}`,
  levels are `[price, amount, orders]`, `bids` descending, `asks` ascending, rows ascending by time).
  Window at most 31 days, `levels` 1..30. At most `DEPTH_EXPORT:MAX_CONCURRENT` (default 4) run at once, otherwise
  HTTP 429 with `Retry-After`; `DEPTH_EXPORT:TIMEOUT_SECONDS` (default 120) bounds one request. Send `Accept-Encoding: gzip`.
  It is fast because only the top N levels are decoded straight from the stored MessagePack bytes (the data endpoint
  materialises both complete books per row and serialises them), cold S3 blocks are projected from their columns
  instead of being turned into row objects, and everything (legacy, S3 blocks, minute buckets, seconds table) is read
  lazily in pages/blocks in time order and written as it arrives, so nothing is buffered as one response. Errors before
  the first row return a 5xx; a failure mid-stream aborts the connection.
- `GET /OrderBook/{itemTag}/history?since&pageSize=1000&pagingState` lists every `order_book` row of the item placed
  since `since` (default 7 days ago, at most 8 days), including closed/expired/claimed ones and `filledAt`
  (first time persisted as fully filled, null if unknown), without player names. Cluster-internal: no ingress or
  SkyApi route forwards `/OrderBook`.
