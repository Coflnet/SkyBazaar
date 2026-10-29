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

## External CQL TLS preparation

Core 0.7.5 handles the existing `CASSANDRA` registration. To opt in, set
`CASSANDRA:TLS:CA_CERT_PATH`, `CERT_PATH` and `KEY_PATH`; use roots in the CA
bundle and a client leaf plus issuing intermediates in the certificate PEM.
Keep the existing CQL account/password, hosts and keyspace. Default name
verification uses each contacted IP. `CASSANDRA:TLS:SERVER_NAME` is an explicit
verified identity override, including normal CN fallback for a legacy no-SAN
server; it never disables chain or hostname validation. For the current Scylla
listener the reviewed overlap bundle includes its actual legacy CQL CA and the
shared root, with the verified legacy name `sky-client`.

Cassandra driver 3.23.0 reloads PEM material for new physical connections through
Core's per-connection hostname resolver; existing connections stay active. This
relies on the driver's callback ordering and must be reverified on a driver
upgrade. Use a writable ephemeral `HOME` for the current user's public client
intermediate store; no root or machine trust is installed. Mount projected PEM
files without `subPath`. When the existing `OLD_CASSANDRA:HOSTS` migration branch
is enabled, its distinct `OLD_CASSANDRA:TLS` section must be reviewed separately.

`/health/ready` reads only `SELECT key FROM system.local` with a five-second query
budget. It is a connection diagnostic; `/ready` still requires recent successful
CQL activity (reads can also refresh it). Promotion must prove both, plus fresh
known-product prices/history, independent Kafka consumer progress, Redis queue
behavior and unchanged archive safety flags.
Current Scylla does not request client certificates: healthy TLS with password
authentication does not prove CQL mTLS or presentation of a renewed client leaf.
Kafka batch consumption, internal HTTP/Redis and archive credentials/flags retain
their existing paths; no Core 0.7.6 single-topic rotation is enabled here.

Server-leaf replacement is blocked separately: these clients currently verify the
fixed name `sky-client`, while the six current server PKI roles allow per-node
DNS/IP identities. Do not issue and deploy per-node-only leaves under this client
configuration. Review a compatible common SAN or per-contact name migration first.
