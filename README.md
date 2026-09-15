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
