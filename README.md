![tests](https://github.com/riverdb/riverdb/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/riverdb/riverdb/branch/master/graph/badge.svg?token=EjhI1wBhtG)](https://codecov.io/gh/riverdb/riverdb)

## River DB - Rust Programmable PostgreSQL Proxy

River DB is a Rust replacement for the connection pools / load balancers Pg Pool or PgBouncer.

What makes it interesting is you can create and mix apps/plugins written in Rust that hook into one or more parts of the PostgreSQL protocol to modify the behavior. Including the replication stream/protocol.

You can use this for logging/auditing, query rewriting, fully customizable partitioning, caching with automatic invalidation, high availability/failover, upgrading PostgreSQL without downtime, extending the protocol, joining/querying/merging data from other data sources, basically anything you can do with a programmable middlware between your application and PostgreSQL.

## Alpha Software

River DB is currently Alpha quality software. We don't run it in production yet, and neither should you. Databases are critical infrastructure and we take that responsibility very seriously.

You can start to play with it, write software with it, and test it out for your intended use case.

We'll update this notice once we've been running it in production ourselves for a while. You can help out by helping us test it, finding and reporting issues, or even submitting fixes and test cases.

## Consulting

If you want help building apps for PostgreSQL using River DB contact us at: info[atsymbol]riverdb.com. Nobody knows the platform better.

## Why is it called River DB?

Our long-term aspiration is to create a global, low-latency, high-throughput database on top of good old rock-solid PostgreSQL.

No, not a fancy Cockroach DB, Spanner, or Yugabyte type of thing, just a straightforward single master (or partitioned multi-master) with replicas potentially geographically distributed.
That might sound boring compared to Spanner with their GPS/atomic clocks, but it may actually perform better
and cost less in many important use cases.

Something we could use efficiently from Cloudflare Workers or new serverless platforms like it. It should be possible
to build something like an Uber or an Asana with just partitioned PostgreSQL and Cloudflare Workers. With none of the insane feats of
software engineering and complexity that currently is the norm to scale applications.

## License

River DB is source-visible, not OSI open-source. See our [Modified PolyForm Shield License](LICENSE.md).

We welcome contributions, bug reports, and bug fixes! You can't make a cloud service out of River DB for profit though, sorry Amazon.


