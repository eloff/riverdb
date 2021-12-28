<img alt="tests" src="https://github.com/riverdb/riverdb/actions/workflows/tests.yml/badge.svg" height=30 />
<a href="https://codecov.io/gh/riverdb/riverdb">
  <img alt="code coverage" src="https://codecov.io/gh/riverdb/riverdb/branch/master/graph/badge.svg?token=EjhI1wBhtG" height=30 />
</a>

## River DB
### Programmable PostgreSQL Proxy and Connection Pool

River DB is a Rust connection pool and middleware proxy.

It offers similar functionality to:
- Pgpool-II
- PgBouncer
- pgagroal
- Odyssey

What makes it uniquely interesting is you can create and mix apps/plugins written in Rust that hook into one or more parts of the PostgreSQL protocol to modify the behavior. Including the replication stream/protocol.

<!---
It parses, normalizes, and provides the PostgreSQL AST for queries (using the Postgres parser). It
does this efficiently by using a high-performance query normalizer and caching the parsed AST
for the normalized query.
-->

You can use this for logging/auditing, query rewriting, fully customizable multi-master partitioning, caching with automatic invalidation, high availability/failover, upgrading PostgreSQL without downtime, extending the protocol, joining/querying/merging data from other data sources, custom authentication, basically anything you can do with a programmable middlware between your application and PostgreSQL.

We'll be providing a number of paid apps/plugins on top of River DB to make
using and operating PostgreSQL easier and more enjoyable with an
emphasis on providing an excellent developer experience.

## Alpha Software

River DB is currently Alpha quality software. We don't run it in production yet, and neither should you. Databases are critical infrastructure and we take that responsibility very seriously.

You can start to play with it, write software with it, and test it out for your intended use case.

We'll update this notice once we've been running it in production ourselves for a while. You can help out by helping us test it, finding and reporting issues, or even submitting fixes and test cases.

## Consulting

If you want help building apps for PostgreSQL using River DB contact us at: info[at]riverdb.com. Nobody knows the platform better.

## License

River DB is source-visible, not OSI open-source. See our [Modified PolyForm Shield License](LICENSE.md).

We welcome contributions, bug reports, and bug fixes!


