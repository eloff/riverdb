/*
The integration tests are organized into the same binary in this directory.

1) For speed (no external inking required)
2) For access to internals
3) So we can compile the library crate with cfg(test) - we use that

See: https://matklad.github.io/2021/02/27/delete-cargo-integration-tests.html
 */

mod common;
mod tls_test;
mod backend_auth_test;