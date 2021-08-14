use std::error::Error;

use test_env_log::test;
use tokio;

use crate::tests::common;
use crate::riverdb::config;
use crate::riverdb::pg::{PostgresCluster, BackendConn, BackendState};

#[test(tokio::test)]
async fn test_backend_auth() -> Result<(), Box<dyn Error>> {
    let cluster = common::cluster();
    let group = cluster.get_by_database(common::TEST_DATABASE).expect("missing database");
    let pool = group.master().expect("expected db pool");

    let backend = BackendConn::connect(pool.config.address.as_ref().unwrap()).await?;
    backend.test_auth(common::TEST_USER, common::TEST_PASSWORD, pool).await?;

    assert_eq!(backend.state(), BackendState::Ready);

    unsafe { Box::from_raw(cluster as *const PostgresCluster as *mut PostgresCluster); }
    Ok(())
}