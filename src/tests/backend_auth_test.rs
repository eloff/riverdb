use std::error::Error;

use test_env_log::test;
use tokio;

use crate::tests::common;

use crate::riverdb::pg::{PostgresCluster, BackendConn, BackendState};
use crate::riverdb::server::Connections;


#[test(tokio::test)]
async fn test_backend_auth() -> Result<(), Box<dyn Error>> {
    let cluster = common::cluster();
    let group = cluster.get_by_database(common::TEST_DATABASE).expect("missing database");
    let pool = group.master().expect("expected db pool");

    let backend = BackendConn::connect(pool.config.address.as_ref().unwrap(), Connections::new(16, 0)).await?;
    backend.test_auth(common::TEST_USER, common::TEST_PASSWORD, pool).await?;

    assert_eq!(backend.state(), BackendState::Ready);
    let params = backend.params();
    assert_eq!(params.get("application_name"), Some("riverdb"));
    assert_eq!(params.get("client_encoding"), Some("UTF8"));
    assert_eq!(params.get("session_authorization"), Some(common::TEST_USER));

    unsafe { Box::from_raw(cluster as *const PostgresCluster as *mut PostgresCluster); }
    Ok(())
}