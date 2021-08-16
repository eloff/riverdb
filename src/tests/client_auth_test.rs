use std::error::Error;

use test_env_log::test;

use crate::register_scoped;
use crate::tests::common;
use crate::riverdb::{Result, Plugin, config};
use crate::riverdb::pg::{ClientConn, ClientState, client_authenticate};
use crate::riverdb::pg::protocol::{Messages, AuthType};
use crate::riverdb::server::Connection;


struct AuthPlugin {}

impl AuthPlugin {
    pub async fn client_authenticate(&self, ev: &mut client_authenticate::Event, client: &ClientConn, auth_type: AuthType, msgs: Messages) -> Result<()> {
        let result = ev.next(client, auth_type, msgs).await;
        result
    }
}

impl Plugin for AuthPlugin {
    fn create(_: Option<&'static config::ConfigMap>) -> Result<Self> {
        Ok(Self{})
    }
}

#[test(tokio::test)]
#[serial_test::serial]
async fn test_client_auth() -> std::result::Result<(), Box<dyn Error>> {
    let cluster = common::cluster();
    let group = cluster.get_by_database(common::TEST_DATABASE).expect("missing database");
    let pool = group.master().expect("expected db pool");

    let listener = common::listener();
    let mut psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str(), "");

    let (s, _) = listener.accept().await?;
    let client = ClientConn::new(s);

    register_scoped!(AuthPlugin:client_authenticate, 'a (auth_type: AuthType, msgs: Messages) -> Result<()>);

    //psql.stdin.unwrap().write_str("\\q\n");
    //client.run().await.expect("unexpected error in run()");

    //assert_eq!(client.)

    Ok(())
}