use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Release, Acquire};

use test_env_log::test;


use crate::register_scoped;
use crate::tests::common;
use crate::riverdb::{Error, Result, Plugin};
use crate::riverdb::pg::{PostgresCluster, ClientConn, ClientState, client_authenticate};
use crate::riverdb::pg::protocol::{Messages, AuthType};
use crate::riverdb::server::Connection;
use crate::riverdb::worker::init_workers;


struct AuthPlugin {
    cluster: &'static PostgresCluster,
    passed: AtomicBool,
}

impl AuthPlugin {
    pub async fn client_authenticate(&self, ev: &mut client_authenticate::Event, client: &ClientConn, auth_type: AuthType, msgs: Messages) -> Result<()> {
        assert_eq!(client.state(), ClientState::Authentication);

        let group = self.cluster.get_by_database(common::TEST_DATABASE).expect("missing database");
        let _pool = group.master().expect("expected db pool");
        client.set_cluster(Some(self.cluster));

        let result = ev.next(client, auth_type, msgs).await;
        assert!(result.is_ok());
        assert_eq!(client.state(), ClientState::Ready);
        self.passed.store(true, Release);
        client.close();
        result
    }
}

impl Plugin for AuthPlugin {
    fn new() -> &'static Self {
        Box::leak(Box::new(Self{
            cluster: common::cluster(),
            passed: AtomicBool::new(false),
        }))
    }
}


#[test(tokio::test)]
#[serial_test::serial]
async fn test_client_auth() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unsafe {
        init_workers(1);
    }

    let listener = common::listener();
    let _psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str(), "");

    let (s, _) = listener.accept().await?;
    let client = ClientConn::new(s);

    let plugin = AuthPlugin::new();
    register_scoped!(plugin, AuthPlugin:client_authenticate<'a>(auth_type: AuthType, msgs: Messages) -> Result<()>);

    //psql.stdin.unwrap().write("\\q\n".as_bytes());
    assert_eq!(client.run().await, Err(Error::closed()));
    assert_eq!(client.state(), ClientState::Closed);
    assert!(plugin.passed.load(Acquire));

    Ok(())
}