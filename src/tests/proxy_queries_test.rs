use std::io::{BufReader, BufRead, Write};
use std::sync::{Mutex};
use std::sync::atomic::{AtomicI32};
use std::sync::atomic::Ordering::{Relaxed};
use std::process::{ChildStdin, ChildStdout};

use test_env_log::test;

use crate::register_scoped;
use crate::tests::common;
use crate::riverdb::common::Ark;
use crate::riverdb::{Error, Result, Plugin};
use crate::riverdb::pg::{
    PostgresCluster, ClientConn, BackendConn, ClientState, client_idle, client_complete_startup
};

use crate::riverdb::server::{Connection, Connections};
use crate::riverdb::worker::init_workers;


struct QueryPlugin {
    stdout: Mutex<BufReader<ChildStdout>>,
    stdin: Mutex<ChildStdin>,
    queries: AtomicI32,
}

impl QueryPlugin {
    fn new(stdout: ChildStdout, stdin: ChildStdin) -> &'static Self {
        Box::leak(Box::new(Self{
            stdout: Mutex::new(BufReader::new(stdout)),
            stdin: Mutex::new(stdin),
            queries: AtomicI32::new(0),
        }))
    }

    pub async fn client_complete_startup<'a>(&'a self, ev: &'a mut client_complete_startup::Event, client: &'a ClientConn, cluster: &'static PostgresCluster) -> Result<()> {
        ev.next(client, cluster).await?;

        let mut stdin = self.stdin.lock().unwrap();
        stdin.write_all("update staff set active = true returning *;\n".as_bytes())?;
        stdin.flush()?;

        Ok(())
    }

    pub async fn client_idle(&self, ev: &mut client_idle::Event, client: &ClientConn) -> Result<Ark<BackendConn>> {
        let prev_count = self.queries.fetch_add(1, Relaxed);
        if prev_count == 0 {
            {
                let mut reader = self.stdout.lock().unwrap();
                let mut out = String::new();
                while reader.read_line(&mut out)? != 0 {
                    if out.ends_with("rows)\n") {
                        break
                    }
                }
                assert!(out.ends_with("(2 rows)\n"));
                assert!(out.contains("Mike.Hillyer@sakilastaff.com"));
                assert!(out.contains("Jon.Stephens@sakilastaff.com"));
            }

            let result = ev.next(client).await;

            let mut stdin = self.stdin.lock().unwrap();
            stdin.write_all("select * from film;\n".as_bytes())?;
            stdin.flush()?;
            result
        } else {
            {
                let mut reader = self.stdout.lock().unwrap();
                let mut out = String::new();
                while reader.read_line(&mut out)? != 0 {
                    if out.ends_with("rows)\n") {
                        break
                    }
                }
                assert!(out.ends_with("(1000 rows)\n"));
                assert!(out.contains("A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies"));
                assert!(out.contains("A Intrepid Panorama of a Mad Scientist And a Boy who must Redeem a Boy in A Monastery"));
                assert_eq!(out.matches("Mad Scientist").count(), 97+4); // Occurs twice in the descriptions of 4 films
            }

            let result = ev.next(client).await;

            let mut stdin = self.stdin.lock().unwrap();
            stdin.write_all("\\q\n".as_bytes())?;
            stdin.flush()?;
            result
        }
    }
}

impl Plugin for QueryPlugin {}

#[test(tokio::test)]
#[serial_test::serial]
async fn test_proxy_queries() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unsafe {
        init_workers(1);
    }

    let listener = common::listener();
    let mut psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str(), "");

    let plugin = QueryPlugin::new(psql.stdout.take().unwrap(), psql.stdin.take().unwrap());
    register_scoped!(plugin, CleanupStartup, QueryPlugin:client_complete_startup<'a>(cluster: &'static PostgresCluster) -> Result<()>);
    register_scoped!(plugin, CleanupIdle, QueryPlugin:client_idle<'a>() -> Result<Ark<BackendConn>>);

    let (s, _) = listener.accept().await?;
    let client = ClientConn::new(s, Connections::new(16, 0));
    client.set_cluster(Some(common::cluster()));

    assert_eq!(client.run().await, Err(Error::closed()));
    assert_eq!(client.state(), ClientState::Closed);
    assert_eq!(plugin.queries.load(Relaxed), 2);

    Ok(())
}