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

struct TransactionPlugin {
    stdout: Mutex<BufReader<ChildStdout>>,
    stdin: Mutex<ChildStdin>,
    transactions: AtomicI32,
}

impl TransactionPlugin {
    fn new(stdout: ChildStdout, stdin: ChildStdin) -> &'static Self {
        Box::leak(Box::new(Self{
            stdout: Mutex::new(BufReader::new(stdout)),
            stdin: Mutex::new(stdin),
            transactions: AtomicI32::new(0),
        }))
    }

    pub async fn client_complete_startup<'a>(&'a self, ev: &'a mut client_complete_startup::Event, client: &'a ClientConn, cluster: &'static PostgresCluster) -> Result<()> {
        ev.next(client, cluster).await?;

        let mut stdin = self.stdin.lock().unwrap();
        // This is deferred until the update statement
        stdin.write_all("begin;\n".as_bytes())?;
        stdin.flush()?;
        // Without the limit this just hangs. The update query below doesn't get sent
        // from psql to rust. I don't know why, so I'll just put the limit for now.
        stdin.write_all("select * from inventory limit 500;\n".as_bytes())?;
        stdin.flush()?;
        stdin.write_all("update inventory set last_update = now() where inventory_id < 10;\n".as_bytes())?;
        stdin.flush()?;
        stdin.write_all("commit transaction;\n".as_bytes())?;
        stdin.flush()?;

        Ok(())
    }

    pub async fn client_idle(&self, ev: &mut client_idle::Event, client: &ClientConn) -> Result<Ark<BackendConn>> {
        let backend = ev.next(client).await?;
        if backend.is_none() {
            return Ok(backend);
        }

        let prev_count = self.transactions.fetch_add(1, Relaxed);
        if prev_count == 0 {
            {
                let mut reader = self.stdout.lock().unwrap();
                let mut out = String::new();
                while reader.read_line(&mut out)? != 0 {
                    if out.ends_with("COMMIT\n") {
                        break
                    }
                }
                assert!(out.contains("(500 rows)"));
                assert!(out.contains("UPDATE 9"));
            }

            let mut stdin = self.stdin.lock().unwrap();
            // Without the limit this just hangs. The update query below doesn't get sent
            // from psql to rust. I don't know why, so I'll just put the limit for now.
            stdin.write_all("begin;\nselect * from customer_list limit 250;\n".as_bytes())?;
            stdin.flush()?;
            stdin.write_all("insert into inventory (inventory_id, film_id, store_id) values (5000, 2, 3);\n".as_bytes())?;
            stdin.flush()?;
            stdin.write_all("rollback;\n".as_bytes())?;
            stdin.flush()?;
        } else if prev_count == 1 {
            {
                let mut reader = self.stdout.lock().unwrap();
                let mut out = String::new();
                while reader.read_line(&mut out)? != 0 {
                    if out.ends_with("ROLLBACK\n") {
                        break
                    }
                }
                assert!(out.contains("(250 rows)\n"));
                assert!(out.contains("INSERT 0 1"));
            }

            let mut stdin = self.stdin.lock().unwrap();
            stdin.write_all("begin;\n".as_bytes())?;
            stdin.flush()?;
            stdin.write_all("commit;\n".as_bytes())?;
            stdin.flush()?;
            stdin.write_all("\\q\n".as_bytes())?;
            stdin.flush()?;
        }

        Ok(backend)
    }
}

impl Plugin for TransactionPlugin {}

#[test(tokio::test)]
#[serial_test::serial]
async fn test_proxy_transactions() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unsafe {
        init_workers(1);
    }

    let listener = common::listener();
    let mut psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str(), "");

    let plugin = TransactionPlugin::new(psql.stdout.take().unwrap(), psql.stdin.take().unwrap());
    register_scoped!(plugin, CleanupStartup, TransactionPlugin:client_complete_startup<'a>(cluster: &'static PostgresCluster) -> Result<()>);
    register_scoped!(plugin, CleanupIdle, TransactionPlugin:client_idle<'a>() -> Result<Ark<BackendConn>>);

    let (s, _) = listener.accept().await?;
    let client = ClientConn::new(s, Connections::new(16, 0));
    client.set_cluster(Some(common::cluster()));

    assert_eq!(client.run().await, Err(Error::closed()));
    assert_eq!(client.state(), ClientState::Closed);
    assert_eq!(plugin.transactions.load(Relaxed), 3);

    Ok(())
}