use std::io::{BufReader, BufRead, Write};
use std::sync::{Mutex};
use std::process::{ChildStdin, ChildStdout};

use test_env_log::test;

use crate::register_scoped;
use crate::tests::common;
use crate::riverdb::{Error, Result, Plugin};
use crate::riverdb::pg::{
    PostgresCluster, ClientConn, BackendConn, ClientState, client_idle, client_complete_startup
};

use crate::riverdb::server::Connection;
use crate::riverdb::worker::init_workers;



struct QueryPlugin {
    stdout: Mutex<BufReader<ChildStdout>>,
    stdin: Mutex<ChildStdin>,
}

impl QueryPlugin {
    fn new(stdout: ChildStdout, stdin: ChildStdin) -> &'static Self {
        Box::leak(Box::new(Self{
            stdout: Mutex::new(BufReader::new(stdout)),
            stdin: Mutex::new(stdin),
        }))
    }

    pub async fn client_complete_startup<'a>(&'a self, ev: &'a mut client_complete_startup::Event, client: &'a ClientConn, cluster: &'static PostgresCluster) -> Result<()> {
        println!("************BEFORE***************");
        ev.next(client, cluster).await?;
        println!("************AFTER***************");

        println!("sending query");
        let mut stdin = self.stdin.lock().unwrap();
        stdin.write_all("select * from staff;\n".as_bytes())?;
        stdin.flush()?;
        println!("sent query");

        Ok(())
    }

    pub async fn client_idle(&self, ev: &mut client_idle::Event, client: &ClientConn) -> Result<Ark<BackendConn>> {
        {
            println!("reading results");
            let mut reader = self.stdout.lock().unwrap();
            let mut line = String::new();
            while reader.read_line(&mut line)? != 0 {
                println!("************************* {}", line.as_str());
            }
            println!("done with results");
        }

        let backend = ev.next(client).await?;

        println!("sending terminate");
        let mut stdin = self.stdin.lock().unwrap();
        stdin.write_all("\\q\n".as_bytes())?;
        stdin.flush()?;
        println!("sent terminate");
        Ok(backend)
    }
}

impl Plugin for QueryPlugin {}

#[test(tokio::test)]
#[serial_test::serial]
async fn test_proxy_simple_query() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unsafe {
        init_workers(1);
    }

    let listener = common::listener();
    let mut psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str(), "");

    let plugin = QueryPlugin::new(psql.stdout.take().unwrap(), psql.stdin.take().unwrap());
    register_scoped!(plugin, CleanupStartup, QueryPlugin:client_complete_startup<'a>(cluster: &'static PostgresCluster) -> Result<()>);
    register_scoped!(plugin, CleanupIdle, QueryPlugin:client_idle<'a>() -> Result<common::Ark<BackendConn>>);

    let (s, _) = listener.accept().await?;
    let client = ClientConn::new(s, );
    client.set_cluster(Some(common::cluster()));

    println!("************HERE***************");

    assert_eq!(client.run().await, Err(Error::closed()));
    assert_eq!(client.state(), ClientState::Closed);

    Ok(())
}