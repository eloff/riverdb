use std::io::{BufReader, BufRead, Write};

use test_env_log::test;

use crate::tests::common;
use crate::riverdb::{Error};
use crate::riverdb::pg::{ClientConn, ClientState};

use crate::riverdb::server::Connection;
use crate::riverdb::worker::init_workers;


#[test(tokio::test)]
#[serial_test::serial]
async fn test_proxy_simple_query() -> std::result::Result<(), Box<dyn std::error::Error>> {
    unsafe {
        init_workers(1);
    }

    let listener = common::listener();
    let mut psql = common::psql(format!("host=localhost port={}", listener.local_addr().unwrap().port()).as_str(), "");

    let (s, _) = listener.accept().await?;
    let client = ClientConn::new(s);
    client.set_cluster(Some(common::cluster()));

    tokio::task::spawn_blocking(move || {
        let reader = BufReader::new(psql.stdout.take().unwrap());
        let mut stdin = psql.stdin.take().unwrap();
        println!("sending query");
        stdin.write_all("select * from staff;\n".as_bytes()).unwrap();
        stdin.flush().unwrap();
        println!("sent query");

        for line in reader.lines() {
            println!("************************* {}", line.unwrap());
        }

        println!("sending terminate");
        stdin.write_all("\\q\n".as_bytes()).unwrap();
        stdin.flush().unwrap();
        println!("sent terminate");
    });

    assert_eq!(client.run().await, Err(Error::closed()));
    assert_eq!(client.state(), ClientState::Closed);

    Ok(())
}