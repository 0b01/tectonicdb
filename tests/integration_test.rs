extern crate tdb_server;
extern crate tdb_cli;

use tdb_server::async_std::task;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn it_works() {
    let host = "0.0.0.0";
    let port = "9001";

    let settings = Arc::new(tdb_server::settings::Settings {
        autoflush: false,
        dtf_folder: "./testdb".to_owned(),
        flush_interval: 1000,
        granularity: 1000,
        q_capacity: 1000,
        influx: None,
    });

    task::block_on(async move {
        let _server = task::spawn(tdb_server::server::run_server(&host, &port, settings));

        let cli = tdb_cli::client_from_env();
        tdb_cli::benchmark(cli, 100_000);

        let mut cli = tdb_cli::client_from_env();
        cli.use_db("benchmark").unwrap();
        task::sleep(Duration::from_secs(15)).await;
        let ret = cli.cmd("COUNT ALL IN MEM\n").unwrap();
        assert_eq!(ret, "100000");

    });
}
