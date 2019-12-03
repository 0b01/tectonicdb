extern crate tdb_server_lib;
extern crate libtdbcli;

use tdb_server_lib::async_std::task;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn it_works() {
    let host = "0.0.0.0";
    let port = "9001";

    let settings = Arc::new(tdb_server_lib::settings::Settings {
        autoflush: false,
        dtf_folder: "./testdb".to_owned(),
        flush_interval: 1000,
        granularity: 1000,
        q_capacity: 1000,
    });

    task::block_on(async move {
        let _server = task::spawn(tdb_server_lib::server::run_server(&host, &port, settings));

        let cli = libtdbcli::client_from_env();
        libtdbcli::benchmark(cli, 1_000_000);

        let mut cli = libtdbcli::client_from_env();
        cli.use_db("benchmark").unwrap();
        task::sleep(Duration::from_secs(4)).await;
        let ret = cli.cmd("COUNT ALL IN MEM\n").unwrap();
        assert_eq!(ret, "1000000");

    });
}
