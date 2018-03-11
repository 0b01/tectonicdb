
This example is an excerpt from a proprietary market data collection engine. It demonstrates the basic ideas of using the Rust client to TectonicDB.

```rust
mod exchanges;
mod db;
mod utils;
mod conf;

use exchanges::Exchanges;

fn main() {
    init_logger();

    let mut exchanges = Exchanges::new();
    let mut cxn = db::get_cxn();

    // Initialize connections to TectonicDB for all of the exchange adapters and create
    // databases for all of the symbols
    match exchanges.init_dbs(&mut cxn) {
        Ok(()) => info!("DB created."),
        _      => panic!("DB cannot be created."),
    }

    // Then, start the websocket connections for all the exchange adapters in separate
    // threads as to not block the application
    let rx = exchanges.start();

    let mut cxnpool = db::get_cxn_pool();
    // Block the main thread, process messages received from all managed adapters.
    for insert_cmd in rx {
        match cxnpool.insert(&insert_cmd) {
            Ok(_) => (),
            Err(e) => error!("{:?}", e),
        }
    }
}
```
