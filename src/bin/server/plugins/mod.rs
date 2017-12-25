/// runner for plugins


// google cloud storage plugin
#[cfg(feature = "gcs")]
pub mod gstorage;

// history plugin
pub mod history;

// global state
use std::sync::{Arc, RwLock};
use state::SharedState;


/// Run each plugin in a separate thread
pub fn run_plugins(global: Arc<RwLock<SharedState>>) {

    history::run(global.clone());

    #[cfg(feature = "gcs")]
    gstorage::run(global.clone());

}