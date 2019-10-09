// google cloud storage plugin
// #[cfg(feature = "gcs")]
// pub mod gstorage;

use crate::prelude::*;

pub mod history;

use std::sync::{Arc, RwLock};

/// Run each plugin in a separate thread
pub async fn run_plugins(broker: Sender<Event>, settings: Settings) {
    info!("initializing plugins");
    history::run(broker, settings).await;

    // #[cfg(feature = "gcs")] gstorage::run(global.clone());
}

// pub fn run_plugin_exit_hooks(state: &ThreadState<'static, 'static>) {
//     // #[cfg(feature = "gcs")] gstorage::run_exit_hook(state);
// }
