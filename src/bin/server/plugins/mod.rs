/// runner for plugins
// google cloud storage plugin
#[cfg(feature = "gcs")]
pub mod gstorage;

// history plugin
pub mod history;

use std::sync::{Arc, RwLock};

/// Run each plugin in a separate thread
pub fn run_plugins(global: Arc<RwLock<SharedState>>) {
    history::run(global.clone());

    #[cfg(feature = "gcs")] gstorage::run(global.clone());
}

pub fn run_plugin_exit_hooks(state: &ThreadState<'static, 'static>) {
    #[cfg(feature = "gcs")] gstorage::run_exit_hook(state);
}
