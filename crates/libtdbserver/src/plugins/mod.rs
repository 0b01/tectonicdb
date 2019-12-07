use crate::prelude::*;

#[cfg(feature = "gcs")]
pub mod gstorage;
pub mod history;

/// Run each plugin in a separate thread
pub async fn run_plugins(broker: Sender<Event>, settings: Arc<Settings>) {
    info!("initializing plugins");
    if settings.granularity > 0 {
        history::run(broker.clone(), settings.clone()).await;
    }
    #[cfg(feature = "gcs")] gstorage::run(broker, settings);
}

#[allow(unused)]
pub fn run_plugin_exit_hooks(_broker: Sender<Event>, settings: Arc<Settings>) {
    #[cfg(feature = "gcs")] gstorage::run_exit_hook(settings);
}
