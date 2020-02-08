use crate::prelude::*;

#[cfg(feature = "gcs")]
pub mod gstorage;
#[cfg(feature = "influx")]
pub mod influx;
pub mod history;

/// Run each plugin in a separate thread
pub async fn run_plugins(broker: Sender<Event>, settings: Arc<Settings>) {
    info!("initializing plugins");
    if settings.granularity > 0 {
        history::run(broker.clone(), settings.clone()).await;
    }
    #[cfg(feature = "gcs")] gstorage::run(broker, settings).await;
    #[cfg(feature = "influx")] influx::run(broker, settings).await;
}

#[allow(unused)]
pub fn run_plugin_exit_hooks(_broker: Sender<Event>, settings: Arc<Settings>) {
    #[cfg(feature = "gcs")] gstorage::run_exit_hook(settings);
}
