//! history recorder
use crate::prelude::*;

use std::time;

pub async fn run(broker: Sender<Event>, settings: Arc<Settings>) {
    task::spawn(timer_loop(broker, settings));
}

pub async fn timer_loop(mut broker: Sender<Event>, settings: Arc<Settings>) {
    let dur = time::Duration::from_secs(settings.granularity);
    loop {
        broker.send(Event::History {}).await.unwrap();
        task::sleep(dur).await;
    }
}