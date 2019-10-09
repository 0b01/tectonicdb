//! history recorder
use crate::prelude::*;

use std::time;

pub async fn run(broker: Sender<Event>, settings: Settings) {
    task::spawn(timer_loop(broker, settings));
    // timer.await;
}

pub async fn timer_loop(mut broker: Sender<Event>, settings: Settings) {
    let dur = time::Duration::from_secs(settings.clone().granularity);
    loop {
        broker.send(Event::History {}).await.unwrap();
        task::sleep(dur).await;
    }
}