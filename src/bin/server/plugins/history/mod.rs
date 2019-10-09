//! history recorder
use crate::prelude::*;

use std::time;
use circular_queue::CircularQueue;
use std::sync::{Arc, RwLock};

pub async fn run(mut broker: Sender<Event>, settings: Settings) {
    let timer = task::spawn(timer_loop(broker, settings));
    // timer.await;
}

pub async fn timer_loop(mut broker: Sender<Event>, settings: Settings) {
    let dur = time::Duration::from_secs(settings.clone().granularity);
    loop {
        broker.send(Event::History {}).await;
        task::sleep(dur).await;
    }
}