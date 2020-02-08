//! history recorder for influx

use std::time;
use crate::prelude::*;

pub async fn run(broker: Sender<Event>, settings: Arc<Settings>) {
    task::spawn(timer_loop(broker, settings));
}

pub async fn timer_loop(mut broker: Sender<Event>, settings: Arc<Settings>) {
    if settings.influx.is_none() { return; }
    let influx = settings.influx.as_ref().unwrap();
    let dur = time::Duration::from_secs(influx.interval);
    let url = format!("{}/write?db={}", influx.host, influx.db);
    info!("InfluxDB enabled: {}, {}", influx.host, influx.db);
    loop {
        let (tx, mut rx) = mpsc::channel(2048);
        broker.send(Event::FetchSizes { tx }).await.unwrap();
        while let Some(sizes) = rx.next().await {
            let mut buf = String::new();
            sizes.iter().for_each(|(ob, sz_disk, sz_mem)| {
                buf += &influx.db;
                buf += ",ob=";
                buf += ob;
                buf += " disk=";
                buf += &sz_disk.to_string();
                buf += ",size=";
                buf += &sz_mem.to_string();
                buf += "\n";
            });
            match surf::post(&url).body_bytes(buf).await {
                Err(e) => error!("{}", e),
                Ok(_res) => info!("posted to influxdb"),
            }
            // dbg!(res.body_string().await);
        }

        task::sleep(dur).await;
    }
}