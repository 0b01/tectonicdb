use std::thread;
use std::sync::{mpsc, Arc, Mutex};
use std::collections::HashMap;
use dtf::Update;

pub type Event = Arc<Mutex<(String, Update)>>;

enum Message {
    Msg(Event),
    Terminate,
}

#[derive(Debug)]
pub struct Subscriptions {
    // /// a list of output receivers
    // o_rxs: HashMap<String, Arc<Mutex<mpsc::Receiver<Update>>>>,

    /// string -> Subscription multiplexer
    subs: HashMap<String, Subscription>,

    /// input receivers
    i_txs: Vec<mpsc::Sender<Message>>,
}

impl Subscriptions {

    pub fn new() -> Subscriptions {
        // let o_rxs = HashMap::new();
        let subs = HashMap::new();
        let i_txs = Vec::new();
        Subscriptions {
            // o_rxs,
            subs,
            i_txs,
        }
    }

    pub fn sub(&mut self, filter: String) -> Arc<Mutex<mpsc::Receiver<Update>>> {

        let (i_tx, i_rx) = mpsc::channel();
        let (o_tx, o_rx) = mpsc::channel();

        let i_rx = Arc::new(Mutex::new(i_rx));
        let o_rx = Arc::new(Mutex::new(o_rx));
        let o_tx = Arc::new(Mutex::new(o_tx));

        self.subs.insert(filter.clone(), Subscription::new(filter.clone(), i_rx, o_tx));
        // self.o_rxs.insert(filter.clone(), o_rx.clone());
        self.i_txs.push(i_tx);

        o_rx
    }

    // pub fn get(&self, filter: &str) -> Arc<Mutex<mpsc::Receiver<Update>>> {
    //     self.o_rxs.get(filter).unwrap().clone()
    // }

    pub fn msg(&self, f: Event) {
        for i_tx in &self.i_txs {
            i_tx.send(Message::Msg(f.clone())).unwrap();
        }
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        for i_tx in &mut self.i_txs {
            i_tx.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.subs.values_mut() {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

#[derive(Debug)]
struct Subscription {
    thread: Option<thread::JoinHandle<()>>,
}

impl Subscription {
    fn new(filter: String, i_rx: Arc<Mutex<mpsc::Receiver<Message>>>, o_tx: Arc<Mutex<mpsc::Sender<Update>>>) -> Subscription {

        let thread = thread::spawn(move ||{
            loop {
                let message = i_rx.lock().unwrap().recv().unwrap();

                match message {
                    Message::Msg(up) => {
                        let (ref symbol, ref up) = *up.lock().unwrap();
                        if symbol == &filter {
                            let _ = o_tx.lock().unwrap().send(*up);
                        }
                    },
                    Message::Terminate => {
                        break;
                    },
                }
            }
        });

        Subscription {
            thread: Some(thread),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_subscription() {

        let up = Update {ts: 0, seq: 0, is_bid: false,
            is_trade: false, price: 0., size: 0.};
        let symbol = "bt_eth_btc".to_owned();
        let event = Arc::new(Mutex::new((symbol.clone(), up)));

        let mut subs = Subscriptions::new();
        subs.add(symbol.clone());
        let rx = subs.get(&symbol);

        subs.msg(event);

        for msg in rx.lock().unwrap().recv() {
            assert_eq!(up, msg);
            break;
        }

    }
}