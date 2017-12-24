use std::thread;
use std::sync::{mpsc, Arc, Mutex};
use dtf::Update;

pub type Event = Arc<Mutex<(String, Update)>>;

enum Message {
    Msg(Event),
    Terminate,
}

pub struct Subscriptions {
    subs: Vec<Subscription>,
    senders: Vec<mpsc::Sender<Message>>,
}

impl Subscriptions {

    pub fn new() -> Subscriptions {
        let subs = Vec::new();
        let senders = Vec::new();
        Subscriptions {
            subs,
            senders,
        }
    }

    pub fn add(&mut self, filter: String) -> mpsc::Receiver<Update> {

        let (i_tx, i_rx) = mpsc::channel();
        let (o_tx, o_rx) = mpsc::channel();
        let i_rx = Arc::new(Mutex::new(i_rx));
        let o_tx = Arc::new(Mutex::new(o_tx));
        self.subs.push(Subscription::new(filter, i_rx, o_tx));
        self.senders.push(i_tx);
        o_rx

    }

    pub fn msg(&self, f: Event) {
        for i_tx in &self.senders {
            i_tx.send(Message::Msg(f.clone())).unwrap();
        }
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        for i_tx in &mut self.senders {
            i_tx.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.subs {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

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
        let rx = subs.add(symbol);

        subs.msg(event);

        for msg in rx.recv() {
            assert_eq!(up, msg);
            break;
        }

    }
}