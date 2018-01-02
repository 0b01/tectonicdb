use std::thread;
use std::sync::{mpsc, Arc, Mutex};
use std::collections::HashMap;
use libtectonic::dtf::update::Update;

pub type Event = Arc<Mutex<(String, Update)>>;

enum Message {
    Msg(Event),
    Terminate,
}

#[derive(Debug)]
/// using SUBSCRIBE [db] command
/// user can poll from the newly inserted updates
/// server will responde to blank line "" query
pub struct Subscriptions {
    // /// a list of output receivers
    // o_rxs: HashMap<String, Arc<Mutex<mpsc::Receiver<Update>>>>,

    /// string -> Subscription multiplexer
    subs: HashMap<String, Vec<Subscription>>,

    /// input receivers
    i_txs: HashMap<String, Vec<mpsc::Sender<Message>>>,

    /// sub count
    sub_count: HashMap<String, usize>,
}

impl Subscriptions {

    pub fn new() -> Subscriptions {
        // let o_rxs = HashMap::new();
        let subs = HashMap::new();
        let sub_count = HashMap::new();
        let i_txs = HashMap::new();
        Subscriptions {
            // o_rxs,
            sub_count,
            subs,
            i_txs,
        }
    }

    pub fn sub(&mut self, filter: String) -> (usize, Arc<Mutex<mpsc::Receiver<Update>>>) {

        let (i_tx, i_rx) = mpsc::channel();
        let (o_tx, o_rx) = mpsc::channel();

        let i_rx = Arc::new(Mutex::new(i_rx));
        let o_rx = Arc::new(Mutex::new(o_rx));
        let o_tx = Arc::new(Mutex::new(o_tx));

        // upsert
        // if there is a subscription on dbname
        let id = if self.subs.contains_key(&filter) {
            let mut count = self.sub_count.get_mut(&filter).unwrap();
            *count += 1;
            let sub_v = self.subs.get_mut(&filter).unwrap();
            sub_v.push(Subscription::new(filter.clone(), i_rx, o_tx));
            self.i_txs.get_mut(&filter).unwrap().push(i_tx);
            *count
        } else {
            self.sub_count.insert(filter.clone(), 1);
            self.subs.insert(filter.clone(), vec![Subscription::new(filter.clone().clone(), i_rx, o_tx)] );
            self.i_txs.insert(filter, vec![i_tx]);
            1
        };

        (id, o_rx)
    }

    pub fn unsub_all(&mut self) {
        let to_unsub = {
            let mut temp = vec![];
            for (symbol, v) in self.subs.iter() {
                for id in 0..v.len() {
                    temp.push((id, symbol.clone()));
                }
            }
            temp
        };
        println!("{:?}", to_unsub);

        for &(ref id, ref symbol) in to_unsub.iter() {
            self.unsub(*id+1, &symbol);
        }
    }

    pub fn unsub(&mut self, id: usize, filter: &str) {

        // decrement count
        let count = match self.sub_count.get_mut(filter) {
            Some(count) => count,
            None => return
        };
        if *count > 0 { *count -= 1; }

        let id = if id == 0 {0} else {id - 1};

        // terminate the thread
        {
            let i_tx = &match self.i_txs.get_mut(filter).unwrap().get(id) {
                Some(i_tx) => i_tx,
                None => return,
            };
            i_tx.send(Message::Terminate).unwrap();
        }

        // remove closed Sender from list
        {
            &self.i_txs.get_mut(filter).unwrap().remove(id);
        }

        let sub = self.subs
                        .get_mut(filter).unwrap()
                        .get_mut(id).unwrap();
        if let Some(thread) = sub.thread.take() {
            thread.join().unwrap();
        }
    }

    // pub fn get(&self, filter: &str) -> Arc<Mutex<mpsc::Receiver<Update>>> {
    //     self.o_rxs.get(filter).unwrap().clone()
    // }

    pub fn msg(&self, f: Event) {
        for i_tx_v in self.i_txs.values() {
            for i_tx in i_tx_v {
                match i_tx.send(Message::Msg(f.clone())) {
                    Err(_) => error!("Mux message failed!"),
                    _ => (),
                }
            }
        }
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        for i_tx_v in self.i_txs.values() {
            for i_tx in i_tx_v.iter() {
                i_tx.send(Message::Terminate).unwrap();
            }
        }

        for worker_v in &mut self.subs.values_mut() {
            for worker in worker_v.iter_mut() {
                if let Some(thread) = worker.thread.take() {
                    thread.join().unwrap();
                }
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
        let (_id, rx) = subs.sub(symbol.clone());

        subs.msg(event);

        for msg in rx.lock().unwrap().recv() {
            assert_eq!(up, msg);
            break;
        }

    }
}
