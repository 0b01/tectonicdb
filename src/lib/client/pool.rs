use std::{thread, time};
use std::collections::VecDeque;

use crate::client::circular_queue::CircularQueue;
use super::{Cxn, InsertCommand, TectonicError};

type InsertQueue = CircularQueue<InsertCommand>;

/// A pool of workers that operate on an internal circular queue of InsertionCommand.
pub struct CxnPool {
    cxns: Vec<Cxn>,
    host: String,
    port: String,
    available_workers: VecDeque<usize>,
    queue: InsertQueue,
}

impl CxnPool {
    /// Create a pool of connections
    pub fn new(n_workers: usize, host: &str, port: &str, capacity: usize) -> Result<Self, TectonicError> {
        let mut cxns = vec![];
        let mut workers = VecDeque::new();

        let queue = CircularQueue::with_capacity(capacity);

        for i in 0..n_workers {
            let cxn = Cxn::new(host, port)?;
            cxns.push(cxn);
            workers.push_back(i);
        }

        Ok(CxnPool{
            cxns,
            host: host.to_owned(),
            port: port.to_owned(),
            available_workers: workers,
            queue: queue,
        })
    }

    /// Create a new datastore
    pub fn create_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        info!("Creating db {}", dbname);
        self.cmd(&format!("CREATE {}\n", dbname))
    }

    /// Send custom command
    pub fn cmd(&mut self, command: &str) -> Result<String, TectonicError> {
        let n = self.available_workers.pop_front();
        let n = match n {
            Some(n) => n,
            None => {
                // grow avail cxns
                warn!("Growing CxnPool to {}", self.cxns.len());
                self.cxns.push(Cxn::new(&self.host, &self.port)?);
                self.cxns.len() - 1
            }
        };

        // exec command
        let result = self.cxns[n].cmd(command);
        let ret = match result {
            Err(TectonicError::ConnectionError) => {
                thread::sleep(time::Duration::from_secs(1));
                // replace current cxn
                self.cxns[n] = Cxn::new(&self.host, &self.port)?;
                error!("REPLACING CXN");
                result
            }
            _ => result,
        };

        // push id back to queue of avail workers
        self.available_workers.push_back(n);

        ret
    }

    /// Insert to current datastore
    pub fn insert(&mut self, cmd: &InsertCommand) -> Result<(), TectonicError> {

        let n = self.available_workers.pop_front();
        let n = match n {
            Some(n) => n,
            None => {
                // grow avail cxns
                self.cxns.push(Cxn::new(&self.host, &self.port)?);
                warn!("Growing CxnPool to {}", self.cxns.len());
                self.cxns.len() - 1
            }
        };

        for c in cmd.clone().into_string() {
            let result = self.cxns[n].cmd(&c);
            match result {
                Err(TectonicError::ConnectionError) => {
                    thread::sleep(time::Duration::from_secs(1));
                    {
                        self.queue.push(cmd.clone());
                    }
                    self.cxns[n] = Cxn::new(&self.host, &self.port)?;
                    error!("Replacing cxn.");
                    self.available_workers.push_back(n);
                    return Err(TectonicError::ConnectionError);
                },

                Err(TectonicError::DBNotFoundError(ref dbname)) => {
                    let _ = self.create_db(dbname);
                    {
                        self.queue.push(cmd.clone());
                    }
                    self.available_workers.push_back(n);
                    return Err(TectonicError::DBNotFoundError(dbname.to_owned()));
                },

                Err(e) => {
                    self.available_workers.push_back(n);
                    return Err(e)
                },

                _ => (),
            }
        }

        // self.insert_retry_queue.pop();

        // push id back to queue of avail workers
        self.available_workers.push_back(n);

        {
            let ins_cmd = self.queue.pop();
            if let Some(i) = ins_cmd {
                let _ = self.insert(&i)?;
            }
        }

        Ok(())
    }
}
