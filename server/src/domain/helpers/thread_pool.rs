//
// Copyright (c) 2023 Nathan Fiedler
//

//! Create a pool of threads to run arbitrary functions. The pool of threads
//! will remain active until the pool is dropped.

use log::{error, info, warn, trace};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

///
/// Use `new()` to create a thread pool and `execute()` to send functions
/// to be executed on the worker threads.
///
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The `size` is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }
        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }

    /// Execute the given function on a worker in the pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Some(worker) = self.sender.as_ref() {
            let job = Box::new(f);
            if let Err(err) = worker.send(job) {
                error!("failed to send job: {err}");
            }
        }
    }

    pub fn size(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            info!("thread_pool: shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take() {
                if let Err(err) = thread.join() {
                    error!("failed to join thread: {err:?}")
                }
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = match receiver.lock() {
                Ok(guard) => guard.recv(),
                Err(poisoned) => {
                    // hard to imagine how this would matter
                    warn!("using poisoned receiver");
                    poisoned.into_inner().recv()
                }
            };
            match message {
                Ok(job) => {
                    trace!("worker {id} got a job; executing...");
                    job();
                }
                Err(_) => {
                    trace!("worker {id} disconnected; shutting down...");
                    break;
                }
            }
        });
        Worker {
            id,
            thread: Some(thread),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_pool() {
        let counter = Arc::new(Mutex::new(0));
        // scope the pool so it will be dropped and shut down
        {
            let pool = ThreadPool::new(4);
            for _ in 0..pool.size() {
                let counter = counter.clone();
                pool.execute(move || {
                    let mut v = counter.lock().unwrap();
                    *v += 1;
                });
            }
        }
        // by now the thread pool has shut down
        let value = counter.lock().unwrap();
        assert_eq!(*value, 4);
    }
}
