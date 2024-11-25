use std::env;
use num_cpus;

pub struct Config {
    pub worker_threads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        let worker_threads = env::var("MR_WORKER_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| num_cpus::get());

        Self { worker_threads }
    }
}
