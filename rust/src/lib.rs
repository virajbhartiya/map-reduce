pub mod coordinator;
pub mod client;
pub mod server;
pub mod functions;
pub mod utils;
pub mod config;

pub mod mapreduce {
    include!(concat!(env!("OUT_DIR"), "/mapreduce.rs"));
}

// Re-export commonly used items
pub use client::run_map_reduce_job;
pub use functions::{MapFunction, ReduceFunction, FunctionRegistry};
