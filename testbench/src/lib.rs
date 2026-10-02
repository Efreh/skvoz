//! Experimental real-NATS test harness, not a production node runtime.

pub mod mesh;
mod node;
pub mod process_workload;
pub mod runtime_scenarios;
pub mod scenarios;
pub mod tcp_demo;

pub use node::{BenchConfig, BenchError, ConnectionConfig, FailureKind, Node, NodeEvent, Role};
