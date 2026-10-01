//! Experimental real-NATS test harness, not a production node runtime.

mod node;
pub mod scenarios;

pub use node::{BenchConfig, BenchError, ConnectionConfig, FailureKind, Node, NodeEvent, Role};
