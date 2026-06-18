#![deny(
    clippy::panic,
    clippy::panicking_unwrap,
    clippy::redundant_clone,
    clippy::implicit_clone,
    clippy::perf,
    clippy::large_types_passed_by_value,
    clippy::large_futures,
    clippy::trivially_copy_pass_by_ref,
    clippy::clone_on_ref_ptr,
    // clippy::unwrap_used,
    // clippy::missing_const_for_fn,
)]

pub mod action_metadata;
pub mod bus;
pub mod codec;
pub mod command;
pub mod config;
pub mod dispatcher;
pub mod error;
pub mod forwarder;
pub mod message;
pub mod mqtt;
pub mod pipeline;
pub mod router;
pub mod rule;
pub mod web;
