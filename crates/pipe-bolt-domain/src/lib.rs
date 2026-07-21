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
    clippy::missing_const_for_fn,
)]

pub mod action;
pub mod command;
pub mod config;
pub mod error;
pub mod event;
pub mod id;
pub mod rule;

pub use action::*;
pub use command::*;
pub use config::*;
pub use error::*;
pub use event::*;
pub use id::*;
pub use rule::*;
