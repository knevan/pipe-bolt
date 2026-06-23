#![deny(
    clippy::panic,
    clippy::panicking_unwrap,
    clippy::redundant_clone,
    clippy::implicit_clone,
    clippy::perf,
    clippy::large_types_passed_by_value,
    clippy::large_futures,
    clippy::trivially_copy_pass_by_ref,
    clippy::clone_on_ref_ptr
)]

pub mod dto;
pub mod error;
pub mod handler;
pub mod model;
pub mod router;
pub mod runtime_control;
pub mod state;

pub use error::ApiError;
pub use router::{management_router, serve_management_api};
pub use runtime_control::{RuntimeControl, RuntimeControlError};
pub use state::{ApiState, ManagementAuth};
