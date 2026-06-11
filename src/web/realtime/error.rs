use salvo::http::StatusError;

use crate::command::CommandValidationError;
use crate::error::MqttEngineError;

#[derive(Debug)]
pub(crate) enum CommandEndpointError {
    Validation(CommandValidationError),
    Queue(MqttEngineError),
}

impl CommandEndpointError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::Validation(err) => err.message(),
            Self::Queue(err) => err.to_string(),
        }
    }

    pub(crate) fn into_status_error(self) -> StatusError {
        match self {
            Self::Validation(err) => StatusError::bad_request().brief(err.message()),
            Self::Queue(MqttEngineError::CommandQueueFull) => {
                StatusError::service_unavailable().brief("command queue is full")
            }
            Self::Queue(MqttEngineError::CommandQueueClosed) => {
                StatusError::service_unavailable().brief("command queue is closed")
            }
            Self::Queue(err) => StatusError::internal_server_error().brief(err.to_string()),
        }
    }
}

impl From<CommandValidationError> for CommandEndpointError {
    fn from(err: CommandValidationError) -> Self {
        Self::Validation(err)
    }
}
