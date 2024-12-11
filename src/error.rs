use thiserror::Error;

#[derive(Error, Debug)]
/// Wrapper error
pub enum KvsError {
    #[error("Failed to read file")]
    /// Failure to read log file
    FileReadError(#[from] std::io::Error),
    #[error("Failed to parse file")]
    /// Failure to parse / deserialize log file
    ParseError(#[from] serde_json::Error),
    #[error("KeyNotFound")]
    /// Attempted to remove key that was never present
    KeyNotFound,
    #[error("Unknown error occured")]
    /// Something terrible has happened
    Unknown,
    #[error("No command specified")]
    /// No command was provided
    NoCommand,
}

/// Type alias
pub type Result<T> = std::result::Result<T, KvsError>;
