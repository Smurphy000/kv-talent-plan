use thiserror::Error;

#[derive(Error, Debug)]
/// Wrapper error
pub enum KVSError {
    #[error("Datastore error")]
    /// Error from datastore
    DSError(#[from] DataStoreError),
    #[error("CLI error ")]
    /// Error from CLI
    CLIError(#[from] CLIError),
    #[error("Path error")]
    /// Error when optaining path
    PathError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
/// Define all error states
pub enum DataStoreError {
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
}

#[derive(Error, Debug)]
/// Error from using the CLI interface
pub enum CLIError {
    #[error("No command specified")]
    /// No command was provided
    NoCommand,
}
