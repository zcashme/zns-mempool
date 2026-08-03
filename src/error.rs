use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("missing required environment variable {0}")]
    MissingEnv(&'static str),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Rpc(#[from] jsonrpsee::core::ClientError),

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),

    #[error(transparent)]
    Status(#[from] tonic::Status),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("invalid view key for recipient {recipient_address}: {reason}")]
    ViewKey {
        recipient_address: String,
        reason: String,
    },

    #[error("invalid transaction from node: {0}")]
    Transaction(String),

    #[error("reserve table name is not a valid SQL identifier: {0}")]
    InvalidTableName(String),
}

pub type Result<T> = std::result::Result<T, Error>;
