use thiserror::Error;
use url;

#[derive(Error, Debug)]
pub enum SnowflakeSourceError {
    #[error(transparent)]
    ConnectorXError(#[from] crate::errors::ConnectorXError),

    #[error(transparent)]
    SnowflakeConnectorError(#[from] snowflake_connector_rs::Error),

    #[error(transparent)]
    SnowflakeUrlError(#[from] url::ParseError),

    #[error(transparent)]
    SnowflakeStdError(#[from] std::io::Error),

    #[error(transparent)]
    SnowflakeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    SnowflakeParseFloatError(#[from] std::num::ParseFloatError),

    #[error(transparent)]
    SnowflakeParseIntError(#[from] std::num::ParseIntError),

    /// Any other errors that are too trivial to be put here explicitly.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
