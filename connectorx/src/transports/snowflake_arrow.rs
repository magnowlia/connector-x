//! Transport from Snowflake Source to Arrow Destination.

use crate::{
    destinations::arrow::{typesystem::ArrowTypeSystem, ArrowDestination, ArrowDestinationError},
    impl_transport,
    sources::snowflake::{SnowflakeSource, SnowflakeSourceError, SnowflakeTypeSystem},
    typesystem::TypeConversion,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnowflakeArrowTransportError {
    #[error(transparent)]
    Source(#[from] SnowflakeSourceError),

    #[error(transparent)]
    Destination(#[from] ArrowDestinationError),

    #[error(transparent)]
    ConnectorX(#[from] crate::errors::ConnectorXError),
}

/// Convert Snowflake data types to Arrow data types.
pub struct SnowflakeArrowTransport;

impl_transport!(
    name = SnowflakeArrowTransport,
    error = SnowflakeArrowTransportError,
    systems = SnowflakeTypeSystem => ArrowTypeSystem,
    route = SnowflakeSource => ArrowDestination,
    mappings = {
        { Boolean[bool]            => Boolean[bool]             | conversion auto }
        { Number[i64]              => Int64[i64]                | conversion auto }
        { Float[f64]               => Float64[f64]              | conversion auto }
        { Varchar[String]          => LargeUtf8[String]         | conversion auto }
        { Date[NaiveDate]          => Date32[NaiveDate]         | conversion auto }
        { Time[NaiveTime]          => Time64[NaiveTime]         | conversion auto }
        { DateTime[NaiveDateTime]  => Date64[NaiveDateTime]     | conversion auto }
        { Binary[Vec<u8>]          => LargeBinary[Vec<u8>]      | conversion auto }
    }
);
