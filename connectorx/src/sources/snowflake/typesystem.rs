use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

#[derive(Copy, Clone, Debug)]
pub enum SnowflakeTypeSystem {
    Boolean(bool),
    Number(bool),
    Float(bool),
    Varchar(bool),
    Binary(bool),
    Date(bool),
    Time(bool),
    Timestamp(bool),
    TimestampTz(bool),
}

impl_typesystem! {
    system = SnowflakeTypeSystem,
    mappings = {
        { Boolean => bool }
        { Number => i64 }
        { Float => f64 }
        { Varchar =>  String }
        { Date => NaiveDate }
        { Time => NaiveTime }
        { Timestamp => NaiveDateTime }
        { TimestampTz => DateTime<Utc> }
        { Binary => Vec<u8> }
    }
}

impl<'a> From<&'a str> for SnowflakeTypeSystem {
    fn from(ty: &'a str) -> SnowflakeTypeSystem {
        // https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
        use SnowflakeTypeSystem::*;
        match ty {
            "NUMBER" => Number(true),
            "DECIMAL" | "NUMERIC" => Number(true),
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "BYTEINT" => Number(true),
            "FLOAT" | "FLOAT4" | "FLOAT8" => Float(true),
            "DOUBLE" | "DOUBLE PRECISION" | "REAL" => Float(true),
            "VARCHAR" => Varchar(true),
            "CHAR" | "CHARACTER" => Varchar(true),
            "STRING" => Varchar(true),
            "TEXT" => Varchar(true),
            "BINARY" => Binary(true),
            "VARBINARY" => Binary(true),
            "BOOLEAN" => Boolean(true),
            "DATE" => Date(true),
            "DATETIME" => Timestamp(true),
            "TIME" => Time(true),
            "TIMESTAMP" => Timestamp(true),
            "TIMESTAMP_NTZ" => Timestamp(true),
            "TIMESTAMP_TZ" => TimestampTz(true),
            "TIMESTAMP_LTZ" => TimestampTz(true),
            _ => unimplemented!("{}", format!("{:?}", ty)),
        }
    }
}

impl<'a> From<SnowflakeTypeSystem> for &'a str {
    fn from(ty: SnowflakeTypeSystem) -> &'a str {
        use SnowflakeTypeSystem::*;
        match ty {
            Number(_) => "NUMBER",
            Float(_) => "FLOAT",
            Varchar(_) => "VARCHAR",
            Binary(_) => "BINARY",
            Boolean(_) => "BOOLEAN",
            Date(_) => "DATE",
            Time(_) => "TIME",
            Timestamp(_) => "TIMESTAMP_NTZ",
            TimestampTz(_) => "TIMESTAMP_TZ",
        }
    }
}
