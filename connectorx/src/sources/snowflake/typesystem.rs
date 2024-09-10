use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

#[derive(Copy, Clone, Debug)]
pub enum SnowflakeTypeSystem {
    Boolean(bool),
    Number(bool),
    Float(bool),
    Varchar(bool),
    Binary(bool),
    Date(bool),
    Time(bool),
    DateTime(bool),
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
        { DateTime => NaiveDateTime }
        { Binary => Vec<u8> }
    }
}

impl<'a> From<&'a str> for SnowflakeTypeSystem {
    fn from(ty: &'a str) -> SnowflakeTypeSystem {
        // https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
        use SnowflakeTypeSystem::*;
        match ty.to_uppercase().as_str() {
            "NUMBER" => Number(true),
            "DECIMAL" | "NUMERIC" => Number(true),
            "FIXED" => Number(true), // not documented in the link above (deprecated?)
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
            "DATETIME" => DateTime(true),
            "TIME" => Time(true),
            "TIMESTAMP" => DateTime(true),
            "TIMESTAMP_NTZ" => DateTime(true),
            "TIMESTAMP_TZ" => DateTime(true),
            "TIMESTAMP_LTZ" => DateTime(true),
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
            DateTime(_) => "TIMESTAMP_NTZ",
        }
    }
}
