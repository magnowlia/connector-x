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
            // FIXED is what Snowflake's REST API actually returns for the NUMBER /
            // DECIMAL / NUMERIC family (not deprecated — the comment used to say
            // so, but it's the live name). AVG / SUM-of-int / DATEDIFF results
            // arrive as FIXED with scale > 0 and string values like "83.269863";
            // mapping FIXED to i64 made the decoder reject them. Match the
            // existing NUMBER -> Float treatment for the whole scale-aware family.
            // INT/INTEGER/BIGINT/etc. stay as i64 — those are user-declared
            // integer types where preserving int precision matters.
            "NUMBER" | "DECIMAL" | "NUMERIC" | "FIXED" => Float(true),
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
