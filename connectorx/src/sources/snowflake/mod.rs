//! Source implementation for Snowflake

mod errors;
mod typesystem;

pub use self::errors::SnowflakeSourceError;
use crate::{
    data_order::DataOrder,
    errors::ConnectorXError,
    sources::{PartitionParser, Produce, Source, SourcePartition},
    sql::{count_query, limit1_query, CXQuery},
};
use anyhow::anyhow;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fehler::{throw, throws};
use serde_json::Value;
use snowflake_connector_rs::{
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeRow,
};
use sqlparser::dialect::SnowflakeDialect;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::runtime::Runtime;
use url::Url;

pub use typesystem::SnowflakeTypeSystem;

pub struct SnowflakeSource {
    rt: Arc<Runtime>,
    client: Arc<SnowflakeClient>,
    origin_query: Option<String>,
    queries: Vec<CXQuery<String>>,
    names: Vec<String>,
    schema: Vec<SnowflakeTypeSystem>,
}

impl SnowflakeSource {
    #[throws(SnowflakeSourceError)]
    pub fn new(rt: Arc<Runtime>, conn: &str) -> Self {
        let url = Url::parse(conn)?;
        let query_pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
        let account = query_pairs
            .get("account")
            .ok_or_else(|| anyhow!("account is not found in connection string"))?;
        let warehouse = query_pairs.get("warehouse");
        let database = query_pairs.get("database");
        let schema = query_pairs.get("schema");
        let role = query_pairs.get("role");
        let timeout = query_pairs
            .get("timeout")
            .map(|t| t.parse::<u64>())
            .transpose()?
            .map(Duration::from_secs);

        let client_config = SnowflakeClientConfig {
            account: account.to_owned(),
            warehouse: warehouse.cloned(),
            database: database.cloned(),
            schema: schema.cloned(),
            role: role.cloned(),
            timeout,
        };

        let username = url.username();
        let password = url.password();
        let private_key = query_pairs.get("private_key");
        let passphrase = query_pairs.get("passphrase");

        let auth = match (password, private_key, passphrase) {
            (Some(password), None, None) => SnowflakeAuthMethod::Password(password.to_string()),
            (None, Some(private_key), Some(passphrase)) => SnowflakeAuthMethod::KeyPair {
                encrypted_pem: private_key.to_owned(),
                password: passphrase.as_bytes().to_vec(),
            },
            _ => throw!(anyhow!("invalid auth parameters")),
        };

        let client = Arc::new(SnowflakeClient::new(username, auth, client_config)?);

        Self {
            rt,
            client,
            origin_query: None,
            queries: vec![],
            names: vec![],
            schema: vec![],
        }
    }
}

impl Source for SnowflakeSource
where
    SnowflakeSourcePartition:
        SourcePartition<TypeSystem = SnowflakeTypeSystem, Error = SnowflakeSourceError>,
{
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type Partition = SnowflakeSourcePartition;
    type TypeSystem = SnowflakeTypeSystem;
    type Error = SnowflakeSourceError;

    #[throws(SnowflakeSourceError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorXError::UnsupportedDataOrder(data_order));
        }
    }

    fn set_queries<Q: ToString>(&mut self, queries: &[CXQuery<Q>]) {
        self.queries = queries.iter().map(|q| q.map(Q::to_string)).collect();
    }

    fn set_origin_query(&mut self, query: Option<String>) {
        self.origin_query = query;
    }

    #[throws(SnowflakeSourceError)]
    fn fetch_metadata(&mut self) {
        assert!(!self.queries.is_empty());
        let session = self.rt.block_on(self.client.create_session())?;
        for query in self.queries.iter() {
            let l1query = limit1_query(query, &SnowflakeDialect {})?;
            let rows: Vec<SnowflakeRow> = self.rt.block_on(session.query(l1query.as_str()))?;
            let first_row = rows
                .first()
                .ok_or_else(|| anyhow!("cannot get columns data due to empty query result"))?;
            let (names, types) = first_row
                .column_types()
                .iter()
                .map(|col| {
                    (
                        col.name().to_owned(),
                        SnowflakeTypeSystem::from(col.column_type().snowflake_type()),
                    )
                })
                .unzip();
            self.names = names;
            self.schema = types;
        }
    }

    #[throws(SnowflakeSourceError)]
    fn result_rows(&mut self) -> Option<usize> {
        match &self.origin_query {
            Some(q) => {
                let cxq = CXQuery::Naked(q.clone());
                let cquery = count_query(&cxq, &SnowflakeDialect {})?;
                let session = self.rt.block_on(self.client.create_session())?;
                let rows: Vec<SnowflakeRow> = self.rt.block_on(session.query(cquery.as_str()))?;
                let first_row = rows
                    .first()
                    .ok_or_else(|| anyhow!("cannot get count result"))?;
                let nrows: u64 = first_row.at(0).map_err(|e| anyhow!(e))?;
                Some(nrows as usize)
            }
            None => None,
        }
    }

    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    fn schema(&self) -> Vec<Self::TypeSystem> {
        self.schema.clone()
    }

    #[throws(SnowflakeSourceError)]
    fn partition(self) -> Vec<Self::Partition> {
        let mut ret = vec![];
        for query in self.queries {
            ret.push(SnowflakeSourcePartition::new(
                self.rt.clone(),
                self.client.clone(),
                &query,
                &self.schema,
            ));
        }
        ret
    }
}

pub struct SnowflakeSourcePartition {
    rt: Arc<Runtime>,
    client: Arc<SnowflakeClient>,
    query: CXQuery<String>,
    schema: Vec<SnowflakeTypeSystem>,
    nrows: usize,
    ncols: usize,
}

impl SnowflakeSourcePartition {
    pub fn new(
        handle: Arc<Runtime>,
        client: Arc<SnowflakeClient>,
        query: &CXQuery<String>,
        schema: &[SnowflakeTypeSystem],
    ) -> Self {
        Self {
            rt: handle,
            client,
            query: query.clone(),
            schema: schema.to_vec(),
            nrows: 0,
            ncols: schema.len(),
        }
    }
}

impl SourcePartition for SnowflakeSourcePartition {
    type TypeSystem = SnowflakeTypeSystem;
    type Parser<'a> = SnowflakeSourceParser;
    type Error = SnowflakeSourceError;

    #[throws(SnowflakeSourceError)]
    fn result_rows(&mut self) {
        let cquery = count_query(&self.query, &SnowflakeDialect {})?;
        let session = self.rt.block_on(self.client.create_session())?;
        let rows: Vec<SnowflakeRow> = self.rt.block_on(session.query(cquery.as_str()))?;
        let first_row = rows
            .first()
            .ok_or_else(|| anyhow!("cannot get count result"))?;
        let nrows: u64 = first_row.at(0).map_err(|e| anyhow!(e))?;
        self.nrows = nrows as usize;
    }

    #[throws(SnowflakeSourceError)]
    fn parser(&mut self) -> Self::Parser<'_> {
        let session = self.rt.block_on(self.client.create_session())?;
        let rows: Vec<SnowflakeRow> = self.rt.block_on(session.query(self.query.as_str()))?;
        SnowflakeSourceParser::new(self.rt.clone(), self.client.clone(), rows, &self.schema)
    }

    fn nrows(&self) -> usize {
        self.nrows
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

pub struct SnowflakeSourceParser {
    _rt: Arc<Runtime>,
    _client: Arc<SnowflakeClient>,
    rows: Vec<SnowflakeRow>,
    current_row: usize,
    ncols: usize,
    current_col: usize,
}

impl SnowflakeSourceParser {
    fn new(
        rt: Arc<Runtime>,
        client: Arc<SnowflakeClient>,
        rows: Vec<SnowflakeRow>,
        schema: &[SnowflakeTypeSystem],
    ) -> Self {
        Self {
            _rt: rt,
            _client: client,
            rows,
            current_row: 0,
            ncols: schema.len(),
            current_col: 0,
        }
    }

    #[throws(SnowflakeSourceError)]
    fn next_loc(&mut self) -> (usize, usize) {
        let ret = (self.current_row, self.current_col);
        self.current_row += (self.current_col + 1) / self.ncols;
        self.current_col = (self.current_col + 1) % self.ncols;
        ret
    }
}

impl<'a> PartitionParser<'a> for SnowflakeSourceParser {
    type TypeSystem = SnowflakeTypeSystem;
    type Error = SnowflakeSourceError;

    #[throws(SnowflakeSourceError)]
    fn fetch_next(&mut self) -> (usize, bool) {
        assert!(self.current_col == 0);
        (self.rows.len(), true)
    }
}

macro_rules! impl_produce {
    ($($t: ty,)+) => {
        $(
            impl<'r> Produce<'r, $t> for SnowflakeSourceParser {
                type Error = SnowflakeSourceError;

                #[throws(SnowflakeSourceError)]
                fn produce(&'r mut self) -> $t {
                    let (ridx, cidx) = self.next_loc()?;
                    let row = self.rows.get(ridx).ok_or_else(|| anyhow!("row is none"))?;
                    let val = row.at(cidx).map_err(|e| anyhow!(e))?;
                    val
                }
            }

            impl<'r> Produce<'r, Option<$t>> for SnowflakeSourceParser {
                type Error = SnowflakeSourceError;

                #[throws(SnowflakeSourceError)]
                fn produce(&'r mut self) -> Option<$t> {
                    let (ridx, cidx) = self.next_loc()?;
                    let row = self.rows.get(ridx).ok_or_else(|| anyhow!("row is none"))?;
                    let val = row.at(cidx).map_err(|e| anyhow!(e))?;
                    val
                }
            }
        )+
    };
}

impl_produce!(
    i8,
    i32,
    i64,
    f64,
    bool,
    NaiveTime,
    NaiveDateTime,
    NaiveDate,
    Value,
    String,
);

impl<'r> Produce<'r, Vec<u8>> for SnowflakeSourceParser {
    type Error = SnowflakeSourceError;

    fn produce(&'r mut self) -> Result<Vec<u8>, Self::Error> {
        let (ridx, cidx) = self.next_loc()?;
        let row = self.rows.get(ridx).ok_or_else(|| anyhow!("row is none"))?;
        let val: String = row.at(cidx).map_err(|e| anyhow!(e))?;
        Ok(val.as_bytes().to_vec())
    }
}

impl<'r> Produce<'r, Option<Vec<u8>>> for SnowflakeSourceParser {
    type Error = SnowflakeSourceError;

    fn produce(&'r mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        let (ridx, cidx) = self.next_loc()?;
        let row = self.rows.get(ridx).ok_or_else(|| anyhow!("row is none"))?;
        let val: Option<String> = row.at(cidx).map_err(|e| anyhow!(e))?;
        let val = val.map(|v| v.as_bytes().to_vec());
        Ok(val)
    }
}
