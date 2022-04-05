#![feature(generic_associated_types)]
#![allow(incomplete_features)]

pub mod arrow;
pub mod arrow2;
pub mod constants;
mod errors;
pub mod pandas;
pub mod read_sql;
mod source_router;

use pyo3::prelude::*;
use pyo3::{wrap_pyfunction, PyResult};
use std::convert::TryFrom;
use std::sync::Once;

#[macro_use]
extern crate lazy_static;

static START: Once = Once::new();

// https://github.com/PyO3/pyo3-built/issues/21
// #[allow(dead_code)]
// mod build {
//     include!(concat!(env!("OUT_DIR"), "/built.rs"));
// }

#[pymodule]
fn connectorx(_: Python, m: &PyModule) -> PyResult<()> {
    START.call_once(|| {
        let _ = env_logger::try_init();
    });

    m.add_wrapped(wrap_pyfunction!(read_sql))?;
    m.add_wrapped(wrap_pyfunction!(partition_sql))?;
    m.add_wrapped(wrap_pyfunction!(get_meta))?;
    m.add_class::<pandas::PandasBlockInfo>()?;
    Ok(())
}

#[pyfunction]
pub fn read_sql<'a>(
    py: Python<'a>,
    conn: &str,
    return_type: &str,
    protocol: Option<&str>,
    queries: Option<Vec<String>>,
    partition_query: Option<read_sql::PartitionQuery>,
) -> PyResult<&'a PyAny> {
    read_sql::read_sql(py, conn, return_type, protocol, queries, partition_query)
}

#[pyfunction]
pub fn partition_sql(
    conn: &str,
    partition_query: read_sql::PartitionQuery,
) -> PyResult<Vec<String>> {
    let source_conn = source_router::SourceConn::try_from(conn)?;
    let queries = read_sql::partition(&partition_query, &source_conn)?;
    Ok(queries.into_iter().map(|q| q.to_string()).collect())
}

#[pyfunction]
pub fn get_meta<'a>(
    py: Python<'a>,
    conn: &str,
    protocol: Option<&str>,
    query: String,
) -> PyResult<&'a PyAny> {
    pandas::get_meta::get_meta(py, conn, protocol.unwrap_or("binary"), query)
        .map_err(|e| From::from(e))
}