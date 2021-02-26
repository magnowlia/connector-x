use anyhow::anyhow;
use connector_agent::writers::arrow::ArrowWriter;
use connector_agent::{
    data_sources::{DataSource, Produce, SourceBuilder},
    ConnectorAgentError, DataOrder, DataType, Dispatcher, Result,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fehler::{throw, throws};
use rand::Rng;

mod profiler;

const NROWS: [usize; 2] = [100000, 100000];
const NCOLS: usize = 100;

fn bench_both_option(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let mut data = vec![];

    NROWS.iter().for_each(|n| {
        let mut val = vec![];
        for _i in 0..(n * NCOLS) {
            let v: u64 = rng.gen();
            if v % 2 == 0 {
                val.push(Some(v));
            } else {
                // val.push(None);
                val.push(Some(0));
            }
        }
        data.push(val);
    });

    let data = data.as_slice();

    c.bench_function("both option", |b| {
        b.iter(|| {
            let data = black_box(data);

            // schema for writer
            let mut writer = ArrowWriter::new();
            let dispatcher = Dispatcher::new(
                OptU64SourceBuilder::new(data.to_vec(), NCOLS),
                &mut writer,
                &NROWS.iter().map(|_| "").collect::<Vec<_>>(),
                &[DataType::U64(true); NCOLS],
            );
            dispatcher.run().expect("run dispatcher");
        })
    });
}

fn bench_source_option(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let mut data = vec![];

    NROWS.iter().for_each(|n| {
        let mut val = vec![];
        for _i in 0..(n * NCOLS) {
            let v: u64 = rng.gen();
            if v % 2 == 0 {
                val.push(Some(v));
            } else {
                // val.push(None);
                val.push(Some(0));
            }
        }
        data.push(val);
    });

    let data = data.as_slice();

    c.bench_function("source option", |b| {
        b.iter(|| {
            let data = black_box(data);

            // schema for writer
            let mut writer = ArrowWriter::new();
            let dispatcher = Dispatcher::new(
                OptU64SourceBuilder::new(data.to_vec(), NCOLS),
                &mut writer,
                &NROWS.iter().map(|_| "").collect::<Vec<_>>(),
                &[DataType::U64(false); NCOLS],
            );
            let _dw = dispatcher.run().expect("run dispatcher");
        })
    });
}

fn bench_writer_option(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let mut data = vec![];

    NROWS.iter().for_each(|n| {
        let mut val = vec![];
        for _i in 0..(n * NCOLS) {
            let v: u64 = rng.gen();
            if v % 2 == 0 {
                val.push(v);
            } else {
                val.push(0);
            }
        }
        data.push(val);
    });

    let data = data.as_slice();

    c.bench_function("write option", |b| {
        b.iter(|| {
            let data = black_box(data);

            // schema for writer
            let mut writer = ArrowWriter::new();
            let dispatcher = Dispatcher::new(
                U64SourceBuilder::new(data.to_vec(), NCOLS),
                &mut writer,
                &NROWS.iter().map(|_| "").collect::<Vec<_>>(),
                &[DataType::U64(true); NCOLS],
            );
            let _dw = dispatcher.run().expect("run dispatcher");
        })
    });
}

fn bench_non_option(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let mut data = vec![];

    NROWS.iter().for_each(|n| {
        let mut val = vec![];
        for _i in 0..(n * NCOLS) {
            let v: u64 = rng.gen();
            if v % 2 == 0 {
                val.push(v);
            } else {
                val.push(0);
            }
        }
        data.push(val);
    });

    let data = data.as_slice();

    c.bench_function("non option", |b| {
        b.iter(|| {
            let data = black_box(data);

            // schema for writer
            let mut writer = ArrowWriter::new();
            let dispatcher = Dispatcher::new(
                U64SourceBuilder::new(data.to_vec(), NCOLS),
                &mut writer,
                &NROWS.iter().map(|_| "").collect::<Vec<_>>(),
                &[DataType::U64(false); NCOLS],
            );
            let _dw = dispatcher.run().expect("run dispatcher");
        })
    });
}

criterion_group!(
    name=benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(120)).sample_size(30).with_profiler(profiler::FlamegraphProfiler::new(100));
    targets = bench_both_option, bench_source_option, bench_writer_option, bench_non_option
);
criterion_main!(benches);

pub struct U64SourceBuilder {
    fake_values: Vec<Vec<u64>>,
    ncols: usize,
}

impl U64SourceBuilder {
    pub fn new(fake_values: Vec<Vec<u64>>, ncols: usize) -> Self {
        U64SourceBuilder { fake_values, ncols }
    }
}

impl SourceBuilder for U64SourceBuilder {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type DataSource = U64TestSource;

    #[throws(ConnectorAgentError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorAgentError::UnsupportedDataOrder(data_order))
        }
    }

    fn build(&mut self) -> Self::DataSource {
        let ret = U64TestSource::new(self.fake_values.swap_remove(0), self.ncols);
        ret
    }
}

pub struct U64TestSource {
    counter: usize,
    vals: Vec<u64>,
    ncols: usize,
}

impl U64TestSource {
    pub fn new(vals: Vec<u64>, ncols: usize) -> Self {
        U64TestSource {
            counter: 0,
            vals: vals,
            ncols,
        }
    }
}

impl DataSource for U64TestSource {
    type TypeSystem = DataType;
    fn prepare(&mut self, _: &str) -> Result<()> {
        Ok(())
    }

    fn nrows(&self) -> usize {
        self.vals.len() / self.ncols
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

impl Produce<u64> for U64TestSource {
    fn produce(&mut self) -> Result<u64> {
        let v = self.vals[self.counter];
        self.counter += 1;
        Ok(v)
    }
}

impl Produce<Option<u64>> for U64TestSource {
    fn produce(&mut self) -> Result<Option<u64>> {
        let v = self.vals[self.counter];
        self.counter += 1;
        Ok(Some(v))
    }
}

impl Produce<f64> for U64TestSource {
    fn produce(&mut self) -> Result<f64> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<Option<f64>> for U64TestSource {
    fn produce(&mut self) -> Result<Option<f64>> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<bool> for U64TestSource {
    fn produce(&mut self) -> Result<bool> {
        throw!(anyhow!("Only Option<u64> is supported"));
    }
}

impl Produce<Option<bool>> for U64TestSource {
    fn produce(&mut self) -> Result<Option<bool>> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<String> for U64TestSource {
    fn produce(&mut self) -> Result<String> {
        throw!(anyhow!("Only Option<u64> is supported"));
    }
}

impl Produce<Option<String>> for U64TestSource {
    fn produce(&mut self) -> Result<Option<String>> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

pub struct OptU64SourceBuilder {
    fake_values: Vec<Vec<Option<u64>>>,
    ncols: usize,
}

impl OptU64SourceBuilder {
    pub fn new(fake_values: Vec<Vec<Option<u64>>>, ncols: usize) -> Self {
        OptU64SourceBuilder { fake_values, ncols }
    }
}

impl SourceBuilder for OptU64SourceBuilder {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::RowMajor];
    type DataSource = OptU64TestSource;

    #[throws(ConnectorAgentError)]
    fn set_data_order(&mut self, data_order: DataOrder) {
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(ConnectorAgentError::UnsupportedDataOrder(data_order))
        }
    }

    fn build(&mut self) -> Self::DataSource {
        let ret = OptU64TestSource::new(self.fake_values.swap_remove(0), self.ncols);
        ret
    }
}

pub struct OptU64TestSource {
    counter: usize,
    vals: Vec<Option<u64>>,
    ncols: usize,
}

impl OptU64TestSource {
    pub fn new(vals: Vec<Option<u64>>, ncols: usize) -> Self {
        OptU64TestSource {
            counter: 0,
            vals: vals,
            ncols,
        }
    }
}

impl DataSource for OptU64TestSource {
    type TypeSystem = DataType;
    fn prepare(&mut self, _: &str) -> Result<()> {
        Ok(())
    }

    fn nrows(&self) -> usize {
        self.vals.len() / self.ncols
    }

    fn ncols(&self) -> usize {
        self.ncols
    }
}

impl Produce<u64> for OptU64TestSource {
    fn produce(&mut self) -> Result<u64> {
        let v = match self.vals[self.counter] {
            Some(v) => v,
            None => 0,
        };
        self.counter += 1;
        Ok(v)
    }
}

impl Produce<Option<u64>> for OptU64TestSource {
    fn produce(&mut self) -> Result<Option<u64>> {
        let v = self.vals[self.counter];
        self.counter += 1;
        Ok(v)
    }
}

impl Produce<f64> for OptU64TestSource {
    fn produce(&mut self) -> Result<f64> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<Option<f64>> for OptU64TestSource {
    fn produce(&mut self) -> Result<Option<f64>> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<bool> for OptU64TestSource {
    fn produce(&mut self) -> Result<bool> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<Option<bool>> for OptU64TestSource {
    fn produce(&mut self) -> Result<Option<bool>> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<String> for OptU64TestSource {
    fn produce(&mut self) -> Result<String> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}

impl Produce<Option<String>> for OptU64TestSource {
    fn produce(&mut self) -> Result<Option<String>> {
        throw!(anyhow!("Only u64 and Option<u64> is supported"));
    }
}
