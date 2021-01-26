use connector_agent::data_sources::dummy::U64CounterSource;
use connector_agent::data_sources::dummy::BoolCounterSource;
use connector_agent::writers::{dummy::U64Writer, dummy::BoolWriter, Writer};
use connector_agent::{DataType, Worker};
use ndarray::array;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

#[test]
#[should_panic]
fn wrong_data_type() {
    let _ = U64Writer::allocate(11, vec![DataType::U64, DataType::U64, DataType::U64, DataType::F64, DataType::U64]).unwrap();
}

#[test]
fn write_array() {
    let mut dw = U64Writer::allocate(11, vec![DataType::U64; 5]).unwrap();
    let schema = dw.schema().to_vec();
    let writers = dw.partition_writers(&[4, 7]);

    writers
        .into_par_iter()
        .for_each(|writer| Worker::new(U64CounterSource::new(), writer, schema.clone(), "").run_checked().expect("Worker failed"));

    assert_eq!(
        array![
            [0, 1, 2, 3, 4],
            [5, 6, 7, 8, 9],
            [10, 11, 12, 13, 14],
            [15, 16, 17, 18, 19],
            [0, 1, 2, 3, 4],
            [5, 6, 7, 8, 9],
            [10, 11, 12, 13, 14],
            [15, 16, 17, 18, 19],
            [20, 21, 22, 23, 24],
            [25, 26, 27, 28, 29],
            [30, 31, 32, 33, 34]
        ],
        dw.buffer()
    );
}

#[test]
fn write_array_bool() {
    let mut dw = BoolWriter::allocate(11, vec![DataType::Bool; 5]).unwrap();
    let schema = dw.schema().to_vec();
    let writers = dw.partition_writers(&[4, 7]);

    writers
        .into_par_iter()
        .for_each(|writer| Worker::new(BoolCounterSource::new(), writer, schema.clone(), "").run_checked().expect("Worker failed"));

    assert_eq!(
        array![
            [false, true, false, true, false],
            [true, false, true, false, true],
            [false, true, false, true, false],
            [true, false, true, false, true],
            [false, true, false, true, false],
            [true, false, true, false, true],
            [false, true, false, true, false],
            [true, false, true, false, true],
            [false, true, false, true, false],
            [true, false, true, false, true],
            [false, true, false, true, false],
        ],
        dw.buffer()
    );

}
