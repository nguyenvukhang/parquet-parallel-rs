use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow::array::{Array, StringArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;

use rayon::{prelude::*, ThreadPoolBuilder};

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
enum TraceKind {
    Read,
    Write,
}
impl Default for TraceKind {
    fn default() -> Self {
        Self::Read
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Trace {
    timestamp: u64,
    kind: TraceKind,
    addr: u64,
    size: u32,
}

fn parse_record_batch(mut rb: RecordBatch) -> Vec<Trace> {
    let x = rb.remove_column(5);
    let sizes = x.as_any().downcast_ref::<UInt32Array>().unwrap();
    let x = rb.remove_column(4);
    let addrs = x.as_any().downcast_ref::<UInt64Array>().unwrap();
    let x = rb.remove_column(3);
    let kinds = x.as_any().downcast_ref::<StringArray>().unwrap();
    let x = rb.remove_column(0);
    let timestamps = x.as_any().downcast_ref::<UInt64Array>().unwrap();
    let iter = timestamps.iter().zip(kinds).zip(addrs).zip(sizes);
    let mut vec = Vec::with_capacity(timestamps.len());
    for (((t, kind), addr), size) in iter {
        let t = t.unwrap();
        let addr = addr.unwrap();
        let size = size.unwrap();
        let kind = match kind.unwrap().as_bytes()[0] {
            b'R' => TraceKind::Read,
            b'W' => TraceKind::Write,
            v => panic!("Invalid trace kind: {v}"),
        };
        vec.push(Trace { timestamp: t, kind, addr, size });
    }
    vec
}

const INDEX: usize = 1258900;

fn main() {
    let data_dir = Path::new("/Users/khang/.local/data/msr-cambridge");
    let f = File::open(data_dir.join("proj_2.typed.parquet")).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let nrows = builder.metadata().file_metadata().num_rows() as usize;
    let mut traces = Vec::with_capacity(nrows);
    traces.resize(nrows, Trace::default());
    let chunk_size = 0x1000;
    let reader = builder.with_batch_size(chunk_size).build().unwrap();

    let builder = ThreadPoolBuilder::new();
    let pool = builder.num_threads(4).build().unwrap();

    println!("Start!");
    let start = Instant::now();

    pool.install(|| {
        let iter = traces.chunks_mut(chunk_size).zip(reader);
        iter.par_bridge().into_par_iter().for_each(|(trace, rb)| {
            let mut rb = rb.unwrap();
            let x = rb.remove_column(5);
            let sizes = x.as_any().downcast_ref::<UInt32Array>().unwrap();
            let x = rb.remove_column(4);
            let addrs = x.as_any().downcast_ref::<UInt64Array>().unwrap();
            let x = rb.remove_column(3);
            let kinds = x.as_any().downcast_ref::<StringArray>().unwrap();
            let x = rb.remove_column(0);
            let timestamps = x.as_any().downcast_ref::<UInt64Array>().unwrap();
            let iter = timestamps.iter().zip(kinds).zip(addrs).zip(sizes);

            for (j, (((t, kind), addr), size)) in iter.enumerate() {
                let t = t.unwrap();
                let addr = addr.unwrap();
                let size = size.unwrap();
                let kind = match kind.unwrap().as_bytes()[0] {
                    b'R' => TraceKind::Read,
                    b'W' => TraceKind::Write,
                    v => panic!("Invalid trace kind: {v}"),
                };
                trace[j] = Trace { timestamp: t, kind, addr, size };
            }
        });
    });

    println!("Len: {}", traces.len());
    println!("nth: {:?}", traces[INDEX]);
    println!("{:?}", Instant::elapsed(&start));

    let f = File::open(data_dir.join("proj_2.typed.parquet")).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let reader = builder.with_batch_size(chunk_size).build().unwrap();

    println!("Start!");
    let start = Instant::now();

    let mut traces2 = vec![];
    for x in reader {
        traces2.extend(parse_record_batch(x.unwrap()));
    }
    println!("Len: {}", traces2.len());
    println!("nth: {:?}", traces2[INDEX]);
    println!("{:?}", Instant::elapsed(&start));

    for i in 0..traces2.len() {
        assert_eq!(traces[i], traces2[i]);
    }
}
