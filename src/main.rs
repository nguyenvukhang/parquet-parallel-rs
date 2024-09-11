use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow::array::{Array, StringArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;

use rayon::{prelude::*, ThreadPoolBuilder};

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug)]
enum TraceKind {
    Read,
    Write,
}

#[derive(Debug)]
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

fn main() {
    let data_dir = Path::new("/Users/khang/.local/data/msr-cambridge");
    let f = File::open(data_dir.join("proj_2.typed.parquet")).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let reader = builder.build().unwrap();

    let builder = ThreadPoolBuilder::new();
    let pool = builder.num_threads(4).build().unwrap();

    println!("Start!");
    let start = Instant::now();

    let record_batches: Vec<_> =
        reader.into_iter().map(|v| v.unwrap()).collect();

    let traces: Vec<Vec<Trace>> = pool.install(|| {
        record_batches
            .into_par_iter()
            .map(|rb| parse_record_batch(rb))
            .collect()
    });
    println!("Len: {}", traces.iter().map(|v| v.len()).sum::<usize>());
    println!("{:?}", Instant::elapsed(&start));

    let f = File::open(data_dir.join("proj_2.typed.parquet")).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let reader = builder.build().unwrap();

    println!("Start!");
    let start = Instant::now();

    let mut vec = vec![];
    for x in reader {
        vec.extend(parse_record_batch(x.unwrap()));
    }
    println!("Len: {}", traces.len());
    println!("{:?}", Instant::elapsed(&start));
}
