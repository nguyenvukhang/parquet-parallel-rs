use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow::array::{Array, StringArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;

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

    let start = Instant::now();
    for x in reader {
        let rb = x.unwrap();
        let traces = parse_record_batch(rb);
    }
    println!("{:?}", Instant::elapsed(&start));

    // let mut record_batch = reader.next().unwrap().unwrap();
    // println!("record_batch: {:?}", record_batch.schema());
    // let x = record_batch.remove_column(4);
    // let addrs = x.as_any().downcast_ref::<UInt64Array>().unwrap();
    // for col in record_batch.columns() {
    //     // println!("{:?}", col);
    // }
    // // println!("{:?}", record_batch);
}
