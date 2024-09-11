use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{reader::RowIter, Row, RowAccessor};

use arrow::array::{AsArray, GenericListArray};
use arrow_array::UInt64Array;

use std::fs::File;
use std::path::{Path, PathBuf};

fn main() {
    let data_dir = Path::new("/Users/khang/.local/data/msr-cambridge");
    let f = File::open(data_dir.join("hm_1.typed.parquet")).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let mut reader = builder.build().unwrap();

    let mut record_batch = reader.next().unwrap().unwrap();
    println!("record_batch: {:?}", record_batch.schema());
    let x = record_batch.remove_column(4);
    let offset_col = x.as_any().downcast_ref::<UInt64Array>().unwrap();
    for x in offset_col {
        println!("{x:?}");
    }
    for col in record_batch.columns() {
        // println!("{:?}", col);
    }
    // println!("{:?}", record_batch);
}
