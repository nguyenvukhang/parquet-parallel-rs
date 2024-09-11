from pandas import DataFrame
import pyarrow as pa, os
import pyarrow.parquet as pq


def read_pq(p: str) -> DataFrame:
    return pq.read_table(p).to_pandas()


def write_pq(df: DataFrame, p: str):
    pq.write_table(pa.Table.from_pandas(df), p)


def migrate(src: str):
    df = read_pq(src)
    print("BEFORE", df.dtypes)
    df["Timestamp"] = df["Timestamp"].astype("UInt64")
    df["DiskNumber"] = df["DiskNumber"].astype("UInt32")
    df["Offset"] = df["Offset"].astype("UInt64")
    df["Size"] = df["Size"].astype("UInt32")
    df["ResponseTime"] = df["ResponseTime"].astype("UInt64")
    print("AFTER", df.dtypes)

    write_pq(df, src.replace(".parquet", ".typed.parquet"))


data_dir = "/Users/khang/.local/data/msr-cambridge"
for x in os.listdir(data_dir):
    if not (x.endswith("parquet") and "typed" not in x):
        continue
    src = os.path.join(data_dir, x)
    migrate(src)
