from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq


def read_pq(p: str) -> DataFrame:
    return pq.read_table(p).to_pandas()

def write_pq(df: DataFrame, p: str):
    pq.write_table(pa.Table.from_pandas(df), p)


df = read_pq("~/.local/data/msr-cambridge/hm_1.parquet")
df = read_pq("out.parquet")
print(df.dtypes)
df["Timestamp"] = df["Timestamp"].astype("UInt64")
df["DiskNumber"] = df["DiskNumber"].astype("UInt32")
df["Offset"] = df["Offset"].astype("UInt64")
df["Size"] = df["Size"].astype("UInt64")
df["ResponseTime"] = df["ResponseTime"].astype("UInt64")
print(df.dtypes)
print(df.max())
print(2**31)

write_pq(df, "out.parquet")
