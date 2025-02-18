import pyarrow as pa
import duckdb
import pyarrow.parquet as pq


con = duckdb.connect(config={"allow_unsigned_extensions":"true"})

# con.execute("load 'arrow'")

con.execute("load '/Users/holanda/Documents/Projects/duckdb-arrow/build/release/extension/nanoarrow/nanoarrow.duckdb_extension'")

data = [
    pa.array([1, 2, 3, 4]),
    pa.array(['foo', 'bar', 'baz', None]),
    pa.array([True, None, False, True])
]

batch = pa.record_batch(data, names=['f0', 'f1', 'f2'])

buffers = []

sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, batch.schema) as writer:
   for i in range(5):
      writer.write_batch(batch)

buffers.append(sink.getvalue())

structs = ''
for buffer in buffers:
    structs = structs + f"{{'ptr': {buffer.address}::UBIGINT, 'size': {buffer.size}::UBIGINT}},"

structs = structs[:-1]
print(structs)

arrow_scan_function = f"FROM scan_arrow_ipc([{structs}])"

print(con.execute(arrow_scan_function).fetchall())
