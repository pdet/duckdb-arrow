import pyarrow as pa
import duckdb
import time
import statistics
import ctypes


def measure_execution_time(con, query):
    times = []
    for _ in range(5):
        start = time.perf_counter()
        con.execute(query)
        end = time.perf_counter()
        times.append(end - start)
    
    return statistics.median(times)

con = duckdb.connect(config={"allow_unsigned_extensions":"true"})
con.execute("install 'arrow'")
con.execute("load 'arrow'")

# con.execute("load '/Users/holanda/Documents/Projects/duckdb-arrow/build/release/extension/nanoarrow/nanoarrow.duckdb_extension'")

def get_queries(table_name):
    return [
        f"SELECT count(*) from {table_name} LIMIT 10",
        f"SELECT sum(l_orderkey) as sum_orderkey FROM {table_name}",
        f"SELECT l_orderkey from {table_name} WHERE l_orderkey=2 LIMIT 2",
        f"SELECT l_extendedprice from {table_name}",
        f"SELECT l_extendedprice from {table_name} WHERE l_extendedprice > 53468 and l_extendedprice < 53469  LIMIT 2",
        f"SELECT count(l_orderkey) from {table_name} where l_commitdate > '1996-10-28'",
        f"SELECT sum(l_extendedprice * l_discount) AS revenue FROM {table_name} WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
    ]

con.execute(f"CALL dbgen(sf=1);")

# Lets see how long it takes to run the queries in DuckDB
print ("DuckDB - Native")
queries = get_queries("lineitem")
for query in queries:
    print(query)
    print(measure_execution_time(con,query))

# How long it takes to produce the buffers
print("Generate IPC Buffers")
print(measure_execution_time(con,"FROM to_arrow_ipc((FROM lineitem))"))
buffers = con.execute("FROM to_arrow_ipc((FROM lineitem))").fetchall()
arrow_buffers = []
for buffer in buffers:
    arrow_buffers.append(pa.py_buffer(buffer[0]))
structs = ''
for buffer in arrow_buffers:
    structs = structs + f"{{'ptr': {buffer.address}::UBIGINT, 'size': {buffer.size}::UBIGINT}},"

structs = structs[:-1]
view = f"CREATE OR REPLACE TEMPORARY VIEW lineitem_arrow AS FROM scan_arrow_ipc([{structs}])"
print(view)
con.execute(view)

# How long it takes to run the queries on
queries = get_queries("lineitem_arrow")
for query in queries:
    print(query)
    print(measure_execution_time(con,query))




