import pyarrow as pa
import duckdb
import time
import statistics
import sys


def measure_execution_time(con, query):
    times = []
    for _ in range(5):
        start = time.perf_counter()
        con.execute(query)
        end = time.perf_counter()
        times.append(end - start)

    return statistics.median(times)

def get_queries(table_name):
    return [
        f"""SELECT
            sum(l_extendedprice * l_discount) AS revenue
        FROM
            {table_name}
        WHERE
            l_shipdate >= CAST('1994-01-01' AS date)
            AND l_shipdate < CAST('1995-01-01' AS date)
            AND l_discount BETWEEN 0.05
            AND 0.07
            AND l_quantity < 24;"""
            ]

def run_duckdb_native(con):
    # Lets see how long it takes to run the queries in DuckDB
    print ("DuckDB - Native")
    queries = get_queries("lineitem")
    for query in queries:
        print(measure_execution_time(con,query))

def run_duckdb_arrow_array_stream(con):
    # Lets see how long it takes to run the queries in DuckDB
    print ("PyArrow - Arrow Stream")
    queries = get_queries("record_batch_reader")
    arrow_table = con.execute("FROM lineitem").arrow()
    batches = arrow_table.to_batches(2048*120)
    for query in queries:
        times = []
        for _ in range(5):
            record_batch_reader = pa.RecordBatchReader.from_batches(arrow_table.schema, batches)
            start = time.perf_counter()
            con.execute(query)
            end = time.perf_counter()
            times.append(end - start)
        print(statistics.median(times))

def run_arrow_ipc(con):
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
    con.execute(view)

    # How long it takes to run the queries on
    queries = get_queries("lineitem_arrow")
    for query in queries:
        print(measure_execution_time(con,query))

def run_pyarrow(con):
    # Lets see how long it takes to run the queries in DuckDB
    print ("DuckDB - Native")
    queries = get_queries("lineitem")
    for query in queries:
        print(query)
        print(measure_execution_time(con,query))

def create_con(path,sf):
    con = duckdb.connect(config={"allow_unsigned_extensions":"true"})
    con.execute(f"load '{path}'")
    con.execute(f"CALL dbgen(sf={sf});")
    return con

if len(sys.argv) < 2:
    print("Usage: lineitem.py <extension_lib_path>")
    sys.exit(1)

path = sys.argv[1]

con = create_con(path,1)
run_duckdb_native(con)
run_duckdb_arrow_array_stream(con)
run_arrow_ipc(con)
