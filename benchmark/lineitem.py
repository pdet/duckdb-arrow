import pyarrow as pa
import duckdb
import time
import statistics
import sys
from decimal import Decimal
import concurrent.futures


def measure_execution_time(con, query, result = None, lineitem_arrow = None):
    times = []
    for _ in range(5):
        start = time.perf_counter()
        res = con.execute(query)
        end = time.perf_counter()
        times.append(end - start)
        if result:
            assert res.fetchall() == result

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
    print ("Read DuckDB - Native")
    queries = get_queries("lineitem")
    for query in queries:
        print(measure_execution_time(con,query,[(Decimal('123141078.2283'),)]))


def run_duckdb_arrow_array_stream(con):
    # Lets see how long it takes to run the queries in DuckDB
    print ("Generate PyArrow - Arrow Stream")
    times = []
    for _ in range(5):
        start = time.perf_counter()
        results = con.execute("FROM lineitem").fetch_record_batch()
        while True:
            try:
                # Process chunks
                results.read_next_batch()
            except StopIteration:
                break
        end = time.perf_counter()
        times.append(end - start)
    print(statistics.median(times))

    print ("Read PyArrow - Arrow Stream")
    queries = get_queries("record_batch_reader")
    arrow_table = con.execute("FROM lineitem").arrow()
    batches = arrow_table.to_batches(2048*120)
    for query in queries:
        times = []
        for _ in range(5):
            record_batch_reader = pa.RecordBatchReader.from_batches(arrow_table.schema, batches)
            start = time.perf_counter()
            res = con.execute(query)
            end = time.perf_counter()
            times.append(end - start)
            assert res.fetchall() == [(Decimal('123141078.2283'),)]
        print(statistics.median(times))

def run_arrow_ipc(con):
    print ("Read IPC Buffers")

    queries = get_queries("lineitem_arrow")
    times = []
    for _ in range(5):
        batches = con.execute("FROM lineitem").arrow().to_batches(2048*120)
        sink = pa.BufferOutputStream()

        with pa.ipc.new_stream(sink, batches[0].schema) as writer:
            for batch in batches:
                writer.write_batch(batch)

        buffer = sink.getvalue()
        with pa.BufferReader(buffer) as buf_reader:
            for query in queries:
                    msg_reader = pa.ipc.MessageReader.open_stream(buf_reader)
                    start = time.perf_counter()
                    lineitem_arrow = con.from_arrow(msg_reader)
                    res = con.execute(query)
                    end = time.perf_counter()
                    times.append(end - start)
                    assert res.fetchall() == [(Decimal('123141078.2283'),)]
    print(statistics.median(times))

    print("Generate IPC Buffers")
    print(measure_execution_time(con,"FROM to_arrow_ipc((FROM lineitem))"))

def run_pyarrow(con):
    # Lets see how long it takes to run the queries in DuckDB
    print ("DuckDB - Native")
    queries = get_queries("lineitem")
    for query in queries:
        print(query)
        print(measure_execution_time(con,query))

def run_parquet(con):
    print("Generate Parquet")
    print(measure_execution_time(con, "COPY lineitem TO 'lineitem.parquet'"))

    queries = get_queries("lineitem.parquet")
    print("Read From Parquet")
    print(measure_execution_time(con, queries[0], [(Decimal('123141078.2283'),)]))

def run_arrow_file(con):
    print("Generate ArrowIPC File")
    print(measure_execution_time(con, "COPY lineitem TO 'lineitem.arrows' (FORMAT arrows)"))

    queries = get_queries("lineitem.arrows")
    print("Read From ArrowIPC File")
    print(measure_execution_time(con, queries[0], [(Decimal('123141078.2283'),)]))

    # Use Arrow IPC to generate an ipc file
    table = con.execute("FROM lineitem").arrow()

    # Write the table to an IPC file (Arrow file format)
    print("Generate ArrowIPC File Pure")
    times = []
    for _ in range(5):
        options = pa.ipc.IpcWriteOptions(compression = 'zstd')
        with open("lineitem_ipc.arrow", "wb") as f:
            start = time.perf_counter()
            writer = pa.ipc.RecordBatchFileWriter(f, table.schema, options=options)
            writer.write_table(table)
            writer.close()
            end = time.perf_counter()
            times.append(end - start)
    print(statistics.median(times))
    queries = get_queries("lineitem_ipc.arrow")
    print("Read From ArrowIPC - Pure File")
    print(measure_execution_time(con, queries[0], [(Decimal('123141078.2283'),)]))


def create_con(path,sf):
    con = duckdb.connect(config={"allow_unsigned_extensions":"true"})
    con.execute(f"load '{path}'")
    con.execute(f"CALL dbgen(sf={sf});")
    return con

if len(sys.argv) < 2:
    print("Usage: lineitem.py <extension_lib_path>")
    sys.exit(1)

path = sys.argv[1]

def run_buffer_benchmark():
    con = create_con(path,1)
    run_duckdb_native(con)
    run_duckdb_arrow_array_stream(con)
    run_arrow_ipc(con)

def run_file_benchmark():
    con = create_con(path,1)
    run_parquet(con)
    run_arrow_file(con)

run_file_benchmark()
