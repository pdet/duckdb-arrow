import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc
from pyarrow.ipc import MessageReader as mr
import os
import sys
import tempfile

# duckdb.duckdb.NotImplementedException: Not implemented Error: Unsupported Internal Arrow Type for Decimal d:37,5,256
# "generated_decimal256.stream",

# duckdb.duckdb.InternalException: INTERNAL Error: Field 'ints' refers to a duplicate column name in IPC file schema
# "generated_duplicate_fieldnames.stream",

# duckdb.duckdb.ConversionException: Conversion Error: Could not convert Interval to Microsecond
# "generated_interval.stream"

# Attempted to dereference shared_ptr that is NULL
# "generated_null_trivial.stream"

# Not implemented Error: Unsupported Internal Arrow Type: "d" Union
# "generated_union.stream"

# "0.17.1/generated_union.stream","0.14.1/generated_datetime.stream","0.14.1/generated_decimal.stream","0.14.1/generated_interval.stream","0.14.1/generated_map.stream","0.14.1/generated_nested.stream","0.14.1/generated_primitive.stream","0.14.1/generated_primitive_no_batches.stream","0.14.1/generated_primitive_zerolength.stream"

integration_files = ["generated_primitive_large_offsets.stream","generated_custom_metadata.stream","generated_datetime.stream","generated_decimal.stream","generated_map_non_canonical.stream","generated_map.stream","generated_nested_large_offsets.stream","generated_nested.stream","generated_null.stream","generated_primitive_no_batches.stream","generated_primitive_zerolength.stream","generated_primitive.stream","generated_recursive_nested.stream"]


script_path = os.path.dirname(os.path.abspath(__file__))

test_folder = os.path.join(script_path,'..','..','arrow-testing','data','arrow-ipc-stream','integration')

def compare_result(arrow_result,duckdb_result, con):
    return con.execute("""
    SELECT COUNT(*) = 0
    FROM (
        (SELECT * FROM arrow_result EXCEPT SELECT * FROM duckdb_result)
        UNION
        (SELECT * FROM duckdb_result EXCEPT SELECT * FROM arrow_result)
    ) """).fetchone()[0]

def get_buffer_struct(file):
    reader = mr.open_stream(file)
    structs = ''
    while True:
        try:
            message = reader.read_next_message()
            if message is None:
                break
            buffer = message.serialize()
            structs = structs + f"{{'ptr': {buffer.address}::UBIGINT, 'size': {buffer.size}::UBIGINT}},"
        except StopIteration:
            break
    return structs[:-1]

# 1. Compare result from reading the IPC file in Arrow, and in Duckdb
def compare_ipc_file_reader(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    duckdb_file_result = con.sql(f"FROM '{file}'").arrow()
    assert compare_result(arrow_result, duckdb_file_result, con)

# 2. Now test the writer, write it to a file from DuckDB, read it with arrow and compare
def compare_ipc_file_writer(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "arrow_duck.arrows")
        con.execute(f"COPY (FROM '{file}') TO '{file_path}'")
        duckdb_file_result = con.sql(f"FROM '{file}'").arrow()
        assert compare_result(arrow_result, duckdb_file_result, con)

# 3. Compare result from reading the IPC file in Arrow, and in Duckdb
def compare_ipc_buffer_reader(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    structs = get_buffer_struct(file)
    duckdb_struct_result = con.execute(f"FROM scan_arrow_ipc([{structs}])")
    assert compare_result(arrow_result, duckdb_struct_result, con)

# 4. Now test the DuckDB buffer writer, by reading it back with arrow and comparing
def compare_ipc_buffer_writer(con, file):
    arrow_result = ipc.open_stream(file).read_all()
    buffers = con.execute(f"FROM to_arrow_ipc((FROM '{file}'))").fetchall()
    arrow_buffers = []
    batches = []
    with pa.BufferReader(arrow_buffers[0]) as reader:
        stream_reader = ipc.RecordBatchStreamReader(reader)
        schema = stream_reader.schema
        batches.extend(stream_reader)
    duck_result = pa.Table.from_batches(batches, schema=schema)
    assert compare_result(arrow_result, duckdb_struct_result, con)


class TestArrowIntegrationTests(object):
    def test_bigendian_integration_file_reader(self, connection):
        big_endian_folder = os.path.join(test_folder,'1.0.0-bigendian')
        for file in integration_files:
            compare_ipc_file_reader(connection,os.path.join(big_endian_folder,file))

    def test_bigendian_integration_file_writer(self, connection):
        big_endian_folder = os.path.join(test_folder,'1.0.0-bigendian')
        for file in integration_files:
            compare_ipc_file_writer(connection,os.path.join(big_endian_folder,file))

    def test_littleendian_integration_file_reader(self, connection):
        little_endian_folder = os.path.join(test_folder,'1.0.0-littleendian')
        for file in integration_files:
            compare_ipc_file_reader(connection,os.path.join(little_endian_folder,file))

    def test_littleendian_integration_file_writer(self, connection):
        little_endian_folder = os.path.join(test_folder,'1.0.0-littleendian')
        for file in integration_files:
            compare_ipc_file_writer(connection,os.path.join(little_endian_folder,file))

    # def test_bigendian_integration_buffer_reader(self, connection):
    #     big_endian_folder = os.path.join(test_folder,'1.0.0-bigendian')
    #     for file in integration_files:
    #         compare_ipc_buffer_reader(connection,os.path.join(big_endian_folder,file))

    # def test_bigendian_integration_buffer_writer(self, connection):
    #     big_endian_folder = os.path.join(test_folder,'1.0.0-bigendian')
    #     for file in integration_files:
    #         compare_ipc_buffer_writer(connection,os.path.join(big_endian_folder,file))
