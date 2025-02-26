import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc
from pyarrow.ipc import MessageReader as mr
import os
import sys

# "generated_decimal256.stream",
# "generated_duplicate_fieldnames.stream",
# ,"generated_interval.stream"
# ,"generated_null_trivial.stream"
# ,"generated_primitive_large_offsets.stream"
# ,"generated_union.stream"

# "0.17.1/generated_union.stream","0.14.1/generated_datetime.stream","0.14.1/generated_decimal.stream","0.14.1/generated_interval.stream","0.14.1/generated_map.stream","0.14.1/generated_nested.stream","0.14.1/generated_primitive.stream","0.14.1/generated_primitive_no_batches.stream","0.14.1/generated_primitive_zerolength.stream"

integration_files = ["generated_custom_metadata.stream","generated_datetime.stream","generated_decimal.stream","generated_map_non_canonical.stream","generated_map.stream","generated_nested_large_offsets.stream","generated_nested.stream","generated_null.stream","generated_primitive_no_batches.stream","generated_primitive_zerolength.stream","generated_primitive.stream","generated_recursive_nested.stream"]


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

def arrow_ipc_file_check(con,file):
    print(file)
    arrow_result = ipc.open_stream(file).read_all()
    duckdb_file_result = con.sql(f"FROM '{file}'").arrow()
    # Here we test our file scanner
    assert compare_result(arrow_result, duckdb_file_result, con)
    structs = get_buffer_struct(file)
    duckdb_struct_result = con.execute(f"FROM scan_arrow_ipc([{structs}])")
    # Here we test our IPC Buffer scanner
    assert compare_result(arrow_result, duckdb_struct_result, con)

class TestArrowIntegrationTests(object):
    def test_bigendian_integration(self, connection):
        big_endian_folder = os.path.join(test_folder,'1.0.0-bigendian')
        for file in integration_files:
            arrow_ipc_file_check(connection,os.path.join(big_endian_folder,file))




