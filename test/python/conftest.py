import os
import pytest
import duckdb
from typing import Union, Optional
from duckdb import DuckDBPyConnection

dir = os.path.dirname(os.path.abspath(__file__))
build_type = "release"

@pytest.fixture(scope="function")
def duckdb_empty_cursor(request):
    connection = duckdb.connect('')
    cursor = connection.cursor()
    return cursor

def add_extension(extension_name, conn: Union[str, DuckDBPyConnection] = '') -> DuckDBPyConnection:
    if (isinstance(conn, str)):
        config = {
            'allow_unsigned_extensions' : 'true'
        }
        conn = duckdb.connect(conn or '', config=config)
    file_path = f"'{dir}/../../build/{build_type}/extension/{extension_name}/{extension_name}.duckdb_extension'"
    conn.execute(f"LOAD {file_path}")
    return conn

@pytest.fixture(scope="function")
def require():
    def _require(extension_name, db_name=''):
        conn = add_extension(extension_name, db_name)
        return conn

    return _require

@pytest.fixture(scope='function')
def connection():
	return add_extension('nanoarrow')
