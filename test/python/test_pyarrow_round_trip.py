import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.ipc as ipc
import pytest

# One column per type the writer supports, NULL on every seventh row
COLUMNS = {
    "p": "i % 3",
    "b": "i % 2 = 0",
    "t1": "(i % 256 - 128)::TINYINT",
    "t2": "(i % 65536 - 32768)::SMALLINT",
    "t4": "i::INTEGER",
    "t8": "(i * 1000000007)::BIGINT",
    "u1": "(i % 256)::UTINYINT",
    "u2": "(i % 65536)::USMALLINT",
    "u4": "i::UINTEGER",
    "u8": "(i * 1000000007)::UBIGINT",
    "h": "(i * 1000000007)::HUGEINT * 1000000007",
    "uh": "(i * 1000000007)::UHUGEINT",
    "f": "(i / 7.0)::FLOAT",
    "d": "i / 7.0",
    "dec4": "((i % 1000) / 10.0)::DECIMAL(4,1)",
    "dec9": "(i / 100.0)::DECIMAL(9,2)",
    "dec18": "(i / 100.0)::DECIMAL(18,3)",
    "dec38": "i::DECIMAL(38,10)",
    "s": "CASE WHEN i % 5 = 0 THEN '' ELSE repeat('ü€', i % 100) || i END",
    "bl": "CASE WHEN i % 5 = 0 THEN ''::BLOB ELSE '\\x00\\xFF'::BLOB || encode('bytes_' || i) END",
    "dt": "DATE '2000-01-01' + i::INTEGER",
    "tm": "TIME '00:00:00' + to_microseconds(i * 1000003)",
    "tmns": "make_timestamp_ns(946684800000000000 + i * 1000000000 + i % 997)::TIME_NS",
    "tss": "(TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND)::TIMESTAMP_S",
    "ts": "TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND + to_microseconds(i % 997)",
    "tsms": "(TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND + to_milliseconds(i % 997))::TIMESTAMP_MS",
    "tsns": "make_timestamp_ns(946684800000000000 + i * 1000000000 + i % 997)",
    "tstzns": "(make_timestamp_ns(946684800000000000 + i * 1000000000 + i % 997)::VARCHAR || '+00')::TIMESTAMPTZ_NS",
    "tstz": "(TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND + to_microseconds(i % 997))::TIMESTAMPTZ",
    "iv": "INTERVAL (i % 13) MONTH + INTERVAL (i) DAY + to_microseconds(i)",
    "uu": "('00000000-0000-0000-0000-' || lpad(i::VARCHAR, 12, '0'))::UUID",
    "bn": "(CASE WHEN i % 2 = 0 THEN '-' ELSE '' END || '12' || repeat('9', i % 60))::BIGNUM",
    "g": "('POINT(' || i || ' ' || i || ')')::GEOMETRY",
    "l": "[i, NULL, i + 1]",
    "ll": "[[i], [], NULL]",
    "st": "{'a': i, 'b': 'x' || i}",
    "m": "MAP {i: 'v' || i}",
    "arr": "[i, i + 1, i + 2]",
    "ls": "[{'k': i}]",
    "un": "CASE WHEN i % 2 = 0 THEN union_value(a := i::INTEGER)::UNION(a INTEGER, b VARCHAR)"
    " ELSE union_value(b := 'v' || i)::UNION(a INTEGER, b VARCHAR) END",
}


def create_source(connection, rows):
    columns = []
    for name, expression in COLUMNS.items():
        nullable = f"CASE WHEN i % 7 = 3 THEN NULL ELSE {expression} END"
        if name == "arr":
            nullable = f"({nullable})::INTEGER[3]"
        if name == "p":
            nullable = expression
        columns.append(f"{nullable} AS {name}")
    connection.execute(f"CREATE OR REPLACE TABLE source AS SELECT i, {', '.join(columns)} FROM range({rows}) t(i)")
    return pa.table(connection.execute("SELECT * FROM source ORDER BY i").arrow())


def assert_same_table(table, expected):
    table.validate(full=True)
    table = table.sort_by("i")
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


def assert_footer_matches_stream(path, reader, row_group_size, rows):
    stream = ipc.open_stream(path.read_bytes()[8:])
    assert reader.schema.equals(stream.schema, check_metadata=True)
    batches = list(stream)
    assert reader.num_record_batches == len(batches)
    # The writer takes whole vectors of 2048 rows, so a smaller row group still holds one
    if rows > -(-row_group_size // 2048) * 2048:
        assert len(batches) > 1
    for index, batch in enumerate(batches):
        block = reader.get_batch(index)
        block.validate(full=True)
        assert block.equals(batch, check_metadata=True)


ROWS = [0, 1, 2049, 5000, 150000]
ROW_GROUP_SIZES = [1000, 122880]
COMPRESSIONS = ["uncompressed", "zstd"]


@pytest.mark.parametrize("compression", COMPRESSIONS)
@pytest.mark.parametrize("row_group_size", ROW_GROUP_SIZES)
@pytest.mark.parametrize("rows", ROWS)
def test_pyarrow_reads_file(connection, tmp_path, rows, row_group_size, compression):
    expected = create_source(connection, rows)
    path = tmp_path / "types.arrow"
    connection.execute(
        f"COPY source TO '{path}' (FORMAT arrow, ROW_GROUP_SIZE {row_group_size}, COMPRESSION '{compression}')"
    )
    reader = ipc.open_file(path)
    assert_same_table(reader.read_all(), expected)
    assert_footer_matches_stream(path, reader, row_group_size, rows)


@pytest.mark.parametrize("compression", COMPRESSIONS)
@pytest.mark.parametrize("row_group_size", ROW_GROUP_SIZES)
@pytest.mark.parametrize("rows", ROWS)
def test_pyarrow_reads_stream(connection, tmp_path, rows, row_group_size, compression):
    expected = create_source(connection, rows)
    path = tmp_path / "types.arrows"
    connection.execute(
        f"COPY source TO '{path}' (FORMAT arrows, ROW_GROUP_SIZE {row_group_size}, COMPRESSION '{compression}')"
    )
    assert_same_table(ipc.open_stream(path).read_all(), expected)


def test_pyarrow_reads_parallel_write(connection, tmp_path):
    expected = create_source(connection, 150000)
    connection.execute("SET threads=4")
    connection.execute("SET preserve_insertion_order=false")
    path = tmp_path / "parallel.arrow"
    connection.execute(f"COPY source TO '{path}' (FORMAT arrow, ROW_GROUP_SIZE 10000)")
    reader = ipc.open_file(path)
    assert_same_table(reader.read_all(), expected)
    assert_footer_matches_stream(path, reader, 10000, 150000)


def test_pyarrow_reads_large_buffers(connection, tmp_path):
    connection.execute("SET arrow_large_buffer_size=true")
    expected = create_source(connection, 5000)
    path = tmp_path / "large.arrow"
    connection.execute(f"COPY source TO '{path}' (FORMAT arrow)")
    table = ipc.open_file(path).read_all()
    assert table.schema.field("s").type == pa.large_string()
    assert_same_table(table, expected)


@pytest.mark.parametrize("format_name", ["arrow", "arrows"])
def test_pyarrow_reads_lossless_conversion(connection, tmp_path, format_name):
    connection.execute("SET arrow_lossless_conversion=true")
    expected = create_source(connection, 5000)
    path = tmp_path / f"lossless.{format_name}"
    connection.execute(f"COPY source TO '{path}' (FORMAT {format_name})")
    reader = ipc.open_file(path) if format_name == "arrow" else ipc.open_stream(path)
    table = reader.read_all()
    assert table.schema.field("uu").type.extension_name == "arrow.uuid"
    assert table.schema.field("h").type.storage_type == pa.binary(16)
    assert_same_table(table, expected)


def test_pyarrow_reads_many_files(connection, tmp_path):
    expected = create_source(connection, 20000)
    connection.execute(
        f"COPY source TO '{tmp_path / 'many'}' (FORMAT arrow, ROW_GROUP_SIZE 1000, ROW_GROUPS_PER_FILE 1)"
    )
    dataset = ds.dataset(tmp_path / "many", format="arrow")
    assert len(dataset.files) > 1
    assert_same_table(dataset.to_table(), expected)


def test_pyarrow_reads_partitions(connection, tmp_path):
    expected = create_source(connection, 5000)
    connection.execute(f"COPY source TO '{tmp_path / 'parts'}' (FORMAT arrow, PARTITION_BY (p))")
    dataset = ds.dataset(tmp_path / "parts", format="arrow", partitioning="hive")
    table = dataset.to_table()
    # The partition value is inferred from the path, so it is cast back to the column's type
    table = table.set_column(table.schema.get_field_index("p"), "p", table.column("p").cast(pa.int64()))
    assert_same_table(table.select(expected.column_names), expected)
