import os
import struct

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest


def assert_size_metadata(payload):
    schema = ipc.open_file(payload).schema
    assert schema.equals(ipc.open_stream(payload[8:]).schema, check_metadata=True)

    def field(metadata, table, index):
        vtable = table - struct.unpack_from("<i", metadata, table)[0]
        entry = 4 + 2 * index
        if entry >= struct.unpack_from("<H", metadata, vtable)[0]:
            return 0
        offset = struct.unpack_from("<H", metadata, vtable + entry)[0]
        return table + offset if offset else 0

    compressed_size = uncompressed_size = 0
    for message in ipc.MessageReader.open_stream(payload[8:]):
        if message.type != "record batch":
            continue
        compressed_size += message.body.size
        metadata = message.metadata
        root = struct.unpack_from("<I", metadata)[0]
        header = field(metadata, root, 2)
        batch = header + struct.unpack_from("<I", metadata, header)[0]
        compression = field(metadata, batch, 3)
        if not compression:
            uncompressed_size += message.body.size
            continue
        compression += struct.unpack_from("<I", metadata, compression)[0]
        codec_field = field(metadata, compression, 0)
        codec = "zstd" if codec_field and metadata[codec_field] == 1 else "lz4"
        buffers = field(metadata, batch, 2)
        buffers += struct.unpack_from("<I", metadata, buffers)[0]
        for i in range(struct.unpack_from("<I", metadata, buffers)[0]):
            offset, length = struct.unpack_from("<qq", metadata, buffers + 4 + 16 * i)
            if length == 0:
                continue
            size = struct.unpack_from("<q", message.body, offset)[0]
            if size == -1:
                size = length - 8
            else:
                data = message.body.slice(offset + 8, length - 8)
                assert pa.decompress(data, decompressed_size=size, codec=codec).size == size
            uncompressed_size += (size + 7) // 8 * 8
    assert schema.metadata[b"total_compressed_size"] == str(compressed_size).encode()
    assert schema.metadata[b"total_uncompressed_size"] == str(uncompressed_size).encode()
    return compressed_size, uncompressed_size


@pytest.fixture
def source_row_count(connection):
    row_count = 500000
    # Use a table scan so COPY can run on multiple workers.
    connection.execute(f"CREATE TABLE source AS SELECT i FROM range({row_count}) t(i)")
    return row_count


@pytest.mark.parametrize(
    "suffix,format_option,file_format",
    [
        ("arrow", "", True),
        ("arrows", "", False),
        ("arrows", "FORMAT ARROW", True),
        ("arrow", "FORMAT ARROWS", False),
    ],
)
def test_copy_aliases_pick_framing(connection, tmp_path, suffix, format_option, file_format):
    path = tmp_path / f"output.{suffix}"
    options = f"({format_option})" if format_option else ""
    connection.execute(f"COPY (SELECT 42 AS x) TO '{path}' {options}")

    payload = path.read_bytes()
    if file_format:
        assert payload[:8] == b"ARROW1\0\0"
        assert payload[-6:] == b"ARROW1"
        reader = ipc.open_file(payload)
    else:
        assert payload[:4] == b"\xff\xff\xff\xff"
        reader = ipc.open_stream(payload)
    assert not reader.schema.metadata
    assert reader.read_all().to_pydict() == {"x": [42]}


def test_buffer_output_remains_a_stream(connection, tmp_path):
    messages = connection.execute("FROM to_arrow_ipc((SELECT 42 AS x))").fetchall()
    assert [header for _, header in messages] == [True, False]
    payload = b"".join(message for message, _ in messages)
    assert payload[:4] == b"\xff\xff\xff\xff"
    assert ipc.open_stream(payload).read_all().to_pydict() == {"x": [42]}

    # Match the JS helper that saves stream bytes before calling read_arrow.
    path = tmp_path / "registered.ipc"
    path.write_bytes(payload)
    assert connection.execute(f"FROM read_arrow('{path}')").fetchall() == [(42,)]


@pytest.mark.parametrize("output", ["copy", "buffer"])
@pytest.mark.parametrize("compression", ["uncompressed", "zstd"])
def test_prepared_output_keeps_bound_settings(connection, tmp_path, output, compression):
    path = tmp_path / "prepared.arrow"
    source = "SELECT 'abcdefghijklmno'::BLOB AS b"
    connection.execute("SET arrow_output_version='1.3'")
    query = (
        f"COPY ({source}) TO '{path}' (COMPRESSION '{compression}')"
        if output == "copy"
        else f"FROM to_arrow_ipc(({source}), compression := '{compression}')"
    )
    connection.execute(f"PREPARE prepared_ipc AS {query}")
    connection.execute("SET arrow_output_version='1.4'")
    messages = connection.execute("EXECUTE prepared_ipc").fetchall()
    reader = (
        ipc.open_file(path)
        if output == "copy"
        else ipc.open_stream(b"".join(message for message, _ in messages))
    )
    assert reader.schema == pa.schema([("b", pa.binary())])
    assert reader.read_all().to_pydict() == {"b": [b"abcdefghijklmno"]}
    assert connection.execute("SELECT 42").fetchone() == (42,)


@pytest.mark.parametrize("preserve_order", [True, False])
@pytest.mark.parametrize("size_metadata", [True, False])
@pytest.mark.parametrize("compression", ["uncompressed", "zstd", "lz4"])
def test_file_footer_random_access(connection, tmp_path, preserve_order, source_row_count, size_metadata, compression):
    path = tmp_path / "batches.arrow"
    connection.execute("SET threads=4")
    connection.execute(f"SET preserve_insertion_order={str(preserve_order).lower()}")
    connection.execute(
        f"""
        COPY (
            SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i::VARCHAR END AS s
            FROM source
        ) TO '{path}' (FORMAT ARROW, ROW_GROUP_SIZE 2048, COMPRESSION '{compression}',
                      SIZE_METADATA {size_metadata},
                      KV_METADATA {{'source': 'file-format-test'}},
                      FIELD_METADATA {{'i': {{'unit': 'count'}}}})
        """
    )

    payload = path.read_bytes()
    file_reader = ipc.open_file(payload)
    # Compare indexed reads against the embedded stream.
    stream_reader = ipc.open_stream(payload[8:])
    assert file_reader.schema.equals(stream_reader.schema, check_metadata=True)
    assert file_reader.schema.metadata[b"source"] == b"file-format-test"
    if size_metadata:
        compressed_size, uncompressed_size = assert_size_metadata(payload)
        if compression != "uncompressed":
            assert compressed_size < uncompressed_size
    else:
        assert file_reader.schema.metadata == {b"source": b"file-format-test"}
    assert file_reader.schema.field("i").metadata[b"unit"] == b"count"
    batches = list(stream_reader)
    assert file_reader.num_record_batches == len(batches) > 1
    for i in reversed(range(len(batches))):
        batch = file_reader.get_batch(i)
        batch.validate(full=True)
        assert batch.equals(batches[i], check_metadata=True)

    result = file_reader.read_all()
    if not preserve_order:
        result = result.sort_by("i")
    assert result.column("i").to_pylist() == list(range(source_row_count))
    assert result.column("s").to_pylist() == [
        None if i % 7 == 0 else str(i) for i in range(source_row_count)
    ]

    # Check the footer version independently of the message versions.
    footer_size = struct.unpack_from("<i", payload, len(payload) - 10)[0]
    assert 0 < footer_size < len(payload) - 18
    footer = payload[-10 - footer_size:-10]
    root = struct.unpack_from("<I", footer, 0)[0]
    vtable = root - struct.unpack_from("<i", footer, root)[0]
    version_offset = struct.unpack_from("<H", footer, vtable + 4)[0]
    assert version_offset != 0
    assert struct.unpack_from("<h", footer, root + version_offset)[0] == pa.MetadataVersion.V5
    for message in ipc.MessageReader.open_stream(payload[8:]):
        assert message.metadata_version == pa.MetadataVersion.V5


@pytest.mark.parametrize("rows", [0, 1])
@pytest.mark.parametrize("compression", ["uncompressed", "zstd", "lz4"])
def test_small_file_with_size_metadata(connection, tmp_path, rows, compression):
    path = tmp_path / "small.arrow"
    connection.execute(f"COPY (SELECT 42 AS x WHERE {bool(rows)}) TO '{path}' (SIZE_METADATA, COMPRESSION '{compression}')")
    compressed_size, uncompressed_size = assert_size_metadata(path.read_bytes())
    reader = ipc.open_file(path)
    assert reader.num_record_batches == rows
    assert reader.schema == pa.schema([("x", pa.int32())])
    assert reader.read_all().to_pydict() == {"x": [42] * rows}
    if rows and compression != "uncompressed":
        assert compressed_size > uncompressed_size


@pytest.mark.parametrize("preserve_order", [True, False])
@pytest.mark.parametrize("compression", ["uncompressed", "zstd"])
def test_nested_columns_across_batches(connection, tmp_path, preserve_order, source_row_count, compression):
    path = tmp_path / "nested.arrow"
    connection.execute("SET threads=4")
    connection.execute(f"SET preserve_insertion_order={str(preserve_order).lower()}")
    source = """
        SELECT i,
               CASE WHEN i % 7 = 0 THEN NULL ELSE [i, NULL, i + 1] END AS l,
               {'number': i, 'text': i::VARCHAR} AS s,
               [i, i + 1]::BIGINT[2] AS a,
               map(['key'], [i]) AS m,
               CASE WHEN i % 2 = 0
                    THEN union_value(n := i)::UNION(n BIGINT, s VARCHAR)
                    ELSE union_value(s := i::VARCHAR)::UNION(n BIGINT, s VARCHAR) END AS u
        FROM source
    """
    expected = connection.execute(source).to_arrow_table().sort_by("i")
    connection.execute(
        f"COPY ({source}) TO '{path}' (FORMAT ARROW, ROW_GROUP_SIZE 2048, "
        f"SIZE_METADATA, COMPRESSION '{compression}')"
    )
    assert_size_metadata(path.read_bytes())
    reader = ipc.open_file(path)
    assert reader.num_record_batches > 1
    result = reader.read_all()
    result.validate(full=True)
    if not preserve_order:
        result = result.sort_by("i")
    assert result.replace_schema_metadata(None).equals(expected, check_metadata=True)


@pytest.mark.parametrize("format_name", ["arrow", "arrows"])
@pytest.mark.parametrize("compression", ["uncompressed", "zstd"])
def test_rotated_files_are_complete(connection, tmp_path, source_row_count, format_name, compression):
    path = tmp_path / "parts"
    connection.execute("SET threads=4")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute(
        f"""
        COPY source TO '{path}'
        (FORMAT {format_name}, ROW_GROUP_SIZE 2048, ROW_GROUPS_PER_FILE 3,
         SIZE_METADATA {format_name == 'arrow'}, COMPRESSION '{compression}')
        """
    )
    files = list(path.glob(f"*.{format_name}"))
    assert len(files) > 1
    values = []
    for file in files:
        if format_name == "arrow":
            assert_size_metadata(file.read_bytes())
        reader = ipc.open_file(file) if format_name == "arrow" else ipc.open_stream(file)
        table = reader.read_all()
        table.validate(full=True)
        values.extend(table.column("i").to_pylist())
    assert sorted(values) == list(range(source_row_count))


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="Named pipes require POSIX")
def test_size_metadata_requires_seekable_output(connection, tmp_path):
    path = tmp_path / "output.arrow"
    os.mkfifo(path)
    read_fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        with pytest.raises(duckdb.IOException, match="SIZE_METADATA requires a seekable local output"):
            connection.execute(
                f"COPY (SELECT 42 AS i) TO '{path}' (SIZE_METADATA, USE_TMP_FILE false)"
            )
    finally:
        os.close(read_fd)
