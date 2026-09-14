import struct

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest


@pytest.mark.parametrize(
    "suffix,format_option",
    [
        ("arrow", ""),
        ("arrows", ""),
        ("arrows", "FORMAT ARROW"),
        ("arrow", "FORMAT ARROWS"),
        ("ipc", "FORMAT ARROW"),
        ("ipc", "FORMAT ARROWS"),
    ],
)
def test_copy_aliases_write_files(connection, tmp_path, suffix, format_option):
    path = tmp_path / f"output.{suffix}"
    options = f"({format_option})" if format_option else ""
    connection.execute(f"COPY (SELECT 42 AS x) TO '{path}' {options}")

    payload = path.read_bytes()
    assert payload[:8] == b"ARROW1\0\0"
    assert payload[-6:] == b"ARROW1"
    reader = ipc.open_file(payload)
    assert reader.read_all().to_pydict() == {"x": [42]}


def test_buffer_output_remains_a_stream(connection, tmp_path):
    messages = connection.execute("FROM to_arrow_ipc((SELECT 42 AS x))").fetchall()
    assert [header for _, header in messages] == [True, False]
    payload = b"".join(message for message, _ in messages)
    assert payload[:4] == b"\xff\xff\xff\xff"
    assert ipc.open_stream(payload).read_all().to_pydict() == {"x": [42]}

    # The JS buffer-registration helper persists stream bytes and uses read_arrow.
    path = tmp_path / "registered.ipc"
    path.write_bytes(payload)
    assert connection.execute(f"FROM read_arrow('{path}')").fetchall() == [(42,)]


@pytest.mark.parametrize("preserve_order", [True, False])
def test_file_footer_random_access(connection, tmp_path, preserve_order):
    path = tmp_path / "batches.arrow"
    connection.execute("SET threads=4")
    connection.execute(f"SET preserve_insertion_order={str(preserve_order).lower()}")
    connection.execute(
        f"""
        COPY (
            SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i::VARCHAR END AS s
            FROM range(100000) t(i)
        ) TO '{path}' (FORMAT ARROW, ROW_GROUP_SIZE 2048,
                      KV_METADATA {{'source': 'file-format-test'}})
        """
    )

    payload = path.read_bytes()
    file_reader = ipc.open_file(payload)
    # Reading the embedded stream independently catches a footer that points to
    # the wrong batches or contains a different schema from the opening message.
    stream_reader = ipc.open_stream(payload[8:])
    assert file_reader.schema.equals(stream_reader.schema, check_metadata=True)
    assert file_reader.schema.metadata[b"source"] == b"file-format-test"
    batches = list(stream_reader)
    assert file_reader.num_record_batches == len(batches) > 1
    for i in reversed(range(len(batches))):
        batch = file_reader.get_batch(i)
        batch.validate(full=True)
        assert batch.equals(batches[i], check_metadata=True)

    result = file_reader.read_all().sort_by("i")
    assert result.column("i").to_pylist() == list(range(100000))
    assert result.column("s").to_pylist() == [
        None if i % 7 == 0 else str(i) for i in range(100000)
    ]

    # The trailer stores a little-endian footer length. Inspect the FlatBuffer's
    # version field as well as each embedded Message's version.
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


def test_file_footer_without_batches(connection, tmp_path):
    path = tmp_path / "empty.arrow"
    connection.execute(f"COPY (SELECT 42 AS x WHERE false) TO '{path}'")
    reader = ipc.open_file(path)
    assert reader.num_record_batches == 0
    assert reader.schema == pa.schema([("x", pa.int32())])
    assert reader.read_all().num_rows == 0


def test_rotated_files_have_independent_footers(connection, tmp_path):
    path = tmp_path / "parts"
    connection.execute("SET threads=4")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute(
        f"""
        COPY (SELECT i FROM range(100000) t(i)) TO '{path}'
        (FORMAT ARROW, ROW_GROUP_SIZE 2048, ROW_GROUPS_PER_FILE 3)
        """
    )
    files = list(path.glob("*.arrow"))
    assert len(files) > 1
    values = []
    for file in files:
        reader = ipc.open_file(file)
        table = reader.read_all()
        table.validate(full=True)
        values.extend(table.column("i").to_pylist())
    assert sorted(values) == list(range(100000))
