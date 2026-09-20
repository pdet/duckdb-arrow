import os
import threading

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest


def stream_bytes(batches):
    sink = pa.BufferOutputStream()
    schema = pa.schema([('id', pa.int32())])
    with ipc.new_stream(sink, schema) as writer:
        for _ in range(batches):
            writer.write_batch(pa.record_batch([pa.array(range(2048), pa.int32())], schema=schema))
    return sink.getvalue().to_pybytes()


def read_through_pipe(connection, path, data):
    os.mkfifo(path)

    def feed():
        try:
            with open(path, 'wb') as pipe:
                pipe.write(data)
        except BrokenPipeError:
            pass

    writer = threading.Thread(target=feed)
    writer.start()
    try:
        return connection.execute(f"SELECT count(*), sum(id) FROM read_arrow('{path}')").fetchone()
    finally:
        writer.join()


@pytest.mark.skipif(not hasattr(os, 'mkfifo'), reason='a pipe needs mkfifo')
class TestArrowIPCPipe(object):
    def test_whole_stream(self, require, tmp_path):
        connection = require('nanoarrow')
        rows = read_through_pipe(connection, str(tmp_path / 'whole.arrows'), stream_bytes(2))
        assert rows == (4096, 2 * 2047 * 1024)

    def test_cut_inside_prefix(self, require, tmp_path):
        connection = require('nanoarrow')
        one = stream_bytes(1)
        two = stream_bytes(2)
        # the second stream starts with the first one minus its end of stream marker
        assert two[: len(one) - 8] == one[:-8]
        with pytest.raises(duckdb.Error, match='ends inside a message prefix'):
            read_through_pipe(connection, str(tmp_path / 'cut.arrows'), two[: len(one) - 8 + 4])
