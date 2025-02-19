import pytest
import pyarrow as pa
import duckdb
import pyarrow.parquet as pq


class TestArrowIPCBufferRead(object):
   def test_single_batch(self, connection):
      data = [
          pa.array([1, 2, 3, 4]),
          pa.array(['foo', 'bar', 'baz', None]),
          pa.array([True, None, False, True])
      ]

      batch = pa.record_batch(data, names=['f0', 'f1', 'f2'])

      buffers = []

      sink = pa.BufferOutputStream()
      with pa.ipc.new_stream(sink, batch.schema) as writer:
         for i in range(5):
            writer.write_batch(batch)

      buffers.append(sink.getvalue())

      structs = ''
      for buffer in buffers:
          structs = structs + f"{{'ptr': {buffer.address}::UBIGINT, 'size': {buffer.size}::UBIGINT}},"

      structs = structs[:-1]
      print(structs)

      arrow_scan_function = f"FROM scan_arrow_ipc([{structs}])"

      connection.execute(arrow_scan_function).fetchall() == [(1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True)]
