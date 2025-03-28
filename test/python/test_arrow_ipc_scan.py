import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc


def get_record_batch():
   data = [
          pa.array([1, 2, 3, 4]),
          pa.array(['foo', 'bar', 'baz', None]),
          pa.array([True, None, False, True])
      ]

   return pa.record_batch(data, names=['f0', 'f1', 'f2'])

def tables_match(result):
   assert result == [(1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True), (1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True)]

class TestArrowIPCBufferRead(object):
   def test_single_buffer(self, connection):
      batch = get_record_batch()
      sink = pa.BufferOutputStream()
      with pa.ipc.new_stream(sink, batch.schema) as writer:
         for i in range(5):
            writer.write_batch(batch)
      buffer = sink.getvalue()
      with pa.BufferReader(buffer) as buf_reader:
         msg_reader = ipc.MessageReader.open_stream(buf_reader)
         tables_match(connection.from_arrow(msg_reader).fetchall())

   def test_multi_buffers(self, connection):
      batch = get_record_batch()
      sink = pa.BufferOutputStream()

      with pa.ipc.new_stream(sink, batch.schema) as writer:
          for _ in range(5):  # Write 5 batches into one stream
              writer.write_batch(batch)

      buffer = sink.getvalue()

      with pa.BufferReader(buffer) as buf_reader:
         msg_reader = ipc.MessageReader.open_stream(buf_reader)
         tables_match(connection.from_arrow(msg_reader).fetchall())

   def test_replacement_scan(self, connection):

      batch = get_record_batch()
      sink = pa.BufferOutputStream()

      with pa.ipc.new_stream(sink, batch.schema) as writer:
         writer.write_batch(batch)

      buffer = sink.getvalue()

      with pa.BufferReader(buffer) as buf_reader:
         msg_reader = ipc.MessageReader.open_stream(buf_reader)
         with pytest.raises(duckdb.InvalidInputException,
                 match="not suitable for replacement scans",):
            connection.execute("FROM msg_reader")
