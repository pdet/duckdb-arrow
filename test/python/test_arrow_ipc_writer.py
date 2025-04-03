import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc

def create_table(connection):
	connection.execute("CREATE TABLE T (f0 integer, f1 varchar, f2 bool )")
	connection.execute("INSERT INTO T values (1, 'foo', true),(2, 'bar', NULL), (3, 'baz', false), (4, NULL, true) ")

def tables_match(result):
	print(result)
	assert result == [(1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True)]

class TestArrowIPCBufferWriter(object):
	def test_round_trip(self, connection):
		create_table(connection)
		buffers = connection.execute("FROM to_arrow_ipc((FROM T))").fetchall()
		buffer = pa.py_buffer(buffers[0][0] + buffers[1][0])
		with pa.BufferReader(buffer) as buf_reader:
			msg_reader = ipc.MessageReader.open_stream(buf_reader)
			tables_match(connection.from_arrow(msg_reader).fetchall())

	def test_arrow_read_duck_buffers(self, connection):
		create_table(connection)
		buffers = connection.execute("FROM to_arrow_ipc((FROM T))").fetchall()
		arrow_buffers = []
		# We have to concatenate the schema to the data
		arrow_buffers.append(pa.py_buffer(buffers[0][0] + buffers[1][0]))
		assert buffers[0][1] == True
		assert buffers[1][1] == False
		batches = []
		with pa.BufferReader(arrow_buffers[0]) as reader:
			stream_reader = ipc.RecordBatchStreamReader(reader)
			schema = stream_reader.schema
			batches.extend(stream_reader)
		arrow_table = pa.Table.from_batches(batches, schema=schema)
		tables_match(connection.execute("FROM arrow_table").fetchall())
