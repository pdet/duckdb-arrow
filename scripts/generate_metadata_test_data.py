"""Generate the schema only IPC fixture read by test/sql/arrow_kv_metadata.test."""

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc


def main():
    metadata = {b'unit': b'count'}
    value_type = pa.struct([pa.field('child', pa.int64(), metadata=metadata)])
    encoded = pa.field(
        'encoded', pa.dictionary(pa.int8(), value_type), metadata={b'role': b'code'}
    )
    schema = pa.schema(
        [
            encoded,
            pa.field(
                'items',
                pa.dictionary(
                    pa.int8(), pa.list_(pa.field('item', pa.int64(), metadata=metadata))
                ),
            ),
            pa.field('nested', pa.struct([encoded])),
        ],
        metadata=pa.KeyValueMetadata([(b'\xff', b'\xfe'), (b'repeat', b'one'), (b'repeat', b'two')]),
    )
    path = Path(__file__).resolve().parents[1] / 'data' / 'metadata_dictionary.arrows'
    with ipc.new_stream(str(path), schema):
        pass


if __name__ == '__main__':
    main()
