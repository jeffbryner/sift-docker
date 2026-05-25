#!/usr/bin/env python3

import sys
import pyarrow as pa
import pyarrow.parquet as pq
from flow.record import RecordReader


def get_pyarrow_type(field_type):
    mapping = {
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "varint": pa.int64(),
        "long": pa.int64(),
        "filesize": pa.int64(),
        "unix_file_mode": pa.int64(),
        "float": pa.float64(),
        "string": pa.string(),
        "wstring": pa.string(),
        "uri": pa.string(),
        "bytes": pa.binary(),
        "digest": pa.binary(),
        "datetime": pa.timestamp("us", tz="UTC"),
    }
    return mapping.get(
        field_type, pa.string()
    )  # default to string for unknown like path


def main():
    if len(sys.argv) < 2:
        print("Usage: target-query... | rdump_to_parquet.py <output.parquet>")
        sys.exit(1)

    output_path = sys.argv[1]

    # We use RecordReader to read from standard input
    # If the input is piped, it will read from stdin.buffer
    try:
        reader = RecordReader(fileobj=sys.stdin.buffer)
    except Exception as e:
        print(f"Error opening RecordReader: {e}")
        sys.exit(1)

    schema = None
    writer = None
    buffer = []
    batch_size = 10000

    def flush_buffer():
        nonlocal buffer
        if not buffer:
            return

        columns = {field.name: [] for field in schema}
        for data in buffer:
            for field in schema:
                val = data.get(field.name)
                # Convert tuples (like flow-record paths) or complex structures to strings
                if isinstance(val, (tuple, dict, list)):
                    val = str(val) if not isinstance(val, tuple) else str(val[0])
                columns[field.name].append(val)

        batch = pa.RecordBatch.from_pydict(columns, schema=schema)
        writer.write_batch(batch)
        buffer.clear()

    count = 0
    try:
        for record in reader:
            if not schema:
                # Generate schema from the first record
                desc = record._desc
                fields = []
                for rf in desc.get_all_fields().values():
                    pa_type = get_pyarrow_type(rf.typename)
                    fields.append(pa.field(rf.name, pa_type, nullable=True))
                schema = pa.schema(fields)
                writer = pq.ParquetWriter(output_path, schema)

            buffer.append(record._packdict())
            count += 1

            if len(buffer) >= batch_size:
                flush_buffer()
    except Exception as e:
        print(f"Error during record processing: {e}")
        if writer:
            writer.close()
        sys.exit(1)

    flush_buffer()
    if writer:
        writer.close()

    if count > 0:
        print(f"Successfully wrote {count} records to {output_path}")
    else:
        print("No records found in stream.")


if __name__ == "__main__":
    main()
