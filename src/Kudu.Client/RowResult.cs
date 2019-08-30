using System;
using System.Buffers;
using System.Collections;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class RowResult {

        private const int INDEX_RESET_LOCATION = -1;
        private Schema _schema;
        private readonly ReadOnlySequence<byte> _rowData;
        private readonly ReadOnlySequence<byte> _indirectData;
        private readonly int _rowSize;
        private readonly int[] columnOffsets;

        private BitArray _nullsBitSet;
        private int _index = INDEX_RESET_LOCATION;
        private int _offset;

        public RowResult(Schema schema, ReadOnlySequence<byte> rowData, ReadOnlySequence<byte> indirectData, int rowIndex)
        {
            _schema = schema;
            _rowData = rowData;
            _rowSize = schema.RowAllocSize;

            int columnOffsetsSize = schema.ColumnCount;
            if (schema.HasNullableColumns) {
                columnOffsetsSize++;
            }

            columnOffsets = new int[columnOffsetsSize];
            // Empty projection, usually used for quick row counting.
            if (columnOffsetsSize == 0) {
                return;
            }
            int currentOffset = 0;
            columnOffsets[0] = currentOffset;
            // Pre-compute the columns offsets in rowData for easier lookups later.
            // If the schema has nullables, we also add the offseIndext for the null bitmap at the end.
            for (int i = 1; i < columnOffsetsSize; i++) {
                ColumnSchema column = schema.GetColumn(i - 1);
                int previousSize = column.Size;
                columnOffsets[i] = previousSize + currentOffset;
                currentOffset += previousSize;
            }
        }

        public int GetInt(String columnName) {
            return GetInt(_schema.GetColumnIndex(columnName));
        }
        public int GetInt(int columnIndex) {
            return KuduEncoder.DecodeInt32(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        internal void resetPointer() {
            AdvancePointerTo(INDEX_RESET_LOCATION);
        }

        internal void AdvancePointerTo(int rowIndex) {

            this._index = rowIndex;
            this._offset = this._rowSize * this._index;
            if (_schema.HasNullableColumns && this._index != INDEX_RESET_LOCATION) {
                this._nullsBitSet = ToBitSet(
                    this._rowData.ToArray(),
                    GetCurrentRowDataOffsetForColumn(_schema.ColumnCount),
                    _schema.ColumnCount);
            }
        }
        
        private int GetCurrentRowDataOffsetForColumn(int columnIndex) {
            return this._offset + this.columnOffsets[columnIndex];
        }

        private static BitArray ToBitSet(byte[] b, int offset, int colCount) {
            BitArray bs = new BitArray(colCount);
            for (int i = 0; i < colCount; i++) {
                if ((b[offset + (i / 8)] >> (i % 8) & 1) == 1) {
                    bs.Set(i, true);
                }
            }
            return bs;
        }
    }
}