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
            _indirectData = indirectData;
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
            AdvancePointerTo(rowIndex);
        }

        public int GetInt(String columnName) {
            return GetInt(_schema.GetColumnIndex(columnName));
        }
        
        public int GetInt(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Int32);
            return KuduEncoder.DecodeInt32(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public int GeShort(String columnName) {
            return GeShort(_schema.GetColumnIndex(columnName));
        }

        public int GeShort(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Int16);
            return KuduEncoder.DecodeInt16(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public bool GetBool(String columnName) {
            return GetBool(_schema.GetColumnIndex(columnName));
        }

        public bool GetBool(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Bool);
            return KuduEncoder.DecodeBool(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public byte GetByte(String columnName) {
            return GetByte(_schema.GetColumnIndex(columnName));
        }

        public byte GetByte(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Int8);
            return _rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray()[0];
        }

        public long GetLong(String columnName) {
            return GetLong(_schema.GetColumnIndex(columnName));
        }
        
        public long GetLong(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Int64,  Builder.DataType.UnixtimeMicros);
            return KuduEncoder.DecodeInt64(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        long GetLongOrOffset(int columnIndex) {
              return KuduEncoder.DecodeInt64(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public float GetFloat(String columnName) {
            return GetFloat(_schema.GetColumnIndex(columnName));
        }
        
        public float GetFloat(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Float);
            return KuduEncoder.DecodeFloat(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public double GetDouble(String columnName) {
            return GetDouble(_schema.GetColumnIndex(columnName));
        }
        
        public double GetDouble(int columnIndex) {

            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Double);
            return KuduEncoder.DecodeDouble(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public decimal GetDecimal(String columnName) {
            return GetDecimal(_schema.GetColumnIndex(columnName));
        }
        
        public decimal GetDecimal(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Decimal128,  Builder.DataType.Decimal64,  Builder.DataType.Decimal32);
            var column = _schema.GetColumn(columnIndex);
            var typeAttributes = column.TypeAttributes;
            return KuduEncoder.DecodeDecimal(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray(), typeAttributes.Precision, typeAttributes.Scale);
        }

        public DateTime GetTimeStamp(String columnName) {
            return GetTimeStamp(_schema.GetColumnIndex(columnName));
        }

        public DateTime GetTimeStamp(int columnIndex)
        {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.UnixtimeMicros);
            return KuduEncoder.DecodeTimestamp(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)).ToArray());
        }

        public Schema ColumnProjection{ get{ return _schema;} }

        public string GetString(String columnName) {
            return GetString(_schema.GetColumnIndex(columnName));
        }
        
        public string GetString(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.String);

            long offset = GetLongOrOffset(columnIndex);
            long length = KuduEncoder.DecodeInt64(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)+ 8).ToArray());
  
            return KuduEncoder.DecodeString(_indirectData.Slice(offset, length).ToArray());
        }

        internal void resetPointer() {
            AdvancePointerTo(INDEX_RESET_LOCATION);
        }

        public byte[] GetBinaryCopy(String columnName) {
            return GetBinaryCopy(_schema.GetColumnIndex(columnName));
        }

        public byte[] GetBinaryCopy(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);

            long offset = GetLongOrOffset(columnIndex);
            long length = KuduEncoder.DecodeInt64(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)+ 8).ToArray());
  
            return _indirectData.Slice(offset, length).ToArray();
        }

        public ReadOnlySequence<byte> GetBinary(int columnIndex) {
            CheckValidColumn(columnIndex);
            CheckNull(columnIndex);
            CheckType(columnIndex, Builder.DataType.Binary);

            long offset = GetLongOrOffset(columnIndex);
            long length = KuduEncoder.DecodeInt64(_rowData.Slice(this.GetCurrentRowDataOffsetForColumn(columnIndex)+ 8).ToArray());
  
            return _indirectData.Slice(offset, length);
        }

        public bool IsNull(string columnName) {
            return IsNull(_schema.GetColumnIndex(columnName));
        }

        public bool IsNull(int columnIndex) {

            if(_nullsBitSet == null)
            {
                return false;
            }
            var column = _schema.GetColumn(columnIndex);
            return column.IsNullable && _nullsBitSet[columnIndex];
        }

        public object GetObject(String columnName) {
            return GetObject(_schema.GetColumnIndex(columnName));
        }
        
        public object GetObject(int columnIndex)
        {
             if (IsNull(columnIndex)) return null;
             var type = _schema.GetColumn(columnIndex).Type;

             switch(type)
             {
                 case Builder.DataType.Binary: return GetBinaryCopy(columnIndex);
                 case Builder.DataType.Bool: return GetBool(columnIndex);
                 case Builder.DataType.Double: return GetDouble(columnIndex);
                 case Builder.DataType.Float: return  GetFloat(columnIndex);
                 case Builder.DataType.Int16: return GeShort(columnIndex);
                 case Builder.DataType.Int32: return  GetInt(columnIndex);
                 case Builder.DataType.Int64: return GetLong(columnIndex);
                 case Builder.DataType.Int8: return GetByte(columnIndex);
                 case Builder.DataType.String: return GetString(columnIndex);
                 case Builder.DataType.UnixtimeMicros: return GetTimeStamp(columnIndex);
                 case Builder.DataType.Decimal128:
                 case Builder.DataType.Decimal32:
                 case Builder.DataType.Decimal64: return GetDecimal(columnIndex); 
                 default: throw new InvalidOperationException("Unsupported type: " + type);            
             }
        }

        public Builder.DataType GetColumnType(String columnName) {
            return GetColumnType(_schema.GetColumnIndex(columnName));
        }

        public Builder.DataType GetColumnType(int columnIndex) {
            return _schema.GetColumn(columnIndex).Type;
        }

        public override string ToString(){
             return "RowResult index: " + this._index + ", size: " + this._rowSize;
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
        
        private void CheckValidColumn(int columnIndex) {
            if (columnIndex >= _schema.ColumnCount) {
                throw new IndexOutOfRangeException($"Requested column is out of range, {columnIndex} out of {_schema.ColumnCount}");
            }
        }

        private void CheckNull(int columnIndex) {
            if (!_schema.HasNullableColumns) {
                return;
            }
            if (IsNull(columnIndex)) {
                ColumnSchema columnSchema = _schema.GetColumn(columnIndex);
                throw new ArgumentException("The requested column (name: {columnSchema.Name index: {columnIndex} is null");
            }
        }

        private void CheckType(int columnIndex, params Builder.DataType[] types) {
            ColumnSchema columnSchema = _schema.GetColumn(columnIndex);
            var columnType = columnSchema.Type;
            foreach (var type in types) {
                if (columnType.Equals(type)) {
                    return;
                }
            }
            throw new ArgumentException($"Column (name:  {columnSchema.Name} , index: {columnIndex} is of type {columnType} but was requested as a type {types}");
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