﻿using System;
using System.Collections.Generic;
using Kudu.Client.Builder;
using Kudu.Client.Protocol;

namespace Kudu.Client
{
    public class Schema
    {
        // TODO: Create a managed class for this?
        // TODO: Store columns instead?
        private readonly SchemaPB _schema;

        /// <summary>
        /// Maps column name to column index.
        /// </summary>
        private readonly Dictionary<string, int> _columnsByName;

        /// <summary>
        /// Maps columnId to column index.
        /// </summary>
        private readonly Dictionary<int, int> _columnsById;

        /// <summary>
        /// Maps column index to data index.
        /// </summary>
        private readonly int[] _columnOffsets;

        public int ColumnCount { get; }

        /// <summary>
        /// The size of all fixed-length columns.
        /// </summary>
        public int RowAllocSize { get; }

        public int VarLengthColumnCount { get; }

        public bool HasNullableColumns { get; }

        public Schema(SchemaPB schema)
        {
            var columns = schema.Columns;

            var size = 0;
            var varLenCnt = 0;
            var hasNulls = false;
            var columnsByName = new Dictionary<string, int>(columns.Count);
            var columnsById = new Dictionary<int, int>(columns.Count);
            var columnOffsets = new int[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.Type == DataTypePB.String || column.Type == DataTypePB.Binary)
                {
                    columnOffsets[i] = varLenCnt;
                    varLenCnt++;
                    // Don't increment size here, these types are stored separately
                    // in PartialRow.
                }
                else
                {
                    columnOffsets[i] = size;
                    size += GetTypeSize((DataType)column.Type);
                }

                hasNulls |= column.IsNullable;
                columnsByName.Add(column.Name, i);
                columnsById.Add((int)column.Id, i);

                // TODO: Remove this hack-fix. Kudu throws an exception if columnId is supplied.
                column.ResetId();

                // TODO: store primary key columns
            }

            _schema = schema;
            _columnOffsets = columnOffsets;
            _columnsByName = columnsByName;
            _columnsById = columnsById;
            ColumnCount = columns.Count;
            RowAllocSize = size;
            VarLengthColumnCount = varLenCnt;
            HasNullableColumns = hasNulls;
        }

        public int GetColumnIndex(string name) => _columnsByName[name];

        /// <summary>
        /// If the column is a fixed-length type, the offset is where that
        /// column should be in <see cref="PartialRow._rowAlloc"/>. If the
        /// column is variable-length, the offset is where that column should
        /// be stored in <see cref="PartialRow._varLengthData"/>.
        /// </summary>
        /// <param name="index">The column index.</param>
        public int GetColumnOffset(int index) => _columnOffsets[index];

        public int GetColumnSize(int index) => GetTypeSize(GetColumnType(index));

        public DataType GetColumnType(int index) => (DataType)_schema.Columns[index].Type;

        public bool IsPrimaryKey(int index) => _schema.Columns[index].IsKey;

        public int GetColumnIndex(int id) => _columnsById[id];

        public static int GetTypeSize(DataType type)
        {
            switch (type)
            {
                case DataType.String:
                case DataType.Binary:
                    return 8 + 8; // Offset then string length.
                case DataType.Bool:
                case DataType.Int8:
                case DataType.UInt8:
                    return 1;
                case DataType.Int16:
                case DataType.UInt16:
                    return 2;
                case DataType.Int32:
                case DataType.UInt32:
                case DataType.Float:
                case DataType.Decimal32:
                    return 4;
                case DataType.Int64:
                case DataType.UInt64:
                case DataType.Double:
                case DataType.UnixtimeMicros:
                case DataType.Decimal64:
                    return 8;
                case DataType.Int128:
                case DataType.Decimal128:
                    return 16;
                default:
                    throw new ArgumentException();
            }
        }

        public static bool IsSigned(DataType type)
        {
            switch (type)
            {
                case DataType.Int8:
                case DataType.Int16:
                case DataType.Int32:
                case DataType.Int64:
                case DataType.Int128:
                    return true;
                default:
                    return false;
            }
        }
    }
}
