using System.Collections.Generic;
using Kudu.Client;
using Kudu.Client.Protocol;

public class ProtobufHelper {


    public static List<ColumnSchemaPB> SchemaToPb(Schema schema) {
        List<ColumnSchemaPB> columns = new List<ColumnSchemaPB>(schema.ColumnCount);
        for(int index = 0; index < schema.ColumnCount; index++)
        {
            var column = schema.GetColumn(index);
            int id = schema.GetColumnIndex(column.Name);
            columns.Add(ColumnToPb(id, column));
        }
        return columns;
    }

    public static ColumnSchemaPB ColumnToPb(int id, ColumnSchema column)
    {
        return new ColumnSchemaPB{
                Name = column.Name,
                Type = (DataTypePB) column.Type,
                Compression = (CompressionTypePB) column.Compression,
                IsKey = column.IsKey,
                IsNullable = column.IsNullable,
                Encoding = (EncodingTypePB)column.Encoding,
                TypeAttributes = column.TypeAttributes != null ? new ColumnTypeAttributesPB{
                    Precision = column.TypeAttributes.Precision,
                    Scale = column.TypeAttributes.Scale
                } : null,
        };
    }
}