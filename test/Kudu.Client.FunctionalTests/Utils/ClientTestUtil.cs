using System.Collections.Generic;
using Kudu.Client.Builder;

namespace Kudu.Client.FunctionalTests.Utils {

    public abstract class ClientTestUtil {
        public static Schema GetSchemaWithAllTypes() {
            List<ColumnSchema> columns = new List<ColumnSchema>{
                new ColumnSchema("int8", DataType.Int8, true),
                new ColumnSchema("int16", DataType.Int16),
                new ColumnSchema("int32", DataType.Int32),
                new ColumnSchema("int64", DataType.Int64),
                new ColumnSchema("bool", DataType.Bool),
                new ColumnSchema("float", DataType.Float),
                new ColumnSchema("double", DataType.Double),
                new ColumnSchema("string", DataType.String),
                new ColumnSchema("binary-array", DataType.Binary),
                new ColumnSchema("binary-bytebuffer", DataType.Binary),
                new ColumnSchema("null", DataType.String, false, true),
                new ColumnSchema("timestamp", DataType.UnixtimeMicros),
                new ColumnSchema("decimal", DataType.Decimal128, false, false, EncodingType.AutoEncoding, CompressionType.DefaultCompression, new ColumnTypeAttributes(5,3)),
            };
            return new Schema(columns);
        }

        public static TableBuilder GetAllTypesCreateTableOptions() {
            return new TableBuilder().SetRangePartitionColumns("int8").SetNumReplicas(1);
        }
    }
}