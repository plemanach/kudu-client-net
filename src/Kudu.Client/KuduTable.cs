using Kudu.Client.Protocol.Master;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class KuduTable
    {
        internal GetTableSchemaResponsePB SchemaPb { get; }

        public Schema Schema { get; }

        public PartitionSchema PartitionSchema { get; }

        public string TableId { get; }

        public KuduTable(GetTableSchemaResponsePB schemaPb)
        {
            Schema = new Schema(schemaPb.Schema);
            SchemaPb = schemaPb;
            PartitionSchema = new PartitionSchema(schemaPb.PartitionSchema);
            TableId = schemaPb.TableId.ToStringUtf8();
        }

        public KuduTable(string tableName, string tableId)
        {
            SchemaPb = new GetTableSchemaResponsePB();
            SchemaPb.TableName = tableName;
            SchemaPb.TableId = System.Text.UTF8Encoding.UTF8.GetBytes(tableId);
            TableId = SchemaPb.TableId.ToStringUtf8();
          
        }

        public int NumReplicas => SchemaPb.NumReplicas;

        public string TableName => SchemaPb.TableName;

        public Operation NewInsert() => NewOperation(RowOperation.Insert);

        public Operation NewUpdate() => NewOperation(RowOperation.Update);

        public Operation NewUpsert() => NewOperation(RowOperation.Upsert);

        public Operation NewDelete() => NewOperation(RowOperation.Delete);

        private Operation NewOperation(RowOperation rowOperation)
        {
            var row = new PartialRow(Schema, rowOperation);
            return new Operation(this, row);
        }
    }
}
