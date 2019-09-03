using System;
using System.Buffers;
using Kudu.Client.FunctionalTests.MiniCluster;
using static Kudu.Client.FunctionalTests.Utils.ClientTestUtil;
using Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class RowResultTests : MiniKuduClusterTestBase {

        [SkippableFact]
        public async void Test() {

            var client = GetKuduClient();        
            var tableName = Guid.NewGuid().ToString();
            var builder = GetAllTypesCreateTableOptions();
            var table = await client.CreateTableAsync(tableName, GetSchemaWithAllTypes(), builder);
            Assert.Equal(tableName, table.TableName);
            Assert.Equal(1, table.NumReplicas);
      
            var insert = table.NewInsert();
            var row = insert.Row;
            row.SetByte(0, 1);
            row.SetInt16(1, 2);
            row.SetInt32(2, 3);
            row.SetInt64(3, 4);
            row.SetBool(4, true);
            row.SetFloat(5, 5.6F);
            row.SetDouble(6, 7.8D);
            row.SetString(7, "string-value");
            row.SetBinary(8, "binary-array".ToUtf8ByteArray());
            row.SetBinary(9, "binary-bytebuffer".ToUtf8ByteArray());
            row.SetNull(10);
            row.SetDateTime(11, new DateTime(1,1,1, 0, 0, 0, DateTimeKind.Utc));
            row.SetDecimal(12, 12.345m);

            var result = await client.WriteRowAsync(insert);
            Assert.Empty(result.PerRowErrors);
            Assert.NotEqual(0UL, result.Timestamp);
            
            KuduScanner scanner = client.NewScannerBuilder(table).Build();
            var firstRows = await scanner.NextRowsAsync();

            foreach(var rr in firstRows)
            {
                Assert.Equal(1, rr.GetByte(0));
                Assert.Equal(2, rr.GeShort(1));
                Assert.Equal(3, rr.GetInt(2));
                Assert.Equal(4, rr.GetLong(3));
                Assert.Equal(true, rr.GetBool(4));
                Assert.Equal(5.6F, rr.GetFloat(5));
                Assert.Equal(7.8D, rr.GetDouble(6));
                Assert.Equal("string-value", rr.GetString(7));
                Assert.Equal("binary-array".ToUtf8ByteArray(), rr.GetBinary(8).ToArray());
                Assert.Equal("binary-array".ToUtf8ByteArray(), rr.GetBinaryCopy(8));
                Assert.Equal(true, rr.IsNull(10));
                Assert.Equal(new DateTime(1,1,1, 0, 0, 0, DateTimeKind.Utc), rr.GetTimeStamp(11));
                Assert.Equal(12.345M, rr.GetDecimal(12));
            }
        }
    }
}