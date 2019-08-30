using System;
using Kudu.Client;
using Kudu.Client.Builder;
using Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;


public class KuduScannerTests : MiniKuduClusterTestBase {

    [SkippableFact]
    public async void TestIterable() {

        var client = GetKuduClient();

        var tableName = Guid.NewGuid().ToString();
        var builder = new TableBuilder()
            .SetTableName(tableName)
            .SetNumReplicas(1)
            .AddColumn(column =>
            {
                column.Name = "column_x";
                column.Type = DataType.Int32;
                column.IsKey = true;
                column.IsNullable = false;
                column.Compression = CompressionType.DefaultCompression;
                column.Encoding = EncodingType.AutoEncoding;
            })
            .AddColumn(column =>
            {
                column.Name = "column_y";
                column.IsNullable = true;
                column.Type = DataType.String;
                column.Encoding = EncodingType.DictEncoding;
            });

        var table = await client.CreateTableAsync(builder);
        Assert.Equal(tableName, table.TableName);
        Assert.Equal(1, table.NumReplicas);


        const int expectedRowNums = 5;

        for(int keyIndice = 0; keyIndice < expectedRowNums; keyIndice++)
        {
            var insert = table.NewInsert();
            var row = insert.Row;
            row.SetInt32(0, keyIndice);
            row.SetString(1, "test value");

            var result = await client.WriteRowAsync(insert);
            Assert.Empty(result.PerRowErrors);
            Assert.NotEqual(0UL, result.Timestamp);
        }
    

        KuduScanner scanner = client.NewScannerBuilder(table).Build();
        var firstRows = await scanner.NextRowsAsync();

        Console.WriteLine($"Rows count:{firstRows.NumRows}");

        Assert.Equal(expectedRowNums, firstRows.NumRows);
     }
}