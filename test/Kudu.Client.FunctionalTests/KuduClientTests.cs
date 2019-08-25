using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using Kudu.Client;
using Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class KuduClientTests : MiniKuduClusterTestBase
    {
        [SkippableFact]
        public async Task BadHostnames()
        {
            const string badHostname = "some-unknown-host-hopefully";
            //arrange
            var kuduClient = KuduClient.Build(badHostname);
            // act & assert
            await Assert.ThrowsAnyAsync<SocketException>( () => kuduClient.ListTabletServersAsync());
           
        }

        [SkippableFact]
        public async Task ListTabletServersAsync()
        {
            //arrange
            var kuduClient =  GetKuduClient();

            // act & assert
            var tablets = await kuduClient.ListTabletServersAsync();
            
            Assert.Equal(3, tablets.Servers.Count);
        }
    }
}