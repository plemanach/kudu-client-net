using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ListTabletServersRequest : KuduRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "ListTabletServers";

        public ListTabletServersRequest(ListTabletServersRequestPB request)
        {
            Request = request;
        }
    }
}
