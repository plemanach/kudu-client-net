using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ListTablesRequest : KuduRpc<ListTablesRequestPB, ListTablesResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "ListTables";

        public ListTablesRequest(KuduTable kuduTable, ListTablesRequestPB request) : base(kuduTable)
        {
            Request = request;
        }
    }
}
