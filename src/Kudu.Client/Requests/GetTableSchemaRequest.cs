using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class GetTableSchemaRequest : KuduRpc<GetTableSchemaRequestPB, GetTableSchemaResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "GetTableSchema";

        public GetTableSchemaRequest(KuduTable kuduTable, GetTableSchemaRequestPB request) : base(kuduTable)
        {
            Request = request;
        }
    }
}
