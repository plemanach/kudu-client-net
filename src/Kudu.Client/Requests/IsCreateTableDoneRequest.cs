using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class IsCreateTableDoneRequest : KuduRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "IsCreateTableDone";

        public IsCreateTableDoneRequest(KuduTable kuduTable, IsCreateTableDoneRequestPB request) : base(kuduTable)
        {
            Request = request;
        }
    }
}
