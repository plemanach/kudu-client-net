using System;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Kudu.Client.Connection;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Requests
{
    public class ScanRequest : KuduRpc<ScanRequestPB, ScanResponsePB>
    {
        public override string ServiceName => TabletServerServiceName;

        public override string MethodName => "Scan";

        private KuduScanner _kuduScanner;

        public ScanRequest(KuduScanner kuduScanner, KuduTable kuduTable, ScanRequestPB request) : base(kuduTable)
        {
            Request = request;
            _kuduScanner = kuduScanner;
        }

        public override void WriteRequest(System.IO.Stream stream)
        {
            if(this.Request.NewScanRequest != null)
            {
                _kuduScanner.CurrentTablet = this.Tablet;
                this.Request.NewScanRequest.TabletId = System.Text.UTF8Encoding.UTF8.GetBytes(this.Tablet.TabletId);
            }
            base.WriteRequest(stream);
        }
    }
}
