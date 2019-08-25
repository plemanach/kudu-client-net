using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Kudu.Client.Connection;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Tablet;
using ProtoBuf;

namespace Kudu.Client.Requests
{
    public abstract class KuduRpc
    {
        // Service names.
        protected const string MasterServiceName = "kudu.master.MasterService";
        protected const string TabletServerServiceName = "kudu.tserver.TabletServerService";

        public KuduTable Table {get;}

        public RemoteTablet Tablet {get; set;}

        public abstract string ServiceName { get; }

        public abstract string MethodName { get; }

        public virtual ReplicaSelection ReplicaSelection => ReplicaSelection.LeaderOnly;

        public KuduRpc(KuduTable kuduTable)
        {
            Table = kuduTable;
        }

        // TODO: Include numAttempts, externalConsistencyMode, etc.

        // TODO: Eventually change this method to
        // public abstract void WriteRequest(IBufferWriter<byte> writer);
        public abstract void WriteRequest(Stream stream);

        public abstract void ParseProtobuf(ReadOnlySequence<byte> buffer);

        public virtual Task ParseSidecarsAsync(ResponseHeader header, PipeReader reader, int length)
        {
            //TODO: throw new NotImplementedException();

            return Task.CompletedTask;
        }
    }

    public abstract class KuduRpc<TRequest, TResponse> : KuduRpc
    {
        public TRequest Request { get; set; }

        public TResponse Response { get; set; }

        public byte[] PartitionKey { get{ return null;}}

        public KuduRpc(KuduTable kuduTable): base(kuduTable)
        {
          
        }
 
        public override void WriteRequest(Stream stream)
        {
            Serializer.SerializeWithLengthPrefix(stream, Request, PrefixStyle.Base128);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var array = buffer.ToArray();
            var stream = new MemoryStream(array);
            Response = Serializer.Deserialize<TResponse>(stream);

            // TODO: Use ProtoReader when it's ready.
        }

        //internal protected virtual byte[] PartitionKey => null;

        //internal protected virtual MasterFeatures[] MasterFeatures => null;
    }
}
