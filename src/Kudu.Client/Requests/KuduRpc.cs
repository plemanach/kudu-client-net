using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
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

        public List<byte[]> SideCars {get;} = new List<byte[]>();

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

        public virtual async Task ParseSidecarsAsync(ResponseHeader header, PipeReader reader, int length)
        {
            //TODO: throw new NotImplementedException();
            var result = await reader.ReadAsync().ConfigureAwait(false);
            var buffer = result.Buffer;
          
            int indexSideCar = 1;
            uint remainingLength = (uint)length;
            uint offsetSideCar = 0;

             Console.WriteLine($"SideCars 1 count:{header.SidecarOffsets.Length}");

            if(buffer.Length < length)
            {
                throw new ApplicationException("Missing sidecars data");
            }

            foreach(var offset in header.SidecarOffsets)
            {
                var sideCarLength = indexSideCar == header.SidecarOffsets.Length ?
                    remainingLength : (header.SidecarOffsets[indexSideCar] - offset);
                remainingLength -= sideCarLength;
                indexSideCar++;

                Console.WriteLine($"remainingLength {length}");
               
                Console.WriteLine($"Slive {offsetSideCar} : {sideCarLength}");

                SideCars.Add(buffer.Slice(offsetSideCar, sideCarLength).ToArray());
                offsetSideCar += sideCarLength;

                Console.WriteLine($"SideCars size:{SideCars.First().Length}");
            }
            buffer = buffer.Slice(length);
            reader.AdvanceTo(buffer.Start);

            Console.WriteLine($"SideCars count:{SideCars.Count}");
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
