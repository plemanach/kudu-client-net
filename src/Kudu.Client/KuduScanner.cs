using System;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.Requests;
using Kudu.Client.Tablet;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class KuduScanner
    {
        private readonly ScanBuilder _scanBuilder;
        private readonly KuduClient _kuduClient;
        private readonly KuduTable _table;

        private bool closed = false;
        private bool hasMore = true;
        private long numRowsReturned = 0;

        /**
        * Maximum number of bytes returned by the scanner, on each batch.
        */
        private uint batchSizeBytes;

        /**
        * The maximum number of rows to scan.
        */
        private long limit;

        /**
        * Set in the builder. If it's not set by the user, it will default to EMPTY_ARRAY.
        * It is then reset to the new start primary key of each tablet we open a scanner on as the scan
        * moves from one tablet to the next.
        */
        private byte[] startPrimaryKey;

        /**
        * Set in the builder. If it's not set by the user, it will default to EMPTY_ARRAY.
        * It's never modified after that.
        */
        private byte[] endPrimaryKey;

        private byte[] lastPrimaryKey;

        /**
        * This is the scanner ID we got from the TabletServer.
        * It's generated randomly so any value is possible.
        */
        private byte[] scannerId;

        /**
        * The tabletSlice currently being scanned.
        * If null, we haven't started scanning.
        * If == DONE, then we're done scanning.
        * Otherwise it contains a proper tabletSlice name, and we're currently scanning.
        */
        private RemoteTablet _tablet;

        /**
        * The sequence ID of this call. The sequence ID should start at 0
        * with the request for a new scanner, and after each successful request,
        * the client should increment it by 1. When retrying a request, the client
        * should _not_ increment this value. If the server detects that the client
        * missed a chunk of rows from the middle of a scan, it will respond with an
        * error.
        */
        private uint sequenceId;


        public KuduScanner(KuduClient kuduClient, KuduTable kuduTable, ScanBuilder scanBuilder)
        {
            _scanBuilder = scanBuilder;
            _kuduClient = kuduClient;
            _table = kuduTable;
            batchSizeBytes = scanBuilder.BatchSizeBytes;
        }

        public ScanRequest GetNextRequest()
        {
            return new ScanRequest(this, _table, new  Protocol.Tserver.ScanRequestPB{
                    ScannerId = scannerId,
                    CallSeqId = sequenceId,
                    BatchSizeBytes = batchSizeBytes
                });
        }

        public RemoteTablet CurrentTablet { 
            get{ return _tablet; } 
            internal set {_tablet = value;} 
        }

        public  async Task<RowResultIterator> NextRowsAsync()
        {
            if(closed)
            {
                return null;

            } else if (CurrentTablet == null)
            {
                var rpc = GetOpenRequest();
                var resp = await _kuduClient.SendRpcToTablet(rpc);
                return GotFirsttRow(rpc);
            }

            return await _kuduClient.ScanNextRowsAsync(this);            
        }

        ScanRequest GetOpenRequest() {
            //checkScanningNotStarted();
             return new ScanRequest(this, _table, new  Protocol.Tserver.ScanRequestPB{
                CallSeqId = sequenceId,
                
                BatchSizeBytes = _scanBuilder.BatchSizeBytes,
                NewScanRequest = new Protocol.Tserver.NewScanRequestPB{
                     ReadMode = Protocol.ReadModePB.ReadLatest,
                     Limit = 50,
                     PropagatedTimestamp = (ulong) EpochTime.ToUnixEpochMicros(DateTime.UtcNow)

                }
             });
        }


        void ScanFinished() {
            Partition partition = CurrentTablet.Partition;
            //pruner.removePartitionKeyRange(partition.getPartitionKeyEnd());
            // Stop scanning if we have scanned until or past the end partition key, or
            // if we have fulfilled the limit.
            //if (!pruner.hasMorePartitionKeyRanges() || numRowsReturned >= limit) {
            //    hasMore = false;
            //   closed = true; // the scanner is closed on the other side at this point
            //    return;
            //}
            //if (LOG.isDebugEnabled()) {
            //    LOG.debug("Done scanning tablet {} for partition {} with scanner id {}",
            //            tablet.getTabletId(), tablet.getPartition(), Bytes.pretty(scannerId));
            //}
            scannerId = null;
            sequenceId = 0;
            lastPrimaryKey = null;
            Invalidate();
        }

        void Invalidate() {
            _tablet = null;
        }

        private RowResultIterator GotFirsttRow(ScanRequest resp)
        {
            numRowsReturned += resp.Response.Data.NumRows;
            if (!resp.Response.HasMoreResults) {  // We're done scanning this tablet.
                ScanFinished();
                return RowResultIterator.MakeRowResultIterator(0, "", _table.Schema, resp, false);
            }
            scannerId = resp.Response.ScannerId;
            sequenceId++;
            hasMore = resp.Response.HasMoreResults;

            //if (LOG.isDebugEnabled()) {
            //    LOG.debug("Scanner " + Bytes.pretty(scannerId) + " opened on " + tablet);
            //}

            return RowResultIterator.MakeRowResultIterator(0, "", _table.Schema, resp, false);
        }

        private RowResultIterator GotNextRow(ScanRequest resp)
        {
            numRowsReturned += resp.Response.Data.NumRows;
            if (!resp.Response.HasMoreResults) {  // We're done scanning this tablet.
                ScanFinished();
                return RowResultIterator.MakeRowResultIterator(0, "", _table.Schema, resp, false);
            }
            sequenceId++;
            hasMore = resp.Response.HasMoreResults;
            return RowResultIterator.MakeRowResultIterator(0, "", _table.Schema, resp, false);
        }
    }
}
