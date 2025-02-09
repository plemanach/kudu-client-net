using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using Kudu.Client.Requests;

namespace Kudu.Client
{
    public class RowResultIterator : IEnumerable<RowResult>{
        
        private Schema _schema;
        private readonly ReadOnlySequence<byte> _rowData;
        private readonly ReadOnlySequence<byte> _indirectData;
        private int _numRows;
        private int _currentRow = 0;
        private RowResult _sharedRowResult;


        private RowResultIterator(Schema schema,
                                    int numRows,
                                    ReadOnlySequence<byte> rowData,
                                    ReadOnlySequence<byte> indirectBs,
                                    bool reuseRowResult) 
        {
            
            this._schema = schema;
            this._numRows = numRows;
            this._rowData = rowData;
            this._indirectData = indirectBs;
            this._sharedRowResult = (reuseRowResult && numRows != 0) ?
                new RowResult(_schema, this._rowData, this._indirectData, -1) : null;
        }

        public static RowResultIterator MakeRowResultIterator(long elapsedMillis,
                                                 string tsUUID,
                                                 Schema schema,
                                                 ScanRequest data,
                                                 bool reuseRowResult) {
            if (data == null || data.Response.Data.NumRows == 0) {
                return new RowResultIterator(
                    schema, 
                    0,  
                    ReadOnlySequence<byte>.Empty, 
                    ReadOnlySequence<byte>.Empty, reuseRowResult);
            }

            

            byte[]  bs = (data.Response.Data.ShouldSerializeRowsSidecar() == true) ? 
                                            data.SideCars[data.Response.Data.RowsSidecar] : ReadOnlySequence<byte>.Empty.ToArray();
            byte[]  indirectBs = (data.Response.Data.ShouldSerializeIndirectDataSidecar() == true) ?
                                            data.SideCars[data.Response.Data.IndirectDataSidecar] : ReadOnlySequence<byte>.Empty.ToArray();
            int numRows = data.Response.Data.NumRows;

            // Integrity check
            int rowSize = schema.RowSize;
            int expectedSize = numRows * rowSize;
            if (expectedSize != bs.Length) 
            {
                //Status statusIllegalState = Status.IllegalState("RowResult block has " + bs.length() +
                //   " bytes of data but expected " + expectedSize + " for " + numRows + " rows");
                throw new Exception($"RowResult block has {bs.Length} bytes of data but expected {expectedSize} for {numRows} rows");
            }
            return new RowResultIterator(schema, numRows, new ReadOnlySequence<byte>(bs), new ReadOnlySequence<byte>(indirectBs), reuseRowResult);
        }
        
        public IEnumerator<RowResult> GetEnumerator()
        {
            while(this._currentRow < _numRows)
            {
                // If sharedRowResult is not null, we should reuse it for every next call.
                if (_sharedRowResult != null) {
                    this._sharedRowResult.AdvancePointerTo(this._currentRow++);
                    yield return _sharedRowResult;
                } else {
                    yield return new RowResult(_schema, _rowData, _indirectData, this._currentRow++);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public int NumRows { get{ return _numRows;} }
    }
}