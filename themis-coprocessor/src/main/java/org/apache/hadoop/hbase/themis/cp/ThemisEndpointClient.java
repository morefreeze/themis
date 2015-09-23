package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.EraseLockRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.EraseLockResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.LockExpiredRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.LockExpiredResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisBatchCommitSecondaryRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisBatchCommitSecondaryResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisBatchCommitSecondaryResult;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisBatchPrewriteSecondaryRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisBatchPrewriteSecondaryResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommit;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommitRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommitResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisGetRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisGetRequest.Builder;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewrite;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteResult;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisService.Stub;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

// coprocessor client for ThemisProtocol
public class ThemisEndpointClient {
  private final HConnection conn;

  public ThemisEndpointClient(HConnection connection) {
    this.conn = connection;
  }

  static abstract class CoprocessorCallable<R> {
    private HConnection conn;
    private byte[] tableName;
    private byte[] row;
    private HTableInterface table = null;

    public CoprocessorCallable(HConnection conn, byte[] tableName, byte[] row) {
      this.conn = conn;
      this.tableName = tableName;
      this.row = row;
    }

    public R run() throws IOException {
      try {
        table = conn.getTable(tableName);
        CoprocessorRpcChannel channel = table.coprocessorService(row);
        Stub stub = (Stub) ProtobufUtil.newServiceStub(ThemisProtos.ThemisService.class, channel);
        return invokeCoprocessor(stub);
      } catch (Throwable e) {
        throw new IOException(e);
      } finally {
        if (table != null) {
          table.close();
        }
      }
    }

    public abstract R invokeCoprocessor(Stub stub) throws Throwable;
  }

  public Result themisGet(final byte[] tableName, final Get get, final long startTs)
      throws IOException {
    return themisGet(tableName, get, startTs, false);
  }

  protected static void checkRpcException(ServerRpcController controller) throws IOException {
    if (controller.getFailedOn() != null) {
      throw controller.getFailedOn();
    }
  }

  public Result themisGet(final byte[] tableName, final Get get, final long startTs,
      final boolean ignoreLock) throws IOException {
    return new CoprocessorCallable<Result>(conn, tableName, get.getRow()) {
      @Override
      public Result invokeCoprocessor(Stub instance) throws Throwable {
        Builder builder = ThemisGetRequest.newBuilder();
        builder.setGet(ProtobufUtil.toGet(get));
        builder.setStartTs(startTs);
        builder.setIgnoreLock(ignoreLock);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ClientProtos.Result> rpcCallback = new BlockingRpcCallback<ClientProtos.Result>();
        instance.themisGet(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return ProtobufUtil.toResult(rpcCallback.get());
      }
    }.run();
  }

  public ThemisLock prewriteSecondaryRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] secondaryLock)
      throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, null, secondaryLock, -1);
  }

  public Map<byte[], ThemisLock> batchPrewriteSecondaryRows(final byte[] tblName, final List<RowMutation> rows,
      final long prewriteTs, final byte[] secondaryLock) throws IOException {
    final Map<byte[], ThemisLock> lockMap = new HashMap<byte[], ThemisLock>();
    CoprocessorCallable<List<ThemisPrewriteResult>> callable = new CoprocessorCallable<List<ThemisPrewriteResult>>(conn,
        tblName, rows.get(0).getRow()) {
      @Override
      public List<ThemisPrewriteResult> invokeCoprocessor(Stub instance) throws Throwable {
        ThemisBatchPrewriteSecondaryRequest.Builder builder = ThemisBatchPrewriteSecondaryRequest.newBuilder();
        ThemisPrewrite.Builder b = null;

        for (RowMutation row : rows) {
          b = ThemisPrewrite.newBuilder();
          b.setRow(HBaseZeroCopyByteString.wrap(row.getRow()));
          for (ColumnMutation mutation : row.mutationList()) {
            b.addMutations(ColumnMutation.toCell(mutation));
          }
          b.setPrewriteTs(prewriteTs);
          b.setPrimaryLock(HBaseZeroCopyByteString.wrap(HConstants.EMPTY_BYTE_ARRAY));
          b.setSecondaryLock(
              HBaseZeroCopyByteString.wrap(secondaryLock == null ? HConstants.EMPTY_BYTE_ARRAY : secondaryLock));
          b.setPrimaryIndex(-1);
          builder.addThemisPrewrite(b);
        }

        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisBatchPrewriteSecondaryResponse> rpcCallback = new BlockingRpcCallback<ThemisBatchPrewriteSecondaryResponse>();
        instance.batchPrewriteSecondaryRows(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        // Perhaps, part row has not in a region, sample : when region split,
        // then need try
        List<ByteString> rowsNotInRegion = rpcCallback.get().getRowsNotInRegionList();
        if (rowsNotInRegion != null && rowsNotInRegion.size() > 0) {
          Map<byte[], List<ColumnMutation>> rowMap = list2Map4Batch(rows);
          for (ByteString rowkey : rowsNotInRegion) {
            ThemisLock l = prewriteRow(tblName, rowkey.toByteArray(), rowMap.get(rowkey.toByteArray()), prewriteTs,
                null, secondaryLock, -1);
            if (l != null) {
              lockMap.put(rowkey.toByteArray(), l);
            }
          }
        }
        return rpcCallback.get().getThemisPrewriteResultList();
      }
    };

    batchJudgePerwriteResultRow(lockMap, tblName, rows, callable.run(), prewriteTs);
    return lockMap;
  }

  private Map<byte[], List<ColumnMutation>> list2Map4Batch(List<RowMutation> rows) {
    Map<byte[], List<ColumnMutation>> rowMap = new HashMap<byte[], List<ColumnMutation>>();
    for (RowMutation rowM : rows) {
      rowMap.put(rowM.getRow(), rowM.mutationList());
    }

    return rowMap;
  }

  public ThemisLock prewriteRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock, primaryIndex, false);
  }
  
  public ThemisLock prewriteSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock, primaryIndex, true);
  }

  protected ThemisLock prewriteRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex, final boolean isSingleRow) throws IOException {
    CoprocessorCallable<ThemisPrewriteResult> callable = new CoprocessorCallable<ThemisPrewriteResult>(conn, tableName, row) {
      @Override
      public ThemisPrewriteResult invokeCoprocessor(Stub instance) throws Throwable {
        ThemisPrewriteRequest.Builder builder = ThemisPrewriteRequest.newBuilder();
        ThemisPrewrite.Builder preBuilder = builder.getThemisPrewriteBuilder();
        
        preBuilder.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          preBuilder.addMutations(ColumnMutation.toCell(mutation));
        }
        preBuilder.setPrewriteTs(prewriteTs);
        preBuilder.setPrimaryLock(HBaseZeroCopyByteString
            .wrap(primaryLock == null ? HConstants.EMPTY_BYTE_ARRAY : primaryLock));
        preBuilder.setSecondaryLock(HBaseZeroCopyByteString
            .wrap(secondaryLock == null ? HConstants.EMPTY_BYTE_ARRAY : secondaryLock));
        preBuilder.setPrimaryIndex(primaryIndex);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisPrewriteResponse> rpcCallback = new BlockingRpcCallback<ThemisPrewriteResponse>();
        
        if (isSingleRow) {
          instance.prewriteSingleRow(controller, builder.build(), rpcCallback);
        } else {
          instance.prewriteRow(controller, builder.build(), rpcCallback);
        }
        checkRpcException(controller);
        return rpcCallback.get().getThemisPrewriteResult();
      }
    };
    return judgePerwriteResultRow(tableName, row, callable.run(), prewriteTs);
  }

  protected void batchJudgePerwriteResultRow(Map<byte[], ThemisLock> lockMap, byte[] tableName, List<RowMutation> rows,
      List<ThemisPrewriteResult> prewriteResults, long prewriteTs) throws IOException {
    if (prewriteResults == null || prewriteResults.size() == 0) {
      return;
    }

    byte[] row = null;
    ThemisLock lock = null;
    for (ThemisPrewriteResult result : prewriteResults) {
      row = result.getRow().toByteArray();
      lock = judgePerwriteResultRow(tableName, row, result, prewriteTs);
      if (lock != null) {
        lockMap.put(row, lock);
      }
    }
  }

  protected ThemisLock judgePerwriteResultRow(byte[] tableName, byte[] row, ThemisPrewriteResult prewriteResult,
      long prewriteTs) throws IOException {
    if (prewriteResult != null && !prewriteResult.getNewerWriteTs().isEmpty()) {
      long commitTs = Bytes.toLong(prewriteResult.getNewerWriteTs().toByteArray());
      if (commitTs != 0) {
        throw new WriteConflictException(
            "encounter write with larger timestamp than prewriteTs=" + prewriteTs + ", commitTs=" + commitTs);
      } else {
        ThemisLock lock = ThemisLock.parseFromByte(prewriteResult.getExistLock().toByteArray());
        ColumnCoordinate column = new ColumnCoordinate(tableName, row, prewriteResult.getFamily().toByteArray(),
            prewriteResult.getQualifier().toByteArray());
        lock.setColumn(column);
        lock.setLockExpired(Bytes.toBoolean(prewriteResult.getLockExpired().toByteArray()));
        return lock;
      }
    }
    return null;
  }

  public void commitSecondaryRow(final byte[] tableName, final byte[] row, List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs) throws IOException {
    commitRow(tableName, row, mutations, prewriteTs, commitTs, -1);
  }

  public void batchCommitSecondaryRows(final byte[] tableName, final List<RowMutation> rows, final long prewriteTs,
      final long commitTs) throws IOException {
    CoprocessorCallable<List<ThemisBatchCommitSecondaryResult>> callable = new CoprocessorCallable<List<ThemisBatchCommitSecondaryResult>>(
        conn, tableName, rows.get(0).getRow()) {
      @Override
      public List<ThemisBatchCommitSecondaryResult> invokeCoprocessor(Stub instance) throws Throwable {
        ThemisBatchCommitSecondaryRequest.Builder builder = ThemisBatchCommitSecondaryRequest.newBuilder();

        ThemisCommit.Builder cb = null;
        for (RowMutation rowMutation : rows) {
          cb = ThemisCommit.newBuilder();
          cb.setRow(HBaseZeroCopyByteString.wrap(rowMutation.getRow()));
          for (ColumnMutation mutation : rowMutation.mutationList()) {
            cb.addMutations(ColumnMutation.toCell(mutation));
          }
          cb.setPrewriteTs(prewriteTs);
          cb.setCommitTs(commitTs);
          cb.setPrimaryIndex(-1);
          builder.addThemisCommit(cb);
        }

        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisBatchCommitSecondaryResponse> rpcCallback = new BlockingRpcCallback<ThemisBatchCommitSecondaryResponse>();
        instance.batchCommitSecondaryRows(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getBatchCommitSecondaryResultList();
      }
    };

    List<ThemisBatchCommitSecondaryResult> results = callable.run();
    StringBuilder failRows = new StringBuilder();
    if (results != null && results.size() > 0) {
      for (ThemisBatchCommitSecondaryResult r : results) {
        if (!r.getSuccess()) {
          failRows.append(Bytes.toString(r.getRow().toByteArray()) + ",");
        }
      }
    }

    if (failRows.length() > 0) {
      throw new IOException("secondary row commit fail, should not happend!, fail rows:" + failRows);
    }
  }

  public void commitRow(final byte[] tableName, final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException {
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(Stub instance) throws Throwable {
        ThemisCommitRequest.Builder builder = ThemisCommitRequest.newBuilder();
        ThemisCommit.Builder cb = ThemisCommit.newBuilder();
        cb.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          cb.addMutations(ColumnMutation.toCell(mutation));
        }
        cb.setPrewriteTs(prewriteTs);
        cb.setCommitTs(commitTs);
        cb.setPrimaryIndex(primaryIndex);
        builder.setThemisCommit(cb);

        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisCommitResponse> rpcCallback = new BlockingRpcCallback<ThemisCommitResponse>();
        instance.commitRow(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getResult();
      }
    };
    if (!callable.run()) {
      if (primaryIndex < 0) {
        throw new IOException("secondary row commit fail, should not happend!");
      } else {
        ColumnMutation primaryMutation = mutations.get(primaryIndex);
        throw new LockCleanedException("lock has been cleaned, column="
            + new ColumnCoordinate(tableName, row, primaryMutation.getFamily(), primaryMutation.getQualifier())
            + ", prewriteTs=" + prewriteTs);
      }
    }
  }
  
  public void commitSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final long commitTs,
      final int primaryIndex) throws IOException {
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(Stub instance) throws Throwable {
        ThemisCommitRequest.Builder builder = ThemisCommitRequest.newBuilder();
        ThemisCommit.Builder cb = ThemisCommit.newBuilder();
        cb.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          cb.addMutations(ColumnMutation.toCell(mutation));
        }
        cb.setPrewriteTs(prewriteTs);
        cb.setCommitTs(commitTs);
        cb.setPrimaryIndex(primaryIndex);
        builder.setThemisCommit(cb);
        
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisCommitResponse> rpcCallback = new BlockingRpcCallback<ThemisCommitResponse>();
        instance.commitSingleRow(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getResult();
      }
    };
    if (!callable.run()) {
      if (primaryIndex < 0) {
        throw new IOException("secondary row commit fail, should not happend!");
      } else {
        ColumnMutation primaryMutation = mutations.get(primaryIndex);
        throw new LockCleanedException("lock has been cleaned, column="
            + new ColumnCoordinate(tableName, row, primaryMutation.getFamily(),
                primaryMutation.getQualifier()) + ", prewriteTs=" + prewriteTs);
      }
    }
  }

  public ThemisLock getLockAndErase(final ColumnCoordinate columnCoordinate, final long prewriteTs)
      throws IOException {
    CoprocessorCallable<byte[]> callable = new CoprocessorCallable<byte[]>(conn,
        columnCoordinate.getTableName(), columnCoordinate.getRow()) {
      @Override
      public byte[] invokeCoprocessor(Stub instance) throws Throwable {
        EraseLockRequest.Builder builder = EraseLockRequest.newBuilder();
        builder.setRow(HBaseZeroCopyByteString.wrap(columnCoordinate.getRow()));
        builder.setFamily(HBaseZeroCopyByteString.wrap(columnCoordinate.getFamily()));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(columnCoordinate.getQualifier()));
        builder.setPrewriteTs(prewriteTs);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<EraseLockResponse> rpcCallback = new BlockingRpcCallback<EraseLockResponse>();
        instance.getLockAndErase(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().hasLock() ? rpcCallback.get().getLock().toByteArray() : null;
      }
    };
    byte[] result = callable.run();
    return result == null ? null : ThemisLock.parseFromByte(result);
  }
  
  public boolean isLockExpired(final byte[] tableName, final byte[] row, final long timestamp)
      throws IOException {
    return new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(Stub instance) throws Throwable {
        LockExpiredRequest.Builder builder = LockExpiredRequest.newBuilder();
        builder.setTimestamp(timestamp);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<LockExpiredResponse> rpcCallback = new BlockingRpcCallback<LockExpiredResponse>();
        instance.isLockExpired(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getExpired();
      }
    }.run();
  }
  
}
