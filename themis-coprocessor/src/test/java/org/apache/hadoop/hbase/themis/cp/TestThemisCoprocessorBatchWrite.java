package org.apache.hadoop.hbase.themis.cp;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import junit.framework.Assert;

public class TestThemisCoprocessorBatchWrite extends TransactionTestBase {
  protected static byte[][] primaryFamilies;
  protected static byte[][] primaryQualifiers;
  protected static byte[] primaryTypes;
  protected static byte[][][] secondaryFamilies;
  protected static byte[][][] secondaryQualifiers;
  protected static byte[][] secondaryTypes;
  private static Map<String, RowMutation> rowmMap = new HashMap<String, RowMutation>();

  static {
    for (Pair<byte[], RowMutation> rowP : SECONDARY_ROWS) {
      rowmMap.put(getLockMapKey4Batch(Bytes.toString(rowP.getFirst()), Bytes.toString(rowP.getSecond().getRow())),
          rowP.getSecond());
    }
  }

  @Test
  public void testCheckBatchPrewriteSecondaryRowsSuccess() throws Exception {
    Map<String, ThemisLock> lockMap = batchPrewriteSecondaryRows();
    for (String key : lockMap.keySet()) {
      Assert.assertNull(lockMap.get(key));
      checkPrewriteRowSuccess(key.split(SPLIT)[0].getBytes(), rowmMap.get(key));
    }
  }

  @Test
  public void testCheckBatchCommitSecondaryRowsSuccess() throws Exception {
    Map<String, ThemisLock> lockMap = batchPrewriteSecondaryRows();
    batchCommitSecondaryRow();
    for (String key : lockMap.keySet()) {
      Assert.assertNull(lockMap.get(key));
      checkCommitRowSuccess(key.split(SPLIT)[0].getBytes(), rowmMap.get(key));
    }

    // commit secondary success without lock
    nextTransactionTs();
    lockMap = batchPrewriteSecondaryRows();
    for (String key : lockMap.keySet()) {
      Assert.assertNull(lockMap.get(key));
      for (Pair<byte[], RowMutation> tableEntry : SECONDARY_ROWS) {
        byte[] tableName = tableEntry.getFirst();
        byte[] row = tableEntry.getSecond().getRow();
        for (ColumnMutation columnMutation : tableEntry.getSecond().mutationList()) {
          ColumnCoordinate columnCoordinate = new ColumnCoordinate(tableName, row, columnMutation);
          eraseLock(columnCoordinate, prewriteTs);
        }
      }
    }

    batchCommitSecondaryRow();
    for (String key : lockMap.keySet()) {
      checkCommitRowSuccess(key.split(SPLIT)[0].getBytes(), rowmMap.get(key));
    }
  }

}
