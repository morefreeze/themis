package org.apache.hadoop.hbase.themis;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestTransactionLockRow extends ClientTestBase {
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
  }

  @Test
  public void testLockRow() throws IOException {
    mockTimestamp(commitTs);
    ThemisPut put = getThemisPut(COLUMN);
    transaction.put(TABLENAME, put);
    transaction.commit();

    mockTimestamp(++commitTs);
    transaction = new Transaction(conf, connection, mockTimestampOracle, mockRegister);
    Result r = transaction.get(TABLENAME, getThemisGet(COLUMN));
    Assert.assertFalse(r.isEmpty());

    Get g = new Get(put.getRow());
    r = table.get(g);
    Assert.assertEquals(2, r.size());
    List<String> cols = new ArrayList<String>();
    cols.add("#p:ThemisCF#Qualifier");
    cols.add("ThemisCF:Qualifier");
    for(KeyValue kv : r.list()) {
      Assert.assertTrue(cols.contains(Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())));
    }

    mockTimestamp(++commitTs);
    transaction = new Transaction(conf, connection, mockTimestampOracle, mockRegister);
    transaction.lockRow(TABLENAME, put.getRow());
    transaction.primary = COLUMN;
    transaction.selectPrimaryAndSecondaries();
    transaction.prewritePrimary();
    transaction.prewriteSecondaries();
    g = new Get(put.getRow());
    r = table.get(g);
    Assert.assertEquals(3, r.size());
    cols = new ArrayList<String>();
    cols.add("L:ThemisCF#Qualifier");
    cols.add("#p:ThemisCF#Qualifier");
    cols.add("ThemisCF:Qualifier");
    for(KeyValue kv : r.list()) {
      Assert.assertTrue(cols.contains(Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())));
    }
    mockTimestamp(++commitTs);
    transaction.commitPrimary();
    transaction.batchCommitSecondaries();
    g = new Get(put.getRow());
    r = table.get(g);
    Assert.assertEquals(2, r.size());
    cols = new ArrayList<String>();
    cols.add("#p:ThemisCF#Qualifier");
    cols.add("ThemisCF:Qualifier");
    for(KeyValue kv : r.list()) {
      Assert.assertTrue(cols.contains(Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())));
    }

  }

  private void printAllData(byte[] row) throws IOException {
    Get g = new Get(row);
    Result r = table.get(g);
    printResult(r);
  }

  private void printResult(Result r) {
    for(KeyValue kv : r.list()) {
      System.out.println(kv.toString());
    }
  }

}
