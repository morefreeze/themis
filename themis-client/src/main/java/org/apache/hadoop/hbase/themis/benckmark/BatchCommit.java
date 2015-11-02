package org.apache.hadoop.hbase.themis.benckmark;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.LocalTimestampOracle;
import org.apache.hadoop.hbase.util.Bytes;

public class BatchCommit {
  private static final byte[] BATCHTABLE = Bytes.toBytes("Themis_Bench"); // test
                                                                          // table
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] COL_CASH = Bytes.toBytes("cash");
  private static Configuration conf;

  private static ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(200);

  static {
    pool.setKeepAliveTime(1, TimeUnit.SECONDS);
    pool.allowCoreThreadTimeOut(true);
  }

  protected static void createTable(HConnection connection) throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
      if (!admin.tableExists(BATCHTABLE)) {
        HTableDescriptor tableDesc = new HTableDescriptor(BATCHTABLE);
        HColumnDescriptor themisCF = new HColumnDescriptor(FAMILY);
        // set THEMIS_ENABLE to allow Themis transaction on this family
        themisCF.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, "true");
        tableDesc.addFamily(themisCF);
        // the splits makes rows of Joe and Bob located in different regions
        admin.createTable(tableDesc);
      } else {
        System.out.println(Bytes.toString(BATCHTABLE) + " exist, won't create, please check the schema of the table");
        if (!admin.isTableEnabled(BATCHTABLE)) {
          admin.enableTable(BATCHTABLE);
        }
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  protected static void dropTable(HConnection connection) throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
      if (admin.tableExists(BATCHTABLE)) {
        admin.disableTable(BATCHTABLE);
        admin.deleteTable(BATCHTABLE);
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  public static void main(String args[]) throws IOException {
    System.out.println("\n############################The Themis Example###########################\n");
    conf = HBaseConfiguration.create();
    conf.setBoolean(TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC, true);
    final HConnection connection = HConnectionManager.createConnection(conf);
    dropTable(connection);
    createTable(connection);

    String timeStampOracleCls = conf.get(TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY,
        LocalTimestampOracle.class.getName());
    System.out.println("use timestamp oracle class : " + timeStampOracleCls);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 50000; i++) {
      pool.execute(new Task(i, connection));
    }

    try {
      pool.shutdown();
      pool.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println("-------------" + (System.currentTimeMillis() - start) / 1000);
    connection.close();
    Transaction.destroy();
    System.out.println("\n###############################Example End###############################");
  }

  static class Task implements Runnable {
    private int i;
    private HConnection con;

    public Task(int i, HConnection con) {
      this.i = i;
      this.con = con;
    }

    public void run() {
      try {
        Transaction transaction = new Transaction(conf, con);
        ThemisPut put = new ThemisPut(("row_a_" + i).getBytes()).add(FAMILY, COL_CASH, Bytes.toBytes(20));
        ThemisPut put2 = new ThemisPut(("row_b_" + i).getBytes()).add(FAMILY, COL_CASH, Bytes.toBytes(20));
        ThemisPut put3 = new ThemisPut(("row_c_" + i).getBytes()).add(FAMILY, COL_CASH, Bytes.toBytes(20));
        ThemisPut put4 = new ThemisPut(("row_d_" + i).getBytes()).add(FAMILY, COL_CASH, Bytes.toBytes(20));
        transaction.put(BATCHTABLE, put);
        transaction.put(BATCHTABLE, put2);
        transaction.put(BATCHTABLE, put3);
        transaction.put(BATCHTABLE, put4);

        transaction.commit();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
