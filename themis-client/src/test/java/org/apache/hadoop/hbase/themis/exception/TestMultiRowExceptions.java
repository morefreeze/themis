package org.apache.hadoop.hbase.themis.exception;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.themis.ConcurrentRowsCallables.TableAndRows;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Test;

public class TestMultiRowExceptions extends TestBase {
  @Test
  public void testConstructMessage() {
    Map<TableAndRows, IOException> exceptions = new TreeMap<TableAndRows, IOException>();
    List<byte[]> rows = new ArrayList<byte[]>();
    rows.add(ROW);
    exceptions.put(new TableAndRows(TABLENAME, rows), new IOException("exceptionA"));
    
    new ArrayList<byte[]>();
    rows.add(ANOTHER_ROW);
    exceptions.put(new TableAndRows(TABLENAME, rows), new IOException("exceptionB"));
    System.out.println(MultiRowExceptions.constructMessage(exceptions));
  }
}
