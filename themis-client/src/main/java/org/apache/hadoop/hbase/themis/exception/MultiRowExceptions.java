package org.apache.hadoop.hbase.themis.exception;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.themis.ConcurrentRowsCallables.TableAndRows;

public class MultiRowExceptions extends ThemisException {
  private static final long serialVersionUID = -5300909468331086844L;
  
  private Map<TableAndRows, IOException> exceptions;
  
  public MultiRowExceptions(String msg, Map<TableAndRows, IOException> exceptions) {
    super(msg + "\n" + constructMessage(exceptions));
    this.exceptions = exceptions;
  }

  public Map<TableAndRows, IOException> getExceptions() {
    return exceptions;
  }
  
  public static String constructMessage(Map<TableAndRows, IOException> exceptions) {
    String message = "";
    for (Entry<TableAndRows, IOException> rowException : exceptions.entrySet()) {
      message += ("tableAndRows=" + rowException.getKey() + ", exception=" + rowException.getValue() + "\n");
    }
    return message;
  }
}