package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.themis.ConcurrentRowsCallables.TableAndRows;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.util.Bytes;

abstract class RowsCallable<R> implements Callable<R> {
  private TableAndRows tableAndRows;

  public RowsCallable(byte[] tblName, List<byte[]> rows) {
	this.tableAndRows = new TableAndRows(tblName, rows);
  }
  
  public TableAndRows getTableAndRows() {
    return this.tableAndRows;
  }
}

public class ConcurrentRowsCallables<R> {
	public static class TableAndRows implements Comparable<TableAndRows> {
		private byte[] tableName;
		private List<byte[]> rows;

		public TableAndRows(byte[] tableName, List<byte[]> rows) {
			this.tableName = tableName;
			if (tableName == null || tableName.length == 0) {
				throw new IllegalArgumentException("must has tableName!");
			}

			this.rows = rows;
			if (rows == null || rows.size() == 0) {
				throw new IllegalArgumentException("must has row key, tableName:" + Bytes.toString(tableName));
			}
		}

		public byte[] getTableName() {
			return this.tableName;
		}

		public List<byte[]> getRowkeys() {
			return this.rows;
		}

		@Override
		public int compareTo(TableAndRows other) {
			int cmp = Bytes.compareTo(tableName, other.tableName);
			if (cmp == 0) {
				if (rows == other.getRowkeys()) {
					return 0;
				}

				int size = rows.size();
				int otherSize = other.getRowkeys().size();
				if (size != otherSize) {
					return size - otherSize;
				}

				for (int i = 0; i < size; i++) {
					int cR = Bytes.compareTo(rows.get(i), other.getRowkeys().get(i));
					if (cR != 0) {
						return cR;
					}
				}
			}

			return cmp;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			if (tableName != null) {
				result = prime * result + Bytes.toString(tableName).hashCode();
			}

			result = prime * result;
			int size = rows.size();
			for (int i = 0; i < size; i++) {
				result += (i + 1) * Bytes.toString(rows.get(i)).hashCode(); // (i+1)* ： same order
			}

			return result;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof TableAndRows)) {
				return false;
			}

			TableAndRows tableAndRows = (TableAndRows) other;
			if (!Bytes.equals(this.getTableName(), tableAndRows.getTableName())) {
				return false;
			}

			if (rows == tableAndRows) {
				return true;
			}

			int size = rows.size();
			int otherSize = tableAndRows.getRowkeys().size();
			if (size != otherSize) {
				return false;
			}

			for (int i = 0; i < size; i++) {
				int cR = Bytes.compareTo(rows.get(i), tableAndRows.getRowkeys().get(i));
				if (cR != 0) {
					return false;
				}
			}

			return true;
		}

		public String toString() {
			StringBuilder rv = new StringBuilder("tableName=" + Bytes.toString(tableName) + "/rowkeys=");
			for (byte[] row : rows) {
				rv.append(Bytes.toString(row)+ ",");
			}

			return rv.toString();
		}
	}
	
	private final ExecutorService threadPool;
	Map<TableAndRows, Future<R>> futureMaps = new TreeMap<TableAndRows, Future<R>>();
	Map<TableAndRows, R> resultMaps = new TreeMap<TableAndRows, R>();
	Map<TableAndRows, IOException> exceptionMaps = new TreeMap<TableAndRows, IOException>();

	public ConcurrentRowsCallables(ExecutorService threadPool) {
	   this.threadPool = threadPool;
	}

	public void addCallable(RowsCallable<R> callable) throws IOException {
		TableAndRows tblAndRows = callable.getTableAndRows();
		if (this.futureMaps.containsKey(tblAndRows) || this.exceptionMaps.containsKey(tblAndRows)) {
			throw new ThemisFatalException("add duplicated tableAndRows callable, tableAndRows=" + tblAndRows);
		}

		try {
			Future<R> future = this.threadPool.submit(callable);
			this.futureMaps.put(tblAndRows, future);
		} catch (Throwable e) {
			exceptionMaps.put(tblAndRows, new IOException(e));
		}
	}

	public void waitForResult() {
		for (Entry<TableAndRows, Future<R>> entry : this.futureMaps.entrySet()) {
			TableAndRows tblAndRows = entry.getKey();
			try {
				R result = entry.getValue().get();
				this.resultMaps.put(tblAndRows, result);
			} catch (Exception e) {
				this.exceptionMaps.put(tblAndRows, new IOException(e));
			}
		}
	}

	public Map<TableAndRows, R> getResults() {
		return resultMaps;
	}

	public R getResult(byte[] tableName, List<byte[]> rowkey) {
		return resultMaps.get(new TableAndRows(tableName, rowkey));
	}

	public Map<TableAndRows, IOException> getExceptions() {
		return exceptionMaps;
	}

	public IOException getException(byte[] tableName, List<byte[]> rowkey) {
		return exceptionMaps.get(new TableAndRows(tableName, rowkey));
	}
}