package org.hadoop.hbase.dataExport.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.Text;

public interface ExportFormatInterface {

	/**
	 * To initialize the append class with given configuration.
	 * @param conf Configurations.
	 */
	public void init(Configuration conf);

	/**
	 * Get information from input Cell and append those into one line.
	 * For one input cell, if the rowkey has changed then return the last one.
	 * And record current key as the start of new record.
	 * @param kv Input Cell.
	 * @return Appended string if switch get a new key. Null otherwise.
	 * @throws IOException
	 */
	public String export(Cell kv) throws IOException;

	/**
	 * Construct one string with stored columns.
	 * @return Appended string.
	 */
	public String finishExport() throws IOException;
}
