package org.hadoop.hbase.dataExport.format;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The class implements Append interface.
 * Use export method to append all columns into one line delimited with given character.
 * Compared to AppendFormat, it will use key as first field in the output line.
 */
public class AppendKeyTSFormat implements ExportFormatInterface {
	static final Log LOG = LogFactory.getLog(AppendKeyTSFormat.class);

	public static String COLUMN = "column";
	public static String DELIMITER = "delimiter";

	private Map<Text, Integer> qualifierMap;
	private String[] resultBuf;
	private Text currentKey;
	private Text tmp;
	private String delimiter;

	/**
	 * To initialize the append class with given configuration.
	 * @param conf Configurations.
	 */
	@Override
	public void init(Configuration conf) {
		String[] columns = conf.getStrings(COLUMN);
		qualifierMap = new HashMap<Text, Integer>();
		for (int i = 0; i < columns.length; i++) {
			qualifierMap.put(new Text(columns[i]), i);
		}
		resultBuf = new String[qualifierMap.values().size()];
		currentKey = null;
		delimiter = conf.get(DELIMITER);
		tmp = new Text();
	}

	/**
	 * Get information from input Cell and append those into one line.
	 * For one input cell, if the rowkey has changed then return the last one.
	 * And record current key as the start of new record.
	 * @param kv Input Cell.
	 * @return Appended string if switch get a new key. Null otherwise.
	 * @throws IOException
	 */
	@Override
	public String export(Cell kv) throws IOException {
		Text key = new Text(CellUtil.cloneRow(kv));
		if (currentKey == null) {
			currentKey = key;
			record(kv);
			return null;
		}
		else if (currentKey.equals(key)) {
			record(kv);
			return null;
		}
		else {
			StringBuilder sb = new StringBuilder();
			sb.append(key.toString());
			for (int i = 0; i < resultBuf.length; i++) {
				sb.append(delimiter);
				sb.append(resultBuf[i]);
				resultBuf[i] = null;
			}
			currentKey = key;
			record(kv);
			return sb.toString();
		}
	}

	/**
	 * Construct one string with stored columns.
	 * @return Appended string.
	 */
	public String finishExport() {
		StringBuilder sb = new StringBuilder();
		sb.append(currentKey.toString());
		for (int i = 0; i < resultBuf.length; i++) {
			sb.append(delimiter);
			sb.append(resultBuf[i]);
			resultBuf[i] = null;
		}
		return sb.toString();
	}

	/**
	 * Store value in one Cell for further string construction.
	 * @param kv Input Cell.
	 */
	public void record(Cell kv) {
		tmp.set(CellUtil.cloneQualifier(kv));
		Integer index = qualifierMap.get(tmp);
		if (index != null) {
//			LOG.info("Get value for qualifier " + tmp.toString());
			resultBuf[index] = new String(CellUtil.cloneValue(kv));
		}
	}

}
