package org.hadoop.hbase.dataExport.text;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.hbase.dataExport.format.AppendFormat;
import org.hadoop.hbase.dataExport.format.ExportFormatInterface;

/**
 * The mapper will open a reader to each HFile.
 * Each line of mapper input is a HFile location.
 */
@Deprecated
public class HbaseExportMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public static String COLUMN = "column";
	public static String EXPORT_CLASS = "exportClass";
	public static String DEFAULT_EXPORT_CLASS = "org.hadoo.hbase.dataExport.format.AppendFormat";
	
	private String[] columns;
	private Configuration conf;
	private ExportFormatInterface export;
	private FileSystem fs;
	private CacheConfig cacheConfig;
	
	enum MyCounter {
		RECORDCOUNT
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		Path file = new Path(value.toString());
		HFile.Reader reader = HFile.createReader(fs, file, cacheConfig, conf);
		reader.loadFileInfo();
		HFileScanner scanner = reader.getScanner(false, false);
		scanner.seekTo();
		Text outText = new Text();
		
		while (scanner.next()) {
			Cell kv = scanner.getKeyValue();
			String data = export.export(kv);
			if (data != null) {
				outText.set(data);
				context.write(NullWritable.get(), outText);
			}
		}
		String data = export.finishExport();
		if (data != null) {
			outText.set(data);
			context.write(NullWritable.get(), outText);
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		conf = context.getConfiguration();
		fs = FileSystem.get(conf);
		cacheConfig = new CacheConfig(conf);
		String exportString = conf.get(EXPORT_CLASS, DEFAULT_EXPORT_CLASS);
		try {
			export = (ExportFormatInterface) Class.forName(exportString).newInstance();
			export.init(conf);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	
}
