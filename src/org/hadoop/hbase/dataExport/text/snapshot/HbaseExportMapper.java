package org.hadoop.hbase.dataExport.text.snapshot;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.hbase.dataExport.format.ExportFormatInterface;

/**
 * Mapper to read from hbase snapshot and construct string through ExportFormat Interface.
 * Output of the Mapper is Text file.
 */
public class HbaseExportMapper extends
	Mapper<ImmutableBytesWritable, Result, NullWritable, Text> {
    public static String COLUMN = "column";
    public static String EXPORT_CLASS = "exportClass";
    public static String DEFAULT_EXPORT_CLASS = "org.hadoop.hbase.dataExport.format.AppendKeyTSFormat";

    private Configuration conf;
    private ExportFormatInterface export;

    enum MyCounter {
		RECORDCOUNT
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
	    throws IOException, InterruptedException {
		Text outText = new Text();

		String data = export.finishExport();
		if (data != null) {
			outText.set(data);
			context.write(NullWritable.get(), outText);
		}
    }

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {
		super.setup(context);
		conf = context.getConfiguration();

		String exportString = conf.get(EXPORT_CLASS, DEFAULT_EXPORT_CLASS);
		try {
			export = (ExportFormatInterface) Class.forName(exportString)
				.newInstance();
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
