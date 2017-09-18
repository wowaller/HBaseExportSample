package org.hadoop.hbase.dataExport.sequence.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The mapper basically do nothing but write data directly to output.
 * Filters are all done through scanner.
 */
public class HbaseExportMapper extends
	Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result> {

    private Configuration conf;

    enum MyCounter {
		RECORDCOUNT
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
	    throws IOException, InterruptedException {
		context.write(key, value);
    }

    @Override
    protected void setup(Context context) throws IOException,
	    InterruptedException {
		super.setup(context);
		conf = context.getConfiguration();
    }

}
