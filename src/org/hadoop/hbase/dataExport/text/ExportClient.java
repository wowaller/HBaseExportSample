package org.hadoop.hbase.dataExport.text;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Client to run MR job to export HBase records.
 * Job will get HBase data by reading HFile directly and export data into text file.
 */
@Deprecated
public class ExportClient {
	static final Log LOG = LogFactory.getLog(ExportClient.class);

	public static String TABLE_NAME = "tableName";
	public static String OUTPUT_PATH = "outputPath";
	
	public static String TMP_DIR = "tmpDir";
	public static String DEFAULT_TMP_DIR = "/user/tmp/dataExport";
	
	public static String REGION_PER_MAP = "regionPerMap";
	public static String DEFAULT_REGION_PER_MAP = "1";
	
	public static String TABLE_DIR = "tableDir";
	public static String DEFAULT_TABLE_DIR = "/hbase/data/default";
	
	public static String FAMILY = "family";
	
	private String tableName;
	private String outputPath;
	private int regionPerMap;
	private Path tmpFilePath;
	private String tmpDir;
	private Configuration conf;
	private String tableDir;
	private String family;
	
	public void init(Properties props, Configuration conf) {
		this.tableName = props.getProperty(TABLE_NAME);
		this.outputPath = props.getProperty(OUTPUT_PATH);
		this.conf = conf;
		this.tmpDir = props.getProperty(TMP_DIR, DEFAULT_TMP_DIR);
		this.tableDir = props.getProperty(TABLE_DIR);
		this.family = props.getProperty(FAMILY);
		if (this.tableDir == null) {
			this.tableDir = DEFAULT_TABLE_DIR + "/" + this.tableName;
		}
		this.regionPerMap = Integer.valueOf(props.getProperty(REGION_PER_MAP, DEFAULT_REGION_PER_MAP));
		DateFormat format = new SimpleDateFormat("YYMMDDHHMMSS");
		this.tmpFilePath = new Path(tmpDir, format.format(new Date()));
		
		for (Object key : props.keySet()) {
			if(key instanceof String) {
				String keyString = (String) key;
				this.conf.set(keyString, props.getProperty(keyString));
			}
		}
	}
	
	public void prepareFiles() {
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream output = fs.create(tmpFilePath);
			HBaseAdmin admin = new HBaseAdmin(conf);
			List<HRegionInfo> regions = admin.getTableRegions(tableName.getBytes());
			
			int count = 0;
			for(HRegionInfo region : regions) {
				Path regionDir = new Path(tableDir, region.getEncodedName());
				Path cfDir = new Path(regionDir, family);
				
				for (FileStatus file : fs.listStatus(cfDir)) {
					if (!file.isDirectory()) {
						count++;
//						LOG.info("Print " + file.getPath().toString());
						output.write(Bytes.toBytes(file.getPath().toString() + "\r\n"));
//						Text out = new Text(file.getPath().toString()+ "\n");
//						out.write(output);
					}
				}
			}
			output.flush();
			output.close();
			
			LOG.info("Found " + count + " input files.");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void submit() {
//		SchemaMetrics.configureGlobally(conf);
		prepareFiles();
		try {
			Job job = new Job(conf, "HBase Export");
			job.setJarByClass(ExportClient.class);
			job.setMapperClass(HbaseExportMapper.class);
			FileInputFormat.setInputPaths(job, tmpFilePath);		
			job.setNumReduceTasks(0);
			
			job.setInputFormatClass(NLineInputFormat.class);
			NLineInputFormat.setNumLinesPerSplit(job, regionPerMap);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {

		
		Configuration conf = HBaseConfiguration.create();
				
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	    
	    //set the configuration back, so that Tool can configure itself
	    
	    args = parser.getRemainingArgs();
	    
		if (args.length < 1) {
			LOG.error("Usage: <prperties-file>");
			System.exit(0);
		}
		
		ExportClient client = new ExportClient();
		Properties props = new Properties();
		props.load(new FileReader(args[0]));
		
		client.init(props, conf);
		client.submit();
	}
}
