# HBaseExportSample
Sample code of reading HBase batch data using snapshot.
It should be at least 2X faster than normal whole table scan.
And can be way much more faster than normal scan when regionserver is busy.

## Sequence File Export
Please check org.hadoop.hbase.sequence.snapshot for export HBase table as sequence file.

Usage is similar to HBase table export command. And it can also be recognized by HBase builtin table import command.

## Text File Export
On the other hand, there are sample code to export table into Text. It will format the output with given format. 

This is basically a MR job so you may also try to implement mapper with your own.
And it's implemented with basic Hadoop API so it should work with Spark HadoopRDD.