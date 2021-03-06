import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyNameCounter extends Configured implements Tool {
	public int run(String[] arg0) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());
		conf.addResource("all-client-conf.xml");
		conf.set("mapred.jar", "file:////users/level4/0801466p/Level4/BigData/Ex2/Query1/MyJar.jar");
		
		Job job = new Job(conf);
		job.setJarByClass(MyNameCounter.class);

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
		scan.setCaching(100);
		scan.setCacheBlocks(false); // Always set this to false for MR jobs!

		TableMapReduceUtil.initTableMapperJob("BD4Project2Sample", scan,
				MyNameCounterMapper.class, ImmutableBytesWritable.class,
				IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("0801466p",
				MyNameCounterReducer.class, job);
		job.setNumReduceTasks(10);
		
		int i = 0;
		
		if (job.waitForCompletion(true)){
			HTable hTable = new HTable(conf, "0801466p");
			
			ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q1"));

			for (Result res : scanner) {
				i++;
				byte[] rowkey = res.getRow();
				byte[] sum = res.getValue(Bytes.toBytes("q1"),Bytes.toBytes("sum"));
				
				//if ( Bytes.toInt(sum)>1)
					System.out.println("ART_ID: " + Bytes.toLong(rowkey) + " " + Bytes.toInt(sum) + "\n");
				if (i > 10)
					break;

			}
			scanner.close();
			hTable.close();
			return 0;
		}
		else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyNameCounter(), args));
	}
}
