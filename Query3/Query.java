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

public class Query extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());
		conf.addResource("all-client-conf.xml");
		conf.set("mapred.jar", "file:////users/level4/0801466p/Level4/BigData/Ex2/Query3/MyJar.jar");
		
		conf.set("start", args[0]);
		conf.set("end", args[1]);
		conf.set("N", args[2]);
		
		
		Job job = new Job(conf);
		job.setJarByClass(Query.class);

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
		scan.setCaching(100);
		scan.setCacheBlocks(false); // Always set this to false for MR jobs!

		TableMapReduceUtil.initTableMapperJob("BD4Project2Sample", scan,
				Mapper.class, ImmutableBytesWritable.class,
				IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("0801466p",
				Reducer.class, job);
		job.setNumReduceTasks(10);
		
		int rows = 10;
		
		if (job.waitForCompletion(true)){
			HTable hTable = new HTable(conf, "0801466p");
			
			ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q3"));

			for (Result res : scanner) {
				
				byte[] artid = res.getRow();
				//byte[] artid = res.getValue(Bytes.toBytes("q1"),Bytes.toBytes("art_id"));
				byte[] sum = res.getValue(Bytes.toBytes("q3"),Bytes.toBytes("sum"));
				//if ( Bytes.toInt(sum)>1)
					System.out.println(Bytes.toLong(artid) + " " + Bytes.toInt(sum) + "\n");
				if (--rows == 0)
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
		System.exit(ToolRunner.run(new Query(), args));
	}
}
