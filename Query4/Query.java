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
		conf.set("mapred.jar", "file:////users/level4/0801466p/Level4/BigData/Ex2/Query4/MyJar.jar");
		
		conf.set("start", args[0]);
		conf.set("end", args[1]);
		conf.set("K", args[2]);
		
		
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
		job.setNumReduceTasks(4);
		
		job.waitForCompletion(true);
		
		Job job2 = new Job(conf);
		job2.setJarByClass(Query.class);
		
		Scan scan2 = new Scan();
		scan2.addColumn(Bytes.toBytes("q4"), Bytes.toBytes("sum"));
		scan2.setCaching(100);
		scan2.setCacheBlocks(false); // Always set this to false for MR jobs!
		
		TableMapReduceUtil.initTableMapperJob("0801466p", scan2,
				Mapper2.class, IntWritable.class,
				CombinedValue.class, job2);
		TableMapReduceUtil.initTableReducerJob("0801466p",
				Reducer2.class, job2);
		job2.setNumReduceTasks(1);
		
		int rows = 10;
		
		if (job2.waitForCompletion(true)){
			HTable hTable = new HTable(conf, "0801466p");
			
			ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q5"));

			for (Result res : scanner) {
				
				byte[] artid = res.getRow();
				//byte[] artid = res.getValue(Bytes.toBytes("q1"),Bytes.toBytes("art_id"));
				byte[] sum = res.getValue(Bytes.toBytes("q5"),Bytes.toBytes("count"));
				//if ( Bytes.toInt(sum)>1)
					System.out.println(Bytes.toInt(artid) + " " + Bytes.toInt(sum) + "\n");
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
