import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

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
		
		int rows = conf.getInt("K", 5);
		
		if (job.waitForCompletion(true)){
			HTable hTable = new HTable(conf, "0801466p");
			
			ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q4"));
			
			List<Result> results = new ArrayList<Result>();

			for (Result res : scanner) {
				results.add(res);
			}
				
				//byte[] artid = res.getRow();
				//byte[] artid = res.getValue(Bytes.toBytes("q1"),Bytes.toBytes("art_id"));
				//byte[] sum = res.getValue(Bytes.toBytes("q4"),Bytes.toBytes("sum"));
				
				//System.out.println(Bytes.toLong(artid) + " " + Bytes.toInt(sum) + "\n");
				//if (--rows == 0)
				//	break;
			
			class ResultComparator implements Comparator<Result>{
				@Override
				public int compare(Result arg0, Result arg1) {
					int sum1 = Bytes.toInt(arg0.getValue(Bytes.toBytes("q4"),Bytes.toBytes("sum")));
					int sum2 = Bytes.toInt(arg1.getValue(Bytes.toBytes("q4"),Bytes.toBytes("sum")));
					long art1 = Bytes.toLong(arg0.getRow());
					long art2 = Bytes.toLong(arg1.getRow());
					return 	sum1 > sum2 ? 1 : 
							sum1 < sum2 ? -1 :
							art1 > art2 ? -1 :
							1;
					
					
				}
			}

			Collections.sort(results, new ResultComparator());
			
			Collections.reverse(results);
			
			for (Result res: results){
				byte[] artid = res.getRow();
				byte[] sum = res.getValue(Bytes.toBytes("q4"),Bytes.toBytes("sum"));
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
