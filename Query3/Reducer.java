import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class Reducer
		extends
		TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
	public void reduce(ImmutableBytesWritable key,
			Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		int n = conf.getInt("N", 5);

		Put put = new Put(key.get());
		// put.add(Bytes.toBytes("q1"), Bytes.toBytes("art_id"), artid);
		int sum = 0;

		for (IntWritable v : values) {
			sum += v.get();
		}

		put.add(Bytes.toBytes("q3"), (Bytes.toBytes("sum")),
				(Bytes.toBytes(sum)));
		System.out.println(Bytes.toLong(key.get()) + " " + sum);
		
		if (sum>n)
			context.write(null, put);

	}
}
