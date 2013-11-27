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

public class MyNameCounterReducer
		extends
		TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
	public void reduce(ImmutableBytesWritable key,
			Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;
		for (IntWritable v : values)
			sum += v.get();
		Put put = new Put(key.get());
		byte[] rowkey = key.get();
		put.add(Bytes.toBytes("q1"), key.get(), rowkey);
		put.add(Bytes.toBytes("q1"), key.get(), Bytes.toBytes(sum));
		System.out.println(Bytes.toString(key.get()) + " " + sum);
		context.write(null, put);
	}
}
