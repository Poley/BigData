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
		TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
	public void reduce(ImmutableBytesWritable key,
			Iterable<ImmutableBytesWritable> values, Context context) throws IOException,
			InterruptedException {
		
		Put put = new Put(key.get());
		//put.add(Bytes.toBytes("q1"), Bytes.toBytes("art_id"), artid);
		
		for (ImmutableBytesWritable v : values) {

			byte[] revid = v.get();
			put.add(Bytes.toBytes("q1"), (Bytes.toBytes("REV")), revid);
			System.out.println(Bytes.toLong(key.get())
					+ " " + Bytes.toLong(v.get()));
			context.write(null, put);
			
		}
		
	}
}
