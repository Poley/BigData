import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

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
	
	byte[] rev_id;
	byte[] timestamp;
	byte[] final_rev_id;
	
	public void reduce(ImmutableBytesWritable key,
			Iterable<ImmutableBytesWritable> values, Context context) throws IOException,
			InterruptedException {
		
		Date currentDate = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date finalDate = null;

		Put put = new Put(key.get());
		// put.add(Bytes.toBytes("q1"), Bytes.toBytes("art_id"), artid);
		
		

		for (ImmutableBytesWritable v : values) {
			byte [] rowkey = v.get();
			rev_id = Arrays.copyOfRange(rowkey, 0, 8);
			timestamp = Arrays.copyOfRange(rowkey, 8, 16);
			currentDate = new Date(Bytes.toLong(timestamp));
			if (finalDate == null){
				final_rev_id = rev_id;
				finalDate = currentDate;
			}
			else if (finalDate.before(currentDate)){
				final_rev_id = rev_id;
				finalDate = currentDate;
			}
		}
		
		String date = sdf.format(finalDate);

		put.add(Bytes.toBytes("q5"), (Bytes.toBytes("rev")), final_rev_id);
		put.add(Bytes.toBytes("q5"), (Bytes.toBytes("time")), Bytes.toBytes(date)) ;
		System.out.println(Bytes.toLong(key.get()) + " " + Bytes.toLong(rev_id) + " " + date);
		
		context.write(null, put);

	}
}
