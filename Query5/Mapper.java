import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class Mapper extends TableMapper<ImmutableBytesWritable,
ImmutableBytesWritable> {
	private static final IntWritable one = new IntWritable(1);
	long timestamp;
	
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		
		Date timeDate = null;
		Date revisionDate = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); //Format for timestamp

		Configuration conf = context.getConfiguration();
		String time = conf.get("time");

		try {
			timeDate = sdf.parse(time);
		} 
		catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] rowkey = value.getRow();
		
		for(KeyValue k: value.list()){
			timestamp = k.getTimestamp();
			revisionDate = new Date(timestamp); break;
		}
		
		byte[] art_id = Arrays.copyOfRange(rowkey, 0, 8);
		byte[] rev_id = Arrays.copyOfRange(rowkey, 8, 16);
		byte[] time_id = Bytes.toBytes(timestamp);
		
		byte[] output = Bytes.add(rev_id, time_id);
		
		if (rowkey != null && revisionDate.before(timeDate))
			context.write(new ImmutableBytesWritable(Arrays.copyOfRange(rowkey, 0, 8)), new ImmutableBytesWritable(output));
		
	}
}

