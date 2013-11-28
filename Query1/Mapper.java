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

	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		
		Date startDate = null;
		Date endDate = null;
		Date revisionDate = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); //Format for timestamp

		Configuration conf = context.getConfiguration();
		String start = conf.get("start");
		String end = conf.get("end");

		try {
			startDate = sdf.parse(start);
			endDate = sdf.parse(end);
		} 
		catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] rowkey = value.getRow();
		
		for(KeyValue k: value.list()){			
			revisionDate = new Date(k.getTimestamp()); break;
		}
		
		if (rowkey != null && (startDate.before(revisionDate) && revisionDate.before(endDate)))
			context.write(new ImmutableBytesWritable(rowkey), 
					new ImmutableBytesWritable(Arrays.copyOfRange(rowkey, 8, 16)));
		
	}
}

