import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class MyNameCounterMapper extends TableMapper<ImmutableBytesWritable,
IntWritable> {
	private static final IntWritable one = new IntWritable(1);

	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		byte[] name = value.getValue(Bytes.toBytes("WD"), Bytes.toBytes("TITLE"));
		byte[] rowkey = value.getRow();
		if (name != null)
			context.write(new ImmutableBytesWritable(name), one);
	}
}

