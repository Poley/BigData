
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

public class Mapper2 extends TableMapper<IntWritable,
CombinedValue> {
	private static final IntWritable one = new IntWritable(1);

	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		
		byte[] article_id = value.getRow();
		byte[] count = value.getValue(Bytes.toBytes("q4"), Bytes.toBytes("sum"));
		int aid = Bytes.toInt(article_id);
		int sum = Bytes.toInt(count);
		
		CombinedValue cv = new CombinedValue(aid, sum);
		System.out.println(cv.toString());
		
		context.write(one, cv);
		
	}
}