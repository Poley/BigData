import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class Reducer2
		extends
		TableReducer<IntWritable, CombinedValue, ImmutableBytesWritable> {
	public void reduce(IntWritable key,
			Iterable<CombinedValue> values, Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		int k = conf.getInt("K", 10);
		
		List<CombinedValue> comvalues = new ArrayList<CombinedValue>();
		
		for (CombinedValue cv : values) {
			comvalues.add(cv);
		}
		
		for (CombinedValue cv : comvalues) {
			System.out.println(cv.toString());
		}
		
		Collections.sort(comvalues, CombinedValue.CombinedValueComparator);
		
		System.out.println("comvalues sorted");
		
		for (CombinedValue cv : comvalues) {
			System.out.println(cv.toString());
		}


//		HTable hTable = new HTable(conf, "0801466p");
//        
//        ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q4"));
//            for (Result res2 : scanner) {
//                Delete rowDel = new Delete(res2.getRow());
//                hTable.delete(rowDel);
//            }
//        scanner.close();
//        hTable.close();
        
		for (CombinedValue cv : comvalues) {
			System.out.println(cv.toString());
			Put put = new Put(Bytes.toBytes(cv.getArticle_id()));
			put.add(Bytes.toBytes("q5"), (Bytes.toBytes("sum")),
					(Bytes.toBytes(cv.getCount())));
			System.out.println(cv.getArticle_id() + " " + cv.getCount());
			context.write(null, put);
			if (--k ==0)
				break;
		}

			

	}
}