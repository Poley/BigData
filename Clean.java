import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Clean extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("all-client-conf.xml");
        String outputJar = "file://" + System.getProperty("user.dir")
                           + "/Run.jar";
        conf.set("mapred.jar", outputJar);

        ArrayList<String> toCheck = new ArrayList<String>();

        if (args.length == 0) {
            toCheck.add("q1");
            toCheck.add("q2");
            toCheck.add("q3");
            toCheck.add("q4");
            toCheck.add("q5");
        } else {
            for (String s : args) {
                toCheck.add(s);
            }
        }

        HTable hTable = new HTable(conf, "0801466p");
        for (String s : toCheck) {
            System.out.println("Cleaning " + s);
            ResultScanner scanner = hTable.getScanner(Bytes.toBytes(s));
            for (Result res2 : scanner) {
                Delete rowDel = new Delete(res2.getRow());
                hTable.delete(rowDel);
            }
            System.out.println("Emptied " + s);
            scanner.close();
        }

        hTable.close();

        return 0;

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Clean(), args));
    }
}
