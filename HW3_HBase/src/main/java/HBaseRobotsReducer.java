import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseRobotsReducer extends TableReducer<HostFromSitesOrPagesPair, Text, ImmutableBytesWritable> {
    private static byte[] docsCF = Bytes.toBytes("docs");
    private static byte[] disabledColumn = Bytes.toBytes("disabled");

    @Override
    protected void reduce(HostFromSitesOrPagesPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> markedUrls = new ArrayList<>();
        Robots rules = new Robots();
        for (Text value: values) {
            if (key.isFromPages()) {
                markedUrls.add(value.toString());
            } else if (key.isFromSites()) {
                try {
                    rules.add(value.toString());
                } catch (Robots.BadRobotsFormatException exception) {
                    context.getCounter("COMMON_COUNTERS", "BadRobotsFormat").increment(1);
                    return; //OR COMPARE?
                }
            }
        }
        for (String markedUrlString: markedUrls) {
            String url = markedUrlString.substring(1);
            boolean isDisallowedInRobots = rules.isDisallowed(url);
            boolean isDisallowedInTable = markedUrlString.substring(0,1).equals("Y");
            //TableKey
            byte[] UrlKey = Bytes.toBytes(MD5Hash.digest(url).toString());
            if (!isDisallowedInTable && isDisallowedInRobots) {
                Put put = new Put(UrlKey).addColumn(docsCF, disabledColumn, Bytes.toBytes("Y"));
                context.write(null, put);
            } else if (isDisallowedInTable && !isDisallowedInRobots) {
                Delete delete = new Delete(UrlKey).addColumn(docsCF, disabledColumn);
                context.write(null, delete);
            }
        }
    }
}
