import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

public class HBaseRobotsMapper extends TableMapper<HostFromSitesOrPagesPair, Text> {
    private byte[] webPagesTable, webSitesTable;
    private static byte[] docsCF = Bytes.toBytes("docs");
    private static byte[] infoCF = Bytes.toBytes("info");
    private static byte[] urlColumn = Bytes.toBytes("url");
    private static byte[] disabledColumn = Bytes.toBytes("disabled");
    private static byte[] siteColumn = Bytes.toBytes("site");
    private static byte[] robotsColumn = Bytes.toBytes("robots");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        webPagesTable = Bytes.toBytes(context.getConfiguration().get(HBaseRobotsJob.PAGES_TABLE));
        webSitesTable = Bytes.toBytes(context.getConfiguration().get(HBaseRobotsJob.SITES_TABLE));
    }

    @Override
    protected void map(ImmutableBytesWritable rowKey, Result columns, Context context)
            throws IOException, InterruptedException {
        TableSplit currentSplit = (TableSplit)context.getInputSplit();
        byte[] tableName = currentSplit.getTableName();
        if (Arrays.equals(tableName, webPagesTable)) {
            String urlString = new String(columns.getValue(docsCF, urlColumn));
            String isDisabledString = columns.getValue(docsCF, disabledColumn) == null ? "N" : "Y";
            try {
                URL url = new URL(urlString);
                context.write(HostFromSitesOrPagesPair.fromPages(url.getHost()),
                              new Text(isDisabledString + urlString));
            } catch (MalformedURLException exception) {
                context.getCounter("COMMON_COUNTERS", "BadURLs").increment(1);
            }
        } else if (Arrays.equals(tableName, webSitesTable)) {
            byte[] robots = columns.getValue(infoCF, robotsColumn);
            byte[] host = columns.getValue(infoCF, siteColumn);
            if (robots != null && host != null) {
                String hostString = new String(host);
                context.write(HostFromSitesOrPagesPair.fromSites(hostString),
                              new Text(robots));
            } else {
                context.getCounter("COMMON_COUNTERS", "EmptyHostOrRobots").increment(1);
            }
        }
    }
}
