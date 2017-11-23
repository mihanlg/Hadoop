import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseRobotsJob extends Configured implements Tool {
    public static final String PAGES_TABLE = "robotsfilter.webpages";
    public static final String SITES_TABLE = "robotsfilter.websites";

    private Job getJobConf(String webPages, String webSites) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HBaseRobotsJob.class);
        job.setJobName("hadoop_hw3: HBaseRobots");
        job.setNumReduceTasks(2);

        List<Scan> scans = new ArrayList<>();

        Scan scanPages = new Scan();
        scanPages.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("url"))
                 .addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
        scanPages.setAttribute("scan.attributes.table.name", Bytes.toBytes(webPages));
        scans.add(scanPages);

        Scan scanSites = new Scan();
        scanSites.addColumn(Bytes.toBytes("info"), Bytes.toBytes("site"))
                 .addColumn(Bytes.toBytes("info"), Bytes.toBytes("robots"));
        scanSites.setAttribute("scan.attributes.table.name", Bytes.toBytes(webSites));
        scans.add(scanSites);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                HBaseRobotsMapper.class,
                HostFromSitesOrPagesPair.class,
                Text.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                webPages,
                HBaseRobotsReducer.class,
                job);

        Configuration config = job.getConfiguration();
        config.set(PAGES_TABLE, webPages);
        config.set(SITES_TABLE, webSites);

        job.setPartitionerClass(HostPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(HostGrouper.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("program arguments: 'webpages table', 'websites table'");
            System.exit(1);
        }
        int ret = ToolRunner.run(new HBaseRobotsJob(), args);
        System.exit(ret);
    }
}
