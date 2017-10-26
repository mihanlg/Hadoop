import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SeoOptimizationJob extends Configured implements Tool {

    public static Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SeoOptimizationJob.class);
        job.setJobName("hadoop_hw2: SeoOptimization");

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SeoOptimizationMapper.class);
        job.setCombinerClass(SeoOptimizationCombiner.class);
        job.setReducerClass(SeoOptimizationReducer.class);

        job.setPartitionerClass(QueryPartition.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(HostGrouper.class);

        job.setMapOutputKeyClass(HostQueryPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);

        return job;
    }

    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("program arguments: 'input directory' 'output directory'");
            System.exit(1);
        }
        int ret = ToolRunner.run(new SeoOptimizationJob(), args);
        System.exit(ret);
    }

}
