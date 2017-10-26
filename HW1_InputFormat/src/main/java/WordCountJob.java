import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountJob extends Configured implements Tool {

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCountJob.class);
        job.setJobName("hadoop_hw1: InputFormat");

        job.setInputFormatClass(IndexedGzipInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("program arguments: 'input directory', 'output directory'");
            System.exit(1);
        }
        int ret = ToolRunner.run(new WordCountJob(), args);
        System.exit(ret);
    }
}
