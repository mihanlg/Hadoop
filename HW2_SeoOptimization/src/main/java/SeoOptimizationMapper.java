import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class SeoOptimizationMapper extends Mapper<LongWritable, Text, HostQueryPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] queryURL = line.split("\t");

        if (queryURL.length == 2) {
            try {
                URL url = new URL(queryURL[1]);
                context.write(new HostQueryPair(url.getHost(), queryURL[0]), one);
            } catch (MalformedURLException error) {
                context.getCounter("COMMON_COUNTERS", "BadURLs").increment(1);
            }
        } else {
            context.getCounter("COMMON_COUNTERS", "BadPair").increment(1);
        }
    }
}
