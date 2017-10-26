import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text word, Iterable<LongWritable> nums, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for(LongWritable i: nums) {
            sum += i.get();
        }
        context.write(word, new LongWritable(sum));
    }
}