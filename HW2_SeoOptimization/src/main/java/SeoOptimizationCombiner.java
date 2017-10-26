import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SeoOptimizationCombiner extends Reducer<HostQueryPair, IntWritable, HostQueryPair, IntWritable> {
    @Override
    protected void reduce(HostQueryPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text host = key.getHost();
        Text prevQuery = new Text();
        int sum = 0;

        for (IntWritable value: values) {
            Text query = key.getQuery();
            if (query.equals(prevQuery)) {
                sum += value.get();
            }
            else {
                if (sum != 0) {
                    context.write(new HostQueryPair(host, prevQuery), new IntWritable(sum));
                }
                prevQuery.set(query);
                sum = value.get();
            }
        }
        if (sum > 0) {
            context.write(new HostQueryPair(host, prevQuery), new IntWritable(sum));
        }
    }

}
