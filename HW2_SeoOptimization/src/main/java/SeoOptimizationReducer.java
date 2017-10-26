import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SeoOptimizationReducer extends Reducer<HostQueryPair, IntWritable, Text, Text> {
    private int minClicks = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        minClicks = context.getConfiguration().getInt("seo.minclicks", minClicks);
    }

    @Override
    protected void reduce(HostQueryPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text host = key.getHost();
        String topQuery = "", prevQuery = "";
        int topTimes = 0, times = 0;

        for (IntWritable value: values) {
            int n = value.get();
            String query = key.getQuery().toString();

            if (query.equals(prevQuery)) {
                times += n;
            }
            else {
                if (times > topTimes) {
                    topQuery = prevQuery;
                    topTimes = times;
                } else if (times == topTimes && prevQuery.compareTo(topQuery) < 0) {
                    topQuery = prevQuery;
                }
                times = n;
                prevQuery = query;
            }
        }
        if (times > topTimes) {
            topQuery = prevQuery;
            topTimes = times;
        } else if (times == topTimes && prevQuery.compareTo(topQuery) < 0) {
            topQuery = prevQuery;
        }
        if (topTimes >= minClicks) {
            context.write(host, new Text(String.format("%s\t%d", topQuery, topTimes)));
        }
    }
}
