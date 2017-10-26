import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class QueryPartition extends Partitioner<HostQueryPair, IntWritable> {
    @Override
    public int getPartition(HostQueryPair key, IntWritable intWritable, int numPartitions) {
        return Math.abs(key.getHost().hashCode()) & numPartitions;
    }
}
