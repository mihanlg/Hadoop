import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HostPartitioner extends Partitioner<HostFromSitesOrPagesPair, Text> {
    @Override
    public int getPartition(HostFromSitesOrPagesPair key, Text val, int numPartitions) {
        return Math.abs(key.getHost().hashCode()) % numPartitions;
    }
}