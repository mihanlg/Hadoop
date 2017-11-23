import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HostGrouper extends WritableComparator {
    protected HostGrouper() {
        super(HostFromSitesOrPagesPair.class, true);
    }

    @Override
    public int compare(WritableComparable pair_a, WritableComparable pair_b) {
        Text host_a = ((HostFromSitesOrPagesPair)pair_a).getHost();
        Text host_b = ((HostFromSitesOrPagesPair)pair_b).getHost();
        return host_a.compareTo(host_b);
    }
}