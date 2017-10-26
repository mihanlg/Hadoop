import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HostGrouper extends WritableComparator {
    protected HostGrouper() {
        super(HostQueryPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text a_host = ((HostQueryPair)a).getHost();
        Text b_host = ((HostQueryPair)b).getHost();
        return a_host.compareTo(b_host);
    }
}
