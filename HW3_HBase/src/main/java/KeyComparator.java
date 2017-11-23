import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(HostFromSitesOrPagesPair.class, true);
    }

    @Override
    public int compare(WritableComparable host_a, WritableComparable host_b) {
        return ((HostFromSitesOrPagesPair)host_a).compareTo((HostFromSitesOrPagesPair)host_b);
    }
}
