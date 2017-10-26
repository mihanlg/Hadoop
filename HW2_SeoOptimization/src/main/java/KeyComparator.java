import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(HostQueryPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((HostQueryPair)a).compareTo((HostQueryPair) b);
    }
}
