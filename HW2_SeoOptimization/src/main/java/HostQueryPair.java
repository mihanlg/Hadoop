import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostQueryPair implements WritableComparable<HostQueryPair> {
    private Text host, query;

    public HostQueryPair() {
        set(new Text(), new Text());
    }

    public HostQueryPair(String host, String query) {
        set(new Text(host), new Text(query));
    }

    public HostQueryPair(Text host, Text query) {
        set(host, query);
    }

    private void set(Text a, Text b) {
        host = a;
        query = b;
    }

    public Text getHost() {
        return host;
    }

    public Text getQuery() {
        return query;
    }

    @Override
    public int compareTo(HostQueryPair o) {
        int cmp = host.compareTo(o.host);
        return (cmp == 0) ? query.compareTo(o.query) : cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        host.write(out);
        query.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        query.readFields(in);
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 163 + query.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HostQueryPair) {
            HostQueryPair tp = (HostQueryPair) obj;
            return host.equals(tp.host) && query.equals(tp.query);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s\t%s", host, query);
    }
}
