import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HostFromSitesOrPagesPair implements WritableComparable<HostFromSitesOrPagesPair> {
    private Text host;
    private Text tableName;

    private void set(Text _host, Text _tableName) {
        host = _host;
        tableName = _tableName;
    }

    private HostFromSitesOrPagesPair(String _host, String _tableName) {
        set(new Text(_host), new Text(_tableName));
    }

    Text getHost() {
        return host;
    }

    public HostFromSitesOrPagesPair() {
        set(new Text(), new Text());
    }


    static HostFromSitesOrPagesPair fromPages(String host) {
        return new HostFromSitesOrPagesPair(host, "pages");
    }

    static HostFromSitesOrPagesPair fromSites(String host) {
        return new HostFromSitesOrPagesPair(host, "sites");
    }

    boolean isFromPages() {
        return tableName.toString().equals("pages");
    }

    boolean isFromSites() {
        return tableName.toString().equals("sites");
    }

    public int compareTo(HostFromSitesOrPagesPair o) {
        int cmp = host.compareTo(o.host);
        return (cmp == 0) ? tableName.compareTo(o.tableName) : cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HostFromSitesOrPagesPair) {
            HostFromSitesOrPagesPair o = (HostFromSitesOrPagesPair)obj;
            return host.equals(o.host) && tableName.equals(o.tableName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 163 + tableName.hashCode();
    }

    @Override
    public String toString() {
        return host + "\t" + tableName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        host.write(out);
        tableName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        tableName.readFields(in);
    }
}
