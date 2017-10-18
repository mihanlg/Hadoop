import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class ChunkedPartsSplit extends FileSplit {
    private ArrayList<Integer> lengths;

    public ChunkedPartsSplit() {
        super();
    }

    public ChunkedPartsSplit(Path path, long offset, long length, ArrayList<Integer> parts) {
        super(path, offset, length, new String[] {});
        lengths = new ArrayList<>(parts);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(lengths.size());
        for (int x: lengths) {
            out.writeInt(x);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int n = in.readInt();
        lengths = new ArrayList<>();
        while(n-- != 0) {
            lengths.add(in.readInt());
        }
    }

    ArrayList<Integer> getPageLengths() {
        return lengths;
    }
}
