import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.Inflater;

public class GzipBlockRecordReader extends RecordReader<LongWritable, Text> {

    LongWritable key = new LongWritable();
    Text value = new Text();

    FSDataInputStream in;
    ArrayList<Integer> parts;

    int currPart = 0;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        ChunkedPartsSplit chunkedParts = (ChunkedPartsSplit) split;
        Path filePath = chunkedParts.getPath();
        FileSystem fs = filePath.getFileSystem(context.getConfiguration());
        in = fs.open(filePath);
        in.seek(chunkedParts.getStart());
        parts = chunkedParts.getPageLengths();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currPart >= parts.size()) {
            return false;
        }
        key.set(in.getPos());
        int len = parts.get(currPart++);
        byte[] buf = new byte[len];
        byte[] inflaterBuf = new byte[4096];
        IOUtils.readFully(in, buf,0, len);

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Inflater inflater = new Inflater();
            int n;

            inflater.setInput(buf, 0, buf.length);
            while ((n = inflater.inflate(inflaterBuf)) > 0) {
                output.write(inflaterBuf, 0, n);
            }
            value.set(output.toByteArray());
            inflater.end();
        }
        catch (java.util.zip.DataFormatException error) {
            throw new IOException("Couldn't inflate data!");
        }
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) currPart / parts.size();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(in);
    }
}
