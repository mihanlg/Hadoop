import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class IndexedGzipInputFormat extends FileInputFormat<LongWritable, Text> {
    public static final String BYTES_PER_MAP = "mapreduce.input.indexedgz.bytespermap";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        GzipBlockRecordReader reader = new GzipBlockRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        for (FileStatus status: listStatus(job)) {
            splits.addAll(getFlatFileSplits(status, job.getConfiguration()));
        }
        return splits;
    }

    List<InputSplit> getFlatFileSplits(FileStatus status, Configuration conf) throws IOException {
        Path flatFilePath = status.getPath();
        Path indexFilePath = flatFilePath.suffix(".idx");
        FileSystem fs = flatFilePath.getFileSystem(conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(indexFilePath);
            return splitFileByIndex(flatFilePath, in, getNumBytesPerSplit(conf));
        }
        finally {
            IOUtils.closeStream(in);
        }
    }

    protected List<InputSplit> splitFileByIndex(Path filePath, FSDataInputStream in, long bytesPerSplit)
            throws IOException {
        long splitOffset = 0;
        long splitLength = 0;
        List<InputSplit> splits = new ArrayList<>();
        ArrayList<Integer> sizes = new ArrayList<>();
        while (true) {
            int pageSize;
            try {
                pageSize = Integer.reverseBytes(in.readInt());
            }
            catch(EOFException error) {
                break;
            }
            splitLength += pageSize;
            sizes.add(pageSize);
            if (splitLength >= bytesPerSplit) {
                splits.add(new ChunkedPartsSplit(filePath, splitOffset, splitLength, sizes));
                splitOffset += splitLength;
                splitLength = 0;
                sizes.clear();
            }
        }
        if (!sizes.isEmpty()) {
            splits.add(new ChunkedPartsSplit(filePath, splitOffset, splitLength, sizes));
        }
        return splits;
    }

    public static long getNumBytesPerSplit(Configuration conf) {
        return conf.getLong(BYTES_PER_MAP, 33554432);
    }
}
