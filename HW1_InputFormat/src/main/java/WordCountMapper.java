import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private final Pattern reWord = Pattern.compile("\\p{L}+");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Matcher matcher = reWord.matcher(value.toString());

        HashSet<String> words = new HashSet<>();
        while(matcher.find()) {
            String word = matcher.group().toLowerCase();
            if (words.add(word)) {
                context.write(new Text(word), one);
            }
        }
    }
}