package lab2.ach;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import lab2.Counters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Lab2AchMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Pattern onlyDigitsPattern = Pattern.compile("\\d+");

    public Lab2AchMapper() {
    }

    protected void map(LongWritable key, Text line, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] data = line.toString().split("\t");
        if(data.length != 3) {
            context.getCounter(Counters.SIZE).increment(1L);
        } else {
            String UID = data[0];
            String URL = data[2];
            if(this.check(UID, context)) {
                context.write(new Text(URL), new IntWritable(1));
                context.getCounter(Counters.ROWS).increment(1L);
            }

        }
    }

    public boolean check(String UID, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws UnsupportedEncodingException {
        if(!onlyDigitsPattern.matcher(UID).matches()) {
            context.getCounter(Counters.UID).increment(1L);
            return false;
        } else {
            return true;
        }
    }
}
