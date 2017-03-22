package lab2.ach;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Lab2AchReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public Lab2AchReducer() {
    }

    protected void reduce(Text url, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;

        IntWritable value;
        for(Iterator var5 = values.iterator(); var5.hasNext(); sum += value.get()) {
            value = (IntWritable)var5.next();
        }

        context.write(url, new IntWritable(sum));
    }
}