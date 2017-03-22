package lab2.ach;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Lab2AchMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
    public Lab2AchMapper2() {
    }

    protected void map(LongWritable key, Text line, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        String[] urlCount = line.toString().split("\t");
        context.write(new IntWritable(2147483647 - Integer.valueOf(urlCount[1]).intValue()), new Text(urlCount[0]));
    }
}
