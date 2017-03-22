package lab2.ach;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.ReduceContext.ValueIterator;

public class Lab2AchReducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
    private static final int LIMIT = 350;

    public Lab2AchReducer2() {
    }

    public void run(Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int iterations = 0;

        while(context.nextKey()) {
            this.reduce((IntWritable)context.getCurrentKey(), context.getValues(), context);
            Iterator iter = context.getValues().iterator();
            if(iter instanceof ValueIterator) {
                ((ValueIterator)iter).resetBackupStore();
            }

            ++iterations;
            if(iterations == 350) {
                break;
            }
        }

    }

    protected void reduce(IntWritable count, Iterable<Text> values, Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), new IntWritable(2147483647 - count.get()));
    }
}