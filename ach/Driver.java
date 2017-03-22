package lab2.ach;

import java.io.IOException;
import java.util.Arrays;
import lab2.ach.Lab2AchMapper;
import lab2.ach.Lab2AchMapper2;
import lab2.ach.Lab2AchReducer;
import lab2.ach.Lab2AchReducer2;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
    public Driver() {
    }

    public int run(String[] args) throws Exception {
        System.out.println("args: " + Arrays.toString(args));
        Job job = this.init("Lab2_v2_ach", Lab2AchMapper.class, Lab2AchReducer.class, args);
        job.setCombinerClass(Lab2AchReducer.class);
        return job.waitForCompletion(true) && args.length == 3?this.run2MR(new String[]{args[1], args[2]}):1;
    }

    private int run2MR(String[] args) throws Exception {
        Job job = this.init("Lab2_v2_ach+MR2", Lab2AchMapper2.class, Lab2AchReducer2.class, args);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true)?0:1;
    }

    private Job init(String jobName, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String[] args) throws IOException {
        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(this.getClass());
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Driver(), args);
        System.exit(status);
    }
}