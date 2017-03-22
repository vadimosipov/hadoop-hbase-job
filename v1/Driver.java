package lab2.v1;

import lab2.TimestampURLPair;
import lab2.v1.Lab2Mapper;
import lab2.v1.Lab2Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
    public Driver() {
    }

    public int run(String[] args) throws Exception {
        if(args.length != 1) {
            System.err.println("ERROR: Wring number of parameters: " + args.length);
            System.err.println("Usage: " + Driver.class.getCanonicalName() + " <input>");
            return -1;
        } else {
            Job job = Job.getInstance(this.getConf(), "Lab2_v1_vadim.osipov");
            job.setJarByClass(this.getClass());
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setMapperClass(Lab2Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TimestampURLPair.class);
            TableMapReduceUtil.initTableReducerJob("vadim.osipov", Lab2Reducer.class, job);
            return job.waitForCompletion(true)?0:1;
        }
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Driver(), args);
        System.exit(status);
    }
}