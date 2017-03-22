package lab2.v2;

import lab2.HBaseService;
import lab2.v2.Lab2Mapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
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
            this.getConf().set("hbase.zookeeper.quorum", "ip-10-63-0-70.eu-west-1.compute.internal,ip-10-63-0-5.eu-west-1.compute.internal,ip-10-63-0-4.eu-west-1.compute.internal");
            this.getConf().set("hbase.zookeeper.property.clientPort", "2181");
            Job job = Job.getInstance(this.getConf(), "Lab2_v2_vadim.osipov");
            job.setJarByClass(this.getClass());
            job.setMapperClass(Lab2Mapper.class);
            job.setOutputFormatClass(TableOutputFormat.class);
            job.getConfiguration().set("hbase.mapred.outputtable", "vadim.osipov");
            job.setNumReduceTasks(0);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            HBaseService hBaseService = new HBaseService(this.getConf());
            hBaseService.createTable();
            return job.waitForCompletion(true)?0:-1;
        }
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Driver(), args);
        System.exit(status);
    }
}
