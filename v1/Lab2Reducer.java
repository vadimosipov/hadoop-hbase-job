package lab2.v1;

import java.io.IOException;
import java.util.Iterator;
import lab2.HBaseService;
import lab2.TimestampURLPair;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Lab2Reducer extends TableReducer<Text, TimestampURLPair, ImmutableBytesWritable> {
    public Lab2Reducer() {
    }

    protected void reduce(Text key, Iterable<TimestampURLPair> values, Reducer<Text, TimestampURLPair, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
        Put put = new Put(Bytes.toBytes(key.toString()));
        Iterator var5 = values.iterator();

        while(var5.hasNext()) {
            TimestampURLPair pair = (TimestampURLPair)var5.next();
            put.addColumn(HBaseService.FAMILY_NAME, HBaseService.QUALIFIER, pair.getTimestamp(), Bytes.toBytes(pair.getUrl()));
        }

        context.write((Object)null, put);
    }
}