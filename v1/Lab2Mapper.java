package lab2.v1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import lab2.Counters;
import lab2.TimestampURLPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Lab2Mapper extends Mapper<LongWritable, Text, Text, TimestampURLPair> {
    private static final long N = 34L;
    private static final long MOD = 256L;
    private static final BigInteger BIG_N = BigInteger.valueOf(34L);
    private static final BigInteger BIG_MOD = BigInteger.valueOf(256L);
    private static final Pattern onlyDigitsPattern = Pattern.compile("\\d+");
    private static final Pattern onlyTimestampPattern = Pattern.compile("\\d{10}\\.\\d{3}");
    private static final int MAX_LONG_LENGTH = String.valueOf(9223372036854775807L).length();

    public Lab2Mapper() {
    }

    protected void map(LongWritable key, Text line, Mapper<LongWritable, Text, Text, TimestampURLPair>.Context context) throws IOException, InterruptedException {
        String[] data = line.toString().split("\t");
        if(data.length != 3) {
            context.getCounter(Counters.SIZE).increment(1L);
        } else {
            String UID = data[0];
            String timestamp = data[1];
            String URL = data[2];
            if(check(UID, timestamp, URL, context)) {
                long ts = (new Long(timestamp.replace(".", ""))).longValue();
                context.write(new Text(UID), new TimestampURLPair(ts, URL));
            }

        }
    }

    public static boolean check(String UID, String timestamp, String URL, Mapper<LongWritable, Text, Text, TimestampURLPair>.Context context) throws UnsupportedEncodingException {
        if(!onlyDigitsPattern.matcher(UID).matches()) {
            context.getCounter(Counters.UID).increment(1L);
            return false;
        } else if(!onlyTimestampPattern.matcher(timestamp).matches()) {
            context.getCounter(Counters.TIMESTAMP).increment(1L);
            return false;
        } else {
            boolean correctURL = false;

            try {
                String result = URLDecoder.decode(URL, StandardCharsets.UTF_8.displayName());
                if(URL.equals(URLEncoder.encode(result, StandardCharsets.UTF_8.displayName()))) {
                    correctURL = true;
                }
            } catch (IllegalArgumentException var8) {
                ;
            }

            if(!correctURL) {
                context.getCounter(Counters.URL).increment(1L);
                return false;
            } else {
                boolean result1;
                if(UID.length() >= MAX_LONG_LENGTH) {
                    result1 = (new BigInteger(UID)).mod(BIG_MOD).equals(BIG_N);
                } else {
                    long uid = Long.valueOf(UID).longValue();
                    result1 = uid % 256L == 34L;
                }

                if(!result1) {
                    context.getCounter(Counters.MOD).increment(1L);
                }

                return result1;
            }
        }
    }
}
