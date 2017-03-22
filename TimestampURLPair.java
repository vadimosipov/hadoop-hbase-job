package lab2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TimestampURLPair implements Writable {
    private long timestamp;
    private String url;

    public TimestampURLPair() {
    }

    public TimestampURLPair(long timestamp, String url) {
        this.timestamp = timestamp;
        this.url = url;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeUTF(this.url);
    }

    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.url = in.readUTF();
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getUrl() {
        return this.url;
    }

    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(!(o instanceof TimestampURLPair)) {
            return false;
        } else {
            TimestampURLPair that = (TimestampURLPair)o;
            return this.timestamp != that.timestamp?false:(this.url != null?this.url.equals(that.url):that.url == null);
        }
    }

    public int hashCode() {
        int result = (int)(this.timestamp ^ this.timestamp >>> 32);
        result = 31 * result + (this.url != null?this.url.hashCode():0);
        return result;
    }

    public String toString() {
        return "TimestampURLPair{timestamp=" + this.timestamp + ", url=\'" + this.url + '\'' + '}';
    }
}