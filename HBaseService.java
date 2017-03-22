package lab2;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseService {
    public static final String TABLE_NAME = "vadim.osipov";
    public static final byte[] FAMILY_NAME = Bytes.toBytes("data");
    public static final byte[] QUALIFIER = Bytes.toBytes("url");
    private static final int MAX_VERSIONS = 4096;
    private final Configuration conf;

    public HBaseService(Configuration conf) {
        this.conf = conf;
    }

    public void createTable() throws IOException, ServiceException {
        Connection connection = ConnectionFactory.createConnection(this.conf);
        Throwable var2 = null;

        try {
            System.out.println("Got a connection to HBase");
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("vadim.osipov");
            System.out.println("Check whether the table exists or not");
            if(!admin.tableExists(tableName)) {
                System.out.println("Create the table vadim.osipov");
                HColumnDescriptor columnDesc = new HColumnDescriptor(FAMILY_NAME);
                columnDesc.setMaxVersions(4096);
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                tableDesc.addFamily(columnDesc);
                admin.createTable(tableDesc);
            }
        } catch (Throwable var14) {
            var2 = var14;
            throw var14;
        } finally {
            if(connection != null) {
                if(var2 != null) {
                    try {
                        connection.close();
                    } catch (Throwable var13) {
                        var2.addSuppressed(var13);
                    }
                } else {
                    connection.close();
                }
            }

        }

    }
}
