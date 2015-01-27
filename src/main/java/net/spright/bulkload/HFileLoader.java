package net.spright.bulkload;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class HFileLoader {
    private final Configuration configuration;
    public HFileLoader(Configuration configuration) {
        this.configuration = new Configuration(configuration);
    }
    public void loadHFileToTable(String tableName, Path inputPath) throws Exception {
        try(HBaseAdmin admin = new HBaseAdmin(configuration)) {
            if(admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for(byte[] family : getFamilies(configuration, inputPath)) {
                tableDesc.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(tableDesc);
            try(HTable table = new HTable(configuration, tableName)) {
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
                loader.doBulkLoad(inputPath, table);
            }
        }
    }
    private static List<byte[]> getFamilies(Configuration configuration, Path inputPath) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        List<byte[]> families = new LinkedList<byte[]>();
        for(FileStatus status : fs.listStatus(inputPath)) {
            String dirName = status.getPath().getName();
            if(dirName.compareTo("_SUCCESS") == 0 || dirName.compareTo("_logs") == 0) {
                continue;
            }
            families.add(Bytes.toBytes(dirName));
        }
        return families;
    }
}
