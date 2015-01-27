package net.spright.test;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import net.spright.builder.EndpointBuilder;
import net.spright.builder.HFileBuilder;
import net.spright.builder.HFileBuilder.Report;
import net.spright.builder.MapBuilder;
import net.spright.builder.MapReduceBuilder;
import net.spright.builder.MultiThreadingBuilder;
import net.spright.bulkload.HFileLoader;
import net.spright.shadow.STableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class RecoveryTest {
    private static final String USERTABLE = "usertable";
    private static final Path OUTPUT_PATH = new Path("/" + USERTABLE);
    private static final String HOME_PATH = System.getProperty("user.home");
    private enum BuilderInfo {
        MAPREDUCE(new MapReduceBuilder(getConfiguration())),
        ENDPOINT(new EndpointBuilder(getConfiguration())),
        MAPPER(new MapBuilder(getConfiguration())),
        MULTITHREADING(new MultiThreadingBuilder(getConfiguration()));
        private final HFileBuilder builder;
        BuilderInfo(HFileBuilder builder) {
            this.builder = builder;
        }
        public HFileBuilder getBuilder() {
            return builder;
        }
        @Override
        public String toString() {
            return name();
        }
    }
    private enum DonerInfo {

        Clean("clean", new DoClean()),
        Init("init", new DoInit()),
        Check("check", new DoCheck()),
        Build("build", new DoBuild()),
        Load("load", new DoLoad());
        private final String cmd;
        private final Doner doner;
        DonerInfo(String cmd, Doner doner) {
            this.cmd = cmd;
            this.doner = doner;
        }
        public String getCmd() {
            return cmd;
        }
        public Doner getDoner() {
            return doner;
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            System.out.println("No arguments");
            return;
        }
        final String cmd = args[0].toLowerCase();
        final String[] remainArg = args.length == 1 ? new String[0] : Arrays.copyOfRange(args, 1, args.length);
        for(DonerInfo doner : DonerInfo.values()) {
            if(doner.getCmd().toLowerCase().compareTo(cmd) == 0) {
                String result = doner.getDoner().toDo(remainArg, getConfiguration());
                System.out.println(result);
                return;
            }
        }

    }
    private static Configuration getConfiguration() {
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.addResource(new Path(HOME_PATH + "/hadoop/conf/core-site.xml"));
        hadoopConfiguration.addResource(new Path(HOME_PATH + "/hadoop/conf/hdfs-site.xml"));
        hadoopConfiguration.addResource(new Path(HOME_PATH + "/hadoop/conf/mapred-site.xml"));
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.addResource(new Path(HOME_PATH + "/hbase/conf/hbase-site.xml"));
        HBaseConfiguration.merge(hbaseConfiguration, hadoopConfiguration);
        return hbaseConfiguration;
    }
    private static class DoBuild implements Doner {

        @Override
        public String toDo(String[] remainArgs, Configuration configuration) throws Exception {
            List<BuilderInfo> infoList = new LinkedList();
            int maxCount = 1;
            for(String arg : remainArgs) {
                boolean isBuilder = false;
                for(BuilderInfo info : BuilderInfo.values()) {
                    if(arg.toUpperCase().compareTo(info.name()) == 0) {
                        infoList.add(info);
                        isBuilder = true;
                        break;
                    }
                }
                if(isBuilder){
                    continue;
                }
                try {
                    maxCount = Integer.valueOf(arg);
                } catch(NumberFormatException e){}
            }
            StringBuilder str = new StringBuilder();
            for(BuilderInfo info : infoList) {
                final long time = System.currentTimeMillis();
                List<Report> allReportList = new LinkedList();
                for(int i = 0; i != maxCount; ++i) {
                    Report report = info.builder.build(STableName.create(USERTABLE), OUTPUT_PATH);
                    allReportList.add(report);
                }
                str.append("\n").append(outputReport(allReportList, time));
            }
            return str.toString();        
        }
        private static String outputReport(List<Report> reportList, long time) throws IOException {
            StringBuilder str = new StringBuilder();
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(new File("recovery_" + String.valueOf(time))))) {
                for(Report report : reportList) {
                    String line = report.toString();
                    str.append("\n").append(line);
                    writer.write(line);
                    writer.newLine();
                }
                return str.toString();
            }
        }
        
    }
    private static class DoLoad implements Doner {

        @Override
        public String toDo(String[] remainArgs, Configuration configuration) throws Exception {
            String rval = null;
            if(remainArgs.length == 1) {
                String name = remainArgs[0];
                BuilderInfo curInfo = null;
                for(BuilderInfo info : BuilderInfo.values()) {
                    if(name.toUpperCase().compareTo(info.name()) == 0) {
                        curInfo = info;
                        break;
                    }
                }
                if(curInfo == null) {
                    return "HFileBuilder Not Found";
                }
                Report report = curInfo.builder.build(STableName.create(USERTABLE), OUTPUT_PATH);
                rval = report.toString();
            }
            HFileLoader loader = new HFileLoader(configuration);
            loader.loadHFileToTable(USERTABLE, OUTPUT_PATH);
            return rval == null ? "load the HFile" : rval;
        }
    }
    private static class DoClean implements Doner {
        private final String[] tableNames = new String[]{USERTABLE, STableName.create(USERTABLE).toString()};
        @Override
        public String toDo(String[] remainArgs, Configuration configuration) throws IOException {
            try(HBaseAdmin admin = new HBaseAdmin(configuration)) {
                for(String tableName : tableNames) {
                    if(admin.tableExists(tableName)) {
                        admin.disableTable(tableName);
                        admin.deleteTable(tableName);
                    }
                    STableName stableName = STableName.create(tableName);
                    if(stableName != null && admin.tableExists(stableName.toString())) {
                            admin.disableTable(stableName.toString());
                            admin.deleteTable(stableName.toString());
                    }
                }
            }
            return "clean usertabel and usertable.shadow";
        }
    }
    private static class DoCheck implements Doner {

        @Override
        public String toDo(String[] remainArgs, Configuration configuration) throws IOException {
            try(HBaseAdmin admin = new HBaseAdmin(configuration)) {
                ClusterStatus status = admin.getClusterStatus();
                Map<String, Long> sizes = new HashMap();
                for(ServerName serverName : status.getServers()) {
                    HServerLoad serverLoad = status.getLoad(serverName);
                    for(Map.Entry<byte[], HServerLoad.RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                        HServerLoad.RegionLoad regionLoad = entry.getValue();
                        String name = regionLoad.getNameAsString();
                        int index = name.indexOf(",");
                        if(index != -1) {
                            name = name.substring(0, index);
                        }
                        Long size = sizes.get(name);
                        if(size == null) {
                            size = new Long(regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB());
                        } else {
                            size += regionLoad.getMemStoreSizeMB() + regionLoad.getStorefileSizeMB();
                        }
                        sizes.put(name, size);
                    }

                }
                StringBuilder str = new StringBuilder();
                for(Map.Entry<String, Long> entry : sizes.entrySet()) {
                    str.append("\t")
                        .append(entry.getValue())
                        .append("\t")
                        .append(entry.getKey())
                        .append("\n");
                }
                return str.toString();
            }
        }
    }
    private static class DoInit implements Doner {
        @Override
        public String toDo(String[] remainArgs, Configuration configuration) throws IOException {
            if(remainArgs.length < 2) {
                return "Init <rsNumberFactor> <SplitFactor> (start number)";
            }
            int rsNumberFactor = -1;
            int splitFactor = -1;
            int offset = 0;
            try {
                rsNumberFactor = Math.max(Integer.valueOf(remainArgs[0]), 1);
                splitFactor = Math.max(Integer.valueOf(remainArgs[1]), 1);
                if(remainArgs.length == 3) {
                    offset = Math.max(Integer.valueOf(remainArgs[2]), 0);
                }
            } catch(NumberFormatException e) {
                return "<rsNumberFactor> is " +  rsNumberFactor + "<SplitFactor> is " + splitFactor;
            }

            try(HBaseAdmin admin = new HBaseAdmin(configuration)) {
                if(admin.tableExists("usertable")) {
                    return "usertable is existed!!";
                }
                ClusterStatus status = admin.getClusterStatus();
                final int rsNumber = status.getServersSize();
                final int splitSize = rsNumber * rsNumberFactor  * splitFactor + offset;
                final int splitLength = String.valueOf(splitSize).length();
                HTableDescriptor tableDesc = new HTableDescriptor("usertable");
                tableDesc.addFamily(new HColumnDescriptor("column"));
                List<byte[]> splits = new LinkedList();
                for(int i = offset; i != splitSize; i += splitFactor) {
                    String value = String.valueOf(i);
                    while(value.length() != splitLength) {
                        value = "0" + value;
                    }
                    splits.add(Bytes.toBytes("user" + value));
                }
                if(!splits.isEmpty()) {
                    splits.remove(0);
                }
                if(splits.isEmpty()) {
                    admin.createTable(tableDesc);
                    return "no Splits";
                }
                else {
                    admin.createTable(tableDesc, splits.toArray(new byte[splits.size()][]));
                    return "split size = " + splits.size() + 1;
                }
            }
        }
    }
}
