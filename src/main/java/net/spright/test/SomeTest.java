package net.spright.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;



public class SomeTest 
{

    private static final String HOME_PATH = System.getProperty("user.home");
    private static Configuration getConfiguration()
    {
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.addResource(new Path(HOME_PATH + "/hadoop/conf/core-site.xml"));
        hadoopConfiguration.addResource(new Path(HOME_PATH + "/hadoop/conf/hdfs-site.xml"));
        hadoopConfiguration.addResource(new Path(HOME_PATH + "/hadoop/conf/mapred-site.xml"));
        Configuration hbaseConfiguration = HBaseConfiguration.create();
        hbaseConfiguration.addResource(new Path(HOME_PATH + "/hbase/conf/hbase-site.xml"));
        HBaseConfiguration.merge(hbaseConfiguration, hadoopConfiguration);
        return hbaseConfiguration;
    }
    public static void main(String[] args) throws IOException 
    {
        Configuration configuration = getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        try
        {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/Mapper_logs/" + InetAddress.getLocalHost().getHostName()), false)));

            try
            {
                writer.write(InetAddress.getLocalHost().getHostName());
            }
            finally
            {
                writer.close();
            }
        }
        finally
        {
            fs.close();
        }
    }
    public static String toHex(byte[] bs)
    {
        StringBuilder str = new StringBuilder();
        for(byte b : bs)
            str.append(toHex(b));
        return str.toString();
    }
    public static String toHex(byte b)
    {
        return (""+"0123456789ABCDEF".charAt(0xf&b>>4)+"0123456789ABCDEF".charAt(b&0xf));
    }
}
