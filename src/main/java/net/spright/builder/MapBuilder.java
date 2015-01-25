package net.spright.builder;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.spright.io.HFileWriter;
import net.spright.io.HFileWriterFactory;
import net.spright.salter.FixedRowSalter;
import net.spright.salter.Salter;
import net.spright.salter.Salter.Scanner;
import net.spright.salter.TimeInterval;
import net.spright.shadow.STableName;
import net.spright.test.RConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapBuilder extends HFileBuilder
{
    private static final Path logPath = new Path("/Mapper_logs");
    private final Path tempPath = new Path("/hfile_tmp_" + String.valueOf(System.currentTimeMillis()));
    public MapBuilder(Configuration configuration) 
    {
        this(configuration, -1);
    }
    public MapBuilder(Configuration configuration, long heapSize)
    {
        super(configuration);
        heapSize = heapSize <= 0 ? RConstants.DEFAULT_MAX_SORTSIZE : heapSize;
        configuration.setLong(RConstants.RECOVERY_MAX_SORTSIZE, heapSize);
        cleanLog(configuration);
    }
    private static void cleanLog(Configuration configuration) 
    {
        try
        {
            FileSystem fs = FileSystem.get(configuration);
            if(fs.exists(logPath))
            {
                fs.delete(logPath, true);
            }
        }
        catch(IOException e){}
    }
    @Override
    protected Report doBuild(STableName stableName, Path outputPath, Scanner scanner)throws Exception 
    {
        if(scanner == null)
                scanner = FixedRowSalter.createScanner(stableName, new TimeInterval(), null);
        final int regions = getRegionsInRange(configuration, stableName.toString());
        Scan[] scans = scanner.createScans(regions, 10);
        //set Job
        Job job = new Job(configuration);
        job.setJobName(this.getClass().getName());
        job.setJarByClass(this.getClass());
        //set inputformat and Mapper
        initTableMapperJob(
            Arrays.asList(scans), 
            RecoveryMapper.class, 
            ImmutableBytesWritable.class, 
            KeyValue.class, 
            job);
        //set Reducer
        job.setNumReduceTasks(0);
        //set outputformat
        //must be set the output path, otherwise it will throw null exception.
        FileOutputFormat.setOutputPath(job, cleanPath(configuration, tempPath));
        //set output path
        job.getConfiguration().set(RConstants.RECOVERY_HFILE_OUTPUT, outputPath.toString());
        final long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        final long stopTime = System.currentTimeMillis();
        cleanPath(configuration, tempPath);
        return new Report(configuration, outputPath)
        {

            @Override
            public String getMetrics() 
            {
                return  "\texecution time\t" + getExecutionTime();
            }
            @Override
            public long getExecutionTime() 
            {
                return stopTime - startTime;
            }

            @Override
            public String getName()
            {
                return this.getClass().getName();
            }
        };
    }
    private static class MapperReport
    {
        private long executionTime = 0;
        private long readTime = 0;
        private long decodeTime = 0;
        private long writeTime = 0;
        private long records = 0;
        private long readBytes = 0;
        private long writeBytes = 0;
        private long nullKVs = 0;
    }
    private static class RecoveryMapper extends TableMapper<ImmutableBytesWritable, KeyValue>
    {
        private final MapperReport report = new MapperReport();
        private final long executionStartTime = System.currentTimeMillis();
        private long currentReadTime = 0;
        private Salter.Decoder decoder;
        private String output;
        private long heapSize;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException 
        {
            heapSize = context.getConfiguration().getLong(RConstants.RECOVERY_MAX_SORTSIZE, RConstants.DEFAULT_MAX_SORTSIZE);
            decoder = FixedRowSalter.createDecoder(heapSize);
            output = context.getConfiguration().get(RConstants.RECOVERY_HFILE_OUTPUT, RConstants.DEFAULT_HFILE_OUTPUT);
            currentReadTime = System.currentTimeMillis();
        }
        @Override
        protected void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException
        {
            report.readTime += (System.currentTimeMillis() - currentReadTime);
            final long decodeStartTime = System.currentTimeMillis();
            for(KeyValue kv : result.list())
            {
                report.readBytes += kv.heapSize();
                KeyValue rs = decoder.decode(kv);
                if(rs == null)
                    ++report.nullKVs;
                ++report.records;
            }
            report.decodeTime += (System.currentTimeMillis() - decodeStartTime);
            if(decoder.overHeapSizeNumber() != 0)
            {
                final long writeStartTime = System.currentTimeMillis();
                for(byte[] family : decoder.overHeapSize())
                {
                    report.writeBytes += doWrite(decoder.take(family), context.getConfiguration(), output, heapSize);	
                }
                report.writeTime += (System.currentTimeMillis() - writeStartTime);
            }
            currentReadTime = System.currentTimeMillis();
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
            if(decoder.size() != 0)
            {
                final long writeStartTime = System.currentTimeMillis();
                for(List<KeyValue> keyValueSet : decoder.take())
                {   
                    report.writeBytes += doWrite(keyValueSet, context.getConfiguration(), output, heapSize);
                }
                report.writeTime += (System.currentTimeMillis() - writeStartTime);
            }
            report.executionTime = (System.currentTimeMillis() - executionStartTime);
            log(report, context.getConfiguration());
        }
        private static void log(MapperReport report, Configuration configuration) throws IOException
        {
            FileSystem fs = FileSystem.get(configuration);
            for(int i = 0; i != 100; ++i)
            {
                Path path = new Path(logPath, InetAddress.getLocalHost().getHostName() + "/" + i);
                if(fs.exists(path))
                    continue;
                try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true))))
                {
                    StringBuilder str = new StringBuilder();
                    str.append("\trecords\t")
                            .append(String.valueOf(report.records))
                            .append("\treadBytes\t")
                            .append(String.valueOf(report.readBytes))
                            .append("\twriteBytes\t")
                            .append(String.valueOf(report.writeBytes))
                            .append("\tNullKVs\t")
                            .append(String.valueOf(report.nullKVs))
                            .append("\tExecutionTime\t")
                            .append(String.valueOf(report.executionTime))
                            .append("\treadTime\t")
                            .append(String.valueOf(report.readTime))
                            .append("\tdecodeTimee\t")
                            .append(String.valueOf(report.decodeTime))
                            .append("\twriteTime\t")
                            .append(String.valueOf(report.writeTime));
                    writer.write(str.toString());
                    writer.newLine();
                    break;
                }
            }

        }
        private static long doWrite(List<KeyValue> keyValueSet, Configuration configuration, String output, long heapSize) throws IOException
        {
            try(HFileWriter writer = HFileWriterFactory.createHFileWriter(configuration, new Path(output), heapSize))
            {
                Set<KeyValue> keyValueSetSorted = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
                keyValueSetSorted.addAll(keyValueSet);
                long hfileWriteBytes = 0;
                for(KeyValue keyValue : keyValueSetSorted)
                {
                    hfileWriteBytes += keyValue.heapSize();
                    writer.write(keyValue);
                }
                return hfileWriteBytes;
            }
        }
    }
    private static void initTableMapperJob(List<Scan> scans, Class<? extends TableMapper<ImmutableBytesWritable, KeyValue>> mapper, Class<? extends ImmutableBytesWritable> outputKeyClass, Class<? extends Writable> outputValueClass, Job job) throws IOException 
    {
        job.setInputFormatClass(MultiTableInputFormat.class);
        job.setMapOutputValueClass(outputValueClass);
        job.setMapOutputKeyClass(outputKeyClass);
        job.setMapperClass(mapper);
        List<String> scanStrings = new ArrayList<String>();
        for (Scan scan : scans) 
        {
            scanStrings.add(convertScanToString(scan));
        }
        job.getConfiguration().setStrings(MultiTableInputFormat.SCANS, scanStrings.toArray(new String[scanStrings.size()]));
    }
    private static String convertScanToString(Scan scan) throws IOException 
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        try
        {
            scan.write(dos);
            return Base64.encodeBytes(out.toByteArray());
        }
        finally
        {
            out.close();
        }
    }
}
