package net.spright.builder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import net.spright.io.HFileWriter;
import net.spright.io.HFileWriterFactory;
import net.spright.salter.Salter;
import net.spright.salter.FixedRowSalter;
import net.spright.salter.TimeInterval;
import net.spright.shadow.STableName;
import net.spright.test.RConstants;

public class MapReduceBuilder extends HFileBuilder
{
    private static 	final int REDUCERS_FACTOR = 1;
    private 		final int reducers;
    public MapReduceBuilder(Configuration configuration)
    {
        this(configuration, -1);
    }
    public MapReduceBuilder(Configuration configuration, int reducers)
    {
        super(configuration);
        this.reducers = reducers <= 0 ? getTaskTrackers(configuration) * REDUCERS_FACTOR  : reducers;
    }
    @Override
    public String toString()
    {
        return this.getClass().toString() + ", reducers = " + reducers;
    }
    @Override
    protected Report doBuild(STableName stableName, Path outputPath, Salter.Scanner scanner) throws Exception 
    {
        if(scanner == null)
                scanner = FixedRowSalter.createScanner(stableName, new TimeInterval(), null);
        final int regions = getRegionsInRange(configuration, stableName.toString());
        Scan[] scans = scanner.createScans(regions, RConstants.CACHING);
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
        //set Partitioner
        job.setPartitionerClass(TotalOrderPartitioner.class);		
        RandomRowSampler sampler = new RandomRowSampler(0.1, 100, 3);
        job.setNumReduceTasks(reducers);
        InputSampler.writePartitionFile(job, sampler);
        String partitionFile =TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
        URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
        DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
        DistributedCache.createSymlink(job.getConfiguration());
        //set Reducer

        job.setReducerClass(RecoveryReducer.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        //set outputformat
        job.setOutputFormatClass(HFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        final long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        final long stopTime = System.currentTimeMillis();
        return new Report(configuration, outputPath)
        {
            @Override
            public String getMetrics() 
            {
                return this.getClass().toString()
                    + "\texecution time\t" + getExecutionTime()
                    + "\tReducers\t" + reducers;
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
    private static class RecoveryMapper extends TableMapper<ImmutableBytesWritable, KeyValue>
    {
        private final ImmutableBytesWritable writable = new ImmutableBytesWritable();
        private final Salter.Decoder decoder = FixedRowSalter.createDecoder();
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
        }
        @Override
        protected void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException
        {
            decoder.decode(result.list());
            for(List<KeyValue> keyValueSet : decoder.take())
            {
                for(KeyValue keyValue : keyValueSet)
                {
                    writable.set(keyValue.getRow());
                    context.write(writable, keyValue);
                }
            }
            decoder.clear();
        }
    }
    private static class RecoveryReducer extends Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue> 
    {
        private final Set<KeyValue> keyValueSet = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        @Override
        protected void reduce(ImmutableBytesWritable row, java.lang.Iterable<KeyValue> kvs, Reducer<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, KeyValue>.Context context) throws java.io.IOException, InterruptedException 
        {
            for(KeyValue kv: kvs) 
            {
                keyValueSet.add(kv.clone());
            }
            context.setStatus("Read " + keyValueSet.getClass());
            int index = 0;
            for(KeyValue kv: keyValueSet) 
            {
                context.write(row, kv);
                if(index > 0 && index % 100 == 0) 
                    context.setStatus("Wrote " + index);
            }
            keyValueSet.clear();
        }
    }
    private static class HFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue>
    {
        @Override
        public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException 
        {
            final Path outputPath = FileOutputFormat.getOutputPath(context);
            final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
            final Configuration configuration = context.getConfiguration();
            final HFileWriter writer = HFileWriterFactory.createHFileWriter(configuration, outputdir);
            return new RecordWriter<ImmutableBytesWritable, KeyValue>() 
            {

                @Override
                public void close(TaskAttemptContext arg0) throws IOException,InterruptedException 
                {
                    writer.close();

                }

                @Override
                public void write(ImmutableBytesWritable row, KeyValue keyValue)throws IOException, InterruptedException 
                {
                    writer.write(keyValue);
                }

            };
        }
    }
    private static int getTaskTrackers(Configuration configuration)
    {
        JobConf jobConf = new JobConf(configuration);
        try
        {
            JobClient client = new JobClient(jobConf);
            ClusterStatus statur = client.getClusterStatus();
            return statur.getTaskTrackers();
        }
        catch(IOException e)
        {
            return 4;
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
