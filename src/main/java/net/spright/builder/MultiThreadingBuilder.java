package net.spright.builder;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.spright.io.HFileWriter;
import net.spright.io.HFileWriterFactory;
import net.spright.salter.FixedRowSalter;
import net.spright.salter.Salter;
import net.spright.salter.TimeInterval;
import net.spright.shadow.STableName;
import net.spright.test.RConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class MultiThreadingBuilder extends HFileBuilder
{
    private static final int THREAD_BUFFER = 4;
    private final long heapSize;
    public MultiThreadingBuilder(Configuration configuration)
    {
        this(configuration, -1);
    }
    public MultiThreadingBuilder(Configuration configuration, long heapSize)
    {
        super(configuration);
        this.heapSize = heapSize <= 0 ? RConstants.DEFAULT_MAX_SORTSIZE : heapSize;
    }
    @Override
    public String toString()
    {
        return this.getClass().toString() + ", heapSize = " + configuration.getLong(RConstants.RECOVERY_MAX_SORTSIZE, RConstants.DEFAULT_MAX_SORTSIZE);
    }
    @Override
    protected Report doBuild(STableName stableName, Path outputPath, Salter.Scanner scanner) throws Exception 
    {
        if(scanner == null)
                scanner = FixedRowSalter.createScanner(stableName, new TimeInterval(), null);
        final int regions = getRegionsInRange(configuration, stableName.toString());
        final long startTime = System.currentTimeMillis();
        final InnerInfo info = new InnerInfo(configuration, outputPath, heapSize);
        Scan[] scans = scanner.createScans(regions, RConstants.CACHING);
        try(HConnection connection = HConnectionManager.createConnection(configuration))
        {
            
            for(Scan scan : scans)
            {
                info.readService.execute(new ReadThread(
                    connection.getTable(stableName.toString()), 
                    scan, 
                    info));			
            }
            info.readService.shutdown();
            info.readService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            info.writeService.shutdown();
            info.writeService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            final long stopTime = System.currentTimeMillis();
            return new Report(configuration, outputPath)
            {
                @Override
                public String getMetrics() 
                {
                    return this.getClass().toString()
                        + "\texecution time\t" + getExecutionTime()
                        + "\tregions\t" + regions
                        + "\trecords\t" + info.records
                        + "\twriteBytes\t" + info.writeBytes
                        + "\treadBytes\t" + info.readBytes
                        + "\theapSize\t" + heapSize;
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
    }
    private static class WriteThread implements Runnable
    {
        private final Set<KeyValue> keyValueSetSorted;
        private final InnerInfo info;
        public WriteThread(List<KeyValue> keyValueSet, InnerInfo info) throws IOException
        {
            keyValueSetSorted = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
            keyValueSetSorted.addAll(keyValueSet);
            this.info = info;

        }
        @Override
        public void run() 
        {
            try(HFileWriter writer = HFileWriterFactory.createHFileWriter(info.configuration, info.outputPath))
            {
                long writeBytes = 0;
                for(KeyValue keyValue : keyValueSetSorted)
                {
                    writeBytes += keyValue.heapSize();
                    writer.write(keyValue);
                }
                info.writeBytes.addAndGet(writeBytes);
            } 
            catch (IOException e) {}
        }	
    }
    private static class ReadThread implements Runnable, Closeable
    {
        private final Scan scan;
        private final HTableInterface table;
        private final InnerInfo info;
        public ReadThread(HTableInterface table, Scan scan, InnerInfo info) throws IOException
        {
            this.table = table;
            this.scan = scan;
            this.info = info;
        }
        @Override
        public void run() 
        {
            
            try(ResultScanner scanner = table.getScanner(scan))
            {
                Salter.Decoder decoder = FixedRowSalter.createDecoder(info.heapSize);
                long readBytes = 0;
                long records = 0;
                for(Result result : scanner)
                {
                    KeyValue[] keyValues = result.raw();
                    for(KeyValue keyValue : keyValues)
                    {
                        decoder.decode(keyValue);
                        readBytes += keyValue.heapSize();
                        records += keyValues.length;
                    }
                    if(decoder.overHeapSizeNumber() != 0)
                    {
                        for(byte[] family : decoder.overHeapSize())
                        {
                                info.writeService.execute(new WriteThread(decoder.take(family), info));
                        }
                    }
                }
                if(decoder.size() != 0)
                {
                    for(List<KeyValue> keyValueSet : decoder.take())
                    {	
                        info.writeService.execute(new WriteThread(keyValueSet, info));
                    }
                }
                decoder.clear();
                info.readBytes.addAndGet(readBytes);
                info.records.addAndGet(records);
            } 
            catch (IOException e){}
        }
        @Override
        public void close() throws IOException 
        {
            table.close();
        }
    }
    private static class InnerInfo
    {
        private final Configuration configuration;
        private final Path outputPath;
        private final long heapSize;
        private final AtomicLong records = new AtomicLong(0);
        private	final AtomicLong readBytes  = new AtomicLong(0);
        private	final AtomicLong writeBytes  = new AtomicLong(0);
        private	final ExecutorService readService = Executors.newFixedThreadPool(THREAD_BUFFER);
        private final ExecutorService writeService = Executors.newFixedThreadPool(THREAD_BUFFER);
        public InnerInfo(Configuration configuration, Path outputPath, long heapSize)
        {
            this.configuration = new Configuration(configuration);
            this.outputPath = new Path(outputPath.toString());
            this.heapSize = heapSize;
        }
    }

}
