package net.spright.io;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import net.spright.test.RConstants;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class HFileWriterFactory 
{
    public enum Type{SEQUENTIAL_WRITER};
    public static HFileWriter createHFileWriter(Configuration configuration, Path outputPath) throws IOException
    {
        return createHFileWriter(configuration, outputPath, -1);
    }
    public static HFileWriter createHFileWriter(Configuration configuration, Path outputPath, long maxSize) throws IOException
    {
        return createHFileWriter(Type.SEQUENTIAL_WRITER, configuration, outputPath, maxSize);
    }
    public static HFileWriter createHFileWriter(Type type, Configuration configuration, Path outputPath) throws IOException
    {
        return createHFileWriter(type, configuration, outputPath, -1);
    }
    public static HFileWriter createHFileWriter(Type type, Configuration configuration, Path outputPath, long maxSize) throws IOException
    {
        switch(type)
        {
            case SEQUENTIAL_WRITER:
                return new SequentialWriterImpl(configuration, outputPath, maxSize);
            default:
                return new SequentialWriterImpl(configuration, outputPath, maxSize);

        }
    }
    private static class SequentialWriterImpl implements HFileWriter
    {
        private final Map<byte [], WriterLength> writers = new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
        private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
        private final Configuration configuration;
        private final FileSystem fs;
        private final long maxSize;
        private final Path outputPath;
        private byte[] previousRow = HConstants.EMPTY_BYTE_ARRAY;
        private boolean rollRequested = false;
        public SequentialWriterImpl(Configuration configuration, Path outputPath, long maxSize) throws IOException
        {
            this.configuration = new Configuration(configuration);
            fs = FileSystem.get(configuration);
            this.outputPath = new Path(outputPath.toString());
            this.maxSize = maxSize <= 0 ? RConstants.DEFAULT_MAX_SORTSIZE : maxSize;
        }
        @Override
        public void write(KeyValue keyValue) throws IOException 
        {
            if(keyValue == null) 
            {
                rollWriters(writers);
                rollRequested = false;
                return;
            }
            byte [] row = keyValue.getRow();
            long length = keyValue.getLength();
            byte [] family = keyValue.getFamily();
            WriterLength wl = this.writers.get(family);
            // If this is a new column family, verify that the directory exists
            if(wl == null) 
            {
                fs.mkdirs(new Path(outputPath, Bytes.toString(family)));
            }
            if(wl != null && wl.written + length >= maxSize) 
            {
                this.rollRequested = true;
            }
            //reach the size limit, but it has to put the same row of data together
            if(rollRequested && Bytes.compareTo(previousRow, row) != 0) 
            {
                rollWriters(writers);
                rollRequested = false;
            }

            // create a new HLog writer, if necessary
            if(wl == null || wl.writer == null) 
            {
                wl = getNewWriter(family, configuration, fs, outputPath);
                writers.put(family, wl);
            }
            // we now have the proper HLog writer. full steam ahead
            keyValue.updateLatestStamp(this.now);
            wl.writer.append(keyValue);
            wl.written += length;
            // Copy the row so we know when a row transition.
            previousRow = row;
        }
        @Override
        public void close() throws IOException 
        {
            for(WriterLength wl: this.writers.values()) 
            {
                closeWithFileInfo(wl.writer);
            }
        }
        private static void closeWithFileInfo(final StoreFile.Writer w) throws IOException 
        {
            if (w != null) 
            {
                w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
//	        w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, Bytes.toBytes(context.getTaskAttemptID().toString()));
                w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
                w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY, Bytes.toBytes(false));
                w.appendTrackedTimestampsToMetadata();
                w.close();
            }
        }
        private static void rollWriters(Map<byte [], WriterLength> writers) throws IOException 
        {
            for(WriterLength wl : writers.values()) 
            {
                if(wl.writer != null) 
                {
                    closeWithFileInfo(wl.writer);
                }       
                wl.writer = null;
                wl.written = 0;
            }
        }
        private static WriterLength getNewWriter(byte[] family, Configuration configuration, FileSystem fs, Path outputPath)throws IOException 
        {
            WriterLength wl = new WriterLength();
            Path familydir = new Path(outputPath, Bytes.toString(family));
            Configuration tempConf = new Configuration(configuration);
            tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
            wl.writer = new StoreFile.WriterBuilder(configuration, new CacheConfig(tempConf), fs, HFile.DEFAULT_BLOCKSIZE)
            .withOutputDir(familydir)
            .withCompression(AbstractHFileWriter.compressionByName(Compression.Algorithm.NONE.getName()))
            .withBloomType(BloomType.NONE)
            .withComparator(KeyValue.COMPARATOR)
            .withDataBlockEncoder(NoOpDataBlockEncoder.INSTANCE)
            .withChecksumType(Store.getChecksumType(configuration))
            .withBytesPerChecksum(Store.getBytesPerChecksum(configuration))
            .build();
            return wl;
        }
        private static class WriterLength 
        {
            private long written;
            private StoreFile.Writer writer;
        }
    }
    private HFileWriterFactory(){}
}
