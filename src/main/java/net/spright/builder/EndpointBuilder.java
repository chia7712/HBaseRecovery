package net.spright.builder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.io.Writable;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class EndpointBuilder extends HFileBuilder {
    private final long heapSize;
    public static interface Protocol extends CoprocessorProtocol {
        public NodeReport buildHFile(String output, String timeIntervalStr, long heapSize) throws IOException;
    }
    public EndpointBuilder(Configuration configuration) {
        this(configuration, -1);
    }
    public EndpointBuilder(Configuration configuration, long heapSize) {
        super(configuration);
        this.heapSize = heapSize <= 0 ? RConstants.DEFAULT_MAX_SORTSIZE : heapSize;
    }
    @Override
    public String toString() {
        return this.getClass().toString() + ", heapSize = " + heapSize;
    }
    @Override
    protected Report doBuild(STableName stableName, Path outputPath, Salter.Scanner scanner)throws IOException {
        final String output = outputPath.toString();
        final TimeInterval timeInterval = scanner == null ? new TimeInterval() : scanner.createTimeInterval();
        HTable shadowTable = new HTable(configuration, stableName.toString());
        Scan scan = new Scan();
        try {
            final long startTime = System.currentTimeMillis();
            final Map<byte[], NodeReport> results = shadowTable.coprocessorExec(
                EndpointBuilder.Protocol.class, 
                scan.getStartRow(), 
                scan.getStopRow(), 
                new Batch.Call<EndpointBuilder.Protocol, NodeReport>() {
                    @Override
                    public NodeReport call(Protocol instance) throws IOException {
                        return instance.buildHFile(output, timeInterval.toString(), heapSize);
                    }

                });
            final long executionTime = (System.currentTimeMillis() - startTime);
            return new EndpointReport(configuration, outputPath, executionTime, results.values());
        } catch(Throwable e) {
            throw new IOException(e.toString());
        } finally {
            shadowTable.close();
        }
    }
    private static class EndpointReport extends HFileBuilder.Report {
        private final long executionTime;
        private final List<NodeReport> nodeReports = new LinkedList();
        public EndpointReport(Configuration configuration, Path outputPath, long executionTime, Collection<NodeReport> nodeReports) {
            super(configuration, outputPath);
            this.executionTime = executionTime;
            this.nodeReports.addAll(nodeReports);
        }
        private NodeReport getNodeReportByMaxExecutionTime() {
            NodeReport max = null;
            for(NodeReport report : nodeReports) {
                if(max == null) {
                    max = report;
                    continue;
                }
                max = max.executionTime >= report.executionTime ? max : report;
            }
            return max;
        }
        private long getRecords() {
            long records = 0;
            for(NodeReport report : nodeReports) {
                records += report.records;
            }
            return records;
        }
        private long getWriteBytes() {
            long bytes = 0;
            for(NodeReport report : nodeReports) {
                bytes += report.writeBytes;
            }
            return bytes;
        }
        private long getReadBytes() {
            long bytes = 0;
            for(NodeReport report : nodeReports) {
                bytes += report.readBytes;
            }
            return bytes;
        }
        private long getNullKeyValues() {
            long nullKVs = 0;
            for(NodeReport report : nodeReports) {
                nullKVs += report.nullKVs;
            }
            return nullKVs;
        }
        @Override
        public String getName() {
            return EndpointBuilder.class.getName();
        }
        @Override
        public String getMetrics() {
            StringBuilder str = new StringBuilder();
            str.append("\tExecutionTime\t")
                    .append(String.valueOf(executionTime))
                    .append("\trecords\t")
                    .append(String.valueOf(getRecords()))
                    .append("\treadBytes\t")
                    .append(String.valueOf(getReadBytes()))
                    .append("\twriteBytes\t")
                    .append(String.valueOf(getWriteBytes()))
                    .append("\tNullKVs\t")
                    .append(String.valueOf(getNullKeyValues()));
            NodeReport max = getNodeReportByMaxExecutionTime();
            str.append("\tExecutionTime(max)\t")
                    .append(String.valueOf(max.executionTime))
                    .append("\treadTime(max)\t")
                    .append(String.valueOf(max.readTime))
                    .append("\tdecodeTimee(max)\t")
                    .append(String.valueOf(max.decodeTime))
                    .append("\twriteTime(max)\t")
                    .append(String.valueOf(max.writeTime));
            return str.toString();
        }
        @Override
        public long getExecutionTime() {
            return executionTime;
        }
        
    }
    public static class NodeReport implements Writable {
        private long executionTime = 0;
        private long readTime = 0;
        private long decodeTime = 0;
        private long writeTime = 0;
        private long records = 0;
        private long readBytes = 0;
        private long writeBytes = 0;
        private long nullKVs = 0;

        @Override
        public void write(DataOutput d) throws IOException {
            d.writeLong(executionTime);
            d.writeLong(readTime);
            d.writeLong(decodeTime);
            d.writeLong(writeTime);
            d.writeLong(records);
            d.writeLong(readBytes);
            d.writeLong(writeBytes);
            d.writeLong(nullKVs);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            executionTime = di.readLong();
            readTime = di.readLong();
            decodeTime = di.readLong();
            writeTime = di.readLong();
            records = di.readLong();
            readBytes = di.readLong();
            writeBytes = di.readLong();
            nullKVs = di.readLong();
        }

    }
    public static class ProtocolImpl extends BaseEndpointCoprocessor implements EndpointBuilder.Protocol {
        @Override
        public NodeReport buildHFile(String output, String timeIntervalStr, long heapSize)throws IOException {
            RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment)getEnvironment();
            HRegion region = env.getRegion();
            Configuration configuration = env.getConfiguration();
            TimeInterval timeInterval = new TimeInterval(timeIntervalStr);
            Salter.Scanner salterScanner = FixedRowSalter.createScanner(
                            STableName.get(region.getTableDesc().getNameAsString()), 
                            timeInterval, 
                            null);
            final long executeStartTime = System.currentTimeMillis();
            try(InternalScanner scanner = region.getScanner(salterScanner.createScan(region.getRegionInfo()))) {
                NodeReport report = new NodeReport();
                List<KeyValue> keyValueList = new LinkedList<KeyValue>();
                Salter.Decoder decoder = FixedRowSalter.createDecoder(heapSize);
                while(true) {
                    final long readStartTime = System.currentTimeMillis();
                    if(!scanner.next(keyValueList)) {
                        break;
                    }
                    report.readTime += (System.currentTimeMillis() - readStartTime);
                    final long decodeStartTime = System.currentTimeMillis();
                     for(KeyValue keyValue : keyValueList) {
                        KeyValue rval = decoder.decode(keyValue);
                        if(rval == null) {
                            report.nullKVs++;
                        }
                        report.readBytes += keyValue.heapSize();
                    }
                    report.decodeTime += (System.currentTimeMillis() - decodeStartTime);
                    report.records += keyValueList.size();
                    if(decoder.overHeapSizeNumber() != 0) {
                        final long writeStartTime = System.currentTimeMillis();
                        for(byte[] family : decoder.overHeapSize()) {
                            report.writeBytes += doWrite(decoder.take(family), configuration, output, heapSize);
                        }
                        report.writeTime += (System.currentTimeMillis() - writeStartTime);
                    }
                    keyValueList.clear();
                }
                if(decoder.size() != 0) {
                    final long writeStartTime = System.currentTimeMillis();
                    for(List<KeyValue> keyValueSet : decoder.take()) {
                        report.writeBytes += doWrite(keyValueSet, configuration, output, heapSize);
                    }
                    report.writeTime += (System.currentTimeMillis() - writeStartTime);
                }
                report.executionTime = (System.currentTimeMillis() - executeStartTime);
                return report;
            }
        }
        private static long doWrite(List<KeyValue> keyValueSet, Configuration configuration, String output, long heapSize) throws IOException {
            try(HFileWriter writer = HFileWriterFactory.createHFileWriter(configuration, new Path(output), heapSize)) {
                Set<KeyValue> keyValueSetSorted = new TreeSet(KeyValue.COMPARATOR);
                keyValueSetSorted.addAll(keyValueSet);
                long hfileWriteBytes = 0;
                for(KeyValue keyValue : keyValueSetSorted) {
                    hfileWriteBytes += keyValue.heapSize();
                    writer.write(keyValue);
                }
                return hfileWriteBytes;
            }
        }
    }
}
