package net.spright.shadow;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.spright.id.HdfsUuid;
import net.spright.salter.FixedRowSalter;
import net.spright.salter.RemainPrefixer;
import net.spright.salter.Salter;
import net.spright.salter.TimeInterval;
import net.spright.test.RConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

public class STableManager implements Closeable
{
    private final HBaseAdmin admin;
    private final HConnection connection;
    public STableManager(Configuration configuration) throws MasterNotRunningException, ZooKeeperConnectionException
    {
        this.admin = new HBaseAdmin(configuration);
        this.connection = HConnectionManager.createConnection(configuration);
    }
    public STableInterface openSTable(STableName stableName, HdfsUuid.Builder builder) throws IOException
    {
        if(!tableExists(stableName))
            return null;
        return new STableImpl(connection.getTable(stableName.toString()), getRegionServerNumber(admin), builder);
    }
    public void createSTable(STableName stableName, HTableDescriptor tableDesc) throws IOException 
    {
        if(!admin.tableExists(stableName.toString()))
        {
            final int rsNumber = getRegionServerNumber(admin);
            Salter.Scanner scanner = FixedRowSalter.createScanner(stableName, new TimeInterval(), null);
            List<byte[]> splitRows = new LinkedList<byte[]>();
            for(Scan scan : scanner.createScans(rsNumber * RConstants.SPLIT_FACTOR))
                splitRows.add(scan.getStopRow());
            switch(splitRows.size())
            {
                case 0:
                    throw new IOException("No split rows");
                case 1:
                {
                    admin.createTable(merge(stableName, tableDesc));
                }
                default:
                {
                    splitRows.remove(splitRows.size() - 1);
                    admin.createTable(merge(stableName, tableDesc), splitRows.toArray(new byte[splitRows.size()][]));
                }
            }
        }
    }

    public void modifyTable(STableName stableName, HTableDescriptor tableDesc) throws IOException
    {
        final String name = stableName.toString();
        final byte[] nameBytes = stableName.toBytes();
        if(admin.tableExists(stableName.toString()))
        {
            admin.disableTable(name);
            admin.modifyTable(nameBytes, merge(stableName, tableDesc, admin.getTableDescriptor(nameBytes)));
            admin.enableTable(name);
        }
    }
    public boolean tableExists(STableName stableName) throws IOException
    {
        return admin.tableExists(stableName.toString());
    }
    public void removeSTable(STableName stableName) throws IOException
    {
        final String name = stableName.toString();
        if(admin.tableExists(name))
        {
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }
    @Override
    public void close() throws IOException 
    {
        try
        {
            connection.close();
        }
        catch(IOException e)
        {
        }
        finally
        {
            admin.close();
        }
    }
    private static int getRegionServerNumber(HBaseAdmin admin) throws IOException
    {
        return admin.getClusterStatus().getServersSize();
    }
    private static class STableImpl implements STableInterface
    {
        private final HTableInterface table;
        private final int regionServerNumber;
        private final HdfsUuid.Builder builder;
        public STableImpl(HTableInterface table, int regionServerNumber, HdfsUuid.Builder builder) throws IOException
        {
            this.table = table;
            this.regionServerNumber = regionServerNumber;
            this.builder = builder;
        }
        @Override
        public void close() throws IOException 
        {
            table.close();
        }
        @Override
        public void put(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp)throws IOException 
        {
            Salter.Encoder encoder = FixedRowSalter.createEncoder(
                new RemainPrefixer(builder.getbatchID(), regionServerNumber), 
                builder);
            for(int index = 0; index != miniBatchOp.size(); ++index)
            {
                OperationStatus status = miniBatchOp.getOperationStatus(index);
                if(status.getOperationStatusCode() != OperationStatusCode.SUCCESS)
                    continue;
                Mutation mutation = miniBatchOp.getOperation(index).getFirst();
                for(Map.Entry<byte[], List<KeyValue>> entry : mutation.getFamilyMap().entrySet())
                {
                    encoder.encode(entry.getValue());
                }
            }
            table.put(encoder.take());
        }
    }
    private static HTableDescriptor merge(STableName stableName, HTableDescriptor ... tableDescs)
    {
        Set<byte[]> columnNames = new HashSet<byte[]>();
        for(HTableDescriptor tableDesc : tableDescs)
        {
            for(HColumnDescriptor columnDesc : tableDesc.getColumnFamilies())
            {
                    columnNames.add(columnDesc.getName());
            }	
        }
        HTableDescriptor newDesc = new HTableDescriptor(stableName.toBytes());
        for(byte[] columnName : columnNames)
        {
            HColumnDescriptor columnDesc = new HColumnDescriptor(columnName);
            columnDesc.setMaxVersions(Integer.MAX_VALUE);
            columnDesc.setMinVersions(Integer.MAX_VALUE);
            newDesc.addFamily(columnDesc);
        }
        return newDesc;
    }
}
