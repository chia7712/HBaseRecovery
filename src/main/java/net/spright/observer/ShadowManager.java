package net.spright.observer;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import net.spright.id.UUIDServer;
import net.spright.shadow.STableManager;
import net.spright.shadow.STableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

public class ShadowManager extends BaseMasterObserver
{
    private STableManager tableManager;
    private UUIDServer uuidServer;
    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws MasterNotRunningException, ZooKeeperConnectionException, IOException 
    {
        uuidServer = new UUIDServer(ctx.getEnvironment().getConfiguration().getInt("uuid.port",  9999));
        tableManager = new STableManager(ctx.getEnvironment().getConfiguration());
        Executors.newSingleThreadExecutor().execute(uuidServer);
    }
    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor tableDesc, HRegionInfo[] regions) throws IOException 
    {
        STableName stableName = STableName.create(tableDesc.getNameAsString());
        if(stableName == null)
            return;
        if(tableManager.tableExists(stableName))
        {
            tableManager.modifyTable(stableName, tableDesc);
        }
        else
            tableManager.createSTable(stableName, tableDesc);
    }
    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)throws IOException 
    {
        uuidServer.close();
    }
}
