package net.spright.observer;

import java.io.IOException;
import net.spright.id.HdfsUuid;
import net.spright.shadow.STableInterface;
import net.spright.shadow.STableManager;
import net.spright.shadow.STableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;

public class ShadowMaintainer extends BaseRegionObserver
{
    private HdfsUuid hdfsUuid = null;
    private STableManager shadowManager = null;
    private STableName stableName;
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) 
    { 
        stableName = STableName.create(e.getEnvironment().getRegion().getTableDesc().getNameAsString());
        if(stableName == null)
            return;
        try 
        {
            shadowManager = new STableManager(e.getEnvironment().getConfiguration());
            hdfsUuid = new HdfsUuid(e.getEnvironment().getConfiguration());
        } 
        catch (IOException ex){}
        
    }
    @Override
    public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c, final MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException 
    {
        if(shadowManager == null)
            return;
        try(STableInterface table = shadowManager.openSTable(stableName, hdfsUuid.createBuilder()))
        {
            table.put(miniBatchOp);
        }
    }
    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) 
    { 
        if(shadowManager != null)
        {
            try 
            {
                shadowManager.close();
            } 
            catch (IOException e1){}
        }
        if(hdfsUuid != null)
        {
            try
            {
                hdfsUuid.close();
            }
            catch (IOException ex){}
        }
    }
}
