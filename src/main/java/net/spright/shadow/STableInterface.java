package net.spright.shadow;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public interface STableInterface extends Closeable {
    public void put(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException;
}
