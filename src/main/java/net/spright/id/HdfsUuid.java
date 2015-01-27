package net.spright.id;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class HdfsUuid implements Closeable {
    private static final String ROOT = "/shadow/";

    public interface Builder {
        public int getRegionID();
        public long getbatchID();
        public int createtKeyValueID();
    }
    private final FileSystem fs;
    private final int regionID;
    private final AtomicLong preBatchID = new AtomicLong(System.currentTimeMillis());
    public HdfsUuid(Configuration configuration) throws IOException {
        fs = FileSystem.get(configuration);
        regionID = createRegionID(fs, configuration);
    }
    public Builder createBuilder() {
        return new BuilderImpl(regionID, createBatchID(preBatchID));
    }
    private static int createRegionID(FileSystem fs, Configuration configuration) throws IOException {
        try(Socket socket = new Socket(configuration.get("uuid.ip", "192.168.200.184"), configuration.getInt("uuid.port", 9999))) {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int number = input.readInt();
            Path path = createPath(number);
           try(BufferedWriter output = new BufferedWriter(new OutputStreamWriter(fs.create(path)))) {
               output.write(InetAddress.getLocalHost().getHostName());
           }
           return number;
        }
    }
    @Override
    public void close() throws IOException {
        Path path = new Path(ROOT + regionID);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
    }
    private static Path createPath(int number) {
        return new Path(ROOT + String.valueOf(number));
    }
    private static class BuilderImpl implements Builder {
        private final int regionID;
        private final long batchID;
        private int keyvaluID = 0;
        public BuilderImpl(int regionID, long batchID) {
            this.regionID = regionID;
            this.batchID = batchID;
        }
        @Override
        public long getbatchID() {
            return batchID;
        }
        @Override
        public int getRegionID() {
            return regionID;
        }

        @Override
        public int createtKeyValueID() {
            return keyvaluID++;
        }

    }
    private static long createBatchID(AtomicLong preBatchID) {
        long now = System.currentTimeMillis();
        while(true) {
            long lastTime = preBatchID.get();
            if (lastTime >= now) {
                now = lastTime + 1;
            }
            if (preBatchID.compareAndSet(lastTime, now)) {
                return now;
            }
        }
    }
}
