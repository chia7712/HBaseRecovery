package net.spright.io;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;

public interface HFileWriter extends Closeable
{
    public void write(KeyValue keyValue) throws IOException ;
}
