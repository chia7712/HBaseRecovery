package net.spright.shadow;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

public class STableName 
{
    private static final String SHADOW_TABLENAME_EXTEND = ".shadow";
    private final byte[] stableName;
    public static STableName create(String tableName)
    {
        String stableName = checkName(tableName);
        return stableName == null ? null : new STableName(stableName);
    }
    public static STableName get(String stableName)
    {
        if(!stableName.contains(SHADOW_TABLENAME_EXTEND))
            return null;
        return new STableName(stableName);
    }
    private STableName(String stableName)
    {
        this.stableName = Bytes.toBytes(stableName);
    }
    private STableName(byte[] stableName)
    {
        this.stableName = Arrays.copyOf(stableName, stableName.length);
    }
    public byte[] toBytes()
    {
        return Arrays.copyOf(stableName, stableName.length);
    }
    public STableName copyOf()
    {
        return new STableName(stableName);
    }
    @Override
    public String toString()
    {
        return Bytes.toString(stableName);
    }
    private static String checkName(String tableName)
    {
        if(tableName.compareTo("-ROOT-") == 0)
            return null;
        if(tableName.compareTo(".META.") == 0)
            return null;
        if(tableName.contains(SHADOW_TABLENAME_EXTEND))
            return null;
        return tableName + SHADOW_TABLENAME_EXTEND;
    }
}
