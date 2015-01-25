package net.spright.salter;

import net.spright.test.RConstants;

public class RemainPrefixer implements Prefixer
{
    private final int prefixValue;
    public RemainPrefixer(long gTimestamp, int regionServerNumber)
    {
        regionServerNumber = regionServerNumber * RConstants.SPLIT_FACTOR;
        prefixValue = (int)(gTimestamp % regionServerNumber);
    }
    @Override
    public int createPrefix() 
    {
        return prefixValue;
    }
}
