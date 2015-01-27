package net.spright.salter;

import net.spright.test.RConstants;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class RemainPrefixer implements Prefixer {
    private final int prefixValue;
    public RemainPrefixer(long gTimestamp, int regionServerNumber) {
        regionServerNumber = regionServerNumber * RConstants.SPLIT_FACTOR;
        prefixValue = (int)(gTimestamp % regionServerNumber);
    }
    @Override
    public int createPrefix() {
        return prefixValue;
    }
}
