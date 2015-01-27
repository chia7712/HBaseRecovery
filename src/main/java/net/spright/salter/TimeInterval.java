package net.spright.salter;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class TimeInterval {
    public static final long MAX_TIME = Long.MAX_VALUE;
    public static final long MIN_TIME = 0;
    private static final String DIVIDER = ":";
    private final long minTimestamp;
    private final long maxTimestamp;
    public TimeInterval() {
        minTimestamp = MIN_TIME;
        maxTimestamp = MAX_TIME;
    }
    public TimeInterval(String str) {
        this(getTime(str, true), getTime(str, false));
    }
    private static long getTime(String str, boolean min) {
        String[] args = str.split(DIVIDER);
        if(args.length != 2) {
            throw new NumberFormatException("TimeInterval String is error format");
        }
        long[] times = new long[]{Long.valueOf(args[0]), Long.valueOf(args[1])};
        return min ? times[0] : times[1];
    }
    public TimeInterval(long minTimestamp, long maxTimestamp) {
        long min = Math.min(maxTimestamp, minTimestamp);
        long max = Math.max(maxTimestamp, minTimestamp);
        if(max <= MIN_TIME) {
            max = MAX_TIME;
        }
        if(min < MIN_TIME) {
            min = MIN_TIME;
        }
        this.minTimestamp = min;
        this.maxTimestamp = max;
    }
    public long getmaxTimestamp() {
        return maxTimestamp;
    }
    public long getminTimestamp() {
        return minTimestamp;
    }
    public boolean betweenTime(long time) {
        return time >= minTimestamp && time <= maxTimestamp;
    }
    public TimeInterval copyOf() {
        return new TimeInterval(minTimestamp, maxTimestamp);
    }
    @Override
    public String toString() {
        return minTimestamp + DIVIDER + maxTimestamp;
    }
}
