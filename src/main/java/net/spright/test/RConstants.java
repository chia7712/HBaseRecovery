package net.spright.test;

/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class RConstants {
    public static String RECOVERY_MAX_SORTSIZE = "recovery.max.sortsize";
    public static long DEFAULT_MAX_SORTSIZE = 256 * 1024 * 1024L;
    public static String RECOVERY_HFILE_OUTPUT = "recovery.hfile.output";
    public static String DEFAULT_HFILE_OUTPUT = "/hfile_output";
    public static String RECOVERY_TIME_INTERVAL = "recovery.time.interval";
    public static int CACHING = 100;
    public static int SPLIT_FACTOR = 1;
    private RConstants(){}
}
