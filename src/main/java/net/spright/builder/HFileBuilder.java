package net.spright.builder;
import java.io.IOException;
import net.spright.salter.Salter;
import net.spright.shadow.STableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;

public abstract class HFileBuilder 
{
    protected final Configuration configuration;
    public HFileBuilder(Configuration configuration)
    {
        this.configuration = new Configuration(configuration);
    }
    public Report build(STableName stableName, Path outputPath)throws Exception
    {
        return build(stableName, outputPath, null);
    }
    public Report build(STableName stableName, Path outputPath, Salter.Scanner scanner) throws Exception
    {
        cleanPath(configuration, outputPath);
        return doBuild(stableName, outputPath, scanner);

    }
    protected static int getRegionsInRange(Configuration configuration, String tableName) throws Exception
    {
        try(HTable table = new HTable(configuration, tableName))
        {
            return table.getRegionLocations().size();
        }
    }
    protected abstract Report doBuild(STableName stableName, Path outputPath, Salter.Scanner scanner) throws Exception;
    protected static Path cleanPath(Configuration configuration, Path outputPath) throws IOException
    {
        FileSystem fs = outputPath.getFileSystem(configuration);
        if(fs.exists(outputPath))
            fs.delete(outputPath, true);
        return outputPath;
}
    public static abstract class Report
    {
        protected final Configuration configuration;
        protected final Path outputPath;
        public Report(Configuration configuration, Path outputPath)
        {
            this.configuration = new Configuration(configuration);
            this.outputPath = new Path(outputPath.toString());
        }
        public abstract String getMetrics();
        public abstract long getExecutionTime();
        public abstract String getName();
        public Configuration getConfiguration()
        {
            return new Configuration(configuration);
        }
        public Path getOutputPath()
        {
            return new Path(outputPath.toString());
        }
        
        @Override
        public String toString()
        {
            return getName() + getMetrics();
        }
    }
}
