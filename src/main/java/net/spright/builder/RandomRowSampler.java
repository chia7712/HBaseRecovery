package net.spright.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.spright.salter.FixedRowSalter;
import net.spright.salter.Salter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;
/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class RandomRowSampler implements Sampler<ImmutableBytesWritable, Result> {
    private final double freq;
    private final int numSamples;
    private final int maxSplitsSampled;
    public RandomRowSampler(double freq, int numSamples, int maxSplitsSampled) {
        this.freq = freq;
        this.numSamples = numSamples;
        this.maxSplitsSampled = maxSplitsSampled;
    }
    @Override
    public ImmutableBytesWritable[] getSample(InputFormat<ImmutableBytesWritable, Result> inf, Job job)throws IOException, InterruptedException {
        List<InputSplit> splits = inf.getSplits(job);
        ArrayList<ImmutableBytesWritable> samples = new ArrayList(numSamples);
        int splitsToSample = Math.min(maxSplitsSampled, splits.size());
        Random r = new Random();
        r.setSeed(r.nextLong());
        for(int i = 0; i < splits.size(); ++i) {
            InputSplit tmp = splits.get(i);
            int j = r.nextInt(splits.size());
            splits.set(i, splits.get(j));
            splits.set(j, tmp);
        }
        double currentFreq = freq;
        Salter.Decoder decoder = FixedRowSalter.createDecoder();
        for(int i = 0; i < splitsToSample || (i < splits.size() && samples.size() < numSamples); ++i) {
            TaskAttemptContext samplingContext = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
            try(RecordReader<ImmutableBytesWritable, Result> reader = inf.createRecordReader(splits.get(i), samplingContext)) {
                reader.initialize(splits.get(i), samplingContext);
                while(reader.nextKeyValue()) {
                    if (r.nextDouble() <= currentFreq)  {
                        for(KeyValue saltingKeyValue : reader.getCurrentValue().raw()) {
                            KeyValue decodedKeyValue = decoder.decodeWithoutBuffer(saltingKeyValue);
                            if(samples.size() < numSamples) {
                                //samples.add(ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentKey(), null));
                                samples.add(new ImmutableBytesWritable(decodedKeyValue.getRow()));
                            } else {
                                int ind = r.nextInt(numSamples);
                                if(ind != numSamples) {
                                    //samples.set(ind, ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentKey(), null));
                                    samples.set(ind, new ImmutableBytesWritable(decodedKeyValue.getRow()));
                                }
                                currentFreq *= (numSamples - 1) / (double) numSamples;
                            }
                        }
                    }
                }
            }
        }
        return samples.toArray(new ImmutableBytesWritable[samples.size()]);
    }
}
