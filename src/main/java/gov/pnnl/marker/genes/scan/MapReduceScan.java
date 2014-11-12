package gov.pnnl.marker.genes.scan;


import gov.pnnl.marker.genes.common.FastaOutputFormat;
import gov.pnnl.marker.genes.common.FilterBitScore;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * A simple map reduce job that inserts word counts into Accumulo. See the
 * README for instructions on how to run this.
 *
 */
public class MapReduceScan extends Configured implements Tool {

    static class Opts extends ClientOnRequiredTable {
        @Parameter(names = "--output", description = "output file")
        String outputDirectory;

        @Parameter(names = "--columns", description = "columns to extract, in cf:cq{,cf:cq,...} form")
        String columns    = "";

        @Parameter(names = "--gtscore", description = "include sequences with bit score greater than value")
        String gtscore    = null;

        @Parameter(names = "--gtpctcov", description = "include sequences with alignment percent coverage greater than value")
        String gtpctcov   = null;

        @Parameter(names = "--gtpostprob", description = "include sequences with posterior probability greater than value")
        String gtpostprob = null;
    }

    public static class MapClass extends Mapper<Key, Value, NullWritable, Text> {
        @Override
        public void map(final Key key, Value value, final Context output) throws IOException, InterruptedException {
            Text data = new Text("data");
            SortedMap<Key, Value> decodedRow = WholeRowIterator.decodeRow(key, value);
            for (Entry<Key, Value> row : decodedRow.entrySet()) {
                if (0 == row.getKey().compareColumnQualifier(data)) {
                    output.write(NullWritable.get(), new Text(row.getValue().toString()));
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Opts opts = new Opts();
        opts.parseArgs(MapReduceScan.class.getName(), args);

        final Job job = Job.getInstance(getConf());
        job.setJobName(MapReduceScan.class.getName());
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(AccumuloInputFormat.class);
        final HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text, Text>>();
        for (final String col : opts.columns.split(",")) {
            final int idx = col.indexOf(":");
            final Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
            final Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
            if (cf.getLength() > 0) {
                columnsToFetch.add(new Pair<Text, Text>(cf, cq));
            }
        }

        if (!columnsToFetch.isEmpty()) {
            InputFormatBase.fetchColumns(job, columnsToFetch);
        }

        final Map<String, String> properties = new HashMap<>();
        if (opts.gtscore != null) {
            properties.put("score", opts.gtscore);
        }
        if (opts.gtpctcov != null) {
            properties.put("percent_coverage", opts.gtpctcov);
        }
        if (opts.gtpostprob != null) {
            properties.put("post_prob", opts.gtpostprob);
        }
        if (!properties.isEmpty()) {
            final IteratorSetting iteratorSetting = new IteratorSetting(21, "greaterthan", FilterBitScore.class, properties);
            InputFormatBase.addIterator(job, iteratorSetting);
            InputFormatBase.setLocalIterators(job, true);
        }

        job.setMapperClass(MapClass.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(FastaOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(opts.outputDirectory));
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        opts.setAccumuloConfigs(job);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MapReduceScan(), args);
    }
}
