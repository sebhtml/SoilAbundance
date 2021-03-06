package gov.pnnl.marker.genes.ingest;

import gov.pnnl.marker.genes.common.AccumuloStructure;

import java.io.IOException;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import au.com.bytecode.opencsv.CSVParser;

import com.beust.jcommander.Parameter;

/**
 * A simple map reduce job that inserts word counts into Accumulo. See the
 * README for instructions on how to run this.
 *
 */
public class MapReduceCSV extends Configured implements Tool {

    static class Opts extends ClientOnRequiredTable {
        @Parameter(names = "--input", description = "input file")
        String inputDirectory;
    }

    public static class MapClass extends Mapper<LongWritable, Text, Text, Mutation> {
        @Override
        public void map(final LongWritable key, final Text value, final Context output)
                throws IOException, InterruptedException {
            final CSVParser parser = new CSVParser();
            final String[] fields = parser.parseLine(value.toString());

            final Mutation mutation = new Mutation(fields[AccumuloStructure.AA_MD5_OFFSET]);
            mutation.put(fields[1], "data", generateFastaRecord(fields));
            mutation.put(fields[1], "post_prob", fields[AccumuloStructure.PP_OFFSET]);
            mutation.put(fields[1], "score", fields[AccumuloStructure.SCORE_OFFSET]);
            mutation.put(fields[1], "pctcov", fields[AccumuloStructure.PCTCOV_OFFSET]);

            output.write(null, mutation);

        }
    }

    private static String generateFastaRecord(final String[] fields) {
        final StringBuilder sb = new StringBuilder();

        sb.append('>');                                         // The start of a FASTA record
        sb.append(fields[AccumuloStructure.SEQ_NAME_OFFSET]);   // Then name of the sequence
        sb.append(AccumuloStructure.SPSEP);                     // Whitespace
        for (int i = 0; i < (fields.length - 2); i++) {
            sb.append(fields[i]);                               // Append the HMM metadata as a comment
            sb.append(',');
        }
        sb.append(fields[fields.length - 2]);
        sb.append(AccumuloStructure.NLSEP);                     // Append a newline marker
        sb.append(fields[fields.length - 1]);                   // Append the actual sequence

        return sb.toString();
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Opts opts = new Opts();
        opts.parseArgs(MapReduceCSV.class.getName(), args);

        final Job job = Job.getInstance(getConf());
        job.setJobName(MapReduceCSV.class.getName());
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(opts.inputDirectory));

        job.setMapperClass(MapClass.class);

        job.setNumReduceTasks(0);                               // We don't need a reducer

        job.setOutputFormatClass(AccumuloOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        opts.setAccumuloConfigs(job);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MapReduceCSV(), args);
    }
}
