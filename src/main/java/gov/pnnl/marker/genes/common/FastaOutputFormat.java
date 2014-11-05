/**
 *
 */
package gov.pnnl.marker.genes.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 */
public class FastaOutputFormat extends FileOutputFormat<NullWritable, Text> {

    private static final class FastaRecordWriter extends RecordWriter<NullWritable, Text> {

        private final FSDataOutputStream output;

        public FastaRecordWriter(final FSDataOutputStream output) {
            this.output = output;
        }

        @Override
        public void write(final NullWritable key, final Text value) throws IOException, InterruptedException {
            String valueString = value.toString();
            valueString = valueString.replace(AccumuloStructure.SPSEP, " ");
            valueString = valueString.replace(AccumuloStructure.NLSEP, "\n");
            output.writeChars(valueString);
            output.writeChar('\n');
        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            output.close();
        }
    }

    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        final Path file = getDefaultWorkFile(job, ".fna");
        final FileSystem fs = file.getFileSystem(conf);
        final FSDataOutputStream fileOut = fs.create(file, false);
        return new FastaRecordWriter(fileOut);
    }

}
