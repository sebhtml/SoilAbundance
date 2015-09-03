package gov.anl.cels.pup;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapred.lib.HashPartitioner;

public class BigMatrixIngestor {

    public static class MyMapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {

                String wordInLine = tokenizer.nextToken();
                word.set(wordInLine);
                output.collect(word, one);
            }
        }
    }

    public static class MyReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {

            int sum = 0;

            while (values.hasNext()) {

                IntWritable value = values.next();
                int count = value.get();
                sum += count;
            }

            IntWritable count = new IntWritable(sum);
            output.collect(key, count);
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(BigMatrixIngestor.class);

        conf.setJobName("BigMatrixIngestor");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);


        conf.setMapperClass(MyMapClass.class);
        conf.setCombinerClass(MyReduceClass.class);
        conf.setReducerClass(MyReduceClass.class);
        conf.setPartitionerClass(HashPartitioner.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        /*
         * We want only one part file, like
         * /user/root/wordcount/output10/part-00000
         */
        //conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
