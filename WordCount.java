package ru.jiht;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int x;
            int y;
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            boolean is_first_coord = true;
            word.set('pi');
            while (tokenizer.hasMoreTokens()) {
                String symbol = tokenizer.nextToken();
                if (is_first_coord) {
                    x = symbol.get().toInt();
                    is_first_coord = false;
                }
                else {
                    y = symbol.get().toInt();
                    is_first_coord = true;
                    if (x * x + y * y <= 1) {
                        context.write(word, one);
                    }
                    else {
                        context.write(word, zero);
                    }
                }
            }
        }
    };

        public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum1 = 0;
                int sum2 = 0
                for (IntWritable val : values) {
                    int value = val.get();
                    if (value) {
                        sum += val.get();
                    }
                    else {
                        sum2 += val.get();
                    }
                }
                float pi = sum1 / (sum1 + sum2);
                context.write(key, new IntWritable(pi));
            }

        };
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

};


