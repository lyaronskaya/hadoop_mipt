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
            int x = 0;
            int y = 0;
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String symbol = tokenizer.nextToken();
            y = Integer.parseInt(symbol);
            while (tokenizer.hasMoreTokens()) {
                x = y;
                symbol = tokenizer.nextToken();
                y = Integer.parseInt(symbol);
                if (x > 0 && y > 0) {
                    word.set("first");
                } else if (x > 0 && y <= 0) {
                    word.set("second");
                } else if (x < 0 && y < 0) {
                    word.set("third");
                } else if (x <= 0 && y >= 0) {
                    word.set("fourth");
                }
                if (x * x + y * y <= 1) {
                    context.write(word, one);
                } else {
                    context.write(word, zero);
                }
            }
        }
    };

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum1 = 0;
            int sum2 = 0;
            for (IntWritable val : values) {
                int value = val.get();
                if (value == 1) {
                    sum1 += value;
                }
                else {
                    sum2 += value;
                }
            }
            float pi = sum1 / (sum1 + sum2);
            Text curr_pi = new Text();
            curr_pi.set(String.valueOf(pi));
            context.write(key, curr_pi);
        }

    };
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

};


