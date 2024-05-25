package com.henryhung.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ClickUrlPreprocessor {

    public static class UrlMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length > 2 && parts[0].contains(":")) {
                String timestamp = parts[0];
                String[] timeParts = timestamp.split(":");
                String formattedTime = String.format("%02d:%02d", Integer.parseInt(timeParts[0]), Integer.parseInt(timeParts[1]));
                String clickUrl = parts[2];
                String domain = clickUrl.contains("/") ? clickUrl.split("/")[0] : clickUrl; // Get domain, stripping after "/"
                domain = domain.trim().replaceAll("^\"|\"$", "");
                outputKey.set(formattedTime);
                outputValue.set(domain);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class UrlReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                result.set(val);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Click URL Preprocessor");
        job.setJarByClass(ClickUrlPreprocessor.class);
        job.setMapperClass(UrlMapper.class);
        job.setReducerClass(UrlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("assets/raw/search_data.sample"));
        FileOutputFormat.setOutputPath(job, new Path("assets/processed/search_data.sample"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}