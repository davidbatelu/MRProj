package org.myorg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//import no.uib.cipr.matrix.sparse.SparseVector;

public class Collab {
	public static class PreMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			String string = value.toString();
			// parts = (user_id, song_id, play_count)
			String[] parts = string.split("\t");
			context.write(new Text(parts[1]),
					new IntWritable(Integer.parseInt(parts[2])));
		}
	}

	public static class PreReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		long unixTime = System.currentTimeMillis() / 1000L;

		Job job = new Job(conf, "collab");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(PreMap.class);
		job.setReducerClass(PreReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// job.setCombinerClass(Reduce.class);
		// job.setPartitionerClass(WordPartitioner.class);
		// job.setNumReduceTasks(5);

		job.setJarByClass(Collab.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// FileInputFormat.addInputPath(job, new
		// Path("../../data/train_triplets_mini.txt"));
		// FileOutputFormat.setOutputPath(job, new
		// Path("../../output/songs_count/" + Long.toString(unixTime)));
		job.waitForCompletion(true);
	}

}
