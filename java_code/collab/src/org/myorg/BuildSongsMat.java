package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Trying to use HashMaps for now
//import no.uib.cipr.matrix.sparse.SparseVector;

public class BuildSongsMat {
	public static class BuildSongsMatMap extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			String string = value.toString();
			// parts = (user_id, song_id, play_count)
			String[] parts = string.split("\t");
			context.write(new Text(parts[0]), new Text(parts[1] + ":"
					+ parts[2]));
		}
	}

	public static class BuildSongsMatRed extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> h = new HashMap<String, Integer>();
			String full = "";
			for (Text val : values) {
				// parts[0] is the songid and parts[1] is the count
				String[] parts = val.toString().split(":");
				h.put(parts[0], new Integer(parts[1]));
			}
			for (Map.Entry<String, Integer> entry : h.entrySet()) {
				String mapkey = (String) entry.getKey();
				Integer value = (Integer) entry.getValue();
				// System.out.print("Here:" + mapkey + value.toString());
				full += mapkey + ":" + value.toString() + ",";
			}
			// String will definitely contain something as a user cannot exists
			// who has not listened to a song
			context.write(key, new Text(full.substring(0, full.length() - 1)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		long unixTime = System.currentTimeMillis() / 1000L;

		Job job = new Job(conf, "buildsongsmat");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(BuildSongsMatMap.class);
		job.setReducerClass(BuildSongsMatRed.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// job.setCombinerClass(BuildSongsMatRed.class);
		// job.setPartitionerClass(WordPartitioner.class);
		// job.setNumReduceTasks(5);

		job.setJarByClass(BuildSongsMat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// FileInputFormat.addInputPath(job, new
		// Path("../../data/train_triplets_mini.txt"));
		// FileOutputFormat.setOutputPath(job, new
		// Path("../../output/build_mat/" + Long.toString(unixTime)));
		job.waitForCompletion(true);
	}

}
