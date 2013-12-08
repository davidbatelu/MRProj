package org.myorg;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RandomCenters {
	static Random generator;
	public static class RCMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void setup(Context context) throws IOException {
			generator = new Random();
		}
		
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			DoubleWritable r = new DoubleWritable(generator.nextDouble());
			context.write(r, value);
		}
	}
	
	public static class RCRed extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
			 long unixTime = System.currentTimeMillis() / 1000L;
			 Configuration conf = new Configuration();
			 Job job = new Job(conf, "randcent");
			 job.setOutputKeyClass(DoubleWritable.class);
			 job.setOutputValueClass(Text.class);
			 job.setMapperClass(RCMap.class);
			 job.setReducerClass(RCRed.class);
			 job.setMapOutputKeyClass(DoubleWritable.class);
			 job.setMapOutputValueClass(Text.class);
			 job.setInputFormatClass(TextInputFormat.class);
			 job.setOutputFormatClass(TextOutputFormat.class);
			 // The combiner involves extra parsing, so not using it may actually be more efficient
//			 job.setCombinerClass(KComb.class);
	//					 job.setPartitionerClass(WordPartitioner.class);
			 job.setNumReduceTasks(1);
			 
			 job.setJarByClass(RandomCenters.class);
	
//		     FileInputFormat.addInputPath(job, new Path(args[0]));
//		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     FileInputFormat.addInputPath(job, new Path("../../data/train_triplets_mini.txt"));
		     FileOutputFormat.setOutputPath(job, new Path("../../output/random_centers/" + Long.toString(unixTime)));
		     job.waitForCompletion(true);
	}
}
