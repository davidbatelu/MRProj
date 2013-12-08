package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Centers {
	static int K = 10;
	static int features = 2;
	
	public static String combine(String[] s, String glue)
	{
	  int k=s.length;
	  if (k==0)
	    return null;
	  StringBuilder out=new StringBuilder();
	  out.append(s[0]);
	  for (int x=1;x<k;++x)
	    out.append(glue).append(s[x]);
	  return out.toString();
	}
	
	public static final double measureDistance(Double[] center, Double[]  v) {
		  double sum = 0;
		  // Ignore last label
		  for (int i = 0; i < features ; i++) {
			  sum += Math.abs(center[i] - v[i]);
		  }
		 
		  return sum;
		}
	
	public static Double[] doubleize(String[] parts, int size) {
		Double[] row = new Double[size > 0? parts.length: size];
		int i = 0;
		for (String part : parts) {
			row[i] = new Double(part);
			i++;
		}
		return row;
	}
	
	class CentersMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws InterruptedException {
        	String[] parts = value.toString().split(" |\t");
			Double[] row = doubleize(parts, 0);
        	String[] centers_str = context.getConfiguration().get("centers").split(";");
        	String[] distances_str = context.getConfiguration().get("distances").split(";");
        	Double[][] distances = new Double[K][features];
        	int cur_dist = 0;
        	Double[][] centers = new Double[centers_str.length][];
        	String new_centers = "";
        	boolean updated = false;
        	int i = 0;
        	
        	for (String center : centers_str) {
        		centers[i] = doubleize(center.split(" "), 0);
        		i++;
        	}
        	
        	for (String distance : distances_str) {
        		distances[cur_dist] = doubleize(distance.split(" "), K);
        		cur_dist++;
        	}
        	
        	if (centers.length < K) {
        		updated = true;
        		new_centers = combine(centers_str, ";") + ";" + combine(parts, " ");
            	i = 0;		
        		for (Double[] distance : distances) {
        			double tempD = measureDistance(centers[i], row);
        			distance[cur_dist] = tempD;
        			distances[cur_dist][i] = tempD;
        			i++;
        		}
        		
        	} else {
        		for (Double[] center : centers) {
        			measureDistance(center, row);
        		}
        	}
        	
        	if (updated) {
        		context.getConfiguration().set("centers", new_centers);
        	}
        }
	}
	
	public static class CentersRed extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
	            throws InterruptedException {
			
		}
	}

	
	
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 conf.set("centers", "");
		 conf.set("distances", "");
		 Job job = new Job(conf, "buildsongsmat");
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 job.setMapperClass(CentersMap.class);
//		 job.setReducerClass(CentersRed.class);
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
//		 job.setCombinerClass(BuildSongsMatRed.class);
//					 job.setPartitionerClass(WordPartitioner.class);
//					 job.setNumReduceTasks(5);
		 
		 job.setJarByClass(Centers.class);

//				     FileInputFormat.addInputPath(job, new Path(args[0]));
//				     FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	     FileInputFormat.addInputPath(job, new Path("../../data/train_triplets_mini.txt"));
//	     FileOutputFormat.setOutputPath(job, new Path("../../output/build_mat/" + Long.toString(unixTime)));
	     job.waitForCompletion(true);
	   }
}
