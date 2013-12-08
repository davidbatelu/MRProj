package org.myorg;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KMeans {
	
	static final int ARTIST_FAMILIARITY = 0; 
	static final int ARTIST_HOTNESS = 1;
	static final int ARTIST_ID = 2;
	static final int ARTIST_LATITUDE = 3;
	static final int ARTIST_LONGITITUDE = 4;
	static final int ARTIST_LOCATION = 5;
	static final int ARTIST_NAME = 6;
	static final int RELEASE = 7;
	static final int SONG_HOTNESS = 8;
	static final int TITLE = 9;
	static final int SONG_ID = 10;
	static final int DANCEABILITY = 11;
	static final int ENERGY = 12;
	static final int ARTISTS_TERMS_FREQ_LENGTH = 12;
	static final int ARTISTS_TERMS_FREQ_ELEM = 13;
	static final int DURATION = 14;
	static final int KEY = 15;
	static final int LOUDNESS = 16;
	static final int MODE = 17;
	static final int TEMPO = 18;
	static final int TIME_SIGNATURE = 19;
	static final int SEGMENT_LOUDNESS = 20;
	static final int SEGMENT_TIMBRE = 21;
	static final int YEAR = 22;
	static final int GENRE = 23;
	static final int SIMILAR_ARTISTS = 24;
	static final int ARTIST_TERMS = 25;
	
	// All numerical features
	static final Integer []REQUIRED_FIELDS = {ARTIST_FAMILIARITY, ARTIST_HOTNESS, SONG_HOTNESS, DANCEABILITY, ENERGY, 
		DURATION, KEY, LOUDNESS, MODE, TEMPO, TIME_SIGNATURE, SEGMENT_LOUDNESS, SEGMENT_TIMBRE, YEAR};
	
	static int K = 3, features_length = REQUIRED_FIELDS.length;
	static Random generator;
	static long MAX_ITR = 10;
	public static class RCMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void setup(Context context) throws IOException {
			generator = new Random();
		}
		
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			DoubleWritable r = new DoubleWritable(generator.nextDouble());
			System.out.print("Map with " + key.toString());
			context.write(r, value);
		}
	}
	
	public static class RCRed extends Reducer<DoubleWritable, Text, Text, Text> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			System.out.print("Reduce with " + key.toString());
			for (Text value : values) {
				context.write(value, new Text(""));
			}
		}
	}
	
	public static final double measureDistance(Double[] center, Double[]  v) {
	  double sum = 0;
	  // Ignore last label
	  for (int i = 0; i < features_length ; i++) {
		  sum += Math.abs(center[REQUIRED_FIELDS[i]] - v[REQUIRED_FIELDS[i]]);
	  }
	 
	  return sum;
	}
	
	@SuppressWarnings("deprecation")
	public static Double[][] load_centers(FileSystem fs, String subPath) throws IOException {
        String line;
        Double[][] temp_centers = new Double[K][];
        System.out.print("In load_centers\n");
		FSDataInputStream cache = fs.open(new Path("/user/dave/" + subPath  + "/part-r-00000"));
	   try {
		   int i = 0;
		   while((line = cache.readLine()) != null && i < K){
			   System.out.print(line + "\n");
               String[] parts = line.trim().split(" |\t");
               // Ignore the first number as it is the random number
               temp_centers[i] = new Double[parts.length];
               for (int j = 0; j < parts.length; j++) {
            	   temp_centers[i][j] = Double.parseDouble(parts[j]);
               }
               i++;
		   }
	   } catch (IOException e) {
	                   // TODO Auto-generated catch block
	            e.printStackTrace();
	   }
       System.out.print("End of load_centers\n");		   
	   return temp_centers;
	}
	
	public static class KMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		static Double[][] centers;	
		public void setup(Context context) throws IOException {
			   System.out.print("In setup\n");
			   FileSystem fs;
			   try {
					fs = FileSystem.get(new URI("/user/dave"),context.getConfiguration());
					centers = load_centers(fs, context.getConfiguration().get("my.centers.path"));
			   } catch (URISyntaxException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
			   }
			   System.out.print(centers[0][0] + "," + centers[0][1] + "," + centers[1][0] + "," + centers[1][1] + "," + centers[2][0] + "," + centers[2][1] + "\n");
		   }
		
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			String[] parts = value.toString().split(" |\t");
			Double[] row = new Double[parts.length];
			int i = 0, minIdx = -1;
			double min = 99999999.0;
			for (String part : parts) {
				row[i] = new Double(part);
				i++;
			}
			for (int c = 0; c < centers.length; c++) {
				double curDst = measureDistance(centers[c], row);
				if (curDst < min) {
					min = curDst;
					minIdx = c;
				}
			}
			context.write(new IntWritable(minIdx), new Text("1 " + value.toString()));
		}
		
	}
	
	
	
	public static void initialize(Double[] arr) {
		for (int i = 0; i < arr.length; i++) {
			arr[i] = 0.0;
		}
	}
	
	public static class KComb extends Reducer<IntWritable, Text, IntWritable, Text> {
//		static Double[][] centers;
//		public void setup(Context context) throws IOException {
//			   System.out.print("In setup\n");
//			   FileSystem fs;
//			   try {
//					fs = FileSystem.get(new URI("/user/dave"),context.getConfiguration());
//					centers = load_centers(fs);
//			   } catch (URISyntaxException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//			   }
//			   System.out.print(centers[0][0] + "," + centers[0][1] + "," + centers[1][0] + "," + centers[1][1] + "\n");
//		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			System.out.print("In combine: " + key.toString() + " with values = " + values.toString() + "\n");
			
			long num = 0;
			Double[] feature_sum = new Double[features_length + 1];
			initialize(feature_sum);
			for (Text value : values) {
//				System.out.print(num + value.toString() + "\n");
				String[] parts = value.toString().split(" |\t");
//				System.out.print(num + value.toString() + "\n");
				int i = 0;
				for (String part : parts) {
					if (i == 0) {
						num += Long.parseLong(part);
						i++;
						continue;
					}
//					System.out.print(part + "\n");
//					System.out.print(feature_sum[i] + "\n");
					feature_sum[i-1] += Double.parseDouble(part);
					i++;
				}
				
			}
			String feature_str = num + " ";
			for (Double feature : feature_sum) {
				feature_str += feature.toString() + " ";
			}
			context.write(key, new Text(feature_str.substring(0, feature_str.length()-1)));
		}
	}
	
	public static class KRed extends Reducer<IntWritable, Text, Text, IntWritable> {
//		static Double[][] centers;
//		public void setup(Context context) throws IOException {
//			   System.out.print("In setup\n");
//			   FileSystem fs;
//			   try {
//					fs = FileSystem.get(new URI("/user/dave"),context.getConfiguration());
//					centers = load_centers(fs);
//			   } catch (URISyntaxException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//			   }
//			   System.out.print(centers[0][0] + "," + centers[0][1] + "," + centers[1][0] + "," + centers[1][1] + "\n");
//		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			System.out.print("In reduce: " + key.toString() + " with values = " + values.toString() + "\n");
			
			long num = 0;
			Double[] feature_sum = new Double[features_length + 1];
			initialize(feature_sum);
			for (Text value : values) {
				System.out.print(num + " " + value.toString() + "\n");
				String[] parts = value.toString().split(" |\t");
				System.out.print(num + " " + value.toString() + "\n");
				int i = 0;
				for (String part : parts) {
					if (i == 0) {
						num += Long.parseLong(part);
						i++;
						continue;
					}
					System.out.print(part + "\n");
					System.out.print(feature_sum[i-1] + "\n");
					feature_sum[i-1] += Double.parseDouble(part);
					i++;
				}
			}
			System.out.print("Done with sums: " + key.toString() + " with total :" + num + "\n");
			int i = 0;
			Double[] center = new Double[features_length + 1];
			for (Double feature : feature_sum) {
				center[i] = feature / num;
				i++;
			}
			System.out.print("Wrote out reduce: " + key.toString() + "\n");
			context.write(new Text(center[0] +" " + center[1]), key);
		}
	}
	
	public static boolean converged(long counter, FileSystem fs) throws IOException {
		boolean conv = true;
		if (counter <= 2) {
			return false;
		} else {
			Double[][] centers_new = load_centers(fs, "outp." + counter);
			Double[][] centers_old = load_centers(fs, "outp." + (counter-1));
			for (int i =0; i < centers_new.length; i++) {
				double cur_diff = 0;
				for (int j = 0; j < centers_new[i].length; j++) {
					double diff = centers_new[i][j] - centers_old[i][j];
					diff = (diff < 0) ? -diff : diff;
					cur_diff += diff;
				}
				if ( cur_diff > 1 ) {
					conv = false;
				}
			}
		}
		return conv;
	}
	
	public static void main(String[] args) throws Exception {
		int failures = 0;
		long counter = 0;
		
		Configuration rconf = new Configuration();
		Job rjob = new Job(rconf, "randcent");
		FileSystem fs = FileSystem.get(new URI("/user/dave"), rconf);
		fs.delete(new Path("outp.0"), true);
		rjob.setOutputKeyClass(Text.class);
		rjob.setOutputValueClass(Text.class);
		rjob.setMapperClass(RCMap.class);
		rjob.setReducerClass(RCRed.class);
		rjob.setMapOutputKeyClass(DoubleWritable.class);
		rjob.setMapOutputValueClass(Text.class);
		rjob.setInputFormatClass(TextInputFormat.class);
		rjob.setOutputFormatClass(TextOutputFormat.class);
		rjob.setNumReduceTasks(1);
		rjob.setJarByClass(KMeans.class);
		FileInputFormat.addInputPath(rjob, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(rjob, new Path("outp.0"));
		boolean ret_val = rjob.waitForCompletion(true);
		if (!ret_val) {
			System.out.print("RANDOM CENTERS JOB FAILURE!!!");
			System.exit(0);
		}
		System.out.print("Finished with random centers");
		
		while (!converged(counter, fs) && counter < MAX_ITR) {
			 Configuration conf = new Configuration();
			 conf.set("my.centers.path", "outp." + String.valueOf(counter));	
			 Job job = new Job(conf, "kmeans");
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);
			 job.setMapperClass(KMap.class);
			 job.setReducerClass(KRed.class);
			 job.setMapOutputKeyClass(IntWritable.class);
			 job.setMapOutputValueClass(Text.class);
			 job.setInputFormatClass(TextInputFormat.class);
			 job.setOutputFormatClass(TextOutputFormat.class);
			 // The combiner involves extra parsing, so not using it may actually be more efficient
			 job.setCombinerClass(KComb.class);
	//					 job.setPartitionerClass(WordPartitioner.class);
	//					 job.setNumReduceTasks(5);
			 
			 job.setJarByClass(KMeans.class);
	
		     FileInputFormat.addInputPath(job, new Path(args[0]));
		     counter++;
		     FileOutputFormat.setOutputPath(job, new Path("outp." + counter));
	//	     FileInputFormat.addInputPath(job, new Path("/Users/dave/proj/MRProj/output/build_mat/1385316530/part-r-00000"));
	//	     FileOutputFormat.setOutputPath(job, new Path("/Users/dave/proj/MRProj/output/collab/" + Long.toString(unixTime)));
		     ret_val = job.waitForCompletion(true);
		     if (!ret_val) {
		    	 if (failures < 2) {
		    		 failures++;
		    		 fs.delete(new Path("outp." + counter), true);
		    		 counter--;
		    	 } else {
		    		 System.out.print("KMEANS JOB FAILURE!!!");
		    		 break;
		    	 }
		     }
		}
	 }
}
