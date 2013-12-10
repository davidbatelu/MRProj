package org.myorg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.lang.Math.pow;

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

import au.com.bytecode.opencsv.CSVParser;


public class Kmeans {
	
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

	static final int ARTISTS_TERMS_FREQ_LENGTH = 13;

	static final int ARTISTS_TERMS_FREQ_ELEM = 14;

	static final int DURATION = 15;

	static final int KEY = 16;

	static final int LOUDNESS = 17;

	static final int MODE = 18;

	static final int TEMPO = 19;

	static final int TIME_SIGNATURE = 20;

	static final int SEGMENT_LOUDNESS = 21;

	static final int SEGMENT_TIMBRE = 22;

	static final int YEAR = 23;

	static final int GENRE = 24;

	static final int SIMILAR_ARTISTS = 25;

	static final int ARTIST_TERMS = 26;
	
	// All numerical features
	static Integer []REQUIRED_FIELDS = {ARTIST_FAMILIARITY, ARTIST_HOTNESS, SONG_HOTNESS, DANCEABILITY, ENERGY, 
		DURATION, KEY, LOUDNESS, MODE, TEMPO, TIME_SIGNATURE, SEGMENT_LOUDNESS, SEGMENT_TIMBRE, YEAR};
	
	static List<Integer> REQUIRED_LIST = Arrays.asList(ARTIST_FAMILIARITY, ARTIST_HOTNESS, SONG_HOTNESS, DANCEABILITY, ENERGY, 
			DURATION, KEY, LOUDNESS, MODE, TEMPO, TIME_SIGNATURE, SEGMENT_LOUDNESS, SEGMENT_TIMBRE, YEAR);
	
	static int K = 10, features_length = 27; 
	static Double CONVERGENCE_DIFF = (double) 1;
	static Random generator;
	static long MAX_ITR = 10;
	
	// Random Points Generator Job.
	// This job is associates every data point with a random number between 0 and 1.
	// We use that random number to pick the top K jobs.
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
				// Instead of "1" could put a counter value here
				context.write(new Text(value.toString() + ",1"), new Text());
			}
		}
	}
	
	// Helper function used for concatenating a string array
	// Primarily used for debugging
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
	
	
	// The main distance function calculation. Given two points it calculates the distance between them
	public static final double measureDistance(String[] centers, String[] row) {
	  double sum = 0;
	  // Ignore last label
	  for (int i = 0; i < REQUIRED_FIELDS.length ; i++) {
//		  if (centers[REQUIRED_FIELDS[i]].isEmpty() || row[REQUIRED_FIELDS[i]].isEmpty()) {
//			  continue;
//		  }
		  sum += Math.abs(Double.parseDouble(centers[REQUIRED_FIELDS[i]]) - Double.parseDouble(row[REQUIRED_FIELDS[i]]));
	  }
	  return sum;
	}
	
	// Function to load the current centers (for this iteration of K-Means) into memory.
	@SuppressWarnings("deprecation")
	public static String[][] load_centers(FileSystem fs, String subPath) throws IOException {
        String line;
        String[][] temp_centers = new String[K][];
        System.out.print("In load_centers\n");
		FSDataInputStream cache = fs.open(new Path("/user/dave/" + subPath  + "/part-r-00000"));
	   try {
		   int i = 0;
		   CSVParser csv = new CSVParser((char)',');
		   while((line = cache.readLine()) != null && i < K){
			   System.out.print(line + "\n");
			   String[] parts = csv.parseLine(line);

			   // Ignore the first number as it is the random number
               temp_centers[i] = new String[parts.length];
               for (int j = 0; j < parts.length; j++) {
            	   temp_centers[i][j] = parts[j];
               }
               i++;
		   }
	   } catch (IOException e) {
	            e.printStackTrace();
	   }
       System.out.print("End of load_centers\n");		   
	   return temp_centers;
	}
	
	// The K-Means job. 
	// The mapper is responsible for emitting the index of the closest center
	public static class KMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		static String[][] centers;	
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
//			   System.out.print(centers[0][0] + "," + centers[0][1] + "," + centers[1][0] + "," + centers[1][1] + "," + centers[2][0] + "," + centers[2][1] + "\n");
		   }
		
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
//			String[] parts = value.toString().split(" |\t");
			System.out.print("Working on : " + key.toString() + "\n");
			CSVParser csv = new CSVParser((char)',');
			String[] parts = csv.parseLine(value.toString());
			String[] row = new String[parts.length];
			int i = 0, minIdx = -1;
			double min = 99999999.0;
			for (String part : parts) {
				row[i] = new String(part);
				i++;
			}
			for (int c = 0; c < centers.length; c++) {
				try {
					double curDst = measureDistance(centers[c], row);
					if (curDst < min) {
						min = curDst;
						minIdx = c;
					}
				}  catch (Exception e) {
					  System.out.print(e + "\nIN MEASURE!!!!\n");
					  System.out.print(combine(centers[c], ",")+"\n" + combine(row, ",") + "\n");
					  System.exit(1);
				}
				
			}
			// here 1 is the count of values aggregated into value
			context.write(new IntWritable(minIdx), new Text("1," + value.toString()));
		}
		
	}
	
	public static void initialize(String[] arr) {
		for (int i = 0; i < arr.length; i++) {
			arr[i] = "0";
		}
	}
	
	// Helper function that is used to add two data points. 
	// It considers only those fields present in the REQUIRED_LIST, ignoring 
	// the rest
	public static String[] sum_features(String[] f1, String f2[]) {
		System.out.print("Inside sum_features\n");
		String[] sum = new String[f1.length];
		initialize(sum);
		int i = 0;
		for (; i < f1.length; i++) {
				if (REQUIRED_LIST.contains(i)) {
					sum[i]  = String.valueOf(Double.parseDouble(f1[i]) + Double.parseDouble(f2[i+1]));
				}
//			}
		}
		return sum;
	}
	
	// The combiner just adds data points to form an aggregated data point 
	// This aggregated data point is used to calculate the new center
	public static class KComb extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			System.out.print("In combine: " + key.toString() + " with values = " + values.toString() + "\n");
			
			long num = 0;
			String[] feature_sum = new String[features_length + 1];
			initialize(feature_sum);
			System.out.print("Starting sum\n");
			for (Text value : values) {
				System.out.print(value.toString() + "\n");
				CSVParser csv = new CSVParser((char)',');
				String[] parts = csv.parseLine(value.toString());
				
				// Could break here
				feature_sum = sum_features(feature_sum, parts);
				num += Long.parseLong(parts[0]);
				
			}
			System.out.print("Starting out\n");
			String feature_str = num + ",";
			for (String feature : feature_sum) {
				feature_str += feature + ",";
			}
			context.write(key, new Text(feature_str.substring(0, feature_str.length()-1)));
		}
	}
	
	// K-Means Reducer
	// The reducer calculates the new center
	public static class KRed extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			System.out.print("In reduce: " + key.toString() + " with values = " + values.toString() + "\n");

			long num = 0;
			String[] feature_sum = new String[features_length + 1];
			initialize(feature_sum);
			for (Text value : values) {
				CSVParser csv = new CSVParser((char)',');
				String[] parts = csv.parseLine(value.toString());
			
				// Could break here
				feature_sum = sum_features(feature_sum, Arrays.copyOfRange(parts, 1, parts.length));
				num += Long.parseLong(parts[0]);				
			}
			System.out.print("Done with sums: " + key.toString() + " with total :" + num + "\n");
			int i = 0;
			String center = "";
			for (String feature : feature_sum) {
				if (Arrays.asList(REQUIRED_FIELDS).contains(i)) {
					center += String.valueOf(Double.parseDouble(feature) / num) + ",";
				} else {
					center += feature + ",";
				}
				i++;
			}
			System.out.print("Wrote out reduce: " + key.toString() + " : " + center + "\n");
			context.write(new Text(center.substring(0, center.length()-1) + "," + key.toString()), new Text());
		}
	}
	
	// Helper function to write to a file in hdfs
	public static void write_to_file(String fn, FileSystem fs, String buf) throws IOException {
		Path file = new Path(fn);
		if ( fs.exists( file )) { fs.delete( file, true ); } 
		OutputStream os = fs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write(buf);
		br.close();
	}
	
	// Helper function to read from an hdfs file
	public static String read_from_file(String fn, FileSystem fs, boolean keep_lines) throws IOException {
		Path pt = new Path(fn);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        String buf = "";
        line=br.readLine();
        while (line != null){
            buf += line;
            if (keep_lines) {
            	buf += "\n";
            }
        	line=br.readLine();    
        }
        return buf;
	}
	
	// Reducer for the Summarizer job.
	// The summarize job is responsible to splitting the data set into K parts, 
	// based on the centers we calculate using K-Means
	public static class SRed extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			     throws IOException, InterruptedException {
			Long count = 0L;
			for (Text value : values) {
				count++;
				String s = value.toString();
				// Ignore "1," in the beginning of value
				context.write(new Text(s.substring(2, s.length())), new Text());
			}
			System.out.print("Count for " + key.toString() + " is " + count.toString() + "\n");
//			context.getConfiguration().set("count." + key.toString(), count.toString());
			FileSystem fs;
			try {
				fs = FileSystem.get(new URI("/user/dave"), context.getConfiguration());
				System.out.print("File is : " + context.getConfiguration().get("base_dir") + ".count." + key.toString() + "\n");
				write_to_file(context.getConfiguration().get("base_dir") + ".count." + key.toString(), fs, count.toString());
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	// Helper function to check if K-Menas has converged
	public static boolean converged(String base,long counter, FileSystem fs) throws IOException {
		boolean conv = true;
		if (counter <= 2) {
			return false;
		} else {
			String[][] centers_new = load_centers(fs, base + counter);
			String[][] centers_old = load_centers(fs, base + (counter-1));
			for (int i =0; i < centers_new.length; i++) {
				double cur_diff = 0;
				if (REQUIRED_LIST.contains(i)) {
					continue;
				}
				for (int j = 0; j < centers_new[i].length; j++) {
					if (REQUIRED_LIST.contains(j)) {
						continue;
					}	
					Double diff = Double.parseDouble(centers_new[i][j]) - Double.parseDouble(centers_old[i][j]);
					diff = (diff < 0) ? -diff : diff;
					cur_diff += diff;
				}
				if ( cur_diff > CONVERGENCE_DIFF ) {
					conv = false;
				}
			}
		}
		return conv;
	}
	
	// Helper function to create the random centers job
	public static boolean random_centers(FileSystem fs, int level, int itr, String ip) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration rconf = new Configuration();
		Job rjob = new Job(rconf, "randcent");
		fs.delete(new Path(level + "." + itr + ".cent.0"), true);
		rjob.setOutputKeyClass(Text.class);
		rjob.setOutputValueClass(Text.class);
		rjob.setMapperClass(RCMap.class);
		rjob.setReducerClass(RCRed.class);
		rjob.setMapOutputKeyClass(DoubleWritable.class);
		rjob.setMapOutputValueClass(Text.class);
		rjob.setInputFormatClass(TextInputFormat.class);
		rjob.setOutputFormatClass(TextOutputFormat.class);
		// TODO: Handle multiple reduce by changing load_cents to use only the 1st file
		rjob.setNumReduceTasks(1);
		rjob.setJarByClass(Kmeans.class);
		FileInputFormat.addInputPath(rjob, new Path(ip));
		
		FileOutputFormat.setOutputPath(rjob, new Path(level + "." + itr + ".cent.0"));
		return rjob.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		int failures = 0;
		int level = 0, Max_Levels = 2;
		boolean exit = false;
		// keep a higher permissible dev
		float Max_Dev = (float) 0.5;
		boolean redo;
		Configuration dummy_conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("/user/dave"), dummy_conf);
		int kmeans_redo;
		int kmeans_max_redo = 2;
		
		
		// The main K-Means driver code.
		// This code tries to orchestrate the creation of good clusters.
		// We try and form a K sub clusters for the data set, and then further split 
		// these K clusters into K sub clusters (each).
		// If the cluster is not to our liking, we try to rerun the job for that data set.
		while (!exit && level < Max_Levels) {
			System.out.print("LEVEL - " + level + "\n");
			int iterations = (int) pow(K, level);
			System.out.print("ITERATIONS ARE - " + iterations + "\n");
			for (int itr = 0; itr < iterations && !exit; itr++) {
				System.out.print("LEVEL - " + level + "    ITR - " + itr + "\n");
				redo = true;
				int redo_cnt = 0;
				while (redo && redo_cnt < 3) {
					long counter = 0;
					redo = false;
					int p = itr / K;
					String input_fn;
					if (level == 0) {
						input_fn = args[0];
					} else {
						input_fn = (level - 1) + "." + p + ".ip/part-r-0000" + (itr % K);
					}
					// Random Centers job
					boolean ret_val = random_centers(fs, level, itr, input_fn);
					if (!ret_val) {
						System.out.print("RANDOM CENTERS JOB FAILURE!!!");
						itr--;
						failures++;
						if (failures < 2) {
							break; //out from redo loop
						} else {
							System.exit(0);
						}
					}
					System.out.print("Finished with random centers");
					
					
					// K-Means job
					kmeans_redo = 0;
					while (!converged(level + "." + itr + ".cent.", counter, fs) && counter < MAX_ITR && !exit) {
						 Configuration conf = new Configuration();
						 conf.set("my.centers.path", level + "." + itr + ".cent." + String.valueOf(counter));	
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
						 
						 job.setJarByClass(Kmeans.class);
				
					     FileInputFormat.addInputPath(job, new Path(input_fn));
					     counter++;
					     fs.delete(new Path(level + "." + itr + ".cent." + counter), true);
					     FileOutputFormat.setOutputPath(job, new Path(level + "." + itr + ".cent." + counter));
				//	     FileInputFormat.addInputPath(job, new Path("/Users/dave/proj/MRProj/output/build_mat/1385316530/part-r-00000"));
				//	     FileOutputFormat.setOutputPath(job, new Path("/Users/dave/proj/MRProj/output/collab/" + Long.toString(unixTime)));
					     ret_val = job.waitForCompletion(true);
					     if (!ret_val) {
					    	 if (failures < 2) {
					    		 failures++;
					    		 fs.delete(new Path(level + "." + itr + ".cent." + counter), true);
					    		 counter--;
					    	 } else {
					    		 System.out.print("KMEANS JOB FAILURE!!!");
					    		 System.exit(0);
					    	 }
					     }
					     String OP = read_from_file(level + "." + itr + ".cent." + counter + "/part-r-00000", fs, true);
					     if (OP.split("\n").length != K) {
					    	 System.out.print("NOT GOING WELL!!!! ---- " + OP + "\n");
					    	 
					    	 counter--;
					    	 kmeans_redo++;
					    	 if (kmeans_redo > kmeans_max_redo) {
					    		 exit = true;
					    		 break;
					    	 }
					     }
					}
					
					 // Actual splits
					 // The summarize job
					 Configuration sconf = new Configuration();
					 sconf.set("my.centers.path", level + "." + itr + ".cent." + String.valueOf(counter));
					 sconf.set("base_dir", level + "." + itr);
					 Job sjob = new Job(sconf, "datasplit");
					 sjob.setOutputKeyClass(Text.class);
					 sjob.setOutputValueClass(Text.class);
					 sjob.setMapperClass(KMap.class);
					 sjob.setReducerClass(SRed.class);
					 sjob.setMapOutputKeyClass(IntWritable.class);
					 sjob.setMapOutputValueClass(Text.class);
					 sjob.setInputFormatClass(TextInputFormat.class);
					 sjob.setOutputFormatClass(TextOutputFormat.class);
					 sjob.setJarByClass(Kmeans.class);
					 sjob.setNumReduceTasks(K);
					 
				     FileInputFormat.addInputPath(sjob, new Path(input_fn));
				     fs.delete(new Path(level + "." + itr + ".ip"), true);
				     FileOutputFormat.setOutputPath(sjob, new Path(level + "." + itr +".ip"));
				     sjob.waitForCompletion(true);
				     
				     Long[] counts = new Long[K];
				     Long sum = 0L;
				     for (Integer i = 0; i < K; i++) {
				    	 System.out.print("Count string " + i.toString() + " : " + read_from_file(level + "." + itr + ".count."+ i.toString(), fs, false) + "\n");
						 counts[i] = new Long(read_from_file(level + "." + itr + ".count."+ i.toString(), fs, false));
						 sum += counts[i];
					 }
				     Long avg = sum / K;
				     for (int i = 0; i < K; i++) {
				    	 long diff = avg - counts[i];
				    	 diff = diff < 0 ? -diff: diff;
				    	 float dev = (float) diff / avg;
				    	 System.out.print("Inside : " + i + " with dev : " + dev + "\n");
				    	 if (dev >= Max_Dev) {
				    		 System.out.print("GOTTA REDO!!");
				    		 // Limit consecutive redos
				    		 redo = true;
				    		 redo_cnt++;
				    	 }
				     }
				} // redo loop
			} // itr loop
			level++;
		}
		if (exit) {
			System.out.print("BAD EXIT!!\n");
		}
	 }
}
