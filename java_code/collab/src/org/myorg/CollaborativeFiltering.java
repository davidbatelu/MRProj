package org.myorg;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.myorg.BuildSongsMat.BuildSongsMatMap;
import org.myorg.BuildSongsMat.BuildSongsMatRed;

public class CollaborativeFiltering {
	public static HashMap<String, Integer> vector_add(HashMap<String, Integer> h1, HashMap<String, Integer>h2) {
		HashMap<String, Integer> h3 = new HashMap<String, Integer>();
		h3.putAll(h1);
		for (Map.Entry<String, Integer> entry : h2.entrySet()) {
			String key = (String) entry.getKey();
			Integer value = (Integer) entry.getValue();
			Integer old_value = h3.get(key);
			if (old_value != null) {
				h3.put(key, new Integer(old_value + value));
			} else {
				h3.put(key, value);
			}
			
		}
		return h3;
	}
	
	public static class CFMap extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
			String string = value.toString();
			HashMap<String, Integer> h = new HashMap<String, Integer>();
//			parts = (user_id, song_id, play_count)
			String[] key_value = string.split("\t");
			String[] parts = key_value[1].split(",");
			for (String parti : parts) {
				for (String partj : parts) {
					h.put(partj, new Integer(1));
				}
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(h);
			    objOut.close();
				context.write(new Text(parti), new BytesWritable(out.toByteArray()));
			}
//		    context.write(new Text("test"), new Text("123"));
		}
	}
	
//	ByteArrayOutputStream out = new ByteArrayOutputStream();
//    ObjectOutputStream objOut = new ObjectOutputStream(out);
//    objOut.writeObject(map);
//    objOut.close();
//    Key.xor = 0x7555AAAA; // make the hashcodes different
//    ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
//    HashMap actual = (HashMap) objIn.readObject();
	
		
	public static class CFRed extends Reducer<Text, BytesWritable, Text, Text> {
	   public void reduce(Text key, Iterable<BytesWritable> values, Context context) 
	     throws IOException, InterruptedException {
    	   HashMap<String, Integer> map = new HashMap<String, Integer>();
	       for (BytesWritable val : values) {
	    	   ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(val.getBytes()));
	    	   try {
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> actual = (HashMap<String, Integer>) objIn.readObject();
					map = vector_add(actual, map);
			   } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			   }   
	       }
	       HashMap<String, Integer> S = new HashMap<String, Integer>();
	       for (Map.Entry<String, Integer> entry : map.entrySet()) {
	    	   String mapkey = (String) entry.getKey();
	    	   Integer value = (Integer) entry.getValue();
	    	   // This may be redundant for now but will be required later when we have values other thank just 1
	    	   // Have to do an actual similarity function here.
	    	   // Figure out how we'll get the norms
	    	   S.put(mapkey, value);
	       }
	       // String will definitely contain something as a user cannot exists who has not listened to a song
	       context.write(key, new Text(S.toString()));
	   }
	}
	
	public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
	 long unixTime = System.currentTimeMillis() / 1000L;

	 Job job = new Job(conf, "cf");
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);
	 job.setMapperClass(CFMap.class);
	 job.setReducerClass(CFRed.class);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(BytesWritable.class);
	 job.setInputFormatClass(TextInputFormat.class);
	 job.setOutputFormatClass(TextOutputFormat.class);
//	 job.setCombinerClass(CFRed.class);
//				 job.setPartitionerClass(WordPartitioner.class);
//				 job.setNumReduceTasks(5);
	 
	 job.setJarByClass(CollaborativeFiltering.class);

     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
//     FileInputFormat.addInputPath(job, new Path("/Users/dave/proj/MRProj/output/build_mat/1384820332/part-r-00000"));
//     FileOutputFormat.setOutputPath(job, new Path("/Users/dave/proj/MRProj/output/collab/" + Long.toString(unixTime)));
     job.waitForCompletion(true);
   }
}
