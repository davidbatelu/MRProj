package org.myorg;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CollaborativeFiltering {
	public static HashMap<String, Integer> vector_add(
			HashMap<String, Integer> h1, HashMap<String, Integer> h2) {
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

	public static class CFMap extends
			Mapper<LongWritable, Text, Text, BytesWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			String string = value.toString();
			HashMap<String, Integer> h = new HashMap<String, Integer>();
			// parts = (user_id, song_id, play_count)
			String[] key_value = string.split("\t");
			String[] parts = key_value[1].split(",");
			for (String parti : parts) {
				String[] id_countsi = parti.split(":");
				Integer counti = new Integer(id_countsi[1]);
				// global_hash.put(id_countsi[0], counti);
				for (String partj : parts) {
					String[] id_countsj = partj.split(":");
					h.put(id_countsj[0], new Integer(id_countsj[1]) * counti);
				}
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(h);
				objOut.close();
				context.write(new Text(id_countsi[0]),
						new BytesWritable(out.toByteArray()));
			}
			// context.write(new Text("test"), new Text("123"));
		}
	}

	@SuppressWarnings("deprecation")
	public static HashMap<String, Integer> load_hash(FileSystem fs)
			throws IOException {
		System.out.print("In load_hash");
		String line;
		// FSDataInputStream cache = fs.open(new Path("/user/dave/counts.txt"));
		FSDataInputStream cache = fs.open(new Path("/Users/nischintr/output2mini/part-r-00000"));
		//FSDataInputStream cache = fs.open(new Path("s3n://mrproj/input/op2"));
		HashMap<String, Integer> hash = new HashMap<String, Integer>();
		try {
			while ((line = cache.readLine()) != null) {
				String[] parts = line.trim().split("\t");
				hash.put(parts[0], new Integer(parts[1]));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hash;
	}

	public static class CFCom extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (BytesWritable val : values) {
				ObjectInputStream objIn = new ObjectInputStream(
						new ByteArrayInputStream(val.getBytes()));
				try {
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> actual = (HashMap<String, Integer>) objIn
							.readObject();
					map = vector_add(actual, map);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(map);
			objOut.close();
			context.write(key, new BytesWritable(out.toByteArray()));
		}
	}

	public static class CFRed extends Reducer<Text, BytesWritable, Text, Text> {
		HashMap<String, Integer> counts;

		public void setup(Context context) throws IOException {
			System.out.print("In setup");
			FileSystem fs;
			try {
				// fs = FileSystem.get(new
				// URI("/user/dave"),context.getConfiguration());
				fs = FileSystem.get(new URI("/Users/nischintr"), context.getConfiguration());
				//fs = FileSystem.get(new URI("s3n://mrproj"),context.getConfiguration());
				counts = load_hash(fs);
			} catch (URISyntaxException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		public Double computeSimilarity(int type, Integer dotij, Integer ni,
				Integer nj) {
			switch (type) {
			case 0:
				// Jaccard
				return (double) dotij / (ni + nj + dotij);
			default:
				return (double) dotij;
			}
		}

		public void reduce(Text key, Iterable<BytesWritable> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (BytesWritable val : values) {
				ObjectInputStream objIn = new ObjectInputStream(
						new ByteArrayInputStream(val.getBytes()));
				try {
					@SuppressWarnings("unchecked")
					HashMap<String, Integer> actual = (HashMap<String, Integer>) objIn
							.readObject();
					map = vector_add(actual, map);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// String str = new String();
			HashMap<String, Double> S = new HashMap<String, Double>();
			DecimalFormat d = new DecimalFormat("#.#####");
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				String mapkey = (String) entry.getKey();
				Integer value = (Integer) entry.getValue();
				// This may be redundant for now but will be required later when
				// we have values other thank just 1
				// Have to do an actual similarity function here.
				// Figure out how we'll get the norms
				Double tmpval = computeSimilarity(0, value,
						counts.get(key.toString()), counts.get(mapkey));
				// S.put(mapkey, d.format(tmpval));
				S.put(mapkey, tmpval);
				// str += mapkey + ":" + tmpval.toString() + ",";
			}
			// String will definitely contain something as a user cannot exists
			// who has not listened to a song
			// TODO: Remove curly braces from start and end

			ValueComparator vc = new ValueComparator(S);
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(vc);
			sorted_map.putAll(S);
			String output = "";
			Integer counter = 1;
			for (Map.Entry<String, Double> entry : sorted_map.entrySet()) {
				output += entry.getKey() + "=" + d.format(entry.getValue()) + ",";
				counter++;
				if (counter > 100)
					break;
			}
			// System.out.println(output.substring(0,output.length()-1));

			context.write(key,
					new Text(output.substring(0, output.length() - 1)));
		}
	}

	public static class ValueComparator implements Comparator<String> {

		Map<String, Double> base;

		public ValueComparator(Map<String, Double> base) {
			this.base = base;
		}

		// Note: this comparator imposes orderings that are inconsistent with
		// equals.
		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			} // returning 0 would merge keys
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// long unixTime = System.currentTimeMillis() / 1000L;

		Job job = new Job(conf, "cf");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CFMap.class);
		job.setReducerClass(CFRed.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setCombinerClass(CFCom.class);

		// job.setPartitionerClass(WordPartitioner.class);
		// job.setNumReduceTasks(5);

		job.setJarByClass(CollaborativeFiltering.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// FileInputFormat.addInputPath(job, new
		// Path("/Users/dave/proj/MRProj/output/build_mat/1385316530/part-r-00000"));
		// FileOutputFormat.setOutputPath(job, new
		// Path("/Users/dave/proj/MRProj/output/collab/" +
		// Long.toString(unixTime)));
		job.waitForCompletion(true);
	}
}
