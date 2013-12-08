package projmr;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BatchRecommendation {
	
	
	static private String TABLE_NAME = "snew";
	static private String COLUMN_NAME = "song";
	static private String COLUMN_QUALIFIER_NAME = "mappedsongs";
	static private int TOP_RECOMMENDATIONS = 3;
		

	public static class CFMap extends
			Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			
			HashMap<String,Double> recommend = new HashMap<String, Double>();
			HashMap<String,Double> haveListened = new HashMap<String, Double>();
			
			//For each User store all the songs he has listened to
			//This map will help us to avoid recommending the songs that has already been listened to
			String string = value.toString();
			String[] key_value = string.split("\t");
			String user = key_value[0].trim(); 
            		String[] parts = key_value[1].split(","); 
			for(String parti: parts){
            		String[] id_counts = parti.split(":");
            		haveListened.put(id_counts[0].trim(),Double.parseDouble(id_counts[1].trim()));        	
            }
            
          //For each of the song, pull its similarity with other songs from Hbase 
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, TABLE_NAME);
            
            //For each song the user has listened to
            for(String parti: parts){
            	String[] id_counts = parti.split(":");
            	String song = id_counts[0].trim();
            	String frequency = id_counts[1].trim();
            	
            	//Fetch row from Hbase based on key
            	Get g = new Get(Bytes.toBytes(song));
            	Result r = table.get(g);
             	byte [] val = r.getValue(Bytes.toBytes(COLUMN_NAME),Bytes.toBytes(COLUMN_QUALIFIER_NAME));
           	   	String valueStr = Bytes.toString(val);
           	   	String[] songs = valueStr.split(",");
           	   	
           	   	//Recommend other similar songs
           	   	for(String partj: songs){
           	   		          	   		
           	   		String[] song_val = partj.split("=");
           	   		
           	   		//recommend only the songs that user has not listened to
           	   		if(!haveListened.containsKey(song_val[0].trim())){
           	   			
           	   	        //calculate the recommendation score:
           	   			//frequency of listened song * similarity score of similar song 
		           	   	double mux = Float.parseFloat(frequency) * Float.parseFloat(song_val[1]) ;
		           	   	// Insert or update the similar song and its recommendation score
		           	   	if(!recommend.containsKey(song_val[0].trim())){
		           	   		recommend.put(song_val[0].trim(),mux);
		           	   	}else{
		           	   		double mapval = recommend.get(song_val[0].trim());
		           	   		recommend.put(song_val[0].trim(),mapval+mux);
		           	   	}
           	   		}//the user has already listened to this song,
           	   		//hence do not recommend this song again
           	   		else{
           	   			continue;
           	   		}
           	   	}
            }
            
          		//Top Recommendations of songs based on their recommendation score
            		DecimalFormat d = new DecimalFormat("#.#####");
            		ValueComparator vc = new ValueComparator(recommend);
			TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(vc);
			sorted_map.putAll(recommend);
			String output = "";
			Integer counter = 1;
			for (Map.Entry<String, Double> entry : sorted_map.entrySet()) {
				output += entry.getKey() + "=" + d.format(entry.getValue()) + ",";
				counter++;
				if (counter > TOP_RECOMMENDATIONS)
					break;
			}
			context.write(new Text(user), new Text(output));
		}   
            
            
		public static class ValueComparator implements Comparator<String> {
			Map<String, Double> base;
			public ValueComparator(Map<String, Double> base) {
				this.base = base;
			}
			
			public int compare(String a, String b) {
				if (base.get(a) >= base.get(b)) {
					return -1;
				} else {
					return 1;
				} // returning 0 would merge keys
			}
		}   
	}
	public static void main(String[] args) throws Exception {
		
	    	Configuration conf = HBaseConfiguration.create();
	    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    	if (otherArgs.length != 1) {
	    		System.err.println("Usage:<in>");
	    		System.exit(2);
	    	}
	      
	        Job job = new Job(conf, "cf");
	        job.setMapperClass(CFMap.class);
	        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
			
		job.setJarByClass(BatchRecommendation.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
						
		FileInputFormat.addInputPath(job,new Path("src/projmr/usersongmap.txt"));
		FileOutputFormat.setOutputPath(job, new Path("src/projmr/dummy"));
				
		job.waitForCompletion(true);
	}
}
