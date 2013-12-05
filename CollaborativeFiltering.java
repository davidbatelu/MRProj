package projmr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class CollaborativeFiltering {
	
	 static private String TABLE_NAME = "snew";
	 static private String COLUMN_NAME = "song";
	 static private String COLUMN_QUALIFIER_NAME = "mappedsongs";
	  
	
	
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
//                        parts = (user_id, song_id, play_count)
                        String[] key_value = string.split("\t");
                        String[] parts = key_value[1].split(",");
                        
                        for (String parti : parts) {
                        	//System.out.println(parti);
                                String[] id_countsi = parti.split(":");
                                Integer counti = new Integer(id_countsi[1]);
//                                global_hash.put(id_countsi[0], counti);
                                for (String partj : parts) {
                                        String[] id_countsj = partj.split(":");
                                        //System.out.println(id_countsj[0]);
                                        //System.out.println(new Integer(id_countsj[1]));
                                        //System.out.println(new Integer(id_countsj[1]) * counti);
                                        
                                        h.put(id_countsj[0], new Integer(id_countsj[1]) * counti);
                                }
                                ByteArrayOutputStream out = new ByteArrayOutputStream();
                                ObjectOutputStream objOut = new ObjectOutputStream(out);
                                objOut.writeObject(h);
                            objOut.close();
                                context.write(new Text(id_countsi[0]), new BytesWritable(out.toByteArray()));
                        }
//                    context.write(new Text("test"), new Text("123"));
                }
        }
        
        @SuppressWarnings("deprecation")
        public static HashMap<String, Integer> load_hash(FileSystem fs) throws IOException {
                System.out.print("In load_hash");
        String line;
                FSDataInputStream cache = fs.open(new Path("/home/dheemanth/workspace/hadoop_test/src/projmr/counts.txt"));
                HashMap<String, Integer> hash = new HashMap<String, Integer>();
           try {
                   while((line = cache.readLine()) != null ){
               String[] parts = line.trim().split("\t");
               hash.put(parts[0], new Integer (parts[1]));
                   }
           } catch (IOException e) {
                           // TODO Auto-generated catch block
                    e.printStackTrace();
           }
           return hash;
        }
                
        public static class CFRed extends
        TableReducer<Text, BytesWritable, ImmutableBytesWritable> {
           HashMap<String, Integer> counts;

           public static final byte[] CF = "cf".getBytes(); 
           public static final byte[] COUNT = "count".getBytes();
           
           public void setup(Context context) throws IOException {
                   System.out.print("In setup");
                   FileSystem fs;
                   try {
                                fs = FileSystem.get(new URI("/home/dheemanth/workspace/hadoop_test/src/projmr"),context.getConfiguration());
                                counts = load_hash(fs);
                   } catch (URISyntaxException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                   }
           }
           
           public float Similarity(int type, Integer dotij, Integer ni, Integer nj) {
                   switch (type) {
                                   case 0:
                                           // Jaccard
                                           return (float) dotij / (ni + nj + dotij); 
                                   default:
                                           return (float) dotij;
                   }
           }
           
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
//               String str = new String();
               HashMap<String, String> S = new HashMap<String, String>();
               DecimalFormat d = new DecimalFormat("#.#####");
               for (Map.Entry<String, Integer> entry : map.entrySet()) {
                       String mapkey = (String) entry.getKey();
                       Integer value = (Integer) entry.getValue();
                       // This may be redundant for now but will be required later when we have values other thank just 1
                       // Have to do an actual similarity function here.
                       // Figure out how we'll get the norms
                       Float tmpval = Similarity(0, value, counts.get(key.toString()), counts.get(mapkey));
                       S.put(mapkey, d.format(tmpval));
//                       str += mapkey + ":" + tmpval.toString() + ",";
               }
               // String will definitely contain something as a user cannot exists who has not listened to a song
               // TODO: Remove curly braces from start and end
               String str = S.toString();
               
               //write to Hbase
               Put put = new Put(Bytes.toBytes(key.toString()));
               //Columns in HBase are comprised of a column family prefix[COLUMN_NAME] followed by a column qualifier suffix[COLUMN_QUALIFIER_NAME].
               put.add(Bytes.toBytes(COLUMN_NAME), Bytes.toBytes(COLUMN_QUALIFIER_NAME), Bytes.toBytes(str.substring(1, str.length()-1)));
               context.write(null, put);
           }
               
        }
        
        
        public static void main(String[] args) throws Exception {
	        
        	//Create a table with a column name
        	Configuration conf = HBaseConfiguration.create();
        	
        	String[] otherArgs = new GenericOptionsParser(conf, args)
			.getRemainingArgs();
        	if (otherArgs.length != 1) {
        			System.err.println("Usage:<in>");
        			System.exit(2);
        	}
        	
        	
	        HTableDescriptor ht = new HTableDescriptor(TABLE_NAME); 
	        ht.addFamily( new HColumnDescriptor(COLUMN_NAME));
	        System.out.println( "connecting" );
	        HBaseAdmin hba = new HBaseAdmin(conf);
	        System.out.println( "Creating Table" );
	        hba.createTable(ht);
	        conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
	            
	        Job job = new Job(conf, "cf");
	        job.setJarByClass(CollaborativeFiltering.class);
	        job.setMapperClass(CFMap.class);
	        job.setReducerClass(CFRed.class);
	             
	        //System.out.println(job.getJar());
	        //TableMapReduceUtil.initTableReducerJob(TABLE_NAME, CollaborativeFiltering.CFRed.class, job);
	             
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(BytesWritable.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TableOutputFormat.class);
	        
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        //new Path("src/projmr/usersongmap.txt"));
	        job.waitForCompletion(true);
	        
	        //Reading from Hbase to make sure the writes are proper
	        HTable table = new HTable(conf, TABLE_NAME);
	        byte[] family = Bytes.toBytes(COLUMN_NAME);
	        byte[] qual = Bytes.toBytes(COLUMN_QUALIFIER_NAME);

	            Scan scan = new Scan();
	            scan.addColumn(family, qual);
	            ResultScanner rs = table.getScanner(scan);
	            for (Result r = rs.next(); r != null; r = rs.next()) {
	            	String key = Bytes.toString(r.getRow()); 
	                byte[] valueObj = r.getValue(family, qual);
	                String value = new String(valueObj);
	                System.out.println(key +"=>"+value);
	            }
      
         //job.setOutputFormatClass(TextOutputFormat.class);
         //job.setCombinerClass(CFRed.class);
         //job.setPartitionerClass(WordPartitioner.class);
         //job.setNumReduceTasks(5);
         //FileInputFormat.addInputPath(job, new Path(args[0]));
         //FileOutputFormat.setOutputPath(job, new Path(args[1]));
         //FileOutputFormat.setOutputPath(job, new Path("output/collab/"));
 
   }
}
