import java.io.*;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopN {
	
	public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        conf.set("myValue", otherArgs[2]);
        System.out.println(conf.get("myValue"));
  
        // if less than two paths 
        // provided will show error
        if (otherArgs.length < 2) 
        {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }
  
        Job job = Job.getInstance(conf, "TopN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(topNMapper.class);
        job.setReducerClass(topNReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
	
	
	

	public static class topNMapper extends Mapper<Object, Text, LongWritable, Text> {

		// data format => word	filename	count
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			String[] tokens = value.toString().split("\t");//split the word by a tab

			String word = tokens[0];
			long count = Long.parseLong(tokens[2]);

			count = (-1) * count;//make negative to auto sort
			
			context.write(new LongWritable(count), new Text(word));//write
		}
	}
	
	

	public static class topNReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		static int count;
		static TreeMap<String, Long> map;

		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			
			Configuration conf = context.getConfiguration();
			String param = conf.get("myValue");
			count = Integer.parseInt(param);
			map = new TreeMap<String, Long>();
		}
		
		public static <K, V extends Comparable<V> > Map<K, V>
	    valueSort(final Map<K, V> map)
	    {//comparable to flip and sort a treemap
	   
	        Comparator<K> valueComparator = new Comparator<K>() {
	            
	                  // return comparison results of values of
	                  // two keys
	        	public int compare(K k1, K k2){
	        		int comp = map.get(k1).compareTo(map.get(k2));
	                 if (comp == 0)
	                	 return 1;
	                 else
	                     return comp;
	            }
	            
	         };
	        
	        // SortedMap created using the comparator
	        Map<K, V> sorted = new TreeMap<K, V>(valueComparator);
	        
	        sorted.putAll(map);
	        
	        return sorted;
	    }

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{

			// key				 values
			//-ve of wordCount [ name ..]
			long wordCount = key.get();

			String name = null;

			for (Text val : values)
			{
				name = val.toString();
			}
			
			
			if(map.containsKey(name)) {//if its already in the map then just add the new count to the current one
				map.put(name, map.get(name) + wordCount);
			}else {//if its not in the map add the word and the count
				map.put(name, wordCount);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			Map sorted = valueSort(map);
			
			Set set = sorted.entrySet();
			
			Iterator i = set.iterator();
			
			while(i.hasNext()) { //iterates through the map and writes the count amount
				Map.Entry mp = (Map.Entry)i.next();
				
				if(count > 0) {
					context.write(new LongWritable((-1) * (long) mp.getValue()), new Text((String) mp.getKey()));
					count--;
				}
			}
		}
	}
}
