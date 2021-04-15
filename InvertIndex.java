import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class InvertIndex {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
          System.err.println("Usage: Inverted Indices <input path> <output path>");
          System.exit(2);
        }

        Job job = Job.getInstance(conf, "invert index");
        job.setJarByClass(InvertIndex.class);
        job.setMapperClass(InvertIndexMapper.class);
        job.setReducerClass(InvertIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
       
    }
    
    public static class InvertIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
    	private Text docID = new Text();
    	private Text word = new Text();

    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

    		String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();//parses the lines into words
    	    StringTokenizer tokenizer = new StringTokenizer(line);

    	    Path filesplit = (((FileSplit) context.getInputSplit()).getPath()); //gets the file name from the context
    	    docID.set(filesplit.getName());

    	    while(tokenizer.hasMoreTokens()){
    	    	word.set(tokenizer.nextToken());
    	        context.write(word, docID); //outputs word and docID
    	    }
    	}
    }
    
    public static class InvertIndexReducer extends Reducer<Text, Text, Text, Text>{ 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            HashMap<String, Integer> postings = new HashMap<String, Integer>();

            for(Text val : values){//goes through the values and adds them 
                int x;
                if(postings.containsKey(val.toString())){
                    x = postings.get(val.toString());
                }else{
                    x = 0;
                }
                postings.put(val.toString(), x + 1);
            }

            for(HashMap.Entry<String, Integer> entry : postings.entrySet()){//write
                context.write(key, new Text(entry.getKey() + "\t" + entry.getValue()));
            }

        }
    }
}