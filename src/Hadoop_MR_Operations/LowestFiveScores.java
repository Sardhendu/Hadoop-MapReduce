// package Hadoop_MR_Operations;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LowestFiveScores{

  	// Mapper Operation, The below code will release all the elements arranged in ascending order to the Combiner. The catch here is that the key noe is the field4 column, so when the data goes to the reducer than the data is presented in ascending order
  	public static class LFiveS_mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        private Text name = new Text();
        private Integer field4_val;
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
      		String[] tokens = value.toString().split(",");
      		name.set(tokens[0]);
            field4_val = Integer.parseInt(tokens[3]);
            context.write(new IntWritable(field4_val), new Text(name));
    	}
    }

    // The combiner implement a hash map technique to store Key Value pair as: Keys are the value of Field4 and Values are Set of people names that have the same value for field4. During the Cleanup phase we just let the top 5 elements through the combiner. That way the reducer received only five elements from Each mapper output.
    public static class LFiveS_combiner extends Reducer<IntWritable, Text, IntWritable, Text>{
        private TreeMap<Integer, Set> hMap = new TreeMap<Integer, Set>();
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{    
            // Define a hash set to store names, Taking Set check duplicate element with the same name.
            Set<String> names = new HashSet<String>();          
            for (Text val: values){
                names.add(val.toString());
            }
            hMap.put(key.get(), names);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            int keepIndex = 0;
            for (Map.Entry<Integer, Set> entryKeyVal : hMap.entrySet()) {
                Iterator<String> entryVal = entryKeyVal.getValue().iterator();
                 while(entryVal.hasNext() && keepIndex<5){
                    context.write(new IntWritable(entryKeyVal.getKey()), new Text(entryVal.next()));
                    keepIndex += 1;
                }

                if (keepIndex>=5){
                    break;
                }
            }  
        }
    }


  	// Reducer Operation: The reducer receives input from several mapper and we need to perform the same step that we did in Mapper to capture only the top 5 elements that have lowest value for field4
  	public static class LFiveS_reducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private TreeMap<Integer, Set> hMap = new TreeMap<Integer, Set>();
    		
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{    
            // Define a hash set to store names, Taking Set check duplicate element with the same name.
            Set<String> names = new HashSet<String>();          
            for (Text val: values){
                names.add(val.toString());
            }
            hMap.put(key.get(), names);
        }

        // Cleanup is called by the Map function at the end of the Reduce task before writing the output to the HDFS
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int keepIndex = 0;
            for (Map.Entry<Integer, Set> entryKeyVal : hMap.entrySet()) {
                Iterator<String> entryVal = entryKeyVal.getValue().iterator();
                 while(entryVal.hasNext() && keepIndex<5){
                    StringBuilder fnlString = new StringBuilder();
                    fnlString.append(entryVal.next()).append(",").append(entryKeyVal.getKey());
                    context.write(new Text (fnlString.toString()), NullWritable.get());
                    keepIndex += 1;
                }

                if (keepIndex>=5){
                    break;
                }
            }  
        }
    }


	// Driver Operation
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    	Job job_run = new Job(new Configuration());

    	job_run.setJobName("Top 5 records with lowest field4 value");
    	job_run.setJarByClass(LowestFiveScores.class);

    	job_run.setMapperClass(LFiveS_mapper.class);
    	job_run.setReducerClass(LFiveS_reducer.class);
    
        job_run.setMapOutputKeyClass(IntWritable.class);
        job_run.setMapOutputValueClass(Text.class);
    	job_run.setOutputKeyClass(Text.class);
    	job_run.setOutputValueClass(NullWritable.class);
    
    	FileInputFormat.addInputPath(job_run, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    	job_run.waitForCompletion(true);
  	}
}

