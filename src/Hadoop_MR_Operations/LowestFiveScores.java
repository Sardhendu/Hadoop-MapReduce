// package Hadoop_MR_Operations;

import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LowestFiveScores{

  	// Mapper Operation, The below code will release five element with lowest scores from each input split that the mapper operates on.
  	public static class LFiveS_mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        private Text name = new Text();
        private Integer field4_val;
        private TreeMap<Integer, Text> recordHolder = new TreeMap<Integer, Text>();
    
    		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
      			String[] tokens = value.toString().split(",");
      			name.set(tokens[0]);

            // Selecting the Token 3 for Min, Max and Mean operation in Combiner and Reducer
            field4_val = Integer.parseInt(tokens[3]);
            recordHolder.put(field4_val, name);

            if (recordHolder.size() > 5) {
                recordHolder.remove(recordHolder.lastKey());   
                // removes the last element from the tree map. A TreeMap contains elements in accending tree structure based on its keys
            } 
    		}

        // Cleanup is called by the Map function at the end of the Map task on one input split (i.e when all the lines of the files are already read). We overide the clean up method and pass the TreeMap with the top 5 elements using the context.write.
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<Integer,Text> entry : recordHolder.entrySet()) {
                context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
            } 
        }
  	}


  	// Reducer Operation: The reducer receives input from several mapper and we need to perform the same step that we did in Mapper to capture only the top 5 elements that have lowest value for field4
  	public static class LFiveS_reducer extends Reducer<IntWritable, Text, Text, IntWritable>{
        private Integer scoreKey;
        private TreeMap<Integer, Text> recordHolder = new TreeMap<Integer, Text>();
    		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            
            scoreKey = key.get();
            for (Text val: values){
                recordHolder.put(scoreKey, val);

                if (recordHolder.size() > 5) {
                    recordHolder.remove(recordHolder.lastKey());   
                    // removes the last element from the tree map. A TreeMap contains elements in accending tree structure based on its keys
                } 
            }
        }

        // Cleanup is called by the Map function at the end of the Reduce task before writing the output to the HDFS
        protected void cleanup(Context context) throws IOException, InterruptedException {
              for(Map.Entry<Integer,Text> entry : recordHolder.entrySet()) {
                  context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
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
    	job_run.setOutputValueClass(IntWritable.class);
    
    	FileInputFormat.addInputPath(job_run, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    	job_run.waitForCompletion(true);
  	}
}

