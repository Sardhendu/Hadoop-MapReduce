import java.util.Random;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SampleTenPrcntg{

	public static class MapSample extends Mapper<LongWritable, Text, NullWritable, Text>{

		private TreeMap<Integer, Text> recordHolder = new TreeMap<Integer, Text>();
		private int totalNum = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			totalNum += 1;
			Random rands = new Random();   				// Create an object for random number generator
			int randNum = rands.nextInt();
			recordHolder.put(randNum, new Text(value));			// Store the randNum with the actual data into a TreeMap that put the data in ascending order.
		}

		protected void cleanup (Context context) throws IOException, InterruptedException{
			int sampleNum = (int) (totalNum * 0.1);
			int indexNum = 0;
			for(Map.Entry<Integer,Text> entry : recordHolder.entrySet()) {
				// Using the if condition we only let the first 10% go through the MapOutput
				if (indexNum<sampleNum){
					context.write(NullWritable.get(), new Text(entry.getValue()));
				}
				else{
					break;
				}
				indexNum += 1;         
		    }
		}
	}

	// Reducer Function
	public static class ReduceSample extends Reducer<NullWritable, Text, NullWritable, Text>{
		private TreeMap<Integer, Text> recordHolder = new TreeMap<Integer, Text>();
		private int totalNum = 0;

		public void reduce(NullWritable key, Text value, Context context) throws IOException, InterruptedException{
			totalNum += 1;
			Random rands = new Random();   				// Create an object for random number generator
			int randNum = rands.nextInt();
			recordHolder.put(randNum, new Text (value));
		}

		protected void cleanup (Context context) throws IOException, InterruptedException{
			int sampleNum = (int) (totalNum * 0.1);
			int indexNum = 0;
			for(Map.Entry<Integer,Text> entry : recordHolder.entrySet()) {
				// Using the if condition we only let the first 10% go through the MapOutput
				if (indexNum<sampleNum){
					context.write(NullWritable.get(), new Text(entry.getValue()));
				}
				else{
					break;
				}
				indexNum += 1;	                
		    }
		}

	}

	// Driver Function
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    	Job job_run = new Job(new Configuration());

    	job_run.setJobName("Sample 10% of the Dataset");
    	job_run.setJarByClass(SampleTenPrcntg.class);

    	job_run.setMapperClass(MapSample.class);
    	job_run.setReducerClass(ReduceSample.class);

    	job_run.setOutputKeyClass(NullWritable.class);
    	job_run.setOutputValueClass(Text.class);
    
    	FileInputFormat.addInputPath(job_run, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    	job_run.waitForCompletion(true);
  	}
}