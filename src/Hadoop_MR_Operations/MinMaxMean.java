// package Hadoop_MR_Operations;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.Reducer.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Writable Operation
public class MinMaxMean{

	public static class MMM_writable implements WritableComparable<MMM_writable>{
        private int field4;
        private String name;
        
        public void setzNameField(String name, int field4){
            System.out.println("setzNameField method of writable class the name is     " + name);
            this.name = name.toString();
            this.field4 = field4;
        }
        
        public String getname(){
            System.out.println("returning the string name");
            return this.name;
        }
        
        public int getfield(){
            System.out.println("returning the int field4");
            return this.field4;
        }
        
        public void readFields(DataInput input) throws IOException{
        	this.field4 = input.readInt();
          	this.name = input.readUTF();
          	System.out.println("readfields method of writable class and the name is   " + this.name);
          	System.out.println("readfields method of writable class and the field4 is   " + this.field4);
        }
        
        public void write(DataOutput output) throws IOException{
        	output.writeInt(this.field4);
        	output.writeUTF(this.name);
        	System.out.println("write method of writable class and the name is    " + this.name);
        	System.out.println("write method of writable class and the field4 is    " + this.field4);
        }
        
        public int compareTo(MMM_writable a){
        	System.out.println("Inside the compare_to method of MMM_writable");
          	System.out.println("this.name is " + this.name + " and a.name is " + a.getname());
          	int val = this.name.compareTo(a.getname());
          	System.out.println("The val after comparision is " + val);
          	if (val == 0){
            	System.out.println("this.name is " + this.name + " and a.name is " + a.getname());

            	if (a.getfield() < this.field4){
              	System.out.println("this.field4 is " + this.field4 + " and a.field4 is " + a.getfield());
              	return 1;
            	}

            	if (a.getfield() > this.field4){
              	System.out.println("this.field4 is " + this.field4 + " and a.field4 is " + a.getfield());
              	return -1;
            	}
            
            	return 0;
          	}
          	return val;
        }
    }



	// Mapper Operation
	public static class MMM_mapper extends Mapper<LongWritable, Text, MMM_writable, NullWritable>{
  		NullWritable null_val = NullWritable.get();
  		MMM_writable composite_val = new MMM_writable();
  
  		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
  			String[] tokens = value.toString().split(",");
  			String name = tokens[0];
  			int field4 = Integer.parseInt(tokens[3]);
  			System.out.println("name is " + name + " and a.field4 is " + field4);
  			this.composite_val.setzNameField(name, field4);
  			context.write(this.composite_val, this.null_val);
  		}
	}

	// Reducer Operation
	public static class MMM_reducer extends Reducer<MMM_writable, NullWritable, Text, NullWritable>{
  		
  		public void reduce(MMM_writable key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{

  			System.out.println("I am inside the reducer aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  			String a = key.getname();
  			int b = key.getfield();

  			System.out.println("the name is " + a);
  			System.out.println("the field4 is " + b);
  			String fin_key = a + " " + b;
  			System.out.println(a);
  			System.out.println(b);
  			context.write(new Text(fin_key), null);
  		}
	}



	// Driver Operation
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    	Job job_run = new Job(new Configuration());

    	job_run.setJobName("Min Max and Mean");
    	job_run.setJarByClass(MinMaxMean.class);

    	job_run.setMapperClass(MMM_mapper.class);
    	// job_run.setPartitionerClass(MMM_partitionar.class);
    	job_run.setReducerClass(MMM_reducer.class);
    
    	job_run.setMapOutputKeyClass(MMM_writable.class);
    	job_run.setMapOutputValueClass(NullWritable.class);
    	job_run.setOutputKeyClass(Text.class);
    	job_run.setOutputValueClass(NullWritable.class);
    
    	FileInputFormat.addInputPath(job_run, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    	job_run.waitForCompletion(true);
  	}
}

