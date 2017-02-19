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
import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class MinMaxMean{
    // Writable Operation
    public static class MMM_Writable implements Writable {
      Integer field4_val;
      Integer field4_min;
      Integer field4_max;
      Integer count_one;
      Integer sum_val;
      Integer mean_val;
      String per_name;

      // Construction and initalization with 0 which is overridden
      public MMM_Writable() {
          field4_val = 0;
          field4_min = 0;
          field4_max = 0;
          count_one = 0;
          sum_val = 0;
          mean_val = 0;
          per_name = null;
        }

      // Implementing Setter Method --> Used for mapper to set the min and max value

      public void setName(String per_name){
          this.per_name = per_name;
      }

      public void setValue(Integer field4_val){
          this.field4_val = field4_val;
      }
      
      public void setMinvalue(Integer field4_min){
          this.field4_min = field4_min;
      }

      public void setMaxvalue(Integer field4_max){
          this.field4_max = field4_max;
      }

      public void setCount(Integer count_one){
          this.count_one = count_one;
      }

      public void setSum(Integer sum_val){
          this.sum_val = sum_val;
      }

      public void setMean(Integer mean_val){
          this.mean_val = mean_val;
      }


      // Implementing Getter Method --> Used by combiner or reducer to get the min and max value

      public Integer getValue(){
          return field4_val;
      }

      public Integer getMinvalue(){
          return field4_min;
      }

      public Integer getMaxvalue(){
          return field4_max;
      }

      public Integer getCount(){
          return count_one;
      }

      public Integer getSum(){
          return sum_val;
      }

      // readFile method
      public void readFields(DataInput input) throws IOException{
          field4_val = new Integer(input.readInt());
          field4_min = new Integer(input.readInt());
          field4_max = new Integer(input.readInt());
          count_one = new Integer(input.readInt());
          sum_val = new Integer(input.readInt()); 
      }

      // writeFile method
      public void write(DataOutput output) throws IOException{    // This method writes the latest output
          output.writeInt(field4_val);
          output.writeInt(field4_min);
          output.writeInt(field4_max);
          output.writeInt(count_one);
          output.writeInt(sum_val);
      }

      // The class requires a return type that writes to the file output (Basically serializing and deswrializing the output throuht th enetwork):
      public String toString() {
          return per_name + "," +field4_min + "," + field4_max + "," + mean_val;
      } 
  }


	// Mapper Operation, the mapper function will simply set the Minvalue and Maxalue for each user repedly. Afer the map job will write the username and a MMM_Writable objet with a Minvalue and Maxvalue
	public static class MMM_mapper extends Mapper<LongWritable, Text, Text, MMM_Writable>{
      private Text name = new Text();
      private Integer field4_val;
      private Integer field4_min;
      private Integer field4_max;
      private Integer one = new Integer(1);

  		MMM_Writable composite_val = new MMM_Writable();  // Cerating a object of class Writable so that the reducer can understand it of that type
  
  		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
  			String[] tokens = value.toString().split(",");
  			name.set(tokens[0]);

        // Selecting the Token 3 for Min, Max and Mean operation in Combiner and Reducer
        field4_val = Integer.parseInt(tokens[3]);

  			// System.out.println("field4_min is " + name + " and a.field4 is " + field4);
  			composite_val.setValue(field4_val);
        composite_val.setCount(one);
  			context.write(name, composite_val);
  		}
	}

	// Reducer Operation
  // In Reducer we perform the heavy lifting. Why? Because we are interested in finding the min, max and mean value of each user or given name. In reduces we know that every thing is sorted so for 1 user we get iterable values and then invoking compareTo would be much easy and comprehendible.
	public static class MMM_combiner extends Reducer<Text, MMM_Writable, Text, MMM_Writable>{
  		private MMM_Writable intermediateResult = new MMM_Writable();

  		public void reduce(Text key, Iterable<MMM_Writable> values, Context context) throws IOException, InterruptedException{
          // Integer minValue = 0;
          // Integer maxValue = 0;
          Integer field4value = 0;
          Integer keepCount = 0;
          Integer keepSum = 0;
  			  
          // The Writable object intermediateResult would give a value of min =0  anf max =0 as these are the value stored in the default constructer. So it is a good idea to set them to null to avoid min_value of 0 for every time.
          intermediateResult.setMinvalue(null);  
          intermediateResult.setMaxvalue(null);
          intermediateResult.setCount(0);
          intermediateResult.setSum(0);

          for (MMM_Writable val: values){
              field4value = val.getValue();

              // val.getCount() gives the count for the current iteraing user 
              keepCount += val.getCount();   
              keepSum += field4value;
              
              // compareTo outputs 0 if the values compared are same, output 1 if the first value is larger than the second and outputs -1 if the first value is smaller than second value
              if (intermediateResult.getMinvalue()==null || field4value.compareTo(intermediateResult.getMinvalue())<0){   
                  intermediateResult.setMinvalue(field4value);  // set the current iterable value as minValue if it is smaller that the previously min set value for the user
              }

              if (intermediateResult.getMaxvalue()==null || field4value.compareTo(intermediateResult.getMaxvalue())>0){   
                  intermediateResult.setMaxvalue(field4value);  // set the current iterable value as maxValue if it is larger that the previous set max value for the user
              }
          }

        intermediateResult.setCount(keepCount);
        intermediateResult.setSum(keepSum);  
  			context.write(key, intermediateResult);  // Provides the output for one user
  		}
	}




  public static class MMM_reducer extends Reducer<Text, MMM_Writable, NullWritable, MMM_Writable>{
      private MMM_Writable finalResult = new MMM_Writable();

      public void reduce(Text key, Iterable<MMM_Writable> values, Context context) throws IOException, InterruptedException{
          Integer minValue = 0;
          Integer maxValue = 0;
          Integer keepCount = 0;
          Integer keepSum = 0;
          Integer keepMean = 0;

          System.out.println("I am inside the reducer aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
          
          // The Writable object finalResult would give a value of min =0  anf max =0 as these are the value stored in the default constructer. So it is a good idea to set them to null to avoid min_value of 0 for every time.
          finalResult.setName(key.toString());
          finalResult.setMinvalue(null);  
          finalResult.setMaxvalue(null);

          for (MMM_Writable val: values){
              minValue = val.getMinvalue();
              maxValue = val.getMaxvalue();
            
              // val.getCount() gives the count for the current iteraing user 
              // finalResult.getCount() gives the count for the last stored count vaue for the same user.
              keepCount += val.getCount();   
              keepSum += val.getSum();
              

              // compareTo outputs 0 if the values compared are same, output 1 if the first value is larger than the second and outputs -1 if the first value is smaller than second value
              if (finalResult.getMinvalue()==null || minValue.compareTo(finalResult.getMinvalue())<0){   
                  finalResult.setMinvalue(minValue);  // set the current iterable value as minValue if it is smaller that the previously min set value for the user
              }

              if (finalResult.getMaxvalue()==null || maxValue.compareTo(finalResult.getMaxvalue())>0){   
                  finalResult.setMaxvalue(maxValue);  // set the current iterable value as maxValue if it is larger that the previous set max value for the user
              }
          }
        keepMean =  keepSum/keepCount;
        finalResult.setMean(keepMean); 

        context.write(NullWritable.get(), finalResult);  // Provides the output for one user
      }
  }




	// Driver Operation
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    	Job job_run = new Job(new Configuration());

    	job_run.setJobName("Min Max and Mean");
    	job_run.setJarByClass(MinMaxMean.class);

    	job_run.setMapperClass(MMM_mapper.class);
      job_run.setCombinerClass(MMM_combiner.class);
    	job_run.setReducerClass(MMM_reducer.class);
    
    	job_run.setMapOutputKeyClass(Text.class);
    	job_run.setMapOutputValueClass(MMM_Writable.class);
    	job_run.setOutputKeyClass(NullWritable.class);
    	job_run.setOutputValueClass(MMM_Writable.class);
    
    	FileInputFormat.addInputPath(job_run, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    	job_run.waitForCompletion(true);
  	}
}

