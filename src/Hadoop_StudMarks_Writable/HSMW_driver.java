package Hadoop_StudMarks_Writable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HSMW_driver
{
  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException
  {
    Job job_run = new Job(new Configuration());
    
    job_run.setJobName("Dsecondary sort");
    
    job_run.setJarByClass(HSMW_driver.class);
    job_run.setMapperClass(HSMW_mapper.class);
    
    job_run.setPartitionerClass(HSMW_partitionar.class);
    job_run.setReducerClass(HSMW_reducer.class);
    
    job_run.setMapOutputKeyClass(HSMW_writable.class);
    job_run.setMapOutputValueClass(NullWritable.class);
    job_run.setOutputKeyClass(Text.class);
    job_run.setOutputValueClass(NullWritable.class);
    
    FileInputFormat.setInputPaths(job_run, new Path[] { new Path("/home/hduser/in_stud_writable.txt") });
    FileOutputFormat.setOutputPath(job_run, new Path("/home/hduser/compWritable_output"));
    
    job_run.waitForCompletion(true);
  }
}
