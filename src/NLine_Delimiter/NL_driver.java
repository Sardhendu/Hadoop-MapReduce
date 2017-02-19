package NLine_Delimiter;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class NL_driver
{
  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException
  {
    JobConf job_run = new JobConf(NL_driver.class);
    job_run.setJobName("customInputformatter with mappered");
    
    job_run.setMapperClass(NL_mapper.class);
    job_run.setReducerClass(NL_reducer.class);
    job_run.setInputFormat(NL_inputformatter.class);
    
    job_run.setOutputKeyClass(Text.class);
    job_run.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.setInputPaths(job_run, new Path[] { new Path(args[0]) });
    FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    JobClient.runJob(job_run);
  }
}