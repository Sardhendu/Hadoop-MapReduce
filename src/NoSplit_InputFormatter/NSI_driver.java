package NoSplit_InputFormatter;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class NSI_driver
{
  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException
  {
    JobConf job_run = new JobConf(NSI_driver.class);
    job_run.setJobName("nosplit input formatter with mappered");
    
    job_run.setMapperClass(NSI_mapper.class);
    job_run.setReducerClass(NSI_reducer.class);
    job_run.setInputFormat(NSI_inputformatter.class);
    
    job_run.setOutputKeyClass(Text.class);
    job_run.setOutputValueClass(NullWritable.class);
    
    FileInputFormat.setInputPaths(job_run, new Path[] { new Path(args[0]) });
    FileOutputFormat.setOutputPath(job_run, new Path(args[1]));
    
    JobClient.runJob(job_run);
  }
}